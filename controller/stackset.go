package controller

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	log "github.com/sirupsen/logrus"
	zv1 "github.com/zalando-incubator/stackset-controller/pkg/apis/zalando/v1"
	clientset "github.com/zalando-incubator/stackset-controller/pkg/client/clientset/versioned"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	stacksetHeritageLabelKey                  = "stackset"
	stackVersionLabelKey                      = "stack-version"
	defaultVersion                            = "default"
	defaultStackLifecycleLimit                = 10
	stacksetControllerControllerAnnotationKey = "stackset-controller.zalando.org/controller"
)

var (
	defaultBackendPort = intstr.FromString("ingress")
)

// StackSetController is the main controller. It watches for changes to
// stackset resources and starts and maintains other controllers per
// stackset resource.
type StackSetController struct {
	logger                  *log.Entry
	kube                    kubernetes.Interface
	appClient               clientset.Interface
	controllerID            string
	interval                time.Duration
	stacksetStackMinGCAge   time.Duration
	noTrafficScaledownTTL   time.Duration
	noTrafficTerminationTTL time.Duration
	controllerTable         map[types.UID]controllerEntry
	stacksetEvents          chan stacksetEvent
	sync.Mutex
}

type stacksetEvent struct {
	Deleted  bool
	StackSet *zv1.StackSet
}

type controllerEntry struct {
	Cancel context.CancelFunc
	Done   []<-chan struct{}
}

// NewStackSetController initializes a new StackSetController.
func NewStackSetController(client kubernetes.Interface, appClient clientset.Interface, controllerID string, stacksetStackMinGCAge, noTrafficScaledownTTL, noTrafficTerminationTTL, interval time.Duration) *StackSetController {
	return &StackSetController{
		logger:                  log.WithFields(log.Fields{"controller": "stackset"}),
		kube:                    client,
		appClient:               appClient,
		controllerID:            controllerID,
		stacksetStackMinGCAge:   stacksetStackMinGCAge,
		noTrafficScaledownTTL:   noTrafficScaledownTTL,
		noTrafficTerminationTTL: noTrafficTerminationTTL,
		stacksetEvents:          make(chan stacksetEvent, 1),
		controllerTable:         map[types.UID]controllerEntry{},
		interval:                interval,
	}
}

// Run runs the main loop of the StackSetController. Before the loops it
// sets up a watcher to watch StackSet resources. The watch will send
// changes over a channel which is polled from the main loop.
func (c *StackSetController) Run(ctx context.Context) {
	c.startWatch(ctx)

	for {
		select {
		case e := <-c.stacksetEvents:
			stackset := *e.StackSet
			// set TypeMeta manually because of this bug:
			// https://github.com/kubernetes/client-go/issues/308
			stackset.APIVersion = "zalando.org/v1"
			stackset.Kind = "StackSet"

			// set default ingress backend port if not specified.
			if stackset.Spec.Ingress != nil && intOrStrIsEmpty(stackset.Spec.Ingress.BackendPort) {
				stackset.Spec.Ingress.BackendPort = defaultBackendPort
			}

			// clear existing entry
			if entry, ok := c.controllerTable[stackset.UID]; ok {
				c.logger.Infof("Stopping controllers for StackSet %s/%s", stackset.Namespace, stackset.Name)
				entry.Cancel()
				for _, d := range entry.Done {
					<-d
				}
				delete(c.controllerTable, stackset.UID)
				c.logger.Infof("Controllers stopped for StackSet %s/%s", stackset.Namespace, stackset.Name)
			}

			if e.Deleted {
				continue
			}

			// check if stackset should be managed by the controller
			if !c.hasOwnership(&stackset) {
				continue
			}

			err := c.manageStackSet(&stackset)
			if err != nil {
				c.logger.Error(err)
				continue
			}

			c.logger.Infof("Starting controllers for StackSet %s/%s", stackset.Namespace, stackset.Name)
			ctx, cancel := context.WithCancel(ctx)
			entry := controllerEntry{
				Cancel: cancel,
				Done:   []<-chan struct{}{},
			}

			stackControllerDone := make(chan struct{}, 1)
			entry.Done = append(entry.Done, stackControllerDone)
			stackController := NewStackController(c.kube, c.appClient, stackset, stackControllerDone, c.noTrafficScaledownTTL, c.noTrafficTerminationTTL, c.interval)
			go stackController.Run(ctx)

			ingressControllerDone := make(chan struct{}, 1)
			entry.Done = append(entry.Done, ingressControllerDone)
			ingressController := NewIngressController(c.kube, c.appClient, stackset, ingressControllerDone, c.interval)
			go ingressController.Run(ctx)

			stackGCDone := make(chan struct{}, 1)
			entry.Done = append(entry.Done, stackGCDone)
			go c.runStackGC(ctx, stackset, stackGCDone)

			updateStatusDone := make(chan struct{}, 1)
			entry.Done = append(entry.Done, updateStatusDone)
			go c.runUpdateStatus(ctx, stackset, updateStatusDone)

			c.controllerTable[stackset.UID] = entry
		case <-ctx.Done():
			c.logger.Info("Terminating main controller loop.")
			// wait for all controllers
			for uid, entry := range c.controllerTable {
				entry.Cancel()
				for _, d := range entry.Done {
					<-d
				}
				delete(c.controllerTable, uid)
			}
			return
		}
	}
}

// hasOwnership returns true if the controller is the "owner" of the stackset.
// Whether it's owner is determined by the value of the
// 'stackset-controller.zalando.org/controller' annotation. If the value
// matches the controllerID then it owns it, or if the controllerID is
// "" and there's no annotation set.
func (c *StackSetController) hasOwnership(stackset *zv1.StackSet) bool {
	if stackset.Annotations != nil {
		if owner, ok := stackset.Annotations[stacksetControllerControllerAnnotationKey]; ok {
			return owner == c.controllerID
		}
	}
	return c.controllerID == ""
}

func (c *StackSetController) startWatch(ctx context.Context) {
	informer := cache.NewSharedIndexInformer(
		cache.NewListWatchFromClient(c.appClient.ZalandoV1().RESTClient(), "stacksets", v1.NamespaceAll, fields.Everything()),
		&zv1.StackSet{},
		0, // skip resync
		cache.Indexers{},
	)

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.add,
		UpdateFunc: c.update,
		DeleteFunc: c.del,
	})

	go informer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		c.logger.Errorf("Timed out waiting for caches to sync")
		return
	}

	c.logger.Info("Synced StackSet watcher")
}

func (c *StackSetController) add(obj interface{}) {
	stackset, ok := obj.(*zv1.StackSet)
	if !ok {
		c.logger.Error("Failed to get StackSet object")
		return
	}

	c.logger.Infof("New StackSet added %s/%s", stackset.Namespace, stackset.Name)
	c.stacksetEvents <- stacksetEvent{
		StackSet: stackset.DeepCopy(),
	}
}

func (c *StackSetController) update(oldObj, newObj interface{}) {
	newStackset, ok := newObj.(*zv1.StackSet)
	if !ok {
		c.logger.Error("Failed to get StackSet object")
		return
	}

	oldStackset, ok := oldObj.(*zv1.StackSet)
	if !ok {
		c.logger.Error("Failed to get StackSet object")
		return
	}

	c.logger.Debugf("StackSet %s/%s changed: %s",
		newStackset.Namespace,
		newStackset.Name,
		cmp.Diff(oldStackset, newStackset, cmpopts.IgnoreUnexported(resource.Quantity{})),
	)

	c.logger.Infof("StackSet updated %s/%s", newStackset.Namespace, newStackset.Name)
	c.stacksetEvents <- stacksetEvent{
		StackSet: newStackset.DeepCopy(),
	}
}

func (c *StackSetController) del(obj interface{}) {
	stackset, ok := obj.(*zv1.StackSet)
	if !ok {
		c.logger.Error("Failed to get StackSet object")
		return
	}

	c.logger.Infof("StackSet deleted %s/%s", stackset.Namespace, stackset.Name)
	c.stacksetEvents <- stacksetEvent{
		StackSet: stackset.DeepCopy(),
		Deleted:  true,
	}
}

func (c *StackSetController) runUpdateStatus(ctx context.Context, stackset zv1.StackSet, done chan<- struct{}) {
	for {
		err := c.updateStatus(stackset)
		if err != nil {
			c.logger.Error(err)
		}

		select {
		case <-time.After(c.interval):
		case <-ctx.Done():
			c.logger.Info("Terminating update status loop.")
			done <- struct{}{}
			return
		}
	}
}

func (c *StackSetController) updateStatus(stackset zv1.StackSet) error {
	heritageLabels := map[string]string{
		stacksetHeritageLabelKey: stackset.Name,
	}
	opts := metav1.ListOptions{
		LabelSelector: labels.Set(heritageLabels).String(),
	}

	stacks, err := c.appClient.ZalandoV1().Stacks(stackset.Namespace).List(opts)
	if err != nil {
		return fmt.Errorf("failed to list Stacks of StackSet %s/%s: %v", stackset.Namespace, stackset.Name, err)
	}

	var traffic map[string]TrafficStatus
	if stackset.Spec.Ingress != nil && len(stacks.Items) > 0 {
		traffic, err = getIngressTraffic(c.kube, &stackset)
		if err != nil {
			return fmt.Errorf("failed to get Ingress traffic for StackSet %s/%s: %v", stackset.Namespace, stackset.Name, err)
		}
	}

	stacksWithTraffic := int32(0)
	for _, stack := range stacks.Items {
		if traffic != nil && traffic[stack.Name].Weight() > 0 {
			stacksWithTraffic++
		}
	}

	newStatus := zv1.StackSetStatus{
		Stacks:            int32(len(stacks.Items)),
		StacksWithTraffic: stacksWithTraffic,
		ReadyStacks:       readyStacks(stacks.Items),
	}

	if !reflect.DeepEqual(newStatus, stackset.Status) {
		stackset.Status = newStatus
		// TODO: log the change in status
		// update status of stackset
		_, err = c.appClient.ZalandoV1().StackSets(stackset.Namespace).UpdateStatus(&stackset)
		if err != nil {
			return err
		}
	}

	return nil
}

func readyStacks(stacks []zv1.Stack) int32 {
	var readyStacks int32
	for _, stack := range stacks {
		replicas := stack.Status.Replicas
		if replicas == stack.Status.ReadyReplicas && replicas == stack.Status.UpdatedReplicas {
			readyStacks++
		}
	}
	return readyStacks
}

func (c *StackSetController) runStackGC(ctx context.Context, stackset zv1.StackSet, done chan<- struct{}) {
	for {
		err := c.gcStacks(stackset)
		if err != nil {
			c.logger.Error(err)
		}

		select {
		// TODO: change GC interval
		case <-time.After(c.interval):
		case <-ctx.Done():
			c.logger.Info("Terminating stack Garbage collector.")
			done <- struct{}{}
			return
		}
	}
}

func (c *StackSetController) gcStacks(stackset zv1.StackSet) error {
	heritageLabels := map[string]string{
		stacksetHeritageLabelKey: stackset.Name,
	}
	opts := metav1.ListOptions{
		LabelSelector: labels.Set(heritageLabels).String(),
	}

	stacks, err := c.appClient.ZalandoV1().Stacks(stackset.Namespace).List(opts)
	if err != nil {
		return fmt.Errorf("failed to list Stacks of StackSet %s/%s: %v", stackset.Namespace, stackset.Name, err)
	}

	var traffic map[string]TrafficStatus
	if stackset.Spec.Ingress != nil && len(stacks.Items) > 0 {
		traffic, err = getIngressTraffic(c.kube, &stackset)
		if err != nil {
			return fmt.Errorf("failed to get Ingress traffic for StackSet %s/%s: %v", stackset.Namespace, stackset.Name, err)
		}
	}

	historyLimit := defaultStackLifecycleLimit
	if stackset.Spec.StackLifecycle != nil && stackset.Spec.StackLifecycle.Limit != nil {
		historyLimit = int(*stackset.Spec.StackLifecycle.Limit)
	}

	gcCandidates := make([]zv1.Stack, 0, len(stacks.Items))
	for _, stack := range stacks.Items {
		// never garbage collect stacks with traffic
		if traffic != nil && traffic[stack.Name].Weight() > 0 {
			continue
		}

		if time.Since(stack.CreationTimestamp.Time) > c.stacksetStackMinGCAge {
			gcCandidates = append(gcCandidates, stack)
		}
	}

	// only garbage collect if history limit is reached
	if len(stacks.Items) <= historyLimit {
		c.logger.Debugf("No Stacks to clean up for StackSet %s/%s (limit: %d/%d)", stackset.Namespace, stackset.Name, len(stacks.Items), historyLimit)
		return nil
	}

	// sort candidates by oldest
	sort.Slice(gcCandidates, func(i, j int) bool {
		return gcCandidates[i].CreationTimestamp.Time.Before(gcCandidates[j].CreationTimestamp.Time)
	})

	excessStacks := len(stacks.Items) - historyLimit
	c.logger.Infof(
		"Found %d Stack(s) exeeding the StackHistoryLimit (%d) for StackSet %s/%s. %d candidate(s) for GC",
		excessStacks,
		historyLimit,
		stackset.Namespace,
		stackset.Name,
		len(gcCandidates),
	)

	gcLimit := int(math.Min(float64(excessStacks), float64(len(gcCandidates))))

	for _, stack := range gcCandidates[:gcLimit] {
		c.logger.Infof(
			"Deleting excess stack %s/%s for StackSet %s/%s",
			stack.Namespace,
			stack.Name,
			stackset.Namespace,
			stackset.Name,
		)
		err := c.appClient.ZalandoV1().Stacks(stack.Namespace).Delete(stack.Name, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func strInSlice(str string, slice []string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func (c *StackSetController) manageStackSet(stackset *zv1.StackSet) error {
	heritageLabels := map[string]string{
		stacksetHeritageLabelKey: stackset.Name,
	}
	opts := metav1.ListOptions{
		LabelSelector: labels.Set(heritageLabels).String(),
	}

	version := stackset.Spec.StackTemplate.Spec.Version
	if version == "" {
		version = defaultVersion
	}

	stackName := stackset.Name + "-" + version

	stacks, err := c.appClient.ZalandoV1().Stacks(stackset.Namespace).List(opts)
	if err != nil {
		return fmt.Errorf("failed to list stacks of StackSet %s/%s: %v", stackset.Namespace, stackset.Name, err)
	}

	var stack *zv1.Stack
	for _, s := range stacks.Items {
		if s.Name == stackName {
			stack = &s
			break
		}
	}

	stackLabels := mergeLabels(
		heritageLabels,
		stackset.Labels,
		map[string]string{stackVersionLabelKey: version},
	)

	createStack := false

	if stack == nil {
		createStack = true
		stack = &zv1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stackName,
				Namespace: stackset.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: stackset.APIVersion,
						Kind:       stackset.Kind,
						Name:       stackset.Name,
						UID:        stackset.UID,
					},
				},
			},
		}
	}

	stack.Labels = stackLabels
	stack.Spec.PodTemplate = *stackset.Spec.StackTemplate.Spec.PodTemplate.DeepCopy()
	stack.Spec.Replicas = stackset.Spec.StackTemplate.Spec.Replicas
	stack.Spec.HorizontalPodAutoscaler = stackset.Spec.StackTemplate.Spec.HorizontalPodAutoscaler
	if stackset.Spec.StackTemplate.Spec.Service != nil {
		stack.Spec.Service = sanitizeServicePorts(stackset.Spec.StackTemplate.Spec.Service)
	}

	if createStack {
		c.logger.Infof(
			"Creating StackSet stack %s/%s for StackSet %s/%s",
			stack.Namespace, stack.Name,
			stackset.Namespace,
			stackset.Name,
		)
		_, err := c.appClient.ZalandoV1().Stacks(stack.Namespace).Create(stack)
		if err != nil {
			return err
		}
	} else {
		c.logger.Infof(
			"Updating StackSet stack %s/%s for StackSet %s/%s",
			stack.Namespace,
			stack.Name,
			stackset.Namespace,
			stackset.Name,
		)
		_, err := c.appClient.ZalandoV1().Stacks(stack.Namespace).Update(stack)
		if err != nil {
			return err
		}
	}

	return nil
}

// sanitizeServicePorts makes sure the ports has the default fields set if not
// specified.
func sanitizeServicePorts(service *zv1.StackServiceSpec) *zv1.StackServiceSpec {
	for i, port := range service.Ports {
		// set default protocol if not specified
		if port.Protocol == "" {
			port.Protocol = v1.ProtocolTCP
		}
		service.Ports[i] = port
	}
	return service
}

func mergeLabels(labelMaps ...map[string]string) map[string]string {
	labels := make(map[string]string)
	for _, labelMap := range labelMaps {
		for k, v := range labelMap {
			labels[k] = v
		}
	}
	return labels
}

func intOrStrIsEmpty(v intstr.IntOrString) bool {
	switch v.Type {
	case intstr.Int:
		return v.IntVal == 0
	case intstr.String:
		return v.StrVal == ""
	default:
		return true
	}
}
