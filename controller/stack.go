package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	log "github.com/sirupsen/logrus"
	zv1 "github.com/zalando-incubator/stackset-controller/pkg/apis/zalando/v1"
	clientset "github.com/zalando-incubator/stackset-controller/pkg/client/clientset/versioned"
	appsv1 "k8s.io/api/apps/v1"
	autoscaling "k8s.io/api/autoscaling/v2beta1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	noTrafficSinceAnnotationKey = "stacksetstacks.zalando.org/no-traffic-since"
)

// StackController is a controller for managing StackSet Stack
// resources like Deployment and Service.
type StackController struct {
	logger                  *log.Entry
	kube                    kubernetes.Interface
	appClient               clientset.Interface
	stackset                zv1.StackSet
	noTrafficScaledownTTL   time.Duration
	noTrafficTerminationTTL time.Duration
	interval                time.Duration
	done                    chan<- struct{}
}

// NewStackController initializes a new StackController.
func NewStackController(client kubernetes.Interface, appClient clientset.Interface, stackset zv1.StackSet, done chan<- struct{}, noTrafficScaledownTTL, noTrafficTerminationTTL, interval time.Duration) *StackController {
	return &StackController{
		logger: log.WithFields(
			log.Fields{
				"controller": "stack",
				"stackset":   stackset.Name,
				"namespace":  stackset.Namespace,
			},
		),
		kube:                    client,
		appClient:               appClient,
		stackset:                stackset,
		noTrafficScaledownTTL:   noTrafficScaledownTTL,
		noTrafficTerminationTTL: noTrafficTerminationTTL,
		done:     done,
		interval: interval,
	}
}

// Run runs the Stack Controller control loop.
func (c *StackController) Run(ctx context.Context) {
	for {
		err := c.runOnce()
		if err != nil {
			c.logger.Error(err)
		}

		select {
		case <-time.After(c.interval):
		case <-ctx.Done():
			c.logger.Info("Terminating Stack Controller.")
			c.done <- struct{}{}
			return
		}
	}
}

// runOnce runs one loop of the Stack Controller.
func (c *StackController) runOnce() error {
	heritageLabels := map[string]string{
		stacksetHeritageLabelKey: c.stackset.Name,
	}
	opts := metav1.ListOptions{
		LabelSelector: labels.Set(heritageLabels).String(),
	}

	stacks, err := c.appClient.ZalandoV1().Stacks(c.stackset.Namespace).List(opts)
	if err != nil {
		return fmt.Errorf("failed to list Stacks of StackSet %s/%s: %v", c.stackset.Namespace, c.stackset.Name, err)
	}

	var traffic map[string]TrafficStatus
	if c.stackset.Spec.Ingress != nil && len(stacks.Items) > 0 {
		traffic, err = getIngressTraffic(c.kube, &c.stackset)
		if err != nil {
			return fmt.Errorf("failed to get Ingress traffic for StackSet %s/%s: %v", c.stackset.Namespace, c.stackset.Name, err)
		}
	}

	for _, stack := range stacks.Items {
		err = c.manageStack(stack, traffic)
		if err != nil {
			log.Errorf("Failed to manage Stack %s/%s: %v", stack.Namespace, stack.Name, err)
			continue
		}
	}

	return nil
}

func getIngressTraffic(client kubernetes.Interface, stackset *zv1.StackSet) (map[string]TrafficStatus, error) {
	ingress, err := getIngress(client, stackset)
	if err != nil {
		return nil, err
	}

	desiredTraffic := make(map[string]float64)
	if weights, ok := ingress.Annotations[stackTrafficWeightsAnnotationKey]; ok {
		err := json.Unmarshal([]byte(weights), &desiredTraffic)
		if err != nil {
			return nil, fmt.Errorf("failed to get current desired Stack traffic weights: %v", err)
		}
	}

	actualTraffic := make(map[string]float64)
	if weights, ok := ingress.Annotations[backendWeightsAnnotationKey]; ok {
		err := json.Unmarshal([]byte(weights), &actualTraffic)
		if err != nil {
			return nil, fmt.Errorf("failed to get current actual Stack traffic weights: %v", err)
		}
	}

	traffic := make(map[string]TrafficStatus, len(desiredTraffic))

	for stackName, weight := range desiredTraffic {
		traffic[stackName] = TrafficStatus{
			ActualWeight:  actualTraffic[stackName],
			DesiredWeight: weight,
		}
	}

	return traffic, nil
}

// manageStack manages the stack by managing the related Deployment and Service
// resources.
func (c *StackController) manageStack(stack zv1.Stack, traffic map[string]TrafficStatus) error {
	err := c.manageDeployment(stack, traffic)
	if err != nil {
		return err
	}

	return nil
}

// manageDeployment manages the deployment owned by the stack.
func (c *StackController) manageDeployment(stack zv1.Stack, traffic map[string]TrafficStatus) error {
	deployment, err := getDeployment(c.kube, stack)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	}

	var origDeployment *appsv1.Deployment
	if deployment != nil {
		origDeployment = deployment.DeepCopy()
	}

	template := templateInjectLabels(stack.Spec.PodTemplate, stack.Labels)

	// apply default pod template spec values into the stack template.
	// This is sort of a hack to make sure the bare template from the stack
	// resource represents the defaults so we can compare changes later on
	// when deciding if the deployment resource should be updated or not.
	// TODO: find less ugly solution.
	template = applyPodTemplateSpecDefaults(template)

	createDeployment := false

	if deployment == nil {
		createDeployment = true
		deployment = &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:        stack.Name,
				Namespace:   stack.Namespace,
				Annotations: map[string]string{},
				Labels:      map[string]string{},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: stack.APIVersion,
						Kind:       stack.Kind,
						Name:       stack.Name,
						UID:        stack.UID,
					},
				},
			},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: stack.Labels,
				},
				Replicas: stack.Spec.Replicas,
			},
		}
	}

	for k, v := range stack.Labels {
		deployment.Labels[k] = v
	}

	deployment.Spec.Template = template

	// if autoscaling is disabled or if autoscaling is ena
	// check if we need to explicitly set replicas on the deplotment. There
	// are two cases:
	// 1. Autoscaling is disabled and we should rely on the replicas set on
	//    the stack
	// 2. Autoscaling is enabled, but current replica count is 0. In this
	//    case we have to set a value > 0 otherwise the autoscaler won't do
	//    anything.
	if stack.Spec.HorizontalPodAutoscaler == nil ||
		(deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == 0) {
		deployment.Spec.Replicas = stack.Spec.Replicas
	}

	if traffic != nil && traffic[stack.Name].Weight() <= 0 {
		if ttl, ok := deployment.Annotations[noTrafficSinceAnnotationKey]; ok {
			noTrafficSince, err := time.Parse(time.RFC3339, ttl)
			if err != nil {
				return fmt.Errorf("failed to parse no-traffic-since timestamp '%s': %v", ttl, err)
			}

			// TODO: make ttl configurable per app/stack
			if !noTrafficSince.IsZero() && time.Since(noTrafficSince) > c.noTrafficScaledownTTL {
				replicas := int32(0)
				deployment.Spec.Replicas = &replicas
			}

			if !noTrafficSince.IsZero() && time.Since(noTrafficSince) > c.noTrafficTerminationTTL {
				// delete deployment
				if !createDeployment {
					c.logger.Infof("Deleting Deployment %s/%s no longer needed", deployment.Namespace, deployment.Name)
					err = c.kube.AppsV1().Deployments(deployment.Namespace).Delete(deployment.Name, nil)
					if err != nil {
						return fmt.Errorf(
							"failed to delete Deployment %s/%s owned by Stack %s/%s",
							deployment.Namespace,
							deployment.Name,
							stack.Namespace,
							stack.Name,
						)
					}
				}
				return nil
			}
		} else {
			deployment.Annotations[noTrafficSinceAnnotationKey] = time.Now().UTC().Format(time.RFC3339)
		}
	} else {
		// ensure the scaledown annotation is removed if the stack has
		// traffic.
		if _, ok := deployment.Annotations[noTrafficSinceAnnotationKey]; ok {
			delete(deployment.Annotations, noTrafficSinceAnnotationKey)
		}
	}

	if createDeployment {
		c.logger.Infof(
			"Creating Deployment %s/%s for StackSet stack %s/%s",
			deployment.Namespace,
			deployment.Name,
			stack.Namespace,
			stack.Name,
		)
		deployment, err = c.kube.AppsV1().Deployments(deployment.Namespace).Create(deployment)
		if err != nil {
			return err
		}
	} else {
		// only update the resource if there are changes
		// TODO: still if we add just the annotation it could mess with
		// the HPA.
		if !reflect.DeepEqual(origDeployment, deployment) {
			c.logger.Debugf("Deployment %s/%s changed: %s",
				deployment.Namespace, deployment.Name,
				cmp.Diff(
					origDeployment,
					deployment,
					cmpopts.IgnoreUnexported(resource.Quantity{}),
				),
			)
			c.logger.Infof(
				"Updating Deployment %s/%s for StackSet stack %s/%s",
				deployment.Namespace,
				deployment.Name,
				stack.Namespace,
				stack.Name,
			)
			deployment, err = c.kube.AppsV1().Deployments(deployment.Namespace).Update(deployment)
			if err != nil {
				return err
			}
		}
	}

	// set TypeMeta manually because of this bug:
	// https://github.com/kubernetes/client-go/issues/308
	deployment.APIVersion = "apps/v1"
	deployment.Kind = "Deployment"

	hpa, err := c.manageAutoscaling(stack, deployment, traffic)
	if err != nil {
		return err
	}

	err = c.manageService(stack, deployment)
	if err != nil {
		return err
	}

	// update stack status
	stack.Status.Replicas = deployment.Status.Replicas
	stack.Status.ReadyReplicas = deployment.Status.ReadyReplicas
	stack.Status.UpdatedReplicas = deployment.Status.UpdatedReplicas

	if traffic != nil {
		stack.Status.ActualTrafficWeight = traffic[stack.Name].ActualWeight
		stack.Status.DesiredTrafficWeight = traffic[stack.Name].DesiredWeight
	}

	if hpa != nil {
		stack.Status.DesiredReplicas = hpa.Status.DesiredReplicas
	}

	// TODO: log the change in status
	// update status of stackset
	_, err = c.appClient.ZalandoV1().Stacks(stack.Namespace).UpdateStatus(&stack)
	if err != nil {
		return err
	}

	return nil
}

type TrafficStatus struct {
	ActualWeight  float64
	DesiredWeight float64
}

func (t TrafficStatus) Weight() float64 {
	return math.Max(t.ActualWeight, t.DesiredWeight)
}

// manageAutoscaling manages the HPA defined for the stack.
func (c *StackController) manageAutoscaling(stack zv1.Stack, deployment *appsv1.Deployment, traffic map[string]TrafficStatus) (*autoscaling.HorizontalPodAutoscaler, error) {
	hpa, err := c.getHPA(deployment)
	if err != nil {
		if !errors.IsNotFound(err) {
			return nil, err
		}
	}

	var origHPA *autoscaling.HorizontalPodAutoscaler
	if hpa != nil {
		hpa.Status = autoscaling.HorizontalPodAutoscalerStatus{}
		origHPA = hpa.DeepCopy()
	}

	// cleanup HPA if autoscaling is disabled or the stack has 0 traffic.
	if stack.Spec.HorizontalPodAutoscaler == nil || (traffic != nil && traffic[stack.Name].Weight() <= 0) {
		if hpa != nil {
			c.logger.Infof(
				"Deleting obsolete HPA %s/%s for Deployment %s/%s",
				hpa.Namespace,
				hpa.Name,
				deployment.Namespace,
				deployment.Name,
			)
			return nil, c.kube.AutoscalingV2beta1().HorizontalPodAutoscalers(hpa.Namespace).Delete(hpa.Name, nil)
		}
		return nil, nil
	}

	createHPA := false

	if hpa == nil {
		createHPA = true
		hpa = &autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:        deployment.Name,
				Namespace:   deployment.Namespace,
				Annotations: stack.Spec.HorizontalPodAutoscaler.Annotations,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: deployment.APIVersion,
						Kind:       deployment.Kind,
						Name:       deployment.Name,
						UID:        deployment.UID,
					},
				},
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					APIVersion: deployment.APIVersion,
					Kind:       deployment.Kind,
					Name:       deployment.Name,
				},
			},
		}
	}

	hpa.Labels = deployment.Labels
	hpa.Spec.MinReplicas = stack.Spec.HorizontalPodAutoscaler.MinReplicas
	hpa.Spec.MaxReplicas = stack.Spec.HorizontalPodAutoscaler.MaxReplicas
	hpa.Spec.Metrics = stack.Spec.HorizontalPodAutoscaler.Metrics

	if createHPA {
		c.logger.Infof(
			"Creating HPA %s/%s for Deployment %s/%s",
			hpa.Namespace,
			hpa.Name,
			deployment.Namespace,
			deployment.Name,
		)
		_, err := c.kube.AutoscalingV2beta1().HorizontalPodAutoscalers(hpa.Namespace).Create(hpa)
		if err != nil {
			return nil, err
		}
	} else {
		if !reflect.DeepEqual(origHPA, hpa) {
			c.logger.Debugf("HPA %s/%s changed: %s", hpa.Namespace, hpa.Name, cmp.Diff(origHPA, hpa))
			c.logger.Infof(
				"Updating HPA %s/%s for Deployment %s/%s",
				hpa.Namespace,
				hpa.Name,
				deployment.Namespace,
				deployment.Name,
			)
			hpa, err = c.kube.AutoscalingV2beta1().HorizontalPodAutoscalers(hpa.Namespace).Update(hpa)
			if err != nil {
				return nil, err
			}
		}
	}

	return hpa, nil
}

// getHPA gets HPA owned by the Deployment.
func (c *StackController) getHPA(deployment *appsv1.Deployment) (*autoscaling.HorizontalPodAutoscaler, error) {
	// check for existing object
	hpa, err := c.kube.AutoscalingV2beta1().HorizontalPodAutoscalers(deployment.Namespace).Get(deployment.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// check if object is owned by the deployment resource
	if !isOwnedReference(deployment.TypeMeta, deployment.ObjectMeta, hpa.ObjectMeta) {
		return nil, fmt.Errorf(
			"found HPA '%s/%s' not managed by the Deployment %s/%s",
			hpa.Namespace,
			hpa.Name,
			deployment.Namespace,
			deployment.Name,
		)
	}

	return hpa, nil
}

// manageService manages the service for a given stack.
func (c *StackController) manageService(stack zv1.Stack, deployment *appsv1.Deployment) error {
	service, err := getStackService(c.kube, *deployment)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	}

	var origService *v1.Service
	if service != nil {
		origService = service.DeepCopy()
	}

	createService := false

	if service == nil {
		createService = true
		service = &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stack.Name,
				Namespace: stack.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: deployment.APIVersion,
						Kind:       deployment.Kind,
						Name:       deployment.Name,
						UID:        deployment.UID,
					},
				},
			},
			Spec: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
			},
		}
	}

	service.Labels = stack.Labels
	service.Spec.Selector = stack.Labels
	// get service ports to be used for the service
	servicePorts, err := getServicePorts(c.stackset.Spec.Ingress.BackendPort, stack)
	if err != nil {
		return err
	}
	service.Spec.Ports = servicePorts

	if createService {
		c.logger.Infof(
			"Creating Service %s/%s for StackSet stack %s/%s",
			service.Namespace,
			service.Name,
			stack.Namespace,
			stack.Name,
		)
		_, err := c.kube.CoreV1().Services(service.Namespace).Create(service)
		if err != nil {
			return err
		}
	} else {
		if !reflect.DeepEqual(origService, service) {
			c.logger.Debugf("Service %s/%s changed: %s", service.Namespace, service.Name, cmp.Diff(origService, service))
			c.logger.Infof(
				"Updating Service %s/%s for StackSet stack %s/%s",
				service.Namespace,
				service.Name,
				stack.Namespace,
				stack.Name,
			)
			_, err := c.kube.CoreV1().Services(service.Namespace).Update(service)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// getDeployment gets Deployment owned by StackSet stack.
func getDeployment(kube kubernetes.Interface, stack zv1.Stack) (*appsv1.Deployment, error) {
	// check for existing object
	deployment, err := kube.AppsV1().Deployments(stack.Namespace).Get(stack.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// check if object is owned by the stack resource
	if !isOwnedReference(stack.TypeMeta, stack.ObjectMeta, deployment.ObjectMeta) {
		return nil, fmt.Errorf(
			"found Deployment '%s/%s' not managed by the StackSet stack %s/%s",
			deployment.Namespace,
			deployment.Name,
			stack.Namespace,
			stack.Name,
		)
	}

	return deployment, nil
}

// getStackService gets service owned by StackSet stack.
func getStackService(kube kubernetes.Interface, deployment appsv1.Deployment) (*v1.Service, error) {
	// check for existing object
	service, err := kube.CoreV1().Services(deployment.Namespace).Get(deployment.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// check if object is owned by the deployment resource
	if !isOwnedReference(deployment.TypeMeta, deployment.ObjectMeta, service.ObjectMeta) {
		return nil, fmt.Errorf(
			"found Service '%s/%s' not managed by the Deployment %s/%s",
			service.Namespace,
			service.Name,
			deployment.Namespace,
			deployment.Name,
		)
	}

	return service, nil
}

// getServicePorts gets the service ports to be used for the stack service.
func getServicePorts(backendPort intstr.IntOrString, stack zv1.Stack) ([]v1.ServicePort, error) {
	var servicePorts []v1.ServicePort
	if stack.Spec.Service == nil || len(stack.Spec.Service.Ports) == 0 {
		servicePorts = servicePortsFromContainers(stack.Spec.PodTemplate.Spec.Containers)
	} else if stack.Spec.Service != nil {
		servicePorts = stack.Spec.Service.Ports
	}

	// validate that one port in the list map to the backendPort.
	for _, port := range servicePorts {
		switch backendPort.Type {
		case intstr.Int:
			if port.Port == backendPort.IntVal {
				return servicePorts, nil
			}
		case intstr.String:
			if port.Name == backendPort.StrVal {
				return servicePorts, nil
			}
		}
	}

	return nil, fmt.Errorf("no service ports matching backendPort '%s'", backendPort.String())
}

// servicePortsFromTemplate gets service port from pod template.
func servicePortsFromContainers(containers []v1.Container) []v1.ServicePort {
	ports := make([]v1.ServicePort, 0)
	for _, container := range containers {
		for i, port := range container.Ports {
			name := fmt.Sprintf("port-%d", i)
			if port.Name != "" {
				name = port.Name
			}
			servicePort := v1.ServicePort{
				Name:       name,
				Protocol:   port.Protocol,
				Port:       port.ContainerPort,
				TargetPort: intstr.FromInt(int(port.ContainerPort)),
			}
			// set default protocol if not specified
			if servicePort.Protocol == "" {
				servicePort.Protocol = v1.ProtocolTCP
			}
			ports = append(ports, servicePort)
		}
	}
	return ports
}

// templateInjectLabels injects labels into a pod template spec.
func templateInjectLabels(template v1.PodTemplateSpec, labels map[string]string) v1.PodTemplateSpec {
	if template.ObjectMeta.Labels == nil {
		template.ObjectMeta.Labels = map[string]string{}
	}

	for key, value := range labels {
		if _, ok := template.ObjectMeta.Labels[key]; !ok {
			template.ObjectMeta.Labels[key] = value
		}
	}
	return template
}

// applyPodTemplateSpecDefaults inject default values into a pod template spec.
func applyPodTemplateSpecDefaults(template v1.PodTemplateSpec) v1.PodTemplateSpec {
	newTemplate := template.DeepCopy()
	for i, container := range newTemplate.Spec.Containers {
		for j, port := range container.Ports {
			if port.Protocol == "" {
				newTemplate.Spec.Containers[i].Ports[j].Protocol = v1.ProtocolTCP
			}
		}
		if container.TerminationMessagePath == "" {
			newTemplate.Spec.Containers[i].TerminationMessagePath = v1.TerminationMessagePathDefault
		}
		if container.TerminationMessagePolicy == "" {
			newTemplate.Spec.Containers[i].TerminationMessagePolicy = v1.TerminationMessageReadFile
		}
		if container.ImagePullPolicy == "" {
			newTemplate.Spec.Containers[i].ImagePullPolicy = v1.PullIfNotPresent
		}
	}
	if newTemplate.Spec.RestartPolicy == "" {
		newTemplate.Spec.RestartPolicy = v1.RestartPolicyAlways
	}
	if newTemplate.Spec.TerminationGracePeriodSeconds == nil {
		gracePeriod := int64(v1.DefaultTerminationGracePeriodSeconds)
		newTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod
	}
	if newTemplate.Spec.DNSPolicy == "" {
		newTemplate.Spec.DNSPolicy = v1.DNSClusterFirst
	}
	if newTemplate.Spec.SecurityContext == nil {
		newTemplate.Spec.SecurityContext = &v1.PodSecurityContext{}
	}
	if newTemplate.Spec.SchedulerName == "" {
		newTemplate.Spec.SchedulerName = v1.DefaultSchedulerName
	}
	return *newTemplate
}
