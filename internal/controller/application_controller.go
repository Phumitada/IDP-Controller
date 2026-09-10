/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	paasv1 "github.com/Phumitada/IDP-Controller/api/v1"
)

// ApplicationReconciler reconciles a Application object
type ApplicationReconciler struct {
	client.Client // Doesnt have name only a type is declared
	Scheme        *runtime.Scheme
}

// type Client interface {
//     Get(ctx, key, obj) error
//     Create(ctx, obj) error
//     Update(ctx, obj) error
//     Delete(ctx, obj) error
//     List(ctx, list) error
// } List of client interface method, to HTTP requeset to apiserver

// +kubebuilder:rbac:groups=paas.internal,resources=applications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=paas.internal,resources=applications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=paas.internal,resources=applications/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Application object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *ApplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	var app paasv1.Application
	err := r.Get(ctx, req.NamespacedName, &app)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, err
	}

	labels := map[string]string{
		"app": app.Name,
	}

	var envFrom []corev1.EnvFromSource

	if len(app.Spec.DatabaseRef) != 0 {
		// Check for what Database is referenced
		var secretRef []string
		// secretRef := fmt.Sprintf("%s-credentials", *app.Spec.DatabaseRef)
		for _, value := range app.Spec.DatabaseRef {
			valueTransformer := fmt.Sprintf("%s-credentials", value)
			secretRef = append(secretRef, valueTransformer)
		}
		secretObj := &corev1.Secret{}
		for _, value := range secretRef {
			err := r.Get(ctx, types.NamespacedName{Name: value, Namespace: app.Namespace}, secretObj)
			if err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			if err == nil {
				envFromSource := &corev1.EnvFromSource{
					SecretRef: &corev1.SecretEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: value,
						},
					},
				}
				envFrom = append(envFrom, *envFromSource)
			}
		}
	}

	envVars := []corev1.EnvVar{}
	for key, value := range app.Spec.EnvVars {
		envVars = append(envVars, corev1.EnvVar{Name: key, Value: value})
	}

	secretName := "ghcr-secret"
	if app.Spec.ImagePullSecret != nil {
		secretName = *app.Spec.ImagePullSecret
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      app.Name,
			Namespace: app.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(&app, paasv1.GroupVersion.WithKind("Application")),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Replicas: ptr.To(int32(1)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "api",
							Image: app.Spec.Image,
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: app.Spec.Port,
								},
							},
							Env:     envVars,
							EnvFrom: envFrom,
						},
					},
					ImagePullSecrets: []corev1.LocalObjectReference{
						{
							Name: secretName,
						},
					},
				},
			},
		},
	}

	var found appsv1.Deployment
	founded := r.Get(ctx, client.ObjectKey{Name: deployment.Name, Namespace: deployment.Namespace}, &found)
	if apierrors.IsNotFound(founded) {
		err := r.Create(ctx, deployment)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	if founded != nil {
		return ctrl.Result{}, founded
	}
	if founded == nil {
		deployment.ResourceVersion = found.ResourceVersion
		if err := r.Update(ctx, deployment); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ApplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&paasv1.Application{}).
		Named("application").
		Complete(r)
}
