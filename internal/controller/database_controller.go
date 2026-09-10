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

	"crypto/rand"
	"encoding/base64"
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

// DatabaseReconciler reconciles a Database object
type DatabaseReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Function for generate Secret using crypto/rand
func generatePassword() (string, error) {
	bytes := make([]byte, 24)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

// Function for prepare secret before insert in to DB CRD
func (r *DatabaseReconciler) reconcileSecret(ctx context.Context, db *paasv1.Database) error {
	secretName := fmt.Sprintf("%s-credentials", db.Name)
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: db.Namespace}, secret)

	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	password, genErr := generatePassword()
	if genErr != nil {
		return genErr
	}

	host := fmt.Sprintf("%s.%s.svc.cluster.local", db.Name, db.Namespace)

	secretMap := map[string]string{}
	if db.Spec.Engine == "postgres" {
		dbName := db.Name
		if db.Spec.Name != nil {
			dbName = *db.Spec.Name
		}
		secretMap["POSTGRES_USER"] = "postgres"
		secretMap["POSTGRES_PASSWORD"] = password
		secretMap["POSTGRES_DB"] = dbName
		secretMap["DATABASE_URL"] = fmt.Sprintf("postgresql://postgres:%s@%s:5432/%s", password, host, dbName)
	} else if db.Spec.Engine == "redis" {
		secretMap["REDIS_PASSWORD"] = password
		secretMap["REDIS_URL"] = fmt.Sprintf("redis://:%s@%s:6379", password, host)
	}

	newSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: db.Namespace,
		},
		StringData: secretMap,
	}
	err = r.Create(ctx, newSecret)
	if err != nil {
		return err
	}
	return nil
}

func (r *DatabaseReconciler) reconcileStatefulSet(ctx context.Context, db *paasv1.Database) error {
	labels := map[string]string{
		"db": db.Name,
	}

	secretName := fmt.Sprintf("%s-credentials", db.Name)

	// My solution
	// containerPorts := map[string]int32{}
	// image := map[string]string{}
	// volumeMount := map[string]string{}

	// if db.Spec.Engine == "postgres"{
	// 	image["image"] = "postgres:15-alpine"
	// 	containerPorts["containerPort"] = 5432
	// 	volumeMount["name"] = "pgdata"
	// 	volumeMount["mountPath"] = "/var/lib/postgresql/data"
	// }else if db.Spec.Engine == "redis"{
	// 	image["redis"] = "redis:7-alpine"
	// 	containerPorts["containerPort"] = 6379
	// 	volumeMount["name"] = "data"
	// 	volumeMount["mountPath"] = "/data"
	// }

	// Fixed Solution
	var image string
	var containerPort int32
	var volumeName string
	var mountPath string

	if db.Spec.Engine == "postgres" {
		image = "postgres:15-alpine"
		containerPort = 5432
		volumeName = "pgdata"
		mountPath = "/var/lib/postgresql/data"
	} else if db.Spec.Engine == "redis" {
		image = "redis:7-alpine"
		containerPort = 6379
		volumeName = "data"
		mountPath = "/data"
	}

	statefulset := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(db, paasv1.GroupVersion.WithKind("Database")),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: db.Name,
			Replicas:    ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  db.Spec.Engine,
							Image: image,
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: containerPort,
								},
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: secretName,
										},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: volumeName, MountPath: mountPath},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: volumeName,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: db.Spec.Storage,
							},
						},
					},
				},
			},
		},
	}

	var found appsv1.StatefulSet
	founded := r.Get(ctx, client.ObjectKey{Name: statefulset.Name, Namespace: statefulset.Namespace}, &found)
	if apierrors.IsNotFound(founded) {
		err := r.Create(ctx, statefulset)
		if err != nil {
			return err
		}
		return nil
	}
	if founded != nil {
		return founded
	}
	if founded == nil {
		statefulset.ResourceVersion = found.ResourceVersion
		if err := r.Update(ctx, statefulset); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (r *DatabaseReconciler) reconcileService(ctx context.Context, db *paasv1.Database) error {
	labels := map[string]string{
		"db": db.Name,
	}
	var containerPort int32
	if db.Spec.Engine == "postgres" {
		containerPort = 5432
	} else if db.Spec.Engine == "redis" {
		containerPort = 6379
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      db.Name,
			Namespace: db.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(db, paasv1.GroupVersion.WithKind("Database")),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{Port: containerPort},
			},
		},
	}
	var found corev1.Service
	founded := r.Get(ctx, client.ObjectKey{Name: service.Name, Namespace: service.Namespace}, &found)
	if apierrors.IsNotFound(founded) {
		err := r.Create(ctx, service)
		if err != nil {
			return err
		}
		return nil
	}
	if founded != nil {
		return founded
	}
	if founded == nil {
		service.ResourceVersion = found.ResourceVersion
		if err := r.Update(ctx, service); err != nil {
			return err
		}
		return nil
	}
	return nil
}

// +kubebuilder:rbac:groups=paas.internal,resources=databases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=paas.internal,resources=databases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=paas.internal,resources=databases/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Database object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *DatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	var database paasv1.Database
	err := r.Get(ctx, req.NamespacedName, &database)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, err
	}
	err = r.reconcileSecret(ctx, &database)
	if err != nil {
		return ctrl.Result{}, err
	}
	err = r.reconcileStatefulSet(ctx, &database)
	if err != nil {
		return ctrl.Result{}, err
	}
	err = r.reconcileService(ctx, &database)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&paasv1.Database{}).
		Named("database").
		Complete(r)
}
