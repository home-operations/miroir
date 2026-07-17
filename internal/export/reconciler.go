/*
Copyright 2026.

Licensed under the GNU Affero General Public License, Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.gnu.org/licenses/agpl-3.0.html

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package export reconciles the NFS gateway for RWX (ReadWriteMany)
// volumes. When a MiroirVolume has spec.export set, this controller-pod
// reconciler maintains a per-volume Deployment (the NFS-Ganesha share
// manager, pinned to the volume's diskful replica nodes) and a ClusterIP
// Service fronting it, and publishes the Service's address on
// status.export.address for the CSI node service to mount. Both workloads
// are owned by the MiroirVolume, so deleting the volume garbage-collects
// them.
package export

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// Reconciler maintains the NFS gateway workloads for RWX volumes. It runs
// in the controller pod: creating Deployments/Services and reading the
// storage topology are controller-scoped concerns.
type Reconciler struct {
	client.Client
	// Namespace is where the gateway Deployments and Services live (the
	// release namespace, the controller's own).
	Namespace string
	// Image is the gateway container image (agent userland + NFS-Ganesha).
	Image string
	// ServiceAccount is the gateway pods' ServiceAccount, chart-created
	// with the RBAC the gateway needs (miroirvolumes get/list/watch +
	// status patch).
	ServiceAccount string
}

// Reconcile ensures the gateway Deployment and Service exist for an RWX
// volume and records the Service address. Owned workloads are garbage
// collected when the volume is deleted, so a missing volume is a no-op.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	vol := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		if apierrors.IsNotFound(err) {
			dropExportMetrics(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	// Only RWX volumes have a gateway; a volume being deleted keeps its
	// workloads until GC removes them with the owner.
	if vol.Spec.Export == nil || !vol.DeletionTimestamp.IsZero() {
		dropExportMetrics(vol.Name)
		return ctrl.Result{}, nil
	}

	available, err := r.ensureDeployment(ctx, vol)
	if err != nil {
		return ctrl.Result{}, err
	}
	addr, err := r.ensureService(ctx, vol)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.publishAddress(ctx, vol, addr); err != nil {
		return ctrl.Result{}, err
	}
	// Serving means a gateway pod is up and the address consumers mount is
	// published; the owned-Deployment watch re-runs this on availability
	// flips, so the gauge tracks failovers without polling.
	recordExportReady(vol, available && addr != "")
	log.Info("reconciled RWX gateway", "volume", vol.Name, "address", addr, "available", available)
	return ctrl.Result{}, nil
}

// ensureDeployment creates or updates the gateway Deployment, refreshing
// its node affinity when the volume's replica set changes. It reports
// whether a gateway pod is currently available (the export-ready signal).
func (r *Reconciler) ensureDeployment(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (bool, error) {
	want := buildDeployment(vol, r.Namespace, r.Image, r.ServiceAccount)
	dep := &appsv1.Deployment{}
	dep.Name = want.Name
	dep.Namespace = want.Namespace
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = want.Labels
		// Overwriting the whole Spec drops apiserver-defaulted fields, so
		// CreateOrUpdate's equality check never holds and every reconcile
		// sends an Update the apiserver re-defaults into a no-op (no RV
		// bump, no event storm — one redundant write per reconcile).
		// Accepted: the alternative is hand-maintaining the default set.
		dep.Spec = want.Spec
		return controllerutil.SetControllerReference(vol, dep, r.Scheme())
	})
	if err != nil {
		return false, err
	}
	return dep.Status.AvailableReplicas > 0, nil
}

// ensureService creates or adopts the gateway Service and returns its
// ClusterIP. It never overwrites the Service spec's ClusterIP — that is
// apiserver-assigned and consumers mount it, so it must stay stable across
// gateway pod restarts.
func (r *Reconciler) ensureService(ctx context.Context, vol *miroirv1alpha1.MiroirVolume) (string, error) {
	want := buildService(vol, r.Namespace)
	svc := &corev1.Service{}
	svc.Name = want.Name
	svc.Namespace = want.Namespace
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = want.Labels
		// Set only the fields we own; ClusterIP/ClusterIPs stay whatever
		// the apiserver assigned so an adopted Service keeps its address.
		svc.Spec.Type = want.Spec.Type
		svc.Spec.Selector = want.Spec.Selector
		svc.Spec.Ports = want.Spec.Ports
		return controllerutil.SetControllerReference(vol, svc, r.Scheme())
	})
	if err != nil {
		return "", err
	}
	return svc.Spec.ClusterIP, nil
}

// publishAddress records the Service ClusterIP on status so the node
// service can mount the export. It is a status patch (no generation bump),
// so it does not re-trigger this generation-filtered reconciler.
func (r *Reconciler) publishAddress(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, addr string) error {
	if addr == "" {
		// The apiserver has not assigned a ClusterIP yet; the owned-Service
		// watch requeues us when it does.
		return nil
	}
	want := &miroirv1alpha1.ExportStatus{Address: addr}
	if equality.Semantic.DeepEqual(vol.Status.Export, want) {
		return nil
	}
	base := vol.DeepCopy()
	vol.Status.Export = want
	return r.Status().Patch(ctx, vol, client.MergeFrom(base))
}

// SetupWithManager registers the reconciler. The volume watch passes
// generation changes (status churn from agents carries nothing this
// reconciler reads) and label changes — the PVC-ref backfill is a
// label-only patch, and without it an existing RWX volume's
// miroir_export_ready series would keep its fallback pvc label until an
// unrelated workload event. It owns its Deployment/Service so drift on
// those heals.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirVolume{},
			builder.WithPredicates(predicate.Or[client.Object](
				predicate.GenerationChangedPredicate{},
				predicate.LabelChangedPredicate{}))).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Named("export").
		Complete(r)
}
