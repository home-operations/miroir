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

package export

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
)

const (
	// sharePrefix names the per-volume Deployment and Service. Volume
	// names are pvc-<uuid> (40 chars); the prefix keeps the Service name
	// (a 63-char DNS label) within bounds.
	sharePrefix = "miroir-share-"
	// gatewayAppName is the app.kubernetes.io/name on gateway pods.
	gatewayAppName = "miroir-gateway"
	// nfsPort is the NFSv4 port the gateway serves and the Service fronts.
	nfsPort = 2049
	// httpPort is the gateway's operational endpoint (/healthz liveness,
	// /metrics for the gateway PodMonitor) — the org-standard pod port.
	httpPort = 8081
)

// shareName is the Deployment/Service name for a volume's gateway.
func shareName(volume string) string {
	return sharePrefix + volume
}

// shareLabels identify a volume's gateway pods and select them from the
// Service. app is constant; volume disambiguates one gateway from another.
func shareLabels(volume string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":            gatewayAppName,
		"app.kubernetes.io/component":       "gateway",
		"miroir.home-operations.com/volume": volume,
	}
}

// diskfulNodes lists the nodes holding a data replica — the only nodes a
// gateway may schedule on (a diskless tie-breaker has no device to mount).
func diskfulNodes(vol *miroirv1alpha1.MiroirVolume) []string {
	reps := vol.Spec.DiskfulReplicas()
	nodes := make([]string, 0, len(reps))
	for _, r := range reps {
		nodes = append(nodes, r.Node)
	}
	return nodes
}

// buildDeployment renders the desired gateway Deployment for a volume.
// replicas:1 with the Recreate strategy: two gateways writing the same
// device is exactly the dual-primary case single-primary DRBD forbids, so
// the old pod must be gone before the new one promotes.
func buildDeployment(vol *miroirv1alpha1.MiroirVolume, namespace, image, serviceAccount string) *appsv1.Deployment {
	labels := shareLabels(vol.Name)
	privileged := true
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: shareName(vol.Name), Namespace: namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas:             ptr.To[int32](1),
			Strategy:             appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			RevisionHistoryLimit: ptr.To[int32](1),
			Selector:             &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccount,
					Affinity:           nodeAffinity(diskfulNodes(vol)),
					// Evict 30s after the node goes unreachable, not the 5m
					// default: the gateway is a singleton, so failover cannot
					// start until this pod is gone from the dead node.
					Tolerations: []corev1.Toleration{
						{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To[int64](30)},
						{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To[int64](30)},
					},
					Containers: []corev1.Container{{
						Name:  "gateway",
						Image: image,
						Args:  []string{"--mode=gateway", "--volume=" + vol.Name},
						Env: []corev1.EnvVar{{
							Name:      "NODE_NAME",
							ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}},
						}, {
							Name:      "POD_NAMESPACE",
							ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
						}},
						SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
						Ports: []corev1.ContainerPort{
							{Name: "nfs", ContainerPort: nfsPort, Protocol: corev1.ProtocolTCP},
							{Name: "metrics", ContainerPort: httpPort, Protocol: corev1.ProtocolTCP},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "dev", MountPath: "/dev",
						}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("10m"),
								corev1.ResourceMemory: resource.MustParse("64Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(nfsPort)},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       10,
						},
						// /healthz answers an NFS NULL RPC against ganesha, so a
						// wedged-but-listening server (green on the TCP readiness
						// probe above) gets restarted. It passes unconditionally
						// while the gateway is still staging — a failover wait must
						// not be liveness-killed — so no InitialDelay is needed.
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(httpPort)},
							},
							PeriodSeconds:    20,
							TimeoutSeconds:   10,
							FailureThreshold: 3,
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "dev",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{Path: "/dev", Type: ptr.To(corev1.HostPathDirectory)},
						},
					}},
				},
			},
		},
	}
}

// buildService renders the desired ClusterIP Service fronting a volume's
// gateway pod. The ClusterIP is allocated by the apiserver and must stay
// stable across gateway pod restarts (consumers mount it), so the
// reconciler adopts an existing Service rather than recreating it.
func buildService(vol *miroirv1alpha1.MiroirVolume, namespace string) *corev1.Service {
	labels := shareLabels(vol.Name)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: shareName(vol.Name), Namespace: namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "nfs",
				Port:       nfsPort,
				TargetPort: intstr.FromInt32(nfsPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

// nodeAffinity pins scheduling to the volume's diskful replica nodes.
func nodeAffinity(nodes []string) *corev1.Affinity {
	if len(nodes) == 0 {
		return nil
	}
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      constants.TopologyKey,
						Operator: corev1.NodeSelectorOpIn,
						Values:   nodes,
					}},
				}},
			},
		},
	}
}
