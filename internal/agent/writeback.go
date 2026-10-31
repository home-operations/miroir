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

package agent

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	defaultRestageSweepInterval = 30 * time.Second

	reasonWritebackError = "WritebackError"
	reasonRestaged       = "Restaged"
	reasonRemounted      = "Remounted"
	reasonUnmounted      = "Unmounted"
	reasonRebooted       = "Rebooted"
)

var metricNeedsRestage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "miroir_volume_needs_restage",
	Help: "1 while the kernel has lost a write under a filesystem still mounted on this node; ext4 aborts its journal on the next metadata change. Restart the pod mounting it (the NFS gateway pod for RWX) to restage. Absent otherwise.",
}, volumeOnlyLabels)

func init() {
	metrics.Registry.MustRegister(metricNeedsRestage)
}

type fsEvent int

const (
	fsEventNone fsEvent = iota
	// fsEventLostWrite: a buffer write to the device failed. The block
	// layer records it in the device's writeback errseq, which jbd2
	// compares against the value it sampled when the journal was created.
	fsEventLostWrite
	// fsEventMounted and fsEventUnmounted bracket an ext4 superblock's
	// life: a new one samples the errseq afresh, a destroyed one takes its
	// journal with it. A mount sharing an existing superblock logs neither.
	fsEventMounted
	fsEventUnmounted
)

// parseFSEvent classifies a kernel record about a DRBD device's
// filesystem. Formats: fs/buffer.c buffer_io_error ("Buffer I/O error on
// dev %pg, logical block %llu, lost {async,sync} page write") and
// fs/ext4/super.c ext4_msg ("EXT4-fs (%s): mounted filesystem ..." /
// "EXT4-fs (%s): unmounting filesystem ...").
func parseFSEvent(line string) (int32, fsEvent) {
	msg, ok := kernelPayload(line)
	if !ok {
		return 0, fsEventNone
	}
	if rest, ok := strings.CutPrefix(msg, "Buffer I/O error on dev drbd"); ok {
		if !strings.HasSuffix(rest, ", lost async page write") && !strings.HasSuffix(rest, ", lost sync page write") {
			return 0, fsEventNone
		}
		dev, _, _ := strings.Cut(rest, ",")
		return parseMinor(dev, fsEventLostWrite)
	}
	if rest, ok := strings.CutPrefix(msg, "EXT4-fs (drbd"); ok {
		dev, what, ok := strings.Cut(rest, "): ")
		if !ok {
			return 0, fsEventNone
		}
		switch {
		case strings.HasPrefix(what, "mounted filesystem"):
			return parseMinor(dev, fsEventMounted)
		case strings.HasPrefix(what, "unmounting filesystem"):
			return parseMinor(dev, fsEventUnmounted)
		}
	}
	return 0, fsEventNone
}

func parseMinor(s string, ev fsEvent) (int32, fsEvent) {
	minor, err := strconv.ParseInt(s, 10, 32)
	if err != nil || minor < 0 {
		return 0, fsEventNone
	}
	return int32(minor), ev
}

// WritebackWatcher maintains this node's ReplicaStatus.WritebackError
// from the kernel log (issue #594). A quorum loss under policy freeze can
// fail only async writeback: ext4 keeps the mount rw and the error stays
// latent until jbd2 trips on it at the next metadata change, possibly
// weeks later.
type WritebackWatcher struct {
	Client client.Client
	// Reader backs the read-modify-write of each status update, so the
	// optimistic lock does not spin on a lagging cache; nil uses Client.
	Reader   client.Reader
	NodeName string
	// Recorder emits the FilesystemNeedsRestage warning; optional.
	Recorder events.EventRecorder

	// Path, ProcDir, Interval and SweepInterval override /dev/kmsg, /proc,
	// the kmsg poll and the sweep period (tests).
	Path          string
	ProcDir       string
	Interval      time.Duration
	SweepInterval time.Duration

	// pending holds each minor's last unapplied event; an API failure
	// keeps it for the next poll.
	pending map[int32]pendingEvent
	// flagged mirrors the volumes whose slot carries a WritebackError, ahead
	// of the informer cache after this watcher's own writes.
	flagged   map[string]bool
	lastSweep time.Time
}

type pendingEvent struct {
	ev     fsEvent
	record string
}

func (w *WritebackWatcher) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("writeback-errors")
	tailKmsg(ctx, log, w.Path, w.Interval, func(lines []string, _ bool) {
		w.handle(ctx, log, lines)
	})
	return nil
}

// handle needs no replay distinction: a boot-history lost write still
// poisons a mount that is live now, and any later mount or unmount of the
// device in the same history cancels it, since the ring keeps records in
// order.
func (w *WritebackWatcher) handle(ctx context.Context, log logr.Logger, lines []string) {
	if w.pending == nil {
		w.pending = map[int32]pendingEvent{}
		w.flagged = map[string]bool{}
	}
	for _, line := range lines {
		if minor, ev := parseFSEvent(line); ev != fsEventNone {
			w.pending[minor] = pendingEvent{ev: ev, record: kmsgPayload(line)}
		}
	}
	if len(w.pending) > 0 {
		w.applyPending(ctx, log)
	}
	sweep := w.SweepInterval
	if sweep <= 0 {
		sweep = defaultRestageSweepInterval
	}
	if time.Since(w.lastSweep) >= sweep {
		w.sweep(ctx, log)
	}
}

func (w *WritebackWatcher) applyPending(ctx context.Context, log logr.Logger) {
	vols := &miroirv1alpha1.MiroirVolumeList{}
	if err := w.Client.List(ctx, vols); err != nil {
		log.Error(err, "listing volumes for kernel writeback errors")
		return
	}
	for minor, p := range w.pending {
		done, err := w.apply(ctx, log, vols.Items, minor, p)
		if err != nil {
			log.Error(err, "recording a kernel writeback error", "minor", minor)
		}
		if done {
			delete(w.pending, minor)
		}
	}
}

func (w *WritebackWatcher) apply(ctx context.Context, log logr.Logger, vols []miroirv1alpha1.MiroirVolume, minor int32, p pendingEvent) (bool, error) {
	vol := volumeForMinor(vols, w.NodeName, minor)
	if vol == nil {
		return true, nil
	}
	if p.ev != fsEventLostWrite {
		// No cached precheck: the cache can lag a raise this watcher just
		// wrote, and clear rereads uncached anyway.
		reason := reasonRemounted
		if p.ev == fsEventUnmounted {
			reason = reasonUnmounted
		}
		if err := w.clear(ctx, log, vol.Name, reason); err != nil {
			return false, err
		}
		return true, nil
	}
	if w.flagged[vol.Name] || vol.Status.PerNode[w.NodeName].WritebackError != nil {
		return true, nil
	}
	mounted, err := deviceMountedAnywhere(w.procDir(), minor)
	if err != nil {
		return false, fmt.Errorf("writeback: mount check for drbd%d: %w", minor, err)
	}
	if !mounted {
		log.Info("kernel lost a write to an unmounted DRBD device; no filesystem to restage",
			"volume", vol.Name, "record", p.record)
		return true, nil
	}
	changed, err := w.updateSlot(ctx, vol.Name, func(st *miroirv1alpha1.ReplicaStatus) bool {
		if st.WritebackError != nil {
			return false
		}
		st.WritebackError = &miroirv1alpha1.WritebackError{Since: metav1.Now(), Record: p.record}
		return true
	})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	w.flag(vol)
	if changed {
		msg := fmt.Sprintf("kernel lost a write to /dev/drbd%d under the filesystem mounted on %s (%s); %s",
			minor, w.NodeName, p.record, restageAdvice(vol))
		log.Info("kernel lost a write under a mounted filesystem; volume needs restage",
			"volume", vol.Name, "record", p.record)
		if w.Recorder != nil {
			w.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, miroirv1alpha1.ConditionFilesystemNeedsRestage,
				reasonWritebackError, "%s", msg)
		}
	}
	return true, nil
}

// restageAdvice names what to restart: an RWX volume's filesystem is
// mounted by its NFS gateway, not by the pods consuming the export.
func restageAdvice(vol *miroirv1alpha1.MiroirVolume) string {
	if vol.Spec.Export != nil {
		return "delete the volume's NFS gateway pod to restage it"
	}
	return "restart the consuming pod to restage the volume"
}

// sweep clears entries whose mount is gone without a kernel line apply saw
// (the ext4 messages are rate limited; other filesystems log nothing
// parseFSEvent knows), and publishes the gauge for the rest.
func (w *WritebackWatcher) sweep(ctx context.Context, log logr.Logger) {
	w.lastSweep = time.Now()
	vols := &miroirv1alpha1.MiroirVolumeList{}
	if err := w.Client.List(ctx, vols); err != nil {
		log.Error(err, "listing volumes for the restage sweep")
		return
	}
	booted, err := bootTime(w.procDir())
	if err != nil {
		log.Error(err, "reading boot time; reboot clears are skipped this sweep")
	}
	unseen := maps.Clone(w.flagged)
	for i := range vols.Items {
		vol := &vols.Items[i]
		// A slot dropped with its error (replica removal) leaves the
		// condition behind; any agent may rederive it.
		if restageConditionStale(vol) {
			if _, err := w.updateVolume(ctx, vol.Name, setRestageCondition); err != nil {
				log.Error(err, "rederiving the restage condition", "volume", vol.Name)
			}
		}
		st, ok := vol.Status.PerNode[w.NodeName]
		if !ok || st.WritebackError == nil {
			continue
		}
		delete(unseen, vol.Name)
		if reason := w.mountGone(st, booted); reason != "" {
			if err := w.clear(ctx, log, vol.Name, reason); err != nil {
				log.Error(err, "clearing a writeback error", "volume", vol.Name)
			}
			continue
		}
		w.flag(vol)
	}
	// A flag the cache does not show yet may be this watcher's own fresh
	// raise: only an uncached read can retire it.
	for name := range unseen {
		vol := &miroirv1alpha1.MiroirVolume{}
		err := w.reader().Get(ctx, types.NamespacedName{Name: name}, vol)
		switch {
		case apierrors.IsNotFound(err):
			w.unflag(name)
		case err != nil:
			log.Error(err, "rereading a flagged volume", "volume", name)
		case vol.Status.PerNode[w.NodeName].WritebackError == nil:
			w.unflag(name)
		}
	}
}

// mountGone returns the clear reason when the mount the entry describes
// can no longer exist, or "" while it may.
func (w *WritebackWatcher) mountGone(st miroirv1alpha1.ReplicaStatus, booted time.Time) string {
	if !booted.IsZero() && st.WritebackError.Since.Time.Before(booted) {
		return reasonRebooted
	}
	if st.DRBDMinor <= 0 {
		return reasonUnmounted
	}
	// A failed check keeps the entry: "could not check" must never read as
	// "not mounted".
	if mounted, err := deviceMountedAnywhere(w.procDir(), st.DRBDMinor); err == nil && !mounted {
		return reasonUnmounted
	}
	return ""
}

func (w *WritebackWatcher) flag(vol *miroirv1alpha1.MiroirVolume) {
	w.flagged[vol.Name] = true
	metricNeedsRestage.With(syncRef(vol).labels()).Set(1)
}

func (w *WritebackWatcher) unflag(name string) {
	delete(w.flagged, name)
	metricNeedsRestage.DeletePartialMatch(prometheus.Labels{volumeLabel: name})
}

func (w *WritebackWatcher) procDir() string {
	if w.ProcDir != "" {
		return w.ProcDir
	}
	return "/proc"
}

func (w *WritebackWatcher) reader() client.Reader {
	if w.Reader != nil {
		return w.Reader
	}
	return w.Client
}

// clear drops this node's entry; a deleted volume has nothing to clear.
func (w *WritebackWatcher) clear(ctx context.Context, log logr.Logger, name, reason string) error {
	changed, err := w.updateSlot(ctx, name, func(st *miroirv1alpha1.ReplicaStatus) bool {
		if st.WritebackError == nil {
			return false
		}
		st.WritebackError = nil
		return true
	})
	if err != nil {
		return client.IgnoreNotFound(err)
	}
	w.unflag(name)
	if changed {
		log.Info("filesystem mount replaced or gone; cleared the writeback error", "volume", name, "reason", reason)
	}
	return nil
}

// updateSlot mutates this node's status slot and rederives the volume
// condition from every slot. The reconciler's apply leaves WritebackError
// alone, as it never sends it.
func (w *WritebackWatcher) updateSlot(ctx context.Context, name string, mutate func(*miroirv1alpha1.ReplicaStatus) bool) (bool, error) {
	return w.updateVolume(ctx, name, func(vol *miroirv1alpha1.MiroirVolume) bool {
		st, ok := vol.Status.PerNode[w.NodeName]
		if !ok || !mutate(&st) {
			return false
		}
		vol.Status.PerNode[w.NodeName] = st
		setRestageCondition(vol)
		return true
	})
}

// updateVolume rereads the volume uncached and merge-patches its status
// under an optimistic lock: the patch carries the whole conditions list,
// so an unlocked write would drop a condition or another node's entry
// written since the read.
func (w *WritebackWatcher) updateVolume(ctx context.Context, name string, mutate func(*miroirv1alpha1.MiroirVolume) bool) (bool, error) {
	var changed bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vol := &miroirv1alpha1.MiroirVolume{}
		if err := w.reader().Get(ctx, types.NamespacedName{Name: name}, vol); err != nil {
			return err
		}
		base := vol.DeepCopy()
		changed = mutate(vol)
		if !changed {
			return nil
		}
		return w.Client.Status().Patch(ctx, vol, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		return false, fmt.Errorf("writeback: update %s status: %w", name, err)
	}
	return changed, nil
}

// restageConditionStale reports whether the condition disagrees with the
// slots, without touching vol.
func restageConditionStale(vol *miroirv1alpha1.MiroirVolume) bool {
	probe := *vol
	probe.Status.Conditions = slices.Clone(vol.Status.Conditions)
	return setRestageCondition(&probe)
}

// setRestageCondition derives ConditionFilesystemNeedsRestage from the
// nodes reporting a WritebackError; true when it changed.
func setRestageCondition(vol *miroirv1alpha1.MiroirVolume) bool {
	var nodes []string
	for node, st := range vol.Status.PerNode {
		if st.WritebackError != nil {
			nodes = append(nodes, node)
		}
	}
	if len(nodes) == 0 {
		if !meta.IsStatusConditionTrue(vol.Status.Conditions, miroirv1alpha1.ConditionFilesystemNeedsRestage) {
			return false
		}
		return meta.SetStatusCondition(&vol.Status.Conditions, metav1.Condition{
			Type:   miroirv1alpha1.ConditionFilesystemNeedsRestage,
			Status: metav1.ConditionFalse,
			Reason: reasonRestaged,
		})
	}
	slices.Sort(nodes)
	return meta.SetStatusCondition(&vol.Status.Conditions, metav1.Condition{
		Type:   miroirv1alpha1.ConditionFilesystemNeedsRestage,
		Status: metav1.ConditionTrue,
		Reason: reasonWritebackError,
		Message: fmt.Sprintf("kernel lost a write under the filesystem mounted on %s (status.perNode.<node>.writebackError); %s",
			strings.Join(nodes, ", "), restageAdvice(vol)),
	})
}

func volumeForMinor(vols []miroirv1alpha1.MiroirVolume, node string, minor int32) *miroirv1alpha1.MiroirVolume {
	for i := range vols {
		if st, ok := vols[i].Status.PerNode[node]; ok && st.DRBDMinor == minor {
			return &vols[i]
		}
	}
	return nil
}

// bootTime reads the host boot time from /proc/stat's btime line.
func bootTime(procDir string) (time.Time, error) {
	data, err := os.ReadFile(filepath.Join(procDir, "stat"))
	if err != nil {
		return time.Time{}, err
	}
	for line := range strings.Lines(string(data)) {
		if v, ok := strings.CutPrefix(line, "btime "); ok {
			secs, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				return time.Time{}, fmt.Errorf("writeback: parse btime: %w", err)
			}
			return time.Unix(secs, 0), nil
		}
	}
	return time.Time{}, fmt.Errorf("writeback: no btime in %s/stat", procDir)
}
