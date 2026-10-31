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
	reasonRemounted      = "Remounted"
	reasonUnmounted      = "Unmounted"
	reasonRebooted       = "Rebooted"
)

var metricNeedsRestage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "miroir_volume_needs_restage",
	Help: "1 while the kernel has lost a write under a filesystem still mounted on this node; ext4 aborts its journal on the next metadata change. Restart the consuming pod to restage. Absent otherwise.",
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

// WritebackWatcher maintains ConditionFilesystemNeedsRestage from the
// kernel log (issue #594). A quorum loss under policy freeze can fail only
// async writeback: ext4 keeps the mount rw and the error stays latent
// until jbd2 trips on it at the next metadata change, possibly weeks later.
type WritebackWatcher struct {
	Client client.Client
	// Reader backs the read-modify-write of each condition update, so the
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
	pending   map[int32]pendingEvent
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
	// w.flagged covers the informer lagging a condition this watcher just
	// raised; updateCondition rereads uncached before writing either way.
	raised := w.flagged[vol.Name] || needsRestageOn(vol.Status.Conditions, w.NodeName)
	if p.ev != fsEventLostWrite {
		if !raised {
			return true, nil
		}
		reason := reasonRemounted
		if p.ev == fsEventUnmounted {
			reason = reasonUnmounted
		}
		if err := w.clear(ctx, vol.Name, reason); err != nil {
			return false, err
		}
		w.unflag(vol.Name)
		log.Info("filesystem journal reset; cleared the restage condition", "volume", vol.Name, "reason", reason)
		return true, nil
	}
	if raised {
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
	msg := restagePrefix(w.NodeName) + fmt.Sprintf("the kernel lost a write to /dev/drbd%d under the mounted filesystem (%s). "+
		"ext4 aborts its journal and goes read-only on its next metadata change; restart the consuming pod to restage the volume.",
		minor, p.record)
	changed, err := w.mark(ctx, vol.Name, msg)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	w.flag(vol)
	if changed {
		log.Info("kernel lost a write under a mounted filesystem; volume needs restage",
			"volume", vol.Name, "record", p.record)
		if w.Recorder != nil {
			w.Recorder.Eventf(vol, nil, corev1.EventTypeWarning, miroirv1alpha1.ConditionFilesystemNeedsRestage,
				reasonWritebackError, "%s", msg)
		}
	}
	return true, nil
}

// sweep clears conditions whose mount is gone without a kernel line apply
// saw (the ext4 messages are rate limited; other filesystems log nothing
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
	stale := maps.Clone(w.flagged)
	for i := range vols.Items {
		vol := &vols.Items[i]
		if !needsRestageOn(vol.Status.Conditions, w.NodeName) {
			continue
		}
		if reason := w.mountGone(vol, booted); reason != "" {
			err := w.clear(ctx, vol.Name, reason)
			if err == nil {
				log.Info("filesystem no longer mounted; cleared the restage condition", "volume", vol.Name, "reason", reason)
				continue
			}
			log.Error(err, "clearing the restage condition", "volume", vol.Name)
		}
		delete(stale, vol.Name)
		w.flag(vol)
	}
	for name := range stale {
		w.unflag(name)
	}
}

// mountGone returns the clear reason when the mount the condition names
// can no longer exist, or "" while it may.
func (w *WritebackWatcher) mountGone(vol *miroirv1alpha1.MiroirVolume, booted time.Time) string {
	cond := meta.FindStatusCondition(vol.Status.Conditions, miroirv1alpha1.ConditionFilesystemNeedsRestage)
	if !booted.IsZero() && cond.LastTransitionTime.Time.Before(booted) {
		return reasonRebooted
	}
	minor := vol.Status.PerNode[w.NodeName].DRBDMinor
	if minor <= 0 {
		return reasonUnmounted
	}
	// A failed check keeps the condition: "could not check" must never
	// read as "not mounted".
	if mounted, err := deviceMountedAnywhere(w.procDir(), minor); err == nil && !mounted {
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

// mark raises the condition for this node; false when it already was.
func (w *WritebackWatcher) mark(ctx context.Context, name, msg string) (bool, error) {
	return w.updateCondition(ctx, name, func(conds *[]metav1.Condition) bool {
		if needsRestageOn(*conds, w.NodeName) {
			return false
		}
		// Removed first so a condition another node raised restarts its
		// transition time: the sweep compares it against this node's boot.
		meta.RemoveStatusCondition(conds, miroirv1alpha1.ConditionFilesystemNeedsRestage)
		meta.SetStatusCondition(conds, metav1.Condition{
			Type:    miroirv1alpha1.ConditionFilesystemNeedsRestage,
			Status:  metav1.ConditionTrue,
			Reason:  reasonWritebackError,
			Message: msg,
		})
		return true
	})
}

// clear lowers the condition when this node raised it.
func (w *WritebackWatcher) clear(ctx context.Context, name, reason string) error {
	_, err := w.updateCondition(ctx, name, func(conds *[]metav1.Condition) bool {
		if !needsRestageOn(*conds, w.NodeName) {
			return false
		}
		meta.SetStatusCondition(conds, metav1.Condition{
			Type:   miroirv1alpha1.ConditionFilesystemNeedsRestage,
			Status: metav1.ConditionFalse,
			Reason: reason,
		})
		return true
	})
	return client.IgnoreNotFound(err)
}

// updateCondition merge-patches the volume's conditions under an
// optimistic lock: a merge patch replaces the whole list, so an unlocked
// write would drop a condition another writer added since the read.
func (w *WritebackWatcher) updateCondition(ctx context.Context, name string, mutate func(*[]metav1.Condition) bool) (bool, error) {
	var changed bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vol := &miroirv1alpha1.MiroirVolume{}
		if err := w.reader().Get(ctx, types.NamespacedName{Name: name}, vol); err != nil {
			return err
		}
		base := vol.DeepCopy()
		changed = mutate(&vol.Status.Conditions)
		if !changed {
			return nil
		}
		return w.Client.Status().Patch(ctx, vol, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		return false, fmt.Errorf("writeback: update %s condition: %w", name, err)
	}
	return changed, nil
}

// restagePrefix opens the condition message with the node whose mount
// holds the error: only that node's remount resets it.
func restagePrefix(node string) string {
	return "node " + node + ": "
}

// needsRestageOn reports whether the condition is raised for node's mount.
func needsRestageOn(conds []metav1.Condition, node string) bool {
	c := meta.FindStatusCondition(conds, miroirv1alpha1.ConditionFilesystemNeedsRestage)
	return c != nil && c.Status == metav1.ConditionTrue && strings.HasPrefix(c.Message, restagePrefix(node))
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
