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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// APIStartupWait bounds how long the startup sweeps wait for the API server,
// so a reboot that races control-plane recovery does not exit on the first
// dial error and churn through CrashLoopBackOff. Kept under the liveness
// kill window: the probe endpoints are not up until the manager starts.
const APIStartupWait = 45 * time.Second

// apiShutdownWait bounds the shutdown barrier sweep's API access: the
// termination grace budget is 60s and the manager stop plus
// DownSecondaries can already spend 45s of it. APIStartupWait would
// guarantee a SIGKILL mid-sweep. The periodic mid-runtime sweep reuses it:
// a short per-tick budget whose failure just logs and retries next tick.
const apiShutdownWait = 5 * time.Second

// APIWithRetry retries one API call until it succeeds, hits a terminal
// (non-transient) error, or the budget elapses — so a control plane still
// coming back up does not crash the process on startup. It is the one
// retry policy every pre-manager API access shares.
func APIWithRetry(ctx context.Context, budget time.Duration, op func(ctx context.Context) error) error {
	var lastErr error
	waitErr := wait.PollUntilContextTimeout(ctx, 2*time.Second, budget, true,
		func(ctx context.Context) (bool, error) {
			lastErr = op(ctx)
			if lastErr == nil {
				return true, nil
			}
			if !transientAPIError(lastErr) {
				return false, lastErr
			}
			setupLog.Info("API server not ready; retrying", "error", lastErr.Error())
			return false, nil
		})
	if waitErr != nil && lastErr != nil {
		return lastErr
	}
	return waitErr
}

// ListWithRetry retries an API list with the shared startup retry policy.
func ListWithRetry(ctx context.Context, c client.Reader, list client.ObjectList, budget time.Duration) error {
	return APIWithRetry(ctx, budget, func(ctx context.Context) error { return c.List(ctx, list) })
}

// transientAPIError reports whether an API error is worth retrying. Dial
// failures during control-plane recovery (connection refused, no route to
// host) arrive as non-APIStatus errors; only explicit terminal statuses
// (auth, not-found, invalid) and a kind the server does not serve (the
// CRDs are not installed) are treated as permanent.
func transientAPIError(err error) bool {
	switch {
	case err == nil:
		return false
	case apierrors.IsUnauthorized(err), apierrors.IsForbidden(err),
		apierrors.IsNotFound(err), apierrors.IsInvalid(err),
		meta.IsNoMatchError(err):
		return false
	default:
		return true
	}
}
