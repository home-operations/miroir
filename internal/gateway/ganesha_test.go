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

package gateway

import (
	"strings"
	"testing"
)

func TestRenderGanesha(t *testing.T) {
	conf := renderGanesha(ganeshaParams{
		Path:          "/export/pvc-abc",
		Pseudo:        "/pvc-abc",
		RecoveryRoot:  "/export/pvc-abc/.ganesha-recovery",
		GracePeriod:   60,
		LeaseLifetime: 30,
		Port:          2049,
	})

	// The recovery root must sit on the exported volume so lock state
	// survives the gateway moving nodes — the whole point of fs backend.
	for _, want := range []string{
		"NFS_Port = 2049;",
		"Protocols = 4;",
		"RecoveryBackend = fs;",
		"RecoveryRoot = /export/pvc-abc/.ganesha-recovery;",
		"Grace_Period = 60;",
		"Path = /export/pvc-abc;",
		"Pseudo = /pvc-abc;",
		"Transports = TCP;",
		"Name = VFS;",
	} {
		if !strings.Contains(conf, want) {
			t.Errorf("rendered config missing %q\n---\n%s", want, conf)
		}
	}

	// NFSv3 and its portmapper-bound helpers must stay off: the per-volume
	// Service exposes only 2049.
	if strings.Contains(conf, "Protocols = 3") || strings.Contains(conf, "Protocols = 4, 3") {
		t.Errorf("NFSv3 must not be enabled:\n%s", conf)
	}
}
