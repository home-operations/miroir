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
	"text/template"
)

// ganeshaParams are the substitutions for one export's ganesha.conf.
type ganeshaParams struct {
	// Path is the local mount point the device is mounted at and the
	// export's real backing directory.
	Path string
	// Pseudo is the NFSv4 pseudo-filesystem path clients mount
	// (<service>:<Pseudo>).
	Pseudo string
	// RecoveryRoot holds NFSv4 client recovery records. It lives on the
	// exported (replicated) volume so grace state survives the gateway
	// pod moving to another replica node on failover.
	RecoveryRoot string
	// GracePeriod and LeaseLifetime are the NFSv4 recovery timings, in
	// seconds.
	GracePeriod   int
	LeaseLifetime int
	// Port is the NFS listen port (2049).
	Port int
}

// ganeshaTemplate is an NFSv4-only, TCP, VFS-FSAL export. NFSv3 and its
// ancillary protocols (mountd/statd/rquotad) are off: a single well-known
// port keeps the per-volume Service simple and needs no portmapper. The fs
// recovery backend rooted on the volume is what makes failover keep locks.
var ganeshaTemplate = template.Must(template.New("ganesha.conf").Parse(
	`NFS_CORE_PARAM {
	NFS_Port = {{.Port}};
	Protocols = 4;
	Enable_UDP = false;
}

NFSv4 {
	RecoveryBackend = fs;
	RecoveryRoot = {{.RecoveryRoot}};
	Grace_Period = {{.GracePeriod}};
	Lease_Lifetime = {{.LeaseLifetime}};
}

EXPORT {
	Export_Id = 1;
	Path = {{.Path}};
	Pseudo = {{.Pseudo}};
	Access_Type = RW;
	Squash = No_Root_Squash;
	Protocols = 4;
	Transports = TCP;
	SecType = sys;

	FSAL {
		Name = VFS;
	}
}
`))

// renderGanesha renders the ganesha.conf for one export.
func renderGanesha(p ganeshaParams) string {
	var b strings.Builder
	// The template is a compile-time constant with only string/int fields,
	// so Execute cannot fail; a builder never returns an error either.
	_ = ganeshaTemplate.Execute(&b, p)
	return b.String()
}
