package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
)

// TestManifestLastDivergenceExported guards the observable gauge that reports
// when each field last diverged. It was documented long before it existed, so
// the failure mode is a silent absence rather than an error.
func TestManifestLastDivergenceExported(t *testing.T) {
	r, err := New("t", 1, "", 0)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	r.ManifestDivergence("shard_bits", "peer-disagree")

	mfs, err := r.promReg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var sb strings.Builder
	enc := expfmt.NewEncoder(&sb, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		_ = enc.Encode(mf)
	}
	out := sb.String()
	if !strings.Contains(out, "multicast_manifest_last_divergence_epoch") {
		t.Error("multicast_manifest_last_divergence_epoch missing from /metrics")
	}
	if !strings.Contains(out, `field="shard_bits"`) {
		t.Errorf("missing field label; got:\n%s", out)
	}
}
