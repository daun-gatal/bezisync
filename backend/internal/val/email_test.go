package val_test

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

func TestEmailMatchesPydantic(t *testing.T) {
	var fx struct {
		Cases []struct {
			In    string `json:"in"`
			Out   string `json:"out"`
			Error string `json:"error"`
		} `json:"cases"`
	}
	contract.Load(t, "email", &fx)
	// The wording of IDNA decoding errors comes from Python's idna package; Go reports the
	// same rejection in its own words.
	idnaWording := map[string]bool{"a@xn--ab.org": true}
	for _, c := range fx.Cases {
		v := &val.V{}
		got, ok := v.Email(nil, c.In)
		if c.Error != "" {
			if ok {
				t.Errorf("%q accepted as %q, want error %q", c.In, got, c.Error)
			} else if v.Errors[0].Message != c.Error && !idnaWording[c.In] {
				t.Errorf("%q:\n got %q\nwant %q", c.In, v.Errors[0].Message, c.Error)
			}
			continue
		}
		if !ok || got != c.Out {
			t.Errorf("%q: got %q %v, want %q", c.In, got, v.Errors, c.Out)
		}
	}
}
