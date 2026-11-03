package security_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/security"
)

type fixture struct {
	MasterKey    string `json:"master_key"`
	PreviousKey  string `json:"previous_key"`
	CurrentKeyID string `json:"current_key_id"`
	Envelopes    []struct {
		Plain    map[string]any `json:"plain"`
		Envelope string         `json:"envelope"`
	} `json:"envelopes"`
	OldEnvelopes []struct {
		Plain    map[string]any `json:"plain"`
		Envelope string         `json:"envelope"`
	} `json:"old_envelopes"`
	TokenHashes []struct {
		Token  string   `json:"token"`
		Hashes []string `json:"hashes"`
	} `json:"token_hashes"`
	Passwords []struct {
		Password string `json:"password"`
		Hash     string `json:"hash"`
	} `json:"passwords"`
}

func setup(t *testing.T) fixture {
	t.Helper()
	var fx fixture
	contract.Load(t, "crypto", &fx)
	s, err := config.Load(func(k string) (string, bool) {
		switch k {
		case "BEZISYNC_MASTER_KEY":
			return fx.MasterKey, true
		case "BEZISYNC_PREVIOUS_MASTER_KEYS":
			return fx.PreviousKey, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	return fx
}

func normalize(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDecryptsPythonEnvelopes(t *testing.T) {
	fx := setup(t)
	if got := security.CurrentKeyID(); got != fx.CurrentKeyID {
		t.Fatalf("key id %s, want %s", got, fx.CurrentKeyID)
	}
	all := append(fx.Envelopes, fx.OldEnvelopes...)
	for _, e := range all {
		got, err := security.DecryptSecret(e.Envelope)
		if err != nil {
			t.Fatalf("decrypt %s: %v", e.Envelope, err)
		}
		if !reflect.DeepEqual(normalize(t, got), normalize(t, e.Plain)) {
			t.Errorf("decrypted %v, want %v", got, e.Plain)
		}
	}
}

func TestEnvelopeRoundTripAndFormat(t *testing.T) {
	fx := setup(t)
	for _, e := range fx.Envelopes {
		raw, _ := json.Marshal(e.Plain)
		plain, err := pyjson.DecodeGeneric(raw) // keep ints as ints, like Python
		if err != nil {
			t.Fatal(err)
		}
		blob, err := security.EncryptSecret(plain)
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(blob, ".")
		if len(parts) != 4 || parts[0] != "v1" || parts[1] != fx.CurrentKeyID {
			t.Fatalf("unexpected envelope %s", blob)
		}
		// Same plaintext length as Python's compact sorted JSON → same ciphertext length.
		pyParts := strings.Split(e.Envelope, ".")
		if len(parts[3]) != len(pyParts[3]) {
			t.Errorf("ciphertext length %d differs from Python's %d", len(parts[3]), len(pyParts[3]))
		}
		back, err := security.DecryptSecret(blob)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(normalize(t, back), normalize(t, e.Plain)) {
			t.Errorf("round trip %v, want %v", back, e.Plain)
		}
	}
}

func TestIntegersSurviveReencryption(t *testing.T) {
	setup(t)
	blob := security.MustEncrypt(map[string]any{"port": 5432, "ratio": 0.5})
	plain, err := security.DecryptSecret(blob)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := security.DecryptRaw(security.MustEncrypt(plain))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"port":5432,"ratio":0.5}` {
		t.Fatalf("got %s", raw)
	}
}

func TestUnknownKeyAndTamperingFail(t *testing.T) {
	fx := setup(t)
	blob := fx.Envelopes[1].Envelope
	parts := strings.Split(blob, ".")
	if _, err := security.DecryptSecret(strings.Join([]string{"v1", "deadbeef", parts[2], parts[3]}, ".")); err == nil {
		t.Error("unknown key id accepted")
	}
	tampered := []byte(parts[3])
	tampered[5] ^= 1
	if _, err := security.DecryptSecret(strings.Join([]string{parts[0], parts[1], parts[2], string(tampered)}, ".")); err == nil {
		t.Error("tampered ciphertext accepted")
	}
	if _, err := security.DecryptSecret("v2." + strings.Join(parts[1:], ".")); err == nil {
		t.Error("unknown version accepted")
	}
}

func TestTokenHashesMatchPython(t *testing.T) {
	fx := setup(t)
	for _, c := range fx.TokenHashes {
		if got := security.TokenHashes(c.Token); !reflect.DeepEqual(got, c.Hashes) {
			t.Errorf("TokenHashes(%q) = %v, want %v", c.Token, got, c.Hashes)
		}
		if got := security.TokenHash(c.Token); got != c.Hashes[0] {
			t.Errorf("TokenHash(%q) = %s", c.Token, got)
		}
	}
}

func TestPasswordsCompatibleWithArgon2CFFI(t *testing.T) {
	fx := setup(t)
	for _, p := range fx.Passwords {
		if !security.VerifyPassword(p.Hash, p.Password) {
			t.Errorf("Python hash for %q did not verify", p.Password)
		}
		if security.VerifyPassword(p.Hash, p.Password+"x") {
			t.Errorf("wrong password verified")
		}
	}
	h, err := security.HashPassword("hunter2hunter2")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(h, "$argon2id$v=19$m=65536,t=3,p=4$") {
		t.Fatalf("unexpected PHC prefix %s", h)
	}
	if !security.VerifyPassword(h, "hunter2hunter2") || security.VerifyPassword("garbage", "x") {
		t.Fatal("verify failed")
	}
}

func TestNewToken(t *testing.T) {
	tok := security.NewToken("bzs")
	if !strings.HasPrefix(tok, "bzs_") || len(tok) != 4+43 {
		t.Fatalf("unexpected token %q", tok)
	}
}
