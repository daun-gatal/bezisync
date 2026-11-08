package flinkhelper

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// metaEntry is one operator entry as Flink's MetadataV5Serializer writes it.
func metaEntry(name, uid string, coordinator []byte, subtasks int32) []byte {
	var b bytes.Buffer
	utf := func(s string) {
		_ = binary.Write(&b, binary.BigEndian, uint16(len(s))) //nolint:gosec // test strings are short
		b.WriteString(s)
	}
	utf(name)
	utf(uid)
	b.Write(operatorID(uid))
	_ = binary.Write(&b, binary.BigEndian, int32(1))   // parallelism
	_ = binary.Write(&b, binary.BigEndian, int32(128)) // max parallelism
	if coordinator == nil {
		b.WriteByte(0)
	} else {
		b.WriteByte(1)
		utf("2b7b5e0c-handle")
		_ = binary.Write(&b, binary.BigEndian, int32(len(coordinator))) //nolint:gosec // test data
		b.Write(coordinator)
	}
	_ = binary.Write(&b, binary.BigEndian, subtasks)
	if subtasks > 0 {
		b.Write([]byte{0, 0, 0, 0, 9, 9, 9}) // subtask 0 and some state
	}
	return b.Bytes()
}

func metaFile(version uint32, entries ...[]byte) []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, uint32(metadataMagic))
	_ = binary.Write(&b, binary.BigEndian, version)
	_ = binary.Write(&b, binary.BigEndian, int64(42)) // checkpoint id
	_ = binary.Write(&b, binary.BigEndian, int32(0))  // master states
	_ = binary.Write(&b, binary.BigEndian, int32(len(entries)))
	for _, e := range entries {
		b.Write(e)
	}
	_ = binary.Write(&b, binary.BigEndian, int32(0)) // trailing properties
	return b.Bytes()
}

const metaShort = "0123456789ab"

func TestOperatorIDIsGuavaMurmur3(t *testing.T) {
	// Guava: Hashing.murmur3_128().hashString("hello", UTF_8).toString()
	if got := hex.EncodeToString(operatorID("hello")); got != "029bbd41b3a7d8cb191dae486a901e5b" {
		t.Fatalf("murmur3_128(hello) = %s", got)
	}
}

func TestForgetCoordinatorStateRemovesOnlyTheSchemaOperators(t *testing.T) {
	uid := SchemaOperatorUID(metaShort)
	source := metaEntry("Source: Flink CDC Event Source: postgres", "bz-"+metaShort+"-source", []byte("enumerator state"), 1)
	schema := metaEntry("SchemaOperator", uid, []byte("old schemas"), 1)
	sink := metaEntry("Sink Writer", "bz-"+metaShort+"-sink-writer", nil, 1)
	raw := metaFile(6, source, schema, sink)
	out, changed, err := forgetCoordinatorState(raw, uid)
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	want := metaFile(6, source, metaEntry("SchemaOperator", uid, nil, 1), sink)
	if !bytes.Equal(out, want) {
		t.Fatalf("metadata after:\n%x\nwant:\n%x", out, want)
	}
	// Again: nothing left to remove.
	again, changed, err := forgetCoordinatorState(out, uid)
	if err != nil || changed || !bytes.Equal(again, out) {
		t.Fatalf("second pass changed=%v err=%v", changed, err)
	}
}

func TestForgetCoordinatorStateVersion5AndEmptyUID(t *testing.T) {
	uid := SchemaOperatorUID(metaShort)
	// An entry whose uid Flink didn't record: found by its operator ID.
	e := metaEntry("SchemaOperator", uid, []byte("x"), 1)
	unnamed := append([]byte{0, 14}, "SchemaOperator"...)
	unnamed = append(unnamed, 0, 0)
	unnamed = append(unnamed, e[2+14+2+len(uid):]...)
	out, changed, err := forgetCoordinatorState(metaFile(5, unnamed), uid)
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	if bytes.Contains(out, []byte("2b7b5e0c-handle")) {
		t.Fatal("coordinator state left")
	}
}

func TestForgetCoordinatorStateNoOperator(t *testing.T) {
	raw := metaFile(6, metaEntry("Source", "bz-"+metaShort+"-source", []byte("s"), 1))
	out, changed, err := forgetCoordinatorState(raw, SchemaOperatorUID(metaShort))
	if err != nil || changed || !bytes.Equal(out, raw) {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
}

func TestForgetCoordinatorStateRefusesWhatItDoesntKnow(t *testing.T) {
	uid := SchemaOperatorUID(metaShort)
	good := metaFile(6, metaEntry("SchemaOperator", uid, []byte("old"), 1))
	cases := map[string][]byte{
		"not metadata": []byte("PK\x03\x04 a zip file"),
		"version 4":    metaFile(4, metaEntry("SchemaOperator", uid, []byte("old"), 1)),
		"file handle": func() []byte {
			b := bytes.Clone(good)
			b[bytes.Index(b, operatorID(uid))+24] = 2
			return b
		}(),
		"truncated": good[:bytes.Index(good, []byte("2b7b5e0c"))+4],
		"twice":     metaFile(6, metaEntry("SchemaOperator", uid, []byte("a"), 1), metaEntry("SchemaOperator", uid, []byte("b"), 1)),
		"misplaced": func() []byte {
			id := operatorID(uid)
			return metaFile(6, append([]byte{0, 3, 'a', 'b', 'c', 0, 1, 'x'}, id...))
		}(),
	}
	for name, raw := range cases {
		if _, _, err := forgetCoordinatorState(raw, uid); !errors.Is(err, errMetadata) {
			t.Errorf("%s: err = %v, want errMetadata", name, err)
		}
	}
}

func TestForgetCoordinatorStateFile(t *testing.T) {
	dir := t.TempDir()
	uid := SchemaOperatorUID(metaShort)
	if err := os.WriteFile(filepath.Join(dir, "_metadata"), metaFile(6, metaEntry("SchemaOperator", uid, []byte("old"), 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, err := ForgetCoordinatorState(dir, uid)
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "_metadata"))
	if bytes.Contains(raw, []byte("old")) {
		t.Fatal("not rewritten")
	}
	if _, err := os.Stat(filepath.Join(dir, "_metadata.bz")); !os.IsNotExist(err) {
		t.Fatal("temp file left")
	}
}
