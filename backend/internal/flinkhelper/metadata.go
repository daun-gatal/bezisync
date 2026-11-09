package flinkhelper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/twmb/murmur3"
)

// ADR 0013: a saved position from before a column change is restored with Flink CDC's schema
// coordinator state left out, so the coordinator learns the new columns from the CreateTableEvents
// the source sends after a restore; every other operator (the source's exact position, the sink's
// pending commits) restores as saved.

// metadataMagic starts every Flink checkpoint and savepoint _metadata file.
const metadataMagic = 0x4960672d

// SchemaOperatorUID is the UID Flink CDC gives a pipeline's schema operator under the compiler's
// operator.uid.prefix ("bz-<short id>").
func SchemaOperatorUID(shortID string) string { return "bz-" + shortID + "-schema-operator" }

// operatorID is the Flink operator ID of an operator with a user-set UID: Guava's murmur3_128
// (seed 0) of the UID's UTF-8 bytes, as Flink writes it (StreamGraphHasherV2).
func operatorID(uid string) []byte {
	h1, h2 := murmur3.Sum128([]byte(uid))
	out := make([]byte, 16)
	binary.LittleEndian.PutUint64(out[:8], h1)
	binary.LittleEndian.PutUint64(out[8:], h2)
	return out
}

// errMetadata is a _metadata file the helper won't change.
var errMetadata = errors.New("unexpected Flink metadata")

// ForgetCoordinatorState removes the coordinator state of the operator with this UID from a
// checkpoint or savepoint directory's _metadata file. It changes nothing unless the file is the
// format Flink 2 writes (metadata version 5 or 6) and the operator's entry is exactly where that
// format puts it; it reports whether it removed anything.
func ForgetCoordinatorState(dir, uid string) (bool, error) {
	path := filepath.Join(dir, "_metadata")
	raw, err := os.ReadFile(path) //nolint:gosec // a directory the helper extracted under state_dir
	if err != nil {
		return false, err
	}
	out, changed, err := forgetCoordinatorState(raw, uid)
	if err != nil || !changed {
		return false, err
	}
	tmp := path + ".bz"
	if err := os.WriteFile(tmp, out, 0o600); err != nil { //nolint:gosec // beside the _metadata the helper extracted under state_dir
		return false, err
	}
	return true, os.Rename(tmp, path)
}

// forgetCoordinatorState is ForgetCoordinatorState on the file's bytes. An operator entry in
// metadata versions 5 and 6 is: name (Java UTF), uid (Java UTF), operator ID (16 bytes),
// parallelism and max parallelism (int32 each), the coordinator state (a stream handle: 0 = none,
// 1 = bytes: Java UTF name, int32 length, the bytes), then the subtask states.
func forgetCoordinatorState(raw []byte, uid string) ([]byte, bool, error) {
	if err := metadataHeader(raw); err != nil {
		return nil, false, err
	}
	if len(uid) == 0 || len(uid) > 0xffff {
		return nil, false, fmt.Errorf("%w: invalid operator uid", errMetadata)
	}
	id := operatorID(uid)
	at := bytes.Index(raw, id)
	if at < 0 {
		return raw, false, nil // the position has no state for this operator: nothing to forget
	}
	if bytes.Contains(raw[at+len(id):], id) {
		return nil, false, fmt.Errorf("%w: the schema operator appears twice", errMetadata)
	}
	// The operator ID follows the entry's uid: this one, or empty when Flink didn't record it.
	named := binary.BigEndian.AppendUint16(nil, uint16(len(uid))) //nolint:gosec // checked above
	named = append(named, uid...)
	if !bytes.HasSuffix(raw[:at], named) && !bytes.HasSuffix(raw[:at], []byte{0, 0}) {
		return nil, false, fmt.Errorf("%w: the schema operator's ID isn't where an operator entry puts it", errMetadata)
	}
	needle := id
	handle := at + len(needle) + 8 // parallelism, max parallelism
	if handle >= len(raw) {
		return nil, false, fmt.Errorf("%w: truncated operator entry", errMetadata)
	}
	end := handle + 1
	switch raw[handle] {
	case 0:
		return raw, false, nil // no coordinator state
	case 1:
		if end+2 > len(raw) {
			return nil, false, fmt.Errorf("%w: truncated coordinator state", errMetadata)
		}
		end += 2 + int(binary.BigEndian.Uint16(raw[end:]))
		if end+4 > len(raw) {
			return nil, false, fmt.Errorf("%w: truncated coordinator state", errMetadata)
		}
		n := int(binary.BigEndian.Uint32(raw[end:]))
		end += 4
		if n < 0 || end+n+4 > len(raw) {
			return nil, false, fmt.Errorf("%w: coordinator state of %d bytes overruns the file", errMetadata, n)
		}
		end += n
	default:
		return nil, false, fmt.Errorf("%w: coordinator state stored as handle type %d", errMetadata, raw[handle])
	}
	// The subtask state count follows: -1 (fully finished) or a small count.
	if subtasks := int32(binary.BigEndian.Uint32(raw[end:])); subtasks < -1 || subtasks > 1<<15 { //nolint:gosec // a signed field
		return nil, false, fmt.Errorf("%w: %d subtask states after the coordinator state", errMetadata, subtasks)
	}
	out := make([]byte, 0, len(raw)-(end-handle)+1)
	out = append(out, raw[:handle]...)
	out = append(out, 0)
	out = append(out, raw[end:]...)
	return out, true, nil
}

// checkpointGap is how far above the highest checkpoint id this pipeline stored a restore of an
// older position starts numbering (ADR 0014): a job can complete a few checkpoints between two
// polls, or stop without a savepoint, and commit them unseen; ids are 64-bit, so a gap costs nothing.
const checkpointGap = 1000

// metadataHeader checks a _metadata file's magic number and version (5 or 6: the checkpoint id is
// the int64 after them).
func metadataHeader(raw []byte) error {
	if len(raw) < 16 || binary.BigEndian.Uint32(raw) != metadataMagic {
		return fmt.Errorf("%w: not a Flink metadata file", errMetadata)
	}
	if v := binary.BigEndian.Uint32(raw[4:]); v != 5 && v != 6 {
		return fmt.Errorf("%w: metadata version %d (BeziSync knows 5 and 6)", errMetadata, v)
	}
	return nil
}

// CheckpointID is the checkpoint id a checkpoint or savepoint directory's _metadata records.
func CheckpointID(dir string) (int64, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "_metadata")) //nolint:gosec // under state_dir
	if err != nil {
		return 0, err
	}
	if err := metadataHeader(raw); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(raw[8:])), nil //nolint:gosec // Flink writes a signed long
}

// SetCheckpointID rewrites the checkpoint id a restored copy's _metadata records. Flink numbers
// the restored job's checkpoints from it, and so do sinks that deduplicate commits by checkpoint id
// (Flink CDC's Iceberg committer skips any id at or below the highest one it committed for the
// writer's job id, which comes back from the writer's state).
func SetCheckpointID(dir string, id int64) error {
	path := filepath.Join(dir, "_metadata")
	raw, err := os.ReadFile(path) //nolint:gosec // under state_dir
	if err != nil {
		return err
	}
	if err := metadataHeader(raw); err != nil {
		return err
	}
	if id <= 0 {
		return fmt.Errorf("%w: invalid checkpoint id %d", errMetadata, id)
	}
	binary.BigEndian.PutUint64(raw[8:], uint64(id))
	tmp := path + ".bz"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil { //nolint:gosec // beside the _metadata the helper extracted under state_dir
		return err
	}
	return os.Rename(tmp, path)
}
