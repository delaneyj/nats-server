package server

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

const (
	metaSnapshotStreamFieldCount   = 6
	metaSnapshotConsumerFieldCount = 7
	metaSnapshotRaftGroupFields    = 6
	metaSnapshotSequenceFields     = 2
	metaSnapshotPendingFields      = 2
)

func encodeMetaSnapshot(streams []writeableStreamAssignment, buf *pooledBuffer) ([]byte, error) {
	buf.Reset()
	w := msgp.NewWriter(buf)

	if err := w.WriteArrayHeader(uint32(len(streams))); err != nil {
		return nil, err
	}
	for i := range streams {
		if err := encodeWriteableStreamAssignment(w, &streams[i]); err != nil {
			return nil, err
		}
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeMetaSnapshot(data []byte) ([]writeableStreamAssignment, error) {
	if len(data) == 0 {
		return nil, nil
	}

	if looksLikeJSON(data) {
		var wsas []writeableStreamAssignment
		if err := json.Unmarshal(data, &wsas); err != nil {
			return nil, err
		}
		return wsas, nil
	}

	reader := msgp.NewReader(bytes.NewReader(data))
	count, err := reader.ReadArrayHeader()
	if err != nil {
		return nil, err
	}

	streams := make([]writeableStreamAssignment, 0, count)
	for i := uint32(0); i < count; i++ {
		wsa, err := decodeWriteableStreamAssignment(reader)
		if err != nil {
			return nil, err
		}
		streams = append(streams, *wsa)
	}

	return streams, nil
}

func encodeWriteableStreamAssignment(w *msgp.Writer, sa *writeableStreamAssignment) error {
	if err := encodeSnapshotClientInfo(w, sa.Client); err != nil {
		return err
	}
	if err := w.WriteTime(sa.Created); err != nil {
		return err
	}
	if err := w.WriteBytes([]byte(sa.ConfigJSON)); err != nil {
		return err
	}
	if err := encodeSnapshotRaftGroup(w, sa.Group); err != nil {
		return err
	}
	if err := w.WriteString(sa.Sync); err != nil {
		return err
	}
	if err := w.WriteArrayHeader(uint32(len(sa.Consumers))); err != nil {
		return err
	}
	for _, consumer := range sa.Consumers {
		if consumer == nil {
			return fmt.Errorf("nil consumer assignment in meta snapshot")
		}
		if err := encodeWriteableConsumerAssignment(w, consumer); err != nil {
			return err
		}
	}
	return nil
}

func decodeWriteableStreamAssignment(r *msgp.Reader) (*writeableStreamAssignment, error) {
	fieldCountRaw, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}
	fieldCount := int(fieldCountRaw)
	if fieldCount < metaSnapshotStreamFieldCount {
		return nil, fmt.Errorf("invalid stream snapshot payload: expected at least %d fields, got %d", metaSnapshotStreamFieldCount, fieldCount)
	}

	var wsa writeableStreamAssignment
	if wsa.Client, err = decodeSnapshotClientInfo(r); err != nil {
		return nil, err
	}
	if wsa.Created, err = r.ReadTime(); err != nil {
		return nil, err
	}
	if wsa.ConfigJSON, err = readRawMessage(r); err != nil {
		return nil, err
	}
	if wsa.Group, err = decodeSnapshotRaftGroup(r); err != nil {
		return nil, err
	}
	if wsa.Sync, err = r.ReadString(); err != nil {
		return nil, err
	}
	consumerCountRaw, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}
	consumerCount := int(consumerCountRaw)
	if consumerCount > 0 {
		wsa.Consumers = make([]*writeableConsumerAssignment, 0, consumerCount)
		for i := 0; i < consumerCount; i++ {
			wc, err := decodeWriteableConsumerAssignment(r)
			if err != nil {
				return nil, err
			}
			if wc != nil {
				wsa.Consumers = append(wsa.Consumers, wc)
			}
		}
	}

	// Skip any newly added fields to keep backwards compatibility.
	for extra := metaSnapshotStreamFieldCount; extra < fieldCount; extra++ {
		if err := r.Skip(); err != nil {
			return nil, err
		}
	}

	return &wsa, nil
}

func encodeWriteableConsumerAssignment(w *msgp.Writer, ca *writeableConsumerAssignment) error {
	if err := encodeSnapshotClientInfo(w, ca.Client); err != nil {
		return err
	}
	if err := w.WriteTime(ca.Created); err != nil {
		return err
	}
	if err := w.WriteString(ca.Name); err != nil {
		return err
	}
	if err := w.WriteString(ca.Stream); err != nil {
		return err
	}
	if err := w.WriteBytes([]byte(ca.ConfigJSON)); err != nil {
		return err
	}
	if err := encodeSnapshotRaftGroup(w, ca.Group); err != nil {
		return err
	}
	return encodeSnapshotConsumerState(w, ca.State)
}

func decodeWriteableConsumerAssignment(r *msgp.Reader) (*writeableConsumerAssignment, error) {
	if typ, err := r.NextType(); err != nil {
		return nil, err
	} else if typ == msgp.NilType {
		if err := r.ReadNil(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	fieldCountRaw, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}
	fieldCount := int(fieldCountRaw)
	if fieldCount < metaSnapshotConsumerFieldCount {
		return nil, fmt.Errorf("invalid consumer snapshot payload: expected at least %d fields, got %d", metaSnapshotConsumerFieldCount, fieldCount)
	}

	var ca writeableConsumerAssignment
	if ca.Client, err = decodeSnapshotClientInfo(r); err != nil {
		return nil, err
	}
	if ca.Created, err = r.ReadTime(); err != nil {
		return nil, err
	}
	if ca.Name, err = r.ReadString(); err != nil {
		return nil, err
	}
	if ca.Stream, err = r.ReadString(); err != nil {
		return nil, err
	}
	if ca.ConfigJSON, err = readRawMessage(r); err != nil {
		return nil, err
	}
	if ca.Group, err = decodeSnapshotRaftGroup(r); err != nil {
		return nil, err
	}
	if ca.State, err = decodeSnapshotConsumerState(r); err != nil {
		return nil, err
	}

	for extra := metaSnapshotConsumerFieldCount; extra < fieldCount; extra++ {
		if err := r.Skip(); err != nil {
			return nil, err
		}
	}

	return &ca, nil
}

func encodeSnapshotClientInfo(w *msgp.Writer, ci *ClientInfo) error {
	if ci == nil {
		return w.WriteNil()
	}
	if err := w.WriteArrayHeader(3); err != nil {
		return err
	}
	if err := w.WriteString(ci.Account); err != nil {
		return err
	}
	if err := w.WriteString(ci.Service); err != nil {
		return err
	}
	return w.WriteString(ci.Cluster)
}

func decodeSnapshotClientInfo(r *msgp.Reader) (*ClientInfo, error) {
	if typ, err := r.NextType(); err != nil {
		return nil, err
	} else if typ == msgp.NilType {
		if err := r.ReadNil(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	countRaw, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}
	count := int(countRaw)
	if count < 3 {
		return nil, fmt.Errorf("invalid client info payload: expected 3 entries, got %d", count)
	}

	account, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	service, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	cluster, err := r.ReadString()
	if err != nil {
		return nil, err
	}

	for extra := 3; extra < count; extra++ {
		if err := r.Skip(); err != nil {
			return nil, err
		}
	}

	return &ClientInfo{Account: account, Service: service, Cluster: cluster}, nil
}

func encodeSnapshotRaftGroup(w *msgp.Writer, rg *raftGroup) error {
	if rg == nil {
		return w.WriteNil()
	}
	if err := w.WriteArrayHeader(metaSnapshotRaftGroupFields); err != nil {
		return err
	}
	if err := w.WriteString(rg.Name); err != nil {
		return err
	}
	if err := writeStringSlice(w, rg.Peers); err != nil {
		return err
	}
	if err := w.WriteInt(int(rg.Storage)); err != nil {
		return err
	}
	if err := w.WriteString(rg.Cluster); err != nil {
		return err
	}
	if err := w.WriteString(rg.Preferred); err != nil {
		return err
	}
	if err := w.WriteBool(rg.ScaleUp); err != nil {
		return err
	}
	return nil
}

func decodeSnapshotRaftGroup(r *msgp.Reader) (*raftGroup, error) {
	if typ, err := r.NextType(); err != nil {
		return nil, err
	} else if typ == msgp.NilType {
		if err := r.ReadNil(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	fieldCountRaw, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}
	fieldCount := int(fieldCountRaw)
	if fieldCount < metaSnapshotRaftGroupFields {
		return nil, fmt.Errorf("invalid raft group payload: expected %d fields, got %d", metaSnapshotRaftGroupFields, fieldCount)
	}

	var rg raftGroup
	if rg.Name, err = r.ReadString(); err != nil {
		return nil, err
	}
	if rg.Peers, err = readStringSlice(r); err != nil {
		return nil, err
	}
	storage, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	rg.Storage = StorageType(storage)
	if rg.Cluster, err = r.ReadString(); err != nil {
		return nil, err
	}
	if rg.Preferred, err = r.ReadString(); err != nil {
		return nil, err
	}
	if rg.ScaleUp, err = r.ReadBool(); err != nil {
		return nil, err
	}

	for extra := metaSnapshotRaftGroupFields; extra < fieldCount; extra++ {
		if err := r.Skip(); err != nil {
			return nil, err
		}
	}

	return &rg, nil
}

func encodeSnapshotConsumerState(w *msgp.Writer, state *ConsumerState) error {
	if state == nil {
		return w.WriteNil()
	}
	if err := w.WriteArrayHeader(4); err != nil {
		return err
	}
	if err := encodeSequencePair(w, state.Delivered); err != nil {
		return err
	}
	if err := encodeSequencePair(w, state.AckFloor); err != nil {
		return err
	}
	if err := encodePendingMap(w, state.Pending); err != nil {
		return err
	}
	return encodeRedeliveredMap(w, state.Redelivered)
}

func decodeSnapshotConsumerState(r *msgp.Reader) (*ConsumerState, error) {
	if typ, err := r.NextType(); err != nil {
		return nil, err
	} else if typ == msgp.NilType {
		if err := r.ReadNil(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	fieldCountRaw, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}
	fieldCount := int(fieldCountRaw)
	if fieldCount < 4 {
		return nil, fmt.Errorf("invalid consumer state payload: expected 4 fields, got %d", fieldCount)
	}

	state := &ConsumerState{}
	if err := decodeSequencePairInto(r, &state.Delivered); err != nil {
		return nil, err
	}
	if err := decodeSequencePairInto(r, &state.AckFloor); err != nil {
		return nil, err
	}
	if state.Pending, err = decodePendingMap(r); err != nil {
		return nil, err
	}
	if state.Redelivered, err = decodeRedeliveredMap(r); err != nil {
		return nil, err
	}

	for extra := 4; extra < fieldCount; extra++ {
		if err := r.Skip(); err != nil {
			return nil, err
		}
	}

	return state, nil
}

func encodeSequencePair(w *msgp.Writer, sp SequencePair) error {
	if err := w.WriteArrayHeader(metaSnapshotSequenceFields); err != nil {
		return err
	}
	if err := w.WriteUint64(sp.Consumer); err != nil {
		return err
	}
	return w.WriteUint64(sp.Stream)
}

func decodeSequencePairInto(r *msgp.Reader, sp *SequencePair) error {
	countRaw, err := r.ReadArrayHeader()
	if err != nil {
		return err
	}
	count := int(countRaw)
	if count < metaSnapshotSequenceFields {
		return fmt.Errorf("invalid sequence pair payload: expected %d fields, got %d", metaSnapshotSequenceFields, count)
	}
	if sp.Consumer, err = r.ReadUint64(); err != nil {
		return err
	}
	if sp.Stream, err = r.ReadUint64(); err != nil {
		return err
	}
	for extra := metaSnapshotSequenceFields; extra < count; extra++ {
		if err := r.Skip(); err != nil {
			return err
		}
	}
	return nil
}

func encodePendingMap(w *msgp.Writer, pending map[uint64]*Pending) error {
	if pending == nil {
		return w.WriteMapHeader(0)
	}
	if err := w.WriteMapHeader(uint32(len(pending))); err != nil {
		return err
	}
	for seq, entry := range pending {
		if err := w.WriteUint64(seq); err != nil {
			return err
		}
		if entry == nil {
			if err := w.WriteNil(); err != nil {
				return err
			}
			continue
		}
		if err := w.WriteArrayHeader(metaSnapshotPendingFields); err != nil {
			return err
		}
		if err := w.WriteUint64(entry.Sequence); err != nil {
			return err
		}
		if err := w.WriteInt64(entry.Timestamp); err != nil {
			return err
		}
	}
	return nil
}

func decodePendingMap(r *msgp.Reader) (map[uint64]*Pending, error) {
	if typ, err := r.NextType(); err != nil {
		return nil, err
	} else if typ == msgp.NilType {
		if err := r.ReadNil(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	countRaw, err := r.ReadMapHeader()
	if err != nil {
		return nil, err
	}
	count := int(countRaw)
	if count == 0 {
		return nil, nil
	}

	pending := make(map[uint64]*Pending, count)
	for i := 0; i < count; i++ {
		seq, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}
		if typ, err := r.NextType(); err != nil {
			return nil, err
		} else if typ == msgp.NilType {
			if err := r.ReadNil(); err != nil {
				return nil, err
			}
			pending[seq] = nil
			continue
		}
		itemFieldsRaw, err := r.ReadArrayHeader()
		if err != nil {
			return nil, err
		}
		itemFields := int(itemFieldsRaw)
		if itemFields < metaSnapshotPendingFields {
			return nil, fmt.Errorf("invalid pending entry payload: expected %d fields, got %d", metaSnapshotPendingFields, itemFields)
		}
		entry := &Pending{}
		if entry.Sequence, err = r.ReadUint64(); err != nil {
			return nil, err
		}
		if entry.Timestamp, err = r.ReadInt64(); err != nil {
			return nil, err
		}
		for extra := metaSnapshotPendingFields; extra < itemFields; extra++ {
			if err := r.Skip(); err != nil {
				return nil, err
			}
		}
		pending[seq] = entry
	}
	return pending, nil
}

func encodeRedeliveredMap(w *msgp.Writer, redelivered map[uint64]uint64) error {
	if redelivered == nil {
		return w.WriteMapHeader(0)
	}
	if err := w.WriteMapHeader(uint32(len(redelivered))); err != nil {
		return err
	}
	for seq, count := range redelivered {
		if err := w.WriteUint64(seq); err != nil {
			return err
		}
		if err := w.WriteUint64(count); err != nil {
			return err
		}
	}
	return nil
}

func decodeRedeliveredMap(r *msgp.Reader) (map[uint64]uint64, error) {
	if typ, err := r.NextType(); err != nil {
		return nil, err
	} else if typ == msgp.NilType {
		if err := r.ReadNil(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	countRaw, err := r.ReadMapHeader()
	if err != nil {
		return nil, err
	}
	count := int(countRaw)
	if count == 0 {
		return nil, nil
	}

	redelivered := make(map[uint64]uint64, count)
	for i := 0; i < count; i++ {
		seq, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}
		cnt, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}
		redelivered[seq] = cnt
	}
	return redelivered, nil
}

func writeStringSlice(w *msgp.Writer, values []string) error {
	if err := w.WriteArrayHeader(uint32(len(values))); err != nil {
		return err
	}
	for _, v := range values {
		if err := w.WriteString(v); err != nil {
			return err
		}
	}
	return nil
}

func readStringSlice(r *msgp.Reader) ([]string, error) {
	count, err := r.ReadArrayHeader()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	values := make([]string, count)
	for i := uint32(0); i < count; i++ {
		if values[i], err = r.ReadString(); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func readRawMessage(r *msgp.Reader) (json.RawMessage, error) {
	data, err := r.ReadBytes(nil)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	return json.RawMessage(copied), nil
}

func looksLikeJSON(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\n', '\r', '\t':
			continue
		case '[', '{':
			return true
		default:
			return false
		}
	}
	return false
}
