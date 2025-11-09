// Copyright 2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/json"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server/jetstream/metasnap"
)

func cloneSnapshotBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	cp := make([]byte, len(src))
	copy(cp, src)
	return cp
}

func encodeClientInfoForSnapshot(ci *ClientInfo) (metasnap.ClientInfo, bool) {
	if ci == nil {
		return metasnap.ClientInfo{}, false
	}
	var out metasnap.ClientInfo
	if ci.Start != nil {
		out.Has_start = true
		out.Start_unix_nano = ci.Start.UnixNano()
	}
	out.Host = ci.Host
	out.Id = ci.ID
	out.Account = ci.Account
	out.Service = ci.Service
	out.User = ci.User
	out.Name = ci.Name
	out.Lang = ci.Lang
	out.Version = ci.Version
	out.Rtt_nano = int64(ci.RTT)
	out.Server = ci.Server
	out.Cluster = ci.Cluster
	if len(ci.Alternates) > 0 {
		out.Alternates = append(out.Alternates, ci.Alternates...)
	}
	if ci.Stop != nil {
		out.Has_stop = true
		out.Stop_unix_nano = ci.Stop.UnixNano()
	}
	out.Jwt = ci.Jwt
	out.Issuer_key = ci.IssuerKey
	out.Name_tag = ci.NameTag
	if len(ci.Tags) > 0 {
		out.Tags = append(out.Tags, ci.Tags...)
	}
	out.Kind = ci.Kind
	out.Client_type = ci.ClientType
	out.Mqtt_client = ci.MQTTClient
	out.Nonce = ci.Nonce
	return out, true
}

func decodeClientInfoFromSnapshot(ci metasnap.ClientInfo) *ClientInfo {
	info := &ClientInfo{
		Host:       ci.Host,
		ID:         ci.Id,
		Account:    ci.Account,
		Service:    ci.Service,
		User:       ci.User,
		Name:       ci.Name,
		Lang:       ci.Lang,
		Version:    ci.Version,
		RTT:        time.Duration(ci.Rtt_nano),
		Server:     ci.Server,
		Cluster:    ci.Cluster,
		Jwt:        ci.Jwt,
		IssuerKey:  ci.Issuer_key,
		NameTag:    ci.Name_tag,
		Kind:       ci.Kind,
		ClientType: ci.Client_type,
		MQTTClient: ci.Mqtt_client,
		Nonce:      ci.Nonce,
	}
	if ci.Has_start {
		start := time.Unix(0, ci.Start_unix_nano).UTC()
		info.Start = &start
	}
	if ci.Has_stop {
		stop := time.Unix(0, ci.Stop_unix_nano).UTC()
		info.Stop = &stop
	}
	if len(ci.Alternates) > 0 {
		info.Alternates = append(info.Alternates, ci.Alternates...)
	}
	if len(ci.Tags) > 0 {
		tags := make(jwt.TagList, len(ci.Tags))
		copy(tags, ci.Tags)
		info.Tags = tags
	}
	return info
}

func encodeRaftGroupForSnapshot(rg *raftGroup) (metasnap.RaftGroup, bool) {
	if rg == nil {
		return metasnap.RaftGroup{}, false
	}
	out := metasnap.RaftGroup{
		Name:         rg.Name,
		Storage_type: int32(rg.Storage),
		Cluster:      rg.Cluster,
		Preferred:    rg.Preferred,
		Scale_up:     rg.ScaleUp,
	}
	if len(rg.Peers) > 0 {
		out.Peers = append(out.Peers, rg.Peers...)
	}
	return out, true
}

func decodeRaftGroupFromSnapshot(rg metasnap.RaftGroup) *raftGroup {
	grp := &raftGroup{
		Name:      rg.Name,
		Storage:   StorageType(rg.Storage_type),
		Cluster:   rg.Cluster,
		Preferred: rg.Preferred,
		ScaleUp:   rg.Scale_up,
	}
	if len(rg.Peers) > 0 {
		grp.Peers = append(grp.Peers, rg.Peers...)
	}
	return grp
}

func encodeSequencePairForSnapshot(sp SequencePair) metasnap.SequencePair {
	return metasnap.SequencePair{Consumer: sp.Consumer, Stream: sp.Stream}
}

func decodeSequencePairFromSnapshot(sp metasnap.SequencePair) SequencePair {
	return SequencePair{Consumer: sp.Consumer, Stream: sp.Stream}
}

func encodeConsumerStateForSnapshot(state *ConsumerState) (metasnap.ConsumerState, bool) {
	if state == nil {
		return metasnap.ConsumerState{}, false
	}
	out := metasnap.ConsumerState{
		Delivered: encodeSequencePairForSnapshot(state.Delivered),
		Ack_floor: encodeSequencePairForSnapshot(state.AckFloor),
	}
	if len(state.Pending) > 0 {
		out.Pending = make(map[uint64]metasnap.PendingEntry, len(state.Pending))
		for seq, pending := range state.Pending {
			if pending == nil {
				continue
			}
			out.Pending[seq] = metasnap.PendingEntry{Sequence: pending.Sequence, Timestamp: pending.Timestamp}
		}
	}
	if len(state.Redelivered) > 0 {
		out.Redelivered = make(map[uint64]uint64, len(state.Redelivered))
		for seq, val := range state.Redelivered {
			out.Redelivered[seq] = val
		}
	}
	return out, true
}

func decodeConsumerStateFromSnapshot(state metasnap.ConsumerState) *ConsumerState {
	cs := &ConsumerState{
		Delivered: decodeSequencePairFromSnapshot(state.Delivered),
		AckFloor:  decodeSequencePairFromSnapshot(state.Ack_floor),
	}
	if len(state.Pending) > 0 {
		cs.Pending = make(map[uint64]*Pending, len(state.Pending))
		for seq, pending := range state.Pending {
			cp := pending
			cs.Pending[seq] = &Pending{Sequence: cp.Sequence, Timestamp: cp.Timestamp}
		}
	}
	if len(state.Redelivered) > 0 {
		cs.Redelivered = make(map[uint64]uint64, len(state.Redelivered))
		for seq, val := range state.Redelivered {
			cs.Redelivered[seq] = val
		}
	}
	return cs
}

func encodeConsumerAssignmentForSnapshot(ca *consumerAssignment) (metasnap.ConsumerAssignment, bool) {
	if ca == nil {
		return metasnap.ConsumerAssignment{}, false
	}
	snap := metasnap.ConsumerAssignment{
		Created_unix_nano: ca.Created.UnixNano(),
		Name:              ca.Name,
		Stream:            ca.Stream,
		Consumer_config:   cloneSnapshotBytes(ca.ConfigJSON),
	}
	if ci := ca.Client; ci != nil {
		if encoded, ok := encodeClientInfoForSnapshot(ci.forAssignmentSnap()); ok {
			snap.Has_client = true
			snap.Client = encoded
		}
	}
	if rg, ok := encodeRaftGroupForSnapshot(ca.Group); ok {
		snap.Has_group = true
		snap.Group = rg
	}
	if state, ok := encodeConsumerStateForSnapshot(ca.State); ok {
		snap.Has_state = true
		snap.State = state
	}
	return snap, true
}

func decodeConsumerAssignmentFromSnapshot(ca metasnap.ConsumerAssignment) *consumerAssignment {
	decoded := &consumerAssignment{
		Created:    time.Unix(0, ca.Created_unix_nano).UTC(),
		Name:       ca.Name,
		Stream:     ca.Stream,
		ConfigJSON: json.RawMessage(cloneSnapshotBytes(ca.Consumer_config)),
	}
	if ca.Has_client {
		decoded.Client = decodeClientInfoFromSnapshot(ca.Client)
	}
	if ca.Has_group {
		decoded.Group = decodeRaftGroupFromSnapshot(ca.Group)
	}
	if ca.Has_state {
		decoded.State = decodeConsumerStateFromSnapshot(ca.State)
	}
	ensureConsumerAssignmentConfig(decoded)
	return decoded
}

func encodeStreamAssignmentForSnapshot(sa *streamAssignment) (metasnap.StreamAssignment, int) {
	snap := metasnap.StreamAssignment{
		Created_unix_nano: sa.Created.UnixNano(),
		Stream_config:     cloneSnapshotBytes(sa.ConfigJSON),
		Sync:              sa.Sync,
	}
	if ci := sa.Client; ci != nil {
		if encoded, ok := encodeClientInfoForSnapshot(ci.forAssignmentSnap()); ok {
			snap.Has_client = true
			snap.Client = encoded
		}
	}
	if rg, ok := encodeRaftGroupForSnapshot(sa.Group); ok {
		snap.Has_group = true
		snap.Group = rg
	}
	if len(sa.consumers) > 0 {
		snap.Consumers = make([]metasnap.ConsumerAssignment, 0, len(sa.consumers))
	}
	count := 0
	for _, ca := range sa.consumers {
		if ca == nil || ca.pending {
			continue
		}
		if encoded, ok := encodeConsumerAssignmentForSnapshot(ca); ok {
			snap.Consumers = append(snap.Consumers, encoded)
			count++
		}
	}
	return snap, count
}

func decodeStreamAssignmentFromSnapshot(sa metasnap.StreamAssignment) *streamAssignment {
	decoded := &streamAssignment{
		Created:    time.Unix(0, sa.Created_unix_nano).UTC(),
		ConfigJSON: json.RawMessage(cloneSnapshotBytes(sa.Stream_config)),
		Sync:       sa.Sync,
	}
	if sa.Has_client {
		decoded.Client = decodeClientInfoFromSnapshot(sa.Client)
	}
	if sa.Has_group {
		decoded.Group = decodeRaftGroupFromSnapshot(sa.Group)
	}
	if len(sa.Consumers) > 0 {
		decoded.consumers = make(map[string]*consumerAssignment, len(sa.Consumers))
		for _, c := range sa.Consumers {
			ca := decodeConsumerAssignmentFromSnapshot(c)
			decoded.consumers[ca.Name] = ca
		}
	}
	ensureStreamAssignmentConfig(decoded)
	return decoded
}

func ensureStreamAssignmentConfig(sa *streamAssignment) {
	if sa == nil || sa.Config != nil || len(sa.ConfigJSON) == 0 {
		return
	}
	var cfg StreamConfig
	if err := json.Unmarshal(sa.ConfigJSON, &cfg); err != nil {
		return
	}
	sa.Config = &cfg
}

func ensureConsumerAssignmentConfig(ca *consumerAssignment) {
	if ca == nil || ca.Config != nil || len(ca.ConfigJSON) == 0 {
		return
	}
	var cfg ConsumerConfig
	if err := json.Unmarshal(ca.ConfigJSON, &cfg); err != nil {
		return
	}
	ca.Config = &cfg
}
