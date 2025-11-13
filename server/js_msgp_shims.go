package server

import "encoding/json"

// Provide msgp shims for types referenced by writeable* assignments.

// json.RawMessage as []byte
type RawJSON = json.RawMessage

//msgp:shim RawJSON as:[]byte using:rawJSONToBytes,bytesToRawJSON
func rawJSONToBytes(r RawJSON) []byte { return []byte(r) }
func bytesToRawJSON(b []byte) RawJSON { return RawJSON(b) }

// (No shim for ConsumerState; generate native msgp for it.)

// ClientInfo compact snapshot
type clientInfoSnap struct {
    Account string `msg:"acc,omitempty"`
    Service string `msg:"svc,omitempty"`
    Cluster string `msg:"cluster,omitempty"`
}

//msgp:shim ClientInfo as:clientInfoSnap using:clientInfoToSnap,clientInfoFromSnap
func clientInfoToSnap(ci ClientInfo) clientInfoSnap {
    return clientInfoSnap{Account: ci.Account, Service: ci.Service, Cluster: ci.Cluster}
}
func clientInfoFromSnap(s clientInfoSnap) ClientInfo {
    return ClientInfo{Account: s.Account, Service: s.Service, Cluster: s.Cluster}
}
