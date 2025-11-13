package server

// Codegen for meta snapshot types only, isolated from the rest of the file.
//go:generate go tool github.com/tinylib/msgp -tests=false -io=false -unexported -file=js_meta_codec_types.go -o=js_meta_msgp.go

import ()

// A minimal snapshot client representation.
type metaClient struct {
    Account string `msg:"acc,omitempty"`
    Service string `msg:"svc,omitempty"`
    Cluster string `msg:"cluster,omitempty"`
}

type metaRaftGroup struct {
    Name      string   `msg:"name"`
    Peers     []string `msg:"peers"`
    Storage   uint8    `msg:"store"`
    Cluster   string   `msg:"cluster,omitempty"`
    Preferred string   `msg:"preferred,omitempty"`
    ScaleUp   bool     `msg:"scale_up,omitempty"`
}

type metaConsumerAssignment struct {
    Client     metaClient `msg:"client,omitempty"`
    CreatedNS  int64      `msg:"created"`
    Name       string     `msg:"name"`
    Stream     string     `msg:"stream"`
    ConfigJSON []byte     `msg:"consumer"`
    Group      metaRaftGroup `msg:"group"`
    State      []byte     `msg:"state,omitempty"`
}

type metaStreamAssignment struct {
    Client     metaClient `msg:"client,omitempty"`
    CreatedNS  int64      `msg:"created"`
    ConfigJSON []byte     `msg:"stream"`
    Group      metaRaftGroup `msg:"group"`
    Sync       string     `msg:"sync"`
    Consumers  []metaConsumerAssignment `msg:"consumers"`
}

type wsaList []metaStreamAssignment
//msgp:encode wsaList
//msgp:decode wsaList
