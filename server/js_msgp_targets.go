package server

// Generate msgp methods for selected types across the package by scanning
// only the files that define them and the shims.
//go:generate go tool github.com/tinylib/msgp -tests=false -io=false -unexported -file=jetstream_cluster.go -file=store.go -file=js_msgp_shims.go -o=js_msgp_gen_all.go

// Targeted types (explicit)
//msgp:encode writeableStreamAssignment
//msgp:decode writeableStreamAssignment
//msgp:encode writeableConsumerAssignment
//msgp:decode writeableConsumerAssignment
//msgp:encode raftGroup
//msgp:decode raftGroup
//msgp:encode SequencePair
//msgp:decode SequencePair
//msgp:encode Pending
//msgp:decode Pending
//msgp:encode ConsumerState
//msgp:decode ConsumerState
