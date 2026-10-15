// Package serve is the serving runtime of ok's roles. A role builds a single root node — its
// assembly — and hands it to Run, which walks the node graph, validates it, sets every node up in
// dependency order, and serves the result on one port.
//
// The node vocabulary (the Node, Service, Application, and WebServer interfaces) lives in
// serve/node; this package is the engine over it — discovery, singleton dedup, duplicate and
// cycle validation, the Setup lifecycle, and the single-port gRPC, REST, and web mux
// (serve/serving). The graph is discovered purely by walking Dependencies() from the root, with
// no registration, init() glue, or reflection.
package serve
