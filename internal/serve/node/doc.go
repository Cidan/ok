// Package node defines the node vocabulary of ok's serving runtime — the interfaces a role's
// constructs implement. It is types only: the engine that consumes them (graph walk, validation,
// the Setup lifecycle, serving) lives in package serve, so a node implementation depends only on
// this small vocabulary, never on the runtime.
//
// Every construct is a Node — name, dependencies, health, Setup. The kind interfaces (Service,
// Application, WebServer) add what a node contributes to the running role. Nodes are singletons,
// obtained through their package's Get() accessor.
package node
