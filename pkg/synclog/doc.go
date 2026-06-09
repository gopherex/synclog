// Package synclog provides a library-first durable sync log.
//
// The package is intentionally domain-agnostic: it stores opaque event bytes,
// assigns per-stream sequence numbers, tracks server-side subscriber cursors,
// and exposes catch-up, subscribe-ready reads, and monotonic acknowledgements.
//
// Product-facing APIs should normally be built through a gateway layer that
// resolves public targets to internal streams and applies product-owned access
// policy. Raw stream identifiers are a platform concern, not a frontend API.
package synclog
