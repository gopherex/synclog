// Package gateway defines product-facing sync contracts over synclog streams.
//
// Gateway code speaks in public SyncTarget values and delegates target
// resolution, authorization, payload exposure, snapshot exposure, and codecs to
// the embedding product. It must not expose internal stream ids to frontend
// clients.
package gateway
