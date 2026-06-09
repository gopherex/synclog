package gateway

import "errors"

// ErrAccessDenied is the canonical error product hooks should wrap or return
// when actor/subscriber/target/payload checks deny a gateway operation.
var ErrAccessDenied = errors.New("synclog gateway: access denied")
