// Command probe-websocket serves a Probe action that exchanges messages with
// a WebSocket server. Probe starts it for a step that uses
// github.com/mozership/probe-websocket@<commit>.
package main

import (
	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe/sdk"
)

// version is set at build time.
var version = "dev"

func main() {
	sdk.Serve(func(log hclog.Logger) sdk.Action {
		return &Action{log: log}
	})
}
