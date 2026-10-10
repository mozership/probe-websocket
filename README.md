# probe-websocket

A [Probe](https://github.com/mozership/probe) action that exchanges messages with a WebSocket server.

Probe downloads it the first time a workflow uses it. It needs Probe v1.17.0 or later, and v1.21.0 or later to run under a guard, which reads the guard and the params the action declares in `action.yml`.

```yaml
name: Ticker
jobs:
- name: ticker
  steps:
  - name: Subscribe and get an update
    uses: github.com/mozership/probe-websocket@<commit SHA>
    with:
      url: wss://example.com/ws
      headers:
        Authorization: Bearer {{ vars.token }}
      messages:
      - send: {type: subscribe, channel: ticker}
      - receive:
          match: {type: subscribed}
      - receive:
          count: 3
          match: {type: update}
    test: res.code == 101 && len(res.messages) == 4 && res.messages[3].data.price > 0
```

Probe only takes a full 40-character commit SHA. The notes of each [release](https://github.com/mozership/probe-websocket/releases) start with the `uses` line to copy.

Every step opens a new connection, runs its `messages` in order, and closes it, so a subscription does not carry over to the next step: put a whole exchange in one step.

## Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `url` | String | Yes | - | The server, `ws` or `wss` |
| `headers` | Object | No | - | Handshake request headers, such as `Authorization`. They override `User-Agent: probe-websocket/<version>` |
| `subprotocols` | List | No | - | The subprotocols to offer, in order of preference |
| `messages` | List | No | - | What to send and receive, in order. See [Messages](#messages). Without it, the step connects and closes |
| `timeout` | Duration | No | `30s` | Time limit for the whole step, from the handshake to the close, as `10s` or a number of seconds. `0` removes it |

A key of `with` that is not one of these fails the step before anything is sent, and `probe check` reports it with its line.

## Messages

Each entry of `messages` has exactly one of these keys.

| Key | Value | What it does |
|---|---|---|
| `send` | String, or any other value | Sends a text message: a string as it is, anything else as JSON |
| `send_binary` | Base64 string | Sends the decoded bytes as a binary message |
| `receive` | Object, or nothing | Waits for messages and keeps them in `res.messages`. Without options, it takes the next message |

`receive` takes these options.

| Option | Type | Description |
|---|---|---|
| `count` | Integer | Takes this many messages. Defaults to `1` |
| `until_close` | Boolean | Takes every message until the server closes the connection. It cannot be used with `count`, and the entry must be the last one |
| `match` | Any | Takes only the messages whose `data` contains it, and skips the others. An object matches an object that has each of its keys with a matching value, a list matches a list of as many matching entries, and anything else matches an equal value, numbers by value |

With `match`, `count` is how many matching messages to take. A skipped message is not kept.

## Result

| Field | Type | Description |
|---|---|---|
| `res.code` | Integer | HTTP status code of the handshake, `101` when the connection was upgraded |
| `res.status` | String | HTTP status line, such as `"101 Switching Protocols"` |
| `res.headers` | Object | Handshake response headers, keyed by canonical name |
| `res.subprotocol` | String | The subprotocol the server chose, or empty |
| `res.messages` | Array | The messages the `receive` entries took, in order. See below |
| `res.close` | Object | The `code` and `reason` the server closed the connection with, or `null` when it did not |
| `res.error` | String | Why the messages stopped before the last entry, or empty when every entry was done |
| `req` | Object | The `url`, `headers`, `subprotocols` and `messages` that were used |
| `rt` | Duration | Time from the handshake to the close |
| `status` | Integer | `0` when the connection was upgraded and every entry was done; `1` otherwise |

Each message in `res.messages` has these fields.

| Field | Type | Description |
|---|---|---|
| `type` | String | `"text"` or `"binary"` |
| `data` | Any | A text message parsed as JSON when it is JSON, otherwise the string. A binary message as base64 |
| `raw` | String | The unparsed text, or the base64 of a binary message |

Once the server answers the handshake, what happens is a result, so a test can assert on a `401` handshake, a close before the expected message, or a message that never came. The messages stop at the first entry that cannot be done, and `res.error` says which, such as `messages[1] took 2 of 3 messages: timed out after 10s`. Only a handshake that gets no answer, such as a refused connection or a timeout, fails the step as an error.

When every entry is done and the server has not closed the connection, the action closes it with `1000`. A message larger than 16 MiB ends the exchange.

## Guard

The action keeps to the guard of the run, and its `action.yml` declares `guard: [read-only, allow-host]`, so Probe runs it under `--read-only` and `--allow-host` without `--allow-action`.

- Under `--read-only`, only a step that receives is run. What a message does on the server cannot be told, so a step with a `send` or a `send_binary` entry is refused before connecting. A server that needs a message before it sends anything, such as a subscribe, has to be allowed with `--allow-action`.
- Under `--allow-host`, the host of `url`, and of each redirect of the handshake, must be one the run allows. A URL without a port is taken at the port of its scheme: `80` for `ws` and `443` for `wss`.

A refused step fails with the kind `refused`.

## Releasing

Pushing a `v*` tag builds the executables with GoReleaser and publishes them on the release. The workflow then commits an `action.yml` with their URLs and SHA-256 digests to `main`, and adds the commit to the release notes. That commit is the one to pin: Probe reads `action.yml` at the pinned commit and refuses an executable whose digest differs.

## License

MIT
