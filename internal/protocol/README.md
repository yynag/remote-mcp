# Protocol Version 1

The control channel uses a JSON WebSocket. Requests contain `version`, `type`, `client_id`, `request_id`, `instance_id`, `method`, and `params`; responses return the corresponding client, request, and instance identifiers. The server sends `hello` immediately after a connection is established. The `/data` WebSocket is used for independent file-chunk connections.

Commands, jobs, instance guides, and file transfers all use the same control protocol. File data is transferred over independent data WebSocket connections; each data connection handles a single file chunk and uses `transfer_id`, chunk index, offset, length, and SHA-256 verification. Chunks are fixed at 5 MiB with 4-way parallelism; transfers can resume by `transfer_id`, and the server retains state for 30 minutes.
