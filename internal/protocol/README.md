# 协议版本一

控制通道使用 JSON WebSocket。请求包含 `version`、`type`、`client_id`、`request_id`、`instance_id`、`method` 和 `params`；响应返回对应的客户端、请求和实例标识。server 建立连接后立即发送 `hello`。`/data` WebSocket 用于独立文件分块连接。

命令、任务、实例介绍和文件传输均使用同一套控制协议。文件数据通过独立的数据 WebSocket 连接传输；每条数据连接只处理一个文件分块，并使用 `transfer_id`、分块编号、偏移量、长度和 SHA-256 校验。固定使用 5 MiB 分块和 4 路并行；传输可通过 `transfer_id` 续传，服务端保留状态 30 分钟。
