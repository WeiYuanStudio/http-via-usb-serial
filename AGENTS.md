# HTTP via USB Serial

通过 USB 串口连接内网机和外网机，实现 HTTP/HTTPS 代理访问互联网。

## 项目简介

- **用途**: 离线电脑通过 USB 串口连接到有外网的电脑，访问互联网
- **语言**: Go
- **功能**:
  - 统一代理 (端口同时支持 CONNECT + 透明代理)
  - 反向代理 (将请求转发到指定上游，如 API 地址)
  - 大文件流式传输 (自动检测 > 4MB 响应体，切换为流式分块)
  - 接收端乱序重排 (per-stream chunkNum 保证数据按序写出)
  - 发送窗口流控 (ACK 驱动的滑动窗口，防止发送端溢出)
  - 双层重传保障 (Scanner 层 CRC 检测 + Handler 层超时重试)

## 编译

```bash
go build -o http-proxy main.go
```

## 使用方法

### 外网机 (Server)

```bash
./http-proxy --role server \
  --serial /dev/ttyUSB0 \
  --baud 115200 \
  --proxy-listen :8080
```

### 内网机 (Client)

```bash
./http-proxy --role client \
  --serial /dev/ttyUSB0 \
  --baud 115200 \
  --proxy-listen :8080
```

## 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--role` | `client` | 角色: `client`(内网机) 或 `server`(外网机) |
| `--serial` | `/dev/ttyUSB0` | 串口设备路径 |
| `--baud` | `115200` | 波特率 |
| `--proxy-listen` | `:8080` | 统一代理监听地址，同时支持 CONNECT + 透明代理 (留空禁用) |
| `--reverse-upstream` | `https://api.deepseek.com` | 反向代理上游地址 |
| `--reverse-listen` | `:8081` | 反向代理监听地址 (留空禁用) |
| `--allow-lan` | `false` | 是否允许局域网其他设备访问 |

## 通信协议

- 消息格式: `[2字节Magic(0x39C5)][1字节类型][4字节流ID][4字节长度][4字节序列号][数据][4字节CRC32]`
- 流ID: Client 使用奇数 (1,3,5...)，用于多路复用多个并发连接
- 序列号(SeqNum): 全局单调递增(uint32)，每帧一个，用于 sendBuf 查找、去重
- 块号(chunkNum): 流内从0开始的连续序号，仅透明代理流式 MsgData 的 payload 前 4 字节携带，用于接收端排序
- CRC32: IEEE 802.3 标准，覆盖数据部分，检测传输错误
- 重传缓冲区(sendBuf): 环形缓冲 2048 条目，按 SeqNum % 2048 索引
- pendingStreams 映射: seqNum → streamID，sendBuf 条目被覆盖后仍能通知客户端
- streamChunkToSeq 映射: (streamID, chunkNum) → globalSeqNum，用于 chunkNum 定向重传
- 数据压缩: 透明代理请求/响应用 gzip 压缩，HTTP 头压缩。MsgData 数据块不压缩
- 帧同步: 扫描器逐字节查找 0x39C5 magic，通过 msgType 范围校验和 CRC32 过滤误识别

### 消息类型

| 类型 | 值 | 说明 | 方向 | Data 格式 |
|------|-----|------|------|-----------|
| `MsgTransparent` | `0x01` | 透明代理 HTTP 请求 | Client → Server | gzip 压缩的 HTTP 请求 |
| `MsgResponse` | `0x02` | 透明代理 HTTP 响应 (小文件) | Server → Client | gzip 压缩的完整 HTTP 响应 |
| `MsgConnect` | `0x03` | CONNECT 打开请求 | Client → Server | host:port 字符串 |
| `MsgConnectOK` | `0x04` | CONNECT 建立成功 | Server → Client | "OK" |
| `MsgData` | `0x05` | 流数据 | 双向 | 透明代理: `[4B chunkNum][body]`；CONNECT: 原始 TCP 数据 |
| `MsgClose` | `0x06` | 流关闭通知 | 双向 | 无 |
| `MsgError` | `0x07` | 错误响应 | Server → Client | 错误描述字符串 |
| `MsgResponseHeaders` | `0x08` | 流式响应头 (大文件/SSE) | Server → Client | gzip 压缩的 HTTP 响应头 |
| `MsgRetransmit` | `0x09` | 重传请求 (streamID=0) | 双向 | 4 字节: `[globalSeqNum]`；或 8 字节: `[4B streamID][4B chunkNum]` |
| `MsgAck` | `0x0A` | 接收确认 (滑动窗口) | Client → Server | 4 字节: `[lastContinuousChunkNum]` |

### 透明代理流程

#### 小文件响应 (< 4MB)

1. Client 把完整 HTTP 请求序列化、压缩后，发送 `MsgTransparent`
2. Server 解压、解析请求、代理访问目标网站
3. Server 把完整 HTTP 响应序列化、压缩后，发送 `MsgResponse`
4. Client 解压、解析响应，返回给浏览器

#### 大文件 / SSE 流式响应 (≥ 4MB 或 Content-Length 未知)

1. Client 发送 `MsgTransparent`
2. Server 发送 `MsgResponseHeaders`（响应头部分）
3. Server 将响应体按 chunkSize 分块（大文件 4096B / SSE 256B），每块 prepend 4 字节 chunkNum(0,1,2...)，发送 `MsgData`
4. Client 收到 `MsgResponseHeaders` → 解析并写入状态码和头
5. Client 收到 `MsgData` → 提取 chunkNum → `streamReorderBuf.put(chunkNum, body)` → 按序写出到 HTTP Response
6. Server 发送完所有数据后发送 `MsgClose`
7. Client 每收到一个 chunk 发送 `MsgAck`（携带最新连续 chunkNum，不越过未收到的块）

### CONNECT 代理流程

1. Client 发送 `MsgConnect`，data 为目标 `host:port`
2. Server 建立到目标的 TCP 连接，发送 `MsgConnectOK`
3. Client 向浏览器返回 `HTTP/1.1 200 Connection Established`
4. 双方进入双向流转发模式：
   - 从浏览器/目标服务器读取数据，发送 `MsgData`
   - 收到 `MsgData` 后写给目标服务器/浏览器
   - 连接关闭时发送 `MsgClose`

### 反向代理流程

1. Client 收到请求后，将 Host/Scheme 改写为配置的上游地址
2. 后续流程与透明代理相同，通过 `MsgTransparent` / `MsgResponse` 收发

### 重传与乱序处理

#### 双层重传机制

| 层 | 触发方 | 触发条件 | 格式 | 延迟 |
|----|--------|---------|------|------|
| Scanner 层 | `readMessageSync` | CRC32 校验失败 | `MsgRetransmit[globalSeqNum]` (4B) | 即时 |
| Handler 层 | client `handleStreamResponse` | reorder buffer 检测到 chunkNum 缺口 | `MsgRetransmit[streamID][chunkNum]` (8B) | 500ms 后 |

#### 流程

1. 发送端每发一帧，将其存入环形发送缓冲区 sendBuf (2048 条目，按 SeqNum 索引)
2. 同时存入 streamChunkToSeq 映射 (chunkNum → globalSeqNum)，供定向重传查询
3. 接收端读帧后校验 CRC32：
   - CRC 正确：deliver 到上层 (MsgData 走 reorder buffer 按 chunkNum 排序)
   - CRC 错误：
     - Scanner 层: streamID≠0 时发送 `MsgRetransmit(seqNum)` 请求重传
     - 不影响后续帧的处理，后续帧正常交付并暂存于 reorder buffer
4. 发送端收到 `MsgRetransmit`：
   - 若 data=4B (seqNum 格式)：直接从 sendBuf 查找 globalSeqNum → 重发原始帧
   - 若 data=8B (streamID+chunkNum 格式)：通过 streamChunkToSeq 映射查找 globalSeqNum → 重发原始帧
   - 若 sendBuf 条目已被覆盖（SEND_BUF_EVICTED）：从 pendingStreams 映射获取 streamID → 发送 MsgError
   - 不限制重传次数（服务端无条件重传），放弃决策由客户端超时控制
5. Client 端 Handler 层：若 reorder buffer 检测到缺口（chunk 不连续），启动 500ms 定时器
   - 定时器触发 → 发送 `MsgRetransmit(streamID, gapChunk)` 8 字节格式
   - 缺口补上 → 取消定时器，连续写出所有待写数据
   - 未补上 → 每 500ms 重试，直到 `streamChunkTimeout`(300s) 超时

#### 发送窗口流控

- Server 端维护 `clientChunkAck`（client 最新确认的连续 chunkNum）
- Server 每次发送前检查: `chunkNum - clientChunkAck < maxSendWindow(32)`
- 若窗口满：阻塞等待 ACK（500ms 间隔轮询），或 client 主动取消
- Client 端每收到一个 chunk 发送 `MsgAck(ackChunk = reorder.nextSeq - 1)`
- ACK 不越过未收到的块（cumulative ACK），缺口端 window 会卡住，配合重传机制填缺口恢复

### SeqNum vs chunkNum

| | SeqNum | chunkNum |
|---|--------|----------|
| 作用域 | 全局 (跨所有流) | 流内 (单个透明代理流) |
| 连续性 | 不连续 (被其他流/控制消息交错) | 严格连续 (0,1,2...) |
| 存储位置 | 帧头 15 字节内 | MsgData payload 前 4 字节 |
| 用途 | sendBuf 索引、去重、CRC 重传请求 | reorder buffer 排序、ACK 窗口计算、定向重传 |

### streamReorderBuf 工作原理

```
收到 chunkNum=2:
  nextSeq=0, started=false → nextSeq=0
  put(2) → pending[2]=data
  检查 pending[0]: 无 → 返回空 (缺口在 0)

收到 chunkNum=1:
  put(1) → pending[1]=data
  检查 pending[0]: 无 → 返回空 (缺口在 0)

收到 chunkNum=0:
  put(0) → pending[0]=data
  flush: 0在pending ✓ → 写出; nextSeq=1
         1在pending ✓ → 写出; nextSeq=2
         2在pending ✓ → 写出; nextSeq=3
         pending 清空 → 返回 [chunk0, chunk1, chunk2]
```

### HTTPS 自动升级

1. 透明代理首次访问某 host 时，优先使用 HTTP 发送请求
2. Server 端收到 HTTP 响应，若为 3xx 重定向且 Location 头包含 https：
   - 记录该 host 到内存 Map（标记为需要 HTTPS）
   - 自动用 HTTPS 重试该请求
3. 后续同一 host 的请求直接走 HTTPS，不再尝试 HTTP
4. Map 为内存存储，重启后清零

## 使用示例

### 方法一: 设置系统代理 (透明代理)

在内网机上设置 HTTP_PROXY 环境变量或系统代理:

```bash
export http_proxy=http://127.0.0.1:8080
export https_proxy=http://127.0.0.1:8080
```

### 方法二: 使用 CONNECT 代理

```bash
curl --proxy http://127.0.0.1:8080 https://example.com
```

### 方法三: 使用反向代理端口

在内网机 GUI 中直接将 API 地址填写为反向代理地址:

```bash
# 启动时配置反向代理
./http-proxy --role client \
  --reverse-upstream https://api.deepseek.com \
  --reverse-listen :8081

# 内网 GUI 中 API 地址填写 http://127.0.0.1:8081
```

## 关键常量

| 常量 | 值 | 说明 |
|------|-----|------|
| `streamChanSize` | 2048 | 流 channel 缓冲区大小 |
| `sendBufSize` | 2048 | 重传环形缓冲区条目数 |
| `maxSendWindow` | 32 | 发送窗口大小 (未确认 chunk 上限) |
| `ackEveryNChunks` | 1 | 每收到 N 个 chunk 发一次 ACK |
| `largeResponseThreshold` | 4 MB | 超过此大小的响应自动切流式传输 |
| `largeStreamChunkSize` | 4096 | 大文件流式分块大小 |
| `streamChunkSize` | 256 | SSE 流分块大小 |
| `streamChunkTimeout` | 300s | 流接收超时，超时后断开 |
| `maxMessageSize` | 10 MB | 单帧最大数据长度 |

## 诊断日志关键字

| 日志 | 位置 | 含义 |
|------|------|------|
| `Bad CRC: seq=... stream=...` | Client/Server readMessageSync | CRC32 校验失败，触发 Scanner 层重传 |
| `SEND_BUF_HIT` | Server handleMsgRetransmit | sendBuf 命中，正常重发 |
| `SEND_BUF_EVICTED` | Server handleMsgRetransmit | sendBuf 已覆盖，发 MsgError 通知 |
| `SEND_BUF_MISSING` | Server handleMsgRetransmit | 任何地方都找不到此帧 |
| `WINDOW_FULL chunk=... ack=...` | Server handleTransparentStream | 发送窗口满，阻塞等 ACK |
| `GAP nextChunk=... recvChunk=... pending=...` | Client handleStreamResponse | reorder buffer 检测到缺口 |
| `CHUNK_SEQ_NOT_FOUND` | Server handleMsgRetransmit | chunkNum 映射不存在 |

## 注意事项

1. 确保串口权限正确 (可能需要加入 dialout 用户组)
2. 透明代理支持 HTTPS 自动升级（HTTP 遇 302 重定向自动切换）；CONNECT 代理原生支持 HTTPS
3. 建议使用较高波特率 (115200+) 以获得更好性能；高波特率下 CRC 校验可检测数据损坏
4. 串口带宽有限，大文件传输会比较慢
5. 新旧协议帧格式不兼容，两端必须同时升级

## 依赖

- github.com/tarm/serial (串口通信)
