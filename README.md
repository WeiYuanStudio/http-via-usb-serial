# HTTP via USB Serial

通过 USB 串口连接内网机和外网机，实现 HTTP/HTTPS 代理访问互联网。

## 功能

- **统一代理** (`:8080`)：同时支持 CONNECT 代理 (HTTP/HTTPS) 和透明代理 (HTTP/HTTPS)
- **反向代理** (`:8081`)：将请求自动转发到指定上游（如 API 地址）
- **串口多路复用**：多个请求通过流 ID 并发传输
- **数据压缩**：透明代理流量使用 gzip 压缩，减少串口传输量
- **CRC32 校验**：每帧尾部带 CRC32，检测串口传输中的数据损坏
- **自动重传**：CRC 校验失败时自动请求对端重传（最多 5 次），缓冲区 2048 条目
- **HTTPS 自动升级**：透明代理首次访问时优先 HTTP，若遇 302 重定向到 HTTPS 则自动升级并记忆

## 适用场景

- 离线电脑（内网机）通过 USB 串口连接到有外网的电脑访问互联网
- 没有网线/WiFi 的情况下临时共享网络
- 串口速度有限（115200 bps ≈ 14KB/s），适合浏览网页、小文件传输

## 推荐硬件

需要购买 **两个** USB-TTL 串口转换器（内网机和外网机各一个），通过杜邦线交叉连接（TX ↔ RX，RX ↔ TX，GND ↔ GND）。总共需要3PIN杜邦线。

推荐使用 **CH343** 芯片的转换器，具体型号 **沁恒官方 CH343G6T**，实测可用，购入价格约 18 元包邮。

> 不推荐 CH340/CH341 系列，在 macOS/Linux 下存在驱动兼容性问题。

## 编译

```bash
go build -o http-proxy main.go
```

依赖：[github.com/tarm/serial](https://github.com/tarm/serial)

## 使用方法

### 外网机（Server）

```bash
./http-proxy --role server \
  --serial /dev/ttyUSB0 \
  --baud 115200 \
  --proxy-listen :8080
```

### 内网机（Client）

```bash
./http-proxy --role client \
  --serial /dev/ttyUSB0 \
  --baud 115200 \
  --proxy-listen :8080
```

### 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--role` | `client` | 角色：`client`（内网机）或 `server`（外网机） |
| `--serial` | `/dev/ttyUSB0` | 串口设备路径 |
| `--baud` | `115200` | 波特率 |
| `--proxy-listen` | `:8080` | 统一代理监听地址，同时支持 CONNECT + 透明代理（留空禁用） |
| `--reverse-upstream` | `https://api.deepseek.com` | 反向代理上游地址 |
| `--reverse-listen` | `:8081` | 反向代理监听地址（留空禁用） |

macOS 串口通常为 `/dev/tty.usbserial-*` 或 `/dev/cu.usbserial-*`。

## 使用示例

### 方法一：系统代理（透明代理）

在内网机上设置环境变量或系统代理：

```bash
export http_proxy=http://127.0.0.1:8080
export https_proxy=http://127.0.0.1:8080
```

> 注意：HTTP 首次访问若被 302 重定向到 HTTPS，会自动升级并记忆该 host，后续直接走 HTTPS。

### 方法二：curl / wget（CONNECT 代理）

```bash
curl --proxy http://127.0.0.1:8080 https://example.com
```

### 方法三：浏览器设置

在浏览器或系统网络设置中，配置 HTTP/HTTPS 代理为 `127.0.0.1:8080`。

### 方法四：反向代理端口

在内网机 GUI 中将 API 地址直接填写为反向代理地址：

```bash
# 启动客户端时配置反向代理
./http-proxy --role client \
  --reverse-upstream https://api.deepseek.com \
  --reverse-listen :8081

# GUI 中 API 地址填写 http://127.0.0.1:8081
```

## 注意事项

1. **串口权限**：Linux/macOS 可能需要将用户加入 `dialout` 组，或使用 `sudo`
2. **波特率**：建议使用 115200 或更高波特率以获得更好性能
3. **带宽限制**：串口速度有限，不适合大文件下载或视频播放
4. **HTTPS 支持**：透明代理支持 HTTPS 自动升级（首次 HTTP 遇 302 重定向自动切换）；CONNECT 代理原生支持 HTTPS

## 协议简介

消息格式：`[2字节Magic(0x39C5)][1字节类型][4字节流ID][4字节长度][4字节序列号][数据][4字节CRC32]`

- **Magic**：帧同步魔数，用于检测和恢复帧边界
- **流 ID**：用于多路复用，区分不同并发请求。Client 用奇数 (1,3,5...)
- **序列号**：单调递增，用于重传请求中定位丢失的帧
- **CRC32**：IEEE 802.3 标准，覆盖数据部分，用于检测传输错误
- **压缩**：透明代理请求/响应使用 gzip 压缩；CONNECT 隧道数据不压缩
- **重传**：接收端 CRC 校验失败时发送 `MsgRetransmit(SeqNum)`，发送端从环形缓冲区重发；单帧最多重试 5 次

详细协议定义见 [AGENTS.md](./AGENTS.md)。

## License

MIT
