# HTTP via USB Serial

通过 USB 串口连接内网机和外网机，实现 HTTP/HTTPS 代理访问互联网。

## 功能

- **CONNECT 代理** (`:8080`)：支持 HTTP/HTTPS，浏览器/应用设置代理后使用
- **透明代理** (`:8081`)：支持 HTTP，通过设置系统代理使用
- **串口多路复用**：多个请求通过流 ID 并发传输
- **数据压缩**：透明代理流量使用 gzip 压缩，减少串口传输量

## 适用场景

- 离线电脑（内网机）通过 USB 串口连接到有外网的电脑访问互联网
- 没有网线/WiFi 的情况下临时共享网络
- 串口速度有限（115200 bps ≈ 14KB/s），适合浏览网页、小文件传输

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
  --proxy-listen :8080 \
  --transparent-listen :8081
```

### 内网机（Client）

```bash
./http-proxy --role client \
  --serial /dev/ttyUSB0 \
  --baud 115200 \
  --proxy-listen :8080 \
  --transparent-listen :8081
```

### 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--role` | `client` | 角色：`client`（内网机）或 `server`（外网机） |
| `--serial` | `/dev/ttyUSB0` | 串口设备路径 |
| `--baud` | `115200` | 波特率 |
| `--proxy-listen` | `:8080` | CONNECT 代理监听地址（留空禁用） |
| `--transparent-listen` | `:8081` | 透明代理监听地址（留空禁用） |

macOS 串口通常为 `/dev/tty.usbserial-*` 或 `/dev/cu.usbserial-*`。

## 使用示例

### 方法一：系统代理（透明代理）

在内网机上设置环境变量或系统代理：

```bash
export http_proxy=http://127.0.0.1:8081
export https_proxy=http://127.0.0.1:8081
```

> 注意：透明代理不支持 HTTPS，HTTPS 网站请使用下方的 CONNECT 代理。

### 方法二：curl / wget（CONNECT 代理）

```bash
curl --proxy http://127.0.0.1:8080 https://example.com
```

### 方法三：浏览器设置

在浏览器或系统网络设置中，配置 HTTP/HTTPS 代理为 `127.0.0.1:8080`。

## 注意事项

1. **串口权限**：Linux/macOS 可能需要将用户加入 `dialout` 组，或使用 `sudo`
2. **波特率**：建议使用 115200 或更高波特率以获得更好性能
3. **带宽限制**：串口速度有限，不适合大文件下载或视频播放
4. **HTTPS 必须走 CONNECT 代理**：透明代理（`8081`）只支持 HTTP

## 协议简介

消息格式：`[1字节类型][4字节流ID][4字节长度][数据]`

- **流 ID**：用于多路复用，区分不同并发请求
- **压缩**：透明代理请求/响应使用 gzip 压缩；CONNECT 隧道数据不压缩

详细协议定义见 [AGENTS.md](./AGENTS.md)。

## License

MIT
