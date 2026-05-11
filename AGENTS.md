<!-- From: /Users/weiyuan/Documents/code/http-via-usb-serail/AGENTS.md -->
# HTTP via USB Serial

通过 USB 串口连接内网机和外网机，实现 HTTP/HTTPS 代理访问互联网。

## 项目简介

- **用途**: 离线电脑通过 USB 串口连接到有外网的电脑，访问互联网
- **语言**: Go
- **功能**:
  - CONNECT 代理 (支持 HTTP/HTTPS)
  - 透明代理 (支持 HTTP)

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
  --proxy-listen :8080 \
  --transparent-listen :8081
```

### 内网机 (Client)

```bash
./http-proxy --role client \
  --serial /dev/ttyUSB0 \
  --baud 115200 \
  --proxy-listen :8080 \
  --transparent-listen :8081
```

## 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--role` | `client` | 角色: `client`(内网机) 或 `server`(外网机) |
| `--serial` | `/dev/ttyUSB0` | 串口设备路径 |
| `--baud` | `115200` | 波特率 |
| `--proxy-listen` | `:8080` | CONNECT 代理监听地址 (留空禁用) |
| `--transparent-listen` | `:8081` | 透明代理监听地址 (留空禁用) |

## 通信协议

- 消息格式: `[1字节类型][4字节流ID][4字节长度][数据]`
- 流ID: Client 使用奇数 (1,3,5...)，用于多路复用多个并发连接
- 数据压缩: 透明代理请求/响应用 gzip 压缩，CONNECT 流数据不压缩

### 消息类型

| 类型 | 值 | 说明 | 方向 |
|------|-----|------|------|
| `MsgTransparent` | `0x01` | 透明代理 HTTP 请求 | Client → Server |
| `MsgResponse` | `0x02` | 透明代理 HTTP 响应 | Server → Client |
| `MsgConnect` | `0x03` | CONNECT 打开请求 (data=host:port) | Client → Server |
| `MsgConnectOK` | `0x04` | CONNECT 建立成功 | Server → Client |
| `MsgData` | `0x05` | TCP 流数据 | 双向 |
| `MsgClose` | `0x06` | 流关闭通知 | 双向 |
| `MsgError` | `0x07` | 错误响应 | Server → Client |

### 透明代理流程

1. Client 把完整 HTTP 请求序列化、压缩后，发送 `MsgTransparent`
2. Server 解压、解析请求、代理访问目标网站
3. Server 把完整 HTTP 响应序列化、压缩后，发送 `MsgResponse`
4. Client 解压、解析响应，返回给浏览器

### CONNECT 代理流程

1. Client 发送 `MsgConnect`，data 为目标 `host:port`
2. Server 建立到目标的 TCP 连接，发送 `MsgConnectOK`
3. Client 向浏览器返回 `HTTP/1.1 200 Connection Established`
4. 双方进入双向流转发模式：
   - 从浏览器/目标服务器读取数据，发送 `MsgData`
   - 收到 `MsgData` 后写给目标服务器/浏览器
   - 连接关闭时发送 `MsgClose`

## 使用示例

### 方法一: 设置系统代理 (透明代理)

在内网机上设置 HTTP_PROXY 环境变量或系统代理:

```bash
export http_proxy=http://127.0.0.1:8081
export https_proxy=http://127.0.0.1:8081
```

### 方法二: 使用 CONNECT 代理

```bash
curl --proxy http://127.0.0.1:8080 https://example.com
```

## 注意事项

1. 确保串口权限正确 (可能需要加入 dialout 用户组)
2. 透明代理只支持 HTTP，HTTPS 需要用 CONNECT 代理
3. 建议使用较高波特率 (115200+) 以获得更好性能
4. 串口带宽有限，大文件传输会比较慢

## 依赖

- github.com/tarm/serial (串口通信)
