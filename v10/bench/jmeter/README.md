# V10 JMeter WebSocket 压测

这个目录保存最小的 JMeter WebSocket 测试计划。JMeter 负责连接、发送和读取采样；大量长连接和广播覆盖率仍使用 `v10/cmd/benchmark` 验证。

## 环境

- JMeter 5.6.3
- WebSocket Samplers 1.3.2
- Java 21

下载插件：

```bash
mkdir -p v10/bench/jmeter/lib/ext
curl -fL -o v10/bench/jmeter/lib/ext/jmeter-websocket-samplers-1.3.2.jar \
  https://repo1.maven.org/maven2/net/luminis/jmeter/jmeter-websocket-samplers/1.3.2/jmeter-websocket-samplers-1.3.2.jar
```

## 运行

先启动不依赖中间件的 V10：

```bash
go run ./v10/cmd/server -port=18081 -redis=false -kafka=false
```

然后使用命令行运行 JMeter。关闭本机 Java 代理，避免 WebSocket 连接被送往代理端口：

```bash
JVM_ARGS='-Dhttp.proxyHost= -Dhttp.proxyPort=0 -Dhttps.proxyHost= -Dhttps.proxyPort=0 -DsocksProxyHost= -DsocksProxyPort=0' \
jmeter -Juser.classpath=v10/bench/jmeter/lib/ext/jmeter-websocket-samplers-1.3.2.jar \
  -Jthreads=20 -Jramp_seconds=2 -Jrun_id=local-smoke \
  -n -t v10/bench/jmeter/smoke.jmx \
  -l results/jmeter.jtl -e -o results/jmeter-report
```

可通过参数覆盖：

- `threads`：连接数量，默认 20
- `ramp_seconds`：连接爬升时间，默认 2 秒
- `host`：服务地址，默认 `127.0.0.1`
- `port`：服务端口，默认 `18081`
- `room`：房间号，默认 `jmeter-smoke`
- `run_id`：压测批次标识

正式压测使用命令行模式，不使用图形监听器，以减少压测端自身开销。

JMeter 的 `Read Broadcast` P50/P95/P99 表示读取采样耗时，不等于消息从发送端到其他客户端的完整端到端延迟。需要业务级延迟时，使用 Go benchmark：

```bash
go run ./v10/cmd/server -port=18088 -redis=false -kafka=false
go run ./v10/cmd/benchmark \
  -port=18088 \
  -clients=100 \
  -active=1 \
  -interval=300ms \
  -duration=10s \
  -room=latency-room
```

benchmark 会在每条压测消息中写入发送时间，并在收到广播时输出 `latency_p50`、`latency_p95` 和 `latency_p99`。该字段只用于压测观测，不会写入 MySQL。
