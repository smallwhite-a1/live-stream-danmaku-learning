import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { MetricSample } from "../metrics/types";

interface MetricChartProps {
  samples: readonly MetricSample[];
}

function formatTime(timestamp: number): string {
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    hour12: false,
    minute: "2-digit",
    second: "2-digit",
  }).format(timestamp);
}

export function MetricChart({ samples }: MetricChartProps) {
  if (samples.length < 2) {
    return (
      <div className="metric-chart__collecting">
        <span className="signal-pulse" aria-hidden="true" />
        正在收集趋势数据
      </div>
    );
  }

  const data = samples.map((sample) => ({
    time: formatTime(sample.sampledAt),
    投递: Number(sample.deliveredPerSecond.toFixed(2)),
    限流: Number(sample.limitedPerSecond.toFixed(2)),
    丢弃: Number(sample.droppedPerSecond.toFixed(2)),
  }));

  return (
    <>
      <div
        aria-label="最近 60 秒消息投递、限流与丢弃趋势"
        className="metric-chart"
        role="img"
      >
        <ResponsiveContainer height={270} width="100%">
          <LineChart data={data} margin={{ bottom: 0, left: -22, right: 8, top: 8 }}>
            <CartesianGrid stroke="#e2e6e8" strokeDasharray="3 3" vertical={false} />
            <XAxis
              axisLine={false}
              dataKey="time"
              minTickGap={28}
              tick={{ fill: "#737b83", fontSize: 10 }}
              tickLine={false}
            />
            <YAxis
              axisLine={false}
              tick={{ fill: "#737b83", fontSize: 10 }}
              tickLine={false}
              width={42}
            />
            <Tooltip
              contentStyle={{
                border: "1px solid #bcc4c9",
                borderRadius: 4,
                boxShadow: "0 8px 24px rgb(22 25 28 / 10%)",
                fontSize: 12,
              }}
            />
            <Legend iconType="line" wrapperStyle={{ fontSize: 11 }} />
            <Line
              dataKey="投递"
              dot={false}
              isAnimationActive={false}
              stroke="#16875d"
              strokeWidth={2}
              type="monotone"
            />
            <Line
              dataKey="限流"
              dot={false}
              isAnimationActive={false}
              stroke="#b26a00"
              strokeWidth={2}
              type="monotone"
            />
            <Line
              dataKey="丢弃"
              dot={false}
              isAnimationActive={false}
              stroke="#d8232a"
              strokeWidth={2}
              type="monotone"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <table aria-label="最近 60 秒趋势数据" className="sr-only">
        <thead>
          <tr>
            <th scope="col">时间</th>
            <th scope="col">投递/秒</th>
            <th scope="col">限流/秒</th>
            <th scope="col">丢弃/秒</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row, index) => (
            <tr key={`${row.time}-${index}`}>
              <th scope="row">{row.time}</th>
              <td>{row.投递}</td>
              <td>{row.限流}</td>
              <td>{row.丢弃}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
