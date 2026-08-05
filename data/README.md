# AI 洞察数据复现

V12 的真实弹幕回放使用 [DanmakuTPP-Events](https://huggingface.co/datasets/FRENKIE-CHIANG/DanmakuTPP) 的文本与时间戳字段。该数据集需要在 Hugging Face 页面接受访问条件后自行下载，因此本仓库不提交原始数据及其清洗产物。

## 本地目录

```text
data/
├── raw/DanmakuTPP/extracted/    # 上游解压后的 JSON 文件
└── processed/                  # 本项目生成的 JSONL 回放文件
```

`raw/` 和 `processed/` 均被 Git 忽略，避免公开转存上游数据或其衍生文本。

## 生成回放数据

完成上游数据下载并解压到 `data/raw/DanmakuTPP/extracted/` 后，在仓库根目录执行：

```bash
cd v11/insight
go run ./cmd/datasetclean
```

该命令会选择 10 段源视频，每段均匀抽取 500 条有效弹幕，生成 `data/processed/danmaku-tpp-10rooms-500.jsonl`。清洗规则包括移除空白、无效 UTF-8、超长文本、URL 和包含 `@` 的文本；上游不提供用户身份，回放中的用户标识为本地匿名映射。

详细的测试边界和结果见 `docs/benchmark/v12-ai-insight-load-report-2026-08-05.md`。使用数据集时请遵循上游页面列出的访问条件并引用其论文。
