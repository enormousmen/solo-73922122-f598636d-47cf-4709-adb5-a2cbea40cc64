# 本地 MapReduce：按路由汇总 5xx

一个用 FastAPI 实现的本地 MapReduce 服务。值班员把多份访问日志分片（路由 + HTTP 状态码）发给它，服务把 5xx 计数按路由汇总，并把每条路由确定性地归入两个 reduce 分区之一。

核心目标不是"能算出来"，而是"中间文件坏掉时还能算出正确的总数，且绝不重复入账"。

- `app/` — 服务实现
- `tests/` — pytest 测试（28 个）
- `examples/demo.py` — 端到端演示脚本（仅用标准库）
- `Dockerfile` / `docker-compose.yml` — 容器启动
- `Dockerfile.test` — 容器内跑测试（可选）

## 目录

- [快速开始](#快速开始)
- [API](#api)
- [设计](#设计)
  - [分区规则](#分区规则)
  - [中间文件格式](#中间文件格式)
  - [故障恢复流程](#故障恢复流程)
  - [单飞补算](#单飞补算)
  - [不重复入账](#不重复入账)
  - [内存占用](#内存占用)
  - [整批失败语义](#整批失败语义)
- [演示](#演示)
- [测试](#测试)
- [已知限制与未完成项](#已知限制与未完成项)

## 快速开始

### Docker Compose

```bash
docker compose up --build -d
curl -s http://localhost:8000/health
# {"status":"ok","data_dir":"/srv/data"}

# 运行演示（损坏中间文件 -> 自动补算 -> 总数一致）
python3 examples/demo.py --url http://localhost:8000

docker compose down -v
```

`docker-compose.yml` 里默认设置：

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `MAPREDUCE_DATA_DIR` | `/srv/data` | 输入分片、中间文件、job 元数据的根目录（命名卷） |
| `MAPREDUCE_REMAP_ATTEMPTS` | `1` | 每个分片最多补算几次；`1` 表示"补算一次，再读仍失败则整批失败" |
| `MAPREDUCE_REMAP_DELAY_SECONDS` | `0` | 人为拉长补算窗口，便于演示并发争抢 |
| `MAPREDUCE_ENABLE_DEBUG_ENDPOINTS` | `1` | 演示用故障注入端点；生产请设为 `0` |

> 注意：故障注入端点会直接改写磁盘上的中间文件，只在演示/测试时开启。

### 本地（不用 Docker）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --port 8000
```

生产镜像只安装 `requirements.txt`（运行期依赖）。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `POST` | `/jobs` | 提交一批日志分片，同步完成 map + reduce，返回按路由的总数 |
| `GET` | `/jobs/{job_id}` | 查询 job 状态（阶段、补算派发次数、错误） |
| `POST` | `/jobs/{job_id}/reduce` | 用已落盘的输入重新执行 reduce（可触发补算），返回总数 |
| `POST` | `/jobs/{job_id}/simulate-fault` | **仅调试**：截断尾部 / 删除中间文件 / 删除输入 |
| `GET` | `/jobs/{job_id}/intermediates` | **仅调试**：对比中间文件大小与 manifest |

### `POST /jobs`

```bash
curl -s -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "shards": [
      {"shard_id": "s1", "entries": [
        {"route": "/api/orders", "status": 500},
        {"route": "/api/orders", "status": 502},
        {"route": "/login", "status": 503}
      ]},
      {"shard_id": "s2", "entries": [
        {"route": "/api/orders", "status": 504},
        {"route": "/login", "status": 200}
      ]}
    ]
  }'
```

响应 `201`（真实运行输出）：

```json
{
  "job_id": "4fdf9d2716754ebaa9ab8280588c6305",
  "shards_mapped": 2,
  "partitions_merged": 2,
  "total_5xx": 4,
  "routes": [
    {"route": "/api/orders", "count": 3},
    {"route": "/login", "count": 1}
  ],
  "recovered": {},
  "remap_dispatches": {},
  "read_attempts": 4
}
```

- `recovered`：哪个分片的哪个分区在归并前触发了补算
- `remap_dispatches`：每个分片实际派发了几次 map 任务（正常为 0；故障恢复时为 1）
- `read_attempts`：两个分区读取中间文件的总尝试次数

### `POST /jobs/{job_id}/simulate-fault`

```json
{
  "shard_id": "s1",
  "mode": "truncate_tail",
  "partitions": [0, 1],
  "drop_records": 1,
  "delete_input": false
}
```

- `mode`：`truncate_tail`（从尾部去掉 N 条**完整**记录并删掉 trailer，模拟"末尾少了几条完整记录"）或 `delete_file`（删除中间文件）
- `delete_input: true` 同时删除持久化输入，让补算必定失败，用于验证整批失败语义

### 错误响应

整批失败时返回 `500`，**不包含** `routes`/`total_5xx`：

```json
{
  "detail": "batch failed; no aggregate is available",
  "job_id": "4fdf9d2716754ebaa9ab8280588c6305",
  "failures": [
    {
      "partition": "p0",
      "stage": "reduce",
      "shard_id": "s1",
      "reason": "cannot recover shard 's1': re-map of shard 's1' failed: input for shard 's1' is unreadable: [Errno 2] No such file or directory: '.../input/s1.json'",
      "attempts": 1
    }
  ],
  "totals_returned": false
}
```

请求校验失败（比如状态码不是 100–599、`shard_id` 重复）返回 FastAPI 标准的 `422`。

## 设计

```
POST /jobs
  │
  ├─ 持久化输入      data/jobs/<job>/input/<shard>.json
  │
  ├─ Map（并发）     每个分片 -> 两个中间文件 p0.jsonl / p1.jsonl + manifest.json
  │
  └─ Reduce（两分区并行）
        p0 任务 ── 逐分片流式读取 p0.jsonl ──┐
        p1 任务 ── 逐分片流式读取 p1.jsonl ──┴─> 合并 -> 按路由总数
                     │
                     └─ 读失败（截断/缺失/摘要不符）
                          -> 单飞补算该分片
                          -> 重读新的 generation
```

### 分区规则

`app/partitioning.py` 用 `hashlib.blake2b(route, person=b"mr5xx-prt") % 2`。不用内置 `hash()`，因为它在不同进程 / 不同 `PYTHONHASHSEED` 下不稳定，会让已落盘的中间文件失效。mapper、读取校验、reducer 使用同一个函数；如果某条路由出现在错误分区，读取会直接判为损坏。

### 中间文件格式

`p0.jsonl` / `p1.jsonl`：

```
{"r":"/api/orders","c":3}
{"_trailer":true,"magic":"mr5xx-intermediate","version":1,"shard_id":"s1","partition":0,"records":1,"body_bytes":30,"body_sha256":"...","total_5xx":3}
```

`manifest.json` 记录 generation（每次 map 递增）、两个文件的字节数、整文件 SHA-256、记录数。写入顺序是：两个数据文件各自原子替换（临时文件 + `os.replace`）→ 最后写 manifest。manifest 是读取方的提交点。

"末尾少了几条完整记录"之所以能发现：少了记录就少了 trailer；即使有人手工把 tail 截在换行处、剩下每行都是合法 JSON，也仍然没有 trailer。trailer 内的 `records`/`body_bytes`/`body_sha256`/`total_5xx` 与读到的内容逐项比对，再与 manifest 的整文件摘要比对。截断在记录中间、空文件、trailer 后还有数据、路由跑错分区、单行超过 64 KiB，都会被判为损坏。

### 故障恢复流程

`app/engine.py::_read_shard`：

1. 读 manifest，拿到 generation N
2. 流式读文件并校验；失败 → 丢弃本地计数（局部 dict 从未并入分区总量）
3. 再读一次 manifest：
   - generation 已大于 N → 说明另一个分区已经补算过，直接重读，不再派发
   - 否则 → 请求单飞补算
4. 补算成功后重读。如果重读仍失败 → 整批失败；如果补算本身失败（如输入被删）→ 整批失败

未受影响的另一个分区始终照常归并；它不会因为对方失败而回滚，但最终结果在任一分区失败时整体不返回。

### 单飞补算

`RemapCoordinator` 以分片为键缓存 `asyncio.Task`：

- 任务在飞 → 后到的请求 `await` 同一个 task
- 任务已完成 → 直接复用缓存结果，不再写盘
- 每个分片每次 job 运行最多派发一次，`remap_dispatches` 可观测

因此两个分区同时读到同一分片的坏文件时，磁盘上的 map 只执行一次。测试用 `threading.Barrier(2)` 证明两个分区的归并确实并发，又用三个并发 `ensure_remapped` 调用证明只派发一次。

### 不重复入账

- 分片计数只有在整文件、trailer、manifest 全部校验通过后才返回；任何失败路径都从空 dict 重新开始
- 补算会同时重写该分片的两个分区文件，并递增 generation；另一个分区即使已经读到旧 generation 的数据，也会在提交前发现 generation 变了，于是重读新文件而不是重复累加
- 重试和补算都不会累加"上次读了一半"的结果

### 内存占用

读取用 `readline()` 逐行流式解析，单行有 64 KiB 上限；内存是 O(唯一路由数)，与中间文件大小无关。测试里给单条路由写了 100,000 行（>1 MB）的文件，`tracemalloc` 峰值 < 文件大小的 1/10 且 < 1 MB。

### 整批失败语义

以下情况返回 `500` 且不返回汇总：

- 补算失败（输入缺失 / 损坏 / 落盘失败）
- 补算成功后重读仍失败（例如补算输出再次损坏）
- 两个分区中任意一个出错（健康分区的中间结果被丢弃）
- 某个分片在读取过程中 generation 反复变化（超过重试上限）

job 状态会落盘为 `failed`，`GET /jobs/{id}` 仍可查询原因。

## 演示

`examples/demo.py` 仅依赖标准库，对运行中的服务执行 4 步并逐一断言：

1. 提交 3 个分片，记录基线总数（本仓库数据为 `total_5xx = 19`）
2. 把 `shard-a` 两个分区文件的尾部各截掉 2 条完整记录（连同 trailer），再列出中间文件与 manifest 的比对结果
3. 重新 reduce：只有 `shard-a` 被补算一次，两个分区都完成归并，总数与基线完全一致
4. 另起一个 job，损坏分片并删除其输入，验证返回 `500`、`totals_returned: false`、没有 `routes`

真实运行输出（本机以 uvicorn 直接运行，`--url http://127.0.0.1:8022`）：

```
== 1. baseline ==
  routes: {"/admin": 1, "/api/orders": 4, "/api/users": 1, "/catalog": 1,
           "/checkout": 5, "/health": 1, "/login": 3, "/metrics": 1, "/search": 2}
  total_5xx: {"total_5xx": 19}

== 2. tear the tail off shard-a's /api/orders partition file ==
  intermediate files: [
    {"matches_manifest": false, "partition": "p0", "shard": "shard-a"},
    {"matches_manifest": false, "partition": "p1", "shard": "shard-a"},
    {"matches_manifest": true,  "partition": "p0", "shard": "shard-b"},
    ... ]

== 3. re-run reduce; shard-a is re-mapped, totals must match ==
  reduce (HTTP 200): {"recovered": {"shard-a": ["p0", "p1"]},
                      "remap_dispatches": {"shard-a": 1},
                      "total_5xx": 19, ...}
  RESULT: totals recovered exactly; one re-map for one shard

== 4. unrecoverable batch: corrupt a shard and delete its input ==
  reduce (HTTP 500): {"detail": "batch failed; no aggregate is available",
                      "totals_returned": false, ...}
  RESULT: batch failed, no aggregate returned

All demo checks passed.
```

## 测试

```bash
python3 -m pytest -q          # 28 passed
ruff check app tests examples # All checks passed
```

覆盖点：

| 测试 | 验证内容 |
| --- | --- |
| `test_storage_recovery.py` | 记录边界截断、记录中间截断、空文件、摘要篡改、缺 trailer、补算恢复、补算失败、补算后仍失败、健康分区不受影响 |
| `test_api.py` | API 契约、分区归属、单分区截断、双分区同时损坏仍只派发一次、重复 reduce 不重复计数、输入被删返回 500 且无 totals、调试端点关闭、未知 job 404、并发单飞 |
| `test_concurrency.py` | 两个分区确实并行归并（`threading.Barrier`） |
| `test_partitioning_and_memory.py` | 分区哈希跨进程稳定；大中间文件流式读取的内存上界 |

## 已知限制与未完成项

- **未在本环境执行 Docker 构建**：当前沙箱没有 `docker`/`docker compose` 可执行文件。Dockerfile、compose 文件已做 YAML 校验，并在本地用 uvicorn 完整跑通了同一套端到端演示；容器内的 `docker compose up` 未实测。
- **单实例**：job 锁与单飞协调器在进程内。多个 uvicorn worker 或副本共享同一数据目录时，`manifest` 的原子替换仍能保证不读到半文件，但"跨进程只派发一次"不成立；要支持需要外部分布式锁。
- **输入整份驻留内存**：map 时 `load_input_entries` 会把一个分片的输入 JSON 整体读入。归并侧已流式，但超大的单个分片仍会占用相应内存。可用 JSONL 输入格式改造成流式解析。
- **`MAPREDUCE_REMAP_ATTEMPTS` 的语义**：当前每次 read 尝试最多补算一次；设为 >1 只有在补算输出再次损坏且 job 重跑时才可能多派发（协调器缓存的是本次 job 的结果）。配置项保留，但常规恢复路径固定为"一次补算 + 一次重读"。
- **临时目录/持久卷清理**：没有过期 job 的 GC；数据会一直留在卷里。
- **调试端点**：故障注入会直接破坏数据，生产必须 `MAPREDUCE_ENABLE_DEBUG_ENDPOINTS=0`（默认关闭）。
- **状态查询的持久化范围**：job 元数据落盘，但进程重启后不会自动续跑未完成的 job；`GET` 仍可读到最后一次状态。
