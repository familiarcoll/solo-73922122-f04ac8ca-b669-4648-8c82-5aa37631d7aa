# 机构库书目采集（OAI-PMH ListRecords / oai_dc）

一套可实际启动的采集演示：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 采集网页 | http://localhost:8000 | 目录浏览/搜索、**书目校订与冲突解析**、进度、开始/取消/取消并重开、轮次记录 |
| 演示 OAI 源 | http://localhost:8001 | OAI-PMH 端点 `/oai`，控制面板 `/`（慢页、令牌失效、更正/同秒更正、撤回、恢复） |

```bash
docker compose up --build
```

首次启动后打开采集网页，点“开始新一轮”即为首次全量采集（无 `from`，`until` 为源端 `Identify` 返回的当前 UTC 秒）。之后每轮都以“上次成功水位 → 源端当前时刻”的闭区间取数。

## 业务约定与实现

### 时间窗
- 每轮开始时调用 `Identify`，把 `<responseDate>` 规范化为秒级 UTC，作为 `until`。
- `from` 固定取“上次成功提交的水位”；首次全量为 `null`，请求中不带 `from`。
- 两端都是闭区间（OAI `from`/`until` 语义）。边界那一秒会被重复读到，但暂存区
  对同一标识只保留一条、提交写入是幂等的，因此不会产生重复或丢数据。
- 若源端当前时刻早于已发布水位（时钟回拨），本轮直接失败，不构造倒置时间窗。

### 分页与令牌
- 每轮从窗口第 1 页开始，沿 `resumptionToken` 取到没有令牌为止。
- 令牌请求只带 `resumptionToken`，不再带 `from`/`until`，分页严格绑定最初的时间窗。
- 一旦源端返回 `badResumptionToken`（过期/注入失效），或分页途中
  `completeListSize` 发生变化（结果集在翻页时变动会让游标分页漏记录）：
  1. **整次尝试的暂存记录全部删除**（`run_records` 按 `run_id` 清空），
     不会把已取到的旧分页与新结果混在一起；
  2. 用**原来的 `from`/`until`** 从第 1 页重新取；
  3. 最多 `HARVEST_MAX_ATTEMPTS`（默认 3）次；仍失败则整轮失败、不发布。
- 记录时间戳必须落在窗口内；越窗记录会让本轮失败（宁可不发布，也不污染目录）。
  分页中途出现 `noRecordsMatch` 同样按协议异常处理。

### 一条标识一条书目，撤回即删除
- 源端 `header/@status="deleted"` 的记录会从已发布目录中删除对应标识。
- 暂存表主键是 `(run_id, identifier)`，同一轮内重复出现时保留 `datestamp` 更新的
  一条（相同时间戳保留后出现的）。
- 成功提交时对非删除记录做 UPSERT，因此“同一标识只保留一条”在目录表层面也成立。

### 提交的原子性
- `commit_run()` 在**一个 `BEGIN IMMEDIATE` 事务**里：校验本轮仍是活动轮次 →
  把暂存记录写入/删除 `catalog` → 更新 `published_state.watermark` → 标记本轮成功
  → 清理暂存。任何一步抛错都会 `ROLLBACK`，目录、水位、成功计数都不变。
- 失败、取消、进程崩溃都不会改变已发布内容：
  - 失败/取消：`finish_failed` / `finish_cancelled` 只改本轮状态并清空暂存；
  - 进程崩溃：下次启动时 `Store` 把所有残留的 `running` 轮次标记为失败并清空暂存，
    同时发布状态完全不动。

### 取消：不等待慢页
- `POST /api/harvest/cancel`：先在 SQLite 里把该轮置为 `cancelled` 并清空暂存（本地事务，
  毫秒级），再 `task.cancel()` 让正在等待的 HTTP 读取立刻放弃——**不等待慢页返回**。
- `POST /api/harvest/restart`（网页上的“取消并立即重开”）：完成上述取消后立即创建
  带新 `run_id` 的新轮次，无需等旧任务退出。
- 旧轮即便迟到：所有写入都以 `run_id` 为键；提交事务会再次校验
  `active_run_id == run_id` 且状态仍为 `running`，否则直接 `ROLLBACK` 拒绝发布，
  更不会触碰新轮次的状态/进度。
- 数据库层还有 `WHERE status='running'` 的唯一索引，跨进程也最多一个活动轮次。

### 书目校订与源端更正合并
采集网页顶部“书目校订”（`/revisions`）提供逐字段校订，`/api/revisions` 提供同样的数据。

- **源值单独保存**：`catalog.source_title / source_creator` 只由采集写入；
  `revisions` 表保存馆员校订及其“校订依据”（做校订时看到的源值）。
  刚采集完成时，目录里的值就是初始源值，因此既有目录无需迁移即有源值。
- **逐字段三路合并**（每次成功提交，在一个事务内完成）：
  | 情况 | 结果 |
  | --- | --- |
  | 该字段没有校订 | 采用最新源值 |
  | 只有校订改动（源值 = 校订依据） | 采用校订值 |
  | 只有源端改动（校订值 = 校订依据） | 采用源值 |
  | 两侧改成相同值 | 直接合并为该校订值，无冲突 |
  | 两侧改成不同值 | 保留校订值并标记冲突，等待人工选择 |
  另一个字段、书目的其他信息（如 `source_datestamp`）照常更新。
- **冲突解析**：编辑页对每个冲突字段给出“采用源值 / 采用校订值”单选；
  “放弃校订并全部采用源值”可一次清空该校订。
- **提交前校验**：表单携带源值版本 `source_version`、校订版本 `revision.version`
  以及打开表单时的源值。任一变化都会**整次拒绝**提交（HTTP 409），
  返回并展示新的源值/校订值差异，不做部分写入。
- **同秒更正**：OAI `datestamp` 只到秒，因此快照版本号由**内容变化**推进，
  而不是由时间戳推进：同一秒内源端内容变了，版本也会变，旧表单同样被拒绝。
  演示源更正时把 datestamp 填为 `last`（上一采集窗口的上界），即可复现
  “时间戳不变、内容改变”的更正。
- **采集中仍可校订**：校订提交与采集提交都在 `BEGIN IMMEDIATE` 事务里；
  采集发布时不覆盖期间已提交的校订，而是按上述规则合并。
- **闭区间重复读**：窗口下界总会被重复读到，但内容未变时源值版本、校订版本、
  冲突标记都不动，也不会产生新冲突。
- **撤回与恢复**：撤回记录会在提交事务里删除目录行，`revisions` 以
  `ON DELETE CASCADE` 一并废止（含尚未被下一次发布合并的在途校订）；
  恢复后按新源值重建，不复用旧校订。
- **失败/取消**：只清空暂存；源值、合并结果、冲突、水位、校订都不变。
  校订与冲突保存在同一个 SQLite 库里，重启后保留。

### 进度可见性
- 网页每秒轮询 `/api/status`：阶段、时间窗、已取页数、已取记录 / `completeListSize`、
  令牌失效重取次数、当前尝试次数、已暂存唯一标识数、最近错误。
- 前端轮询用“代号”防止旧响应覆盖新渲染；轮次开始/结束时整页刷新，使目录和水位
  立即反映已发布结果。

## 演示源的能力
- `Identify` / `ListRecords`（`metadataPrefix=oai_dc`）/ `ListMetadataFormats`；
- 秒级 UTC `datestamp`，闭区间 `from`/`until`，`resumptionToken` 分页；
- 控制面板（`http://localhost:8001/`）：
  - **慢页**：`slow_seconds`，每个响应延迟 N 秒（取消演示）；
  - **令牌失效**：`token_failures`，接下来 N 次令牌调用返回
    `badResumptionToken`，并使所有未用令牌作废（重取演示）；
  - 分页大小、令牌 TTL（设为 0 可让令牌立即过期）；
  - 新增、更正（标题/作者，更新 `datestamp`；也可手工填写 datestamp，
  保持原时间以演示“同秒更正”）、撤回（`status="deleted"`）、恢复、撤回全部。
- 演示数据保存在独立 SQLite 卷 `demo-data`，采集网页保存在 `harvester-data`。

## 配置（docker-compose.yml）
| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `OAI_URL` | `http://demo-oai:8001/oai` | 源端地址 |
| `OAI_READ_TIMEOUT` | `20` | 单页读取超时（秒） |
| `HARVEST_MAX_ATTEMPTS` | `3` | 令牌失效后最多重取次数 |
| `HARVEST_RETRY_BACKOFF_MS` | `300` | 重取前等待 |
| `DEMO_PAGE_SIZE` | `3` | 演示源初始分页大小 |
| `APP_PORT` / `DEMO_OAI_PORT` | `8000` / `8001` | 宿主机端口 |

## 本地开发与测试
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

DATA_DIR=./data uvicorn demo_oai.main:app --port 8001
DATA_DIR=./data OAI_URL=http://127.0.0.1:8001/oai uvicorn app.main:app --port 8000

pytest -q
```
测试使用 `httpx.ASGITransport` 在同一进程内调用真实的演示 FastAPI 应用，
覆盖 XML 解析、时间窗、分页/令牌失效重取、撤回、原子提交、取消/重开、
迟到响应、进程崩溃恢复，以及校订合并的全部五种情况、冲突解析、同秒更正、
采集中校订不被覆盖、撤回废止校订/恢复不沿用、失败/取消不改校订与冲突、
重启后校订与冲突保留、网页与 JSON 接口。
