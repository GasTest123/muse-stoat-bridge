# muse-stoat-bridge

把 [Stoat](https://stoat.chat)（原 Revolt，开源的 Slack / Discord 替代品）接到 Muse：用户在
Stoat 里私信 bot、@ 它，或者回复它发过的消息，Muse 就会被唤醒，读取上下文后回复。

**多频道**：每个 Stoat 频道可以对应 Muse 里一个独立的 side chat。每条路由有自己的 hook、
worker、租约和唤醒状态，所以不同频道可以同时处理，彼此的上下文互不相通。

**图片 / 视频**：worker 可以把生成的图片、视频作为附件发回频道（`stoatctl send --attach`，
走 Stoat 的文件服务 Autumn）。完整的“生成 → 整理格式和大小 → 发送”流程做成了独立的
**stoat-media skill**（`skills/stoat-media/`），部署 Muse 时可以选择安装（§4.4）。

**自动开 side chat**：打开 auto 模式后，新频道第一次 @ bot 或私信 bot 时，会自动获得自己的
side chat，第一条消息也在这个新 side chat 里回复。也可以保持手动，由人决定哪些频道单独开。

工作方式：**常驻桥只负责接收 → 写入持久收件箱 → Muse hook 唤醒 worker → worker 通过持久
发送管线回复**。

依赖用 [uv](https://docs.astral.sh/uv/) 管理，版本锁在 `pyproject.toml` 和 `uv.lock` 里。

## 安装与卸载：对 Muse 说一句话

把下面这句话发给 Muse（换成实际的仓库地址），它会按仓库里的 `AGENTS.md` 完成安装：

```
帮我安装 muse-stoat-bridge，把 Stoat 接入你：<仓库地址>
```

卸载也只需要一句：

```
帮我卸载 muse-stoat-bridge，清理所有文件
```

Muse 会用 `git clone` 把项目下载到 `~/workspace/muse-stoat-bridge`，项目目录就是安装。过程中只有
一处需要你：亲手把 bot token 填进 `.env`。stoat-media skill（画图、做视频）随安装一起装好。卸载后不留文件，
见 §4.5。

## 1. 架构

```
Stoat ──Bonfire WebSocket（推送，毫秒级）──> bridge.py（常驻，只收不发）
                       │ 过滤：私信 / @bot / 回复 bot 的消息 / 白名单 bot 的 @
                       │       / STOAT_LISTEN_CHANNELS 覆盖的频道或服务器里的全部消息（可选）
                       │ 断线或重启后：按每个频道的游标，用 REST 补拉缺失的消息
                       ▼
          data/inbox.jsonl（追加写 + fsync + 完成标记 tombstone）
                       │
          routes.json：频道 → 路由（route）→ Muse side chat
                       │
   ┌───────────────────┼────────────────────┬──────────────────────┐
   ▼                   ▼                    ▼                      ▼
hook stoat-general   hook stoat-dm-alice   hook stoat-random    hook stoat-inbox（兜底）
   │（各自每 ~5s 被调度一次，每次最多盯 25s；有消息立即 wake）
   ▼                   ▼                    ▼                      ▼
side chat #general   side chat dm-alice    side chat #random    side chat 收件箱
   worker：stoatctl --route X peek → history → 决定回复 → send / no-reply → warm-wait
                       ▼
stoatctl send ──POST /channels/{id}/messages（带 Idempotency-Key）──> Stoat
   └ --attach 文件 ──先 POST {autumn}/attachments 上传，拿到 id 放进消息的 attachments
```

Stoat 协议的几个特点，以及本项目的对应做法：

| Stoat（Bonfire）的特点 | 本项目的做法 |
|---|---|
| 事件**没有确认和重投**，断线期间的消息直接丢失 | 每个频道记一个游标；连上后收到 Ready 时，用 `last_message_id` 和游标比较，再用 `GET …/messages?after=` 补拉 |
| bot **会收到所有事件**，没有“只订阅 @ 和私信”的选项 | `src/filters.py` 自行判断哪些消息是发给 bot 的 |
| 消息没有自定义 metadata | 发送时带 `Idempotency-Key` 请求头，服务端会把它存为消息的 `nonce`；结果不确定时在历史里查找该 nonce |

## 2. 目录结构

```
muse-stoat-bridge/
│  ── 入口 ──
├── install.sh           ★ 安装 / 修复，写安装列表 install-manifest.txt（§4.2）
├── uninstall.sh         ★ 按安装列表彻底卸载（§4.5）
├── bridge.py            ★ 常驻接收桥：Bonfire、过滤、落盘、游标、补拉、频道目录、断线重连
├── stoatctl.py          ★ worker / hook / 运维共用的命令行（§7）
│
│  ── 实现：入口程序引用的模块，不直接运行 ──
├── src/
│   ├── config.py        配置：.env / 环境变量，数据目录等路径（以项目根目录为基准）
│   ├── routes.py        频道 → 路由 → side chat 的映射，以及按路由生成 hook
│   ├── delivery.py      持久发送状态机 + 退出码定义
│   ├── filters.py       哪些消息要入队（纯函数）
│   ├── inbox_store.py   收件箱队列
│   ├── send_state.py    发送状态（文件损坏即拒绝发送）
│   ├── stoat_api.py     REST 客户端 + Autumn 上传（只用标准库）
│   ├── media.py         附件检查：按文件头识别图片 / 视频，大小与数量上限
│   └── ulidutil.py      ULID 与时间戳互转
│
│  ── 交给 Muse 的东西 ──
├── hook/
│   ├── stoat-watch.sh   ★ Muse hook 脚本（按 STOAT_ROUTE 只处理一条路由）
│   ├── WORKER.md        ★ hook 指令：worker 被唤醒后怎么做（交给 Muse 的约定）
│   ├── ADMIN.md         ★ admin hook 指令：为新频道注册 hook、开 side chat（auto 模式）
│   ├── hook.example.json    单路由模式的 hook 定义示例
│   └── routes/          `stoatctl routes hooks --write` 生成的每条路由一个 hook（不入库）
├── skills/
│   ├── install.sh       把 skill 以软链接装进 ~/workspace/custom-skills（§4.4）
│   └── stoat-media/     ★ 独立 skill：SKILL.md + scripts/stoat_media.py（prepare / demo）
├── AGENTS.md            给 Muse 看的安装 / 卸载步骤
│
│  ── 测试 ──
├── tests/               离线测试（REST 测试替身在 tests/fakes.py）+ 可选的真实服务器测试
│
│  ── 配置与运行数据 ──
├── .env.example         复制成 .env 填 token（.env 不入库）
├── pyproject.toml / uv.lock / .python-version     uv 项目定义（只用于建环境，不打包）
└── data/                bridge 的正式运行数据（不入库；STOAT_DATA_DIR 可改）
```

## 3. 快速上手：先单独跑 bridge

不接 Muse 也能验证整条收发链路：bridge 把发给 bot 的消息写进收件箱，你用 `stoatctl` 手动
做 worker 要做的事。

1. 安装 [uv](https://docs.astral.sh/uv/)（会自动准备 Python 3.12），同步依赖并填写 `.env`
   （至少填 `STOAT_BOT_TOKEN`，bot 的建法见 §4.1）：

```bash
uv sync && cp .env.example .env && chmod 600 .env
```

2. 确认 token 可用（能打印出 `bot_user_id`）：

```bash
uv run stoatctl.py whoami
```

3. 前台启动 bridge，看到 `ready: …` 后 bot 就在线了：

```bash
uv run bridge.py
```

4. 在 Stoat 里私信 bot，或在频道里 @ 它。另开一个终端查看收件箱，每行一条待处理消息：

```bash
uv run stoatctl.py peek
```

5. 用其中的 `msg_id` 手动回复，回复会挂在原消息下，这条消息随即从收件箱里完成：

```bash
printf '收到，这是手动回复' | uv run stoatctl.py send <msg_id>
```

哪些消息会入队：

| 消息 | 是否入队 |
|---|---|
| 私信 bot | 是，`kind=dm` |
| 频道里 @ bot | 是，`kind=mention` |
| 回复 bot 发过的消息 | 是，`kind=reply` |
| 频道里没有 @ 的普通消息 | 默认否；频道在 `STOAT_LISTEN_CHANNELS` 里时入队，`kind=channel`，worker 按 `STOAT_LISTEN_REPLY` 决定回不回 |
| 其他 bot 的消息 | 只有 @ 了本 bot 才入队（设了 `STOAT_ALLOWED_BOTS` 时还要在白名单里） |
| bot 自己、系统消息、webhook | 否 |

接上 Muse 之后，这些手动步骤由 hook 唤醒的 worker 按 `hook/WORKER.md` 完成（§4.3）。
响应丢失、409、429、5xx 这类故障，放在离线测试里覆盖（§9）。

## 4. 部署到真实 Stoat + Muse

### 4.1 Stoat 侧

1. 在 Stoat 客户端进入 **Settings → My Bots → Create a bot**，复制 token。
2. 用邀请链接把 bot 拉进服务器，确认它在目标频道有 *View Channel / Read Message History /
   Send Messages* 权限。
3. 如果是自建实例，用 `curl https://<实例>/api/` 确认返回里有 `ws` 字段，再把 API 地址填到
   `STOAT_API_URL`。

### 4.2 安装桥

在 Muse 里，项目用 `git clone` 下载到 `~/workspace/muse-stoat-bridge`，**项目目录就是安装**，
`install.sh` 在原地完成安装，不拷贝文件，也不往别处放东西：

```bash
git clone <仓库地址> ~/workspace/muse-stoat-bridge && ~/workspace/muse-stoat-bridge/install.sh
```

`install.sh` 只在项目目录里建东西，重复运行是安全的（用来修复或升级后重建环境）：

| 项目目录里的 | 内容 |
|---|---|
| `.venv/` | Python 环境（`uv sync --frozen`） |
| `.uv/` | uv 的缓存和它下载的 Python；机器上没有 uv 时，uv 本身也装在 `.uv/bin/`（不改 PATH、不写 shell 配置） |
| `.env` | 配置和 bot token，从 `.env.example` 复制，权限 600；安装时加上 `STOAT_AUTOSTART=1` |
| `data/` | 收件箱、游标、发送状态、hook 状态、生成的媒体文件、`bridge.log` |
| `routes.json`、`hook/routes/` | 配置路由后生成（§4.3） |
| `install-manifest.txt` | 安装列表，`uninstall.sh` 按它清理 |

所有输出文件（日志、数据、生成的媒体、hook 的临时文件、uv 缓存）都在项目目录里。

填好 `.env` 里的 `STOAT_BOT_TOKEN` 后，检查 token 并启动 bridge：

```bash
cd ~/workspace/muse-stoat-bridge && .venv/bin/python stoatctl.py whoami
```

```bash
cd ~/workspace/muse-stoat-bridge && .venv/bin/python stoatctl.py bridge start
```

- `bridge start | stop | status` 在后台运行 bridge，日志写到 `data/bridge.log`；同一个数据目录
  只会有一个 bridge（`data/bridge.lock`）。
- **常驻**：`STOAT_AUTOSTART=1` 时，每次 hook 运行都会检查 bridge，没在运行就把它拉起来（最多
  每 30 秒一次），不需要额外的服务文件。
  两者同时开着也没关系，单实例锁保证只有一个 bridge。

确认状态正常（输出中 `"healthy": true`）：

```bash
cd ~/workspace/muse-stoat-bridge && .venv/bin/python stoatctl.py status
```

首次启动时，每个频道的游标从“当前最新消息”开始，不会把历史消息当成新消息补进来。bridge
会把它看到的频道写进 `data/channel_directory.json`，供下一步的路由命令使用。

开发时也可以不安装，直接在仓库里 `uv sync` 后用 `uv run stoatctl.py …` / `uv run bridge.py`（§3）。

### 4.3 配置路由并注册 Muse hook

有两种做法，详细说明见 §5：

- **自动（推荐）**：开启 auto 模式，只注册 `stoat-admin` 和 `stoat-inbox` 两个 hook，之后
  每个频道第一次和 bot 说话时，会自动注册自己的 hook、开出自己的 side chat（§5.4）。
- **手动**：按下面的步骤一次性给已知频道建好路由，以后有新频道再手动添加。

手动做法的最短流程：

```bash
uv run stoatctl.py routes discover        # 列出 bridge 看到的频道，以及每个频道当前去哪条路由
```

```bash
uv run stoatctl.py routes add-each        # 给每个还没有路由的频道建一条（加 --no-dms 则私信留在兜底路由）
```

```bash
uv run stoatctl.py routes hooks --write   # 每条路由生成一个 hook 脚本，并汇总到 hook/routes/hooks.json
```

然后**每条路由在 Muse 里注册一个 hook**，字段直接取自 `hooks.json` 的对应条目：

| 字段 | 取值 |
|---|---|
| script | `hook/routes/<route>.sh`（生成的包装脚本，已设好 `STOAT_ROUTE`） |
| poll_interval_secs / script_timeout_secs | 5 / 60（`script_timeout_secs` 必须大于 `STOAT_HOOK_WINDOW` 再加 30） |
| instructions | `hook/WORKER.md` 全文 |
| 投递位置 | `deliver_to` 里写的那个 side chat，每条路由各用一个 |

验收步骤：

1. 每个 hook 都运行一次 `hooks.dry_run`，返回结果应为 `silent: no new messages`。
2. 在不同频道各发一条消息，确认每个 side chat 只处理自己频道的消息，并且回复挂在原消息下。
3. `uv run stoatctl.py status` 显示每条路由的 `pending` 都是 0，`sends: {}`。

> 关于 Muse hook 运行时：`wake` / `silent` / `HATCH_HOOK_RESULT` 的行为来自第三方逆向资料，
> 并非 Meta 官方文档；Muse `hooks.add` 的真实字段名也没有公开。上线前请用 `hooks.dry_run` 和
> 真实账号确认一遍。

### 4.4 stoat-media skill（图片 / 视频）

桥本身支持附件（`send --attach`）；skill 教 Muse 一整套做法：先回进度、长时间生成时占住租约、
按 Stoat 的限制转换和压缩、失败时也把分段发完。`install.sh` 默认就会装好它：

- 装在 Muse 加载用户安装的技能的目录：`~/workspace/custom-skills/stoat-media` 是指向项目里
  `skills/stoat-media` 的**软链接**，不拷贝文件；更新项目，skill 跟着更新。这条链接记进安装列表，
  卸载时只删链接本身。它是项目目录外**唯一**的东西。
- 别的 agent 用其他技能目录时，运行 `install.sh --skills-dir <目录>`。
- 依赖：`python3`；转码和压缩视频需要 `ffmpeg` / `ffprobe`（macOS 上压缩图片也可以用自带的
  `sips`），安装时会检查并提示。
- skill 生成的文件放在 `data/media/` 下（`stoatctl media-limits` 的 `work_dir`），不进 /tmp。
- 装上后不用改 hook：WORKER.md 已经写明“装了 stoat-media 就按它做”。生成能力用 Muse 自己的；
  Muse 不能生成视频时，skill 会让它照实说明，不会拿占位文件冒充。
- `STOAT_ATTACH_TYPES`（默认 `image,video`）控制允许上传的种类。桥按**文件头**判断类型，
  `.env`、日志、源码这类文本文件不管叫什么名字都传不上去。
- 不经过安装脚本时，也可以单独用 `skills/install.sh` 链接（默认同样是 `~/workspace/custom-skills`，
  `--dest` 可改）、`--uninstall` 卸载；这样装的链接不在安装列表里，需要自己卸载。

### 4.5 卸载

先在 Muse 里删掉 hook，再运行卸载脚本。顺序不能反：hook 是 Muse 的登记，脚本删不到；
安装目录删掉后，还登记着的 hook 每次运行都会报错。

```bash
~/workspace/muse-stoat-bridge/uninstall.sh
```

不带参数只打印计划，不删任何东西：要删的 Muse hook（所有 `script_path` 在项目目录下的
hook，并列出这次安装用到的 id）、安装列表里的目录外路径、项目目录本身。确认 hook 已经删掉后：

```bash
~/workspace/muse-stoat-bridge/uninstall.sh --yes
```

它依次：

1. 留下 `.uninstalling` 标记，让 hook 不再拉起 bridge；
2. 停掉 bridge，等所有从项目目录运行的 `bridge.py` / `stoatctl.py` 进程退出（超时就结束它们）；
3. 删除安装列表里的目录外路径（skill 软链接只删链接，不碰它指向的文件）；
4. 删除整个项目目录（包括卸载脚本自己和安装列表）。

卸载后不留任何文件。完整清单：

| 安装时可能出现的东西 | 位置 | 卸载 |
|---|---|---|
| 代码、`.venv`、`.uv`、`.env`、`data/`、日志、`routes.json`、`hook/routes/`、安装列表 | `~/workspace/muse-stoat-bridge/` | 整个目录删除 |
| stoat-media skill（软链接） | `~/workspace/custom-skills/stoat-media` | 按安装列表删除链接 |
| Muse hook | Muse 里的登记，不是文件 | 先用 `hooks.remove` 删除 |
| side chat | Muse 里的对话记录，不是文件 | 由用户决定是否删除 |

bridge 运行时不会往项目目录以外写文件：hook 脚本的临时输出在 `data/` 下，媒体文件在
`data/media/` 下，uv 的缓存和 Python 在 `.uv/` 下。

## 5. 多频道：频道 ↔ side chat

### 5.1 routes.json

文件位置由 `STOAT_ROUTES_FILE` 指定，默认是 `<bridge 目录>/routes.json`：

```json
{
  "default_route": "inbox",
  "alerts_route": "inbox",
  "routes": {
    "inbox":   {"side_chat": "Stoat · 收件箱", "notes": "兜底：没有专属 side chat 的频道"},
    "general": {"side_chat": "Stoat · #general", "match": ["01J...频道ID"],
                "notes": "工程讨论频道，回复要技术化"},
    "dms":     {"side_chat": "Stoat · 私信", "match": ["type:DirectMessage"]},
    "team-x":  {"side_chat": "Stoat · Team X", "match": ["server:01J...服务器ID"]}
  }
}
```

- `match` 中的每一项称为选择器，有三种写法：**频道 ID**、`server:<服务器 ID>`、
  `type:<DirectMessage|Group|TextChannel>`。
- 一条消息归属哪条路由，按以下顺序查找，命中即停：频道 ID → 服务器 → 频道类型 →
  `default_route`。因此每条消息恰好落到一条路由，不会被丢弃。
- 同一个选择器只能属于一条路由，冲突时会直接报错。
- `notes` 会作为 `route_notes` 放进唤醒载荷，用来给这个 side chat 定制回复风格。
- `alerts_route`：只有这一条路由会收到 `bridge_down` / `bridge_recovered` 告警，避免 N 个频道
  重复报警。
- 没有 routes.json 时只有一条 `default` 路由，即单 hook、单 side chat 的原始模式。
- 路由是在**消费时**解析的。修改 routes.json 后不需要重启 bridge，已经排队的消息也会按新
  映射处理；正在等待中的 hook 运行检测到文件变化后会重新加载，不会出现两个 hook 同时被唤醒。

### 5.2 每条路由彼此隔离的部分

| | 实现 |
|---|---|
| 队列视图 | `peek`、`warm-wait`、`hook-poll` 只看本路由的消息 |
| 写操作 | 对其他路由的消息或频道执行 `send` / `no-reply` / `history` / `typing`，返回 `wrong-route`，退出码 4 |
| 租约 | `worker_lease.<route>.json`：A 频道的 worker 在忙，不影响 B 频道被唤醒 |
| 唤醒状态 | `hook_state.<route>.json`：冷却时间、唤醒次数上限、stuck 消息都按路由分别统计 |
| hook 锁 | `hook.<route>.lock` |
| 并发 | 不同路由的 worker 并行；同一路由内同一时刻只有一个 worker（Muse 也是每个 hook 一个 worker） |
| 共享部分 | 收件箱、发送状态、bridge 都是全局唯一的，按文件锁协调 |

### 5.3 出现了新频道怎么办

1. bridge 收到 `ChannelCreate`，或启动时读到新频道，会更新频道目录。
2. 新频道的消息先进入**兜底路由**（收件箱）。那里的 worker 正常回复，并在载荷的 `unrouted`
   字段里看到“这个频道还没有专属 side chat”，于是在收件箱 side chat 里附上建议命令，例如
   `stoatctl routes add dev <id>`。
3. 主人决定给它单独开 side chat 时，依次执行：

```bash
uv run stoatctl.py routes add dev <channel_id> --side-chat "Stoat · #dev" --notes "研发频道"
```

```bash
uv run stoatctl.py routes hooks --write
```

   然后在 Muse 里注册新生成的 `hook/routes/dev.sh`。从下一条消息开始，#dev 就进入自己的
   side chat。

auto 模式下这一步会自动完成，见 §5.4。

其他路由命令：`routes remove <name>`（该路由的频道回到兜底路由）、`routes list`、
`routes discover`。

### 5.4 自动开 side chat（auto 模式）

开启方法：

```bash
uv run stoatctl.py routes auto --mode auto --types TextChannel,Group --max-routes 20 --max-per-hour 5
```

```bash
uv run stoatctl.py routes hooks --write
```

然后在 Muse 里**只注册两个 hook**：`stoat-admin`（指令 `hook/ADMIN.md`，投递到「Stoat · 管理」）
和 `stoat-inbox`（指令 `hook/WORKER.md`）。之后的 hook 都由 admin worker 自动注册。

流程：

```
新频道第一次 @ bot / 私信 bot
  → 该频道符合策略？
      否 → 交给收件箱处理（载荷 unrouted 里说明原因）
      是 → 消息被扣住（状态 AWAITING，收件箱看不到它）
           → stoat-admin hook-poll：在 routes.json 里建一条 pending 路由，生成 hook 脚本，
             wake provision_routes
           → admin worker：用 Muse 的 hooks.list 确认不存在，再调用 hooks.add
             （脚本、ADMIN/WORKER 指令全文、投递到新的 side chat）
           → 新 hook 第一次运行：pending → active，处理被扣住的消息
```

`routes.json` 里的 `auto` 配置项（用 `routes auto` 命令修改）：

| 字段 | 默认值 | 含义 |
|---|---|---|
| `mode` | `off` | `auto` 开启 |
| `types` | `["TextChannel","Group"]` | 哪些频道类型会自动开；加上 `DirectMessage` 后，每个私信用户也各开一个 |
| `servers` | `[]` | 只对这些服务器生效；空表示任何服务器 |
| `max_routes` | 20 | 自动创建且仍有效的路由上限；达到上限后，新频道交给收件箱 |
| `max_per_hour` | 5 | 每小时最多新开几个 |
| `pending_timeout_secs` | 600 | 新 hook 必须在这段时间内被注册，否则路由记为 `failed` |
| `admin_side_chat` / `side_chat_prefix` | `Stoat · 管理` / `Stoat · ` | 命名 |

安全与失败处理：

- **只在频道真的和 bot 说话时才创建**：单纯建一个频道不会占用 hook。
- **注册新 hook 是 admin 的专职**：处理 Stoat 消息的 worker 不负责注册 hook。admin worker
  也只注册载荷里列出的 hook，不改动其他 hook，也不改 `routes.json`。
- **以“首次运行”作为注册成功的证据**：只有 hook 真的被 Muse 跑起来，路由才会变成 `active`。
- **超时或失败都会退回收件箱**：
  - `hooks.add` 失败时，admin 执行 `routes fail`；
  - 超时后路由自动变为 `failed`；
  - 被扣住的消息都改由收件箱回复，不会丢；
  - 没注册 admin hook 时，AWAITING 状态的消息超时后同样退回收件箱。
- **之后补注册也能生效**：`failed` 的自动路由，只要它的 hook 后来被注册并运行，就会重新激活。
- **有限次重试**：admin 通知后 3 分钟内还没注册，会再提醒一次，最多两次。
- 想停止自动创建：执行 `routes auto --mode off`。已经建好的路由不受影响。

## 6. 延迟

| 环节 | 方式 | 延迟 |
|---|---|---|
| Stoat → bridge → inbox | WebSocket 推送 | 毫秒级 |
| inbox → wake | 每个 hook 在运行窗口内每 0.5 秒检查一次 | 通常 < 1 秒；两次运行之间的空档最多一个 `poll_interval` |
| wake → 回复 | worker 冷启动 + 模型生成 | 取决于 Muse |
| 连续对话 | worker 进入 `warm-wait` 热态守候，hook 不会重复唤醒 | 基本只剩模型生成时间 |
| 多频道 | 各路由独立 | 一个频道慢不会拖慢其他频道 |

## 7. stoatctl 参考

worker 使用的命令前缀由唤醒载荷中的 `ctl` 字段给出：
`<.venv python> <dir>/stoatctl.py --data-dir <data> --routes-file <routes.json> --route <route>`。
前缀里显式写出了所有路径，因为 worker 的 shell 不会继承 hook 脚本里 export 的环境变量。

### 7.1 worker 命令

worker 调用以下任一命令都会刷新**本路由**的租约。

| 命令 | 作用 |
|---|---|
| `peek [--count]` | 列出本路由待处理的消息（JSON lines） |
| `history <ch> [--limit N] [--around MSG] [--chain MSG]` | 读取最新上下文；读取失败时退出码为 2，绝不会被当成“频道为空” |
| `send <msg_id> [--part k/n] [--reply-to MSG\|none] [--mention UID] [--mention-reply] [--attach FILE …]` | 持久发送，正文从 stdin 读取。`--attach` 可重复，上传图片 / 视频作为附件；带附件时正文可以为空 |
| `media-limits` | 附件限制：`max_bytes`（单个文件，Stoat 默认 20 MB）、`max_per_message`（默认 5）、允许的种类 |
| `no-reply <msg_id> --reason TEXT` | 决定不回复，并持久记录原因 |
| `typing <ch> start\|stop` | 显示或取消“正在输入”，由 bridge 通过 WebSocket 转发 |
| `lease [--release \| --hold SECS]` / `warm-wait` | 刷新或释放租约；`--hold` 让租约在没有心跳的情况下保持最多 1800 秒，用于长时间生成 / 回复后热态守候 |

### 7.2 send / no-reply 的退出码

| 码 | 含义 | 调用方该怎么做 |
|---|---|---|
| 0 | 已完成 | 无需处理 |
| 1 | 确定没有发出 | 修正问题后可以重试 |
| 2 | 结果不确定，或正被其他进程发送 | **不要重发**；重跑同一条命令只会去核验结果 |
| 3 | 已发出，但完成标记没写成功 | 重跑同一条命令只会补写标记，不会重发 |
| 4 | fail-closed：未知的 msg_id、身份不明、状态损坏、`wrong-route` | 交给人工处理 |
| 75 | 被限流 | 在 `retry_at` 之前不要重试 |

带附件时可能多出这些结果（都是退出码 1，消息确定没发出）：`attachment-missing`、
`attachment-empty`、`attachment-type-refused`（不是图片 / 视频）、`attachment-too-large`、
`too-many-attachments`、`upload-failed`（上传到 Autumn 失败；临时错误会自动重试，最多尝试 3 次）。

### 7.3 运维命令（只在主聊天或终端里由人执行）

| 命令 | 作用 |
|---|---|
| `status` | 健康状况、各路由的积压和租约、发送状态、stuck 消息 |
| `routes list / discover / add / add-each / remove / hooks [--write]` | 管理频道到 side chat 的映射（§5） |
| `routes auto [--mode auto\|off] [--types …] [--servers …] [--max-routes N] [--max-per-hour N] [--timeout S]` | 查看或修改自动开 side chat 的策略（§5.4） |
| `routes activate <name>` / `routes fail <name> --reason R` | 手动激活路由 / 放弃路由，让该频道退回收件箱 |
| `sends` | 列出未完成的发送尝试 |
| `release <key> --confirm-not-sent` | 在 Stoat 里**亲眼确认消息不存在**之后，释放一条 `uncertain` 的发送尝试 |
| `compact` | 压缩收件箱（bridge 每 6 小时也会自动做一次） |
| `whoami` | 检查 token 和 bot 身份 |
| `bridge start \| stop \| status` | 后台启动 / 停止 bridge，或查看它是否在运行（日志 `data/bridge.log`） |

## 8. 可靠性语义

- **接收**：至少送达一次（at-least-once）。WebSocket 推送和 REST 补拉之间的重复靠 `msg_id`
  去重；某条消息处理失败时，所在频道的游标停止前进，每 30 秒从旧游标重新补拉一次。
- **离线窗口**：从每个频道的游标开始补拉；离线期间新出现的频道从上次在线的时间开始补拉。
  早于 `STOAT_BACKFILL_MAX_AGE_SECS`（默认 3 天）的消息不再补拉，并会记录警告。
- **发送**：先持久化领取记录，再发 POST，绝不盲目重发。真正用来判断“是否已发出”的是持久化
  在消息上的 `nonce`；Stoat 的 `Idempotency-Key` 缓存只是单进程内的 1000 条 LRU，只能作为辅助。
- **唤醒**：同一批消息 4 分钟内不会重复唤醒；同一条消息最多唤醒 3 次，之后标记为 stuck，
  交给人工处理。以上都按路由分别计算。
- **附件**：文件在领取记录**之前**检查（类型、大小、数量），在领取**之后**、POST 之前上传。
  上传失败等于“没发出”，可以安全重试；上传本身不是幂等的，但没被消息引用的文件只是文件
  服务器上的孤儿，没人看得见，所以重试时重新上传即可。POST 之后的不确定情况仍按 nonce 核验，
  不会出现两条带图的消息。

## 9. 测试

离线测试，不连任何服务器，也不读 `.env`：

```bash
uv run python -m unittest discover -s tests
```

`tests/fakes.py` 是一个**只在测试里使用**的 Stoat REST 替身（没有 WebSocket）。它的作用有两个：
让测试可重复，以及制造真实服务器不会按需出现的故障，比如响应丢失、409、429、5xx。bridge 的
判断逻辑在进程内直接调用真实的 `Bridge` 方法来测试。

- `test_units`：ULID、过滤规则、监听选择器、收件箱、状态文件损坏、hook 唤醒冷却与 stuck 上限。
- `test_bridge`：哪些消息会入队（私信、@、回复 bot、bot 消息）、首次启动不回放历史、
  重启后补拉、处理失败时频道标记为 dirty 并由补拉恢复、频道目录、新建频道配合 `server:` 监听。
- `test_delivery`：发送状态机。注入的故障包括：响应丢失、消息未保存、429、400 / 500、连接
  被拒、ACK 失败、并发发送方；另外覆盖分段发送和输入检查。
- `test_hooks`：`hook/stoat-watch.sh` 的唤醒、冷却、租约，bridge 宕机告警与恢复（Muse 的
  hook 运行时用 `tests/hook_runtime_stub.sh` 代替）。
- `test_warm_wait`：worker 回复后的热态守候：守候前刚到的消息、发送状态未定的消息、新会话。
- `test_routes`：路由解析顺序、选择器冲突、生成 hook、按路由的唤醒与租约、`wrong-route`
  隔离、等待期间 routes.json 发生变化。
- `test_media`：附件发送（只发图、进度 + 附件分两段、非媒体文件在上传前被拒、服务器给的
  大小和数量上限、上传失败与重试、发出后响应丢失时按 nonce 核验）、`lease --hold`、skill 的
  安装与 `prepare` / `demo`（转码部分需要 ffmpeg）。
- `test_install`：`bridge start / stop / status`、单实例锁、hook 在 `STOAT_AUTOSTART=1` 时拉起
  bridge（dry run 不会），以及在假的 home 里离线跑真实的 `install.sh` / `uninstall.sh`：skill 是软链接、
  重复安装不重复登记、卸载计划不删东西、`--yes` 停掉 bridge 并删掉项目目录和 skill 链接（不碰链接
  指向的文件）、安装列表和目录不符时拒绝卸载。
- `test_auto_routes`：自动开 side chat 的策略、admin hook 的登记载荷和各种失败路径。

**真实服务器测试**（可选，使用 `.env` 里的 token 和服务器）：

```bash
STOAT_LIVE=1 uv run python -m unittest discover -s tests -p test_live.py
```

默认只读，检查三件事：API 根地址与 token、bot 身份、真实的 Bonfire 连接能收到 Ready。测试期间
bot 会短暂显示在线。

再设置 `STOAT_LIVE_CHANNEL=<频道 ID>`，会往该频道**发送一条可见的测试消息**，并通过 nonce
核验它已经发出。所有测试状态都写在临时目录里，不会碰 bridge 自己的 `data/`。

## 10. 排障

| 现象 | 原因与处理 |
|---|---|
| 网页端显示 bot 不在线 | 只有 bridge **正在连接**时 bot 才在线：Bonfire 认证成功后，服务端会给 bot 建立在线会话，bridge 一停就下线。用 `uv run stoatctl.py status` 确认 `healthy: true`。如果 bridge 在跑、网页仍显示离线，刷新一下网页，在线状态是靠订阅推送更新的，长时间打开的页面可能没收到。 |
| 在服务器频道发消息，bridge 没反应 | 默认只接收**@ bot 的消息、回复 bot 的消息、私信**。bridge 每 30 秒会打一行汇总日志，例如 `messages in last 30s: 1 (ignore not-addressed-to-us×1)`，出现 `not-addressed-to-us` 就是这个原因。在网页端用 `@` 选中 bot 再发；如果希望不 @ 也能收，在 `.env` 的 `STOAT_LISTEN_CHANNELS` 里填写以下任一种（逗号分隔，改完要重启 bridge）：单个频道 ID；`server:<服务器 ID>`，表示整个服务器，**包括以后新建的频道**；`*`，表示 bot 能看到的所有服务器频道。频道和服务器 ID 用 `routes discover` 查。 |
| 新建的频道收不到消息 | 和上一条原因相同：`STOAT_LISTEN_CHANNELS` 里只写了某个频道的 ID，新频道不在其中。改成 `server:<服务器 ID>` 即可覆盖之后新建的频道。注意：改配置之前被忽略的消息不会补回来，需要重新发送。 |
| 监听频道里发了消息，记录却显示 `no-reply`、没有回复 | 说明 `STOAT_LISTEN_REPLY=judge`，worker 判断这条消息不需要回应（比如只有 “hi”、“1”），原因会写在收件箱的完成记录里。改成 `all`（默认值）后，监听频道里的每条消息都会回复；连发的几条仍然合并成一条回复。 |
| hook 一直不唤醒，`stoatctl status` 里 `worker_on_duty: true` | 还有 worker 进程占着租约，比如上一个 worker 被强行杀掉、没来得及释放。确认它已经不在运行后执行 `uv run stoatctl.py lease --release`。 |
| 发图 / 视频时 `send` 返回 `attachment-type-refused` | 文件不是图片或视频（桥按文件头判断，不看扩展名），或者种类不在 `STOAT_ATTACH_TYPES` 里。先用 `skills/stoat-media/scripts/stoat_media.py prepare <文件>` 转换。 |
| 返回 `attachment-too-large` | 超过服务器的单文件上限（`stoatctl media-limits`）。用 `prepare --max-bytes <上限>` 压缩；视频太长、怎么压都放不下时会明确报错，需要剪短。 |
| 发了“正在生成”之后就没有下文 | worker 在生成途中退出了。`--part 1/2` 发出后，源消息还没完成，下次唤醒会重新处理它。生成很慢时要先 `lease --hold`，否则超过 2 分钟没心跳，hook 会再起一个 worker。 |
| 日志里连汇总行都没有 | bridge 根本没收到事件。看启动日志里 `ready: N channels`：如果目标频道不在其中，说明 bot 没加入那个服务器，或者没有 View Channel 权限。 |
| 想看每条消息被过滤的原因 | 启动时加 `STOAT_LOG_LEVEL=DEBUG`，每条被忽略的消息都会打一行 `ignored <channel>:<id> … (<原因>)`。 |
| 消息进了队列但没有回复 | 这一步和 bridge 无关，是 Muse 侧的问题。检查 hook 是否在运行（`status` 里各路由的 `pending`，`hooks.dry_run`）。auto 模式下，`awaiting_side_chat` 大于 0 说明在等 admin hook 注册新的 hook。 |

## 11. 已知限制与待验证项

已在真实 Stoat（api.stoat.chat，2026-11-14）上验证：

- bot token 可以连上 Bonfire；连接期间 bot 显示在线。
- 频道里的 @ 提及和 `STOAT_LISTEN_CHANNELS` 的消息都能入队。
- 回复经 `Idempotency-Key` 发出后，回读消息时能拿到 `nonce`，所以发送结果的核验机制可用。
- 回复正确挂在原消息下。
- 文件服务（2026-11-20）：`GET /` 的 `features.autumn.url` 是 `https://cdn.stoatusercontent.com`，
  附件上限 20 MB、每条 5 个；用 bot token 向 `{autumn}/attachments` 上传 PNG 成功并拿到 id。
  **带附件的消息还没有在真实频道里发过**（只验证了上传，没有发出可见消息）。

- **需要用真实账号验证**：
  - 用户能否直接私信 bot；
  - bot 是否真的能收到服务器内所有频道的事件；
  - Muse 是否支持“每个 hook 投递到不同的 side chat”，以及 hook worker 能否看到该 side chat
    的历史；
  - auto 模式下，hook worker 能否调用 `hooks.add`、能否新建 side chat，以及 Muse 的 hook
    数量配额是多少。
- 关闭 auto 模式时，新路由需要人工在 Muse 里注册对应的 hook；处理消息的 worker 永远不会自己注册 hook。
- 频道被删除后，它的路由和 hook 不会自动清理（`routes remove` + Muse `hooks.remove`）。
- 用户发来的附件只把元数据和下载 URL 交给 worker，bridge 不会替它下载内容。
- `STOAT_PROXY_URL` 只对 REST 生效，WebSocket 走直连（可以配置 `STOAT_CA_BUNDLE`）。
- 假服务器只实现了 bridge 用到的接口；真实 Stoat 的权限、限流桶和事件字段可能更复杂。
