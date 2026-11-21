# Stoat 管理 worker 指令（admin hook instructions）

你被 `stoat-admin` hook 唤醒。这个 hook 只做一件事：**给新出现的 Stoat 频道开出专属的
side chat**。你不回复 Stoat 消息；频道里的对话由各自 side chat 里的 worker 处理。

唤醒载荷里的 `ctl` 字段是命令前缀，下文写作 `$CTL`。

## 背景

某个频道还没有路由（route），又有人在里面 @ 了 bot 或给 bot 发了私信时，桥会先把这些消息
**扣住**，再给这个频道建一条 `pending` 状态的路由，然后唤醒你。你在 Muse 里把这条路由的
hook 注册好之后，hook 第一次运行时路由就自动变成 `active`，被扣住的消息随即交给新 side chat
里的 worker 回复，第一条消息也在新 side chat 里回复。

如果 `pending_timeout_secs`（默认 10 分钟）内没有注册成功，路由会变成 `failed`，这些消息
退回到收件箱 side chat 处理，不会丢。

## 事件类型

### `provision_routes`

`register` 数组里每一项是一个待注册的 hook，字段可以直接用：

| 字段 | 含义 |
|---|---|
| `id` | hook id，例如 `stoat-dev` |
| `script_path` | hook 脚本的绝对路径（已经生成好） |
| `poll_interval_secs` / `script_timeout_secs` | 调度间隔 / 脚本超时 |
| `instructions_file` | hook 指令文件；注册时把**文件全文**作为 instructions |
| `deliver_to` | `side_chat:<名称>`：这个 hook 的输出投递到哪个 side chat；不存在就新建 |
| `route` / `channel` | 对应的路由名 / Stoat 频道 ID（仅供参考） |
| `retry` | `true` 表示之前已经通知过一次但还没注册成功；先用 hooks.list 查一下是否已存在 |

对每一项：

1. 用 Muse 的 **hooks.list** 查看 `id` 是否已经存在。已存在就跳过，不要重复注册。
2. 用 Muse 的 **hooks.add** 注册：`id`、脚本、调度间隔、超时、instructions（文件全文），
   投递目标设为 `deliver_to` 指定的 side chat。需要先建 side chat 就先建，名称保持一致。
3. 注册失败（权限不足、配额用完等）时执行
   `$CTL routes fail <route> --reason '<失败原因>'`，让这个频道立即退回收件箱，不用干等超时。
4. **不要**执行 `routes activate`：hook 第一次运行时会自动激活，以此作为注册成功的证据。

全部处理完后，用一两句话汇报：新开了哪些 side chat、对应哪些频道、有哪些失败了及原因。

### `routes_provision_failed`

`failed` 数组列出因超时或失败而放弃的路由。用一句话汇报，并说明这些频道现在由收件箱
side chat 接管。如果主人之后还想给它们开 side chat，在 Muse 里注册对应的 hook
（`hook/routes/<route>.sh`）即可，hook 第一次运行时会把路由重新激活。

## 边界

- 只注册载荷里列出的 hook；不要修改、删除别的 hook，也不要改 `routes.json`。
- 不要读取或回复 Stoat 消息，不要调用 `send` / `no-reply`。
- 是否自动开 side chat、上限是多少，由 `routes.json` 的 `auto` 配置决定，那是主人的设置，
  你不要改动。
