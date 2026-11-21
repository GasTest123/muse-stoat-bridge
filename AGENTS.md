# AGENTS.md：给安装 / 卸载 muse-stoat-bridge 的 agent（通常是 Muse）

用户说“安装 muse-stoat-bridge”时按 **A** 做，说“卸载 muse-stoat-bridge”时按 **B** 做。全程
**不要在聊天里索要、显示、转述或记住 bot token**，也不要把它写进你的记忆。

安装位置固定为 **`~/workspace/muse-stoat-bridge`**：用 `git clone` 下载到这里，项目目录就是安装。
代码、Python 环境、配置、数据、日志、生成的文件都在这个目录里；目录外的只有下表这几样：

| 位置 | 是什么 | 何时有 | 怎么卸载 |
|---|---|---|---|
| `~/workspace/muse-stoat-bridge` | 代码和全部运行文件 | 一直 | `uninstall.sh --yes` 整个删除 |
| `~/workspace/custom-skills/stoat-media` | 指向 `~/workspace/muse-stoat-bridge/skills/stoat-media` 的软链接（Muse 加载用户安装的技能的目录） | 一直 | 记在安装列表 `install-manifest.txt` 里，`uninstall.sh` 删除链接 |
| Muse 里的 hook 登记 | 不是文件，是 Muse 的设置 | 一直 | 你用 `hooks.remove` 删掉 `script_path` 在 `~/workspace/muse-stoat-bridge/` 下的所有 hook |

## A. 安装

用户的一句话通常是：**“帮我安装 muse-stoat-bridge，把 Stoat 接入你：<仓库地址>”**。

1. **下载并安装**：

   ```bash
   git clone <仓库地址> ~/workspace/muse-stoat-bridge && ~/workspace/muse-stoat-bridge/install.sh
   ```

   它同时装好 stoat-media skill（在 Stoat 里让你画图、做视频时，用你自己的生成能力做出来，
   作为附件发回频道）：在 `~/workspace/custom-skills/` 下建软链接 `stoat-media`，指向项目里的
   `skills/stoat-media`，不拷贝文件。
   `~/workspace/muse-stoat-bridge` 已经存在时说明装过了：`git -C ~/workspace/muse-stoat-bridge pull` 后
   重跑 `install.sh` 即可（安全，保留安装列表）。

   `install.sh` 在项目目录里建好 `.venv`、`.uv`（uv 的缓存和 Python；没装 uv 时连 uv 也放这里）、
   `.env`、`data/`，并写出安装列表 `install-manifest.txt`。

2. **请用户自己填 token**：打开 `~/workspace/muse-stoat-bridge/.env`，填 `STOAT_BOT_TOKEN=`（bot 的建法：
   Stoat 客户端 Settings → My Bots → Create a bot，再用邀请链接把 bot 拉进服务器）。
   填好后检查并启动：

   ```bash
   cd ~/workspace/muse-stoat-bridge && .venv/bin/python stoatctl.py whoami && .venv/bin/python stoatctl.py bridge start
   ```

   能打印出 `bot_user_id` 就对了。之后 bridge 由 hook 看护：它停了，下一次 hook 运行会把它拉起来
   （`.env` 里的 `STOAT_AUTOSTART=1`）。

3. **问用户监听范围**：只响应私信和 @（默认），还是某些频道 / 整个服务器里的每条消息都回
   （`.env` 的 `STOAT_LISTEN_CHANNELS`，写法见 `.env.example`）。改了要 `bridge stop` 再 `bridge start`。

4. **注册 hook**（推荐 auto 模式：每个新频道自动开自己的 side chat）：

   ```bash
   cd ~/workspace/muse-stoat-bridge && .venv/bin/python stoatctl.py routes auto --mode auto && .venv/bin/python stoatctl.py routes hooks --write
   ```

   然后把 `~/workspace/muse-stoat-bridge/hook/routes/hooks.json` 里的每一项用 `hooks.add` 登记：
   `script_path`、`poll_interval_secs`、`script_timeout_secs` 照抄，instructions 用
   `instructions_file` 指向的文件全文，投递到 `deliver_to` 指定的 side chat。

5. **验收**：
   - 每个 hook 跑一次 `hooks.dry_run`，结果是 `silent: no new messages`；
   - 请用户私信或 @ bot 发一句话，确认回复挂在原消息下；
   - 请用户说“画一张测试图”，确认图片作为附件出现；
   - `.venv/bin/python stoatctl.py status` 里 `healthy: true`，每条路由 `pending` 为 0。

6. 告诉用户装在哪里、目录外装了什么（上表），以及卸载只需要说“帮我卸载 muse-stoat-bridge”。

## B. 卸载

用户的一句话通常是：**“帮我卸载 muse-stoat-bridge，清理所有文件”**。

1. **看卸载计划**（不会删除任何东西）：

   ```bash
   ~/workspace/muse-stoat-bridge/uninstall.sh
   ```

2. **先删 Muse 里的 hook**：`hooks.list`，对每个 `script_path` 以 `~/workspace/muse-stoat-bridge/`（展开后的
   绝对路径）开头的 hook 调用 `hooks.remove`。计划里列出的 id 只是参考，以路径为准。
   auto 模式后来自动加的 hook 也在其中。

3. **执行卸载**：

   ```bash
   ~/workspace/muse-stoat-bridge/uninstall.sh --yes
   ```

   它会停掉 bridge 和所有从项目目录运行的进程，删除安装列表里登记的 skill 软链接，最后删除整个
   `~/workspace/muse-stoat-bridge`。

4. **核对**：`~/workspace/muse-stoat-bridge` 不存在了；`~/workspace/custom-skills/stoat-media`
   也不存在了；`hooks.list` 里没有指向它的 hook。

5. 告诉用户卸载完成。这些 hook 当初投递的 side chat 是 Muse 里的对话记录，不是文件，是否删除
   由用户决定。如果 bot 不再使用，可以在 Stoat 的 Settings → My Bots 里删除它，token 随之作废。

`~/workspace/muse-stoat-bridge` 已经不存在、却还有指向它的 hook 时，直接用 `hooks.remove` 删掉这些 hook 即可。
