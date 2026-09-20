# NewIM Agent Engineering

NewIM 是澜遇科技自主研发的即时通讯基础设施项目。本仓库是 **Agent 工程控制面**：用于让 Codex 按软件工程方式拆分、实现、评审、测试和交付 NewIM，而不是用一个超长 Prompt 直接生成整套 IM。

负责人：`luckyice6@gmail.com`

## 项目文件位置与仓库

- 产品项目与 Git 工作目录：`/Users/luckyice/Desktop/NewIM工程/NewIM`
- Agent 工程原始目录：`/Users/luckyice/Desktop/NewIM工程/NewIM-Agent-Engineering`
- 远程仓库：`git@github.com:m-ice/NewIM.git`（[GitHub](https://github.com/m-ice/NewIM)）
- 路径配置来源：`tasks/manifest.json` 的 `repository` 字段。

控制面文件随初始基线提交至产品仓库根目录。此后以 `NewIM` 中已纳入 Git 的文件为准，产品源码、任务记录和验收证据都在该仓库内维护，避免两个目录分别开发。
本次只登记路径和提交工程基线；不启动 Agent、业务服务或产品任务。详细约定见 [项目位置说明](docs/16-project-location.md)。

## 目标

- 从零实现独立 NewIM，不复制受 AGPL/商业许可约束的客户端源码或私有协议。
- 以可靠性为核心：不丢消息、可恢复、可去重、可排序、可观测、可升级。
- 服务端与客户端模块化，高内聚、低耦合；协议、存储、传输、业务规则分层。
- 支持服务端、桌面端、iOS、Android、HarmonyOS、uni-app 与 Flutter 多平台工程协作。
- 基础能力开源并允许商业使用；增值/升级能力通过独立授权边界接入，Core 不依赖商业授权服务才能工作。
- 代码与文档使用正常工程团队语气，避免模板化、夸张、冗余的“AI 口吻”。

## Agent 工程结构

```text
.codex/agents/       Codex 项目级专业子 Agent
AGENTS.md            全仓库不可违反的工程约束
docs/                架构、协议、质量、授权与路线图
workstreams/         可并行工作流与依赖
specs/               稳定契约与版本化规范
tasks/manifest.json  Agent 可执行任务图
templates/           ADR、任务、交接、测试模板
scripts/             校验、任务渲染、初始化脚本
prompts/             启动与编排入口
reference/           用户基线与外部资料边界说明
```

## 后续获得启动授权后使用

1. 安装并登录 Codex CLI。
2. 进入 `/Users/luckyice/Desktop/NewIM工程/NewIM`，准备 Python 3.11+，运行 `make check`（仅检查控制面，不启动服务或 Agent）。
3. 先让 Codex 阅读 `prompts/bootstrap.md`，确认仓库规则与首批任务。
4. 再执行一个任务：

```bash
python3 scripts/render_task_prompt.py NIM-FND-001 > /tmp/newim-task.md
codex exec "$(cat /tmp/newim-task.md)"
```

执行任务前先阅读 [控制面检测与验收](docs/15-control-plane-checks.md)。带 pending 验收的任务必须先确定真实检查命令；禁止无证据标记 done。

也可以进入交互式 `codex`，直接要求“按 tasks/manifest.json 从 ready 任务开始，并使用合适的子 Agent”。

## 第一阶段不要做

不要为了“像大厂”提前引入 Kafka、分库分表、跨地域多活、大群、频道、RTC、端到端加密。先把 1v1 消息可靠性、同步、本地状态、Push 分层和可观测性做对，再由压测数据驱动演进。

## 法律边界

OpenIM 等项目用于学习公开架构、API 语义、Issue/PR 暴露的问题和工程方法。若引用第三方代码，必须先经过 `license` Agent 审查并记录来源、版本、许可证与修改说明。默认策略是“学习设计，不复制实现”。
