# ADR 0001 — 控制面任务与验收契约

Status: Accepted (本次控制面完善范围；不决定产品技术栈)

## Context

任务清单 v1 只有文字验收，CI 只检查部分字段与依赖，无法阻止漏项发布或无证据完成。

## Decision

升级到 schema_version 2；每个验收项明确 command/manual/pending 检查方式。
产品工具链未确定的检查保留 pending 和原因，禁止带 pending 进入 in_progress/review/done。
blocked 必须区分 dependencies 与 manual；只有 dependencies 阻塞可在依赖完成后成为候选。
完成证据绑定被验证的完整 Git 提交，记录每项结果、命令、仓库内报告与 SHA-256，以及全部指定评审。
证据记录提交可以晚于被验证提交；不得用自身提交哈希制造循环依赖。
发布任务必须覆盖所有非发布任务，禁止仅完成部分平台而绕过核心能力。
PR 用 Task: 任务ID 声明范围，检查 base 与 head 两端的任务授权，禁止同一 PR 扩大已有任务范围来绕过门禁。
新增任务或扩范围先单独合入控制面变更，再实施该任务。

## Consequences

仅使用 Python 3.11+ 标准库，无需启动服务或 Agent。
结构、引用、依赖、证据完整性可自动检查；报告真实性及人工评审身份仍需独立评审和 GitHub required checks 保护。
校验器不会执行清单中的任意命令。工具链确定后，由受审查的 CI 工作流执行对应命令。
