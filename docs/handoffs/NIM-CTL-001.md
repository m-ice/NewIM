# Handoff — NIM-CTL-001

Task: NIM-CTL-001
Owner: program_manager
Status: review
Commit/branch: 不适用；本工作目录尚未初始化 Git，未创建提交。

## Contract delivered

任务清单升级至 schema v2；严格校验字段、引用、图、workstream、发布覆盖和完成证据。
区分人工阻塞和依赖等待；未确定验收计划的任务不能进入实施或生成执行提示。
新增 PR 范围检查，比较 base/head 两端授权，包含删除和重命名旧路径。
新增 Python 标准库回归测试与 CI；未引入产品技术栈或运行服务。

## Files/modules changed

scripts/control.py、validate.py、ready_tasks.py、render_task_prompt.py、check_scope.py；
tests/control/；tasks/manifest.json 与 workstreams/foundation.json；Makefile、.github/；
控制面 ADR、检测说明、质量规范、README、AGENTS、提示词及证据模板。

## Validation run

- `make check`：通过，26 个任务契约有效，41 项回归测试通过。
- 本地 Python：3.14.6；CI 配置 Python 3.11 与 3.12，但本次未在 GitHub 执行。
- `python3 -B scripts/ready_tasks.py`：仅列出 NIM-FND-001。
- `python3 -B scripts/render_task_prompt.py NIM-FND-001`：正确生成手工 ADR 验收要求。
- 临时 Git 仓库集成测试：真实提交差异、重命名旧路径、PR 事件解析及缺失任务声明拒绝均通过。

## Compatibility/migration notes

旧清单已原位迁移至 schema v2，保留产品任务原有验收语义。
尚未确定产品工具链的检查显式标为 pending；实施前需要单独完善检查计划。
初次 Git 导入应先建立基线，之后 PR 才能基于双方已有契约检查修改范围。

## Known limitations

- 未启动业务服务、Codex 或子 Agent，未执行产品构建/集成测试。
- 无当前项目 Git 提交，无法生成真实提交绑定的完成证据。
- 未执行独立 QA/reviewer 评审，不伪造审批；因此任务保留 review，不标 done。
- 证据校验检查结构、报告摘要和内部一致性，不认证报告真实性或评审者身份。
- GitHub required checks、分支保护和 CODEOWNERS 账号有效性需在真实托管仓库配置确认。

## Downstream tasks unblocked

未自动开始或完成任何产品任务。NIM-FND-001 仍是唯一 ready 候选。

## Acceptance evidence

本文件记录本次实际本地验证，不代替 done 所需的提交绑定检查报告及独立评审。
