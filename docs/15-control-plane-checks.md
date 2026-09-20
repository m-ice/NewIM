# 控制面检测与验收

## 运行边界

要求 Python 3.11+（使用标准库 tomllib），不需要安装第三方依赖。
`make check` 仅运行控制面校验和控制脚本测试，不启动业务服务、Codex 或子 Agent。
当前产品技术栈尚未决定，控制面通过不等于业务构建、可靠性、安全或性能合格。

```sh
make check
make ready
python3 -B scripts/render_task_prompt.py NIM-FND-001
```

## 任务清单 v2

`tasks/manifest.json` 的 schema_version 为 2。每个任务要求唯一 ID、有效状态、已定义的 owner/reviewers、非空验收项、合法仓库相对 write_scopes、依赖、flags、blocker、acceptance_checks、evidence_path。
目录范围以 `/` 结尾，文件范围精确匹配；不接受绝对路径和 `..`。
每个任务在对应 phase 的 workstream 中恰好出现一次。实现任务必须间接或直接依赖基础 CI 任务；发布任务必须覆盖全部非发布任务。

- `ready`：依赖已经 done，可以被选择。
- `blocked` + `blocker.kind=dependencies`：等待依赖；依赖全部 done 后可被列为候选。
- `blocked` + `blocker.kind=manual`：需要决策或人工解除；即使依赖完成也不调度。
- `in_progress` / `review` / `done`：依赖必须全部 done，验收计划不得存在 pending。
- 非 blocked 状态的 blocker 必须为 null。

ready_tasks 只列出候选，不修改状态。render_task_prompt 还会拒绝存在 pending 验收的任务。
每个 acceptance_checks 条目与 acceptance 按顺序一一对应：

```json
{"criterion":"Root build command succeeds","kind":"command","argv":["make","build"]}
```

这是结构示例，不表示仓库已经提供产品 `make build`。
`manual` 必须写明检查原因、方法和所需证据；`pending` 必须说明尚未确定的前提。
产品工具链 ADR 完成后，在控制面 PR 中替换各任务的 pending 检查，并补齐被调用的构建/测试入口，再开始实施。
除技术选型 ADR 任务外，实施/评审/完成任务至少有一条 command 检查。验收计划不得用无意义命令替代真实测试，具体命令质量仍需评审。
校验器不会自动执行 argv；由开发者与经过评审的 CI 执行，防止清单成为任意命令执行器。

## done 的证据

任务先进入 review；所有检查和指定评审完成后，才进入 done。
证据路径固定为 `tasks/evidence/<任务ID>.json`，报告建议放在 `tasks/evidence/<任务ID>/`。
结构参照 `templates/acceptance-evidence.json`，占位符必须替换，模板不能直接作为通过证据。

- task_id 匹配任务，commit 为被检查的完整 Git SHA。
- checks 与验收计划一一对应，criterion 匹配，result 为 pass。
- command 记录实际 argv 和整数 exit_code=0。
- 每个检查与评审均引用非空的仓库内报告及其 SHA-256；不接受越界软链接。
- reviews 恰好覆盖所有指定角色，每条包含实际评审者身份、approved 和相同 commit。
- security/license/reliability 标志分别要求 security/license/qa 评审角色。
- 同一角色可以有不同实际人员；角色名不构成身份认证，不能代替独立评审。

证据描述被验证的代码提交，随后单独提交证据和状态，避免“报告包含自己的提交哈希”的循环。
修改被测代码后必须重新执行相应检查并更新证据；当前校验只验证记录的结构、引用、摘要及内部一致性，不验证 SHA 是否存在、报告内容是否真实，也不自动证明其仍覆盖最新代码。
这些真实性和时效性由 CI 的实际运行与独立审查承担；不得仅凭手写 JSON 宣布通过。
没有真实提交或评审时保留 review，不填伪造 SHA、不代签。

## PR 修改范围

PR 描述中每个任务独占一行：

```text
Task: NIM-CTL-001
```

CI 从 GitHub 事件文件读取声明，不把 PR 文本拼接为 shell 命令。
检查 base/head 两端 task scopes 与实际 diff 的所有路径，包括删除与重命名旧路径。
每个任务隐式允许写入自己的证据 JSON 和报告目录，不能借此改其他任务的证据。
手动阻塞或依赖未完成的任务不能作为改动授权。

```sh
python3 -B scripts/check_scope.py --task NIM-CTL-001 --base <完整SHA> --head <完整SHA>
```

新增任务/扩大范围必须先通过单独控制面 PR；同一个 PR 不能扩大范围并利用扩大后的范围写业务代码。
首次导入仓库（尚无 Git 或 base 不含本控制契约）需要先建立受审查的基线提交；不为此提供自动跳过门禁选项。
工作流在 push main 时运行校验与测试，在 PR 时额外运行范围检查。
该检查比较提交，不检查未提交改动，不替代工作区隔离和并发冲突控制。

## GitHub 与产品门槛

初始化 Git 并推送之后，应在 GitHub 将工作流的两个 Python 版本检查设为 required checks，要求独立评审，并保护 main；仓库文件不能自行开启服务端分支保护。
CODEOWNERS 中的账号/团队需要在实际组织中确认存在且拥有权限。
对 scripts/、tasks/、.github/ 的规则变更必须重点审查，避免修改校验器本身绕过规则。

技术栈确定后由 NIM-FND-002 建立并实际执行格式、lint/类型检查、构建、单元 smoke 检查。
后续按任务接入协议兼容、数据库迁移、故障注入、恢复演练、真实设备、性能、密钥/依赖安全/许可证检查。
每项要求对应命令或明确人工证据；无法运行时记录阻塞，不能把本控制面 CI 的绿色结果当作产品验收。
