# 项目位置与提交约定

## 本机位置

| 用途 | 位置 |
|---|---|
| 产品项目 / Git 工作目录 | `/Users/luckyice/Desktop/NewIM工程/NewIM` |
| Agent 工程原始目录 | `/Users/luckyice/Desktop/NewIM工程/NewIM-Agent-Engineering` |
| Git origin | `git@github.com:m-ice/NewIM.git` |

上述位置同时记录在 `tasks/manifest.json.repository`，任务提示词会输出产品工作目录和 origin。
本机路径仅描述当前工作站；CI 校验路径格式，不要求 GitHub runner 存在该绝对路径。
其他机器应按实际克隆位置更新配置并验证 origin，不能在本机路径不存在时另建一个无关项目。

## 文件归属

首次登记时产品目录只有 `.git`，没有业务文件或提交。将完整控制面基线导入产品仓库根目录，保留 `.codex/`、`.github/`、AGENTS.md、脚本、任务、规范与测试的相对结构，确保现有校验及 CI 入口有效。
原始 Agent 目录保留，不删除、不作为嵌套 Git 仓库提交。

基线导入后，以产品 Git 仓库中的控制面文件为维护版本。原始目录只是导入源副本，不形成第二个独立开发工作区。后续不要从旧副本盲目覆盖 Git 仓库。
`core/`、`server/`、`sdk/`、`apps/`、`infra/` 等产品路径均相对于产品仓库根目录；当前仅有规范或占位目录，不代表业务已经实现。
任务 write_scopes、报告路径、构建测试命令均以产品仓库根目录解析。

## 提交与运行

提交前在产品目录运行 `make check`，检查待提交文件，排除 `.git/`、缓存、本地环境文件等无关内容，再提交到已配置的 origin。
不要另行初始化原始 Agent 目录，不修改远程地址来规避权限问题。
提交文件、登记路径和运行控制面测试不意味着启动 Agent 工程。本次不执行 Codex 编排、产品构建或服务启动。
首次导入作为基线提交；后续 PR 再使用 base/head 任务范围检查。required checks 和分支保护仍需在 GitHub 配置。
