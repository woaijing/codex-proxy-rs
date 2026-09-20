<!-- prettier-ignore -->
<div align="center">

<img src="frontend/public/favicon.svg" alt="Codex Proxy RS" width="80" height="80" />

# Codex Proxy RS

面向 Codex 的自托管多账号 AI 网关

[![CI](https://github.com/zyycn/codex-proxy-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/zyycn/codex-proxy-rs/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/zyycn/codex-proxy-rs?display_name=tag&sort=semver&style=flat-square)](https://github.com/zyycn/codex-proxy-rs/releases)
[![GHCR](https://img.shields.io/badge/GHCR-codex--proxy--rs-2496ED?logo=docker&logoColor=white&style=flat-square)](https://github.com/zyycn/codex-proxy-rs/pkgs/container/codex-proxy-rs)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](LICENSE)

[快速预览](#快速预览) · [快速开始](#快速开始) · [客户端接入](#客户端接入) · [文档](#文档) · [上游同步](#上游同步) · [社区](#社区) · [许可证](#许可证)

</div>

> [!NOTE]
> 本项目提供 Responses API，不支持 `/v1/chat/completions`。接入前请确认客户端支持 Responses 协议。

> [!IMPORTANT]
> 此仓库基于 [zyycn/codex-proxy-rs](https://github.com/zyycn/codex-proxy-rs) 增加 OpenCode Zen / Go API Key 支持。
> 部署 OpenCode 版本请克隆 `https://github.com/woaijing/codex-proxy-rs.git`，按[源码构建](deploy/README.md#镜像升级与源码构建)部署。
> 下方一键安装、Release 和镜像链接指向上游发布版；它们不包含本仓库新增功能。

## 快速预览

无需部署，打开 [快速预览服务](https://codex-proxy-rs.ainz.cc) 即可体验管理端的系统概览、账号分组、代理管理与用量统计。

| 登录信息 | 值 |
| --- | --- |
| 地址 | <https://codex-proxy-rs.ainz.cc> |
| 登录身份 | 管理员 |
| 账号 | `admin@cpr.local` |
| 密码 | `039c18de2aeac46d23ead7766bb07bbbe3747233c66a128e` |

预览服务运行已发布版本，展示的账号、代理与使用记录均为模拟数据，每天北京时间 `00:00` 自动生成当天数据。
这是公开共享的功能预览环境，不提供真实模型调用；请勿导入真实账号、密钥或其他敏感信息。

## 快速开始

使用 Docker Compose 部署版本固定的发布镜像，同时启动 PostgreSQL 和 Redis。
以下命令适用于 Linux amd64/arm64，需要 Docker Engine、Docker Compose Plugin、curl 和 OpenSSL。已有部署请先看
[升级说明](deploy/README.md#镜像升级与源码构建)，不要覆盖原配置。

### 一键安装

请确保当前用户能访问 Docker，并可通过 `sudo` 或 root 设置目录权限。

```bash
curl -fsSL https://raw.githubusercontent.com/zyycn/codex-proxy-rs/main/deploy/install.sh -o install.sh && bash install.sh
```

[安装脚本](deploy/install.sh) 默认安装到当前目录下的 `codex-proxy-rs/`，下载同一正式 Release 的部署文件，
自动生成密码、设置目录权限并启动服务。完成后会显示访问地址和管理员密码，请保存密码，再按下方步骤
[添加账号与客户端密钥](#添加账号与客户端密钥)。

可在执行 `bash install.sh` 时传入环境变量：

| 变量 | 用途 | 默认值 |
| --- | --- | --- |
| `INSTALL_DIR` | 安装目录，建议使用绝对路径 | 当前目录下的 `codex-proxy-rs/` |
| `CPR_RELEASE_TAG` | 指定发布标签 | 最新正式版本 |
| `ADMIN_PASSWORD` | 管理员初始密码，至少 12 位，不能包含 `$`，不能使用常见弱口令 | 随机生成 |

例如，自定义安装目录：

```bash
INSTALL_DIR="$HOME/services/codex-proxy-rs" bash install.sh
```

重复运行时请使用同一安装目录。检测到 `deploy/config.yaml` 后，脚本保留现有配置和部署文件，
忽略传入的管理员密码，也不执行版本升级。

### 手动安装

自行下载部署文件、配置密码和启动服务的完整步骤见 [部署文档](deploy/README.md#手动安装)。

### 登录管理端

部署完成后，打开 `http://127.0.0.1:8080`，使用 `admin@cpr.local` 和管理员密码登录。
API Key 持有者可在同一登录页切换登录身份，进入 `/key-usage` 查看自己的用量、趋势、请求日志、额度与健康时间线；不能访问管理员页面。

默认地址只能在服务器本机访问。从其他设备使用时，需要配置
[HTTPS 反向代理](deploy/README.md#公网访问)。

### 添加账号与客户端密钥

1. 在「账号」中添加账号，完成授权或导入。
2. 按需建立账号分组，再创建客户端密钥并选择可用分组。**不选分组表示可使用全部账号**。
3. 打开密钥的「使用密钥」，复制客户端配置。

OpenCode 账号在导入时选择「OpenCode → API Key」，填写名称、Zen / Go 产品和控制台生成的 Key。
可配置账号代理、分组、权重和并发限制；支持范围与 JSON 导入格式见 [OpenCode 接入](docs/api.md#opencode-zen--go-api-key)。

## 客户端接入

**Codex CLI / 桌面端**：在「使用密钥」中按操作系统复制配置，或通过 CCSwitch 导入。
合并到客户端配置后重启 Codex。完整步骤、生图配置与排障见[客户端配置](deploy/README.md#客户端配置)。

**其他 Responses API 客户端**：填写以下信息。

| 配置 | 值 |
| --- | --- |
| Base URL | `http://127.0.0.1:8080/v1`；远程接入使用服务器的 HTTPS 地址 |
| API Key | 管理端创建的客户端密钥 |

可用模型以该密钥查询到的模型列表为准：

```bash
curl http://127.0.0.1:8080/v1/models \
  -H 'Authorization: Bearer <client-api-key>'
```

## 文档

- [客户端接入与生图](deploy/README.md#客户端配置)
- [部署、备份与恢复](deploy/README.md)
- [API 参考](docs/api.md)
- [模型定价与手动同步](docs/api.md#模型定价)
- [系统架构](docs/architecture.md)
- [管理端主题](docs/theme.md)
- [数据库迁移](backend/migrations/README.md)
- [贡献与审查](CONTRIBUTING.md)

## 上游同步

定期同步上游的重要修复，服务器使用经过验证的固定版本。需要同步时，在项目的新会话中复制以下提示词；
项目路径不同则替换为实际路径。模板包含提交和推送授权，仅阅读本文不代表执行该任务。

```text
请直接完成本项目的上游同步，包含修改、验证、提交和推送。

项目：D:\github\codex-proxy-rs
我的仓库 origin：https://github.com/woaijing/codex-proxy-rs.git
上游 upstream：https://github.com/zyycn/codex-proxy-rs.git
目标：将 upstream/main 最新更新合入我的 main，保留 OpenCode 扩展。

执行要求：
1. 阅读 AGENTS.md、CONTRIBUTING.md 和 dev-guide，确认工作区、远程地址与实际提交差异。已有无关改动保留，必要时使用独立 worktree。
2. 拉取上游，简要说明本次变化及影响，随后直接合并、解决冲突。
3. 保留 OpenCode Zen/Go Key、管理端导入编辑、协议转换、身份与父会话关联、代理绑定、会话亲和和共享冷却；所有 Key 冷却时不得继续请求。
4. 重点检查上游对账号调度、Provider 接口、管理端和数据库迁移的影响。迁移冲突先核实兼容性，不通过修改数据库迁移记录或 checksum 绕过问题。
5. 根据实际差异执行必要验证：Rustfmt、严格 Clippy、相关测试、架构检查；前端有变化时执行 lint、构建和受影响流程验证。区分新增失败、已知环境失败和未执行项；仅在改动或失败需要时追加验证。新增且影响本次功能的失败应修复后再推送。
6. 已授权创建提交、合并并推送到 origin/main。保留原始历史和 upstream，禁止强推、删除数据、推送到上游仓库。此次不发布 Release、不部署服务器。
7. 常规实现选择自行决定，持续推进到完成。遇到必须由我处理的权限、凭据或数据兼容问题，说明具体阻塞及所需操作。

最后用中文简要报告：同步的上游 SHA、我的最新 SHA、保留的功能、验证结果、剩余风险和推送结果。
```

服务器已经部署时，在提示词后补充当前运行版本或提交；尚未部署则注明“尚未部署”：

```text
服务器当前运行版本/提交是：____。请评估升级及数据库迁移兼容性，但不要连接或改动生产服务器。
```

在已打开本项目的会话中，也可以使用简短指令：

```text
请执行 README「上游同步」中的任务模板；已授权其中的提交和推送操作。服务器当前版本/部署状态：____。
```

## 社区

欢迎到 [Discussions 讨论区](https://github.com/zyycn/codex-proxy-rs/discussions)交流：
[使用问答](https://github.com/zyycn/codex-proxy-rs/discussions/categories/使用问答)、
[想法讨论](https://github.com/zyycn/codex-proxy-rs/discussions/categories/想法讨论)、
[实验反馈](https://github.com/zyycn/codex-proxy-rs/discussions/categories/实验反馈)与
[经验分享](https://github.com/zyycn/codex-proxy-rs/discussions/categories/经验分享)。
明确的 Bug 或功能需求请使用 [Issue 模板](https://github.com/zyycn/codex-proxy-rs/issues/new/choose)，无需先发讨论。

感谢 [LINUX DO](https://linux.do) 社区提供开放、友善的技术交流平台。

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 开源。
