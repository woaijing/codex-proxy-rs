# OpenCode 模型协议快照

`models.json` 提取自 2026-09-19 的 https://models.opencode.ai/api.json，保留 `opencode`（Zen）和
`opencode-go`（Go）下未标记 deprecated 的可适配模型。当前为 Zen 63 项、Go 27 项。

协议选择按模型 `provider.npm` 覆盖平台默认 npm：`@ai-sdk/openai` 为 Responses，
`@ai-sdk/openai-compatible` 为 Chat Completions，`@ai-sdk/anthropic` 为 Messages；Google 和未知包不纳入此快照。
同名模型在两个产品下可以使用不同协议，不可合并成一个端点。

官方客户端的目录继承和身份头依据 OpenCode v1.18.31：

- [模型元数据继承](https://github.com/anomalyco/opencode/blob/014614d35b397775e5d397a490fc72368c894ec2/packages/opencode/src/provider/provider.ts#L1265)
- [推理请求身份头](https://github.com/anomalyco/opencode/blob/014614d35b397775e5d397a490fc72368c894ec2/packages/opencode/src/session/llm/request.ts#L177)
- [Zen 接口](https://github.com/anomalyco/opencode/blob/014614d35b397775e5d397a490fc72368c894ec2/packages/web/src/content/docs/zen.mdx#L57)
- [Go 接口](https://github.com/anomalyco/opencode/blob/014614d35b397775e5d397a490fc72368c894ec2/packages/web/src/content/docs/go.mdx#L285)

Go 文档与当日目录对部分 Qwen 模型的协议声明不一致。本快照按官方客户端的 npm 继承规则处理，
未取得这些模型的真实推理验证。更新时需重新核对两个产品的端点、认证及模型覆盖，并运行协议测试；
`/models` 仅返回 ID，不能用它推断协议。快照不代表账号实时授权或余额。
