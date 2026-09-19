import type { ApiKeyConfiguration } from '@/api'

export interface ApiKeyAccountForm extends ApiKeyConfiguration {
  name: string
  apiKey: string
  tier: 'zen' | 'go'
}

export function emptyApiKeyAccountForm(): ApiKeyAccountForm {
  return { name: '', base_url: '', apiKey: '', transport: 'http', tier: 'zen' }
}

export function apiKeyAccountError(form: ApiKeyAccountForm, editing = false, provider = 'openai'): string | undefined {
  if (!editing && !form.name.trim())
    return '请输入账号名称'
  if (provider === 'opencode') {
    if (!editing && !form.apiKey)
      return '请输入 OpenCode API Key'
    if (form.apiKey && (!/^[\x21-\x7E]+$/.test(form.apiKey) || form.apiKey.length > 4096))
      return 'API Key 不能包含空格或控制字符，且不能超过 4096 个字符'
    return undefined
  }
  if (form.base_url.trim().length > 2048)
    return '上游 API 地址不能超过 2048 个字符'
  try {
    const url = new URL(form.base_url)
    const loopback = url.hostname === 'localhost' || url.hostname === '[::1]' || /^127(?:\.\d{1,3}){3}$/.test(url.hostname)
    if ((url.protocol !== 'https:' && !(url.protocol === 'http:' && loopback)) || url.username || url.password || url.search || url.hash)
      return '请输入不含认证、查询参数或片段的 HTTPS 地址'
  }
  catch {
    return '请输入完整的上游 API 地址'
  }
  if (!editing && !form.apiKey)
    return '请输入 API Key'
  if (form.apiKey && (!/^[\x21-\x7E]+$/.test(form.apiKey) || form.apiKey.length > 16384))
    return 'API Key 不能包含空格或控制字符'
  return undefined
}
