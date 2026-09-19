<script setup lang="ts">
import type { AccountCreateProvider } from './model'
import { Openai, Xai } from '@boxicons/vue'
import { Code, LayoutGrid } from '@lucide/vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import { PROVIDER_DISPLAY_NAMES } from '@/utils/providers'

withDefaults(
  defineProps<{
    disabled?: boolean
    selected?: AccountCreateProvider | ''
  }>(),
  {
    disabled: false,
  },
)

const emit = defineEmits<{
  select: [provider: AccountCreateProvider]
}>()

const providers = [
  {
    value: 'batch' as const,
    label: '批量导入',
    icon: LayoutGrid,
  },
  {
    value: 'openai' as const,
    label: PROVIDER_DISPLAY_NAMES.openai,
    icon: Openai,
  },
  {
    value: 'xai' as const,
    label: PROVIDER_DISPLAY_NAMES.xai,
    icon: Xai,
  },
  { value: 'opencode' as const, label: PROVIDER_DISPLAY_NAMES.opencode, icon: Code },
]

function selectProvider(value: string) {
  const provider = providers.find(provider => provider.value === value)
  if (provider)
    emit('select', provider.value)
}
</script>

<template>
  <BaseSegmented
    :model-value="selected ?? ''"
    label="选择账号平台"
    :options="providers"
    :disabled="disabled"
    size="lg"
    class="w-full"
    @update:model-value="selectProvider"
  />
</template>
