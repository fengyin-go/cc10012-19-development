<template>
  <section class="page" data-module="pipeline">
    <header class="page-head">
      <div>
        <h2>分析流水线管理</h2>
        <p class="page-desc">维护分析流水线节点的输入、执行约束与结果记录，为数据处理编排提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记流水线节点</button>
        <button class="btn" type="button" @click="exportRows">导出分析流水线清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无分析流水线数据，可先登记流水线节点</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条分析流水线记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section class="run-panel" data-panel="pipeline-run">
      <header class="run-head">
        <div>
          <h3>流水线运行批次</h3>
          <p class="page-desc">
            按「依赖上游」生成无环运行批次：同一波次内的节点并行，波次之间串行；依赖环路或重复上游会阻止批次启动。
          </p>
        </div>
        <div class="run-actions">
          <label class="run-field">
            <span>冲突判定</span>
            <select v-model="policy" :disabled="busy">
              <option value="latest">取最新批次（新批次优先）</option>
              <option value="pinned">指定批次（旧批次优先）</option>
            </select>
          </label>
          <label v-if="policy === 'pinned'" class="run-field">
            <span>指定旧批次</span>
            <select v-model="pinnedChoice" :disabled="busy">
              <option value="">按节点默认批次</option>
              <option v-for="item in pinnableOptions" :key="item.value" :value="item.value">
                {{ item.label }}
              </option>
            </select>
          </label>
          <label class="run-field">
            <span>演练失败节点（仅启动时生效）</span>
            <select v-model="failNode" :disabled="busy">
              <option value="">不制造失败</option>
              <option v-for="node in failOptions" :key="node" :value="node">
                {{ node }}
              </option>
            </select>
          </label>
          <button class="btn primary" type="button" :disabled="busy" @click="startRun">启动运行</button>
          <button
            class="btn"
            type="button"
            :disabled="busy || !runBatch || runBatch.status !== '已失败'"
            @click="resumeRun"
          >
            重试失败节点
          </button>
          <button class="btn" type="button" :disabled="busy" @click="refreshRun">刷新批次</button>
          <button class="btn" type="button" :disabled="busy" @click="verifyRun">核对批次</button>
          <button class="btn" type="button" :disabled="busy || !runBatch" @click="exportRows">
            导出含批次依据
          </button>
        </div>
      </header>

      <p v-if="runMessage" class="run-message" :class="runFailed ? 'error-text' : 'ok-text'">
        {{ runMessage }}
      </p>

      <div v-if="!runBatch" class="empty-panel">暂无数据</div>

      <template v-else>
        <dl class="run-meta">
          <div><dt>批次编号</dt><dd>{{ runBatch.id }}</dd></div>
          <div><dt>批次状态</dt><dd>{{ runBatch.status }}</dd></div>
          <div><dt>编排策略</dt><dd>{{ policyName(runBatch.policy) }}</dd></div>
          <div><dt>波次 / 节点</dt><dd>{{ runBatch.waves.length }} / {{ runBatch.nodes.length }}</dd></div>
          <div><dt>运行次数</dt><dd>{{ runBatch.runCount }}</dd></div>
          <div><dt>批次依据</dt><dd class="basis-text">{{ runBatch.basis }}</dd></div>
        </dl>

        <h4>波次编排</h4>
        <table class="data-table" data-panel="pipeline-waves">
          <thead>
            <tr>
              <th>波次</th>
              <th>顺序</th>
              <th>节点编号</th>
              <th>依赖上游</th>
              <th>节点状态</th>
              <th>结果批次</th>
              <th>输出目标</th>
              <th>依赖选择与批次依据</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="node in sortedNodes" :key="node.code" :class="rowClass(node)">
              <td>{{ node.wave }}</td>
              <td>{{ node.order }}</td>
              <td>{{ node.code }}</td>
              <td>{{ node.dependsOn.length ? node.dependsOn.join('、') : '无' }}</td>
              <td>{{ node.status }}</td>
              <td>{{ node.resultBatch || '—' }}</td>
              <td>{{ node.outputTarget || '—' }}</td>
              <td class="basis-text">
                <div v-for="selection in node.upstreamSelections" :key="selection.upstreamCode">
                  {{ selection.upstreamCode }} → {{ selection.resultBatch || '未产出' }}
                  （{{ policyName(selection.policy) }}：{{ selection.note }}）
                </div>
                <div>{{ node.basis }}</div>
                <div v-if="node.message" class="node-message">{{ node.message }}</div>
              </td>
            </tr>
          </tbody>
        </table>

        <h4>失败断点</h4>
        <div v-if="!runBatch.breakpoint" class="empty-panel">暂无数据（当前批次没有失败断点）</div>
        <div v-else class="breakpoint" data-panel="pipeline-breakpoint">
          <p>
            断点节点：<strong>{{ runBatch.breakpoint.nodeCode }}</strong>
            · 第 {{ runBatch.breakpoint.wave }} 波 / 顺序 {{ runBatch.breakpoint.order }}
            · {{ runBatch.breakpoint.reason }}
          </p>
          <table class="data-table">
            <thead>
              <tr><th>保留输出节点</th><th>波次</th><th>顺序</th><th>结果批次</th><th>输出目标</th></tr>
            </thead>
            <tbody>
              <tr v-for="item in runBatch.breakpoint.retainedOutputs" :key="item.nodeCode">
                <td>{{ item.nodeCode }}</td>
                <td>{{ item.wave }}</td>
                <td>{{ item.order }}</td>
                <td>{{ item.resultBatch }}</td>
                <td>{{ item.outputTarget || '—' }}</td>
              </tr>
              <tr v-if="!runBatch.breakpoint.retainedOutputs.length">
                <td colspan="5" class="empty-state">暂无数据（断点之前没有已完成的输出）</td>
              </tr>
            </tbody>
          </table>
        </div>

        <h4>结果记录</h4>
        <table class="data-table" data-panel="pipeline-results">
          <thead>
            <tr>
              <th>结果批次</th>
              <th>节点编号</th>
              <th>来源批次</th>
              <th>波次</th>
              <th>顺序</th>
              <th>输出目标</th>
              <th>结果状态</th>
              <th>复核算据</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="record in resultRecords" :key="record.resultBatch">
              <td>{{ record.resultBatch }}</td>
              <td>{{ record.nodeCode }}</td>
              <td>{{ record.sourceBatchId }}</td>
              <td>{{ record.wave }}</td>
              <td>{{ record.order }}</td>
              <td>{{ record.outputTarget || '—' }}</td>
              <td>{{ record.status }}</td>
              <td class="basis-text">{{ record.basis }}</td>
            </tr>
            <tr v-if="!resultRecords.length">
              <td colspan="8" class="empty-state">暂无数据</td>
            </tr>
          </tbody>
        </table>

        <h4>批次核对</h4>
        <div v-if="verification" class="verification" data-panel="pipeline-verification">
          <p>
            核对结果：<strong :class="verification.ok ? 'ok-text' : 'error-text'">
              {{ verification.ok ? '通过' : '未通过' }}
            </strong>
            · 已核对节点 {{ verification.checked }} 个
          </p>
          <p class="basis-text">{{ verification.basis }}</p>
          <ul v-if="verification.missing.length" class="missing-list">
            <li v-for="item in verification.missing" :key="item">{{ item }}</li>
          </ul>
        </div>
        <div v-else class="empty-panel">暂无数据（尚未核对）</div>
      </template>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/local-service'
import {
  getPipelineRunState,
  listResultBatches,
  resumePipelineRun,
  startPipelineRun,
  verifyPipelineRun,
} from '@/api/pipeline-service'
import type { PipelineBatchNode, PipelineBatchVerification, PipelineConflictPolicy } from '@/data/pipeline-types'
import type { EntryRow } from '@/data/types'

const meta = moduleMeta('pipeline')
const columns = ["节点编号", "节点类型", "输入源", "输出目标", "处理脚本", "依赖上游", "超时秒数", "重试次数"]
const actions = ["提交调度", "启动运行", "终止运行", "归档结果"]
const statuses = ["草稿", "待调度", "运行中", "已完成", "已失败"]

const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const runBatch = ref(getPipelineRunState().current)
const resultRecords = ref(getPipelineRunState().results)
const verification = ref<PipelineBatchVerification | null>(runBatch.value?.verification ?? null)
const runMessage = ref('')
const runFailed = ref(false)
const busy = ref(false)
const policy = ref<PipelineConflictPolicy>('latest')
const pinnedChoice = ref('')
const failNode = ref('')
const pinned = ref<Record<string, Record<string, string>>>({})

const batchNodes = computed<PipelineBatchNode[]>(() =>
  runBatch.value ? [...runBatch.value.nodes].sort((left, right) => left.order - right.order) : [],
)
const sortedNodes = computed(() => batchNodes.value)

// 班次演练选项直接来自节点定义，即便当前还没有运行批次也能选。
const failOptions = computed(() =>
  rows.value
    .map((row) => String(row['节点编号'] ?? ''))
    .filter((code) => code !== ''),
)

const pinnableOptions = computed(() => {
  const options: { value: string; label: string }[] = []
  for (const node of batchNodes.value) {
    for (const dep of node.dependsOn) {
      const records = listResultBatches(dep)
      for (const record of records) {
        options.push({
          value: `${node.code}::${dep}::${record.resultBatch}`,
          label: `${node.code} ← ${dep} 的 ${record.resultBatch}`,
        })
      }
    }
  }
  return options
})

const stats = computed(() => [
  { label: "流水线节点总数", value: rows.value.length || total.value },
  { label: "运行中节点", value: rows.value.filter((row) => String(row.status) === '运行中').length },
  { label: "已完成节点", value: rows.value.filter((row) => String(row.status) === '已完成').length },
  { label: "运行批次波次", value: runBatch.value ? runBatch.value.waves.length : 0 },
])

const statusSummary = computed(() =>
  statuses.map((status: string) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function policyName(value: string): string {
  if (value === 'pinned') return '指定批次'
  if (value === 'latest') return '取最新批次'
  return '无可用结果'
}

function rowClass(node: PipelineBatchNode) {
  return {
    'row-failed': node.status === '已失败',
    'row-skipped': node.status === '已跳过',
    'row-done': node.status === '已完成',
  }
}

function resetFilters() {
  filters.value = {}
  reload()
}

function exportRows() {
  // 导出和列表使用同一套筛选条件，明细与页面列表保持一致。
  downloadEntries(meta.key, filters.value)
}

function openCreate() {
  errorMessage.value = '流水线节点登记入口尚未接入审批流'
}

function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  const result = applyAction(meta.key, Number(row.id), action)
  if (!result.ok) {
    errorMessage.value = result.message
    return
  }
  reload()
}

function reload() {
  errorMessage.value = ''
  try {
    const payload = listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分析流水线列表读取失败'
  }
  syncRunState()
}

function syncRunState() {
  const state = getPipelineRunState()
  runBatch.value = state.current
  resultRecords.value = state.results
  verification.value = state.current?.verification ?? null
}

function currentPinned(): Record<string, Record<string, string>> {
  if (policy.value !== 'pinned' || !pinnedChoice.value) {
    return {}
  }
  const [nodeCode, upstreamCode, resultBatch] = pinnedChoice.value.split('::')
  if (!nodeCode || !upstreamCode || !resultBatch) {
    return {}
  }
  return { [nodeCode]: { [upstreamCode]: resultBatch } }
}

function startRun() {
  busy.value = true
  runMessage.value = ''
  pinned.value = currentPinned()
  const result = startPipelineRun({
    policy: policy.value,
    pinned: pinned.value,
    failNodes: failNode.value ? [failNode.value] : [],
  })
  busy.value = false
  runFailed.value = !result.ok
  runMessage.value = result.message
  syncRunState()
}

function resumeRun() {
  busy.value = true
  runMessage.value = ''
  // 重试不再制造失败：同一节点连续失败会反复停在同一点，恢复应是向前推进的。
  const result = resumePipelineRun()
  busy.value = false
  runFailed.value = !result.ok
  runMessage.value = result.message
  syncRunState()
}

function refreshRun() {
  syncRunState()
  runFailed.value = false
  runMessage.value = runBatch.value ? `已刷新，批次 ${runBatch.value.id} 的依据仍保存在本地` : '暂无数据'
}

function verifyRun() {
  const result = verifyPipelineRun()
  verification.value = result
  syncRunState()
  runFailed.value = !result.ok
  runMessage.value = result.ok
    ? `批次核对通过：${result.basis}`
    : `批次核对未通过：${result.missing.join('；')}`
}

onMounted(reload)
</script>

<style scoped>
.run-panel {
  margin-top: 24px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px 18px;
}
.run-head { display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; }
.run-head h3 { margin: 0 0 4px; }
.run-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-end; }
.run-field { display: flex; flex-direction: column; font-size: 12px; color: var(--muted); gap: 2px; }
.run-field select { border: 1px solid var(--border); border-radius: 6px; padding: 5px 8px; background: #fff; max-width: 240px; }
.run-message { font-size: 12px; margin: 8px 0 0; }
.ok-text { color: #067647; }
.empty-panel { padding: 18px; text-align: center; color: var(--muted); font-size: 13px; background: #f8fafc; border: 1px dashed var(--border); border-radius: 8px; }
.run-meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin: 14px 0 6px; }
.run-meta div { border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; }
.run-meta dt { font-size: 12px; color: var(--muted); }
.run-meta dd { margin: 4px 0 0; font-size: 13px; }
.basis-text { font-size: 12px; color: var(--muted); word-break: break-all; }
.node-message { color: #b42318; }
h4 { margin: 16px 0 8px; font-size: 14px; }
.breakpoint { border: 1px solid #fda29b; background: #fff6f5; border-radius: 8px; padding: 10px 12px; }
.breakpoint p { margin: 0 0 8px; font-size: 13px; }
.verification { border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; background: #f8fafc; }
.verification p { margin: 0 0 6px; font-size: 13px; }
.missing-list { margin: 0; padding-left: 18px; font-size: 12px; color: #b42318; }
.row-failed td { background: #fff1f0; }
.row-skipped td { background: #fffbe6; }
.row-done td { background: #f6ffed; }
</style>
