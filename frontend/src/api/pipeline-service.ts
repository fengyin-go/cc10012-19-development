import { listRows } from '@/data/local-store'
import type { EntryRow } from '@/data/types'
import type {
  PipelineBatchNode,
  PipelineBatchNodeStatus,
  PipelineBatchVerification,
  PipelineConflictPolicy,
  PipelineDependencySelection,
  PipelineOutputResult,
  PipelineRetainedOutput,
  PipelineRunBatch,
  PipelineRunBatchResult,
  PipelineRunState,
  PipelineWave,
} from '@/data/pipeline-types'

// 运行批次单独持久化：刷新、重开浏览器后仍能读到同一份编排与依据。
const STORAGE_KEY = 'data-explore-studio:pipeline-run'
const RESULT_STATUS: PipelineOutputResult['status'] = '有效'

type GraphNode = {
  id: number
  code: string
  type: string
  inputSource: string
  outputTarget: string
  script: string
  dependsOn: string[]
}

type GraphRead = {
  nodes: GraphNode[]
  issues: string[]
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function emptyState(): PipelineRunState {
  return { current: null, results: [], batchSeq: 0, resultSeq: 0 }
}

function normalizeState(raw: unknown): PipelineRunState {
  if (!raw || typeof raw !== 'object') {
    return emptyState()
  }
  const value = raw as Partial<PipelineRunState>
  const results = Array.isArray(value.results)
    ? value.results.filter(
        (item): item is PipelineOutputResult =>
          Boolean(item) && typeof item === 'object' && typeof item.resultBatch === 'string',
      )
    : []
  return {
    current:
      value.current && typeof value.current === 'object'
        ? (value.current as PipelineRunBatch)
        : null,
    results,
    batchSeq: Number.isFinite(Number(value.batchSeq)) ? Number(value.batchSeq) : 0,
    resultSeq: Number.isFinite(Number(value.resultSeq)) ? Number(value.resultSeq) : 0,
  }
}

function readState(): PipelineRunState {
  if (typeof window === 'undefined' || !window.localStorage) {
    return emptyState()
  }
  const raw = window.localStorage.getItem(STORAGE_KEY)
  if (!raw) {
    return emptyState()
  }
  try {
    return normalizeState(JSON.parse(raw))
  } catch {
    return emptyState()
  }
}

function saveState(state: PipelineRunState): void {
  if (typeof window !== 'undefined' && window.localStorage) {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state))
  }
}

function pad(value: number, length = 4): string {
  return String(value).padStart(length, '0')
}

function nowIso(): string {
  return new Date().toISOString()
}

/** 依赖上游用逗号、顿号、空格或斜杠分隔；空值与「无」表示根节点。 */
export function dependencyTokens(raw: unknown): string[] {
  return String(raw ?? '')
    .split(/[,，、;；\s|/]+/)
    .map((token) => token.trim())
    .filter((token) => token !== '' && token !== '无' && token !== '—' && token !== '-')
}

function pipelineRows(): EntryRow[] {
  return listRows('pipeline')
}

function readGraph(): GraphRead {
  const rows = pipelineRows()
  const nodes: GraphNode[] = rows.map((row) => ({
    id: Number(row.id),
    code: String(row['节点编号'] ?? '').trim(),
    type: String(row['节点类型'] ?? ''),
    inputSource: String(row['输入源'] ?? ''),
    outputTarget: String(row['输出目标'] ?? '').trim(),
    script: String(row['处理脚本'] ?? ''),
    dependsOn: dependencyTokens(row['依赖上游']),
  }))

  const issues: string[] = []
  const byCode = new Map<string, GraphNode>()
  for (const node of nodes) {
    if (!node.code) {
      issues.push(`存在没有节点编号的流水线节点（id=${node.id}）`)
      continue
    }
    if (byCode.has(node.code)) {
      issues.push(`节点编号重复：${node.code}`)
    } else {
      byCode.set(node.code, node)
    }
  }

  for (const node of nodes) {
    const seen = new Set<string>()
    for (const dep of node.dependsOn) {
      if (seen.has(dep)) {
        issues.push(`节点 ${node.code} 重复依赖上游 ${dep}，已阻止批次启动`)
      }
      seen.add(dep)
      if (dep === node.code) {
        issues.push(`节点 ${node.code} 依赖自身，依赖图存在环路`)
      } else if (!byCode.has(dep)) {
        issues.push(`节点 ${node.code} 的上游 ${dep} 不存在`)
      }
    }
  }

  // Kahn 拓扑排序：排不完的部分就是环，批次不能启动。
  const indegree = new Map<string, number>()
  const downstream = new Map<string, string[]>()
  for (const node of nodes) {
    if (!byCode.has(node.code)) continue
    const validDeps = [...new Set(node.dependsOn)].filter(
      (dep) => dep !== node.code && byCode.has(dep),
    )
    indegree.set(node.code, validDeps.length)
    for (const dep of validDeps) {
      const list = downstream.get(dep) ?? []
      list.push(node.code)
      downstream.set(dep, list)
    }
  }
  const queue = [...indegree.entries()].filter(([, degree]) => degree === 0).map(([code]) => code)
  const visited = new Set<string>()
  while (queue.length) {
    const code = queue.shift() as string
    visited.add(code)
    for (const next of downstream.get(code) ?? []) {
      const degree = (indegree.get(next) ?? 0) - 1
      indegree.set(next, degree)
      if (degree === 0) queue.push(next)
    }
  }
  const cycles = nodes
    .map((node) => node.code)
    .filter((code) => byCode.has(code) && !visited.has(code))
  if (cycles.length > 0) {
    issues.push(`依赖图存在环路（${cycles.join(' → ')}），已阻止批次启动`)
  }

  return { nodes, issues }
}

function fingerprintOf(nodes: GraphNode[]): string {
  const text = nodes
    .filter((node) => node.code)
    .map(
      (node) =>
        `${node.code}|${node.type}|${node.inputSource}|${node.outputTarget}|${node.script}|${[...new Set(node.dependsOn)].sort().join('+')}`,
    )
    .sort()
    .join(';')
  let hash = 2166136261
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return `FP-${(hash >>> 0).toString(16).padStart(8, '0')}`
}

function buildWaves(nodes: GraphNode[]): { waves: PipelineWave[]; ordered: GraphNode[] } {
  const byCode = new Map(nodes.map((node) => [node.code, node]))
  const waveOf = new Map<string, number>()
  const resolving = new Set<string>()

  function resolve(code: string): number {
    if (waveOf.has(code)) return waveOf.get(code) as number
    if (resolving.has(code)) return 1
    resolving.add(code)
    const node = byCode.get(code)
    const deps = [...new Set((node?.dependsOn ?? []).filter((dep) => dep !== code && byCode.has(dep)))]
    const wave = deps.length ? Math.max(...deps.map((dep) => resolve(dep))) + 1 : 1
    resolving.delete(code)
    waveOf.set(code, wave)
    return wave
  }

  const ordered = [...nodes].sort((left, right) => {
    const diff = resolve(left.code) - resolve(right.code)
    return diff !== 0 ? diff : left.id - right.id
  })

  const waveMap = new Map<number, string[]>()
  for (const node of ordered) {
    const wave = resolve(node.code)
    const list = waveMap.get(wave) ?? []
    list.push(node.code)
    waveMap.set(wave, list)
  }
  const waves = [...waveMap.entries()]
    .sort((left, right) => left[0] - right[0])
    .map(([wave, nodeCodes]) => ({ wave, nodeCodes }))
  return { waves, ordered }
}

function policyLabel(policy: PipelineConflictPolicy): string {
  return policy === 'pinned' ? '指定批次' : '取最新批次'
}

function buildBatch(
  state: PipelineRunState,
  graph: GraphRead,
  policy: PipelineConflictPolicy,
  pinned: Record<string, Record<string, string>>,
): PipelineRunBatch {
  const { waves, ordered } = buildWaves(graph.nodes)
  const waveOf = new Map<string, number>()
  for (const item of waves) {
    for (const code of item.nodeCodes) waveOf.set(code, item.wave)
  }
  const nodes: PipelineBatchNode[] = ordered.map((node, index) => ({
    id: node.id,
    code: node.code,
    type: node.type,
    inputSource: node.inputSource,
    outputTarget: node.outputTarget,
    script: node.script,
    dependsOn: [...new Set(node.dependsOn)],
    wave: waveOf.get(node.code) ?? 1,
    order: index + 1,
    status: '待运行' as PipelineBatchNodeStatus,
    resultBatch: '',
    upstreamSelections: [],
    basis: '等待按依赖图执行',
    message: '',
  }))
  state.batchSeq += 1
  const id = `PRUN-${pad(state.batchSeq)}`
  const createdAt = nowIso()
  const fingerprint = fingerprintOf(graph.nodes)
  return {
    id,
    status: '待启动',
    createdAt,
    updatedAt: createdAt,
    policy,
    basis: `依赖图 ${nodes.length} 个节点 / ${waves.length} 个波次；编排策略=${policyLabel(policy)}；节点依据=${fingerprint}`,
    fingerprint,
    waves,
    nodes,
    breakpoint: null,
    retainedOutputs: [],
    verification: {
      ok: false,
      checked: 0,
      missing: ['批次尚未核对'],
      basis: '批次刚生成，等待核对',
    },
    pinned: clone(pinned),
    runCount: 0,
  }
}

function selectionFor(
  state: PipelineRunState,
  batch: PipelineRunBatch,
  node: PipelineBatchNode,
  upstreamCode: string,
): PipelineDependencySelection {
  const records = state.results
    .filter((item) => item.nodeCode === upstreamCode)
    .sort((left, right) => right.sequence - left.sequence)
  const candidateBatches = records.map((item) => item.resultBatch)
  const pinnedBatch = batch.pinned[node.code]?.[upstreamCode]
  const pinnedRecord = pinnedBatch
    ? records.find((item) => item.resultBatch === pinnedBatch)
    : undefined
  // 同一上游产出的旧批次、新批次都合法；两种冲突判定路线：
  // 取最新批次（latest）优先本批次产出、其次最新历史批次；指定批次（pinned）优先人工指定，其次沿用最旧批次。
  const ownRecord = records.find((item) => item.sourceBatchId === batch.id)
  const oldest = records.length ? records[records.length - 1] : undefined
  const chosen =
    pinnedRecord ?? (batch.policy === 'pinned' ? (oldest ?? ownRecord) : (ownRecord ?? records[0]))

  if (!chosen) {
    return {
      upstreamCode,
      resultBatch: '',
      outputTarget: '',
      policy: 'none',
      candidateBatches,
      note: '上游尚未产出结果，后续节点不能启动',
    }
  }

  const isPinned = Boolean(pinnedRecord) || (batch.policy === 'pinned' && Boolean(oldest))
  const newest = records[0]
  let note = '唯一可用批次'
  if (candidateBatches.length > 1) {
    if (pinnedRecord && newest && chosen.resultBatch !== newest.resultBatch) {
      note = `按指定沿用旧批次（最新批次 ${newest.resultBatch} 仍保留为合法输入）`
    } else if (pinnedRecord) {
      note = '指定批次即当前最新批次'
    } else if (isPinned) {
      note = `指定批次策略沿用最旧批次（最新批次 ${newest.resultBatch} 仍保留为合法输入）`
    } else if (ownRecord) {
      note = `取最新批次：采用本批次产出（另有 ${candidateBatches.length - 1} 个历史批次仍可选用）`
    } else {
      note = `取最新批次：沿用最新历史批次（共 ${candidateBatches.length} 个候选批次）`
    }
  } else if (isPinned) {
    note = '指定批次'
  }
  if (pinnedBatch && !pinnedRecord) {
    note = `指定批次 ${pinnedBatch} 已不存在，按${batch.policy === 'pinned' ? '最旧' : '最新'}批次回退`
  }
  return {
    upstreamCode,
    resultBatch: chosen.resultBatch,
    outputTarget: chosen.outputTarget,
    policy: isPinned ? 'pinned' : 'latest',
    candidateBatches,
    note,
  }
}

function nodeBasis(batch: PipelineRunBatch, node: PipelineBatchNode): string {
  const parts = node.upstreamSelections.map(
    (item) => `${item.upstreamCode}@${item.resultBatch}（${item.note}）`,
  )
  const depText = parts.length ? `上游选择：${parts.join(' + ')}` : '根节点无上游'
  return `第${node.wave}波/顺序${node.order}；${depText}；编排策略=${policyLabel(batch.policy)}；${batch.fingerprint}`
}

function recordOutput(
  state: PipelineRunState,
  batch: PipelineRunBatch,
  node: PipelineBatchNode,
): PipelineOutputResult {
  // 幂等保护：同一批次里已经成功过的节点不再写第二条结果，下游不会读到重复批次。
  const existing = state.results.find(
    (item) => item.sourceBatchId === batch.id && item.nodeCode === node.code,
  )
  if (existing) {
    return existing
  }
  state.resultSeq += 1
  const sequence = state.resultSeq
  const record: PipelineOutputResult = {
    resultBatch: `RES-${pad(sequence, 6)}-${node.code}`,
    sequence,
    nodeCode: node.code,
    nodeId: node.id,
    sourceBatchId: batch.id,
    outputTarget: node.outputTarget,
    wave: node.wave,
    order: node.order,
    basis: nodeBasis(batch, node),
    createdAt: nowIso(),
    status: RESULT_STATUS,
  }
  state.results.push(record)
  return record
}

/** 断点保留的是已完整产出的节点输出；恢复时直接跳过它们，不会再写一次。 */
function computeRetained(batch: PipelineRunBatch): PipelineRetainedOutput[] {
  return batch.nodes
    .filter((node) => node.status === '已完成' && node.resultBatch)
    .sort((left, right) => left.order - right.order)
    .map((node) => ({
      nodeCode: node.code,
      outputTarget: node.outputTarget,
      resultBatch: node.resultBatch,
      wave: node.wave,
      order: node.order,
    }))
}

function markSkipped(batch: PipelineRunBatch, reason: string): void {
  for (const node of batch.nodes) {
    if (node.status === '待运行') {
      node.status = '已跳过'
      node.message = reason
    }
  }
}

function executeBatch(
  state: PipelineRunState,
  batch: PipelineRunBatch,
  failNodes: string[],
): void {
  batch.status = '运行中'
  batch.updatedAt = nowIso()
  batch.runCount += 1
  const failed = new Set(failNodes)
  const byCode = new Map(batch.nodes.map((node) => [node.code, node]))
  const ordered = [...batch.nodes].sort((left, right) => left.order - right.order)

  for (const node of ordered) {
    if (node.status === '已完成') {
      // 恢复时直接跳过已成功节点，绝不重复写结果。
      continue
    }
    const missingUpstream = node.dependsOn.filter(
      (dep) => byCode.get(dep)?.status !== '已完成',
    )
    if (missingUpstream.length > 0) {
      node.status = '已跳过'
      node.message = `上游 ${missingUpstream.join('、')} 未完成，节点停在断点`
      continue
    }

    node.upstreamSelections = node.dependsOn.map((dep) => selectionFor(state, batch, node, dep))
    const missingResult = node.upstreamSelections.filter((item) => item.policy === 'none')
    if (missingResult.length > 0) {
      node.status = '已跳过'
      node.message = `上游 ${missingResult.map((item) => item.upstreamCode).join('、')} 没有可用的完成结果`
      continue
    }
    node.basis = nodeBasis(batch, node)

    if (failed.has(node.code)) {
      node.status = '已失败'
      node.message = '节点执行失败，批次停在断点'
      batch.status = '已失败'
      batch.breakpoint = {
        nodeCode: node.code,
        wave: node.wave,
        order: node.order,
        reason: `节点 ${node.code} 执行失败，后续节点停在断点等待重试`,
        retainedOutputs: [],
      }
      // 出错即停在断点：后续节点一律不再执行，已成功的输出保留下来。
      for (const other of ordered) {
        if (other.order > node.order && other.status !== '已完成') {
          other.status = '已跳过'
          other.message = `批次在 ${node.code} 处失败，节点停在断点`
        }
      }
      break
    }

    const record = recordOutput(state, batch, node)
    node.status = '已完成'
    node.resultBatch = record.resultBatch
    node.message = `已产出到 ${node.outputTarget || '（未登记输出目标）'}`
  }

  if (batch.status === '已失败' && batch.breakpoint) {
    batch.breakpoint.retainedOutputs = computeRetained(batch)
    batch.retainedOutputs = clone(batch.breakpoint.retainedOutputs)
    markSkipped(batch, `批次在 ${batch.breakpoint.nodeCode} 处失败，等待重试后继续`)
  } else {
    batch.status = '已完成'
    batch.breakpoint = null
    batch.retainedOutputs = []
  }
  batch.updatedAt = nowIso()
}

function summarize(batch: PipelineRunBatch): string {
  if (batch.status === '已完成') {
    return `运行批次 ${batch.id} 已完成，${batch.nodes.length} 个节点按 ${batch.waves.length} 个波次产出`
  }
  if (batch.status === '已失败' && batch.breakpoint) {
    return `运行批次 ${batch.id} 在 ${batch.breakpoint.nodeCode} 处失败，保留 ${batch.retainedOutputs.length} 个已成功输出，可重试`
  }
  return `运行批次 ${batch.id} 当前状态「${batch.status}」`
}

export function startPipelineRun(
  options: {
    policy?: PipelineConflictPolicy
    pinned?: Record<string, Record<string, string>>
    failNodes?: string[]
  } = {},
): PipelineRunBatchResult {
  const graph = readGraph()
  if (graph.nodes.length === 0) {
    return { ok: false, message: '暂无流水线节点，无法生成运行批次', batch: null }
  }
  if (graph.issues.length > 0) {
    return { ok: false, message: graph.issues.join('；'), batch: null }
  }

  // 连续提交只保留一份运行批次：新批次直接替换旧的，不会叠加。
  const state = readState()
  const policy: PipelineConflictPolicy = options.policy === 'pinned' ? 'pinned' : 'latest'
  const batch = buildBatch(state, graph, policy, options.pinned ?? {})
  state.current = batch
  executeBatch(state, batch, options.failNodes ?? [])
  batch.verification = verifyBatch(state, batch)
  saveState(state)
  return { ok: true, message: summarize(batch), batch: clone(batch) }
}

export function resumePipelineRun(failNodes: string[] = []): PipelineRunBatchResult {
  const state = readState()
  const batch = state.current
  if (!batch) {
    return { ok: false, message: '暂无运行批次，请先启动运行', batch: null }
  }
  if (batch.status === '已完成') {
    return { ok: false, message: `运行批次 ${batch.id} 已全部完成，无需重试`, batch: clone(batch) }
  }
  if (batch.status !== '已失败') {
    return {
      ok: false,
      message: `运行批次 ${batch.id} 当前为「${batch.status}」，不能继续运行`,
      batch: clone(batch),
    }
  }

  // 只把失败节点和停在断点的节点放回待运行；已成功节点保持原结果批次。
  for (const node of batch.nodes) {
    if (node.status === '已完成') continue
    node.status = '待运行'
    node.resultBatch = ''
    node.upstreamSelections = []
    node.basis = '等待按依赖图执行'
    node.message = ''
  }
  batch.breakpoint = null
  batch.retainedOutputs = []
  executeBatch(state, batch, failNodes)
  batch.verification = verifyBatch(state, batch)
  saveState(state)
  return { ok: true, message: summarize(batch), batch: clone(batch) }
}

export function getPipelineRunState(): PipelineRunState {
  return clone(readState())
}

export function listResultBatches(upstreamCode: string): PipelineOutputResult[] {
  return readState()
    .results.filter((item) => item.nodeCode === upstreamCode)
    .sort((left, right) => right.sequence - left.sequence)
    .map((item) => clone(item))
}

export function pipelineExportSections(): string[] {
  const state = readState()
  const batch = state.current
  const lines: string[] = []
  lines.push('')
  lines.push('分析流水线运行批次')
  if (!batch) {
    lines.push('批次状态,暂无数据')
    lines.push('说明,当前没有运行批次，可在流水线页面启动运行')
    return lines
  }
  lines.push('批次编号,批次状态,编排策略,波次数,节点数,运行次数,批次依据')
  lines.push(
    csvRow([
      batch.id,
      batch.status,
      policyLabel(batch.policy),
      batch.waves.length,
      batch.nodes.length,
      batch.runCount,
      batch.basis,
    ]),
  )
  lines.push('')
  lines.push('波次编排（节点顺序、依赖上游与输出目标）')
  lines.push('波次,顺序,节点编号,依赖上游,节点状态,结果批次,输出目标,批次依据')
  for (const node of [...batch.nodes].sort((left, right) => left.order - right.order)) {
    lines.push(
      csvRow([
        node.wave,
        node.order,
        node.code,
        node.dependsOn.join('|') || '无',
        node.status,
        node.resultBatch || '—',
        node.outputTarget || '—',
        node.basis,
      ]),
    )
  }
  lines.push('')
  lines.push('依赖选择（同一上游的旧批次与新批次都可作为合法输入）')
  lines.push('节点编号,上游节点,采用批次,候选批次,选择策略,说明')
  for (const node of batch.nodes) {
    for (const selection of node.upstreamSelections) {
      lines.push(
        csvRow([
          node.code,
          selection.upstreamCode,
          selection.resultBatch || '—',
          selection.candidateBatches.join('|') || '—',
          selection.policy === 'pinned'
            ? '指定批次'
            : selection.policy === 'latest'
              ? '取最新批次'
              : '无可用结果',
          selection.note,
        ]),
      )
    }
  }
  lines.push('')
  lines.push('运行结果（不同结果批次不合并进同一波次）')
  lines.push('波次,结果批次,来源批次,节点编号,节点顺序,输出目标')
  const completed = batch.nodes
    .filter((node) => node.status === '已完成' && node.resultBatch)
    .sort((left, right) => left.order - right.order)
  for (const node of completed) {
    lines.push(
      csvRow([
        node.wave,
        node.resultBatch,
        batch.id,
        node.code,
        node.order,
        node.outputTarget || '—',
      ]),
    )
  }
  if (completed.length === 0) {
    lines.push('—,暂无数据')
  }
  lines.push('')
  lines.push('失败断点')
  if (!batch.breakpoint) {
    lines.push('断点,无失败断点')
  } else {
    lines.push('节点编号,波次,顺序,原因')
    lines.push(
      csvRow([
        batch.breakpoint.nodeCode,
        batch.breakpoint.wave,
        batch.breakpoint.order,
        batch.breakpoint.reason,
      ]),
    )
    lines.push('')
    lines.push('断点保留的完整输出')
    lines.push('节点编号,波次,顺序,结果批次,输出目标')
    if (batch.breakpoint.retainedOutputs.length === 0) {
      lines.push('—,暂无数据')
    }
    for (const item of batch.breakpoint.retainedOutputs) {
      lines.push(csvRow([item.nodeCode, item.wave, item.order, item.resultBatch, item.outputTarget]))
    }
  }
  lines.push('')
  lines.push('结果记录（可复核）')
  lines.push('结果批次,节点编号,来源批次,波次,顺序,输出目标,结果状态,复核算据')
  if (state.results.length === 0) {
    lines.push('—,暂无数据')
  }
  for (const record of [...state.results].sort((left, right) => left.sequence - right.sequence)) {
    lines.push(
      csvRow([
        record.resultBatch,
        record.nodeCode,
        record.sourceBatchId,
        record.wave,
        record.order,
        record.outputTarget,
        record.status,
        record.basis,
      ]),
    )
  }
  lines.push('')
  lines.push('批次核对')
  lines.push('核对结果,已核对节点,缺失项,核对依据')
  lines.push(
    csvRow([
      batch.verification.ok ? '通过' : '未通过',
      batch.verification.checked,
      batch.verification.missing.join('|') || '无',
      batch.verification.basis,
    ]),
  )
  return lines
}

function csvRow(values: (string | number)[]): string {
  return values
    .map((value) => {
      const text = String(value ?? '')
      return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text
    })
    .join(',')
}

function verifyBatch(state: PipelineRunState, batch: PipelineRunBatch): PipelineBatchVerification {
  const missing: string[] = []
  const graph = readGraph()
  if (graph.issues.length > 0) {
    missing.push(...graph.issues)
  } else if (fingerprintOf(graph.nodes) !== batch.fingerprint) {
    missing.push('节点定义已变更，运行批次的批次依据已失效')
  }

  const ordered = [...batch.nodes].sort((left, right) => left.order - right.order)
  ordered.forEach((node, index) => {
    if (node.order !== index + 1) {
      missing.push(`批内节点顺序不连续：${node.code}`)
    }
  })

  const waveCodes = new Map<number, string[]>()
  for (const wave of batch.waves) {
    waveCodes.set(wave.wave, [...wave.nodeCodes].sort())
    for (const code of wave.nodeCodes) {
      const node = batch.nodes.find((item) => item.code === code)
      if (!node) {
        missing.push(`波次 ${wave.wave} 引用了不存在的节点 ${code}`)
      } else if (node.wave !== wave.wave) {
        missing.push(`节点 ${code} 的波次与波次编排不一致`)
      }
    }
  }
  let lastWave = 0
  for (const node of ordered) {
    if (node.wave < lastWave) {
      missing.push(`节点 ${node.code} 的波次顺序倒置`)
    }
    lastWave = Math.max(lastWave, node.wave)
    if (!(waveCodes.get(node.wave) ?? []).includes(node.code)) {
      missing.push(`节点 ${node.code} 未登记在第 ${node.wave} 波`)
    }
    for (const dep of node.dependsOn) {
      const upstream = batch.nodes.find((item) => item.code === dep)
      if (!upstream) {
        missing.push(`节点 ${node.code} 的依赖上游 ${dep} 不在批次中`)
      } else if (upstream.order >= node.order) {
        missing.push(`节点 ${node.code} 的上游 ${dep} 未排在其之前`)
      }
    }
    if (!node.outputTarget) {
      missing.push(`节点 ${node.code} 缺少输出目标`)
    }
    const sourceRow = pipelineRows().find((row) => Number(row.id) === node.id)
    if (sourceRow && String(sourceRow['输出目标'] ?? '') !== node.outputTarget) {
      missing.push(`节点 ${node.code} 的输出目标与节点定义不一致`)
    }
    if (node.status === '已完成') {
      if (!node.resultBatch) {
        missing.push(`节点 ${node.code} 已完成但缺少结果批次`)
        continue
      }
      const record = state.results.find(
        (item) => item.resultBatch === node.resultBatch && item.nodeCode === node.code,
      )
      if (!record) {
        missing.push(`节点 ${node.code} 的结果记录缺失（${node.resultBatch}）`)
      } else if (record.outputTarget !== node.outputTarget) {
        missing.push(`节点 ${node.code} 的结果记录输出目标不一致`)
      } else if (record.sourceBatchId !== batch.id) {
        missing.push(`节点 ${node.code} 的结果记录不属于运行批次 ${batch.id}`)
      }
    }
  }

  const targetCount = new Map<string, number>()
  for (const node of batch.nodes) {
    if (!node.outputTarget) continue
    targetCount.set(node.outputTarget, (targetCount.get(node.outputTarget) ?? 0) + 1)
  }
  for (const [target, count] of targetCount) {
    if (count > 1) missing.push(`输出目标 ${target} 被 ${count} 个节点重复占用`)
  }

  if (batch.status === '已失败') {
    if (!batch.breakpoint) {
      missing.push('批次已失败但缺少失败断点')
    } else {
      const failedNode = batch.nodes.find((item) => item.code === batch.breakpoint?.nodeCode)
      if (!failedNode) {
        missing.push(`失败断点 ${batch.breakpoint.nodeCode} 不在批次中`)
      } else if (failedNode.status !== '已失败') {
        missing.push(`失败断点 ${failedNode.code} 的状态不是已失败`)
      }
      const retainedCodes = new Set(batch.breakpoint.retainedOutputs.map((item) => item.nodeCode))
      for (const retained of batch.breakpoint.retainedOutputs) {
        const node = batch.nodes.find((item) => item.code === retained.nodeCode)
        if (!node || node.status !== '已完成') {
          missing.push(`断点保留输出 ${retained.nodeCode} 没有对应的已完成节点`)
        } else if (node.resultBatch !== retained.resultBatch) {
          missing.push(`断点保留输出 ${retained.nodeCode} 的结果批次不一致`)
        }
      }
      for (const node of batch.nodes) {
        if (node.status === '已完成' && !retainedCodes.has(node.code)) {
          missing.push(`已成功输出 ${node.code} 未保留在断点记录中`)
        }
      }
    }
  }
  if (batch.status === '已完成') {
    const unfinished = batch.nodes.filter((node) => node.status !== '已完成')
    if (unfinished.length > 0) {
      missing.push(`批次已完成但仍有未完成节点：${unfinished.map((node) => node.code).join('、')}`)
    }
    if (batch.breakpoint) {
      missing.push('批次已完成但仍保留失败断点')
    }
  }

  const resultCount = batch.nodes.filter((node) => node.status === '已完成').length
  return {
    ok: missing.length === 0,
    checked: batch.nodes.length,
    missing,
    basis: `已核对 ${batch.nodes.length} 个节点 / ${batch.waves.length} 个波次 / ${targetCount.size} 个输出目标 / ${resultCount} 条批次结果；节点依据=${batch.fingerprint}`,
  }
}

export function verifyPipelineRun(): PipelineBatchVerification {
  const state = readState()
  const batch = state.current
  if (!batch) {
    return {
      ok: false,
      checked: 0,
      missing: ['暂无运行批次'],
      basis: '暂无流水线运行批次，暂无可核对依据',
    }
  }
  const verification = verifyBatch(state, batch)
  batch.verification = verification
  saveState(state)
  return clone(verification)
}

export function pipelineNodeStatuses(): PipelineBatchNodeStatus[] {
  return ['待运行', '运行中', '已完成', '已失败', '已跳过']
}
