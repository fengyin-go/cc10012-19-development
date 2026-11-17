/** 分析流水线编排相关的类型：运行批次、依赖选择、批次依据与结果记录。 */

export type PipelineConflictPolicy = 'latest' | 'pinned'

export type PipelineBatchNodeStatus = '待运行' | '运行中' | '已完成' | '已失败' | '已跳过'

export type PipelineBatchStatus = '待启动' | '运行中' | '已完成' | '已失败'

export type PipelineWave = {
  wave: number
  nodeCodes: string[]
}

/** 一个下游节点对某个上游产出的选择：旧批次、新批次都能作为合法输入。 */
export type PipelineDependencySelection = {
  upstreamCode: string
  resultBatch: string
  outputTarget: string
  /** latest = 自动取最新批次；pinned = 人工指定批次；none = 上游尚未产出。 */
  policy: PipelineConflictPolicy | 'none'
  candidateBatches: string[]
  /** 便于页面直接展示「新批次 / 旧批次 / 等待上游产出」。 */
  note: string
}

export type PipelineBatchNode = {
  id: number
  code: string
  type: string
  inputSource: string
  outputTarget: string
  script: string
  dependsOn: string[]
  wave: number
  order: number
  status: PipelineBatchNodeStatus
  resultBatch: string
  upstreamSelections: PipelineDependencySelection[]
  basis: string
  message: string
}

export type PipelineRetainedOutput = {
  nodeCode: string
  outputTarget: string
  resultBatch: string
  wave: number
  order: number
}

export type PipelineBreakpoint = {
  nodeCode: string
  wave: number
  order: number
  reason: string
  /** 断点之前已经完整产出的上游输出，恢复时不能再写一次。 */
  retainedOutputs: PipelineRetainedOutput[]
}

export type PipelineBatchVerification = {
  ok: boolean
  checked: number
  missing: string[]
  basis: string
}

export type PipelineRunBatch = {
  id: string
  status: PipelineBatchStatus
  createdAt: string
  updatedAt: string
  policy: PipelineConflictPolicy
  basis: string
  fingerprint: string
  waves: PipelineWave[]
  nodes: PipelineBatchNode[]
  breakpoint: PipelineBreakpoint | null
  retainedOutputs: PipelineRetainedOutput[]
  verification: PipelineBatchVerification
  /** 人工指定上游结果批次时记录 nodeCode -> { upstreamCode: resultBatch }。 */
  pinned: Record<string, Record<string, string>>
  runCount: number
}

/** 节点每次成功执行都会留下一条结果记录；旧记录不会被覆盖，可继续作为合法输入。 */
export type PipelineOutputResult = {
  resultBatch: string
  /** 递增序号，用来判断新旧批次；越大越新。 */
  sequence: number
  nodeCode: string
  nodeId: number
  sourceBatchId: string
  outputTarget: string
  wave: number
  order: number
  basis: string
  createdAt: string
  status: '有效'
}

export type PipelineRunState = {
  current: PipelineRunBatch | null
  results: PipelineOutputResult[]
  batchSeq: number
  resultSeq: number
}

export type PipelineRunBatchResult = {
  ok: boolean
  message: string
  batch: PipelineRunBatch | null
}
