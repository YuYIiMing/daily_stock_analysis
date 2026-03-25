import type React from 'react';
import { RefreshCcw } from 'lucide-react';
import { Button, Card } from '../common';
import type { QuantSyncStatusSummary } from '../../types/quantStrategy';

type SyncActionStatus = 'ok' | 'partial' | 'error' | null;

interface QuantSyncStatusCardProps {
  status: QuantSyncStatusSummary | null;
  isLoading: boolean;
  isSyncing: boolean;
  syncingMode: 'latest' | 'full' | null;
  errorMessage?: string | null;
  apiMessage?: string | null;
  actionMessage?: string | null;
  actionStatus?: SyncActionStatus;
  onRetry: () => void;
  onSyncLatest: () => void;
  onSyncFull: () => void;
}

function formatDate(value: string | null | undefined): string {
  return value && value.trim() ? value : '--';
}

function formatCount(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '--';
  return `${value} 个`;
}

function isStatusEmpty(status: QuantSyncStatusSummary | null): boolean {
  if (!status) return true;
  return (
    status.conceptBoardCoverageCount == null
    && status.membershipDistinctCodes == null
    && status.latestBoardDate == null
    && status.latestBoardCount == null
    && status.stockPoolSize == null
    && status.latestStockFeatureDate == null
    && status.latestStockFeatureCount == null
    && status.latestIndexFeatureDate == null
  );
}

const SyncMetricItem: React.FC<{ label: string; value: string; hint?: string }> = ({ label, value, hint }) => (
  <div className="rounded-lg border border-white/8 bg-white/4 p-3">
    <p className="text-xs text-content-tertiary">{label}</p>
    <p className="mt-1 font-mono text-sm font-semibold text-content-primary">{value}</p>
    {hint ? <p className="mt-1 text-xs text-content-quaternary">{hint}</p> : null}
  </div>
);

function getActionAlertClassName(status: SyncActionStatus): string {
  if (status === 'error') {
    return 'border-semantic-danger/35 bg-semantic-danger-subtle text-semantic-danger';
  }
  if (status === 'partial') {
    return 'border-semantic-warning/35 bg-semantic-warning-subtle text-content-primary';
  }
  return 'border-semantic-success/35 bg-semantic-success-subtle text-content-primary';
}

function buildProgressSummary(status: QuantSyncStatusSummary | null): { title: string; detail: string } | null {
  if (!status) return null;

  const latestDates = [
    status.latestBoardDate,
    status.latestMembershipDate,
    status.latestStockFeatureDate,
    status.latestIndexFeatureDate,
  ].filter((value): value is string => typeof value === 'string' && value.trim().length > 0);
  const targetDate = latestDates.length > 0 ? latestDates.sort().slice(-1)[0] : null;
  const dailyReady = Boolean(
    targetDate
    && status.latestBoardDate === targetDate
    && status.latestMembershipDate === targetDate
    && status.latestStockFeatureDate === targetDate
    && status.latestIndexFeatureDate === targetDate,
  );

  const stockPoolSize = typeof status.stockPoolSize === 'number' ? status.stockPoolSize : null;
  const stockDailyDistinctCodes = typeof status.stockDailyDistinctCodes === 'number' ? status.stockDailyDistinctCodes : null;
  const hasFullCoverageProgress = Boolean(stockPoolSize && stockDailyDistinctCodes != null);

  if (!dailyReady && !hasFullCoverageProgress) {
    return null;
  }

  if (dailyReady && hasFullCoverageProgress && stockPoolSize && stockDailyDistinctCodes && stockDailyDistinctCodes >= stockPoolSize) {
    return {
      title: '当日数据已完成，全量历史也已补齐',
      detail: `最新快照已到 ${targetDate}，且已落库日线股票数 ${stockDailyDistinctCodes} / ${stockPoolSize}。`,
    };
  }

  if (dailyReady && hasFullCoverageProgress && stockPoolSize != null && stockDailyDistinctCodes != null) {
    return {
      title: '当日数据已完成，全量历史补齐中',
      detail: `最新快照已到 ${targetDate}，但全量历史覆盖仍在补齐：${stockDailyDistinctCodes} / ${stockPoolSize}。`,
    };
  }

  if (dailyReady && targetDate) {
    return {
      title: '当日数据已完成',
      detail: `板块、归属、个股特征和指数特征都已更新到 ${targetDate}。`,
    };
  }

  return null;
}

export const QuantSyncStatusCard: React.FC<QuantSyncStatusCardProps> = ({
  status,
  isLoading,
  isSyncing,
  syncingMode,
  errorMessage,
  apiMessage,
  actionMessage,
  actionStatus,
  onRetry,
  onSyncLatest,
  onSyncFull,
}) => {
  const empty = isStatusEmpty(status);
  const boardDate = formatDate(status?.latestBoardDate);
  const stockFeatureDate = formatDate(status?.latestStockFeatureDate);
  const indexDate = formatDate(status?.latestIndexFeatureDate);
  const actionClassName = getActionAlertClassName(actionStatus ?? null);
  const progressSummary = buildProgressSummary(status);

  return (
    <Card variant="bordered" padding="md" className="border-white/10">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="text-sm font-semibold text-content-primary">同步状态 / 数据覆盖</p>
          <p className="mt-1 text-xs text-content-tertiary">
            可以在这里手动执行最新日同步或全窗口重同步，并观察页面数据是否已切到最新快照。
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button variant="ghost" size="sm" onClick={onRetry} isLoading={isLoading} disabled={isSyncing}>
            <RefreshCcw className="h-3.5 w-3.5" />
            刷新状态
          </Button>
          <Button
            variant="aurora"
            size="sm"
            onClick={onSyncLatest}
            isLoading={syncingMode === 'latest'}
            disabled={isSyncing || isLoading}
          >
            仅同步最新日
          </Button>
          <Button
            variant="nebula"
            size="sm"
            onClick={onSyncFull}
            isLoading={syncingMode === 'full'}
            disabled={isSyncing || isLoading}
          >
            全窗口重同步
          </Button>
        </div>
      </div>

      <div className="mt-3 grid gap-2 md:grid-cols-2">
        <div className="rounded-lg border border-white/8 bg-white/4 px-3 py-2">
          <p className="text-xs font-medium text-content-secondary">仅同步最新日</p>
          <p className="mt-1 text-xs text-content-tertiary">
            用于日常收盘后更新最新信号日的板块、归属和个股特征，速度更快。
          </p>
        </div>
        <div className="rounded-lg border border-white/8 bg-white/4 px-3 py-2">
          <p className="text-xs font-medium text-content-secondary">全窗口重同步</p>
          <p className="mt-1 text-xs text-content-tertiary">
            用于历史数据修复或大范围重建；完成后如需更新回测，请再手动运行结构化回测。
          </p>
        </div>
      </div>

      {errorMessage ? (
        <div className="mt-3 rounded-lg border border-semantic-danger/35 bg-semantic-danger-subtle px-3 py-2">
          <p className="text-xs text-semantic-danger">
            同步状态读取失败：{errorMessage}
          </p>
          <p className="mt-1 text-xs text-semantic-danger/90">
            页面数据变化不明显通常是因为当天同步未完成，或接口暂时无法返回最新覆盖结果。
          </p>
        </div>
      ) : null}

      {!errorMessage && empty ? (
        <div className="mt-3 rounded-lg border border-semantic-warning/35 bg-semantic-warning-subtle px-3 py-2">
          <p className="text-xs text-content-primary">
            当前尚未拿到同步覆盖数据。页面数据变化不明显，通常是因为概念板块或个股特征还未完成入库。
          </p>
        </div>
      ) : null}

      {actionMessage ? (
        <div className={`mt-3 rounded-lg border px-3 py-2 ${actionClassName}`}>
          <p className="text-xs font-medium">{actionMessage}</p>
        </div>
      ) : null}

      {apiMessage ? (
        <p className="mt-3 text-xs text-content-tertiary">{apiMessage}</p>
      ) : null}

      {progressSummary ? (
        <div className="mt-3 rounded-lg border border-semantic-info/35 bg-semantic-info-subtle px-3 py-2">
          <p className="text-xs font-medium text-content-primary">{progressSummary.title}</p>
          <p className="mt-1 text-xs text-content-secondary">{progressSummary.detail}</p>
        </div>
      ) : null}

      <div className="mt-3 grid gap-2 md:grid-cols-2 xl:grid-cols-5">
        <SyncMetricItem
          label="概念板块覆盖数"
          value={formatCount(status?.conceptBoardCoverageCount)}
        />
        <SyncMetricItem
          label="概念归属覆盖股票数"
          value={`${formatCount(status?.membershipDistinctCodes)} / ${formatDate(status?.latestMembershipDate)}`}
          hint={typeof status?.latestMembershipCount === 'number' ? `当日归属记录 ${status.latestMembershipCount} 条` : undefined}
        />
        <SyncMetricItem
          label="最新板块日期 / 当日板块数"
          value={`${boardDate} / ${formatCount(status?.latestBoardCount)}`}
        />
        <SyncMetricItem
          label="股票池规模 / 已落库日线股票数"
          value={`${formatCount(status?.stockPoolSize)} / ${formatCount(status?.stockDailyDistinctCodes)}`}
        />
        <SyncMetricItem
          label="最新个股特征日期 / 当日特征股票数"
          value={`${stockFeatureDate} / ${formatCount(status?.latestStockFeatureCount)}`}
        />
        <SyncMetricItem
          label="指数特征最新日期"
          value={indexDate}
        />
      </div>
    </Card>
  );
};
