# 修复市场情绪仪表盘布局问题

## 问题描述

用户反馈市场情绪仪表盘（ScoreGauge）位置不对。从截图分析，问题在于：

1. **装饰性光晕定位失效**：光晕使用了 `absolute` 定位，但父容器没有 `relative` 定位，导致位置错乱
2. **垂直居中问题**：仪表盘在容器内的垂直对齐可能需要调整

## 修复方案

### 文件: `apps/dsa-web/src/components/report/ReportOverview.tsx`

**修改内容**：

第 201-217 行，给情绪仪表盘容器添加 `relative` 类，并确保内容垂直居中：

```diff
          <div 
-           className="flex-1 flex flex-col min-h-0 rounded-[20px] p-5 overflow-visible"
+           className="relative flex-1 flex flex-col min-h-0 rounded-[20px] p-5 overflow-visible"
            style={{...}}
          >
-           <div className="text-center flex-1 flex flex-col justify-center">
+           <div className="text-center flex-1 flex flex-col justify-center items-center">
              <h3 className="text-sm font-medium text-[rgba(255,255,255,0.8)] mb-4">市场情绪</h3>
              <ScoreGauge score={summary.sentimentScore} size="lg" />
            </div>
```

## 验证步骤

1. 重新构建前端: `cd apps/dsa-web && npm run build`
2. 刷新页面查看市场情绪仪表盘位置是否正确
3. 确认装饰性光晕显示在右下角

## 其他状态确认

✅ **删除功能已接入前端**：
- `HistoryList.tsx` 已导入 `historyApi` 并实现了 `onDelete` prop
- `HomePage.tsx` 已传递 `handleDeleteHistory` 回调
- `historyApi.deleteRecord()` 方法已实现

---

**执行此计划需要授权修改文件**