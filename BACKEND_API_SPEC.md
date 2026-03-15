# 后端API需求文档

## 删除分析历史记录API

### 需求背景
前端已完成功能UI，需要后端提供API支持删除分析历史记录功能。

### 实现状态
✅ **已实现** - 2024年实现

### API端点

```
DELETE /api/v1/history/{recordId}
```

### 请求参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| recordId | integer | 是 | 分析记录ID（URL路径参数） |

### 请求头

```
Authorization: Bearer {token}
Content-Type: application/json
```

### 响应

#### 成功响应 (200 OK)

```json
{
  "success": true,
  "message": "分析记录已删除"
}
```

#### 错误响应

**404 Not Found**
```json
{
  "error": "not_found",
  "message": "未找到 ID={recordId} 的分析记录"
}
```

**500 Internal Server Error**
```json
{
  "error": "internal_error",
  "message": "删除分析记录失败: {错误详情}"
}
```

### 业务逻辑

1. **记录存在性检查**: 验证recordId是否存在
2. **关联数据清理**: 
   - ✅ 删除关联的回测数据（backtest_results表）
   - ✅ 保留新闻情报（news_intel表）- 可能被其他分析引用
   - ✅ 删除主记录（analysis_history表）
3. **物理删除**: 直接从数据库删除记录，不可恢复

### 数据库操作

```sql
-- 1. 删除关联的回测结果
DELETE FROM backtest_results 
WHERE analysis_history_id = ?;

-- 2. 删除分析历史记录
DELETE FROM analysis_history 
WHERE id = ?;
```

### 前端调用代码

```typescript
// historyApi.ts
export const historyApi = {
  // ... 现有方法
  
  /**
   * 删除分析历史记录
   * @param recordId 记录ID
   */
  deleteRecord: async (recordId: number): Promise<{ success: boolean; message: string }> => {
    const response = await apiClient.delete(`/api/v1/history/${recordId}`);
    return response.data;
  },
};
```

### 实现文件

| 文件路径 | 说明 |
|---------|------|
| `api/v1/schemas/history.py` | `DeleteResponse` 模型定义 |
| `api/v1/endpoints/history.py` | DELETE `/{record_id}` 端点 |
| `src/services/history_service.py` | `delete_history()` 业务方法 |
| `src/storage.py` | `delete_analysis_history()` 数据访问方法 |

### 安全考虑

1. **权限控制**: 当前系统无用户认证，暂不做权限校验
2. **物理删除**: 数据删除后不可恢复，建议未来考虑软删除
3. **审计日志**: 删除操作有日志记录

### 后续扩展

- 批量删除API: `POST /api/v1/history/batch-delete`
- 回收站功能: `GET /api/v1/history/trash` + `POST /api/v1/history/restore`
- 定期清理任务: 自动清理已软删除超过90天的记录

---

## 实现优先级

**优先级: 中** ✅ 已完成

该功能影响用户体验，已在当前迭代中实现。
