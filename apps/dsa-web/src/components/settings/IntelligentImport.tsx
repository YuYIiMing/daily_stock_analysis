import type React from 'react';
import { useCallback, useState } from 'react';
import { getParsedApiError } from '../../api/error';
import { stocksApi, type ExtractItem } from '../../api/stocks';
import { systemConfigApi, SystemConfigConflictError } from '../../api/systemConfig';

const IMG_EXT = ['.jpg', '.jpeg', '.png', '.webp', '.gif'];
const IMG_MAX = 5 * 1024 * 1024;
const FILE_MAX = 2 * 1024 * 1024;
const TEXT_MAX = 100 * 1024;

interface IntelligentImportProps {
  stockListValue: string;
  configVersion: string;
  maskToken: string;
  onMerged: () => void;
  disabled?: boolean;
}

type ItemWithChecked = ExtractItem & { id: string; checked: boolean };

function normalizeConfidence(confidence?: string | null): 'high' | 'medium' | 'low' {
  if (confidence === 'high' || confidence === 'low' || confidence === 'medium') {
    return confidence;
  }
  return 'medium';
}

function mergeItems(
  prev: ItemWithChecked[],
  newItems: ExtractItem[]
): ItemWithChecked[] {
  const byCode = new Map<string, ItemWithChecked>();
  const confOrder: Record<'high' | 'medium' | 'low', number> = {
    high: 3,
    medium: 2,
    low: 1,
  };
  const failed: ItemWithChecked[] = [];
  for (const p of prev) {
    if (p.code) {
      byCode.set(p.code, p);
    } else {
      failed.push(p);
    }
  }
  for (const it of newItems) {
    const normalizedConfidence = normalizeConfidence(it.confidence);
    if (it.code) {
      const existing = byCode.get(it.code);
      if (!existing) {
        byCode.set(it.code, {
          ...it,
          confidence: normalizedConfidence,
          id: `${it.code}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
          checked: normalizedConfidence === 'high',
        });
      } else {
        const existingConfidence = normalizeConfidence(existing.confidence);
        const shouldUpgradeConfidence = confOrder[normalizedConfidence] > confOrder[existingConfidence];
        const shouldFillName = !existing.name && !!it.name;

        if (shouldUpgradeConfidence || shouldFillName) {
          byCode.set(it.code, {
            ...existing,
            name: it.name || existing.name,
            confidence: shouldUpgradeConfidence ? normalizedConfidence : existingConfidence,
            checked: shouldUpgradeConfidence
              ? (normalizedConfidence === 'high' ? true : existing.checked)
              : existing.checked,
          });
        }
      }
    } else {
      failed.push({
        ...it,
        confidence: normalizedConfidence,
        id: `fail-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        checked: false,
      });
    }
  }
  return [...byCode.values(), ...failed];
}

export const IntelligentImport: React.FC<IntelligentImportProps> = ({
  stockListValue,
  configVersion,
  maskToken,
  onMerged,
  disabled,
}) => {
  const [items, setItems] = useState<ItemWithChecked[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isMerging, setIsMerging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [pasteText, setPasteText] = useState('');

  const parseCurrentList = useCallback(() => {
    return stockListValue
      .split(',')
      .map((c) => c.trim())
      .filter(Boolean);
  }, [stockListValue]);

  const addItems = useCallback((newItems: ExtractItem[]) => {
    setItems((prev) => mergeItems(prev, newItems));
  }, []);

  const handleImageFile = useCallback(
    async (file: File) => {
      const ext = '.' + (file.name.split('.').pop() ?? '').toLowerCase();
      if (!IMG_EXT.includes(ext)) {
        setError('Image only supports JPG, PNG, WebP, GIF');
        return;
      }
      if (file.size > IMG_MAX) {
        setError('Image size must not exceed 5MB');
        return;
      }
      setError(null);
      setIsLoading(true);
      try {
        const res = await stocksApi.extractFromImage(file);
        addItems(res.items ?? res.codes.map((c) => ({ code: c, name: null, confidence: 'medium' })));
      } catch (e) {
        const parsed = getParsedApiError(e);
        const err = e && typeof e === 'object' ? (e as { response?: { status?: number }; code?: string }) : null;
        let fallback = 'Recognition failed, please retry';
        if (err?.response?.status === 429) fallback = 'Too many requests, please try again later';
        else if (err?.code === 'ECONNABORTED') fallback = 'Request timeout, please check network and retry';
        setError(parsed.message || fallback);
      } finally {
        setIsLoading(false);
      }
    },
    [addItems],
  );

  const handleDataFile = useCallback(
    async (file: File) => {
      if (file.size > FILE_MAX) {
        setError('File size must not exceed 2MB');
        return;
      }
      setError(null);
      setIsLoading(true);
      try {
        const res = await stocksApi.parseImport(file);
        addItems(res.items ?? res.codes.map((c) => ({ code: c, name: null, confidence: 'medium' })));
      } catch (e) {
        const parsed = getParsedApiError(e);
        setError(parsed.message || 'Parse failed');
      } finally {
        setIsLoading(false);
      }
    },
    [addItems],
  );

  const handlePasteParse = useCallback(() => {
    const t = pasteText.trim();
    if (!t) return;
    if (new Blob([t]).size > TEXT_MAX) {
      setError('Pasted text must not exceed 100KB');
      return;
    }
    setError(null);
    setIsLoading(true);
    stocksApi
      .parseImport(undefined, t)
      .then((res) => {
        addItems(res.items ?? res.codes.map((c) => ({ code: c, name: null, confidence: 'medium' })));
        setPasteText('');
      })
      .catch((e) => {
        const parsed = getParsedApiError(e);
        setError(parsed.message || 'Parse failed');
      })
      .finally(() => setIsLoading(false));
  }, [pasteText, addItems]);

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);
      if (disabled || isLoading) return;
      const f = e.dataTransfer?.files?.[0];
      if (!f) return;
      const ext = '.' + (f.name.split('.').pop() ?? '').toLowerCase();
      if (IMG_EXT.includes(ext)) void handleImageFile(f);
      else void handleDataFile(f);
    },
    [disabled, isLoading, handleImageFile, handleDataFile],
  );

  const onImageInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const f = e.target.files?.[0];
      if (f) void handleImageFile(f);
      e.target.value = '';
    },
    [handleImageFile],
  );

  const onDataFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const f = e.target.files?.[0];
      if (f) void handleDataFile(f);
      e.target.value = '';
    },
    [handleDataFile],
  );

  const toggleChecked = useCallback((id: string) => {
    setItems((prev) => prev.map((p) => (p.id === id && p.code ? { ...p, checked: !p.checked } : p)));
  }, []);

  const toggleAll = useCallback((checked: boolean) => {
    setItems((prev) => prev.map((p) => (p.code ? { ...p, checked } : p)));
  }, []);

  const removeItem = useCallback((id: string) => {
    setItems((prev) => prev.filter((p) => p.id !== id));
  }, []);

  const clearAll = useCallback(() => {
    setItems([]);
    setPasteText('');
    setError(null);
  }, []);

  const mergeToWatchlist = useCallback(async () => {
    const toMerge = items.filter((i) => i.checked && i.code).map((i) => i.code!);
    if (toMerge.length === 0) return;
    if (!configVersion) {
      setError('Please load config before merging');
      return;
    }
    const current = parseCurrentList();
    const merged = [...new Set([...current, ...toMerge])];
    const value = merged.join(',');

    setIsMerging(true);
    setError(null);
    try {
      await systemConfigApi.update({
        configVersion,
        maskToken,
        reloadNow: true,
        items: [{ key: 'STOCK_LIST', value }],
      });
      setItems([]);
      setPasteText('');
      onMerged();
    } catch (e) {
      if (e instanceof SystemConfigConflictError) {
        onMerged();
        setError('Config updated, please click "Merge to Watchlist" again');
      } else {
        setError(e instanceof Error ? e.message : 'Merge failed');
      }
    } finally {
      setIsMerging(false);
    }
  }, [items, configVersion, maskToken, onMerged, parseCurrentList]);

  const validCount = items.filter((i) => i.code).length;
  const checkedCount = items.filter((i) => i.checked && i.code).length;

  return (
    <div className="card-bordered p-4">
      <p className="mb-2 text-sm font-semibold text-content-primary">Smart Import</p>
      <p className="mb-3 text-xs text-content-tertiary">
        Supports images, CSV/Excel files, and clipboard paste. Image requires Vision API. Recommend manual verification before merging.
      </p>

      <div
        onDrop={onDrop}
        onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
        onDragLeave={(e) => { e.preventDefault(); setIsDragging(false); }}
        className={`mb-3 flex min-h-[80px] flex-col gap-4 rounded-xl border-2 border-dashed p-4 transition-all ${
          isDragging ? 'border-brand-primary bg-brand-primary-subtle' : 'border-border-default'
        } ${disabled || isLoading ? 'cursor-not-allowed opacity-50' : ''}`}
      >
        <div className="flex flex-wrap items-center gap-2">
          <label className="cursor-pointer">
            <span className="btn-ghost text-sm">Select Image</span>
            <input type="file" accept=".jpg,.jpeg,.png,.webp,.gif" className="hidden" onChange={onImageInput} disabled={disabled || isLoading} />
          </label>
          <label className="cursor-pointer">
            <span className="btn-ghost text-sm">Select File</span>
            <input type="file" accept=".csv,.xlsx,.txt" className="hidden" onChange={onDataFileInput} disabled={disabled || isLoading} />
          </label>
        </div>
        <div className="flex gap-2">
          <textarea
            placeholder="Or paste CSV/Excel text..."
            className="input-base min-h-[60px] w-full"
            value={pasteText}
            onChange={(e) => setPasteText(e.target.value)}
            disabled={disabled || isLoading}
          />
          <button type="button" className="btn-ghost shrink-0" onClick={handlePasteParse} disabled={disabled || isLoading || !pasteText.trim()}>
            Parse
          </button>
        </div>
      </div>

      {isLoading && <p className="mb-2 text-sm text-brand-primary">Processing...</p>}
      {error && (
        <div className="mb-3 rounded-xl border border-semantic-danger/30 bg-semantic-danger-subtle px-3 py-2 text-sm text-semantic-danger">{error}</div>
      )}

      {items.length > 0 && (
        <div className="space-y-2">
          <p className="rounded-xl border border-semantic-warning/40 bg-semantic-warning-subtle px-2 py-1.5 text-xs text-semantic-warning">
            Please verify items before merging. High confidence items are auto-checked.
          </p>
          <div className="flex items-center justify-between">
            <span className="text-xs text-content-secondary">
              {validCount} items available, {checkedCount} selected
            </span>
            <div className="flex gap-2">
              <button type="button" className="text-xs text-content-tertiary hover:text-content-primary" onClick={() => toggleAll(true)}>
                Select All
              </button>
              <button type="button" className="text-xs text-content-tertiary hover:text-content-primary" onClick={() => toggleAll(false)}>
                Deselect All
              </button>
              <button type="button" className="text-xs text-content-tertiary hover:text-content-primary" onClick={clearAll}>
                Clear
              </button>
            </div>
          </div>
          <div className="max-h-[200px] overflow-y-auto space-y-1">
            {items.map((it) => (
              <div
                key={it.id}
                className={`flex items-center gap-2 rounded-xl border px-2 py-1.5 text-sm ${
                  it.code ? 'border-border-default bg-surface-3' : 'border-semantic-danger/30 bg-semantic-danger-subtle'
                }`}
              >
                <input
                  type="checkbox"
                  checked={it.checked}
                  onChange={() => toggleChecked(it.id)}
                  disabled={!it.code || disabled}
                  className="rounded border-border-default text-brand-primary focus:ring-brand-primary"
                />
                <span className={it.code ? 'text-content-primary' : 'text-semantic-danger'}>
                  {it.code || 'Parse Failed'}
                </span>
                {it.name && <span className="text-content-tertiary">({it.name})</span>}
                <span className="ml-auto text-xs text-content-tertiary">
                  {it.confidence === 'high' ? 'High' : it.confidence === 'low' ? 'Low' : 'Medium'}
                </span>
                <button
                  type="button"
                  className="text-content-tertiary hover:text-content-primary"
                  onClick={() => removeItem(it.id)}
                  disabled={disabled}
                >
                  ×
                </button>
              </div>
            ))}
          </div>
          <button
            type="button"
            className="btn-primary mt-2"
            onClick={() => void mergeToWatchlist()}
            disabled={disabled || isMerging || checkedCount === 0}
          >
            {isMerging ? 'Saving...' : 'Merge to Watchlist'}
          </button>
        </div>
      )}
    </div>
  );
};