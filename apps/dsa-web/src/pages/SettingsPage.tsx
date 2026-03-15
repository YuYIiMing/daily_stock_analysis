import type React from 'react';
import { useEffect } from 'react';
import { Settings, RefreshCw, Save } from 'lucide-react';
import { useAuth, useSystemConfig } from '../hooks';
import { ApiErrorAlert, Button } from '../components/common';
import {
  ChangePasswordCard,
  IntelligentImport,
  LLMChannelEditor,
  SettingsAlert,
  SettingsField,
  SettingsLoading,
} from '../components/settings';
import { getCategoryDescriptionZh, getCategoryTitleZh } from '../utils/systemConfigI18n';

const SettingsPage: React.FC = () => {
  const { passwordChangeable } = useAuth();
  const {
    categories,
    itemsByCategory,
    issueByKey,
    activeCategory,
    setActiveCategory,
    hasDirty,
    dirtyCount,
    toast,
    clearToast,
    isLoading,
    isSaving,
    loadError,
    saveError,
    retryAction,
    retry,
    load,
    save,
    setDraftValue,
    configVersion,
    maskToken,
  } = useSystemConfig();

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!toast) {
      return;
    }

    const timer = window.setTimeout(() => {
      clearToast();
    }, 3200);

    return () => {
      window.clearTimeout(timer);
    };
  }, [clearToast, toast]);

  const rawActiveItems = itemsByCategory[activeCategory] || [];

  // Hide per-channel LLM_*_ env vars from the normal field list;
  // they are managed by the LLMChannelEditor component instead.
  const LLM_CHANNEL_KEY_RE = /^LLM_[A-Z0-9]+_(BASE_URL|API_KEY|API_KEYS|MODELS|EXTRA_HEADERS)$/;
  const activeItems =
    activeCategory === 'ai_model'
      ? rawActiveItems.filter((item) => !LLM_CHANNEL_KEY_RE.test(item.key))
      : rawActiveItems;

  return (
    <div className="min-h-screen px-4 pb-6 pt-4 md:px-6 md:ml-20">
      <header className="glass-card-v3 mb-4 p-4">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <h1 className="text-xl font-semibold text-content-primary">
              <Settings className="mr-2 inline-block" size={24} style={{ color: 'var(--brand-primary)' }} />
              系统设置
            </h1>
            <p className="text-sm text-content-secondary">
              来自.env文件的默认配置
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => void load()}
              disabled={isLoading || isSaving}
            >
              <RefreshCw className="mr-1.5 inline-block" size={16} />
              重置
            </Button>
            <Button
              variant="aurora"
              size="sm"
              onClick={() => void save()}
              disabled={!hasDirty || isSaving || isLoading}
            >
              <Save className="mr-1.5 inline-block" size={16} />
              {isSaving ? '保存中...' : `保存配置${dirtyCount ? ` (${dirtyCount})` : ''}`}
            </Button>
          </div>
        </div>

        {saveError ? (
          <ApiErrorAlert
            className="mt-3"
            error={saveError}
            actionLabel={retryAction === 'save' ? '重试保存' : undefined}
            onAction={retryAction === 'save' ? () => void retry() : undefined}
          />
        ) : null}
      </header>

      {loadError ? (
        <ApiErrorAlert
          error={loadError}
          actionLabel={retryAction === 'load' ? '重试加载' : '重新加载'}
          onAction={() => void retry()}
          className="mb-4"
        />
      ) : null}

      {isLoading ? (
        <SettingsLoading />
      ) : (
        <div className="grid grid-cols-1 gap-4">
          <div className="flex flex-col gap-4 lg:flex-row">
            {/* Sidebar */}
            <aside className="glass-card-v3 p-3 w-64 flex-shrink-0">
              <p className="mb-2 text-xs uppercase tracking-wide text-content-quaternary">
                配置分类
              </p>
              <div className="space-y-2">
                {categories.map((category) => {
                  const isActive = category.category === activeCategory;
                  const count = (itemsByCategory[category.category] || []).length;
                  const title = getCategoryTitleZh(category.category, category.title);
                  const description = getCategoryDescriptionZh(category.category, category.description);

                  return (
                    <button
                      key={category.category}
                      type="button"
                      className={`
                        w-full text-left transition-all duration-200 p-3 rounded-lg text-sm
                        ${isActive
                          ? 'bg-brand-primary/10 border border-brand-primary/30 text-brand-primary'
                          : 'bg-surface-4 border border-white/5 text-content-secondary hover:text-content-primary hover:bg-surface-5'
                        }
                      `}
                      onClick={() => setActiveCategory(category.category)}
                    >
                      <span className="flex items-center justify-between font-medium">
                        {title}
                        <span className="text-xs text-content-quaternary">
                          {count}
                        </span>
                      </span>
                      {description ? (
                        <span className="mt-1 block text-xs text-content-quaternary">
                          {description}
                        </span>
                      ) : null}
                    </button>
                  );
                })}
              </div>
            </aside>

            {/* Main Content */}
            <section className="flex-1 glass-card-v3 p-4">
              {activeCategory === 'base' ? (
                <div className="space-y-3">
                  <IntelligentImport
                    stockListValue={
                      (activeItems.find((i) => i.key === 'STOCK_LIST')?.value as string) ?? ''
                    }
                    configVersion={configVersion}
                    maskToken={maskToken}
                    onMerged={() => void load()}
                    disabled={isSaving || isLoading}
                  />
                </div>
              ) : null}
              {activeCategory === 'ai_model' ? (
                <LLMChannelEditor
                  items={rawActiveItems}
                  configVersion={configVersion}
                  maskToken={maskToken}
                  onSaved={() => void load()}
                  disabled={isSaving || isLoading}
                />
              ) : null}
              {activeCategory === 'system' && passwordChangeable ? (
                <div className="space-y-3">
                  <ChangePasswordCard />
                </div>
              ) : null}
              {activeItems.length ? (
                activeItems.map((item) => (
                  <SettingsField
                    key={item.key}
                    item={item}
                    value={item.value}
                    disabled={isSaving}
                    onChange={setDraftValue}
                    issues={issueByKey[item.key] || []}
                  />
                ))
              ) : (
                <div className="text-sm text-content-secondary p-5 bg-surface-3 rounded-lg border border-white/5">
                  该分类下暂无配置项。
                </div>
              )}
            </section>
          </div>
        </div>
      )}

      {toast ? (
        <div className="fixed bottom-5 right-5 z-50 w-80 max-w-[calc(100vw-24px)]">
          {toast.type === 'success'
            ? <SettingsAlert title="成功" message={toast.message} variant="success" />
            : <ApiErrorAlert error={toast.error} />}
        </div>
      ) : null}
    </div>
  );
};

export default SettingsPage;
