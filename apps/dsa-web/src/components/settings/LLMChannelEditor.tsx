import { useState, useMemo, useCallback } from 'react';
import type React from 'react';
import type { ParsedApiError } from '../../api/error';
import { getParsedApiError } from '../../api/error';
import { ApiErrorAlert, EyeToggleIcon } from '../common';
import { systemConfigApi } from '../../api/systemConfig';

const CHANNEL_PRESETS: Record<string, { label: string; baseUrl: string; placeholder: string }> = {
  aihubmix: {
    label: 'AIHubmix (Aggregated)',
    baseUrl: 'https://aihubmix.com/v1',
    placeholder: 'gpt-4o-mini,claude-3-5-sonnet,qwen-plus',
  },
  deepseek: {
    label: 'DeepSeek Official',
    baseUrl: 'https://api.deepseek.com/v1',
    placeholder: 'deepseek-chat,deepseek-reasoner',
  },
  dashscope: {
    label: 'Qwen (Dashscope)',
    baseUrl: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
    placeholder: 'qwen-plus,qwen-turbo',
  },
  zhipu: {
    label: 'Zhipu GLM',
    baseUrl: 'https://open.bigmodel.cn/api/paas/v4',
    placeholder: 'glm-4-flash,glm-4-plus',
  },
  moonshot: {
    label: 'Moonshot',
    baseUrl: 'https://api.moonshot.cn/v1',
    placeholder: 'moonshot-v1-8k',
  },
  siliconflow: {
    label: 'SiliconFlow',
    baseUrl: 'https://api.siliconflow.cn/v1',
    placeholder: 'deepseek-ai/DeepSeek-V3',
  },
  openrouter: {
    label: 'OpenRouter',
    baseUrl: 'https://openrouter.ai/api/v1',
    placeholder: 'gpt-4o,claude-3.5-sonnet',
  },
  gemini: {
    label: 'Gemini (Native, no base_url)',
    baseUrl: '',
    placeholder: 'gemini/gemini-2.5-flash',
  },
  custom: {
    label: 'Custom Channel',
    baseUrl: '',
    placeholder: 'model-name-1,model-name-2',
  },
};

interface ChannelConfig {
  name: string;
  baseUrl: string;
  apiKey: string;
  models: string;
}

interface LLMChannelEditorProps {
  items: Array<{ key: string; value: string }>;
  configVersion: string;
  maskToken: string;
  onSaved: () => void;
  disabled?: boolean;
}

function parseChannelsFromItems(items: Array<{ key: string; value: string }>): ChannelConfig[] {
  const itemMap = new Map(items.map((i) => [i.key, i.value]));
  const channelNames = (itemMap.get('LLM_CHANNELS') || '')
    .split(',')
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);

  if (channelNames.length === 0) {
    return [];
  }

  return channelNames.map((name) => ({
    name: name.toLowerCase(),
    baseUrl: itemMap.get(`LLM_${name}_BASE_URL`) || '',
    apiKey: itemMap.get(`LLM_${name}_API_KEY`) || itemMap.get(`LLM_${name}_API_KEYS`) || '',
    models: itemMap.get(`LLM_${name}_MODELS`) || '',
  }));
}

function channelsToUpdateItems(
  channels: ChannelConfig[],
  previousChannelNames: string[],
): Array<{ key: string; value: string }> {
  const updates: Array<{ key: string; value: string }> = [];
  const activeNames = channels.map((c) => c.name.toUpperCase());

  updates.push({ key: 'LLM_CHANNELS', value: channels.map((c) => c.name).join(',') });

  for (const ch of channels) {
    const prefix = `LLM_${ch.name.toUpperCase()}`;
    updates.push({ key: `${prefix}_BASE_URL`, value: ch.baseUrl });
    const isMultiKey = ch.apiKey.includes(',');
    updates.push({ key: `${prefix}_API_KEY${isMultiKey ? 'S' : ''}`, value: ch.apiKey });
    updates.push({ key: `${prefix}_API_KEY${isMultiKey ? '' : 'S'}`, value: '' });
    updates.push({ key: `${prefix}_MODELS`, value: ch.models });
  }

  for (const oldName of previousChannelNames) {
    const upper = oldName.toUpperCase();
    if (!activeNames.includes(upper)) {
      const prefix = `LLM_${upper}`;
      updates.push({ key: `${prefix}_BASE_URL`, value: '' });
      updates.push({ key: `${prefix}_API_KEY`, value: '' });
      updates.push({ key: `${prefix}_API_KEYS`, value: '' });
      updates.push({ key: `${prefix}_MODELS`, value: '' });
    }
  }

  return updates;
}

export const LLMChannelEditor: React.FC<LLMChannelEditorProps> = ({
  items,
  configVersion,
  maskToken,
  onSaved,
  disabled = false,
}) => {
  const initialChannels = useMemo(() => parseChannelsFromItems(items), [items]);
  const initialNames = useMemo(
    () => initialChannels.map((c) => c.name),
    [initialChannels],
  );

  const [channels, setChannels] = useState<ChannelConfig[]>(initialChannels);
  const [isSaving, setIsSaving] = useState(false);
  const [saveMessage, setSaveMessage] = useState<
    { type: 'success'; text: string } | { type: 'error'; error: ParsedApiError } | null
  >(null);
  const [visibleKeys, setVisibleKeys] = useState<Record<number, boolean>>({});
  const [isCollapsed, setIsCollapsed] = useState(initialChannels.length === 0);
  const [addPreset, setAddPreset] = useState('aihubmix');

  const hasChanges = useMemo(() => {
    if (channels.length !== initialChannels.length) return true;
    return channels.some((ch, idx) => {
      const init = initialChannels[idx];
      if (!init) return true;
      return (
        ch.name !== init.name ||
        ch.baseUrl !== init.baseUrl ||
        ch.apiKey !== init.apiKey ||
        ch.models !== init.models
      );
    });
  }, [channels, initialChannels]);

  const updateChannel = useCallback((index: number, field: keyof ChannelConfig, value: string) => {
    setChannels((prev) => {
      const next = [...prev];
      next[index] = { ...next[index], [field]: value };
      return next;
    });
  }, []);

  const removeChannel = useCallback((index: number) => {
    setChannels((prev) => prev.filter((_, i) => i !== index));
    setVisibleKeys((prev) => {
      const next = { ...prev };
      delete next[index];
      return next;
    });
  }, []);

  const addChannel = useCallback(() => {
    const preset = CHANNEL_PRESETS[addPreset] || CHANNEL_PRESETS.custom;
    const baseName = addPreset === 'custom' ? 'custom' : addPreset;
    const existingNames = new Set(channels.map((c) => c.name));
    let name = baseName;
    let counter = 2;
    while (existingNames.has(name)) {
      name = `${baseName}${counter}`;
      counter++;
    }

    setChannels((prev) => [
      ...prev,
      { name, baseUrl: preset.baseUrl, apiKey: '', models: '' },
    ]);
    setIsCollapsed(false);
  }, [addPreset, channels]);

  const handleSave = useCallback(async () => {
    setIsSaving(true);
    setSaveMessage(null);

    try {
      const updateItems = channelsToUpdateItems(channels, initialNames);
      await systemConfigApi.update({
        configVersion,
        maskToken,
        reloadNow: true,
        items: updateItems,
      });
      setSaveMessage({ type: 'success', text: 'Channel config saved' });
      onSaved();
    } catch (error: unknown) {
      setSaveMessage({ type: 'error', error: getParsedApiError(error) });
    } finally {
      setIsSaving(false);
    }
  }, [channels, configVersion, initialNames, maskToken, onSaved]);

  const toggleKeyVisibility = useCallback((index: number) => {
    setVisibleKeys((prev) => ({ ...prev, [index]: !prev[index] }));
  }, []);

  const busy = disabled || isSaving;

  return (
    <div className="card-bordered border-brand-primary/20 p-4">
      <button
        type="button"
        className="flex w-full items-center justify-between text-left"
        onClick={() => setIsCollapsed((prev) => !prev)}
      >
        <div>
          <h3 className="text-sm font-semibold text-content-primary">LLM Channel Config</h3>
          <p className="mt-0.5 text-xs text-content-tertiary">
            {channels.length > 0
              ? `${channels.length} channels configured: ${channels.map((c) => c.name).join(', ')}`
              : 'Enable when using multiple model platforms; skip if using single model'}
          </p>
        </div>
        <span className="text-xs text-content-tertiary">{isCollapsed ? '▶ Expand' : '▼ Collapse'}</span>
      </button>

      {!isCollapsed && (
        <div className="mt-4 space-y-3">
          {channels.map((channel, index) => (
            <div
              key={`${channel.name}-${index}`}
              className="rounded-xl border border-border-default bg-surface-3/40 p-3 space-y-2"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-brand-primary">
                    {CHANNEL_PRESETS[channel.name]?.label || channel.name}
                  </span>
                </div>
                <button
                  type="button"
                  className="text-xs text-semantic-danger hover:text-semantic-danger/80 disabled:opacity-40"
                  disabled={busy}
                  onClick={() => removeChannel(index)}
                >
                  Remove
                </button>
              </div>

              <div>
                <label className="mb-1 block text-xs text-content-secondary">Channel Name</label>
                <input
                  type="text"
                  className="input-base w-full"
                  value={channel.name}
                  disabled={busy}
                  onChange={(e) => updateChannel(index, 'name', e.target.value.replace(/[^a-zA-Z0-9_]/g, '').toLowerCase())}
                  placeholder="e.g., aihubmix, deepseek"
                />
              </div>

              <div>
                <label className="mb-1 block text-xs text-content-secondary">API Base URL</label>
                <input
                  type="text"
                  className="input-base w-full"
                  value={channel.baseUrl}
                  disabled={busy}
                  onChange={(e) => updateChannel(index, 'baseUrl', e.target.value)}
                  placeholder="https://api.example.com/v1 (leave empty for Gemini native)"
                />
              </div>

              <div>
                <label className="mb-1 block text-xs text-content-secondary">API Key (comma-separated for multiple)</label>
                <div className="flex items-center gap-2">
                  <input
                    type={visibleKeys[index] ? 'text' : 'password'}
                    className="input-base flex-1"
                    value={channel.apiKey}
                    disabled={busy}
                    onChange={(e) => updateChannel(index, 'apiKey', e.target.value)}
                    placeholder="sk-xxxxxxxxxxxxxxxx"
                  />
                  <button
                    type="button"
                    className="btn-ghost !p-2"
                    onClick={() => toggleKeyVisibility(index)}
                    title={visibleKeys[index] ? 'Hide' : 'Show'}
                  >
                    <EyeToggleIcon visible={!!visibleKeys[index]} />
                  </button>
                </div>
              </div>

              <div>
                <label className="mb-1 block text-xs text-content-secondary">Models (comma-separated)</label>
                <input
                  type="text"
                  className="input-base w-full"
                  value={channel.models}
                  disabled={busy}
                  onChange={(e) => updateChannel(index, 'models', e.target.value)}
                  placeholder={CHANNEL_PRESETS[channel.name]?.placeholder || 'model-1,model-2'}
                />
                <p className="mt-1 text-[11px] text-content-tertiary">
                  For channels with base URL, no openai/ prefix needed, system auto-completes
                </p>
              </div>
            </div>
          ))}

          <div className="flex flex-wrap items-center gap-2">
            <select
              className="input-base text-xs"
              value={addPreset}
              disabled={busy}
              onChange={(e) => setAddPreset(e.target.value)}
            >
              {Object.entries(CHANNEL_PRESETS).map(([key, preset]) => (
                <option key={key} value={key}>
                  {preset.label}
                </option>
              ))}
            </select>
            <button
              type="button"
              className="btn-ghost !px-3 !py-1.5 text-xs"
              disabled={busy}
              onClick={addChannel}
            >
              + Add Channel
            </button>
          </div>

          {hasChanges && (
            <div className="flex items-center gap-3 border-t border-border-default pt-3">
              <button
                type="button"
                className="btn-primary !px-4 !py-1.5 text-xs"
                disabled={busy}
                onClick={() => void handleSave()}
              >
                {isSaving ? 'Saving...' : 'Save Channels'}
              </button>
              <button
                type="button"
                className="btn-ghost !px-3 !py-1.5 text-xs"
                disabled={busy}
                onClick={() => setChannels(initialChannels)}
              >
                Undo
              </button>
              <span className="text-[11px] text-content-tertiary">Channel config saves independently</span>
            </div>
          )}

          {saveMessage && (
            saveMessage.type === 'success'
              ? <p className="text-xs text-semantic-success">{saveMessage.text}</p>
              : <ApiErrorAlert error={saveMessage.error} />
          )}
        </div>
      )}
    </div>
  );
};