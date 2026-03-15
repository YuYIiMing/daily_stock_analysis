import { useState } from 'react';
import type React from 'react';
import { EyeToggleIcon, Select } from '../common';
import type { ConfigValidationIssue, SystemConfigItem } from '../../types/systemConfig';
import { getFieldDescriptionZh, getFieldTitleZh } from '../../utils/systemConfigI18n';

function isMultiValueField(item: SystemConfigItem): boolean {
  const validation = (item.schema?.validation ?? {}) as Record<string, unknown>;
  return Boolean(validation.multiValue ?? validation.multi_value);
}

function parseMultiValues(value: string): string[] {
  if (!value) {
    return [''];
  }

  const values = value.split(',').map((entry) => entry.trim());
  return values.length ? values : [''];
}

function serializeMultiValues(values: string[]): string {
  return values.map((entry) => entry.trim()).join(',');
}

interface SettingsFieldProps {
  item: SystemConfigItem;
  value: string;
  disabled?: boolean;
  onChange: (key: string, value: string) => void;
  issues?: ConfigValidationIssue[];
}

function renderFieldControl(
  item: SystemConfigItem,
  value: string,
  disabled: boolean,
  onChange: (nextValue: string) => void,
  isSecretVisible: boolean,
  onToggleSecretVisible: () => void,
  isPasswordEditable: boolean,
  onPasswordFocus: () => void,
) {
  const schema = item.schema;
  const commonClass = 'input-base';
  const controlType = schema?.uiControl ?? 'text';
  const isMultiValue = isMultiValueField(item);

  if (controlType === 'textarea') {
    return (
      <textarea
        className={`${commonClass} min-h-[92px] resize-y`}
        value={value}
        disabled={disabled || !schema?.isEditable}
        onChange={(event) => onChange(event.target.value)}
      />
    );
  }

  if (controlType === 'select' && schema?.options?.length) {
    return (
        <Select
          value={value}
          onChange={onChange}
          options={schema.options.map((option) => ({ value: option, label: option }))}
          disabled={disabled || !schema.isEditable}
          placeholder="Please select"
        />
      );
  }

  if (controlType === 'switch') {
    const checked = value.trim().toLowerCase() === 'true';
    return (
      <label className="inline-flex cursor-pointer items-center gap-3">
        <input
          type="checkbox"
          checked={checked}
          disabled={disabled || !schema?.isEditable}
          onChange={(event) => onChange(event.target.checked ? 'true' : 'false')}
          className="rounded border-border-default text-brand-primary focus:ring-brand-primary"
        />
        <span className="text-sm text-content-secondary">{checked ? '已启用' : '已禁用'}</span>
      </label>
    );
  }

  if (controlType === 'password') {
    if (isMultiValue) {
      const values = parseMultiValues(value);

      return (
        <div className="space-y-2">
          {values.map((entry, index) => (
            <div className="flex items-center gap-2" key={`${item.key}-${index}`}>
              <input
                type={isSecretVisible ? 'text' : 'password'}
                readOnly={!isPasswordEditable}
                onFocus={onPasswordFocus}
                className={`${commonClass} flex-1`}
                value={entry}
                disabled={disabled || !schema?.isEditable}
                onChange={(event) => {
                  const nextValues = [...values];
                  nextValues[index] = event.target.value;
                  onChange(serializeMultiValues(nextValues));
                }}
              />
              <button
                type="button"
                className="btn-ghost !p-2"
                disabled={disabled || !schema?.isEditable}
                onClick={onToggleSecretVisible}
                title={isSecretVisible ? '隐藏' : '显示'}
                aria-label={isSecretVisible ? '隐藏密码' : '显示密码'}
              >
                <EyeToggleIcon visible={isSecretVisible} />
              </button>
              <button
                type="button"
                className="btn-ghost !px-3 !py-2 text-xs"
                disabled={disabled || !schema?.isEditable || values.length <= 1}
                onClick={() => {
                  const nextValues = values.filter((_, rowIndex) => rowIndex !== index);
                  onChange(serializeMultiValues(nextValues.length ? nextValues : ['']));
                }}
              >
                移除
              </button>
            </div>
          ))}

          <div className="flex items-center gap-2">
            <button
              type="button"
              className="btn-ghost !px-3 !py-2 text-xs"
              disabled={disabled || !schema?.isEditable}
              onClick={() => onChange(serializeMultiValues([...values, '']))}
            >
              添加密钥
            </button>
          </div>
        </div>
      );
    }

    return (
      <div className="flex items-center gap-2">
        <input
          type={isSecretVisible ? 'text' : 'password'}
          readOnly={!isPasswordEditable}
          onFocus={onPasswordFocus}
          className={`${commonClass} flex-1`}
          value={value}
          disabled={disabled || !schema?.isEditable}
          onChange={(event) => onChange(event.target.value)}
        />
        <button
          type="button"
          className="btn-ghost !p-2"
          disabled={disabled || !schema?.isEditable}
          onClick={onToggleSecretVisible}
          title={isSecretVisible ? '隐藏' : '显示'}
          aria-label={isSecretVisible ? '隐藏密码' : '显示密码'}
        >
          <EyeToggleIcon visible={isSecretVisible} />
        </button>
      </div>
    );
  }

  const inputType = controlType === 'number' ? 'number' : controlType === 'time' ? 'time' : 'text';

  return (
    <input
      type={inputType}
      className={commonClass}
      value={value}
      disabled={disabled || !schema?.isEditable}
      onChange={(event) => onChange(event.target.value)}
    />
  );
}

export const SettingsField: React.FC<SettingsFieldProps> = ({
  item,
  value,
  disabled = false,
  onChange,
  issues = [],
}) => {
  const schema = item.schema;
  const isMultiValue = isMultiValueField(item);
  const title = getFieldTitleZh(item.key, item.key);
  const description = getFieldDescriptionZh(item.key);
  const hasError = issues.some((issue) => issue.severity === 'error');
  const [isSecretVisible, setIsSecretVisible] = useState(false);
  const [isPasswordEditable, setIsPasswordEditable] = useState(false);

  return (
    <div className={`card-bordered p-4 ${hasError ? 'border-semantic-danger/35' : ''}`}>
      <div className="mb-2 flex items-center gap-2">
        <label className="text-sm font-semibold text-content-primary" htmlFor={`setting-${item.key}`}>
          {title}
        </label>
        {schema?.isSensitive ? (
          <span className="badge badge-nebula text-[10px]">敏感</span>
        ) : null}
      </div>

      {description ? (
        <p className="mb-3 text-xs text-content-tertiary" title={description}>
          {description}
        </p>
      ) : null}

      <div id={`setting-${item.key}`}>
        {renderFieldControl(
          item,
          value,
          disabled,
          (nextValue) => onChange(item.key, nextValue),
          isSecretVisible,
          () => setIsSecretVisible((previous) => !previous),
          isPasswordEditable,
          () => setIsPasswordEditable(true),
        )}
      </div>

      {schema?.isSensitive ? (
        <p className="mt-2 text-[11px] text-content-secondary">
          敏感值默认隐藏，点击眼睛图标查看。
          {isMultiValue ? ' 支持多个条目。' : ''}
        </p>
      ) : null}

      {issues.length ? (
        <div className="mt-2 space-y-1">
          {issues.map((issue, index) => (
            <p
              key={`${issue.code}-${issue.key}-${index}`}
              className={issue.severity === 'error' ? 'text-xs text-semantic-danger' : 'text-xs text-semantic-warning'}
            >
              {issue.message}
            </p>
          ))}
        </div>
      ) : null}
    </div>
  );
};