import React, { useState } from 'react';

interface JsonViewerProps {
  data: Record<string, unknown> | unknown[] | null | undefined;
  maxHeight?: string;
  className?: string;
}

export const JsonViewer: React.FC<JsonViewerProps> = ({
  data,
  maxHeight = '400px',
  className = '',
}) => {
  const [copied, setCopied] = useState(false);

  if (!data) {
    return (
      <div className="text-content-tertiary italic py-4 text-center">No data</div>
    );
  }

  const jsonString = JSON.stringify(data, null, 2);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(jsonString);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const highlightJson = (json: string): React.ReactNode => {
    return json.split('\n').map((line, index) => {
      let highlighted = line.replace(
        /"([^"]+)":/g,
        '<span style="color: var(--brand-primary)">"$1"</span>:'
      );
      highlighted = highlighted.replace(
        /: "([^"]*)"/g,
        ': <span style="color: var(--semantic-success)">"$1"</span>'
      );
      highlighted = highlighted.replace(
        /: (-?\d+\.?\d*)/g,
        ': <span style="color: var(--semantic-warning)">$1</span>'
      );
      highlighted = highlighted.replace(
        /: (true|false|null)/g,
        ': <span style="color: var(--brand-secondary)">$1</span>'
      );

      return (
        <div
          key={index}
          className="leading-relaxed"
          dangerouslySetInnerHTML={{ __html: highlighted }}
        />
      );
    });
  };

  return (
    <div className={`relative ${className}`}>
      <button
        onClick={handleCopy}
        className="absolute top-2 right-2 px-2 py-1 text-xs rounded-xl
          bg-surface-5 hover:bg-surface-6 text-content-secondary
          transition-colors z-10"
      >
        {copied ? 'Copied!' : 'Copy'}
      </button>

      <div
        className="bg-surface-2 rounded-xl p-4 overflow-auto custom-scrollbar
          border border-border-default font-mono text-sm text-content-secondary"
        style={{ maxHeight }}
      >
        <pre className="whitespace-pre-wrap break-words">
          {highlightJson(jsonString)}
        </pre>
      </div>
    </div>
  );
};