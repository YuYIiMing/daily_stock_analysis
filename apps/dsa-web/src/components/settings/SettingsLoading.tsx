import type React from 'react';

export const SettingsLoading: React.FC = () => {
  return (
    <div className="space-y-4 animate-fade-in">
      {Array.from({ length: 6 }).map((_, index) => (
        <div key={index} className="card-bordered p-4">
          <div className="h-3 w-32 rounded bg-surface-6" />
          <div className="mt-3 h-10 rounded-xl bg-surface-4" />
        </div>
      ))}
    </div>
  );
};