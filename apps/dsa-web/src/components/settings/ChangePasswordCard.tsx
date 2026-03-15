import type React from 'react';
import { useState } from 'react';
import type { ParsedApiError } from '../../api/error';
import { isParsedApiError } from '../../api/error';
import { useAuth } from '../../hooks';
import { ApiErrorAlert, EyeToggleIcon } from '../common';
import { SettingsAlert } from './SettingsAlert';

export const ChangePasswordCard: React.FC = () => {
  const { changePassword } = useAuth();
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [newPasswordConfirm, setNewPasswordConfirm] = useState('');
  const [showCurrent, setShowCurrent] = useState(false);
  const [showNew, setShowNew] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | ParsedApiError | null>(null);
  const [success, setSuccess] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setSuccess(false);

    if (!currentPassword.trim()) {
      setError('Please enter current password');
      return;
    }
    if (!newPassword.trim()) {
      setError('Please enter new password');
      return;
    }
    if (newPassword.length < 6) {
      setError('New password must be at least 6 characters');
      return;
    }
    if (newPassword !== newPasswordConfirm) {
      setError('New passwords do not match');
      return;
    }

    setIsSubmitting(true);
    try {
      const result = await changePassword(currentPassword, newPassword, newPasswordConfirm);
      if (result.success) {
        setSuccess(true);
        setCurrentPassword('');
        setNewPassword('');
        setNewPasswordConfirm('');
        setShowCurrent(false);
        setShowNew(false);
        setShowConfirm(false);
        setTimeout(() => setSuccess(false), 4000);
      } else {
        setError(result.error ?? 'Change failed');
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="card-bordered p-4">
      <div className="mb-2 flex items-center gap-2">
        <label className="text-sm font-semibold text-content-primary">Change Password</label>
      </div>
      <p className="mb-3 text-xs text-content-tertiary">Update admin login password</p>

      <form onSubmit={handleSubmit} className="space-y-3">
        <div>
          <label
            htmlFor="change-pass-current"
            className="mb-1 block text-xs font-medium text-content-secondary"
          >
            Current Password
          </label>
          <div className="flex items-center gap-2">
            <input
              id="change-pass-current"
              type={showCurrent ? 'text' : 'password'}
              className="input-base flex-1"
              placeholder="Enter current password"
              value={currentPassword}
              onChange={(e) => setCurrentPassword(e.target.value)}
              disabled={isSubmitting}
              autoComplete="current-password"
            />
            <button
              type="button"
              className="btn-ghost !p-2 shrink-0"
              disabled={isSubmitting}
              onClick={() => setShowCurrent((v) => !v)}
              title={showCurrent ? 'Hide' : 'Show'}
              aria-label={showCurrent ? 'Hide password' : 'Show password'}
            >
              <EyeToggleIcon visible={showCurrent} />
            </button>
          </div>
        </div>
        <div>
          <label
            htmlFor="change-pass-new"
            className="mb-1 block text-xs font-medium text-content-secondary"
          >
            New Password
          </label>
          <div className="flex items-center gap-2">
            <input
              id="change-pass-new"
              type={showNew ? 'text' : 'password'}
              className="input-base flex-1"
              placeholder="Enter new password (min 6 chars)"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              disabled={isSubmitting}
              autoComplete="new-password"
            />
            <button
              type="button"
              className="btn-ghost !p-2 shrink-0"
              disabled={isSubmitting}
              onClick={() => setShowNew((v) => !v)}
              title={showNew ? 'Hide' : 'Show'}
              aria-label={showNew ? 'Hide password' : 'Show password'}
            >
              <EyeToggleIcon visible={showNew} />
            </button>
          </div>
        </div>
        <div>
          <label
            htmlFor="change-pass-confirm"
            className="mb-1 block text-xs font-medium text-content-secondary"
          >
            Confirm New Password
          </label>
          <div className="flex items-center gap-2">
            <input
              id="change-pass-confirm"
              type={showConfirm ? 'text' : 'password'}
              className="input-base flex-1"
              placeholder="Re-enter new password"
              value={newPasswordConfirm}
              onChange={(e) => setNewPasswordConfirm(e.target.value)}
              disabled={isSubmitting}
              autoComplete="new-password"
            />
            <button
              type="button"
              className="btn-ghost !p-2 shrink-0"
              disabled={isSubmitting}
              onClick={() => setShowConfirm((v) => !v)}
              title={showConfirm ? 'Hide' : 'Show'}
              aria-label={showConfirm ? 'Hide password' : 'Show password'}
            >
              <EyeToggleIcon visible={showConfirm} />
            </button>
          </div>
        </div>

        {error
          ? isParsedApiError(error)
            ? <ApiErrorAlert error={error} className="!mt-3" />
            : <SettingsAlert title="Change Failed" message={error} variant="error" className="!mt-3" /> : null}
        {success ? (
          <p className="text-xs text-semantic-success">Password changed successfully</p>
        ) : null}

        <button
          type="submit"
          className="btn-primary mt-2"
          disabled={isSubmitting}
        >
          {isSubmitting ? 'Changing...' : 'Change Password'}
        </button>
      </form>
    </div>
  );
};