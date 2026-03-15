import type React from 'react';
import { useState } from 'react';
import { ApiErrorAlert } from '../components/common';
import { useNavigate, useSearchParams } from 'react-router-dom';
import type { ParsedApiError } from '../api/error';
import { isParsedApiError } from '../api/error';
import { useAuth } from '../hooks';
import { SettingsAlert } from '../components/settings';

const LoginPage: React.FC = () => {
  const { login, passwordSet } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const rawRedirect = searchParams.get('redirect') ?? '';
  const redirect =
    rawRedirect.startsWith('/') && !rawRedirect.startsWith('//') ? rawRedirect : '/';

  const [password, setPassword] = useState('');
  const [passwordConfirm, setPasswordConfirm] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | ParsedApiError | null>(null);

  const isFirstTime = !passwordSet;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    if (isFirstTime && password !== passwordConfirm) {
      setError('Passwords do not match');
      return;
    }
    setIsSubmitting(true);
    try {
      const result = await login(password, isFirstTime ? passwordConfirm : undefined);
      if (result.success) {
        navigate(redirect, { replace: true });
      } else {
        setError(result.error ?? 'Login failed');
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="flex min-h-screen flex-col items-center justify-center bg-surface-0 px-4">
      <div className="w-full max-w-sm rounded-2xl border border-border-default bg-surface-2/80 p-6 backdrop-blur-sm">
        <h1 className="mb-2 text-xl font-semibold text-content-primary">
          {isFirstTime ? 'Set Initial Password' : 'Admin Login'}
        </h1>
        <p className="mb-6 text-sm text-content-secondary">
          {isFirstTime
            ? 'Please set admin password, enter twice to confirm'
            : 'Please enter password to continue'}
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="password" className="mb-1 block text-sm font-medium text-content-secondary">
              {isFirstTime ? 'New Password' : 'Password'}
            </label>
            <input
              id="password"
              type="password"
              className="input-base"
              placeholder={isFirstTime ? 'Enter new password' : 'Enter password'}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              disabled={isSubmitting}
              autoFocus
              autoComplete={isFirstTime ? 'new-password' : 'current-password'}
            />
          </div>

          {isFirstTime ? (
            <div>
              <label
                htmlFor="passwordConfirm"
                className="mb-1 block text-sm font-medium text-content-secondary"
              >
                Confirm Password
              </label>
              <input
                id="passwordConfirm"
                type="password"
                className="input-base"
                placeholder="Re-enter password"
                value={passwordConfirm}
                onChange={(e) => setPasswordConfirm(e.target.value)}
                disabled={isSubmitting}
                autoComplete="new-password"
              />
            </div>
          ) : null}

          {error
            ? isParsedApiError(error)
              ? <ApiErrorAlert error={error} className="!mt-3" />
              : (
                <SettingsAlert
                  title={isFirstTime ? 'Setup Failed' : 'Login Failed'}
                  message={error}
                  variant="error"
                  className="!mt-3"
                />
              )
            : null}

          <button
            type="submit"
            className="btn-primary w-full"
            disabled={isSubmitting}
          >
            {isSubmitting ? (isFirstTime ? 'Setting...' : 'Logging in...') : isFirstTime ? 'Set Password' : 'Login'}
          </button>
        </form>
      </div>
    </div>
  );
};

export default LoginPage;