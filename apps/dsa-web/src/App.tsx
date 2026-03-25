import type React from 'react';
import { useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, NavLink, useLocation, Navigate } from 'react-router-dom';
import { 
  Home, 
  MessageSquare, 
  LineChart, 
  Settings, 
  LogOut,
  TrendingUp,
  CandlestickChart
} from 'lucide-react';
import HomePage from './pages/HomePage';
import BacktestPage from './pages/BacktestPage';
import QuantStrategyPage from './pages/QuantStrategyPage';
import SettingsPage from './pages/SettingsPage';
import LoginPage from './pages/LoginPage';
import NotFoundPage from './pages/NotFoundPage';
import ChatPage from './pages/ChatPage';
import { ApiErrorAlert } from './components/common';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { useAgentChatStore } from './stores/agentChatStore';
import './App.css';

// Navigation icons using Lucide React

interface NavItem {
    key: string;
    label: string;
    to: string;
    icon: React.ElementType;
}

const NAV_ITEMS: NavItem[] = [
    {
        key: 'home',
        label: '首页',
        to: '/',
        icon: Home,
    },
    {
        key: 'chat',
        label: '问股',
        to: '/chat',
        icon: MessageSquare,
    },
    {
        key: 'backtest',
        label: '回测',
        to: '/backtest',
        icon: LineChart,
    },
    {
        key: 'quant',
        label: '量化策略',
        to: '/quant-strategy',
        icon: CandlestickChart,
    },
    {
        key: 'settings',
        label: '设置',
        to: '/settings',
        icon: Settings,
    },
];

// Dock navigation component
const DockNav: React.FC = () => {
    const { authEnabled, logout } = useAuth();
    const completionBadge = useAgentChatStore((s) => s.completionBadge);
    
    return (
        <aside className="dock-nav" aria-label="Main navigation">
            <div className="dock-surface">
                <NavLink to="/" className="dock-logo" title="Home" aria-label="Home">
                    <TrendingUp className="w-5 h-5" strokeWidth={2} />
                </NavLink>

                <nav className="dock-items" aria-label="Pages">
                    {NAV_ITEMS.map((item) => {
                        const Icon = item.icon;
                        if (item.key === 'chat') {
                            return (
                                <div key="chat" className="relative inline-flex">
                                    <NavLink
                                        to="/chat"
                                        end={false}
                                        title="Ask Stock"
                                        aria-label="Ask Stock"
                                        className={({ isActive }) => `dock-item${isActive ? ' is-active' : ''}`}
                                    >
                                        {({ isActive }) => (
                                            <Icon className="w-6 h-6" strokeWidth={isActive ? 2.5 : 1.5} />
                                        )}
                                    </NavLink>
                                    {completionBadge && (
                                        <span
                                            className="absolute top-0.5 right-0.5 w-2.5 h-2.5 rounded-full bg-primary border-2 border-layer-0 z-10 pointer-events-none"
                                            aria-label="New message in chat"
                                        />
                                    )}
                                </div>
                            );
                        }
                        return (
                            <NavLink
                                key={item.key}
                                to={item.to}
                                end={item.to === '/'}
                                title={item.label}
                                aria-label={item.label}
                                className={({ isActive }) => `dock-item${isActive ? ' is-active' : ''}`}
                            >
                                {({ isActive }) => (
                                    <Icon className="w-6 h-6" strokeWidth={isActive ? 2.5 : 1.5} />
                                )}
                            </NavLink>
                        );
                    })}
                </nav>

                {authEnabled ? (
                    <button
                        type="button"
                        onClick={() => logout()}
                        title="Logout"
                        aria-label="Logout"
                        className="dock-item"
                    >
                        <LogOut className="w-6 h-6" strokeWidth={1.5} />
                    </button>
                ) : null}

                <div className="dock-footer" />
            </div>
        </aside>
    );
};

const AppContent: React.FC = () => {
    const location = useLocation();
    const { authEnabled, loggedIn, isLoading, loadError, refreshStatus } = useAuth();

    useEffect(() => {
        useAgentChatStore.getState().setCurrentRoute(location.pathname);
    }, [location.pathname]);

    if (isLoading) {
        return (
            <div className="flex min-h-screen items-center justify-center" style={{ background: 'var(--bg-layer-0)' }}>
                <div className="flex flex-col items-center gap-4">
                    <div className="w-10 h-10 border-2 rounded-full animate-spin" 
                         style={{ 
                             borderColor: 'rgba(0, 229, 204, 0.2)', 
                             borderTopColor: 'var(--color-primary)' 
                         }} 
                    />
                    <span className="text-sm text-tertiary">Loading...</span>
                </div>
            </div>
        );
    }

    if (loadError) {
        return (
            <div className="flex min-h-screen flex-col items-center justify-center gap-4 px-4" style={{ background: 'var(--bg-layer-0)' }}>
                <div className="w-full max-w-lg">
                    <ApiErrorAlert error={loadError}/>
                </div>
                <button
                    type="button"
                    className="btn-primary"
                    onClick={() => void refreshStatus()}
                >
                    Retry
                </button>
            </div>
        );
    }

    if (authEnabled && !loggedIn) {
        if (location.pathname === '/login') {
            return <LoginPage />;
        }
        const redirect = encodeURIComponent(location.pathname + location.search);
        return <Navigate to={`/login?redirect=${redirect}`} replace />;
    }

    if (location.pathname === '/login') {
        return <Navigate to="/" replace />;
    }

    return (
        <div className="flex min-h-screen" style={{ background: 'var(--bg-layer-0)' }}>
            <DockNav/>
            <main className="flex-1 dock-safe-area">
                <Routes>
                    <Route path="/" element={<HomePage/>}/>
                    <Route path="/chat" element={<ChatPage/>}/>
                    <Route path="/backtest" element={<BacktestPage/>}/>
                    <Route path="/quant-strategy" element={<QuantStrategyPage/>}/>
                    <Route path="/settings" element={<SettingsPage/>}/>
                    <Route path="/login" element={<LoginPage/>}/>
                    <Route path="*" element={<NotFoundPage/>}/>
                </Routes>
            </main>
        </div>
    );
};

const App: React.FC = () => {
    return (
        <Router>
            <AuthProvider>
                <AppContent/>
            </AuthProvider>
        </Router>
    );
};

export default App;
