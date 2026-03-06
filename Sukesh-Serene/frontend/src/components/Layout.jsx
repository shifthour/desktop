import React from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import {
  LayoutDashboard,
  Building2,
  Home,
  Users,
  IndianRupee,
  LogOut,
  Menu,
  X,
  CheckCircle,
  Lock,
  ChevronDown,
  MessageSquare
} from 'lucide-react';
import { useAuth } from '../context/AuthContext';

const Layout = ({ children }) => {
  const [sidebarOpen, setSidebarOpen] = React.useState(false);
  const [userMenuOpen, setUserMenuOpen] = React.useState(false);
  const location = useLocation();
  const navigate = useNavigate();
  const { user, logout } = useAuth();

  const allNavigation = [
    { name: 'Dashboard', href: '/', icon: LayoutDashboard, roles: ['admin'] },
    { name: 'PG Properties', href: '/pg-properties', icon: Building2, roles: ['admin', 'supervisor'] },
    { name: 'Rooms', href: '/rooms', icon: Home, roles: ['admin', 'supervisor'] },
    { name: 'Tenants', href: '/tenants', icon: Users, roles: ['admin', 'supervisor'] },
    { name: 'Payment Review', href: '/payment-review', icon: CheckCircle, roles: ['admin', 'supervisor'] },
    { name: 'Complaints', href: '/complaints', icon: MessageSquare, roles: ['admin', 'supervisor'] },
    { name: 'Expenses', href: '/expenses', icon: IndianRupee, roles: ['admin', 'supervisor'] },
    { name: 'Payments', href: '/payments', icon: IndianRupee, roles: ['admin', 'supervisor'] },
  ];

  // Filter navigation based on user role
  const navigation = allNavigation.filter(item =>
    item.roles.includes(user?.role || 'admin')
  );

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Mobile sidebar backdrop */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-gray-600 bg-opacity-75 z-20 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <div className={`
        fixed inset-y-0 left-0 z-30 w-64 bg-white shadow-lg transform transition-transform duration-300 ease-in-out
        ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'} lg:translate-x-0
      `}>
        <div className="flex flex-col h-full">
          {/* Logo */}
          <div className="flex items-center justify-between h-16 px-6 border-b border-gray-200">
            <div className="flex items-center space-x-3">
              <img
                src="/assets/logo.png"
                alt="Serene Living Logo"
                className="h-12 w-auto object-contain"
              />
              <span className="text-xl font-bold text-gray-900">Serene Living</span>
            </div>
            <button
              className="lg:hidden"
              onClick={() => setSidebarOpen(false)}
            >
              <X size={24} />
            </button>
          </div>

          {/* Navigation */}
          <nav className="flex-1 px-4 py-6 space-y-2">
            {navigation.map((item) => {
              const Icon = item.icon;
              const isActive = location.pathname === item.href;

              return (
                <Link
                  key={item.name}
                  to={item.href}
                  className={`
                    flex items-center space-x-3 px-4 py-3 rounded-lg transition-colors
                    ${isActive
                      ? 'bg-gray-900 text-white'
                      : 'text-gray-700 hover:bg-gray-100'
                    }
                  `}
                >
                  <Icon size={20} />
                  <span className="font-medium">{item.name}</span>
                </Link>
              );
            })}
          </nav>

          {/* User section */}
          <div className="p-4 border-t border-gray-200 relative">
            <button
              onClick={() => setUserMenuOpen(!userMenuOpen)}
              className="w-full flex items-center justify-between hover:bg-gray-50 rounded-lg p-2 transition-colors"
            >
              <div className="flex items-center space-x-3">
                <div className="w-8 h-8 bg-gray-300 rounded-full flex items-center justify-center">
                  <span className="text-gray-700 font-medium text-sm">
                    {user?.name?.charAt(0).toUpperCase()}
                  </span>
                </div>
                <div className="flex-1 text-left">
                  <p className="text-sm font-medium text-gray-900">{user?.name}</p>
                  {user?.role && (
                    <span className={`inline-block text-xs px-2 py-0.5 rounded-full mt-1 ${
                      user.role === 'admin'
                        ? 'bg-blue-100 text-blue-800'
                        : 'bg-green-100 text-green-800'
                    }`}>
                      {user.role.charAt(0).toUpperCase() + user.role.slice(1)}
                    </span>
                  )}
                </div>
              </div>
              <ChevronDown size={16} className={`text-gray-500 transition-transform ${userMenuOpen ? 'rotate-180' : ''}`} />
            </button>

            {/* User Menu Dropdown */}
            {userMenuOpen && (
              <div className="absolute bottom-full left-4 right-4 mb-2 bg-white border border-gray-200 rounded-lg shadow-lg py-2 z-50">
                <Link
                  to="/change-password"
                  onClick={() => {
                    setUserMenuOpen(false);
                    setSidebarOpen(false);
                  }}
                  className="flex items-center space-x-3 px-4 py-2 text-gray-700 hover:bg-gray-100 transition-colors"
                >
                  <Lock size={18} />
                  <span className="text-sm">Change Password</span>
                </Link>
                <button
                  onClick={() => {
                    setUserMenuOpen(false);
                    handleLogout();
                  }}
                  className="w-full flex items-center space-x-3 px-4 py-2 text-red-600 hover:bg-red-50 transition-colors"
                >
                  <LogOut size={18} />
                  <span className="text-sm">Logout</span>
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Main content */}
      <div className="lg:pl-64">
        {/* Top bar */}
        <div className="h-16 bg-white border-b border-gray-200 flex items-center px-6">
          <button
            className="lg:hidden mr-4"
            onClick={() => setSidebarOpen(true)}
          >
            <Menu size={24} />
          </button>
          <h1 className="text-xl font-semibold text-gray-900">
            PG Management Portal
          </h1>
        </div>

        {/* Page content */}
        <main className="p-6">
          {children}
        </main>
      </div>
    </div>
  );
};

export default Layout;
