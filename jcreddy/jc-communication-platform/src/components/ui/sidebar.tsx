'use client'

import { useState } from 'react'
import Link from 'next/link'
import Image from 'next/image'
import { usePathname, useRouter } from 'next/navigation'
import { cn } from '@/lib/utils'
import {
  LayoutDashboard,
  Ticket,
  Users,
  Bus,
  Bell,
  Webhook,
  Settings,
  RefreshCw,
  BarChart3,
  MessageSquare,
  X,
  Download,
  IndianRupee,
  PieChart,
  UserCheck,
  Database,
  Receipt,
  LogOut,
  Loader2,
  TrendingUp,
  Swords,
} from 'lucide-react'

const navigation = [
  // Primary Domain Dashboards
  { name: 'Analytics Home', href: '/', icon: LayoutDashboard },
  { name: 'Executive Overview', href: '/executive', icon: PieChart },
  { name: 'Bookings & Seats', href: '/bookings', icon: Ticket },
  { name: 'Revenue & P&L', href: '/revenue', icon: IndianRupee },
  { name: 'Dynamic Pricing', href: '/pricing', icon: TrendingUp },
  { name: 'Competitor Pricing', href: '/competitor-pricing', icon: Swords },
  { name: 'Trips & Operations', href: '/trips', icon: Bus },
  { name: 'Masters', href: '/masters', icon: Database },
  { name: 'Expenses', href: '/expenses', icon: Receipt },
  { name: 'Notifications & SLA', href: '/notifications', icon: Bell },
  { name: 'Customer & Growth', href: '/customers', icon: UserCheck },
  // Utilities
  { name: 'Passengers', href: '/passengers', icon: Users },
  { name: 'Pull Data', href: '/pull-data', icon: Download },
  { name: 'Webhooks', href: '/webhooks', icon: Webhook },
  { name: 'Templates', href: '/templates', icon: MessageSquare },
  { name: 'Analytics (Legacy)', href: '/analytics', icon: BarChart3 },
  { name: 'Sync', href: '/sync', icon: RefreshCw },
  { name: 'Settings', href: '/settings', icon: Settings },
]

interface SidebarProps {
  isOpen?: boolean
  onClose?: () => void
}

export function Sidebar({ isOpen, onClose }: SidebarProps) {
  const pathname = usePathname()
  const router = useRouter()
  const [loggingOut, setLoggingOut] = useState(false)

  const handleLogout = async () => {
    setLoggingOut(true)
    try {
      await fetch('/api/auth/logout', { method: 'POST' })
      router.push('/login')
      router.refresh()
    } catch (error) {
      console.error('Logout error:', error)
      setLoggingOut(false)
    }
  }

  return (
    <>
      {/* Mobile overlay */}
      {isOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/50 lg:hidden"
          onClick={onClose}
        />
      )}

      {/* Sidebar */}
      <aside
        className={cn(
          'fixed left-0 top-0 z-50 h-screen w-64 border-r bg-white transition-transform duration-300 ease-in-out lg:translate-x-0 lg:z-40',
          isOpen ? 'translate-x-0' : '-translate-x-full'
        )}
      >
        <div className="flex h-full flex-col">
          {/* Logo */}
          <div className="flex h-16 items-center justify-between border-b px-4 lg:px-6">
            <div className="flex items-center gap-2">
              <Image
                src="/suraksha-ride-logo.jpeg"
                alt="SurakshaRide Logo"
                width={40}
                height={40}
                className="rounded-lg"
              />
              <div>
                <h1 className="text-lg font-bold text-gray-900">SurakshaRide</h1>
                <p className="text-xs text-gray-500">Predict. Prevent. Protect.</p>
              </div>
            </div>
            {/* Close button - mobile only */}
            <button
              onClick={onClose}
              className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100 hover:text-gray-600 lg:hidden"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Navigation */}
          <nav className="flex-1 space-y-1 overflow-y-auto p-3 lg:p-4">
            {navigation.map((item) => {
              const isActive = pathname === item.href
              return (
                <Link
                  key={item.name}
                  href={item.href}
                  onClick={onClose}
                  className={cn(
                    'sidebar-link',
                    isActive
                      ? 'bg-blue-50 text-blue-600'
                      : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                  )}
                >
                  <item.icon className="h-5 w-5" />
                  {item.name}
                </Link>
              )
            })}
          </nav>

          {/* Footer */}
          <div className="border-t p-3 lg:p-4 space-y-3">
            <div className="flex items-center gap-3 rounded-xl bg-gradient-to-r from-blue-50 to-indigo-50 p-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-white shadow-sm">
                <span className="text-sm font-semibold text-blue-600">MT</span>
              </div>
              <div className="flex-1 min-w-0">
                <p className="truncate text-sm font-medium text-gray-900">
                  Mythri Travels
                </p>
                <p className="text-xs text-gray-500">Active Operator</p>
              </div>
              <div className="pulse-dot green" />
            </div>

            {/* Logout Button */}
            <button
              onClick={handleLogout}
              disabled={loggingOut}
              className="w-full flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-medium text-gray-600 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors disabled:opacity-50"
            >
              {loggingOut ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Signing out...
                </>
              ) : (
                <>
                  <LogOut className="h-4 w-4" />
                  Sign Out
                </>
              )}
            </button>
          </div>
        </div>
      </aside>
    </>
  )
}
