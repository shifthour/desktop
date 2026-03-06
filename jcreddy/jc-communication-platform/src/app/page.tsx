'use client'

import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { formatCurrency } from '@/lib/utils'
import {
  LayoutDashboard,
  Ticket,
  IndianRupee,
  Bus,
  Bell,
  Users,
  TrendingUp,
  TrendingDown,
  AlertTriangle,
  ArrowRight,
  ChevronRight,
  Activity,
  XCircle,
  Clock,
  Banknote,
  UserPlus,
  UserCheck,
  BarChart3,
  PieChart,
  Zap,
  Target,
  Calendar,
  CalendarDays,
} from 'lucide-react'
import Link from 'next/link'

async function fetchDashboardStats(dateType: string, dateRange: string, customStart?: string, customEnd?: string) {
  let url = `/api/dashboard/stats?dateType=${dateType}&range=${dateRange}`
  if (dateRange === 'custom' && customStart && customEnd) {
    url += `&startDate=${customStart}&endDate=${customEnd}`
  }
  const res = await fetch(url)
  if (!res.ok) throw new Error('Failed to fetch stats')
  return res.json()
}

// Date range options per document spec
const dateRangeOptions = [
  { value: 'today', label: 'Today' },
  { value: 'yesterday', label: 'Yesterday' },
  { value: '7days', label: 'Last 7 Days' },
  { value: 'next7days', label: 'Next 7 Days' },
  { value: 'month', label: 'This Month' },
  { value: 'lastMonth', label: 'Last Month' },
  { value: 'nextMonth', label: 'Next Month' },
]

// Domain tile configuration
const domainTiles = [
  {
    id: 'executive',
    title: 'Executive Overview',
    description: '10-sec health check + drill entry',
    icon: LayoutDashboard,
    href: '/executive',
    color: 'blue',
    bgColor: 'bg-blue-50',
    iconColor: 'text-blue-600',
    borderColor: 'border-blue-200',
    hoverBorder: 'hover:border-blue-400',
  },
  {
    id: 'bookings',
    title: 'Bookings & Seats',
    description: 'Booking lifecycle + hourly anomalies',
    icon: Ticket,
    href: '/bookings',
    color: 'purple',
    bgColor: 'bg-purple-50',
    iconColor: 'text-purple-600',
    borderColor: 'border-purple-200',
    hoverBorder: 'hover:border-purple-400',
  },
  {
    id: 'revenue',
    title: 'Revenue & P&L',
    description: 'Profit, leakage, cancellations impact',
    icon: IndianRupee,
    href: '/revenue',
    color: 'green',
    bgColor: 'bg-green-50',
    iconColor: 'text-green-600',
    borderColor: 'border-green-200',
    hoverBorder: 'hover:border-green-400',
  },
  {
    id: 'trips',
    title: 'Trips & Operations',
    description: 'Service execution control + disruptions',
    icon: Bus,
    href: '/trips',
    color: 'indigo',
    bgColor: 'bg-indigo-50',
    iconColor: 'text-indigo-600',
    borderColor: 'border-indigo-200',
    hoverBorder: 'hover:border-indigo-400',
  },
  {
    id: 'notifications',
    title: 'Notifications & SLA',
    description: 'Backlog, SLA, gateway failures',
    icon: Bell,
    href: '/notifications',
    color: 'orange',
    bgColor: 'bg-orange-50',
    iconColor: 'text-orange-600',
    borderColor: 'border-orange-200',
    hoverBorder: 'hover:border-orange-400',
  },
  {
    id: 'customers',
    title: 'Customer & Growth',
    description: 'Acquisition, retention, churn, cohorts',
    icon: Users,
    href: '/customers',
    color: 'pink',
    bgColor: 'bg-pink-50',
    iconColor: 'text-pink-600',
    borderColor: 'border-pink-200',
    hoverBorder: 'hover:border-pink-400',
  },
]

// Status indicator component
function StatusIndicator({ value, threshold, inverse = false }: { value: number; threshold: number; inverse?: boolean }) {
  const isGood = inverse ? value < threshold : value >= threshold
  return (
    <div className={`h-2 w-2 rounded-full ${isGood ? 'bg-green-500' : 'bg-red-500'}`} />
  )
}

export default function AnalyticsHomePage() {
  // Date Type: 'travel' for Travel Date, 'booked' for Booked Date
  const [dateType, setDateType] = useState<'travel' | 'booked'>('travel')
  const [dateRange, setDateRange] = useState('today')
  const [showCustomDate, setShowCustomDate] = useState(false)
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')

  const { data: stats, isLoading } = useQuery({
    queryKey: ['dashboard-stats', dateType, dateRange, customStartDate, customEndDate],
    queryFn: () => fetchDashboardStats(dateType, dateRange, customStartDate, customEndDate),
    refetchInterval: 30000,
  })

  const today = new Date().toLocaleDateString('en-IN', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  })

  const handleDateRangeChange = (value: string) => {
    if (value === 'custom') {
      setShowCustomDate(true)
    } else {
      setShowCustomDate(false)
      setDateRange(value)
    }
  }

  // Calculate needs attention items
  const needsAttention = []

  if (stats?.notifications?.pending > 10) {
    needsAttention.push({
      id: 'pending-notifications',
      title: `${stats.notifications.pending} Pending Notifications`,
      description: 'Notifications awaiting delivery',
      href: '/notifications?status=pending',
      severity: 'warning',
      icon: Clock,
    })
  }

  if (stats?.notifications?.failed > 0) {
    needsAttention.push({
      id: 'failed-notifications',
      title: `${stats.notifications.failed} Failed Notifications`,
      description: 'Delivery failures need attention',
      href: '/notifications?status=failed',
      severity: 'error',
      icon: XCircle,
    })
  }

  if (stats?.trips?.blocked > 0) {
    needsAttention.push({
      id: 'blocked-trips',
      title: `${stats.trips.blocked} Blocked Trips`,
      description: 'Trips blocked and need review',
      href: '/trips?status=blocked',
      severity: 'error',
      icon: AlertTriangle,
    })
  }

  if (stats?.bookings?.change < -20) {
    needsAttention.push({
      id: 'booking-drop',
      title: 'Booking Drop Detected',
      description: `${Math.abs(stats.bookings.change)}% drop vs yesterday`,
      href: '/bookings',
      severity: 'warning',
      icon: TrendingDown,
    })
  }

  return (
    <DashboardLayout>
      {/* Header */}
      <div className="border-b bg-white px-6 py-4">
        <div className="flex flex-col gap-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Analytics Home</h1>
              <p className="text-sm text-gray-500">Operations & Growth Overview • {today}</p>
            </div>
            <div className="flex items-center gap-3">
              <Link
                href="/pull-data"
                className="flex items-center gap-2 rounded-lg border border-gray-200 px-4 py-2 text-sm font-medium text-gray-600 hover:bg-gray-50"
              >
                <Activity className="h-4 w-4" />
                Sync Data
              </Link>
            </div>
          </div>

          {/* Date Filters - as per document spec */}
          <div className="flex flex-col sm:flex-row sm:items-center gap-4 pt-2 border-t">
            {/* Date Type Toggle */}
            <div className="flex items-center gap-2">
              <CalendarDays className="h-4 w-4 text-gray-400" />
              <span className="text-sm text-gray-500">Date Type:</span>
              <div className="flex rounded-lg border border-gray-200 overflow-hidden">
                <button
                  onClick={() => setDateType('travel')}
                  className={`px-3 py-1.5 text-sm font-medium transition-colors ${
                    dateType === 'travel'
                      ? 'bg-blue-600 text-white'
                      : 'bg-white text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  Travel Date
                </button>
                <button
                  onClick={() => setDateType('booked')}
                  className={`px-3 py-1.5 text-sm font-medium transition-colors border-l ${
                    dateType === 'booked'
                      ? 'bg-blue-600 text-white'
                      : 'bg-white text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  Booked Date
                </button>
              </div>
            </div>

            {/* Date Range Selector */}
            <div className="flex items-center gap-2 flex-wrap">
              <Calendar className="h-4 w-4 text-gray-400" />
              <span className="text-sm text-gray-500">Range:</span>
              {dateRangeOptions.map((option) => (
                <button
                  key={option.value}
                  onClick={() => handleDateRangeChange(option.value)}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                    dateRange === option.value && !showCustomDate
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  {option.label}
                </button>
              ))}
              <button
                onClick={() => setShowCustomDate(!showCustomDate)}
                className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                  showCustomDate
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                Custom
              </button>
            </div>
          </div>

          {/* Custom Date Range Picker */}
          {showCustomDate && (
            <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg">
              <div className="flex items-center gap-2">
                <label className="text-sm text-gray-500">From:</label>
                <input
                  type="date"
                  value={customStartDate}
                  onChange={(e) => setCustomStartDate(e.target.value)}
                  className="px-3 py-1.5 rounded-lg border border-gray-200 text-sm"
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm text-gray-500">To:</label>
                <input
                  type="date"
                  value={customEndDate}
                  onChange={(e) => setCustomEndDate(e.target.value)}
                  className="px-3 py-1.5 rounded-lg border border-gray-200 text-sm"
                />
              </div>
              <button
                onClick={() => {
                  if (customStartDate && customEndDate) {
                    setDateRange('custom')
                  }
                }}
                disabled={!customStartDate || !customEndDate}
                className="px-4 py-1.5 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Apply
              </button>
            </div>
          )}
        </div>
      </div>

      <div className="p-6">
        {/* Section 1: Business Pulse KPIs */}
        <div className="mb-8">
          <div className="mb-4 flex items-center gap-2">
            <Zap className="h-5 w-5 text-yellow-500" />
            <h2 className="text-lg font-semibold text-gray-900">Business Pulse</h2>
            <span className="text-sm text-gray-500">Is everything okay today?</span>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
            {/* Bookings KPI */}
            <Link href="/bookings" className="group">
              <div className="rounded-xl border bg-white p-4 transition-all hover:border-blue-300 hover:shadow-md">
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-500">Bookings</span>
                  <div className="flex items-center gap-1">
                    {stats?.bookings?.change >= 0 ? (
                      <TrendingUp className="h-4 w-4 text-green-500" />
                    ) : (
                      <TrendingDown className="h-4 w-4 text-red-500" />
                    )}
                  </div>
                </div>
                <p className="text-2xl font-bold text-gray-900">{stats?.bookings?.today || 0}</p>
                <p className={`text-xs ${stats?.bookings?.change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                  {stats?.bookings?.change >= 0 ? '+' : ''}{stats?.bookings?.change || 0}% vs yesterday
                </p>
              </div>
            </Link>

            {/* Revenue KPI */}
            <Link href="/revenue" className="group">
              <div className="rounded-xl border bg-white p-4 transition-all hover:border-green-300 hover:shadow-md">
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-500">Revenue</span>
                  <IndianRupee className="h-4 w-4 text-green-500" />
                </div>
                <p className="text-2xl font-bold text-gray-900">{formatCurrency(stats?.revenue?.today || 0)}</p>
                <p className="text-xs text-gray-500">Gross today</p>
              </div>
            </Link>

            {/* Active Trips KPI */}
            <Link href="/trips" className="group">
              <div className="rounded-xl border bg-white p-4 transition-all hover:border-indigo-300 hover:shadow-md">
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-500">Active Trips</span>
                  <Bus className="h-4 w-4 text-indigo-500" />
                </div>
                <p className="text-2xl font-bold text-gray-900">{stats?.trips?.active || 0}</p>
                <p className="text-xs text-gray-500">Scheduled today</p>
              </div>
            </Link>

            {/* Pending Notifications KPI */}
            <Link href="/notifications?status=pending" className="group">
              <div className={`rounded-xl border bg-white p-4 transition-all hover:shadow-md ${
                (stats?.notifications?.pending || 0) > 10 ? 'border-yellow-300 hover:border-yellow-400' : 'hover:border-orange-300'
              }`}>
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-500">Pending</span>
                  <Clock className={`h-4 w-4 ${(stats?.notifications?.pending || 0) > 10 ? 'text-yellow-500' : 'text-orange-500'}`} />
                </div>
                <p className="text-2xl font-bold text-gray-900">{stats?.notifications?.pending || 0}</p>
                <p className="text-xs text-gray-500">Notifications</p>
              </div>
            </Link>

            {/* Net Profit KPI */}
            <Link href="/revenue" className="group">
              <div className="rounded-xl border bg-white p-4 transition-all hover:border-emerald-300 hover:shadow-md">
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-500">Net Profit</span>
                  <Banknote className="h-4 w-4 text-emerald-500" />
                </div>
                <p className="text-2xl font-bold text-emerald-600">{formatCurrency(stats?.revenue?.net || 0)}</p>
                <p className="text-xs text-gray-500">Margin: {stats?.revenue?.margin || 0}%</p>
              </div>
            </Link>

            {/* Customer Growth KPI */}
            <Link href="/customers" className="group">
              <div className="rounded-xl border bg-white p-4 transition-all hover:border-pink-300 hover:shadow-md">
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-500">Customers</span>
                  <Users className="h-4 w-4 text-pink-500" />
                </div>
                <div className="flex items-baseline gap-2">
                  <p className="text-2xl font-bold text-gray-900">{stats?.customers?.total || 0}</p>
                </div>
                <p className="text-xs text-gray-500">
                  <span className="text-green-600">{stats?.customers?.new || 0} new</span> • <span className="text-blue-600">{stats?.customers?.repeated || 0} repeat</span>
                </p>
              </div>
            </Link>
          </div>
        </div>

        {/* Section 2: Domain Navigation Tiles */}
        <div className="mb-8">
          <div className="mb-4 flex items-center gap-2">
            <Target className="h-5 w-5 text-blue-500" />
            <h2 className="text-lg font-semibold text-gray-900">Domain Dashboards</h2>
            <span className="text-sm text-gray-500">Navigate to detailed views</span>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {domainTiles.map((tile) => {
              const Icon = tile.icon
              // Get metric for this tile
              let metric = ''
              let status: 'good' | 'warning' | 'error' = 'good'

              switch (tile.id) {
                case 'executive':
                  metric = `${stats?.bookings?.today || 0} bookings today`
                  break
                case 'bookings':
                  metric = `${stats?.passengers?.today || 0} passengers`
                  break
                case 'revenue':
                  metric = formatCurrency(stats?.revenue?.gross || 0)
                  break
                case 'trips':
                  metric = `${stats?.trips?.active || 0} active trips`
                  break
                case 'notifications':
                  metric = `${stats?.notifications?.deliveryRate || 0}% delivery rate`
                  if ((stats?.notifications?.pending || 0) > 20) status = 'warning'
                  if ((stats?.notifications?.failed || 0) > 5) status = 'error'
                  break
                case 'customers':
                  metric = `${stats?.customers?.new || 0} new today`
                  break
              }

              return (
                <Link key={tile.id} href={tile.href}>
                  <div className={`group rounded-xl border ${tile.borderColor} ${tile.bgColor} p-5 transition-all ${tile.hoverBorder} hover:shadow-lg cursor-pointer`}>
                    <div className="flex items-start justify-between">
                      <div className={`flex h-12 w-12 items-center justify-center rounded-xl bg-white shadow-sm`}>
                        <Icon className={`h-6 w-6 ${tile.iconColor}`} />
                      </div>
                      <div className="flex items-center gap-2">
                        <div className={`h-2 w-2 rounded-full ${
                          status === 'good' ? 'bg-green-500' :
                          status === 'warning' ? 'bg-yellow-500' : 'bg-red-500'
                        }`} />
                        <ChevronRight className="h-5 w-5 text-gray-400 transition-transform group-hover:translate-x-1" />
                      </div>
                    </div>
                    <div className="mt-4">
                      <h3 className="text-lg font-semibold text-gray-900">{tile.title}</h3>
                      <p className="mt-1 text-sm text-gray-500">{tile.description}</p>
                    </div>
                    <div className="mt-3 flex items-center justify-between">
                      <span className="text-sm font-medium text-gray-700">{metric}</span>
                    </div>
                  </div>
                </Link>
              )
            })}
          </div>
        </div>

        {/* Section 3: Needs Attention */}
        {needsAttention.length > 0 && (
          <div className="mb-8">
            <div className="mb-4 flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-red-500" />
              <h2 className="text-lg font-semibold text-gray-900">Needs Attention</h2>
              <span className="rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-600">
                {needsAttention.length} items
              </span>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {needsAttention.map((item) => {
                const Icon = item.icon
                return (
                  <Link key={item.id} href={item.href}>
                    <div className={`group rounded-xl border p-4 transition-all hover:shadow-md ${
                      item.severity === 'error'
                        ? 'border-red-200 bg-red-50 hover:border-red-300'
                        : 'border-yellow-200 bg-yellow-50 hover:border-yellow-300'
                    }`}>
                      <div className="flex items-start gap-3">
                        <div className={`flex h-10 w-10 items-center justify-center rounded-lg ${
                          item.severity === 'error' ? 'bg-red-100' : 'bg-yellow-100'
                        }`}>
                          <Icon className={`h-5 w-5 ${
                            item.severity === 'error' ? 'text-red-600' : 'text-yellow-600'
                          }`} />
                        </div>
                        <div className="flex-1">
                          <p className="font-medium text-gray-900">{item.title}</p>
                          <p className="text-sm text-gray-500">{item.description}</p>
                        </div>
                        <ArrowRight className="h-5 w-5 text-gray-400 transition-transform group-hover:translate-x-1" />
                      </div>
                    </div>
                  </Link>
                )
              })}
            </div>
          </div>
        )}

        {/* Section 4: Quick Stats Summary */}
        <div className="grid gap-6 lg:grid-cols-2">
          {/* P&L Summary */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5 text-gray-400" />
                <h3 className="font-semibold text-gray-900">P&L Summary</h3>
              </div>
              <Link href="/revenue" className="text-sm text-blue-600 hover:text-blue-700">
                View details →
              </Link>
            </div>
            <div className="grid grid-cols-4 gap-3">
              <div className="rounded-lg bg-blue-50 p-3 text-center">
                <p className="text-xs text-blue-600">Gross</p>
                <p className="text-lg font-bold text-blue-700">{formatCurrency(stats?.revenue?.gross || 0)}</p>
              </div>
              <div className="rounded-lg bg-gray-50 p-3 text-center">
                <p className="text-xs text-gray-600">GST</p>
                <p className="text-lg font-bold text-gray-700">{formatCurrency(stats?.revenue?.gst || 0)}</p>
              </div>
              <div className="rounded-lg bg-red-50 p-3 text-center">
                <p className="text-xs text-red-600">Expenses</p>
                <p className="text-lg font-bold text-red-700">{formatCurrency(stats?.revenue?.expenses || 0)}</p>
              </div>
              <div className="rounded-lg bg-green-50 p-3 text-center">
                <p className="text-xs text-green-600">Net Profit</p>
                <p className="text-lg font-bold text-green-700">{formatCurrency(stats?.revenue?.net || 0)}</p>
              </div>
            </div>
          </div>

          {/* Customer Summary */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <PieChart className="h-5 w-5 text-gray-400" />
                <h3 className="font-semibold text-gray-900">Customer Summary</h3>
              </div>
              <Link href="/customers" className="text-sm text-blue-600 hover:text-blue-700">
                View details →
              </Link>
            </div>
            <div className="grid grid-cols-4 gap-3">
              <div className="rounded-lg bg-green-50 p-3 text-center">
                <div className="mb-1 flex items-center justify-center">
                  <UserPlus className="h-4 w-4 text-green-600" />
                </div>
                <p className="text-lg font-bold text-green-700">{stats?.customers?.new || 0}</p>
                <p className="text-xs text-green-600">New</p>
              </div>
              <div className="rounded-lg bg-blue-50 p-3 text-center">
                <div className="mb-1 flex items-center justify-center">
                  <UserCheck className="h-4 w-4 text-blue-600" />
                </div>
                <p className="text-lg font-bold text-blue-700">{stats?.customers?.repeated || 0}</p>
                <p className="text-xs text-blue-600">Repeat</p>
              </div>
              <div className="rounded-lg bg-indigo-50 p-3 text-center">
                <p className="text-lg font-bold text-indigo-700">{stats?.passengers?.male || 0}</p>
                <p className="text-xs text-indigo-600">Male</p>
              </div>
              <div className="rounded-lg bg-pink-50 p-3 text-center">
                <p className="text-lg font-bold text-pink-700">{stats?.passengers?.female || 0}</p>
                <p className="text-xs text-pink-600">Female</p>
              </div>
            </div>
          </div>
        </div>

        {/* Section 5: Notification Status */}
        <div className="mt-6 rounded-xl border bg-white p-5">
          <div className="mb-4 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Bell className="h-5 w-5 text-gray-400" />
              <h3 className="font-semibold text-gray-900">Notification Status</h3>
            </div>
            <Link href="/notifications" className="text-sm text-blue-600 hover:text-blue-700">
              View all →
            </Link>
          </div>
          <div className="grid grid-cols-5 gap-3">
            <div className="rounded-lg bg-indigo-50 p-3 text-center">
              <p className="text-2xl font-bold text-indigo-700">{stats?.notifications?.sent || 0}</p>
              <p className="text-xs text-indigo-600">Sent</p>
            </div>
            <div className="rounded-lg bg-green-50 p-3 text-center">
              <p className="text-2xl font-bold text-green-700">{stats?.notifications?.delivered || 0}</p>
              <p className="text-xs text-green-600">Delivered</p>
            </div>
            <div className="rounded-lg bg-yellow-50 p-3 text-center">
              <p className="text-2xl font-bold text-yellow-700">{stats?.notifications?.pending || 0}</p>
              <p className="text-xs text-yellow-600">Pending</p>
            </div>
            <div className="rounded-lg bg-red-50 p-3 text-center">
              <p className="text-2xl font-bold text-red-700">{stats?.notifications?.failed || 0}</p>
              <p className="text-xs text-red-600">Failed</p>
            </div>
            <div className="rounded-lg bg-emerald-50 p-3 text-center">
              <p className="text-2xl font-bold text-emerald-700">{stats?.notifications?.deliveryRate || 0}%</p>
              <p className="text-xs text-emerald-600">Delivery Rate</p>
            </div>
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}
