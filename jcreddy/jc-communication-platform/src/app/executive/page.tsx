'use client'

import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { formatCurrency } from '@/lib/utils'
import {
  Home,
  Ticket,
  IndianRupee,
  Bus,
  Bell,
  Users,
  TrendingUp,
  TrendingDown,
  AlertTriangle,
  ArrowRight,
  Calendar,
  BarChart3,
  Activity,
  Clock,
  CheckCircle,
  XCircle,
  Banknote,
  Percent,
  Lock,
} from 'lucide-react'
import Link from 'next/link'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  Cell,
} from 'recharts'

async function fetchDashboardStats(dateType: string, dateRange: string, customStart?: string, customEnd?: string) {
  let url = `/api/dashboard/stats?dateType=${dateType}&range=${dateRange}`
  if (dateRange === 'custom' && customStart && customEnd) {
    url += `&startDate=${customStart}&endDate=${customEnd}`
  }
  const res = await fetch(url)
  if (!res.ok) throw new Error('Failed to fetch stats')
  return res.json()
}

const dateRangeOptions = [
  { value: 'today', label: 'Today' },
  { value: 'yesterday', label: 'Yesterday' },
  { value: '7days', label: 'Last 7 Days' },
  { value: 'next7days', label: 'Next 7 Days' },
  { value: '15days', label: 'Last 15 Days' },
  { value: 'month', label: 'This Month' },
  { value: 'lastMonth', label: 'Last Month' },
  { value: 'nextMonth', label: 'Next Month' },
  { value: 'custom', label: 'Custom' },
]

// Navigation items for domain switching
const domainNav = [
  { id: 'executive', label: 'Executive', href: '/executive', active: true },
  { id: 'bookings', label: 'Bookings', href: '/bookings' },
  { id: 'revenue', label: 'Revenue', href: '/revenue' },
  { id: 'trips', label: 'Trips', href: '/trips' },
  { id: 'notifications', label: 'Notifications', href: '/notifications' },
  { id: 'customers', label: 'Customers', href: '/customers' },
]

export default function ExecutiveOverviewPage() {
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

  const handleDateRangeChange = (value: string) => {
    setDateRange(value)
    if (value === 'custom') {
      setShowCustomDate(true)
    } else {
      setShowCustomDate(false)
    }
  }

  return (
    <DashboardLayout>
      {/* Navigation Header */}
      <div className="border-b bg-white">
        <div className="px-6 py-3">
          {/* Breadcrumb */}
          <div className="flex items-center gap-2 text-sm text-gray-500 mb-2">
            <Link href="/" className="hover:text-blue-600 flex items-center gap-1">
              <Home className="h-4 w-4" />
              Analytics Home
            </Link>
            <span>›</span>
            <span className="text-gray-900 font-medium">Executive Overview</span>
          </div>

          {/* Domain Switch Buttons */}
          <div className="flex items-center gap-2 overflow-x-auto pb-2">
            {domainNav.map((item) => (
              <Link
                key={item.id}
                href={item.href}
                className={`px-4 py-2 rounded-lg text-sm font-medium whitespace-nowrap transition-colors ${
                  item.active
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {item.label}
              </Link>
            ))}
          </div>
        </div>
      </div>

      {/* Header with Title and Date Filter */}
      <div className="border-b bg-white px-6 py-4">
        <div className="flex flex-col gap-4">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Executive Overview</h1>
              <p className="text-sm text-gray-500">10-second business health check</p>
            </div>

            {/* Date Type Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Filter by:</span>
              <div className="flex rounded-lg border bg-gray-50 p-1">
                <button
                  onClick={() => setDateType('travel')}
                  className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
                    dateType === 'travel'
                      ? 'bg-white text-blue-600 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Travel Date
                </button>
                <button
                  onClick={() => setDateType('booked')}
                  className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
                    dateType === 'booked'
                      ? 'bg-white text-blue-600 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Booked Date
                </button>
              </div>
            </div>
          </div>

          {/* Date Range Filter */}
          <div className="flex flex-wrap items-center gap-2">
            <Calendar className="h-5 w-5 text-gray-400" />
            {dateRangeOptions.map((option) => (
              <button
                key={option.value}
                onClick={() => handleDateRangeChange(option.value)}
                className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                  dateRange === option.value
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {option.label}
              </button>
            ))}
          </div>

          {/* Custom Date Range Picker */}
          {showCustomDate && (
            <div className="flex flex-wrap items-center gap-3 p-3 rounded-lg bg-gray-50 border">
              <div className="flex items-center gap-2">
                <label className="text-sm text-gray-600">From:</label>
                <input
                  type="date"
                  value={customStartDate}
                  onChange={(e) => setCustomStartDate(e.target.value)}
                  className="px-3 py-1.5 rounded-lg border bg-white text-sm"
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm text-gray-600">To:</label>
                <input
                  type="date"
                  value={customEndDate}
                  onChange={(e) => setCustomEndDate(e.target.value)}
                  className="px-3 py-1.5 rounded-lg border bg-white text-sm"
                />
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="p-6">
        {/* Row 1: Key Executive KPIs */}
        <div className="mb-6 grid gap-4 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-7">
          {/* Bookings */}
          <Link href="/bookings" className="group">
            <div className="rounded-xl border bg-white p-4 transition-all hover:border-blue-300 hover:shadow-md h-full">
              <div className="flex items-center gap-2 mb-2">
                <Ticket className="h-5 w-5 text-blue-600" />
                <span className="text-sm font-medium text-gray-500">Bookings</span>
              </div>
              <p className="text-3xl font-bold text-gray-900">{stats?.bookings?.today || 0}</p>
              <div className="mt-1 flex items-center gap-1">
                {(stats?.bookings?.change || 0) >= 0 ? (
                  <TrendingUp className="h-4 w-4 text-green-500" />
                ) : (
                  <TrendingDown className="h-4 w-4 text-red-500" />
                )}
                <span className={`text-sm ${(stats?.bookings?.change || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                  {stats?.bookings?.change || 0}%
                </span>
              </div>
            </div>
          </Link>

          {/* Revenue */}
          <Link href="/revenue" className="group">
            <div className="rounded-xl border bg-white p-4 transition-all hover:border-green-300 hover:shadow-md h-full">
              <div className="flex items-center gap-2 mb-2">
                <IndianRupee className="h-5 w-5 text-green-600" />
                <span className="text-sm font-medium text-gray-500">Revenue</span>
              </div>
              <p className="text-3xl font-bold text-gray-900">{formatCurrency(stats?.revenue?.today || 0)}</p>
              <p className="text-sm text-gray-500 mt-1">Gross</p>
            </div>
          </Link>

          {/* Active Trips */}
          <Link href="/trips" className="group">
            <div className="rounded-xl border bg-white p-4 transition-all hover:border-indigo-300 hover:shadow-md h-full">
              <div className="flex items-center gap-2 mb-2">
                <Bus className="h-5 w-5 text-indigo-600" />
                <span className="text-sm font-medium text-gray-500">Active Trips</span>
              </div>
              <p className="text-3xl font-bold text-gray-900">{stats?.trips?.active || 0}</p>
              <p className="text-sm text-gray-500 mt-1">Scheduled</p>
            </div>
          </Link>

          {/* Blocked Trips */}
          <Link href="/trips?status=blocked" className="group">
            <div className={`rounded-xl border bg-white p-4 transition-all hover:shadow-md h-full ${
              (stats?.trips?.blocked || 0) > 0 ? 'border-red-200 hover:border-red-300' : 'hover:border-gray-300'
            }`}>
              <div className="flex items-center gap-2 mb-2">
                <Lock className="h-5 w-5 text-red-600" />
                <span className="text-sm font-medium text-gray-500">Blocked</span>
              </div>
              <p className={`text-3xl font-bold ${(stats?.trips?.blocked || 0) > 0 ? 'text-red-600' : 'text-gray-900'}`}>
                {stats?.trips?.blocked || 0}
              </p>
              <p className="text-sm text-gray-500 mt-1">Trips</p>
            </div>
          </Link>

          {/* Pending Notifications */}
          <Link href="/notifications?status=pending" className="group">
            <div className={`rounded-xl border bg-white p-4 transition-all hover:shadow-md h-full ${
              (stats?.notifications?.pending || 0) > 10 ? 'border-yellow-200 hover:border-yellow-300' : 'hover:border-orange-300'
            }`}>
              <div className="flex items-center gap-2 mb-2">
                <Clock className="h-5 w-5 text-yellow-600" />
                <span className="text-sm font-medium text-gray-500">Pending</span>
              </div>
              <p className={`text-3xl font-bold ${(stats?.notifications?.pending || 0) > 10 ? 'text-yellow-600' : 'text-gray-900'}`}>
                {stats?.notifications?.pending || 0}
              </p>
              <p className="text-sm text-gray-500 mt-1">Notifications</p>
            </div>
          </Link>

          {/* Net Profit */}
          <Link href="/revenue" className="group">
            <div className="rounded-xl border bg-white p-4 transition-all hover:border-emerald-300 hover:shadow-md h-full">
              <div className="flex items-center gap-2 mb-2">
                <Banknote className="h-5 w-5 text-emerald-600" />
                <span className="text-sm font-medium text-gray-500">Net Profit</span>
              </div>
              <p className="text-3xl font-bold text-emerald-600">{formatCurrency(stats?.revenue?.net || 0)}</p>
              <p className="text-sm text-gray-500 mt-1">After expenses</p>
            </div>
          </Link>

          {/* Margin */}
          <Link href="/revenue" className="group">
            <div className="rounded-xl border bg-white p-4 transition-all hover:border-purple-300 hover:shadow-md h-full">
              <div className="flex items-center gap-2 mb-2">
                <Percent className="h-5 w-5 text-purple-600" />
                <span className="text-sm font-medium text-gray-500">Margin</span>
              </div>
              <p className="text-3xl font-bold text-purple-600">{stats?.revenue?.margin || 0}%</p>
              <p className="text-sm text-gray-500 mt-1">Profit margin</p>
            </div>
          </Link>
        </div>

        {/* Row 2: Trends & Route Performance */}
        <div className="mb-6 grid gap-6 lg:grid-cols-3">
          {/* Booking & Revenue Trend */}
          <div className="lg:col-span-2 rounded-xl border bg-white p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h3 className="font-semibold text-gray-900">Booking & Revenue Trend</h3>
                <p className="text-sm text-gray-500">
                  {dateRange === 'today' || dateRange === 'yesterday' ? 'Hourly' : 'Daily'} pattern
                </p>
              </div>
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-2">
                  <div className="h-3 w-3 rounded-full bg-blue-500" />
                  <span className="text-sm text-gray-600">Bookings</span>
                </div>
              </div>
            </div>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={stats?.bookingTrend || []}>
                  <defs>
                    <linearGradient id="colorBookings" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.1} />
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="name" axisLine={false} tickLine={false} fontSize={12} />
                  <YAxis axisLine={false} tickLine={false} fontSize={12} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px',
                    }}
                  />
                  <Area
                    type="monotone"
                    dataKey="bookings"
                    stroke="#3b82f6"
                    strokeWidth={2}
                    fillOpacity={1}
                    fill="url(#colorBookings)"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* High-level Exceptions */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4">
              <h3 className="font-semibold text-gray-900">Exceptions</h3>
              <p className="text-sm text-gray-500">Items needing attention</p>
            </div>
            <div className="space-y-3">
              {(stats?.notifications?.pending || 0) > 10 && (
                <Link href="/notifications?status=pending">
                  <div className="flex items-center gap-3 p-3 rounded-lg bg-yellow-50 border border-yellow-200 hover:border-yellow-300 transition-colors">
                    <Clock className="h-5 w-5 text-yellow-600" />
                    <div className="flex-1">
                      <p className="font-medium text-gray-900">{stats?.notifications?.pending} Pending</p>
                      <p className="text-sm text-gray-500">Notifications backlog</p>
                    </div>
                    <ArrowRight className="h-5 w-5 text-gray-400" />
                  </div>
                </Link>
              )}

              {(stats?.notifications?.failed || 0) > 0 && (
                <Link href="/notifications?status=failed">
                  <div className="flex items-center gap-3 p-3 rounded-lg bg-red-50 border border-red-200 hover:border-red-300 transition-colors">
                    <XCircle className="h-5 w-5 text-red-600" />
                    <div className="flex-1">
                      <p className="font-medium text-gray-900">{stats?.notifications?.failed} Failed</p>
                      <p className="text-sm text-gray-500">Delivery failures</p>
                    </div>
                    <ArrowRight className="h-5 w-5 text-gray-400" />
                  </div>
                </Link>
              )}

              {(stats?.trips?.blocked || 0) > 0 && (
                <Link href="/trips?status=blocked">
                  <div className="flex items-center gap-3 p-3 rounded-lg bg-red-50 border border-red-200 hover:border-red-300 transition-colors">
                    <Lock className="h-5 w-5 text-red-600" />
                    <div className="flex-1">
                      <p className="font-medium text-gray-900">{stats?.trips?.blocked} Blocked</p>
                      <p className="text-sm text-gray-500">Trips need review</p>
                    </div>
                    <ArrowRight className="h-5 w-5 text-gray-400" />
                  </div>
                </Link>
              )}

              {(stats?.bookings?.change || 0) < -20 && (
                <Link href="/bookings">
                  <div className="flex items-center gap-3 p-3 rounded-lg bg-orange-50 border border-orange-200 hover:border-orange-300 transition-colors">
                    <TrendingDown className="h-5 w-5 text-orange-600" />
                    <div className="flex-1">
                      <p className="font-medium text-gray-900">{Math.abs(stats?.bookings?.change || 0)}% Drop</p>
                      <p className="text-sm text-gray-500">Booking decline</p>
                    </div>
                    <ArrowRight className="h-5 w-5 text-gray-400" />
                  </div>
                </Link>
              )}

              {(stats?.notifications?.pending || 0) <= 10 &&
               (stats?.notifications?.failed || 0) === 0 &&
               (stats?.trips?.blocked || 0) === 0 &&
               (stats?.bookings?.change || 0) >= -20 && (
                <div className="flex items-center gap-3 p-3 rounded-lg bg-green-50 border border-green-200">
                  <CheckCircle className="h-5 w-5 text-green-600" />
                  <div className="flex-1">
                    <p className="font-medium text-green-700">All Clear</p>
                    <p className="text-sm text-green-600">No issues detected</p>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Row 3: Top/Bottom Routes */}
        <div className="grid gap-6 lg:grid-cols-2">
          {/* Top Performing Routes */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h3 className="font-semibold text-gray-900">Top Routes</h3>
                <p className="text-sm text-gray-500">By bookings</p>
              </div>
              <Link href="/bookings" className="text-sm text-blue-600 hover:text-blue-700">
                View all →
              </Link>
            </div>
            <div className="space-y-3">
              {stats?.serviceStats?.slice(0, 5).map((service: any, idx: number) => (
                <div key={idx} className="flex items-center gap-3">
                  <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-green-100 text-sm font-bold text-green-600">
                    {idx + 1}
                  </div>
                  <div className="flex-1">
                    <p className="font-medium text-gray-900">{service.service}</p>
                    <p className="text-xs text-gray-500">
                      {service.routeInfo?.origin || '--'} → {service.routeInfo?.destination || '--'}
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="font-semibold text-gray-900">{service.bookings}</p>
                    <p className="text-xs text-green-600">{formatCurrency(service.revenue)}</p>
                  </div>
                </div>
              )) || (
                <p className="text-center text-gray-500 py-4">No data available</p>
              )}
            </div>
          </div>

          {/* Inactive/Low Routes */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h3 className="font-semibold text-gray-900">Inactive Routes</h3>
                <p className="text-sm text-gray-500">No upcoming bookings</p>
              </div>
              <Link href="/trips" className="text-sm text-blue-600 hover:text-blue-700">
                View all →
              </Link>
            </div>
            <div className="space-y-3">
              {stats?.inactiveServices?.slice(0, 5).map((route: any, idx: number) => (
                <div key={route.id || idx} className="flex items-center gap-3">
                  <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gray-100 text-sm font-bold text-gray-500">
                    <Activity className="h-4 w-4" />
                  </div>
                  <div className="flex-1">
                    <p className="font-medium text-gray-900">{route.service_number || route.route_name}</p>
                    <p className="text-xs text-gray-500">
                      {route.origin} → {route.destination}
                    </p>
                  </div>
                  <div>
                    <span className="rounded bg-gray-100 px-2 py-1 text-xs font-medium text-gray-600">
                      Inactive
                    </span>
                  </div>
                </div>
              )) || (
                <p className="text-center text-gray-500 py-4">All routes active</p>
              )}
            </div>
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}
