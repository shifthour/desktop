'use client'

import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { formatCurrency } from '@/lib/utils'
import {
  Home,
  IndianRupee,
  TrendingUp,
  TrendingDown,
  Calendar,
  BarChart3,
  ArrowRight,
  Banknote,
  Percent,
  Receipt,
  MinusCircle,
  PlusCircle,
  AlertTriangle,
  XCircle,
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
  ComposedChart,
  Line,
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
  { id: 'executive', label: 'Executive', href: '/executive' },
  { id: 'bookings', label: 'Bookings', href: '/bookings' },
  { id: 'revenue', label: 'Revenue', href: '/revenue', active: true },
  { id: 'trips', label: 'Trips', href: '/trips' },
  { id: 'notifications', label: 'Notifications', href: '/notifications' },
  { id: 'customers', label: 'Customers', href: '/customers' },
]

export default function RevenuePLPage() {
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

  // Build waterfall data for Gross to Net to Profit
  const waterfallData = [
    { name: 'Gross Revenue', value: stats?.revenue?.gross || 0, fill: '#3b82f6' },
    { name: 'GST', value: -(stats?.revenue?.gst || 0), fill: '#6b7280' },
    { name: 'Expenses', value: -(stats?.revenue?.expenses || 0), fill: '#ef4444' },
    { name: 'Net Profit', value: stats?.revenue?.net || 0, fill: '#22c55e' },
  ]

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
            <span className="text-gray-900 font-medium">Revenue & P&L</span>
          </div>

          {/* Domain Switch Buttons */}
          <div className="flex items-center gap-2 overflow-x-auto pb-2">
            {domainNav.map((item) => (
              <Link
                key={item.id}
                href={item.href}
                className={`px-4 py-2 rounded-lg text-sm font-medium whitespace-nowrap transition-colors ${
                  item.active
                    ? 'bg-green-600 text-white'
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
              <h1 className="text-2xl font-bold text-gray-900">Revenue & P&L Dashboard</h1>
              <p className="text-sm text-gray-500">Financial performance & leakage analysis</p>
            </div>

            {/* Date Type Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Filter by:</span>
              <div className="flex rounded-lg border bg-gray-50 p-1">
                <button
                  onClick={() => setDateType('travel')}
                  className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
                    dateType === 'travel'
                      ? 'bg-white text-green-600 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Travel Date
                </button>
                <button
                  onClick={() => setDateType('booked')}
                  className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
                    dateType === 'booked'
                      ? 'bg-white text-green-600 shadow-sm'
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
                    ? 'bg-green-600 text-white'
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
        {/* Row 1: Executive Finance KPI Strip */}
        <div className="mb-6 grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
          {/* Gross Revenue */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <IndianRupee className="h-5 w-5 text-blue-600" />
              <span className="text-sm font-medium text-gray-500">Gross Revenue</span>
            </div>
            <p className="text-2xl font-bold text-blue-600">{formatCurrency(stats?.revenue?.gross || 0)}</p>
            <p className="text-xs text-gray-500 mt-1">Before deductions</p>
          </div>

          {/* Net Revenue */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <Banknote className="h-5 w-5 text-indigo-600" />
              <span className="text-sm font-medium text-gray-500">Net Revenue</span>
            </div>
            <p className="text-2xl font-bold text-indigo-600">{formatCurrency(stats?.revenue?.today || 0)}</p>
            <p className="text-xs text-gray-500 mt-1">After adjustments</p>
          </div>

          {/* GST Collected */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <Receipt className="h-5 w-5 text-gray-600" />
              <span className="text-sm font-medium text-gray-500">GST Collected</span>
            </div>
            <p className="text-2xl font-bold text-gray-700">{formatCurrency(stats?.revenue?.gst || 0)}</p>
            <p className="text-xs text-gray-500 mt-1">Tax liability</p>
          </div>

          {/* Expenses */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <MinusCircle className="h-5 w-5 text-red-600" />
              <span className="text-sm font-medium text-gray-500">Expenses</span>
            </div>
            <p className="text-2xl font-bold text-red-600">{formatCurrency(stats?.revenue?.expenses || 0)}</p>
            <p className="text-xs text-gray-500 mt-1">Operating costs</p>
          </div>

          {/* Net Profit */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <PlusCircle className="h-5 w-5 text-emerald-600" />
              <span className="text-sm font-medium text-gray-500">Net Profit</span>
            </div>
            <p className="text-2xl font-bold text-emerald-600">{formatCurrency(stats?.revenue?.net || 0)}</p>
            <p className="text-xs text-gray-500 mt-1">Bottom line</p>
          </div>

          {/* Margin */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <Percent className="h-5 w-5 text-purple-600" />
              <span className="text-sm font-medium text-gray-500">Margin %</span>
            </div>
            <p className={`text-2xl font-bold ${(stats?.revenue?.margin || 0) >= 20 ? 'text-emerald-600' : (stats?.revenue?.margin || 0) >= 10 ? 'text-yellow-600' : 'text-red-600'}`}>
              {stats?.revenue?.margin || 0}%
            </p>
            <p className="text-xs text-gray-500 mt-1">Profit margin</p>
          </div>
        </div>

        {/* Row 2: Trends & Waterfall */}
        <div className="mb-6 grid gap-6 lg:grid-cols-2">
          {/* Revenue vs Expenses Trend */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4">
              <h3 className="font-semibold text-gray-900">Revenue Trend</h3>
              <p className="text-sm text-gray-500">
                {dateRange === 'today' || dateRange === 'yesterday' ? 'Hourly' : 'Daily'} revenue pattern
              </p>
            </div>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={stats?.bookingTrend || []}>
                  <defs>
                    <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#22c55e" stopOpacity={0.1} />
                      <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="name" axisLine={false} tickLine={false} fontSize={12} />
                  <YAxis axisLine={false} tickLine={false} fontSize={12} tickFormatter={(v) => `₹${(v/1000).toFixed(0)}k`} />
                  <Tooltip
                    formatter={(value: any) => [formatCurrency(value), 'Revenue']}
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px',
                    }}
                  />
                  <Area
                    type="monotone"
                    dataKey="revenue"
                    stroke="#22c55e"
                    strokeWidth={2}
                    fillOpacity={1}
                    fill="url(#colorRevenue)"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* P&L Waterfall */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4">
              <h3 className="font-semibold text-gray-900">P&L Breakdown</h3>
              <p className="text-sm text-gray-500">Gross to Net Profit flow</p>
            </div>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={waterfallData} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} />
                  <XAxis type="number" axisLine={false} tickLine={false} fontSize={12} tickFormatter={(v) => `₹${(Math.abs(v)/1000).toFixed(0)}k`} />
                  <YAxis type="category" dataKey="name" axisLine={false} tickLine={false} fontSize={12} width={100} />
                  <Tooltip
                    formatter={(value: any) => [formatCurrency(Math.abs(value)), value < 0 ? 'Deduction' : 'Amount']}
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px',
                    }}
                  />
                  <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                    {waterfallData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* Row 3: Route Profitability */}
        <div className="grid gap-6 lg:grid-cols-2">
          {/* Top Profitable Routes */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h3 className="font-semibold text-gray-900">Top Revenue Routes</h3>
                <p className="text-sm text-gray-500">By revenue generated</p>
              </div>
              <Link href="/bookings" className="text-sm text-blue-600 hover:text-blue-700">
                View all →
              </Link>
            </div>
            <div className="space-y-3">
              {stats?.serviceStats?.slice(0, 5).map((service: any, idx: number) => {
                const margin = service.revenue > 0 ? Math.round((service.revenue * 0.3) / service.revenue * 100) : 0 // Placeholder margin
                return (
                  <div key={idx} className="flex items-center gap-3 p-3 rounded-lg bg-gray-50">
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
                      <p className="font-semibold text-green-600">{formatCurrency(service.revenue)}</p>
                      <p className="text-xs text-gray-500">{service.bookings} bookings</p>
                    </div>
                  </div>
                )
              }) || (
                <p className="text-center text-gray-500 py-4">No data available</p>
              )}
            </div>
          </div>

          {/* Revenue Breakdown by Channel/Category */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4">
              <h3 className="font-semibold text-gray-900">Financial Summary</h3>
              <p className="text-sm text-gray-500">Detailed breakdown</p>
            </div>
            <div className="space-y-4">
              {/* Gross Revenue */}
              <div className="flex items-center justify-between p-3 rounded-lg bg-blue-50">
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-blue-100">
                    <IndianRupee className="h-5 w-5 text-blue-600" />
                  </div>
                  <div>
                    <p className="font-medium text-gray-900">Gross Revenue</p>
                    <p className="text-xs text-gray-500">Total bookings value</p>
                  </div>
                </div>
                <p className="text-xl font-bold text-blue-600">{formatCurrency(stats?.revenue?.gross || 0)}</p>
              </div>

              {/* GST */}
              <div className="flex items-center justify-between p-3 rounded-lg bg-gray-50">
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-gray-200">
                    <Receipt className="h-5 w-5 text-gray-600" />
                  </div>
                  <div>
                    <p className="font-medium text-gray-900">GST Collected</p>
                    <p className="text-xs text-gray-500">CGST + SGST</p>
                  </div>
                </div>
                <p className="text-xl font-bold text-gray-600">- {formatCurrency(stats?.revenue?.gst || 0)}</p>
              </div>

              {/* Expenses */}
              <div className="flex items-center justify-between p-3 rounded-lg bg-red-50">
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-red-100">
                    <MinusCircle className="h-5 w-5 text-red-600" />
                  </div>
                  <div>
                    <p className="font-medium text-gray-900">Operating Expenses</p>
                    <p className="text-xs text-gray-500">Fuel, maintenance, staff</p>
                  </div>
                </div>
                <p className="text-xl font-bold text-red-600">- {formatCurrency(stats?.revenue?.expenses || 0)}</p>
              </div>

              {/* Net Profit */}
              <div className="flex items-center justify-between p-3 rounded-lg bg-green-50 border-2 border-green-200">
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-green-100">
                    <Banknote className="h-5 w-5 text-green-600" />
                  </div>
                  <div>
                    <p className="font-medium text-gray-900">Net Profit</p>
                    <p className="text-xs text-gray-500">Margin: {stats?.revenue?.margin || 0}%</p>
                  </div>
                </div>
                <p className="text-xl font-bold text-green-600">{formatCurrency(stats?.revenue?.net || 0)}</p>
              </div>
            </div>
          </div>
        </div>

        {/* Row 4: Cancellation Impact (Leakage Monitoring) */}
        <div className="mt-6 rounded-xl border bg-white p-5">
          <div className="mb-4 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-orange-500" />
              <h3 className="font-semibold text-gray-900">Revenue Leakage Monitor</h3>
            </div>
            <Link href="/bookings?status=cancelled" className="text-sm text-blue-600 hover:text-blue-700">
              View cancellations →
            </Link>
          </div>
          <div className="grid gap-4 sm:grid-cols-3">
            <div className="p-4 rounded-lg bg-orange-50 border border-orange-200">
              <div className="flex items-center gap-2 mb-2">
                <XCircle className="h-5 w-5 text-orange-600" />
                <span className="text-sm font-medium text-orange-800">Cancellations</span>
              </div>
              <p className="text-2xl font-bold text-orange-600">{stats?.cancellations?.today || 0}</p>
              <p className="text-xs text-orange-700 mt-1">Cancelled bookings</p>
            </div>
            <div className="p-4 rounded-lg bg-yellow-50 border border-yellow-200">
              <div className="flex items-center gap-2 mb-2">
                <AlertTriangle className="h-5 w-5 text-yellow-600" />
                <span className="text-sm font-medium text-yellow-800">Refund Amount</span>
              </div>
              <p className="text-2xl font-bold text-yellow-600">{formatCurrency((stats?.cancellations?.today || 0) * 500)}</p>
              <p className="text-xs text-yellow-700 mt-1">Estimated refunds</p>
            </div>
            <div className="p-4 rounded-lg bg-red-50 border border-red-200">
              <div className="flex items-center gap-2 mb-2">
                <TrendingDown className="h-5 w-5 text-red-600" />
                <span className="text-sm font-medium text-red-800">Revenue Impact</span>
              </div>
              <p className="text-2xl font-bold text-red-600">
                {((stats?.cancellations?.today || 0) / Math.max(stats?.bookings?.today || 1, 1) * 100).toFixed(1)}%
              </p>
              <p className="text-xs text-red-700 mt-1">Cancellation rate</p>
            </div>
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}
