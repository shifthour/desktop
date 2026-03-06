'use client'

import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { formatCurrency } from '@/lib/utils'
import {
  Home,
  Users,
  UserPlus,
  UserCheck,
  TrendingUp,
  TrendingDown,
  Calendar,
  ArrowRight,
  RefreshCw,
  UserX,
  Activity,
  Target,
  Award,
  Heart,
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
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
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
  { id: 'revenue', label: 'Revenue', href: '/revenue' },
  { id: 'trips', label: 'Trips', href: '/trips' },
  { id: 'notifications', label: 'Notifications', href: '/notifications' },
  { id: 'customers', label: 'Customers', href: '/customers', active: true },
]

const COLORS = ['#22c55e', '#3b82f6', '#f59e0b', '#ef4444']

export default function CustomerGrowthPage() {
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

  // Customer type distribution
  const customerTypeData = [
    { name: 'New', value: stats?.customers?.new || 0, color: '#22c55e' },
    { name: 'Repeat', value: stats?.customers?.repeated || 0, color: '#3b82f6' },
  ]

  // Gender distribution
  const genderData = [
    { name: 'Male', value: stats?.passengers?.male || 0, color: '#6366f1' },
    { name: 'Female', value: stats?.passengers?.female || 0, color: '#ec4899' },
    { name: 'Other', value: stats?.passengers?.other || 0, color: '#8b5cf6' },
  ]

  const totalCustomers = (stats?.customers?.new || 0) + (stats?.customers?.repeated || 0)
  const repeatRate = totalCustomers > 0 ? Math.round((stats?.customers?.repeated || 0) / totalCustomers * 100) : 0

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
            <span className="text-gray-900 font-medium">Customer & Growth</span>
          </div>

          {/* Domain Switch Buttons */}
          <div className="flex items-center gap-2 overflow-x-auto pb-2">
            {domainNav.map((item) => (
              <Link
                key={item.id}
                href={item.href}
                className={`px-4 py-2 rounded-lg text-sm font-medium whitespace-nowrap transition-colors ${
                  item.active
                    ? 'bg-pink-600 text-white'
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
              <h1 className="text-2xl font-bold text-gray-900">Customer & Growth Dashboard</h1>
              <p className="text-sm text-gray-500">Acquisition, retention, churn & cohort analysis</p>
            </div>

            {/* Date Type Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Filter by:</span>
              <div className="flex rounded-lg border bg-gray-50 p-1">
                <button
                  onClick={() => setDateType('travel')}
                  className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
                    dateType === 'travel'
                      ? 'bg-white text-pink-600 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Travel Date
                </button>
                <button
                  onClick={() => setDateType('booked')}
                  className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
                    dateType === 'booked'
                      ? 'bg-white text-pink-600 shadow-sm'
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
                    ? 'bg-pink-600 text-white'
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
        {/* Row 1: Customer KPIs */}
        <div className="mb-6 grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
          {/* Total Customers */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <Users className="h-5 w-5 text-gray-600" />
              <span className="text-sm font-medium text-gray-500">Total Customers</span>
            </div>
            <p className="text-3xl font-bold text-gray-900">{stats?.customers?.total || 0}</p>
            <p className="text-xs text-gray-500 mt-1">Unique passengers</p>
          </div>

          {/* New Customers */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <UserPlus className="h-5 w-5 text-green-600" />
              <span className="text-sm font-medium text-gray-500">New Customers</span>
            </div>
            <p className="text-3xl font-bold text-green-600">{stats?.customers?.new || 0}</p>
            <p className="text-xs text-gray-500 mt-1">First-time bookings</p>
          </div>

          {/* Repeat Customers */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <UserCheck className="h-5 w-5 text-blue-600" />
              <span className="text-sm font-medium text-gray-500">Repeat Customers</span>
            </div>
            <p className="text-3xl font-bold text-blue-600">{stats?.customers?.repeated || 0}</p>
            <p className="text-xs text-gray-500 mt-1">Returning passengers</p>
          </div>

          {/* Repeat Rate */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <RefreshCw className="h-5 w-5 text-purple-600" />
              <span className="text-sm font-medium text-gray-500">Repeat Rate</span>
            </div>
            <p className="text-3xl font-bold text-purple-600">{repeatRate}%</p>
            <p className="text-xs text-gray-500 mt-1">Customer retention</p>
          </div>

          {/* Total Passengers */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <Activity className="h-5 w-5 text-indigo-600" />
              <span className="text-sm font-medium text-gray-500">Total Passengers</span>
            </div>
            <p className="text-3xl font-bold text-indigo-600">{stats?.passengers?.today || 0}</p>
            <p className="text-xs text-gray-500 mt-1">All bookings</p>
          </div>

          {/* Average Booking Value */}
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 mb-2">
              <Target className="h-5 w-5 text-orange-600" />
              <span className="text-sm font-medium text-gray-500">Avg Booking</span>
            </div>
            <p className="text-3xl font-bold text-orange-600">
              {formatCurrency(
                (stats?.bookings?.today || 0) > 0
                  ? Math.round((stats?.revenue?.today || 0) / (stats?.bookings?.today || 1))
                  : 0
              )}
            </p>
            <p className="text-xs text-gray-500 mt-1">Per booking</p>
          </div>
        </div>

        {/* Row 2: Charts */}
        <div className="mb-6 grid gap-6 lg:grid-cols-3">
          {/* Customer Type Distribution */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4">
              <h3 className="font-semibold text-gray-900">New vs Repeat</h3>
              <p className="text-sm text-gray-500">Customer acquisition mix</p>
            </div>
            <div className="flex items-center justify-center">
              <div className="h-48 w-48">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={customerTypeData}
                      cx="50%"
                      cy="50%"
                      innerRadius={50}
                      outerRadius={80}
                      paddingAngle={4}
                      dataKey="value"
                    >
                      {customerTypeData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </div>
            <div className="mt-4 flex justify-center gap-6">
              {customerTypeData.map((item) => (
                <div key={item.name} className="flex items-center gap-2">
                  <div
                    className="h-3 w-3 rounded-full"
                    style={{ backgroundColor: item.color }}
                  />
                  <span className="text-sm text-gray-600">{item.name}</span>
                  <span className="text-sm font-medium">{item.value}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Gender Distribution */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4">
              <h3 className="font-semibold text-gray-900">Gender Distribution</h3>
              <p className="text-sm text-gray-500">Passenger demographics</p>
            </div>
            <div className="flex items-center justify-center">
              <div className="h-48 w-48">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={genderData.filter(g => g.value > 0)}
                      cx="50%"
                      cy="50%"
                      innerRadius={50}
                      outerRadius={80}
                      paddingAngle={4}
                      dataKey="value"
                    >
                      {genderData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </div>
            <div className="mt-4 flex justify-center gap-4">
              {genderData.filter(g => g.value > 0).map((item) => (
                <div key={item.name} className="flex items-center gap-2">
                  <div
                    className="h-3 w-3 rounded-full"
                    style={{ backgroundColor: item.color }}
                  />
                  <span className="text-sm text-gray-600">{item.name}</span>
                  <span className="text-sm font-medium">{item.value}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Booking Frequency */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4">
              <h3 className="font-semibold text-gray-900">Customer Insights</h3>
              <p className="text-sm text-gray-500">Key metrics</p>
            </div>
            <div className="space-y-4">
              <div className="flex items-center justify-between p-3 rounded-lg bg-green-50">
                <div className="flex items-center gap-3">
                  <UserPlus className="h-5 w-5 text-green-600" />
                  <span className="text-sm font-medium text-gray-700">Acquisition Rate</span>
                </div>
                <span className="text-lg font-bold text-green-600">
                  {totalCustomers > 0 ? Math.round((stats?.customers?.new || 0) / totalCustomers * 100) : 0}%
                </span>
              </div>
              <div className="flex items-center justify-between p-3 rounded-lg bg-blue-50">
                <div className="flex items-center gap-3">
                  <RefreshCw className="h-5 w-5 text-blue-600" />
                  <span className="text-sm font-medium text-gray-700">Retention Rate</span>
                </div>
                <span className="text-lg font-bold text-blue-600">{repeatRate}%</span>
              </div>
              <div className="flex items-center justify-between p-3 rounded-lg bg-purple-50">
                <div className="flex items-center gap-3">
                  <Award className="h-5 w-5 text-purple-600" />
                  <span className="text-sm font-medium text-gray-700">Loyalty Index</span>
                </div>
                <span className="text-lg font-bold text-purple-600">
                  {repeatRate >= 50 ? 'High' : repeatRate >= 25 ? 'Medium' : 'Low'}
                </span>
              </div>
              <div className="flex items-center justify-between p-3 rounded-lg bg-pink-50">
                <div className="flex items-center gap-3">
                  <Heart className="h-5 w-5 text-pink-600" />
                  <span className="text-sm font-medium text-gray-700">Customer Health</span>
                </div>
                <span className={`text-lg font-bold ${
                  repeatRate >= 40 ? 'text-green-600' : repeatRate >= 20 ? 'text-yellow-600' : 'text-red-600'
                }`}>
                  {repeatRate >= 40 ? 'Healthy' : repeatRate >= 20 ? 'Moderate' : 'At Risk'}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Row 3: Booking Trend */}
        <div className="rounded-xl border bg-white p-5">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <h3 className="font-semibold text-gray-900">Booking Trend</h3>
              <p className="text-sm text-gray-500">
                {dateRange === 'today' || dateRange === 'yesterday' ? 'Hourly' : 'Daily'} customer activity
              </p>
            </div>
            <Link href="/bookings" className="text-sm text-blue-600 hover:text-blue-700">
              View all bookings →
            </Link>
          </div>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={stats?.bookingTrend || []}>
                <defs>
                  <linearGradient id="colorCustomers" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#ec4899" stopOpacity={0.1} />
                    <stop offset="95%" stopColor="#ec4899" stopOpacity={0} />
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
                  stroke="#ec4899"
                  strokeWidth={2}
                  fillOpacity={1}
                  fill="url(#colorCustomers)"
                  name="Bookings"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Row 4: Route Preferences */}
        <div className="mt-6 grid gap-6 lg:grid-cols-2">
          {/* Top Routes by Customer Volume */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h3 className="font-semibold text-gray-900">Popular Routes</h3>
                <p className="text-sm text-gray-500">By customer preference</p>
              </div>
              <Link href="/trips" className="text-sm text-blue-600 hover:text-blue-700">
                View routes →
              </Link>
            </div>
            <div className="space-y-3">
              {stats?.serviceStats?.slice(0, 5).map((service: any, idx: number) => (
                <div key={idx} className="flex items-center gap-3">
                  <div className={`flex h-8 w-8 items-center justify-center rounded-lg text-sm font-bold ${
                    idx === 0 ? 'bg-yellow-100 text-yellow-600' :
                    idx === 1 ? 'bg-gray-200 text-gray-600' :
                    idx === 2 ? 'bg-orange-100 text-orange-600' :
                    'bg-gray-100 text-gray-500'
                  }`}>
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
                    <p className="text-xs text-gray-500">bookings</p>
                  </div>
                </div>
              )) || (
                <p className="text-center text-gray-500 py-4">No data available</p>
              )}
            </div>
          </div>

          {/* Customer Segments Summary */}
          <div className="rounded-xl border bg-white p-5">
            <div className="mb-4">
              <h3 className="font-semibold text-gray-900">Customer Segments</h3>
              <p className="text-sm text-gray-500">Breakdown by type</p>
            </div>
            <div className="space-y-4">
              <div className="p-4 rounded-lg bg-gradient-to-r from-green-50 to-green-100 border border-green-200">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <UserPlus className="h-5 w-5 text-green-600" />
                    <span className="font-medium text-green-800">New Customers</span>
                  </div>
                  <span className="text-2xl font-bold text-green-600">{stats?.customers?.new || 0}</span>
                </div>
                <p className="text-sm text-green-700">First-time travelers - Great for acquisition!</p>
                <div className="mt-2 h-2 bg-green-200 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-green-500 rounded-full"
                    style={{ width: `${totalCustomers > 0 ? (stats?.customers?.new || 0) / totalCustomers * 100 : 0}%` }}
                  />
                </div>
              </div>

              <div className="p-4 rounded-lg bg-gradient-to-r from-blue-50 to-blue-100 border border-blue-200">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <UserCheck className="h-5 w-5 text-blue-600" />
                    <span className="font-medium text-blue-800">Repeat Customers</span>
                  </div>
                  <span className="text-2xl font-bold text-blue-600">{stats?.customers?.repeated || 0}</span>
                </div>
                <p className="text-sm text-blue-700">Loyal passengers - Focus on retention!</p>
                <div className="mt-2 h-2 bg-blue-200 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-blue-500 rounded-full"
                    style={{ width: `${repeatRate}%` }}
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}
