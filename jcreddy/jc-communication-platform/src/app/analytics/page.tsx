'use client'

import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { formatCurrency } from '@/lib/utils'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  AreaChart,
  Area,
  Legend,
} from 'recharts'
import {
  TrendingUp,
  TrendingDown,
  Ticket,
  Users,
  DollarSign,
  Bell,
  CheckCircle,
  XCircle,
  Calendar,
  Filter,
  BarChart3,
  Route,
  MapPin,
  ArrowRight,
} from 'lucide-react'

// Fetch routes for filter
async function fetchRoutes() {
  const res = await fetch('/api/routes')
  if (!res.ok) return { routes: [] }
  return res.json()
}

const dateRangeOptions = [
  { value: 'today', label: 'Today' },
  { value: 'yesterday', label: 'Yesterday' },
  { value: '7days', label: 'Last 7 Days' },
  { value: '15days', label: 'Last 15 Days' },
  { value: 'month', label: 'This Month' },
]

const trendOptions = [
  { value: 'bookings', label: 'Bookings' },
  { value: 'revenue', label: 'Revenue' },
  { value: 'occupancy', label: 'Occupancy' },
]

// Mock data for analytics
const bookingsByDay = [
  { day: 'Mon', bookings: 45, passengers: 92, revenue: 78000 },
  { day: 'Tue', bookings: 52, passengers: 108, revenue: 91000 },
  { day: 'Wed', bookings: 48, passengers: 96, revenue: 82000 },
  { day: 'Thu', bookings: 61, passengers: 125, revenue: 105000 },
  { day: 'Fri', bookings: 78, passengers: 162, revenue: 138000 },
  { day: 'Sat', bookings: 89, passengers: 185, revenue: 156000 },
  { day: 'Sun', bookings: 72, passengers: 148, revenue: 125000 },
]

const notificationStats = [
  { name: 'Booking Confirm', sent: 245, delivered: 238, failed: 7 },
  { name: '24h Reminder', sent: 198, delivered: 192, failed: 6 },
  { name: '2h Reminder', sent: 187, delivered: 181, failed: 6 },
  { name: 'Cancellation', sent: 23, delivered: 22, failed: 1 },
  { name: 'Delay Alert', sent: 8, delivered: 8, failed: 0 },
]

const routePerformance = [
  { route: 'Yanam - Hyderabad', serviceNo: 'MYTHRI-28', bookings: 156, availableSeats: 180, revenue: 234000, expenses: 45000, netRev: 189000, asp: 1500, occupancy: 85, channel: 'Redbus' },
  { route: 'Vijayawada - Bangalore', serviceNo: 'MYTHRI-15', bookings: 142, availableSeats: 180, revenue: 284000, expenses: 52000, netRev: 232000, asp: 2000, occupancy: 78, channel: 'Direct' },
  { route: 'Hyderabad - Chennai', serviceNo: 'MYTHRI-22', bookings: 128, availableSeats: 180, revenue: 256000, expenses: 48000, netRev: 208000, asp: 2000, occupancy: 72, channel: 'Abhibus' },
  { route: 'Guntur - Mumbai', serviceNo: 'MYTHRI-08', bookings: 98, availableSeats: 144, revenue: 294000, expenses: 65000, netRev: 229000, asp: 3000, occupancy: 68, channel: 'Redbus' },
  { route: 'Vizag - Hyderabad', serviceNo: 'MYTHRI-12', bookings: 87, availableSeats: 108, revenue: 130500, expenses: 28000, netRev: 102500, asp: 1500, occupancy: 82, channel: 'Direct' },
]

const channelBreakdown = [
  { name: 'WhatsApp', value: 680, color: '#25D366' },
  { name: 'SMS', value: 120, color: '#3B82F6' },
  { name: 'Email', value: 45, color: '#8B5CF6' },
]

const hourlyBookings = [
  { hour: '6am', bookings: 8 },
  { hour: '8am', bookings: 15 },
  { hour: '10am', bookings: 22 },
  { hour: '12pm', bookings: 28 },
  { hour: '2pm', bookings: 32 },
  { hour: '4pm', bookings: 45 },
  { hour: '6pm', bookings: 52 },
  { hour: '8pm', bookings: 38 },
  { hour: '10pm', bookings: 25 },
]

// Route Booking Trends (7-day view)
const routeBookingTrends = [
  { day: 'Mon', 'HYD-CHN': 45, 'HYD-BLR': 38, 'VJA-HYD': 52 },
  { day: 'Tue', 'HYD-CHN': 48, 'HYD-BLR': 42, 'VJA-HYD': 48 },
  { day: 'Wed', 'HYD-CHN': 52, 'HYD-BLR': 45, 'VJA-HYD': 55 },
  { day: 'Thu', 'HYD-CHN': 58, 'HYD-BLR': 50, 'VJA-HYD': 60 },
  { day: 'Fri', 'HYD-CHN': 72, 'HYD-BLR': 65, 'VJA-HYD': 75 },
  { day: 'Sat', 'HYD-CHN': 85, 'HYD-BLR': 78, 'VJA-HYD': 88 },
  { day: 'Sun', 'HYD-CHN': 68, 'HYD-BLR': 62, 'VJA-HYD': 70 },
]

// Advance Occupancy Trends (next 7 days)
const advanceOccupancy = [
  { date: 'Today', occupancy: 78, bookings: 156 },
  { date: '+1 Day', occupancy: 65, bookings: 130 },
  { date: '+2 Days', occupancy: 52, bookings: 104 },
  { date: '+3 Days', occupancy: 38, bookings: 76 },
  { date: '+4 Days', occupancy: 28, bookings: 56 },
  { date: '+5 Days', occupancy: 18, bookings: 36 },
  { date: '+6 Days', occupancy: 12, bookings: 24 },
]

// Competitor Comparison (mock data)
const competitorComparison = [
  { route: 'HYD-CHN', ours: 1200, competitor1: 1100, competitor2: 1300 },
  { route: 'HYD-BLR', ours: 1500, competitor1: 1450, competitor2: 1550 },
  { route: 'VJA-HYD', ours: 800, competitor1: 850, competitor2: 750 },
  { route: 'GNT-MUM', ours: 2200, competitor1: 2100, competitor2: 2400 },
]

// Quota Seats Analysis
const quotaSeats = [
  { type: 'Own', value: 65, color: '#3b82f6' },
  { type: 'Redbus', value: 20, color: '#ef4444' },
  { type: 'Abhibus', value: 10, color: '#22c55e' },
  { type: 'MakeMyTrip', value: 5, color: '#f59e0b' },
]

// Booking Source Analysis
const bookingSources = [
  { name: 'Direct', bookings: 320, revenue: 480000, color: '#3b82f6' },
  { name: 'Redbus', bookings: 280, revenue: 420000, color: '#ef4444' },
  { name: 'Abhibus', bookings: 150, revenue: 225000, color: '#22c55e' },
  { name: 'MakeMyTrip', bookings: 95, revenue: 142500, color: '#f59e0b' },
]

// Via Route Wise Performance Data
const viaRoutePerformance = [
  {
    mainRoute: 'Hyderabad - Chennai',
    viaRoutes: [
      { via: 'Direct (NH65)', bookings: 85, revenue: 127500, occupancy: 82, avgFare: 1500, trips: 12 },
      { via: 'Via Vijayawada', bookings: 62, revenue: 86800, occupancy: 75, avgFare: 1400, trips: 8 },
      { via: 'Via Guntur', bookings: 45, revenue: 58500, occupancy: 68, avgFare: 1300, trips: 6 },
    ]
  },
  {
    mainRoute: 'Hyderabad - Bangalore',
    viaRoutes: [
      { via: 'Direct (NH44)', bookings: 92, revenue: 184000, occupancy: 88, avgFare: 2000, trips: 10 },
      { via: 'Via Kurnool', bookings: 68, revenue: 122400, occupancy: 78, avgFare: 1800, trips: 8 },
      { via: 'Via Anantapur', bookings: 52, revenue: 88400, occupancy: 72, avgFare: 1700, trips: 6 },
    ]
  },
  {
    mainRoute: 'Vijayawada - Hyderabad',
    viaRoutes: [
      { via: 'Express (NH65)', bookings: 110, revenue: 99000, occupancy: 90, avgFare: 900, trips: 15 },
      { via: 'Via Khammam', bookings: 75, revenue: 60000, occupancy: 78, avgFare: 800, trips: 10 },
      { via: 'Via Suryapet', bookings: 48, revenue: 36000, occupancy: 65, avgFare: 750, trips: 8 },
    ]
  },
  {
    mainRoute: 'Guntur - Mumbai',
    viaRoutes: [
      { via: 'Via Hyderabad', bookings: 65, revenue: 195000, occupancy: 75, avgFare: 3000, trips: 8 },
      { via: 'Via Pune', bookings: 45, revenue: 126000, occupancy: 68, avgFare: 2800, trips: 6 },
      { via: 'Via Nagpur', bookings: 32, revenue: 83200, occupancy: 58, avgFare: 2600, trips: 4 },
    ]
  },
]

// Via Route Trends (for chart)
const viaRouteTrends = [
  { day: 'Mon', 'Direct': 45, 'Via City': 28, 'Via Highway': 32 },
  { day: 'Tue', 'Direct': 52, 'Via City': 30, 'Via Highway': 35 },
  { day: 'Wed', 'Direct': 48, 'Via City': 32, 'Via Highway': 38 },
  { day: 'Thu', 'Direct': 58, 'Via City': 35, 'Via Highway': 42 },
  { day: 'Fri', 'Direct': 72, 'Via City': 45, 'Via Highway': 52 },
  { day: 'Sat', 'Direct': 85, 'Via City': 55, 'Via Highway': 62 },
  { day: 'Sun', 'Direct': 68, 'Via City': 42, 'Via Highway': 48 },
]

export default function AnalyticsPage() {
  const [dateRange, setDateRange] = useState('7days')
  const [selectedRoute, setSelectedRoute] = useState('')
  const [trendType, setTrendType] = useState('bookings')

  const { data: routesData } = useQuery({
    queryKey: ['routes'],
    queryFn: fetchRoutes,
  })

  return (
    <DashboardLayout>
      <Header title="Analytics" subtitle="Comprehensive insights and reports" />

      <div className="p-4 sm:p-6">
        {/* Filters Section */}
        <div className="mb-6 flex flex-wrap items-center gap-3 rounded-xl border bg-white p-4">
          <Filter className="h-5 w-5 text-gray-400" />
          <span className="text-sm font-medium text-gray-600">Filters:</span>

          {/* Date Range Filter */}
          <select
            value={dateRange}
            onChange={(e) => setDateRange(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            {dateRangeOptions.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>

          {/* Route/Service Filter */}
          <select
            value={selectedRoute}
            onChange={(e) => setSelectedRoute(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            <option value="">All Routes/Services</option>
            {routesData?.routes?.map((route: any) => (
              <option key={route.id} value={route.id}>
                {route.service_number || route.route_name || `${route.origin} - ${route.destination}`}
              </option>
            ))}
          </select>

          {/* Trend Type Filter */}
          <select
            value={trendType}
            onChange={(e) => setTrendType(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            {trendOptions.map((option) => (
              <option key={option.value} value={option.value}>
                Trend: {option.label}
              </option>
            ))}
          </select>
        </div>

        {/* KPI Cards */}
        <div className="mb-6 grid gap-4 grid-cols-2 lg:grid-cols-4 sm:mb-8">
          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="flex items-center justify-between">
              <div className="min-w-0 flex-1">
                <p className="text-xs sm:text-sm text-gray-500 truncate">Total Bookings</p>
                <p className="text-xl sm:text-3xl font-bold text-gray-900">1,245</p>
                <div className="mt-1 sm:mt-2 flex items-center gap-1 text-green-600">
                  <TrendingUp className="h-3 w-3 sm:h-4 sm:w-4" />
                  <span className="text-xs sm:text-sm font-medium">+12.5%</span>
                  <span className="text-[10px] sm:text-xs text-gray-500 hidden sm:inline">vs last week</span>
                </div>
              </div>
              <div className="flex h-10 w-10 sm:h-12 sm:w-12 items-center justify-center rounded-lg sm:rounded-xl bg-blue-100 flex-shrink-0">
                <Ticket className="h-5 w-5 sm:h-6 sm:w-6 text-blue-600" />
              </div>
            </div>
          </div>

          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="flex items-center justify-between">
              <div className="min-w-0 flex-1">
                <p className="text-xs sm:text-sm text-gray-500 truncate">Total Passengers</p>
                <p className="text-xl sm:text-3xl font-bold text-gray-900">2,580</p>
                <div className="mt-1 sm:mt-2 flex items-center gap-1 text-green-600">
                  <TrendingUp className="h-3 w-3 sm:h-4 sm:w-4" />
                  <span className="text-xs sm:text-sm font-medium">+8.3%</span>
                  <span className="text-[10px] sm:text-xs text-gray-500 hidden sm:inline">vs last week</span>
                </div>
              </div>
              <div className="flex h-10 w-10 sm:h-12 sm:w-12 items-center justify-center rounded-lg sm:rounded-xl bg-purple-100 flex-shrink-0">
                <Users className="h-5 w-5 sm:h-6 sm:w-6 text-purple-600" />
              </div>
            </div>
          </div>

          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="flex items-center justify-between">
              <div className="min-w-0 flex-1">
                <p className="text-xs sm:text-sm text-gray-500 truncate">Revenue</p>
                <p className="text-xl sm:text-3xl font-bold text-gray-900">{formatCurrency(775000)}</p>
                <div className="mt-1 sm:mt-2 flex items-center gap-1 text-green-600">
                  <TrendingUp className="h-3 w-3 sm:h-4 sm:w-4" />
                  <span className="text-xs sm:text-sm font-medium">+15.2%</span>
                  <span className="text-[10px] sm:text-xs text-gray-500 hidden sm:inline">vs last week</span>
                </div>
              </div>
              <div className="flex h-10 w-10 sm:h-12 sm:w-12 items-center justify-center rounded-lg sm:rounded-xl bg-green-100 flex-shrink-0">
                <DollarSign className="h-5 w-5 sm:h-6 sm:w-6 text-green-600" />
              </div>
            </div>
          </div>

          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="flex items-center justify-between">
              <div className="min-w-0 flex-1">
                <p className="text-xs sm:text-sm text-gray-500 truncate">Delivery Rate</p>
                <p className="text-xl sm:text-3xl font-bold text-gray-900">97.2%</p>
                <div className="mt-1 sm:mt-2 flex items-center gap-1 text-green-600">
                  <TrendingUp className="h-3 w-3 sm:h-4 sm:w-4" />
                  <span className="text-xs sm:text-sm font-medium">+1.5%</span>
                  <span className="text-[10px] sm:text-xs text-gray-500 hidden sm:inline">vs last week</span>
                </div>
              </div>
              <div className="flex h-10 w-10 sm:h-12 sm:w-12 items-center justify-center rounded-lg sm:rounded-xl bg-indigo-100 flex-shrink-0">
                <Bell className="h-5 w-5 sm:h-6 sm:w-6 text-indigo-600" />
              </div>
            </div>
          </div>
        </div>

        {/* Charts Row 1 */}
        <div className="mb-6 grid gap-4 sm:gap-6 lg:grid-cols-2 sm:mb-8">
          {/* Weekly Bookings Chart */}
          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h3 className="text-base sm:text-lg font-semibold text-gray-900">Weekly Overview</h3>
              <p className="text-xs sm:text-sm text-gray-500">Bookings, passengers, and revenue</p>
            </div>
            <div className="h-56 sm:h-72">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={bookingsByDay}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="day" axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '12px',
                      fontSize: '12px',
                    }}
                  />
                  <Bar dataKey="bookings" fill="#3b82f6" radius={[4, 4, 0, 0]} name="Bookings" />
                  <Bar dataKey="passengers" fill="#8b5cf6" radius={[4, 4, 0, 0]} name="Passengers" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Hourly Distribution */}
          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h3 className="text-base sm:text-lg font-semibold text-gray-900">Booking Distribution</h3>
              <p className="text-xs sm:text-sm text-gray-500">Hourly booking pattern</p>
            </div>
            <div className="h-56 sm:h-72">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={hourlyBookings}>
                  <defs>
                    <linearGradient id="colorBookings" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.2} />
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="hour" axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '12px',
                      fontSize: '12px',
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
        </div>

        {/* Charts Row 2 */}
        <div className="mb-6 grid gap-4 sm:gap-6 lg:grid-cols-3 sm:mb-8">
          {/* Notification Performance */}
          <div className="lg:col-span-2 rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h3 className="text-base sm:text-lg font-semibold text-gray-900">Notification Performance</h3>
              <p className="text-xs sm:text-sm text-gray-500">Delivery status by notification type</p>
            </div>
            <div className="h-56 sm:h-72">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={notificationStats} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                  <XAxis type="number" axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <YAxis type="category" dataKey="name" axisLine={false} tickLine={false} width={80} tick={{ fontSize: 11 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '12px',
                      fontSize: '12px',
                    }}
                  />
                  <Bar dataKey="delivered" fill="#22c55e" radius={[0, 4, 4, 0]} name="Delivered" />
                  <Bar dataKey="failed" fill="#ef4444" radius={[0, 4, 4, 0]} name="Failed" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Channel Breakdown */}
          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h3 className="text-base sm:text-lg font-semibold text-gray-900">Channel Usage</h3>
              <p className="text-xs sm:text-sm text-gray-500">Messages by channel</p>
            </div>
            <div className="h-40 sm:h-48">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={channelBreakdown}
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={60}
                    paddingAngle={4}
                    dataKey="value"
                  >
                    {channelBreakdown.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-3 sm:mt-4 space-y-2">
              {channelBreakdown.map((item) => (
                <div key={item.name} className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div
                      className="h-3 w-3 rounded-full"
                      style={{ backgroundColor: item.color }}
                    />
                    <span className="text-xs sm:text-sm text-gray-600">{item.name}</span>
                  </div>
                  <span className="text-xs sm:text-sm font-medium">{item.value}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Charts Row 3 - Route Trends & Advance Occupancy */}
        <div className="mb-6 grid gap-4 sm:gap-6 lg:grid-cols-2 sm:mb-8">
          {/* Route Booking Trends */}
          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h3 className="text-base sm:text-lg font-semibold text-gray-900">Route Booking Trends</h3>
              <p className="text-xs sm:text-sm text-gray-500">Weekly booking comparison by route</p>
            </div>
            <div className="h-56 sm:h-72">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={routeBookingTrends}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="day" axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '12px',
                      fontSize: '12px',
                    }}
                  />
                  <Legend />
                  <Line type="monotone" dataKey="HYD-CHN" stroke="#3b82f6" strokeWidth={2} dot={{ r: 4 }} />
                  <Line type="monotone" dataKey="HYD-BLR" stroke="#8b5cf6" strokeWidth={2} dot={{ r: 4 }} />
                  <Line type="monotone" dataKey="VJA-HYD" stroke="#22c55e" strokeWidth={2} dot={{ r: 4 }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Advance Occupancy Trends */}
          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h3 className="text-base sm:text-lg font-semibold text-gray-900">Advance Occupancy</h3>
              <p className="text-xs sm:text-sm text-gray-500">Booking occupancy for upcoming departures</p>
            </div>
            <div className="h-56 sm:h-72">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={advanceOccupancy}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="date" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '12px',
                      fontSize: '12px',
                    }}
                    formatter={(value: any, name: string) => [
                      name === 'occupancy' ? `${value}%` : value,
                      name === 'occupancy' ? 'Occupancy' : 'Bookings'
                    ]}
                  />
                  <Bar dataKey="occupancy" fill="#3b82f6" radius={[4, 4, 0, 0]} name="Occupancy %" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* Charts Row 4 - Competitor & Quota Analysis */}
        <div className="mb-6 grid gap-4 sm:gap-6 lg:grid-cols-3 sm:mb-8">
          {/* Competitor Price Comparison */}
          <div className="lg:col-span-2 rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h3 className="text-base sm:text-lg font-semibold text-gray-900">Fare Comparison</h3>
              <p className="text-xs sm:text-sm text-gray-500">Our fares vs competitors by route</p>
            </div>
            <div className="h-56 sm:h-72">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={competitorComparison}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="route" axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '12px',
                      fontSize: '12px',
                    }}
                    formatter={(value: any) => [formatCurrency(value), '']}
                  />
                  <Legend />
                  <Bar dataKey="ours" fill="#3b82f6" radius={[4, 4, 0, 0]} name="Our Fare" />
                  <Bar dataKey="competitor1" fill="#ef4444" radius={[4, 4, 0, 0]} name="Competitor A" />
                  <Bar dataKey="competitor2" fill="#f59e0b" radius={[4, 4, 0, 0]} name="Competitor B" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Quota Seats Distribution */}
          <div className="rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h3 className="text-base sm:text-lg font-semibold text-gray-900">Quota Distribution</h3>
              <p className="text-xs sm:text-sm text-gray-500">Seat allocation by channel</p>
            </div>
            <div className="h-40 sm:h-48">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={quotaSeats}
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={60}
                    paddingAngle={4}
                    dataKey="value"
                    nameKey="type"
                  >
                    {quotaSeats.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: any) => [`${value}%`, 'Quota']} />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-3 sm:mt-4 space-y-2">
              {quotaSeats.map((item) => (
                <div key={item.type} className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div
                      className="h-3 w-3 rounded-full"
                      style={{ backgroundColor: item.color }}
                    />
                    <span className="text-xs sm:text-sm text-gray-600">{item.type}</span>
                  </div>
                  <span className="text-xs sm:text-sm font-medium">{item.value}%</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Booking Sources Table */}
        <div className="mb-6 rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6 sm:mb-8">
          <div className="mb-4">
            <h3 className="text-base sm:text-lg font-semibold text-gray-900">Booking Sources Analysis</h3>
            <p className="text-xs sm:text-sm text-gray-500">Revenue breakdown by booking channel</p>
          </div>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            {bookingSources.map((source) => (
              <div key={source.name} className="rounded-xl border p-4">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <div
                      className="h-3 w-3 rounded-full"
                      style={{ backgroundColor: source.color }}
                    />
                    <span className="font-medium text-gray-900">{source.name}</span>
                  </div>
                </div>
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-gray-500">Bookings</span>
                    <span className="font-semibold text-gray-900">{source.bookings}</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-gray-500">Revenue</span>
                    <span className="font-semibold text-green-600">{formatCurrency(source.revenue)}</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-gray-500">Avg Ticket</span>
                    <span className="text-sm text-gray-700">{formatCurrency(Math.round(source.revenue / source.bookings))}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Via Route Wise Performance Section */}
        <div className="mb-6 sm:mb-8">
          <div className="mb-4 flex items-center gap-2">
            <Route className="h-5 w-5 text-blue-600" />
            <h3 className="text-base sm:text-lg font-semibold text-gray-900">Via Route Wise Performance</h3>
          </div>

          {/* Via Route Trends Chart */}
          <div className="mb-6 rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6">
            <div className="mb-4 sm:mb-6">
              <h4 className="text-sm sm:text-base font-semibold text-gray-900">Via Route Booking Trends</h4>
              <p className="text-xs sm:text-sm text-gray-500">Weekly comparison of route variants</p>
            </div>
            <div className="h-56 sm:h-72">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={viaRouteTrends}>
                  <defs>
                    <linearGradient id="colorDirect" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="colorViaCity" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="colorViaHighway" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="day" axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '12px',
                      fontSize: '12px',
                    }}
                  />
                  <Legend />
                  <Area type="monotone" dataKey="Direct" stroke="#3b82f6" strokeWidth={2} fillOpacity={1} fill="url(#colorDirect)" />
                  <Area type="monotone" dataKey="Via City" stroke="#8b5cf6" strokeWidth={2} fillOpacity={1} fill="url(#colorViaCity)" />
                  <Area type="monotone" dataKey="Via Highway" stroke="#22c55e" strokeWidth={2} fillOpacity={1} fill="url(#colorViaHighway)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Via Route Performance Cards */}
          <div className="grid gap-4 sm:gap-6 lg:grid-cols-2">
            {viaRoutePerformance.map((route) => (
              <div key={route.mainRoute} className="rounded-xl sm:rounded-2xl border bg-white overflow-hidden">
                <div className="border-b bg-gradient-to-r from-blue-50 to-indigo-50 p-4">
                  <div className="flex items-center gap-2">
                    <MapPin className="h-4 w-4 text-blue-600" />
                    <h4 className="font-semibold text-gray-900">{route.mainRoute}</h4>
                  </div>
                  <p className="text-xs text-gray-500 mt-1">{route.viaRoutes.length} route variants</p>
                </div>
                <div className="divide-y">
                  {route.viaRoutes.map((via, idx) => (
                    <div key={via.via} className="p-4 hover:bg-gray-50 transition-colors">
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-2">
                          <span className={`flex h-6 w-6 items-center justify-center rounded-full text-xs font-semibold ${
                            idx === 0 ? 'bg-green-100 text-green-700' :
                            idx === 1 ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-700'
                          }`}>
                            {idx + 1}
                          </span>
                          <span className="font-medium text-sm text-gray-900">{via.via}</span>
                        </div>
                        <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${
                          via.occupancy >= 80 ? 'bg-green-100 text-green-700' :
                          via.occupancy >= 70 ? 'bg-blue-100 text-blue-700' :
                          via.occupancy >= 60 ? 'bg-yellow-100 text-yellow-700' :
                          'bg-red-100 text-red-700'
                        }`}>
                          {via.occupancy}% occ
                        </span>
                      </div>
                      <div className="grid grid-cols-4 gap-2 text-center">
                        <div className="rounded-lg bg-gray-50 p-2">
                          <p className="text-[10px] text-gray-500">Bookings</p>
                          <p className="text-sm font-semibold text-gray-900">{via.bookings}</p>
                        </div>
                        <div className="rounded-lg bg-green-50 p-2">
                          <p className="text-[10px] text-gray-500">Revenue</p>
                          <p className="text-sm font-semibold text-green-700">{formatCurrency(via.revenue)}</p>
                        </div>
                        <div className="rounded-lg bg-blue-50 p-2">
                          <p className="text-[10px] text-gray-500">Avg Fare</p>
                          <p className="text-sm font-semibold text-blue-700">{formatCurrency(via.avgFare)}</p>
                        </div>
                        <div className="rounded-lg bg-purple-50 p-2">
                          <p className="text-[10px] text-gray-500">Trips</p>
                          <p className="text-sm font-semibold text-purple-700">{via.trips}</p>
                        </div>
                      </div>
                      {/* Occupancy Bar */}
                      <div className="mt-3">
                        <div className="h-1.5 overflow-hidden rounded-full bg-gray-100">
                          <div
                            className={`h-full transition-all ${
                              via.occupancy >= 80 ? 'bg-green-500' :
                              via.occupancy >= 70 ? 'bg-blue-500' :
                              via.occupancy >= 60 ? 'bg-yellow-500' :
                              'bg-red-500'
                            }`}
                            style={{ width: `${via.occupancy}%` }}
                          />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Route Performance Table */}
        <div className="rounded-xl sm:rounded-2xl border bg-white overflow-hidden">
          <div className="border-b p-4 sm:p-6">
            <h3 className="text-base sm:text-lg font-semibold text-gray-900">Route Performance</h3>
            <p className="text-xs sm:text-sm text-gray-500">Top performing routes this week</p>
          </div>

          {/* Mobile Card View */}
          <div className="block sm:hidden divide-y">
            {routePerformance.map((route, index) => (
              <div key={route.route} className="p-4 space-y-3">
                <div className="flex items-center gap-3">
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-blue-100 text-xs font-semibold text-blue-600">
                    {index + 1}
                  </span>
                  <span className="font-medium text-gray-900 text-sm">{route.route}</span>
                </div>
                <div className="grid grid-cols-3 gap-2 text-center">
                  <div className="rounded-lg bg-gray-50 p-2">
                    <p className="text-xs text-gray-500">Bookings</p>
                    <p className="font-semibold text-gray-900">{route.bookings}</p>
                  </div>
                  <div className="rounded-lg bg-gray-50 p-2">
                    <p className="text-xs text-gray-500">Revenue</p>
                    <p className="font-semibold text-green-600 text-sm">{formatCurrency(route.revenue)}</p>
                  </div>
                  <div className="rounded-lg bg-gray-50 p-2">
                    <p className="text-xs text-gray-500">Occupancy</p>
                    <p className="font-semibold text-gray-900">{route.occupancy}%</p>
                  </div>
                </div>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2 flex-1">
                    <div className="h-2 flex-1 overflow-hidden rounded-full bg-gray-100">
                      <div
                        className="h-full bg-blue-600"
                        style={{ width: `${route.occupancy}%` }}
                      />
                    </div>
                  </div>
                  <div className="ml-3">
                    {route.occupancy >= 80 ? (
                      <span className="inline-flex items-center gap-1 rounded-full bg-green-100 px-2 py-1 text-xs font-medium text-green-700">
                        <TrendingUp className="h-3 w-3" />
                        Excellent
                      </span>
                    ) : route.occupancy >= 70 ? (
                      <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-2 py-1 text-xs font-medium text-blue-700">
                        <TrendingUp className="h-3 w-3" />
                        Good
                      </span>
                    ) : (
                      <span className="inline-flex items-center gap-1 rounded-full bg-yellow-100 px-2 py-1 text-xs font-medium text-yellow-700">
                        <TrendingDown className="h-3 w-3" />
                        Average
                      </span>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* Desktop Table View */}
          <div className="hidden sm:block overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b bg-gray-50">
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Route
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Service No
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Bookings
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Avail. Seats
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Revenue
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Expenses
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Net Rev
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    ASP
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Occupancy
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Channel
                  </th>
                  <th className="px-3 py-3 text-left text-xs font-semibold uppercase text-gray-500">
                    Performance
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {routePerformance.map((route, index) => (
                  <tr key={route.route} className="hover:bg-gray-50">
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-2">
                        <span className="flex h-6 w-6 items-center justify-center rounded-full bg-blue-100 text-xs font-semibold text-blue-600">
                          {index + 1}
                        </span>
                        <span className="font-medium text-gray-900 text-sm">{route.route}</span>
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <span className="rounded bg-indigo-50 px-2 py-1 text-xs font-medium text-indigo-700">
                        {route.serviceNo}
                      </span>
                    </td>
                    <td className="px-3 py-3">
                      <span className="font-medium text-sm">{route.bookings}</span>
                    </td>
                    <td className="px-3 py-3">
                      <span className="text-sm text-gray-600">{route.availableSeats}</span>
                    </td>
                    <td className="px-3 py-3">
                      <span className="font-medium text-sm text-green-600">
                        {formatCurrency(route.revenue)}
                      </span>
                    </td>
                    <td className="px-3 py-3">
                      <span className="text-sm text-red-600">
                        {formatCurrency(route.expenses)}
                      </span>
                    </td>
                    <td className="px-3 py-3">
                      <span className="font-medium text-sm text-blue-600">
                        {formatCurrency(route.netRev)}
                      </span>
                    </td>
                    <td className="px-3 py-3">
                      <span className="text-sm">{formatCurrency(route.asp)}</span>
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-2">
                        <div className="h-2 w-16 overflow-hidden rounded-full bg-gray-100">
                          <div
                            className={`h-full ${route.occupancy >= 80 ? 'bg-green-500' : route.occupancy >= 60 ? 'bg-yellow-500' : 'bg-red-500'}`}
                            style={{ width: `${route.occupancy}%` }}
                          />
                        </div>
                        <span className="text-xs font-medium">{route.occupancy}%</span>
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <span className="rounded bg-gray-100 px-2 py-1 text-xs font-medium text-gray-700">
                        {route.channel}
                      </span>
                    </td>
                    <td className="px-3 py-3">
                      {route.occupancy >= 80 ? (
                        <span className="inline-flex items-center gap-1 rounded-full bg-green-100 px-2 py-1 text-xs font-medium text-green-700">
                          <TrendingUp className="h-3 w-3" />
                          Excellent
                        </span>
                      ) : route.occupancy >= 70 ? (
                        <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-2 py-1 text-xs font-medium text-blue-700">
                          <TrendingUp className="h-3 w-3" />
                          Good
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 rounded-full bg-yellow-100 px-2 py-1 text-xs font-medium text-yellow-700">
                          <TrendingDown className="h-3 w-3" />
                          Average
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}
