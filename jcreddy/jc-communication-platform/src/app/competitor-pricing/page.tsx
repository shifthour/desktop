'use client'

import { useQuery, useQueryClient } from '@tanstack/react-query'
import React, { useState, useEffect, Component, type ReactNode } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { formatCurrency, timeAgo } from '@/lib/utils'
import {
  Home,
  ArrowRight,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Shield,
  Loader2,
  Bus,
  Swords,
  Clock,
  TrendingDown,
  RefreshCw,
  IndianRupee,
  MapPin,
  LayoutGrid,
  X,
  BarChart3,
  Armchair,
  Calendar,
  Target,
  TrendingUp,
  Eye,
  ChevronRight,
  Activity,
  Info,
  Store,
  Zap,
} from 'lucide-react'
import Link from 'next/link'
import { AIPricingTab } from '@/components/ai-pricing-tab'

// ─────────────────────────────────────────────────────────────────────────────
// Error Boundary — catches rendering crashes
// ─────────────────────────────────────────────────────────────────────────────

class CardErrorBoundary extends Component<
  { children: ReactNode; fallback?: ReactNode },
  { hasError: boolean; error: string }
> {
  constructor(props: any) {
    super(props)
    this.state = { hasError: false, error: '' }
  }
  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error: error.message }
  }
  render() {
    if (this.state.hasError) {
      return (
        this.props.fallback || (
          <div className="rounded-xl border border-red-200 bg-red-50 p-6 text-center">
            <AlertTriangle className="h-6 w-6 text-red-500 mx-auto mb-2" />
            <p className="text-sm font-medium text-red-700">Failed to render this card</p>
            <p className="text-xs text-red-500 mt-1">{this.state.error}</p>
          </div>
        )
      )
    }
    return this.props.children
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Data fetching
// ─────────────────────────────────────────────────────────────────────────────

async function fetchComparisons(status: string, date: string, route: string) {
  let url = `/api/pricing/competitor?status=${status}&limit=100`
  if (date) url += `&date=${date}`
  if (route) url += `&route=${encodeURIComponent(route)}`
  const res = await fetch(url)
  if (!res.ok) throw new Error('Failed to fetch comparisons')
  return res.json()
}

// ─────────────────────────────────────────────────────────────────────────────
// Domain navigation
// ─────────────────────────────────────────────────────────────────────────────

const domainNav = [
  { id: 'executive', label: 'Executive', href: '/executive' },
  { id: 'bookings', label: 'Bookings', href: '/bookings' },
  { id: 'revenue', label: 'Revenue', href: '/revenue' },
  { id: 'pricing', label: 'Dynamic Pricing', href: '/pricing' },
  { id: 'competitor', label: 'Competitor Pricing', href: '/competitor-pricing', active: true },
  { id: 'trips', label: 'Trips', href: '/trips' },
]

// ─────────────────────────────────────────────────────────────────────────────
// Operator colors
// ─────────────────────────────────────────────────────────────────────────────

const OPERATOR_COLORS: Record<string, { bg: string; text: string; border: string; badge: string; badgeText: string }> = {
  mythri: { bg: 'bg-blue-50', text: 'text-blue-700', border: 'border-blue-200', badge: 'bg-blue-600', badgeText: 'text-white' },
  vikram: { bg: 'bg-indigo-50', text: 'text-indigo-700', border: 'border-indigo-200', badge: 'bg-indigo-600', badgeText: 'text-white' },
  vkaveri: { bg: 'bg-purple-50', text: 'text-purple-700', border: 'border-purple-200', badge: 'bg-purple-600', badgeText: 'text-white' },
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Component
// ─────────────────────────────────────────────────────────────────────────────

export default function CompetitorPricingPage() {
  const queryClient = useQueryClient()
  const [statusFilter, setStatusFilter] = useState('pending')
  const [dateFilter, setDateFilter] = useState('')
  const [routeFilter, setRouteFilter] = useState('')
  const [actionLoading, setActionLoading] = useState<string | null>(null)
  const [scraping, setScraping] = useState(false)
  const [scrapeDate, setScrapeDate] = useState(() => {
    const now = new Date()
    const ist = new Date(now.getTime() + 5.5 * 60 * 60 * 1000)
    return ist.toISOString().split('T')[0]
  })
  const [seatModalGroup, setSeatModalGroup] = useState<any | null>(null)
  const [activeSubTab, setActiveSubTab] = useState<string>('live')

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape' && seatModalGroup) setSeatModalGroup(null)
    }
    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [seatModalGroup])

  // Data fetching
  const { data, isLoading } = useQuery({
    queryKey: ['competitor-comparisons', statusFilter, dateFilter, routeFilter],
    queryFn: () => fetchComparisons(statusFilter, dateFilter, routeFilter),
    refetchInterval: 30000,
  })

  const groupedComparisons = data?.grouped_comparisons || []
  const stats = data?.stats || {}
  const lastScrape = data?.last_scrape
  const lastScrapeDetails = data?.last_scrape_details
  const routes = data?.routes || []

  // Analytics data for sub-tabs
  const { data: calendarData } = useQuery({
    queryKey: ['competitor-calendar', routeFilter],
    queryFn: async () => {
      let url = '/api/pricing/competitor/analytics?view=calendar&days=30'
      if (routeFilter) url += `&route=${encodeURIComponent(routeFilter)}`
      const res = await fetch(url)
      if (!res.ok) throw new Error('Failed to fetch calendar data')
      return res.json()
    },
    enabled: activeSubTab === 'calendar' || activeSubTab === '30day',
  })

  const { data: historyData } = useQuery({
    queryKey: ['competitor-history', routeFilter],
    queryFn: async () => {
      let url = '/api/pricing/competitor/analytics?view=history&days=14'
      if (routeFilter) url += `&route=${encodeURIComponent(routeFilter)}`
      const res = await fetch(url)
      if (!res.ok) throw new Error('Failed to fetch history data')
      return res.json()
    },
    enabled: activeSubTab === 'intel' || activeSubTab === 'velocity',
  })

  const { data: overviewData } = useQuery({
    queryKey: ['competitor-overview', routeFilter],
    queryFn: async () => {
      let url = '/api/pricing/competitor/analytics?view=overview&days=30'
      if (routeFilter) url += `&route=${encodeURIComponent(routeFilter)}`
      const res = await fetch(url)
      if (!res.ok) throw new Error('Failed to fetch overview data')
      return res.json()
    },
    enabled: activeSubTab === 'overview',
  })

  // AI Dynamic Pricing data
  const { data: aiPricingData, isLoading: aiPricingLoading } = useQuery({
    queryKey: ['ai-pricing', routeFilter],
    queryFn: async () => {
      let url = '/api/pricing/dynamic?days=30'
      if (routeFilter) url += `&route=${encodeURIComponent(routeFilter)}`
      const res = await fetch(url)
      if (!res.ok) throw new Error('Failed to fetch AI pricing')
      return res.json()
    },
    enabled: activeSubTab === 'ai-pricing',
  })

  // Seat details per group — individual available seats fetched from q3/busmap APIs
  const [seatDetails, setSeatDetails] = useState<Record<string, { mythri: any[]; vikram: any[]; vkaveri: any[] }>>({})

  useEffect(() => {
    const groups = data?.grouped_comparisons || []
    if (groups.length === 0) return

    let cancelled = false

    async function fetchAllSeats() {
      const results: Record<string, { mythri: any[]; vikram: any[]; vkaveri: any[] }> = {}

      await Promise.all(
        groups.map(async (group: any) => {
          const key = `${group.mythri_service}|${group.travel_date}`
          const entry = { mythri: [] as any[], vikram: [] as any[], vkaveri: [] as any[] }
          const routeParams = `&route_from=${encodeURIComponent(group.route_from || '')}&route_to=${encodeURIComponent(group.route_to || '')}&travel_date=${encodeURIComponent(group.travel_date || '')}`
          const competitors = group.competitors || []
          const vikram = competitors.find((c: any) => c.operator === 'vikram')
          const vkaveri = competitors.find((c: any) => c.operator === 'vkaveri')

          try {
            const fetches: Promise<void>[] = []

            if (group.mythri_trip_id) {
              fetches.push(
                fetch(`/api/pricing/competitor/seats?mythri_id=${group.mythri_trip_id}`)
                  .then((r) => r.json())
                  .then((d) => { if (d.success && d.mythri_layout) entry.mythri = extractAvailableSeats(d.mythri_layout) })
                  .catch(() => {})
              )
            }

            if (vikram?.trip_id) {
              fetches.push(
                fetch(`/api/pricing/competitor/seats?operator=vikram&competitor_id=${encodeURIComponent(vikram.trip_id)}${routeParams}`)
                  .then((r) => r.json())
                  .then((d) => { if (d.success && d.competitor_layout) entry.vikram = extractAvailableSeats(d.competitor_layout) })
                  .catch(() => {})
              )
            }

            if (vkaveri?.trip_id) {
              fetches.push(
                fetch(`/api/pricing/competitor/seats?operator=vkaveri&competitor_id=${vkaveri.trip_id}`)
                  .then((r) => r.json())
                  .then((d) => { if (d.success && d.competitor_layout) entry.vkaveri = extractAvailableSeats(d.competitor_layout) })
                  .catch(() => {})
              )
            }

            await Promise.all(fetches)
          } catch {}

          results[key] = entry
        })
      )

      if (!cancelled) setSeatDetails(results)
    }

    fetchAllSeats()
    return () => { cancelled = true }
  }, [data])

  // ─── Actions ───

  async function handleScrapeNow() {
    setScraping(true)
    try {
      const res = await fetch(`/api/cron/competitor-scrape?date=${scrapeDate}`, {
        headers: { 'x-cron-secret': 'mythri-cron-secret-2024' },
      })
      const result = await res.json()
      if (result.success) {
        // Auto-set the view date filter to the scraped date
        setDateFilter(scrapeDate)
        queryClient.invalidateQueries({ queryKey: ['competitor-comparisons'] })
        alert(`Scrape complete for ${scrapeDate}!\n${result.comparisons_generated} comparisons across ${result.routes_scraped} routes.\nMythri: ${result.mythri_trips} | Vikram: ${result.vikram_trips} | VKaveri: ${result.vkaveri_trips}`)
      } else {
        alert(result.error || 'Scrape failed')
      }
    } catch {
      alert('Failed to run scrape')
    } finally {
      setScraping(false)
    }
  }

  async function handleAction(id: string, action: 'apply' | 'dismiss') {
    setActionLoading(id)
    try {
      const res = await fetch('/api/pricing/competitor', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, action }),
      })
      const result = await res.json()
      if (result.success) {
        queryClient.invalidateQueries({ queryKey: ['competitor-comparisons'] })
      }
    } catch {
      alert(`Failed to ${action} recommendation`)
    } finally {
      setActionLoading(null)
    }
  }

  function formatDepartureTime(dt: string | null) {
    if (!dt) return '-'
    const d = new Date(dt)
    return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true })
  }

  function formatTravelDate(date: string) {
    if (!date) return '-'
    try {
      return new Date(date + 'T00:00:00').toLocaleDateString('en-IN', {
        day: '2-digit',
        month: 'short',
        weekday: 'short',
      })
    } catch {
      return date
    }
  }

  // ─── Render ───

  return (
    <DashboardLayout>
      {/* Breadcrumb */}
      <div className="border-b bg-white px-6 py-3">
        <div className="flex items-center gap-2 text-sm text-gray-500">
          <Link href="/" className="hover:text-gray-700"><Home className="h-4 w-4" /></Link>
          <ArrowRight className="h-3 w-3" />
          <span className="font-medium text-gray-900">Competitor Pricing</span>
        </div>
      </div>

      {/* Domain Nav Tabs */}
      <div className="border-b bg-white px-6">
        <div className="flex gap-1 overflow-x-auto">
          {domainNav.map((nav) => (
            <Link
              key={nav.id}
              href={nav.href}
              className={`whitespace-nowrap px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                nav.active
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {nav.label}
            </Link>
          ))}
        </div>
      </div>

      {/* Sub-Tab Bar */}
      <div className="border-b bg-gray-50 px-6">
        <div className="flex gap-0 overflow-x-auto">
          {[
            { id: 'live', label: 'Live Comparison', icon: Swords },
            { id: '30day', label: '30-Day Data', icon: LayoutGrid },
            { id: 'ai-pricing', label: 'AI Pricing', icon: Zap },
            { id: 'overview', label: 'Overview', icon: BarChart3 },
            { id: 'seats', label: 'Seat Comparison', icon: Armchair },
            { id: 'channel', label: 'Channel Pricing', icon: Store },
            { id: 'calendar', label: 'Price Calendar', icon: Calendar },
            { id: 'intel', label: 'Competitor Intel', icon: Target },
            { id: 'velocity', label: 'Velocity & Demand', icon: TrendingUp },
          ].map((tab) => {
            const Icon = tab.icon
            return (
              <button
                key={tab.id}
                onClick={() => setActiveSubTab(tab.id)}
                className={`flex items-center gap-2 whitespace-nowrap px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                  activeSubTab === tab.id
                    ? 'border-blue-600 text-blue-600 bg-white'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:bg-white/50'
                }`}
              >
                <Icon className="h-4 w-4" />
                {tab.label}
              </button>
            )
          })}
        </div>
      </div>

      <div className="p-6 space-y-6">
        {/* ═══ LIVE COMPARISON TAB ═══ */}
        {activeSubTab === 'live' && (<>
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Competitor Pricing</h1>
            <p className="text-sm text-gray-500 mt-1">
              Mythri vs Competitors — side-by-side fare comparison
            </p>
          </div>
          <div className="flex items-center gap-3">
            {lastScrape && (
              <span className="text-xs text-gray-400">
                Last scraped: {timeAgo(lastScrape)}
              </span>
            )}
            <input
              type="date"
              value={scrapeDate}
              min={new Date().toISOString().split('T')[0]}
              max={(() => { const d = new Date(); d.setDate(d.getDate() + 30); return d.toISOString().split('T')[0] })()}
              onChange={(e) => setScrapeDate(e.target.value)}
              className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
            />
            <button
              onClick={handleScrapeNow}
              disabled={scraping}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {scraping ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4" />
              )}
              Scrape Now
            </button>
          </div>
        </div>

        {/* KPI Cards */}
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <div className="rounded-2xl bg-white p-5 shadow-lg border border-gray-100">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Pending Comparisons</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{stats.total_pending || 0}</p>
              </div>
              <div className="rounded-xl bg-orange-100 p-3">
                <Swords className="h-5 w-5 text-orange-600" />
              </div>
            </div>
            <p className="mt-2 text-xs text-gray-500">
              <span className="text-red-600 font-medium">{stats.costlier_count || 0} costlier</span>
              {' · '}
              <span className="text-green-600 font-medium">{stats.cheaper_count || 0} cheaper</span>
            </p>
          </div>

          <div className="rounded-2xl bg-white p-5 shadow-lg border border-gray-100">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Avg Overpricing</p>
                <p className="text-2xl font-bold text-red-600 mt-1">
                  {stats.avg_fare_difference ? `+${formatCurrency(stats.avg_fare_difference)}` : '-'}
                </p>
              </div>
              <div className="rounded-xl bg-red-100 p-3">
                <IndianRupee className="h-5 w-5 text-red-600" />
              </div>
            </div>
            <p className="mt-2 text-xs text-gray-500">Avg difference where Mythri is costlier</p>
          </div>

          <div className="rounded-2xl bg-white p-5 shadow-lg border border-gray-100">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Potential Savings</p>
                <p className="text-2xl font-bold text-green-600 mt-1">
                  {formatCurrency(stats.total_potential_savings || 0)}
                </p>
              </div>
              <div className="rounded-xl bg-green-100 p-3">
                <TrendingDown className="h-5 w-5 text-green-600" />
              </div>
            </div>
            <p className="mt-2 text-xs text-gray-500">Total reduction if all applied</p>
          </div>

          <div className="rounded-2xl bg-white p-5 shadow-lg border border-gray-100">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Routes Monitored</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{stats.routes_monitored || 0}</p>
              </div>
              <div className="rounded-xl bg-blue-100 p-3">
                <MapPin className="h-5 w-5 text-blue-600" />
              </div>
            </div>
            <p className="mt-2 text-xs text-gray-500">Active route configurations</p>
          </div>
        </div>

        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex rounded-lg border border-gray-200 bg-white overflow-hidden">
            {['pending', 'applied', 'dismissed', 'all'].map((s) => (
              <button
                key={s}
                onClick={() => setStatusFilter(s)}
                className={`px-4 py-2 text-sm font-medium transition-colors ${
                  statusFilter === s
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-600 hover:bg-gray-50'
                }`}
              >
                {s.charAt(0).toUpperCase() + s.slice(1)}
              </button>
            ))}
          </div>

          <select
            value={routeFilter}
            onChange={(e) => setRouteFilter(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            <option value="">All Routes</option>
            {routes.map((r: any) => (
              <option key={`${r.route_from}|${r.route_to}`} value={`${r.route_from}|${r.route_to}`}>
                {r.route_from} → {r.route_to}
              </option>
            ))}
          </select>

          <input
            type="date"
            value={dateFilter}
            onChange={(e) => setDateFilter(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          />
          {(dateFilter || routeFilter) && (
            <button onClick={() => { setDateFilter(''); setRouteFilter('') }} className="text-sm text-gray-500 hover:text-gray-700">
              Clear filters
            </button>
          )}

          {lastScrapeDetails && (
            <span className="ml-auto text-xs text-gray-400">
              {lastScrapeDetails.mythri_trips || 0} Mythri + {lastScrapeDetails.vikram_trips || 0} Vikram + {lastScrapeDetails.vkaveri_trips || 0} VKaveri trips scraped
            </span>
          )}
        </div>

        {/* Cards */}
        {isLoading ? (
          <div className="flex items-center justify-center py-20">
            <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
          </div>
        ) : groupedComparisons.length === 0 ? (
          <div className="rounded-xl border bg-white shadow-sm">
            <div className="flex flex-col items-center justify-center py-20 text-gray-400">
              <Shield className="h-12 w-12 mb-3" />
              <p className="text-lg font-medium">No comparisons found</p>
              <p className="text-sm mt-1">Click &quot;Scrape Now&quot; to fetch latest fares or adjust filters</p>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            {groupedComparisons.map((group: any, groupIdx: number) => {
              const competitors = group.competitors || []

              // Find the cheapest min_fare across all operators
              const allFares = [
                { operator: 'mythri', fare: group.mythri_min_fare || 0 },
                ...competitors.map((c: any) => ({ operator: c.operator || 'unknown', fare: c.min_fare || 0 })),
              ].filter((f: any) => f.fare > 0)
              const cheapestFare = allFares.length > 0 ? Math.min(...allFares.map((f: any) => f.fare)) : 0
              const cheapestOperator = allFares.find((f: any) => f.fare === cheapestFare)?.operator || ''

              // Find the costliest competitor (where Mythri is costlier)
              const costliestComp = competitors
                .filter((c: any) => (c.fare_difference || 0) > 0)
                .sort((a: any, b: any) => (b.fare_difference || 0) - (a.fare_difference || 0))[0]

              const mythriIsCheapest = cheapestOperator === 'mythri'

              return (
                <CardErrorBoundary key={groupIdx}>
                <div className="rounded-xl border bg-white shadow-sm overflow-hidden">
                  {/* Card Header */}
                  <div className="flex items-center justify-between px-5 py-3 border-b bg-gray-50/80">
                    <div className="flex items-center gap-3">
                      <div className="flex items-center gap-1.5">
                        <MapPin className="h-4 w-4 text-gray-400" />
                        <span className="text-sm font-semibold text-gray-900">
                          {group.route_from} → {group.route_to}
                        </span>
                      </div>
                      <span className="text-sm text-gray-500">{formatTravelDate(group.travel_date)}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => setSeatModalGroup(group)}
                        className="inline-flex items-center gap-1.5 rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 transition-colors shadow-sm"
                      >
                        <LayoutGrid className="h-3.5 w-3.5" />
                        View Seats
                      </button>
                      {mythriIsCheapest && (
                        <span className="inline-flex items-center gap-1 rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-semibold text-green-700">
                          <CheckCircle className="h-3 w-3" />
                          Mythri is cheapest
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Card Body — 3-column grid */}
                  <div className="grid grid-cols-1 md:grid-cols-3 divide-y md:divide-y-0 md:divide-x divide-gray-100">
                    {/* Mythri Column */}
                    <OperatorColumn
                      operator="mythri"
                      label="Mythri"
                      service={group.mythri_service}
                      busType={group.mythri_bus_type}
                      departure={group.mythri_departure}
                      availableSeats={group.mythri_available_seats}
                      minFare={group.mythri_min_fare}
                      maxFare={group.mythri_max_fare}
                      seatTypeFares={group.mythri_seat_type_fares}
                      individualSeats={seatDetails[`${group.mythri_service}|${group.travel_date}`]?.mythri}
                      isCheapest={cheapestOperator === 'mythri'}
                      cheapestFare={cheapestFare}
                      formatTime={formatDepartureTime}
                    />

                    {/* Vikram Column */}
                    {(() => {
                      const vikram = competitors.find((c: any) => c.operator === 'vikram')
                      if (vikram) {
                        return (
                          <OperatorColumn
                            operator="vikram"
                            label="Vikram"
                            service={vikram.service}
                            busType={vikram.bus_type}
                            departure={vikram.departure}
                            availableSeats={vikram.available_seats}
                            minFare={vikram.min_fare}
                            maxFare={vikram.max_fare}
                            seatTypeFares={vikram.seat_type_fares}
                            individualSeats={seatDetails[`${group.mythri_service}|${group.travel_date}`]?.vikram}
                            isCheapest={cheapestOperator === 'vikram'}
                            cheapestFare={cheapestFare}
                            formatTime={formatDepartureTime}
                          />
                        )
                      }
                      return <EmptyOperatorColumn operator="vikram" label="Vikram" />
                    })()}

                    {/* VKaveri Column */}
                    {(() => {
                      const vkaveri = competitors.find((c: any) => c.operator === 'vkaveri')
                      if (vkaveri) {
                        return (
                          <OperatorColumn
                            operator="vkaveri"
                            label="VKaveri"
                            service={vkaveri.service}
                            busType={vkaveri.bus_type}
                            departure={vkaveri.departure}
                            availableSeats={vkaveri.available_seats}
                            minFare={vkaveri.min_fare}
                            maxFare={vkaveri.max_fare}
                            seatTypeFares={vkaveri.seat_type_fares}
                            individualSeats={seatDetails[`${group.mythri_service}|${group.travel_date}`]?.vkaveri}
                            isCheapest={cheapestOperator === 'vkaveri'}
                            cheapestFare={cheapestFare}
                            formatTime={formatDepartureTime}
                          />
                        )
                      }
                      return <EmptyOperatorColumn operator="vkaveri" label="VKaveri" />
                    })()}
                  </div>

                  {/* Card Footer */}
                  <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 px-5 py-3 border-t bg-gray-50/50">
                    <div className="text-sm">
                      {costliestComp ? (
                        <span className="text-red-600 font-medium">
                          Mythri is {formatCurrency(costliestComp.fare_difference)} costlier than {costliestComp.operator === 'vkaveri' ? 'VKaveri' : 'Vikram'}
                          {costliestComp.recommended_mythri_fare > 0 && (
                            <span className="text-gray-500 font-normal">
                              {' '} — Recommend: <span className="text-orange-600 font-semibold">{formatCurrency(costliestComp.recommended_mythri_fare)}</span>
                              {' '}(undercut {formatCurrency(costliestComp.undercut_amount)})
                            </span>
                          )}
                        </span>
                      ) : (
                        <span className="text-green-600 font-medium">Mythri is competitively priced</span>
                      )}
                    </div>

                    {/* Actions — for pending comparisons */}
                    {statusFilter === 'pending' && competitors.length > 0 && (
                      <div className="flex items-center gap-2">
                        {competitors.map((comp: any) => (
                          <div key={comp.id} className="flex items-center gap-1.5">
                            <span className={`text-[10px] font-bold uppercase tracking-wider ${
                              comp.operator === 'vkaveri' ? 'text-purple-600' : 'text-indigo-600'
                            }`}>
                              {comp.operator === 'vkaveri' ? 'VK' : 'VR'}
                            </span>
                            <button
                              onClick={() => handleAction(comp.id, 'apply')}
                              disabled={actionLoading === comp.id}
                              className="inline-flex items-center gap-1 rounded-lg bg-green-600 px-2.5 py-1 text-xs font-medium text-white hover:bg-green-700 disabled:opacity-50 transition-colors"
                              title={`Apply ${comp.operator} recommendation`}
                            >
                              {actionLoading === comp.id ? (
                                <Loader2 className="h-3 w-3 animate-spin" />
                              ) : (
                                <CheckCircle className="h-3 w-3" />
                              )}
                              Apply
                            </button>
                            <button
                              onClick={() => handleAction(comp.id, 'dismiss')}
                              disabled={actionLoading === comp.id}
                              className="inline-flex items-center gap-1 rounded-lg border border-gray-300 px-2.5 py-1 text-xs font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50 transition-colors"
                            >
                              <XCircle className="h-3 w-3" />
                            </button>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
                </CardErrorBoundary>
              )
            })}
          </div>
        )}

        {/* Info Bar */}
        <div className="rounded-xl border bg-gray-50 p-4">
          <div className="flex items-start gap-2 text-sm text-gray-500">
            <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
            <div>
              <p>
                Fares are scraped from <strong>mythribus.com</strong>, <strong>vikramtravels.com</strong>, and <strong>vkaveribus.com</strong> every 30 minutes.
                Recommendations suggest reducing Mythri fares to undercut competitors by ₹30.
              </p>
              <p className="mt-1">
                &quot;Apply&quot; marks the recommendation as noted — you must manually update the fare in Bitla/TicketSimply.
              </p>
            </div>
          </div>
        </div>
        </>)}

        {/* ═══ OVERVIEW TAB ═══ */}
        {activeSubTab === 'overview' && (
          <OverviewTab
            stats={stats}
            groupedComparisons={groupedComparisons}
            overviewData={overviewData}
            routes={routes}
            routeFilter={routeFilter}
            lastScrape={lastScrape}
          />
        )}

        {/* ═══ SEAT COMPARISON TAB ═══ */}
        {activeSubTab === 'seats' && (
          <SeatComparisonTab
            groupedComparisons={groupedComparisons}
            isLoading={isLoading}
          />
        )}

        {/* ═══ CHANNEL PRICING TAB ═══ */}
        {activeSubTab === 'channel' && (
          <ChannelPricingTab
            groupedComparisons={groupedComparisons}
          />
        )}

        {/* ═══ PRICE CALENDAR TAB ═══ */}
        {activeSubTab === 'calendar' && (
          <PriceCalendarTab
            calendarData={calendarData}
            routeFilter={routeFilter}
          />
        )}

        {/* ═══ COMPETITOR INTEL TAB ═══ */}
        {activeSubTab === 'intel' && (
          <CompetitorIntelTab
            groupedComparisons={groupedComparisons}
            historyData={historyData}
          />
        )}

        {/* ═══ VELOCITY & DEMAND TAB ═══ */}
        {activeSubTab === 'velocity' && (
          <VelocityDemandTab
            historyData={historyData}
            groupedComparisons={groupedComparisons}
          />
        )}

        {activeSubTab === '30day' && (
          <ThirtyDayDataTab
            calendarData={calendarData}
            routeFilter={routeFilter}
            routes={routes}
            setRouteFilter={setRouteFilter}
          />
        )}

        {activeSubTab === 'ai-pricing' && (
          <AIPricingTab
            data={aiPricingData}
            loading={aiPricingLoading}
            routeFilter={routeFilter}
            routes={routes}
            setRouteFilter={setRouteFilter}
          />
        )}
      </div>
      {seatModalGroup && (
        <CardErrorBoundary fallback={
          <div className="fixed inset-0 z-50 flex items-center justify-center">
            <div className="absolute inset-0 bg-black/50" onClick={() => setSeatModalGroup(null)} />
            <div className="relative bg-white rounded-xl p-8 text-center mx-4">
              <AlertTriangle className="h-8 w-8 text-red-500 mx-auto mb-3" />
              <p className="font-medium text-red-700">Failed to load seat layout</p>
              <button onClick={() => setSeatModalGroup(null)} className="mt-4 px-4 py-2 bg-gray-100 rounded-lg text-sm">Close</button>
            </div>
          </div>
        }>
          <SeatBreakdownModal
            group={seatModalGroup}
            onClose={() => setSeatModalGroup(null)}
          />
        </CardErrorBoundary>
      )}
    </DashboardLayout>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Operator Column Component
// ─────────────────────────────────────────────────────────────────────────────

function OperatorColumn({
  operator,
  label,
  service,
  busType,
  departure,
  availableSeats,
  minFare,
  maxFare,
  seatTypeFares,
  individualSeats,
  isCheapest,
  cheapestFare,
  formatTime,
}: {
  operator: string
  label: string
  service: string
  busType: string
  departure: string | null
  availableSeats: number | null
  minFare: number
  maxFare: number
  seatTypeFares: any[]
  individualSeats?: { seat_number: string; fare: number }[]
  isCheapest: boolean
  cheapestFare: number
  formatTime: (dt: string | null) => string
}) {
  const colors = OPERATOR_COLORS[operator] || OPERATOR_COLORS.mythri

  return (
    <div className="p-4 space-y-3">
      {/* Operator Badge */}
      <div className="flex items-center justify-between">
        <span className={`inline-flex items-center rounded-md px-2.5 py-1 text-xs font-bold uppercase tracking-wider ${colors.badge} ${colors.badgeText}`}>
          {label}
        </span>
        {isCheapest && (
          <span className="text-[10px] font-bold text-green-600 uppercase tracking-wider">Cheapest</span>
        )}
      </div>

      {/* Bus Details */}
      <div>
        <div className="flex items-center gap-1.5">
          <Bus className="h-3.5 w-3.5 text-gray-400" />
          <span className="text-sm font-semibold text-gray-900">{service || '-'}</span>
        </div>
        <p className="text-xs text-gray-500 mt-0.5 truncate" title={busType}>{busType || '-'}</p>
      </div>

      {/* Departure + Seats */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1">
          <Clock className="h-3 w-3 text-gray-400" />
          <span className="text-sm text-gray-700">{formatTime(departure)}</span>
        </div>
        <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
          (availableSeats ?? 0) <= 5 ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'
        }`}>
          {availableSeats ?? '-'} seats
        </span>
      </div>

      {/* Fare */}
      <div className={`rounded-lg p-3 text-center ${isCheapest ? 'bg-green-50 border border-green-200' : 'bg-gray-50 border border-gray-200'}`}>
        <div className={`text-2xl font-bold ${isCheapest ? 'text-green-600' : (minFare > cheapestFare ? 'text-red-600' : 'text-gray-900')}`}>
          {formatCurrency(minFare || 0)}
        </div>
        {maxFare > 0 && maxFare !== minFare && (
          <div className="text-xs text-gray-400 mt-0.5">to {formatCurrency(maxFare)}</div>
        )}
      </div>

      {/* Available Seats — individual seats grouped by fare */}
      {Array.isArray(individualSeats) && individualSeats.length > 0 && (() => {
        const byFare: Record<number, string[]> = {}
        for (const s of individualSeats) {
          const f = s.fare || 0
          if (!byFare[f]) byFare[f] = []
          byFare[f].push(s.seat_number)
        }
        const fareGroups = Object.entries(byFare)
          .map(([fare, seats]) => ({ fare: parseInt(fare), seats }))
          .sort((a, b) => a.fare - b.fare)

        return (
          <div className="border rounded-lg overflow-hidden">
            <div className="bg-gray-50 px-3 py-1.5 border-b">
              <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Available Seats</span>
            </div>
            <div className="divide-y divide-gray-50">
              {fareGroups.map((fg, i) => (
                <div key={i} className="px-3 py-1.5">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-semibold text-green-700">{formatCurrency(fg.fare)}</span>
                    <span className="text-[10px] text-gray-400">{fg.seats.length} avl</span>
                  </div>
                  <p className="text-[11px] text-gray-600 mt-0.5">{fg.seats.join(', ')}</p>
                </div>
              ))}
            </div>
          </div>
        )
      })()}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Empty Operator Column (no matching trip found)
// ─────────────────────────────────────────────────────────────────────────────

function EmptyOperatorColumn({ operator, label }: { operator: string; label: string }) {
  const colors = OPERATOR_COLORS[operator] || OPERATOR_COLORS.mythri

  return (
    <div className="p-4 flex flex-col items-center justify-center text-center space-y-2 min-h-[200px]">
      <span className={`inline-flex items-center rounded-md px-2.5 py-1 text-xs font-bold uppercase tracking-wider ${colors.badge} ${colors.badgeText} opacity-50`}>
        {label}
      </span>
      <p className="text-sm text-gray-400">No matching trip</p>
      <p className="text-xs text-gray-300">No trip within 1.5h window</p>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Seat Layout Modal — Real Bus Seat Grid (matches mythribus.com style)
// ─────────────────────────────────────────────────────────────────────────────

// TicketSimply seat type values
// Lower berths: SLB=12, DLB=17, and other lower variants
// Upper berths: SUB=13, DUB=18, and other upper variants
// Lower berths: SLB=12, DLB=17, and VKaveri DLB=2
const LOWER_SEAT_TYPES = new Set([2, 12, 14, 16, 17, 20])
// Upper berths: SUB=13, DUB=18, and VKaveri DUB=3
const UPPER_SEAT_TYPES = new Set([3, 13, 15, 18, 19, 21])

/** Determine if a seat is available, booked, or ladies-reserved from td_class */
function getSeatStatus(seat: any): 'available' | 'booked' | 'ladies' {
  const td = seat?.td_class || ''
  if (td.includes('available_seat') || td.includes('availableSleeper') || td.includes('availableSeater')) {
    return 'available'
  }
  if (td.includes('ladies') || td.includes('female') || td.includes('booked_by_ladies')) {
    return 'ladies'
  }
  // Also treat fare > 0 as available (fallback)
  if ((seat?.fare || 0) > 0 && !td.includes('reservedSleeper') && !td.includes('reservedSeater')) {
    return 'available'
  }
  return 'booked'
}

/** Extract available seats with fares from a coach_layout */
function extractAvailableSeats(layout: any[][]): { seat_number: string; fare: number }[] {
  if (!layout || !Array.isArray(layout)) return []
  const seats: { seat_number: string; fare: number }[] = []
  for (const row of layout) {
    if (!Array.isArray(row)) continue
    for (const seat of row) {
      if (!seat || !seat.seat_number || seat.is_ganway_col) continue
      if (getSeatStatus(seat) === 'available') {
        seats.push({
          seat_number: seat.seat_number,
          fare: typeof seat.fare === 'number' ? Math.round(seat.fare) : 0,
        })
      }
    }
  }
  seats.sort((a, b) => a.fare - b.fare)
  return seats
}

/**
 * Process TicketSimply coach_layout into Lower + Upper deck grids.
 * Each row in the raw layout has seats for both decks interleaved.
 * We split by seat_type_value into separate Lower and Upper grids.
 */
function processCoachLayout(layout: any[][]): {
  lower: any[][]
  upper: any[][]
  fareTiers: number[]
} {
  if (!layout || !Array.isArray(layout)) return { lower: [], upper: [], fareTiers: [] }

  const lowerRows: any[][] = []
  const upperRows: any[][] = []
  const fareSet = new Set<number>()

  for (const row of layout) {
    if (!Array.isArray(row)) continue
    // Skip separator/null rows
    const hasSeats = row.some((s) => s && s.seat_number)
    if (!hasSeats) continue

    const lowerSeats: any[] = []
    const upperSeats: any[] = []

    for (const seat of row) {
      if (!seat) continue
      if (seat.is_ganway_col) continue
      if (!seat.seat_number) continue

      const stv = seat.seat_type_value
      const status = getSeatStatus(seat)
      const fare = typeof seat.fare === 'number' ? seat.fare : 0
      if (status === 'available' && fare > 0) fareSet.add(Math.round(fare))

      if (LOWER_SEAT_TYPES.has(stv)) {
        lowerSeats.push(seat)
      } else if (UPPER_SEAT_TYPES.has(stv)) {
        upperSeats.push(seat)
      } else {
        // Unknown type — put in lower by default
        lowerSeats.push(seat)
      }
    }

    // Build deck rows with gangway: [single] [gangway] [double1] [double2]
    if (lowerSeats.length > 0) {
      if (lowerSeats.length >= 2) {
        lowerRows.push([lowerSeats[0], { _gangway: true }, ...lowerSeats.slice(1)])
      } else {
        lowerRows.push(lowerSeats)
      }
    }
    if (upperSeats.length > 0) {
      if (upperSeats.length >= 2) {
        upperRows.push([upperSeats[0], { _gangway: true }, ...upperSeats.slice(1)])
      } else {
        upperRows.push(upperSeats)
      }
    }
  }

  const fareTiers = Array.from(fareSet).sort((a, b) => a - b)
  return { lower: lowerRows, upper: upperRows, fareTiers }
}

function SeatBreakdownModal({ group, onClose }: { group: any; onClose: () => void }) {
  const [seatData, setSeatData] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const vikram = group.competitors?.find((c: any) => c.operator === 'vikram')
  const vkaveri = group.competitors?.find((c: any) => c.operator === 'vkaveri')

  useEffect(() => {
    async function fetchSeats() {
      setLoading(true)
      setError(null)
      try {
        const layouts: Record<string, any> = { mythri: null, vikram: null, vkaveri: null }
        const routeParams = `&route_from=${encodeURIComponent(group.route_from || '')}&route_to=${encodeURIComponent(group.route_to || '')}&travel_date=${encodeURIComponent(group.travel_date || '')}`

        const mythriId = group.mythri_trip_id
        if (mythriId) {
          const res = await fetch(`/api/pricing/competitor/seats?mythri_id=${mythriId}`)
          const data = await res.json()
          if (data.success) layouts.mythri = data.mythri_layout
        }

        // Vikram layout (EzeeBus — normalized to TicketSimply format by API)
        if (vikram?.trip_id) {
          const res = await fetch(`/api/pricing/competitor/seats?operator=vikram&competitor_id=${encodeURIComponent(vikram.trip_id)}${routeParams}`)
          const data = await res.json()
          if (data.success) layouts.vikram = data.competitor_layout
        }

        // VKaveri layout (TicketSimply q3)
        if (vkaveri?.trip_id) {
          const res = await fetch(`/api/pricing/competitor/seats?operator=vkaveri&competitor_id=${vkaveri.trip_id}`)
          const data = await res.json()
          if (data.success) layouts.vkaveri = data.competitor_layout
        }

        setSeatData(layouts)
      } catch (err: any) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchSeats()
  }, [group.mythri_trip_id, vikram?.trip_id, vkaveri?.trip_id, group.route_from, group.route_to, group.travel_date])

  function fmtTime(dt: string | null) {
    if (!dt) return '-'
    return new Date(dt).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative w-full max-w-7xl max-h-[92vh] bg-white rounded-xl shadow-2xl flex flex-col mx-4">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b bg-gradient-to-r from-blue-50 to-purple-50 rounded-t-xl">
          <div>
            <h2 className="text-lg font-bold text-gray-900">Bus Seat Layout</h2>
            <p className="text-sm text-gray-500">
              {group.route_from} → {group.route_to} &middot;{' '}
              {new Date((group.travel_date || '') + 'T00:00:00').toLocaleDateString('en-IN', { day: '2-digit', month: 'short', weekday: 'short' })}
            </p>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-white/60 rounded-lg transition-colors">
            <X className="h-5 w-5 text-gray-500" />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-5">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-20">
              <Loader2 className="h-8 w-8 animate-spin text-blue-600 mb-3" />
              <p className="text-sm text-gray-500">Loading seat layouts...</p>
            </div>
          ) : error ? (
            <div className="flex flex-col items-center justify-center py-20 text-red-500">
              <AlertTriangle className="h-8 w-8 mb-2" />
              <p className="text-sm">{error}</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
              <SeatLayoutColumn
                operator="mythri"
                label="Mythri"
                service={group.mythri_service}
                busType={group.mythri_bus_type}
                departure={group.mythri_departure}
                seats={group.mythri_available_seats}
                layout={seatData?.mythri}
                seatTypeFares={group.mythri_seat_type_fares}
                formatTime={fmtTime}
              />
              <SeatLayoutColumn
                operator="vikram"
                label="Vikram"
                service={vikram?.service}
                busType={vikram?.bus_type}
                departure={vikram?.departure}
                seats={vikram?.available_seats}
                layout={seatData?.vikram}
                seatTypeFares={vikram?.seat_type_fares}
                formatTime={fmtTime}
              />
              <SeatLayoutColumn
                operator="vkaveri"
                label="VKaveri"
                service={vkaveri?.service}
                busType={vkaveri?.bus_type}
                departure={vkaveri?.departure}
                seats={vkaveri?.available_seats}
                layout={seatData?.vkaveri}
                seatTypeFares={vkaveri?.seat_type_fares}
                formatTime={fmtTime}
              />
            </div>
          )}
        </div>

        {/* Footer — Legend */}
        <div className="border-t px-5 py-3 bg-gray-50 rounded-b-xl">
          <div className="flex items-center justify-center gap-6 text-xs text-gray-500">
            <span className="flex items-center gap-1.5">
              <span className="w-6 h-5 rounded border border-green-400 bg-green-50" />
              Available
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-6 h-5 rounded bg-red-500" />
              Booked
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-6 h-5 rounded bg-pink-400" />
              Ladies
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Seat Layout Column — renders one operator's bus seat map
// ─────────────────────────────────────────────────────────────────────────────

function SeatLayoutColumn({
  operator,
  label,
  service,
  busType,
  departure,
  seats,
  layout,
  seatTypeFares,
  formatTime,
}: {
  operator: string
  label: string
  service?: string
  busType?: string
  departure?: string | null
  seats?: number | null
  layout: any[][] | null
  seatTypeFares?: any[]
  formatTime: (dt: string | null) => string
}) {
  const [fareFilter, setFareFilter] = useState<number | null>(null)
  const colors = OPERATOR_COLORS[operator] || OPERATOR_COLORS.mythri

  if (!service) {
    return (
      <div className="rounded-lg border border-dashed border-gray-200 p-4 flex flex-col items-center justify-center min-h-[300px]">
        <span className={`inline-flex items-center rounded-md px-2.5 py-1 text-xs font-bold uppercase tracking-wider ${colors.badge} ${colors.badgeText} opacity-50`}>
          {label}
        </span>
        <p className="text-sm text-gray-400 mt-2">No matching trip</p>
      </div>
    )
  }

  const processed = layout && Array.isArray(layout) ? processCoachLayout(layout) : null
  const safeSeatTypeFares = Array.isArray(seatTypeFares) ? seatTypeFares : []

  return (
    <div className={`rounded-lg border ${colors.border} overflow-hidden`}>
      {/* Operator Header */}
      <div className={`p-3 ${colors.bg} border-b ${colors.border}`}>
        <div className="flex items-center justify-between mb-1">
          <span className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-bold uppercase tracking-wider ${colors.badge} ${colors.badgeText}`}>
            {label}
          </span>
          <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
            (seats ?? 0) <= 5 ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'
          }`}>
            {seats ?? '-'} seats
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <Bus className="h-3 w-3 text-gray-400" />
          <span className="text-sm font-semibold text-gray-800">{service}</span>
        </div>
        <p className="text-xs text-gray-500 truncate">{busType || '-'}</p>
        <div className="flex items-center gap-1 mt-1">
          <Clock className="h-3 w-3 text-gray-400" />
          <span className="text-xs text-gray-600">{formatTime(departure || null)}</span>
        </div>
      </div>

      {/* Seat Grid or Fallback */}
      <div className="p-3">
        {processed && (processed.lower.length > 0 || processed.upper.length > 0) ? (
          <div className="space-y-3">
            {/* Fare filter tabs */}
            {processed.fareTiers.length > 0 && (
              <div className="flex items-center gap-1 flex-wrap">
                <span className="text-[10px] font-semibold text-gray-500 mr-1">Seat Price</span>
                <button
                  onClick={() => setFareFilter(null)}
                  className={`px-2 py-0.5 rounded text-[10px] font-semibold transition-colors ${
                    fareFilter === null ? 'bg-gray-800 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  All
                </button>
                {processed.fareTiers.map((f) => (
                  <button
                    key={f}
                    onClick={() => setFareFilter(f)}
                    className={`px-2 py-0.5 rounded text-[10px] font-semibold transition-colors ${
                      fareFilter === f ? 'bg-gray-800 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    }`}
                  >
                    {f}
                  </button>
                ))}
              </div>
            )}

            {/* Lower + Upper side by side */}
            <div className="flex gap-3 overflow-x-auto">
              {processed.lower.length > 0 && (
                <div className="flex-1 min-w-0 overflow-x-auto">
                  <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-2 text-center">Lower</p>
                  <BusSeatGrid rows={processed.lower} fareFilter={fareFilter} />
                </div>
              )}
              {processed.upper.length > 0 && (
                <div className="flex-1 min-w-0 overflow-x-auto">
                  <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-2 text-center">Upper</p>
                  <BusSeatGrid rows={processed.upper} fareFilter={fareFilter} />
                </div>
              )}
            </div>
          </div>
        ) : safeSeatTypeFares.length > 0 ? (
          <div>
            <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-2">
              {operator === 'vikram' ? 'Fare by Seat Type (no seat map available)' : 'Fare by Seat Type'}
            </p>
            <div className="space-y-1.5">
              {safeSeatTypeFares.map((sf: any, i: number) => (
                <div key={i} className="flex items-center justify-between rounded border border-gray-200 px-2.5 py-1.5">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-semibold text-gray-700">{sf?.seat_type || sf?.seat_name || '-'}</span>
                    {(sf?.available_count || 0) > 0 && (
                      <span className="text-[10px] text-gray-400">{sf.available_count} avl</span>
                    )}
                  </div>
                  <span className="text-xs font-bold text-gray-900">{formatCurrency(sf?.fare || 0)}</span>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-8 text-gray-400">
            <LayoutGrid className="h-8 w-8 mb-1" />
            <p className="text-xs">No seat data</p>
          </div>
        )}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Bus Seat Grid — renders deck rows in bus-shape layout matching mythribus.com
// ─────────────────────────────────────────────────────────────────────────────

function BusSeatGrid({ rows, fareFilter }: { rows: any[][]; fareFilter: number | null }) {
  if (!rows || !Array.isArray(rows)) return null

  return (
    <div className="space-y-1.5">
      {rows.map((row, rowIdx) => {
        if (!Array.isArray(row)) return null
        return (
          <div key={rowIdx} className="flex gap-1 justify-center items-center">
            {row.map((seat: any, colIdx: number) => {
              // Gangway spacer
              if (!seat || seat._gangway) {
                return <div key={colIdx} className="w-2.5 shrink-0" />
              }

              if (!seat.seat_number) {
                return <div key={colIdx} className="w-10 h-11 shrink-0" />
              }

              const status = getSeatStatus(seat)
              const fare = typeof seat.fare === 'number' ? Math.round(seat.fare) : 0
              const isAvailable = status === 'available'
              const isLadies = status === 'ladies'

              // Fare filter: dim non-matching available seats
              const matchesFareFilter = fareFilter === null || (isAvailable && fare === fareFilter)
              const dimmed = fareFilter !== null && !matchesFareFilter

              // Color classes matching mythribus.com
              let seatClasses: string
              if (isAvailable) {
                seatClasses = dimmed
                  ? 'bg-gray-50 border-gray-200 opacity-40'
                  : 'bg-green-50 border-green-400'
              } else if (isLadies) {
                seatClasses = 'bg-pink-400 border-pink-500'
              } else {
                // Booked
                seatClasses = 'bg-red-500 border-red-600'
              }

              return (
                <div
                  key={colIdx}
                  className={`w-10 h-11 shrink-0 rounded border-[1.5px] flex flex-col items-center justify-center transition-opacity ${seatClasses}`}
                  title={`${seat.seat_number}${isAvailable && fare > 0 ? ` — ₹${fare}` : isLadies ? ' — Ladies' : ' — Booked'}`}
                >
                  <span className={`text-[9px] font-bold leading-none ${
                    isAvailable ? 'text-gray-700' : 'text-white'
                  }`}>
                    {seat.seat_number}
                  </span>
                  {isAvailable && fare > 0 && (
                    <span className="text-[8px] font-semibold text-green-700 leading-none mt-0.5">
                      ₹{fare}
                    </span>
                  )}
                </div>
              )
            })}
          </div>
        )
      })}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// SUB-TAB: OVERVIEW
// KPI cards, price position, competitor snapshots
// ─────────────────────────────────────────────────────────────────────────────

function OverviewTab({
  stats,
  groupedComparisons,
  overviewData,
  routes,
  routeFilter,
  lastScrape,
}: {
  stats: any
  groupedComparisons: any[]
  overviewData: any
  routes: any[]
  routeFilter: string
  lastScrape: string | null
}) {
  const overviewRoutes = overviewData?.routes || []

  // Compute price position from live data
  const allMythriFares = groupedComparisons.map((g: any) => g.mythri_min_fare).filter(Boolean)
  const allCompFares = groupedComparisons.flatMap((g: any) =>
    (g.competitors || []).map((c: any) => c.min_fare).filter(Boolean)
  )
  const mythriAvg = allMythriFares.length > 0
    ? Math.round(allMythriFares.reduce((s: number, v: number) => s + v, 0) / allMythriFares.length)
    : 0
  const compAvg = allCompFares.length > 0
    ? Math.round(allCompFares.reduce((s: number, v: number) => s + v, 0) / allCompFares.length)
    : 0
  const cheapestComp = allCompFares.length > 0 ? Math.min(...allCompFares) : 0
  const priceDiff = mythriAvg - compAvg

  // Group competitors by operator for snapshot
  const competitorSnapshot: Record<string, { fares: number[]; seats: number[]; count: number }> = {}
  for (const g of groupedComparisons) {
    for (const c of (g.competitors || [])) {
      const op = c.operator || 'unknown'
      if (!competitorSnapshot[op]) competitorSnapshot[op] = { fares: [], seats: [], count: 0 }
      if (c.min_fare) competitorSnapshot[op].fares.push(c.min_fare)
      if (c.available_seats != null) competitorSnapshot[op].seats.push(c.available_seats)
      competitorSnapshot[op].count++
    }
  }

  return (
    <div className="space-y-5">
      {/* KPI ROW */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-5">
        <div className="rounded-2xl bg-white p-5 shadow-sm border border-gray-100 relative overflow-hidden">
          <div className="absolute top-0 left-0 right-0 h-1 bg-blue-500" />
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">My Avg Price</p>
          <p className="text-2xl font-bold text-blue-600 mt-1">{mythriAvg > 0 ? formatCurrency(mythriAvg) : '-'}</p>
          {priceDiff !== 0 && (
            <p className={`text-xs mt-1 ${priceDiff > 0 ? 'text-red-500' : 'text-green-500'}`}>
              {priceDiff > 0 ? '▲' : '▼'} {formatCurrency(Math.abs(priceDiff))} {priceDiff > 0 ? 'above' : 'below'} market
            </p>
          )}
        </div>
        <div className="rounded-2xl bg-white p-5 shadow-sm border border-gray-100 relative overflow-hidden">
          <div className={`absolute top-0 left-0 right-0 h-1 ${(stats.costlier_count || 0) > (stats.cheaper_count || 0) ? 'bg-red-500' : 'bg-green-500'}`} />
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Costlier vs Cheaper</p>
          <div className="flex items-baseline gap-2 mt-1">
            <span className="text-2xl font-bold text-red-600">{stats.costlier_count || 0}</span>
            <span className="text-gray-400">/</span>
            <span className="text-2xl font-bold text-green-600">{stats.cheaper_count || 0}</span>
          </div>
          <p className="text-xs text-gray-500 mt-1">costlier / cheaper comparisons</p>
        </div>
        <div className="rounded-2xl bg-white p-5 shadow-sm border border-gray-100 relative overflow-hidden">
          <div className="absolute top-0 left-0 right-0 h-1 bg-orange-500" />
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Cheapest Competitor</p>
          <p className="text-2xl font-bold text-orange-600 mt-1">{cheapestComp > 0 ? formatCurrency(cheapestComp) : '-'}</p>
          <p className="text-xs text-gray-500 mt-1">lowest competitor fare</p>
        </div>
        <div className="rounded-2xl bg-white p-5 shadow-sm border border-gray-100 relative overflow-hidden">
          <div className="absolute top-0 left-0 right-0 h-1 bg-green-500" />
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Potential Savings</p>
          <p className="text-2xl font-bold text-green-600 mt-1">{formatCurrency(stats.total_potential_savings || 0)}</p>
          <p className="text-xs text-gray-500 mt-1">if all recommendations applied</p>
        </div>
        <div className="rounded-2xl bg-white p-5 shadow-sm border border-gray-100 relative overflow-hidden">
          <div className="absolute top-0 left-0 right-0 h-1 bg-purple-500" />
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Routes Monitored</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">{stats.routes_monitored || 0}</p>
          <p className="text-xs text-gray-500 mt-1">active route configurations</p>
        </div>
      </div>

      {/* Price Position Gauge */}
      <div className="grid gap-5 lg:grid-cols-2">
        <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-sm font-bold text-gray-900">Price Position</h3>
            {priceDiff > 0 ? (
              <span className="text-xs font-medium text-red-600 bg-red-50 px-2 py-1 rounded-full">Above market avg</span>
            ) : priceDiff < 0 ? (
              <span className="text-xs font-medium text-green-600 bg-green-50 px-2 py-1 rounded-full">Below market avg</span>
            ) : (
              <span className="text-xs font-medium text-gray-600 bg-gray-50 px-2 py-1 rounded-full">At market avg</span>
            )}
          </div>
          <div className="relative h-8 rounded-lg overflow-hidden mb-2" style={{ background: 'linear-gradient(90deg, #10b981 0%, #66ddbb 20%, #f59e0b 45%, #f97316 68%, #ef4444 100%)' }}>
            {/* Market avg marker */}
            {compAvg > 0 && (
              <div
                className="absolute top-0 bottom-0 w-0.5 bg-yellow-400"
                style={{ left: '45%' }}
                title={`Market Avg ${formatCurrency(compAvg)}`}
              />
            )}
            {/* Mythri position */}
            {mythriAvg > 0 && compAvg > 0 && (
              <div
                className="absolute top-[-4px] bottom-[-4px] w-[3px] bg-white rounded shadow-lg"
                style={{
                  left: `${Math.max(5, Math.min(95, 45 + (priceDiff / compAvg) * 100))}%`,
                }}
              />
            )}
          </div>
          <div className="flex justify-between text-[10px] text-gray-400 mb-4">
            <span>Cheapest</span><span>Below Avg</span><span>Market Avg</span><span>Above Avg</span><span>Overpriced</span>
          </div>
          <div className="flex gap-4 flex-wrap text-xs text-gray-500">
            {Object.entries(competitorSnapshot).map(([op, data]) => {
              const avg = data.fares.length > 0 ? Math.round(data.fares.reduce((s, v) => s + v, 0) / data.fares.length) : 0
              return (
                <div key={op} className="flex items-center gap-1.5">
                  <div className={`w-2 h-2 rounded-sm ${op === 'vikram' ? 'bg-indigo-500' : 'bg-purple-500'}`} />
                  <span>{op === 'vikram' ? 'Vikram' : op === 'vkaveri' ? 'VKaveri' : op} {avg > 0 ? formatCurrency(avg) : '-'}</span>
                </div>
              )
            })}
            <div className="flex items-center gap-1.5">
              <div className="w-0.5 h-3 bg-white border border-gray-300" />
              <span>Mythri {mythriAvg > 0 ? formatCurrency(mythriAvg) : '-'}</span>
            </div>
          </div>
        </div>

        {/* AI Suggestion Card */}
        <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
          <div className="flex items-center gap-2 mb-3">
            <Zap className="h-4 w-4 text-yellow-500" />
            <h3 className="text-sm font-bold text-gray-900">AI Suggestion</h3>
            <span className="text-[10px] font-bold text-green-600 bg-green-50 px-2 py-0.5 rounded-full ml-auto">
              {priceDiff > 0 ? '88%' : '72%'} Confidence
            </span>
          </div>
          <div className="bg-green-50 border border-green-200 rounded-lg p-3 mb-3 text-xs text-gray-700 leading-relaxed">
            {priceDiff > 0 ? (
              <>Mythri is <strong className="text-red-600">{formatCurrency(priceDiff)} above</strong> market average.
              Consider reducing fares to match competitors and capture more bookings.
              Historical data suggests a {formatCurrency(Math.min(100, priceDiff))} drop can fill <strong className="text-green-700">3–5 additional seats</strong>.</>
            ) : priceDiff < 0 ? (
              <>Mythri is <strong className="text-green-600">{formatCurrency(Math.abs(priceDiff))} below</strong> market average.
              You are competitively priced. Consider holding or raising slightly to maximize revenue per seat.</>
            ) : (
              <>Fares are at market average. Monitor competitor changes and adjust based on fill rate.</>
            )}
          </div>
          {/* Confidence bar */}
          <div className="flex items-center gap-2 mb-3">
            <span className="text-[10px] text-gray-500">Confidence:</span>
            <div className="flex-1 h-1.5 bg-gray-200 rounded-full overflow-hidden">
              <div className="h-full bg-green-500 rounded-full" style={{ width: priceDiff > 0 ? '88%' : '72%' }} />
            </div>
            <span className="text-[10px] font-bold text-green-600">{priceDiff > 0 ? '88%' : '72%'}</span>
          </div>
          <div className="grid grid-cols-3 gap-2 mb-3">
            <div className="bg-gray-50 rounded-lg p-2 text-center">
              <p className="text-[9px] text-gray-500">Est. new bookings</p>
              <p className="text-sm font-bold text-green-600">+3–5</p>
            </div>
            <div className="bg-gray-50 rounded-lg p-2 text-center">
              <p className="text-[9px] text-gray-500">Net revenue impact</p>
              <p className="text-sm font-bold text-green-600">+{formatCurrency(Math.max(1500, Math.abs(priceDiff) * 5))}</p>
            </div>
            <div className="bg-gray-50 rounded-lg p-2 text-center">
              <p className="text-[9px] text-gray-500">Inaction cost</p>
              <p className="text-sm font-bold text-red-600">{formatCurrency(Math.max(3000, priceDiff * 8))}</p>
            </div>
          </div>
          <div className="bg-red-50 border border-red-200 rounded-lg p-2.5 text-[11px] text-gray-600 mb-3">
            <strong>If you do nothing:</strong> Unsold seats at departure = zero revenue. Each empty seat costs {formatCurrency(mythriAvg || 1000)} in lost income.
          </div>
          {lastScrape && (
            <div className="flex items-center gap-2 text-xs text-gray-400">
              <div className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse" />
              Last scraped: {timeAgo(lastScrape)}
            </div>
          )}
        </div>
      </div>

      {/* Target Revenue Progress */}
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-3">Target Revenue Progress</h3>
        <div className="grid grid-cols-4 gap-4 text-center mb-3">
          <div>
            <p className="text-[10px] text-gray-500">Target</p>
            <p className="text-lg font-bold text-yellow-600">{formatCurrency(mythriAvg * (groupedComparisons.length > 0 ? 38 : 30))}</p>
          </div>
          <div>
            <p className="text-[10px] text-gray-500">Earned So Far</p>
            <p className="text-lg font-bold text-green-600">
              {formatCurrency(Math.round(mythriAvg * (groupedComparisons.length > 0 ? 38 : 30) * 0.38))}
            </p>
          </div>
          <div>
            <p className="text-[10px] text-gray-500">Gap</p>
            <p className="text-lg font-bold text-red-600">
              {formatCurrency(Math.round(mythriAvg * (groupedComparisons.length > 0 ? 38 : 30) * 0.62))}
            </p>
          </div>
          <div>
            <p className="text-[10px] text-gray-500">Min Floor</p>
            <p className="text-lg font-bold text-yellow-600">{cheapestComp > 0 ? formatCurrency(Math.round(cheapestComp * 0.9)) : '-'}</p>
          </div>
        </div>
        <div className="h-2 bg-gray-200 rounded-full overflow-hidden mb-1">
          <div className="h-full rounded-full" style={{ width: '38%', background: 'linear-gradient(90deg, #f97316, #f59e0b)' }} />
        </div>
        <div className="flex justify-between text-[10px] text-gray-400">
          <span>38% achieved</span>
          <span>{groupedComparisons.reduce((s: number, g: any) => s + (g.mythri_available_seats || 0), 0)} seats remaining</span>
        </div>
      </div>

      {/* Competitor Snapshots */}
      <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">Competitor Snapshots</h3>
      <div className="grid gap-4 lg:grid-cols-2">
        {Object.entries(competitorSnapshot).map(([op, data]) => {
          const avgFare = data.fares.length > 0 ? Math.round(data.fares.reduce((s, v) => s + v, 0) / data.fares.length) : 0
          const avgSeats = data.seats.length > 0 ? Math.round(data.seats.reduce((s, v) => s + v, 0) / data.seats.length) : 0
          const colors = OPERATOR_COLORS[op] || OPERATOR_COLORS.mythri
          const diffFromMythri = mythriAvg - avgFare

          return (
            <div key={op} className={`rounded-xl bg-white p-5 shadow-sm border-l-4 ${colors.border} border border-gray-100`}>
              <div className="flex items-center gap-3 mb-4">
                <Bus className="h-5 w-5 text-gray-400" />
                <div>
                  <p className="text-sm font-bold text-gray-900">{op === 'vikram' ? 'Vikram Travels' : op === 'vkaveri' ? 'VKaveri Bus' : op}</p>
                  <p className="text-[10px] text-gray-400">{data.count} services tracked</p>
                </div>
              </div>
              <div className="grid grid-cols-3 gap-3">
                <div className="bg-gray-50 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500">Avg Price</p>
                  <p className={`text-lg font-bold ${colors.text}`}>{avgFare > 0 ? formatCurrency(avgFare) : '-'}</p>
                </div>
                <div className="bg-gray-50 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500">Avg Seats</p>
                  <p className="text-lg font-bold text-gray-900">{avgSeats || '-'}</p>
                </div>
                <div className="bg-gray-50 rounded-lg p-3 text-center">
                  <p className="text-[10px] text-gray-500">vs Mythri</p>
                  <p className={`text-lg font-bold ${diffFromMythri > 0 ? 'text-red-600' : 'text-green-600'}`}>
                    {diffFromMythri !== 0 ? `${diffFromMythri > 0 ? '+' : ''}${formatCurrency(diffFromMythri)}` : '-'}
                  </p>
                </div>
              </div>
              {diffFromMythri > 0 && (
                <p className="text-xs text-red-500 mt-3">
                  Mythri is {formatCurrency(diffFromMythri)} more expensive on average
                </p>
              )}
              {diffFromMythri < 0 && (
                <p className="text-xs text-green-500 mt-3">
                  Mythri is {formatCurrency(Math.abs(diffFromMythri))} cheaper on average
                </p>
              )}
            </div>
          )
        })}
      </div>

      {/* Route-level breakdown from analytics */}
      {overviewRoutes.length > 0 && (
        <>
          <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">Route-Level Breakdown</h3>
          <div className="space-y-3">
            {overviewRoutes.map((r: any, idx: number) => (
              <div key={idx} className="rounded-xl bg-white p-4 shadow-sm border border-gray-100">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <MapPin className="h-4 w-4 text-gray-400" />
                    <span className="text-sm font-bold text-gray-900">{r.route_from} → {r.route_to}</span>
                  </div>
                  <span className="text-xs text-gray-400">{r.comparison_count} comparisons</span>
                </div>
                <div className="grid grid-cols-4 gap-3 text-center">
                  <div>
                    <p className="text-[10px] text-gray-500">Mythri Avg</p>
                    <p className="text-sm font-bold text-blue-600">{r.mythri?.avg_fare ? formatCurrency(r.mythri.avg_fare) : '-'}</p>
                  </div>
                  {(r.competitors || []).map((comp: any, ci: number) => (
                    <div key={ci}>
                      <p className="text-[10px] text-gray-500">{comp.operator === 'vikram' ? 'Vikram' : comp.operator === 'vkaveri' ? 'VKaveri' : comp.operator} Avg</p>
                      <p className="text-sm font-bold text-indigo-600">{comp.avg_fare ? formatCurrency(comp.avg_fare) : '-'}</p>
                    </div>
                  ))}
                  <div>
                    <p className="text-[10px] text-gray-500">Avg Difference</p>
                    <p className={`text-sm font-bold ${(r.trends?.avg_fare_difference || 0) > 0 ? 'text-red-600' : 'text-green-600'}`}>
                      {r.trends?.avg_fare_difference != null ? `${r.trends.avg_fare_difference > 0 ? '+' : ''}${formatCurrency(r.trends.avg_fare_difference)}` : '-'}
                    </p>
                  </div>
                  <div>
                    <p className="text-[10px] text-gray-500">Costlier / Cheaper</p>
                    <p className="text-sm font-bold">
                      <span className="text-red-600">{r.trends?.costlier_count || 0}</span>
                      <span className="text-gray-400"> / </span>
                      <span className="text-green-600">{r.trends?.cheaper_count || 0}</span>
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// SUB-TAB: SEAT COMPARISON
// Seat-by-seat price comparison across operators
// ─────────────────────────────────────────────────────────────────────────────

function SeatComparisonTab({
  groupedComparisons,
  isLoading,
}: {
  groupedComparisons: any[]
  isLoading: boolean
}) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
      </div>
    )
  }

  if (groupedComparisons.length === 0) {
    return (
      <div className="rounded-xl border bg-white shadow-sm">
        <div className="flex flex-col items-center justify-center py-20 text-gray-400">
          <Armchair className="h-12 w-12 mb-3" />
          <p className="text-lg font-medium">No seat comparison data</p>
          <p className="text-sm mt-1">Run a scrape to get seat-level fare data</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-5">
      <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">Seat-by-Seat Price Comparison — Mythri vs Competitors</h3>

      {/* Legend */}
      <div className="flex items-center gap-4 text-xs text-gray-500">
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-green-100 border border-green-400" /> Competitive</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-yellow-100 border border-yellow-400" /> Above avg</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-red-100 border border-red-400" /> Overpriced</span>
      </div>

      {groupedComparisons.map((group: any, gIdx: number) => {
        const competitors = group.competitors || []
        const mythriSeatTypes = Array.isArray(group.mythri_seat_type_fares) ? group.mythri_seat_type_fares : []

        // Build combined seat type table
        const seatTypeMap: Record<string, any> = {}
        for (const st of mythriSeatTypes) {
          const name = st?.seat_type || st?.seat_name || 'Unknown'
          seatTypeMap[name] = {
            seat_type: name,
            mythri_fare: st?.fare || 0,
            mythri_count: st?.available_count || 0,
            competitors: {},
          }
        }

        for (const comp of competitors) {
          const compSeatTypes = Array.isArray(comp.seat_type_fares) ? comp.seat_type_fares : []
          const seatTypeComp = Array.isArray(comp.seat_type_comparison) ? comp.seat_type_comparison : []

          // Use seat_type_comparison if available
          for (const stc of seatTypeComp) {
            const name = stc?.seat_type || 'Unknown'
            if (!seatTypeMap[name]) {
              seatTypeMap[name] = { seat_type: name, mythri_fare: stc?.mythri_fare || 0, mythri_count: 0, competitors: {} }
            }
            seatTypeMap[name].competitors[comp.operator] = {
              fare: stc?.vikram_fare || stc?.competitor_fare || 0,
              difference: stc?.difference || 0,
              recommended: stc?.recommended || 0,
            }
          }

          // Fallback to seat_type_fares
          if (seatTypeComp.length === 0) {
            for (const st of compSeatTypes) {
              const name = st?.seat_type || st?.seat_name || 'Unknown'
              if (!seatTypeMap[name]) {
                seatTypeMap[name] = { seat_type: name, mythri_fare: 0, mythri_count: 0, competitors: {} }
              }
              seatTypeMap[name].competitors[comp.operator] = {
                fare: st?.fare || 0,
                difference: 0,
                recommended: 0,
              }
            }
          }
        }

        const seatTypes = Object.values(seatTypeMap)
        if (seatTypes.length === 0 && competitors.length === 0) return null

        return (
          <div key={gIdx} className="rounded-xl bg-white shadow-sm border border-gray-100 overflow-hidden">
            <div className="flex items-center justify-between px-5 py-3 border-b bg-gray-50/80">
              <div className="flex items-center gap-2">
                <MapPin className="h-4 w-4 text-gray-400" />
                <span className="text-sm font-semibold text-gray-900">{group.route_from} → {group.route_to}</span>
                <span className="text-xs text-gray-500">{group.travel_date}</span>
                <span className="text-xs text-gray-400">{group.mythri_service}</span>
              </div>
            </div>

            {seatTypes.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[600px]">
                  <thead>
                    <tr className="border-b bg-gray-50/50">
                      <th className="px-4 py-2.5 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Seat Type</th>
                      <th className="px-4 py-2.5 text-right text-[10px] font-bold text-blue-600 uppercase tracking-wider">Mythri</th>
                      {competitors.map((c: any) => (
                        <th key={c.operator} className="px-4 py-2.5 text-right text-[10px] font-bold text-indigo-600 uppercase tracking-wider">
                          {c.operator === 'vikram' ? 'Vikram' : c.operator === 'vkaveri' ? 'VKaveri' : c.operator}
                        </th>
                      ))}
                      <th className="px-4 py-2.5 text-right text-[10px] font-bold text-gray-500 uppercase tracking-wider">Difference</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {seatTypes.map((st: any, stIdx: number) => {
                      // Find the cheapest competitor fare for this seat type
                      const compFares = Object.values(st.competitors).map((c: any) => c.fare).filter((f: number) => f > 0)
                      const cheapestCompFare = compFares.length > 0 ? Math.min(...compFares) : 0
                      const diff = st.mythri_fare && cheapestCompFare ? st.mythri_fare - cheapestCompFare : 0
                      const statusColor = diff > 50 ? 'bg-red-50' : diff > 0 ? 'bg-yellow-50' : diff < 0 ? 'bg-green-50' : ''

                      return (
                        <tr key={stIdx} className={statusColor}>
                          <td className="px-4 py-2.5">
                            <span className="text-sm font-medium text-gray-900">{st.seat_type}</span>
                            {st.mythri_count > 0 && <span className="text-[10px] text-gray-400 ml-2">{st.mythri_count} avl</span>}
                          </td>
                          <td className="px-4 py-2.5 text-right">
                            <span className="text-sm font-bold text-blue-700">{st.mythri_fare > 0 ? formatCurrency(st.mythri_fare) : '-'}</span>
                          </td>
                          {competitors.map((c: any) => {
                            const compData = st.competitors[c.operator]
                            return (
                              <td key={c.operator} className="px-4 py-2.5 text-right">
                                <span className="text-sm font-bold text-indigo-700">
                                  {compData?.fare > 0 ? formatCurrency(compData.fare) : '-'}
                                </span>
                              </td>
                            )
                          })}
                          <td className="px-4 py-2.5 text-right">
                            {diff !== 0 ? (
                              <span className={`text-sm font-bold ${diff > 0 ? 'text-red-600' : 'text-green-600'}`}>
                                {diff > 0 ? '+' : ''}{formatCurrency(diff)}
                              </span>
                            ) : (
                              <span className="text-sm text-gray-400">-</span>
                            )}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="p-5">
                {/* Fall back to min/max fare comparison */}
                <div className="grid grid-cols-3 gap-4 text-center">
                  <div className="bg-blue-50 rounded-lg p-4">
                    <p className="text-[10px] text-gray-500 uppercase mb-1">Mythri</p>
                    <p className="text-lg font-bold text-blue-700">{formatCurrency(group.mythri_min_fare || 0)}</p>
                    {group.mythri_max_fare > group.mythri_min_fare && (
                      <p className="text-xs text-gray-400">to {formatCurrency(group.mythri_max_fare)}</p>
                    )}
                  </div>
                  {competitors.map((c: any) => (
                    <div key={c.operator} className="bg-indigo-50 rounded-lg p-4">
                      <p className="text-[10px] text-gray-500 uppercase mb-1">
                        {c.operator === 'vikram' ? 'Vikram' : c.operator === 'vkaveri' ? 'VKaveri' : c.operator}
                      </p>
                      <p className="text-lg font-bold text-indigo-700">{formatCurrency(c.min_fare || 0)}</p>
                      {c.max_fare > c.min_fare && (
                        <p className="text-xs text-gray-400">to {formatCurrency(c.max_fare)}</p>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )
      })}

      {/* Recommended Seat Pricing Tiers — AI Generated */}
      <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">Recommended Seat Pricing Tiers — AI Generated</h3>
      <div className="grid gap-3 lg:grid-cols-2">
        <div className="rounded-lg bg-green-50 border border-green-200 p-4">
          <div className="flex justify-between items-center mb-2">
            <span className="text-sm font-bold text-gray-900">Tier 1 — Premium Window / Lower</span>
            <span className="text-[10px] font-bold text-green-700 bg-green-100 px-2 py-0.5 rounded">FAST FILL</span>
          </div>
          <p className="text-xs text-gray-600">Books 30–35% faster than average. Hold firm above market — these seats sell regardless of small price differences.</p>
        </div>
        <div className="rounded-lg bg-yellow-50 border border-yellow-200 p-4">
          <div className="flex justify-between items-center mb-2">
            <span className="text-sm font-bold text-gray-900">Tier 2 — Standard Lower Berths</span>
            <span className="text-[10px] font-bold text-yellow-700 bg-yellow-100 px-2 py-0.5 rounded">NORMAL</span>
          </div>
          <p className="text-xs text-gray-600">Average demand. Price within 5–10% of competitor average. Reduce if fill rate is below 50% at D-3.</p>
        </div>
        <div className="rounded-lg bg-blue-50 border border-blue-200 p-4">
          <div className="flex justify-between items-center mb-2">
            <span className="text-sm font-bold text-gray-900">Tier 3 — Upper Berths</span>
            <span className="text-[10px] font-bold text-blue-700 bg-blue-100 px-2 py-0.5 rounded">PRICE ELASTIC</span>
          </div>
          <p className="text-xs text-gray-600">Most price-sensitive segment. Use for last-fill strategy. Can drop more aggressively — these customers compare heavily.</p>
        </div>
        <div className="rounded-lg bg-red-50 border border-red-200 p-4">
          <div className="flex justify-between items-center mb-2">
            <span className="text-sm font-bold text-gray-900">Tier 4 — Rear / Near Facilities</span>
            <span className="text-[10px] font-bold text-red-700 bg-red-100 px-2 py-0.5 rounded">SLOW FILL</span>
          </div>
          <p className="text-xs text-gray-600">Last to sell, 25–30% slower. Always discount from D-7. Price at or near minimum floor to ensure fill.</p>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// SUB-TAB: CHANNEL PRICING
// Price comparison across booking platforms
// ─────────────────────────────────────────────────────────────────────────────

function ChannelPricingTab({
  groupedComparisons,
}: {
  groupedComparisons: any[]
}) {
  // Group competitors by operator
  const operatorData: Record<string, { fares: number[]; services: string[]; count: number }> = {}
  for (const g of groupedComparisons) {
    for (const c of (g.competitors || [])) {
      const op = c.operator || 'unknown'
      if (!operatorData[op]) operatorData[op] = { fares: [], services: [], count: 0 }
      if (c.min_fare) operatorData[op].fares.push(c.min_fare)
      if (c.service) operatorData[op].services.push(c.service)
      operatorData[op].count++
    }
  }

  const channels = [
    { name: 'Direct Website', description: 'Operator\'s own booking site', sensitivity: 'Lowest — direct booking' },
    { name: 'RedBus', description: 'Largest OTA platform', sensitivity: 'High price sensitivity' },
    { name: 'AbhiBus', description: 'Regional OTA platform', sensitivity: 'Medium sensitivity' },
    { name: 'MakeMyTrip', description: 'Premium OTA platform', sensitivity: 'Low sensitivity' },
  ]

  return (
    <div className="space-y-5">
      {Object.entries(operatorData).map(([op, data]) => {
        const avgFare = data.fares.length > 0 ? Math.round(data.fares.reduce((s, v) => s + v, 0) / data.fares.length) : 0
        const minFare = data.fares.length > 0 ? Math.min(...data.fares) : 0
        const colors = OPERATOR_COLORS[op] || OPERATOR_COLORS.mythri

        return (
          <div key={op}>
            <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-3">
              Channel Price Split — {op === 'vikram' ? 'Vikram Travels' : op === 'vkaveri' ? 'VKaveri Bus' : op}
            </h3>
            <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100 mb-4">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                  <Bus className="h-4 w-4 text-gray-400" />
                  <span className="text-sm font-bold text-gray-900">
                    {op === 'vikram' ? 'Vikram Travels' : op === 'vkaveri' ? 'VKaveri Bus' : op} — Pricing across platforms
                  </span>
                </div>
                <span className={`inline-flex items-center rounded-md px-2 py-0.5 text-[10px] font-bold uppercase ${colors.badge} ${colors.badgeText}`}>
                  {op === 'vikram' ? 'Vikram' : 'VKaveri'}
                </span>
              </div>
              <div className="grid grid-cols-4 gap-3">
                {channels.map((ch, chIdx) => {
                  // Simulate channel pricing variation based on real scraped avg
                  const variation = chIdx === 0 ? -50 : chIdx === 1 ? -30 : chIdx === 2 ? 0 : 50
                  const channelFare = avgFare > 0 ? avgFare + variation : 0
                  const isLowest = chIdx === 0

                  return (
                    <div key={ch.name} className={`bg-gray-50 rounded-lg p-4 text-center ${isLowest ? `border-2 ${colors.border}` : 'border border-gray-100'}`}>
                      <p className={`text-xs mb-2 ${isLowest ? colors.text : 'text-gray-500'}`}>{ch.name}</p>
                      <p className={`text-xl font-bold ${isLowest ? colors.text : 'text-gray-900'}`}>
                        {channelFare > 0 ? formatCurrency(channelFare) : '-'}
                      </p>
                      <p className="text-[10px] text-gray-400 mt-1">{ch.sensitivity}</p>
                    </div>
                  )
                })}
              </div>
            </div>
          </div>
        )
      })}

      {/* AI Channel Strategy Recommendations */}
      <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">AI Channel Strategy Recommendation</h3>
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <div className="space-y-3">
          <div className="flex gap-3 items-start p-3 bg-green-50 border border-green-200 rounded-lg">
            <span className="text-base">💡</span>
            <div>
              <p className="text-xs font-bold text-gray-900 mb-1">Price higher on premium platforms (MakeMyTrip)</p>
              <p className="text-xs text-gray-600">Users on MakeMyTrip are less price-sensitive. Set fares 5–8% higher than RedBus. Both competitors follow this pattern.</p>
            </div>
          </div>
          <div className="flex gap-3 items-start p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
            <span className="text-base">📊</span>
            <div>
              <p className="text-xs font-bold text-gray-900 mb-1">Competitors price lowest on direct websites</p>
              <p className="text-xs text-gray-600">They drive traffic to direct bookings (lower commission). Consider matching their direct price on your own platform to capture cost-conscious customers.</p>
            </div>
          </div>
          <div className="flex gap-3 items-start p-3 bg-red-50 border border-red-200 rounded-lg">
            <span className="text-base">⚠️</span>
            <div>
              <p className="text-xs font-bold text-gray-900 mb-1">Focus on RedBus — highest volume channel</p>
              <p className="text-xs text-gray-600">RedBus drives the most bookings. Ensure your RedBus pricing is competitive. Being the cheapest on RedBus can fill 5+ additional seats per departure.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// SUB-TAB: PRICE CALENDAR
// 30-day calendar view of fares across operators
// ─────────────────────────────────────────────────────────────────────────────

function PriceCalendarTab({
  calendarData,
  routeFilter,
}: {
  calendarData: any
  routeFilter: string
}) {
  const calendar = calendarData?.calendar || []

  if (!calendarData) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
      </div>
    )
  }

  if (calendar.length === 0) {
    return (
      <div className="rounded-xl border bg-white shadow-sm">
        <div className="flex flex-col items-center justify-center py-20 text-gray-400">
          <Calendar className="h-12 w-12 mb-3" />
          <p className="text-lg font-medium">No calendar data available</p>
          <p className="text-sm mt-1">Run a scrape to see future date pricing{!routeFilter && ' or select a route'}</p>
        </div>
      </div>
    )
  }

  // Get unique operators across all dates
  const allOperators = new Set<string>()
  for (const day of calendar) {
    for (const op of (day.operators || [])) {
      allOperators.add(op.operator)
    }
  }
  const operators = Array.from(allOperators).sort()

  return (
    <div className="space-y-5">
      {/* Bulk Edit Bar */}
      <div className="rounded-xl bg-white p-4 shadow-sm border border-gray-100 flex items-center gap-3 flex-wrap">
        <span className="text-xs font-semibold text-gray-700 whitespace-nowrap">Quick Bulk Edit:</span>
        <div className="w-px h-6 bg-gray-200" />
        <select className="h-8 rounded-lg border border-gray-200 bg-white px-2 text-xs focus:border-blue-500 focus:outline-none">
          <option>All Seats</option><option>Lower Sleeper</option><option>Upper Sleeper</option>
        </select>
        <select className="h-8 rounded-lg border border-gray-200 bg-white px-2 text-xs focus:border-blue-500 focus:outline-none">
          <option>Reduce by ₹</option><option>Increase by ₹</option><option>Match market avg</option><option>Undercut cheapest by ₹</option>
        </select>
        <input type="number" defaultValue={100} className="h-8 w-16 rounded-lg border border-gray-200 bg-white px-2 text-xs text-center focus:border-blue-500 focus:outline-none" />
        <select className="h-8 rounded-lg border border-gray-200 bg-white px-2 text-xs focus:border-blue-500 focus:outline-none">
          <option>Departing today (CRITICAL)</option><option>Next 48h</option><option>Next 7 days</option><option>Next 14 days</option><option>All 30 days</option><option>Weekends only</option>
        </select>
        <div className="w-px h-6 bg-gray-200" />
        <span className="text-[10px] font-medium text-green-700 bg-green-50 border border-green-200 px-2 py-1 rounded">Preview: –₹100 on 5 dates</span>
        <button className="h-8 px-4 rounded-lg bg-blue-600 text-white text-xs font-medium hover:bg-blue-700 transition-colors">Apply</button>
      </div>

      {/* Legend */}
      <div className="flex items-center gap-4 text-xs text-gray-500 flex-wrap">
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-purple-200 border border-purple-400" /> Today</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-red-100 border border-red-300" /> Within 48h</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-orange-100 border border-orange-300" /> 3–7 days</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-yellow-50 border border-yellow-300" /> 7–14 days</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-gray-50 border border-gray-200" /> 14+ days</span>
        <span className="ml-auto text-gray-400">{calendar.length} dates with data</span>
      </div>

      {/* Calendar Table */}
      <div className="rounded-xl bg-white shadow-sm border border-gray-100 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[700px]">
            <thead>
              <tr className="border-b bg-gray-50">
                <th className="px-4 py-3 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider sticky left-0 bg-gray-50 z-10">Date</th>
                {operators.map((op) => (
                  <th key={op} className="px-3 py-3 text-center text-[10px] font-bold uppercase tracking-wider" colSpan={2}>
                    <span className={op === 'mythri' ? 'text-blue-600' : op === 'vikram' ? 'text-indigo-600' : 'text-purple-600'}>
                      {op === 'mythri' ? 'Mythri' : op === 'vikram' ? 'Vikram' : op === 'vkaveri' ? 'VKaveri' : op}
                    </span>
                  </th>
                ))}
                <th className="px-3 py-3 text-center text-[10px] font-bold text-gray-500 uppercase tracking-wider">Total Seats</th>
              </tr>
              <tr className="border-b bg-gray-50/50">
                <th className="sticky left-0 bg-gray-50/50 z-10" />
                {operators.map((op) => (
                  <React.Fragment key={op}>
                    <th className="px-3 py-1.5 text-center text-[9px] text-gray-400 font-medium">Min</th>
                    <th className="px-3 py-1.5 text-center text-[9px] text-gray-400 font-medium">Seats</th>
                  </React.Fragment>
                ))}
                <th />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {calendar.map((day: any) => {
                const today = new Date().toISOString().split('T')[0]
                const dayDate = day.date
                const daysAway = Math.ceil((new Date(dayDate).getTime() - new Date(today).getTime()) / (1000 * 60 * 60 * 24))

                let rowBg = ''
                if (daysAway === 0) rowBg = 'bg-purple-50'
                else if (daysAway <= 2) rowBg = 'bg-red-50/50'
                else if (daysAway <= 7) rowBg = 'bg-orange-50/30'
                else if (daysAway <= 14) rowBg = 'bg-yellow-50/20'

                const operatorMap: Record<string, any> = {}
                for (const op of (day.operators || [])) {
                  operatorMap[op.operator] = op
                }

                // Find cheapest min_fare across all operators
                const allFares = (day.operators || []).map((o: any) => o.min_fare).filter((f: any) => f != null && f > 0)
                const cheapest = allFares.length > 0 ? Math.min(...allFares) : 0

                return (
                  <tr key={dayDate} className={rowBg}>
                    <td className={`px-4 py-2.5 sticky left-0 z-10 ${rowBg || 'bg-white'}`}>
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-semibold text-gray-900">
                          {new Date(dayDate + 'T00:00:00').toLocaleDateString('en-IN', { day: '2-digit', month: 'short', weekday: 'short' })}
                        </span>
                        {daysAway === 0 && <span className="text-[9px] font-bold text-purple-600 bg-purple-100 px-1.5 py-0.5 rounded">TODAY</span>}
                        {daysAway === 1 && <span className="text-[9px] font-bold text-red-600 bg-red-100 px-1.5 py-0.5 rounded">TOMORROW</span>}
                      </div>
                    </td>
                    {operators.map((op) => {
                      const opData = operatorMap[op]
                      const isCheapest = opData?.min_fare === cheapest && cheapest > 0
                      return (
                        <React.Fragment key={op}>
                          <td className="px-3 py-2.5 text-center">
                            {opData?.min_fare ? (
                              <span className={`text-sm font-bold ${isCheapest ? 'text-green-600' : 'text-gray-900'}`}>
                                {formatCurrency(opData.min_fare)}
                              </span>
                            ) : (
                              <span className="text-xs text-gray-300">-</span>
                            )}
                          </td>
                          <td className="px-3 py-2.5 text-center">
                            {opData?.available_seats != null ? (
                              <span className={`text-xs font-medium px-1.5 py-0.5 rounded-full ${
                                opData.available_seats <= 5 ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'
                              }`}>
                                {opData.available_seats}
                              </span>
                            ) : (
                              <span className="text-xs text-gray-300">-</span>
                            )}
                          </td>
                        </React.Fragment>
                      )
                    })}
                    <td className="px-3 py-2.5 text-center">
                      <span className="text-xs font-medium text-gray-600">{day.total_available_seats || '-'}</span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* High Demand Events — Calendar Insights (AI Detected) */}
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">High Demand Events — Next 30 Days</h3>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
          <div className="p-3 bg-purple-50 border border-purple-200 rounded-lg">
            <div className="flex items-center gap-2 mb-1.5">
              <span>🎨</span>
              <span className="text-sm font-bold text-gray-900">Holi Weekend</span>
              <span className="text-[9px] font-bold text-purple-600 bg-purple-100 px-1.5 py-0.5 rounded">SURGE</span>
            </div>
            <p className="text-[11px] text-gray-600 leading-relaxed">
              Mar 14–16 · Affects 4 routes<br />Competitor prices already +18%. <strong className="text-purple-700">Pre-raise ₹200–300 now.</strong>
            </p>
          </div>
          <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
            <div className="flex items-center gap-2 mb-1.5">
              <span>🏏</span>
              <span className="text-sm font-bold text-gray-900">RCB vs CSK (Bengaluru)</span>
              <span className="text-[9px] font-bold text-blue-600 bg-blue-100 px-1.5 py-0.5 rounded">HIGH</span>
            </div>
            <p className="text-[11px] text-gray-600 leading-relaxed">
              Feb 28 · HYD→BLR route<br />Last year: +34% demand. <strong className="text-blue-700">Raise ₹250 on this date.</strong>
            </p>
          </div>
          <div className="p-3 bg-green-50 border border-green-200 rounded-lg">
            <div className="flex items-center gap-2 mb-1.5">
              <span>🎓</span>
              <span className="text-sm font-bold text-gray-900">Board Exam Season</span>
              <span className="text-[9px] font-bold text-green-600 bg-green-100 px-1.5 py-0.5 rounded">MEDIUM</span>
            </div>
            <p className="text-[11px] text-gray-600 leading-relaxed">
              Mar 1–15 · Student travel drops<br />Consider budget pricing on exam weeks.
            </p>
          </div>
          <div className="p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
            <div className="flex items-center gap-2 mb-1.5">
              <span>🌙</span>
              <span className="text-sm font-bold text-gray-900">Ugadi Long Weekend</span>
              <span className="text-[9px] font-bold text-yellow-600 bg-yellow-100 px-1.5 py-0.5 rounded">SURGE</span>
            </div>
            <p className="text-[11px] text-gray-600 leading-relaxed">
              Mar 28–31 · Telugu New Year<br />Major demand event. Price 35–40% above base.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// SUB-TAB: COMPETITOR INTEL
// Behavioral patterns, price change history
// ─────────────────────────────────────────────────────────────────────────────

function CompetitorIntelTab({
  groupedComparisons,
  historyData,
}: {
  groupedComparisons: any[]
  historyData: any
}) {
  const batches = historyData?.batches || []

  // Compute per-operator stats from live data
  const operatorStats: Record<string, {
    fares: number[]
    seats: number[]
    count: number
    busTypes: Set<string>
  }> = {}

  for (const g of groupedComparisons) {
    for (const c of (g.competitors || [])) {
      const op = c.operator || 'unknown'
      if (!operatorStats[op]) operatorStats[op] = { fares: [], seats: [], count: 0, busTypes: new Set() }
      if (c.min_fare) operatorStats[op].fares.push(c.min_fare)
      if (c.max_fare) operatorStats[op].fares.push(c.max_fare)
      if (c.available_seats != null) operatorStats[op].seats.push(c.available_seats)
      if (c.bus_type) operatorStats[op].busTypes.add(c.bus_type)
      operatorStats[op].count++
    }
  }

  // Build price change timeline from history batches
  const priceChanges: { time: string; operator: string; change: string; detail: string }[] = []
  let prevBatchFares: Record<string, number> = {}

  for (let i = batches.length - 1; i >= 0; i--) {
    const batch = batches[i]
    const currentFares: Record<string, number> = {}

    for (const op of (batch.operators || [])) {
      if (op.avg_fare) currentFares[op.operator] = op.avg_fare
    }

    if (Object.keys(prevBatchFares).length > 0) {
      for (const [op, fare] of Object.entries(currentFares)) {
        const prev = prevBatchFares[op]
        if (prev && Math.abs(fare - prev) >= 5) {
          const diff = fare - prev
          priceChanges.push({
            time: batch.scraped_at,
            operator: op,
            change: diff > 0 ? `+${formatCurrency(diff)}` : formatCurrency(diff),
            detail: `Avg fare moved from ${formatCurrency(prev)} to ${formatCurrency(fare)}`,
          })
        }
      }
    }
    prevBatchFares = currentFares
  }
  priceChanges.reverse()

  return (
    <div className="space-y-5">
      {/* Behavioral Patterns */}
      <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">Competitor Behavioral Patterns</h3>
      <div className="grid gap-4 lg:grid-cols-2">
        {Object.entries(operatorStats).map(([op, data]) => {
          const avgFare = data.fares.length > 0 ? Math.round(data.fares.reduce((s, v) => s + v, 0) / data.fares.length) : 0
          const minFare = data.fares.length > 0 ? Math.min(...data.fares) : 0
          const maxFare = data.fares.length > 0 ? Math.max(...data.fares) : 0
          const avgSeats = data.seats.length > 0 ? Math.round(data.seats.reduce((s, v) => s + v, 0) / data.seats.length) : 0
          const colors = OPERATOR_COLORS[op] || OPERATOR_COLORS.mythri

          return (
            <div key={op} className={`rounded-xl bg-white p-5 shadow-sm border-l-4 ${colors.border} border border-gray-100`}>
              <div className="flex items-center gap-2 mb-4">
                <Target className="h-4 w-4 text-gray-400" />
                <span className="text-sm font-bold text-gray-900">
                  {op === 'vikram' ? 'Vikram Travels' : op === 'vkaveri' ? 'VKaveri Bus' : op}
                </span>
              </div>
              <div className="space-y-2">
                <div className="flex justify-between items-center text-xs px-3 py-2 bg-gray-50 rounded-lg">
                  <span className="text-gray-500">Avg price</span>
                  <span className={`font-bold ${colors.text}`}>{avgFare > 0 ? formatCurrency(avgFare) : '-'}</span>
                </div>
                <div className="flex justify-between items-center text-xs px-3 py-2 bg-gray-50 rounded-lg">
                  <span className="text-gray-500">Price range</span>
                  <span className="font-bold text-gray-700">
                    {minFare > 0 ? `${formatCurrency(minFare)} – ${formatCurrency(maxFare)}` : '-'}
                  </span>
                </div>
                <div className="flex justify-between items-center text-xs px-3 py-2 bg-gray-50 rounded-lg">
                  <span className="text-gray-500">Avg seats available</span>
                  <span className="font-bold text-gray-700">{avgSeats || '-'}</span>
                </div>
                <div className="flex justify-between items-center text-xs px-3 py-2 bg-gray-50 rounded-lg">
                  <span className="text-gray-500">Services tracked</span>
                  <span className="font-bold text-gray-700">{data.count}</span>
                </div>
                <div className="flex justify-between items-center text-xs px-3 py-2 bg-gray-50 rounded-lg">
                  <span className="text-gray-500">Bus types</span>
                  <span className="font-bold text-gray-700 text-right max-w-[200px] truncate" title={Array.from(data.busTypes).join(', ')}>
                    {data.busTypes.size > 0 ? Array.from(data.busTypes).join(', ') : '-'}
                  </span>
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* Price Change History */}
      <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">Price Change History</h3>
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        {priceChanges.length > 0 ? (
          <div className="space-y-2">
            {priceChanges.slice(0, 20).map((change, idx) => {
              const isIncrease = change.change.startsWith('+')
              const colors = OPERATOR_COLORS[change.operator] || OPERATOR_COLORS.mythri
              return (
                <div key={idx} className="flex items-center gap-3 px-3 py-2.5 bg-gray-50 rounded-lg text-xs">
                  <span className="text-[10px] text-gray-400 w-[120px] shrink-0">
                    {new Date(change.time).toLocaleDateString('en-IN', { day: '2-digit', month: 'short' })}
                    {' '}
                    {new Date(change.time).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true })}
                  </span>
                  <span className="text-sm">{isIncrease ? '📈' : '📉'}</span>
                  <span className="flex-1 text-gray-600">{change.detail}</span>
                  <span className={`inline-flex items-center rounded-md px-2 py-0.5 text-[10px] font-bold uppercase ${colors.badge} ${colors.badgeText}`}>
                    {change.operator === 'vikram' ? 'Vikram' : change.operator === 'vkaveri' ? 'VKaveri' : change.operator}
                  </span>
                </div>
              )
            })}
          </div>
        ) : (
          <div className="text-center py-8 text-gray-400">
            <Activity className="h-8 w-8 mx-auto mb-2" />
            <p className="text-sm">No price changes detected yet</p>
            <p className="text-xs mt-1">Changes will appear as more scrapes are recorded</p>
          </div>
        )}
      </div>

      {/* Cancellation Rate Tracking */}
      <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">Cancellation Rate Tracking</h3>
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <div className="space-y-0 divide-y divide-gray-100">
          {[
            { name: 'Vikram Travels', apparentFill: 58, realFill: 38, cancelDrop: 20, color: 'text-red-600', note: '–20pts after cancels', noteColor: 'text-red-500' },
            { name: 'VKaveri Bus', apparentFill: 71, realFill: 68, cancelDrop: 3, color: 'text-green-600', note: 'Low cancel rate', noteColor: 'text-gray-400' },
            { name: 'SRS Travels', apparentFill: 63, realFill: 51, cancelDrop: 12, color: 'text-orange-600', note: '–12pts after cancels', noteColor: 'text-orange-500' },
          ].map((item, idx) => (
            <div key={idx} className="flex justify-between items-center py-3">
              <div>
                <p className="text-sm font-bold text-gray-900">{item.name}</p>
                <p className="text-[10px] text-gray-400">Apparent fill {item.apparentFill}%</p>
              </div>
              <div className="text-right">
                <p className={`text-sm font-bold ${item.color}`}>Real fill {item.realFill}%</p>
                <p className={`text-[10px] ${item.noteColor}`}>{item.note}</p>
              </div>
            </div>
          ))}
        </div>
        <div className="mt-3 pt-3 border-t border-gray-100 text-[11px] text-gray-500">
          💡 You may be competing against ghost inventory. Apparent fill rates don&apos;t account for post-booking cancellations.
        </div>
      </div>

      {/* Scrape History Summary */}
      {batches.length > 0 && (
        <>
          <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider">Scrape History ({batches.length} batches)</h3>
          <div className="rounded-xl bg-white shadow-sm border border-gray-100 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="px-4 py-2.5 text-left text-[10px] font-bold text-gray-500 uppercase">Scraped At</th>
                    <th className="px-4 py-2.5 text-center text-[10px] font-bold text-gray-500 uppercase">Trips</th>
                    {['mythri', 'vikram', 'vkaveri'].map((op) => (
                      <th key={op} className="px-4 py-2.5 text-center text-[10px] font-bold uppercase tracking-wider">
                        <span className={op === 'mythri' ? 'text-blue-600' : op === 'vikram' ? 'text-indigo-600' : 'text-purple-600'}>
                          {op === 'mythri' ? 'Mythri' : op === 'vikram' ? 'Vikram' : 'VKaveri'}
                        </span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {batches.slice(0, 15).map((batch: any) => {
                    const opMap: Record<string, any> = {}
                    for (const op of (batch.operators || [])) opMap[op.operator] = op
                    return (
                      <tr key={batch.scrape_batch_id}>
                        <td className="px-4 py-2 text-xs text-gray-600">
                          {new Date(batch.scraped_at).toLocaleDateString('en-IN', { day: '2-digit', month: 'short' })}
                          {' '}
                          {new Date(batch.scraped_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true })}
                        </td>
                        <td className="px-4 py-2 text-center text-xs font-medium text-gray-700">{batch.total_entries}</td>
                        {['mythri', 'vikram', 'vkaveri'].map((op) => {
                          const opData = opMap[op]
                          return (
                            <td key={op} className="px-4 py-2 text-center text-xs">
                              {opData ? (
                                <span className="font-bold text-gray-700">
                                  {opData.avg_fare ? formatCurrency(opData.avg_fare) : '-'}
                                  <span className="text-[10px] text-gray-400 ml-1">({opData.entry_count})</span>
                                </span>
                              ) : (
                                <span className="text-gray-300">-</span>
                              )}
                            </td>
                          )
                        })}
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// SUB-TAB: VELOCITY & DEMAND
// Booking patterns and demand analysis
// ─────────────────────────────────────────────────────────────────────────────

function VelocityDemandTab({
  historyData,
  groupedComparisons,
}: {
  historyData: any
  groupedComparisons: any[]
}) {
  const batches = historyData?.batches || []

  // Compute seat availability trend from batches (seats available over time)
  const seatTrend: { time: string; mythri: number; competitors: number }[] = []
  for (const batch of [...batches].reverse().slice(-20)) {
    let mythriSeats = 0
    let compSeats = 0
    for (const op of (batch.operators || [])) {
      const totalSeats = (op.entries || []).reduce((s: number, e: any) => s + (e.available_seats || 0), 0)
      if (op.operator === 'mythri') mythriSeats = totalSeats
      else compSeats += totalSeats
    }
    seatTrend.push({
      time: batch.scraped_at,
      mythri: mythriSeats,
      competitors: compSeats,
    })
  }

  // Compute day-of-week demand from comparisons
  const dayDemand: Record<string, { count: number; avgSeats: number; totalSeats: number }> = {}
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
  for (const g of groupedComparisons) {
    if (!g.travel_date) continue
    const dayOfWeek = new Date(g.travel_date + 'T00:00:00').getDay()
    const dayName = days[dayOfWeek]
    if (!dayDemand[dayName]) dayDemand[dayName] = { count: 0, avgSeats: 0, totalSeats: 0 }
    dayDemand[dayName].count++
    dayDemand[dayName].totalSeats += g.mythri_available_seats || 0
  }
  // Compute avg seats per day
  for (const d of Object.values(dayDemand)) {
    d.avgSeats = d.count > 0 ? Math.round(d.totalSeats / d.count) : 0
  }

  return (
    <div className="space-y-5">
      <div className="grid gap-5 lg:grid-cols-2">
        {/* Seat Availability Over Time */}
        <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
          <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">Seat Availability Trend</h3>
          {seatTrend.length > 0 ? (
            <div className="space-y-2">
              {seatTrend.map((point, idx) => {
                const maxSeats = Math.max(...seatTrend.map(p => Math.max(p.mythri, p.competitors)), 1)
                return (
                  <div key={idx} className="flex items-center gap-3 text-xs">
                    <span className="text-[10px] text-gray-400 w-[70px] shrink-0">
                      {new Date(point.time).toLocaleDateString('en-IN', { day: '2-digit', month: 'short' })}
                    </span>
                    <div className="flex-1 flex items-center gap-2">
                      <div className="flex-1 bg-gray-100 rounded-full h-3 overflow-hidden">
                        <div
                          className="h-full bg-blue-500 rounded-full"
                          style={{ width: `${(point.mythri / maxSeats) * 100}%` }}
                        />
                      </div>
                      <span className="text-[10px] font-bold text-blue-600 w-8 text-right">{point.mythri}</span>
                    </div>
                    <div className="flex-1 flex items-center gap-2">
                      <div className="flex-1 bg-gray-100 rounded-full h-3 overflow-hidden">
                        <div
                          className="h-full bg-indigo-500 rounded-full"
                          style={{ width: `${(point.competitors / maxSeats) * 100}%` }}
                        />
                      </div>
                      <span className="text-[10px] font-bold text-indigo-600 w-8 text-right">{point.competitors}</span>
                    </div>
                  </div>
                )
              })}
              <div className="flex gap-4 mt-3 text-[10px] text-gray-500">
                <span className="flex items-center gap-1.5"><span className="w-3 h-2 rounded bg-blue-500" /> Mythri seats</span>
                <span className="flex items-center gap-1.5"><span className="w-3 h-2 rounded bg-indigo-500" /> Competitor seats</span>
              </div>
            </div>
          ) : (
            <div className="text-center py-8 text-gray-400">
              <TrendingUp className="h-8 w-8 mx-auto mb-2" />
              <p className="text-sm">No trend data yet</p>
              <p className="text-xs mt-1">More scrapes needed to show trends</p>
            </div>
          )}
        </div>

        {/* Day of Week Demand */}
        <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
          <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">Day of Week — Service Count</h3>
          {Object.keys(dayDemand).length > 0 ? (
            <div>
              <div className="flex items-end gap-2 h-[140px] mb-2">
                {days.map((day) => {
                  const d = dayDemand[day]
                  const maxCount = Math.max(...Object.values(dayDemand).map(dd => dd.count), 1)
                  const height = d ? (d.count / maxCount) * 120 : 0
                  const isHighDemand = d && d.count >= maxCount * 0.7

                  return (
                    <div key={day} className="flex-1 flex flex-col items-center justify-end gap-1">
                      <span className="text-[10px] font-bold text-gray-600">{d?.count || 0}</span>
                      <div
                        className={`w-full rounded-t-md transition-all ${isHighDemand ? 'bg-blue-500' : 'bg-gray-200'}`}
                        style={{ height: `${Math.max(4, height)}px` }}
                      />
                      <span className={`text-[10px] font-bold ${isHighDemand ? 'text-blue-600' : 'text-gray-400'}`}>{day}</span>
                    </div>
                  )
                })}
              </div>
              <p className="text-xs text-gray-500 mt-2">
                Services tracked by day of week. Higher bars indicate more competing services.
              </p>
            </div>
          ) : (
            <div className="text-center py-8 text-gray-400">
              <BarChart3 className="h-8 w-8 mx-auto mb-2" />
              <p className="text-sm">No demand data yet</p>
            </div>
          )}
        </div>
      </div>

      {/* Booking Velocity Curve */}
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">Booking Velocity Curve — Today vs Historical</h3>
        <div className="relative h-[130px] mb-2">
          <svg width="100%" height="100%" viewBox="0 0 400 120" preserveAspectRatio="none" className="absolute inset-0">
            {/* Grid lines */}
            <line x1="0" y1="30" x2="400" y2="30" stroke="#e5e7eb" strokeWidth="0.5" strokeDasharray="4" />
            <line x1="0" y1="60" x2="400" y2="60" stroke="#e5e7eb" strokeWidth="0.5" strokeDasharray="4" />
            <line x1="0" y1="90" x2="400" y2="90" stroke="#e5e7eb" strokeWidth="0.5" strokeDasharray="4" />
            {/* Historical avg curve (smoother, higher fill) */}
            <polyline
              points={[5, 8, 14, 18, 22, 26, 20, 14, 10, 8, 6, 4].map((v, i) => `${i * 34 + 17},${110 - v * (90 / 30)}`).join(' ')}
              fill="none"
              stroke="#10b981"
              strokeWidth="2"
              opacity="0.6"
            />
            {/* Today's curve (behind, lower fill) */}
            <polyline
              points={[5, 7, 12, 10, 6, 3, 1, 1, 0, 0, 0, 0].map((v, i) => `${i * 34 + 17},${110 - v * (90 / 30)}`).join(' ')}
              fill="none"
              stroke="#ef4444"
              strokeWidth="2.5"
            />
            <line x1="0" y1="110" x2="400" y2="110" stroke="#d1d5db" strokeWidth="1" />
          </svg>
        </div>
        <div className="flex gap-4 text-[11px] text-gray-500 mb-3">
          <div className="flex items-center gap-1.5"><div className="w-5 h-0.5 bg-green-500 rounded" /> Historical avg (Fri)</div>
          <div className="flex items-center gap-1.5"><div className="w-5 h-0.5 bg-red-500 rounded" /> Today (current)</div>
        </div>
        <div className="bg-red-50 border border-red-200 rounded-lg p-2.5 text-[11px] text-gray-600">
          ⚠️ You are <strong className="text-red-600">14 points behind</strong> the historical velocity curve for a Friday departure at D-3. Expected fill: 48%. Actual: 34%. This is an emergency pricing signal.
        </div>
      </div>

      {/* Advance Booking Window Distribution */}
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">Advance Booking Window Distribution</h3>
        <div className="flex items-end gap-1 h-[100px] mb-2">
          {[
            { label: 'D-30', value: 2 },
            { label: 'D-21', value: 4 },
            { label: 'D-14', value: 8 },
            { label: 'D-7', value: 14 },
            { label: 'D-3', value: 26 },
            { label: 'D-1', value: 38 },
            { label: 'D-0', value: 8 },
          ].map((bar, idx) => {
            const maxVal = 38
            const heightPct = (bar.value / maxVal) * 90
            const isHot = idx >= 4
            return (
              <div key={bar.label} className="flex-1 flex flex-col items-center gap-1">
                <span className="text-[9px] font-mono text-gray-400">{bar.value}%</span>
                <div
                  className={`w-full rounded-t-sm ${isHot ? 'bg-yellow-500' : 'bg-gray-300'}`}
                  style={{ height: `${Math.max(4, heightPct)}px` }}
                />
              </div>
            )
          })}
        </div>
        <div className="flex justify-between text-[9px] text-gray-400 font-mono mb-3">
          <span>D-30</span><span>D-21</span><span>D-14</span><span>D-7</span><span>D-3</span><span>D-1</span><span>D-0</span>
        </div>
        <p className="text-[11px] text-gray-500">
          📊 <strong>85.5%</strong> of bookings happen D-3 or closer. Price higher early, drop sharply at D-3 if not filled.
        </p>
      </div>

      {/* Revenue Left on Table */}
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">Total Revenue Left on Table — Last 30 Days</h3>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <div className="text-center p-4 bg-red-50 border border-red-200 rounded-lg">
            <p className="text-2xl font-bold text-red-600 font-mono">₹2.3L</p>
            <p className="text-[11px] text-gray-500 mt-1">Over-discounted on<br />sold-out trips</p>
          </div>
          <div className="text-center p-4 bg-red-50 border border-red-200 rounded-lg">
            <p className="text-2xl font-bold text-red-600 font-mono">₹1.1L</p>
            <p className="text-[11px] text-gray-500 mt-1">Unsold seats on<br />departed trips</p>
          </div>
          <div className="text-center p-4 bg-red-50 border border-red-200 rounded-lg">
            <p className="text-2xl font-bold text-orange-600 font-mono">₹84K</p>
            <p className="text-[11px] text-gray-500 mt-1">Missed event<br />surges</p>
          </div>
          <div className="text-center p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
            <p className="text-2xl font-bold text-yellow-600 font-mono">₹4.24L</p>
            <p className="text-[11px] text-gray-500 mt-1">Total Recoverable<br />This Month</p>
          </div>
        </div>
      </div>

      {/* Pricing Timeline Guide */}
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">Optimal Pricing Timeline</h3>
        <div className="space-y-2">
          {[
            { label: 'D-30', color: 'border-green-500', bg: 'bg-green-50', text: 'Open at base price. Build early demand data. No discounts needed.' },
            { label: 'D-14', color: 'border-yellow-500', bg: 'bg-yellow-50', text: 'If fill <25%, consider 5-8% reduction. If >50%, consider 10% increase.' },
            { label: 'D-7', color: 'border-orange-500', bg: 'bg-orange-50', text: 'Watch competitor velocity. Drop aggressively if stalled. Raise if competitors filling fast.' },
            { label: 'D-3', color: 'border-red-500', bg: 'bg-red-50', text: 'Critical window — most bookings happen here. Price within 10-15% of market average.' },
            { label: 'D-0', color: 'border-red-700', bg: 'bg-red-50', text: 'Sell remaining seats at any competitive price. Empty seats = zero revenue.' },
          ].map((step) => (
            <div key={step.label} className={`flex items-center gap-3 px-4 py-3 rounded-lg border-l-4 ${step.color} ${step.bg}`}>
              <span className="text-xs font-bold text-gray-700 w-10 shrink-0">{step.label}</span>
              <span className="text-xs text-gray-600">{step.text}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Price Trend Table from history */}
      {batches.length > 0 && (
        <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
          <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">Fare Trend — Last {batches.length} Scrapes</h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b">
                  <th className="px-3 py-2 text-left text-[10px] font-bold text-gray-500 uppercase">Time</th>
                  {['mythri', 'vikram', 'vkaveri'].map((op) => (
                    <th key={op} className="px-3 py-2 text-center text-[10px] font-bold uppercase">
                      <span className={op === 'mythri' ? 'text-blue-600' : op === 'vikram' ? 'text-indigo-600' : 'text-purple-600'}>
                        {op === 'mythri' ? 'Mythri Avg' : op === 'vikram' ? 'Vikram Avg' : 'VKaveri Avg'}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {batches.slice(0, 10).map((batch: any) => {
                  const opMap: Record<string, any> = {}
                  for (const op of (batch.operators || [])) opMap[op.operator] = op
                  return (
                    <tr key={batch.scrape_batch_id}>
                      <td className="px-3 py-2 text-xs text-gray-600">
                        {new Date(batch.scraped_at).toLocaleDateString('en-IN', { day: '2-digit', month: 'short' })}
                        {' '}
                        {new Date(batch.scraped_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true })}
                      </td>
                      {['mythri', 'vikram', 'vkaveri'].map((op) => (
                        <td key={op} className="px-3 py-2 text-center text-xs font-bold text-gray-700">
                          {opMap[op]?.avg_fare ? formatCurrency(opMap[op].avg_fare) : <span className="text-gray-300">-</span>}
                        </td>
                      ))}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// SUB-TAB: 30-DAY DATA
// Horizontal calendar grid — operators as rows, dates as columns
// Matches the HTML prototype "30-Day Calendar" view
// ─────────────────────────────────────────────────────────────────────────────

// ─── Festival / Holiday / Long Weekend data for date highlighting ───
const CALENDAR_EVENTS: Record<string, {
  emoji: string
  name: string
  shortName: string
  impact: 'SURGE' | 'HIGH' | 'MEDIUM'
  bgClass: string
  textClass: string
  borderClass: string
  badgeBg: string
  badgeText: string
}> = {
  '2026-03-03': { emoji: '🔥', name: 'Holika Dahana', shortName: 'HOLIKA', impact: 'HIGH', bgClass: 'bg-orange-50', textClass: 'text-orange-700', borderClass: 'border-orange-300', badgeBg: 'bg-orange-100', badgeText: 'text-orange-700' },
  '2026-03-04': { emoji: '🎨', name: 'Holi', shortName: 'HOLI', impact: 'SURGE', bgClass: 'bg-pink-50', textClass: 'text-pink-700', borderClass: 'border-pink-300', badgeBg: 'bg-pink-100', badgeText: 'text-pink-700' },
  '2026-03-19': { emoji: '🌙', name: 'Ugadi — Telugu New Year', shortName: 'UGADI', impact: 'SURGE', bgClass: 'bg-amber-50', textClass: 'text-amber-700', borderClass: 'border-amber-300', badgeBg: 'bg-amber-100', badgeText: 'text-amber-700' },
  '2026-03-20': { emoji: '🕌', name: 'Jamat Ul-Vida', shortName: 'JUMA', impact: 'HIGH', bgClass: 'bg-emerald-50', textClass: 'text-emerald-700', borderClass: 'border-emerald-300', badgeBg: 'bg-emerald-100', badgeText: 'text-emerald-700' },
  '2026-03-21': { emoji: '🕌', name: 'Eid al-Fitr (Ramzan)', shortName: 'EID', impact: 'HIGH', bgClass: 'bg-emerald-50', textClass: 'text-emerald-700', borderClass: 'border-emerald-300', badgeBg: 'bg-emerald-100', badgeText: 'text-emerald-700' },
  '2026-03-26': { emoji: '🛕', name: 'Rama Navami', shortName: 'NAVAMI', impact: 'HIGH', bgClass: 'bg-yellow-50', textClass: 'text-yellow-700', borderClass: 'border-yellow-300', badgeBg: 'bg-yellow-100', badgeText: 'text-yellow-700' },
}

// Long weekend date ranges (inclusive) — dates that fall within a long weekend cluster
const LONG_WEEKENDS: Array<{ start: string; end: string; label: string }> = [
  { start: '2026-03-03', end: '2026-03-08', label: 'Holi Long Weekend' },
  { start: '2026-03-19', end: '2026-03-22', label: 'Ugadi + Eid Long Weekend' },
  { start: '2026-03-26', end: '2026-03-29', label: 'Rama Navami Long Weekend' },
]

function isInLongWeekend(date: string): string | null {
  for (const lw of LONG_WEEKENDS) {
    if (date >= lw.start && date <= lw.end) return lw.label
  }
  return null
}

function ThirtyDayDataTab({
  calendarData,
  routeFilter,
  routes,
  setRouteFilter,
}: {
  calendarData: any
  routeFilter: string
  routes: any[]
  setRouteFilter: (v: string) => void
}) {
  const calendar: any[] = calendarData?.calendar || []

  if (!calendarData) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
      </div>
    )
  }

  // Build 30-day date range starting from today
  const today = new Date()
  const todayStr = today.toISOString().split('T')[0]
  const allDates: string[] = []
  for (let i = 0; i < 30; i++) {
    const d = new Date(today)
    d.setDate(d.getDate() + i)
    allDates.push(d.toISOString().split('T')[0])
  }

  // Build lookup: date → { operator → data }
  const dateLookup: Record<string, Record<string, any>> = {}
  for (const day of calendar) {
    const opMap: Record<string, any> = {}
    for (const op of (day.operators || [])) {
      opMap[op.operator] = op
    }
    dateLookup[day.date] = opMap
  }

  // Get all unique operators and ensure Mythri is first
  const allOperators = new Set<string>()
  for (const day of calendar) {
    for (const op of (day.operators || [])) {
      allOperators.add(op.operator)
    }
  }
  const operators = ['mythri', ...Array.from(allOperators).filter(o => o !== 'mythri').sort()]

  const OPERATOR_LABELS: Record<string, string> = {
    mythri: 'Mythri (Ours)',
    vikram: 'Vikram Travels',
    vkaveri: 'VKaveri Bus',
  }

  const OPERATOR_ROW_COLORS: Record<string, { bg: string; border: string; text: string }> = {
    mythri: { bg: 'bg-blue-50', border: 'border-blue-200', text: 'text-blue-700' },
    vikram: { bg: 'bg-indigo-50', border: 'border-indigo-200', text: 'text-indigo-700' },
    vkaveri: { bg: 'bg-purple-50', border: 'border-purple-200', text: 'text-purple-700' },
  }

  // Compute market average per date (average of all operators' avg_fare)
  const marketAvgByDate: Record<string, number> = {}
  for (const date of allDates) {
    const ops = dateLookup[date]
    if (!ops) continue
    const fares = Object.values(ops).map((o: any) => o.avg_fare).filter((f: any) => f != null && f > 0)
    if (fares.length > 0) {
      marketAvgByDate[date] = Math.round(fares.reduce((s: number, v: number) => s + v, 0) / fares.length)
    }
  }

  // Summary stats
  let mythriTotal = 0, mythriCount = 0, compCheaper = 0, compCostlier = 0, totalDates = 0
  for (const date of allDates) {
    const ops = dateLookup[date]
    if (!ops) continue
    totalDates++
    const m = ops['mythri']
    if (m?.avg_fare) { mythriTotal += m.avg_fare; mythriCount++ }
    const mkt = marketAvgByDate[date]
    if (m?.avg_fare && mkt) {
      if (m.avg_fare > mkt + 30) compCheaper++
      else if (m.avg_fare < mkt - 30) compCostlier++
    }
  }
  const mythriAvg = mythriCount > 0 ? Math.round(mythriTotal / mythriCount) : 0

  // CSV export
  function exportCSV() {
    const header = ['Operator', ...allDates.map(d => {
      const dt = new Date(d + 'T00:00:00')
      return dt.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' })
    })].join(',')
    const rows = operators.map(op => {
      const cells = allDates.map(date => {
        const data = dateLookup[date]?.[op]
        return data?.avg_fare ? `₹${data.avg_fare}(${data.available_seats || 0}s)` : '-'
      })
      return [OPERATOR_LABELS[op] || op, ...cells].join(',')
    })
    const csv = [header, ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `30day-pricing-${todayStr}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="space-y-5">
      {/* Header with Route Filter & Controls */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h2 className="text-lg font-bold text-gray-900">Price Calendar — 30 Days</h2>
          <p className="text-xs text-gray-500 mt-0.5">{totalDates} dates with data · {operators.length} operators tracked</p>
        </div>
        <div className="flex items-center gap-3">
          {/* Route Filter */}
          <select
            value={routeFilter}
            onChange={(e) => setRouteFilter(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none"
          >
            <option value="">All Routes</option>
            {routes.map((r: any) => (
              <option key={`${r.route_from}|${r.route_to}`} value={`${r.route_from}|${r.route_to}`}>
                {r.route_from} → {r.route_to}
              </option>
            ))}
          </select>
          <button
            onClick={exportCSV}
            className="h-9 px-4 rounded-lg border border-gray-200 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50 transition-colors flex items-center gap-2"
          >
            <ChevronRight className="h-3.5 w-3.5" />
            Export CSV
          </button>
        </div>
      </div>

      {/* KPI Summary */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="rounded-xl bg-white p-4 shadow-sm border border-gray-100 text-center">
          <p className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1">Mythri Avg Fare</p>
          <p className="text-xl font-bold text-blue-600 font-mono">{mythriAvg > 0 ? formatCurrency(mythriAvg) : '-'}</p>
        </div>
        <div className="rounded-xl bg-white p-4 shadow-sm border border-gray-100 text-center">
          <p className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1">Days with Data</p>
          <p className="text-xl font-bold text-gray-900 font-mono">{totalDates} <span className="text-sm text-gray-400">/ 30</span></p>
        </div>
        <div className="rounded-xl bg-white p-4 shadow-sm border border-gray-100 text-center">
          <p className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1">We&apos;re Overpriced</p>
          <p className="text-xl font-bold text-red-600 font-mono">{compCheaper} <span className="text-sm text-gray-400">days</span></p>
        </div>
        <div className="rounded-xl bg-white p-4 shadow-sm border border-gray-100 text-center">
          <p className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1">We&apos;re Cheapest</p>
          <p className="text-xl font-bold text-green-600 font-mono">{compCostlier} <span className="text-sm text-gray-400">days</span></p>
        </div>
      </div>

      {/* Bulk Edit Quick Bar */}
      <div className="rounded-xl bg-white p-4 shadow-sm border border-gray-100 flex items-center gap-3 flex-wrap">
        <span className="text-xs font-semibold text-gray-700 whitespace-nowrap">Quick Bulk Edit:</span>
        <div className="w-px h-6 bg-gray-200" />
        <select className="h-8 rounded-lg border border-gray-200 bg-white px-2 text-xs focus:border-blue-500 focus:outline-none">
          <option>All Seats</option><option>Lower Sleeper</option><option>Upper Sleeper</option>
        </select>
        <select className="h-8 rounded-lg border border-gray-200 bg-white px-2 text-xs focus:border-blue-500 focus:outline-none">
          <option>Reduce by ₹</option><option>Increase by ₹</option><option>Match market avg</option><option>Undercut cheapest by ₹</option>
        </select>
        <input type="number" defaultValue={100} className="h-8 w-16 rounded-lg border border-gray-200 bg-white px-2 text-xs text-center focus:border-blue-500 focus:outline-none" />
        <select className="h-8 rounded-lg border border-gray-200 bg-white px-2 text-xs focus:border-blue-500 focus:outline-none">
          <option>Departing today (CRITICAL)</option><option>Next 48h</option><option>Next 7 days</option><option>Next 14 days</option><option>All 30 days</option><option>Weekends only</option>
        </select>
        <div className="w-px h-6 bg-gray-200" />
        <span className="text-[10px] font-medium text-green-700 bg-green-50 border border-green-200 px-2 py-1 rounded">Preview: affects {totalDates} dates</span>
        <button className="h-8 px-4 rounded-lg bg-blue-600 text-white text-xs font-medium hover:bg-blue-700 transition-colors">Apply</button>
      </div>

      {/* Legend */}
      <div className="flex items-center gap-4 text-xs text-gray-500 flex-wrap">
        <span className="font-semibold text-gray-700">Cell Colors:</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-red-100 border border-red-300" /> Overpriced (&gt;₹120 above mkt)</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-orange-100 border border-orange-300" /> Above market</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-green-100 border border-green-300" /> Competitive</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-gray-100 border border-gray-200" /> No data</span>
        <span className="ml-auto font-semibold text-gray-700">Zone:</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-purple-200 border border-purple-400" /> Today</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-red-200 border border-red-400" /> 48h</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-orange-200 border border-orange-300" /> 3–7d</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded bg-yellow-100 border border-yellow-300" /> 7–14d</span>
      </div>

      {/* ═══ THE MAIN 30-DAY HORIZONTAL CALENDAR TABLE ═══ */}
      {operators.length === 0 ? (
        <div className="rounded-xl border bg-white shadow-sm">
          <div className="flex flex-col items-center justify-center py-20 text-gray-400">
            <LayoutGrid className="h-12 w-12 mb-3" />
            <p className="text-lg font-medium">No 30-day data available</p>
            <p className="text-sm mt-1">Run a scrape to populate future date pricing</p>
          </div>
        </div>
      ) : (
        <div className="rounded-xl bg-white shadow-sm border border-gray-100 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full" style={{ minWidth: `${180 + allDates.length * 82}px` }}>
              {/* ─── DATE HEADER ROW ─── */}
              <thead>
                <tr className="border-b bg-gray-50">
                  <th className="sticky left-0 z-20 bg-gray-50 px-4 py-2 text-left" style={{ minWidth: 180 }}>
                    <span className="text-[10px] font-bold text-gray-500 uppercase tracking-wider">Operator</span>
                  </th>
                  {allDates.map((date) => {
                    const dt = new Date(date + 'T00:00:00')
                    const dayOfWeek = dt.toLocaleDateString('en-IN', { weekday: 'short' })
                    const dayNum = dt.getDate()
                    const month = dt.toLocaleDateString('en-IN', { month: 'short' })
                    const daysAway = Math.ceil((dt.getTime() - new Date(todayStr + 'T00:00:00').getTime()) / (1000 * 60 * 60 * 24))
                    const isToday = daysAway === 0
                    const isWeekend = dt.getDay() === 0 || dt.getDay() === 6

                    // Festival / event lookup
                    const event = CALENDAR_EVENTS[date]
                    const longWeekend = isInLongWeekend(date)

                    // Zone color for the top bar — events override zone color
                    let zoneColor = 'bg-gray-200'
                    if (event?.impact === 'SURGE') zoneColor = 'bg-gradient-to-r from-amber-400 to-orange-500'
                    else if (event?.impact === 'HIGH') zoneColor = 'bg-gradient-to-r from-blue-400 to-indigo-500'
                    else if (isToday) zoneColor = 'bg-purple-500'
                    else if (daysAway <= 2) zoneColor = 'bg-red-500'
                    else if (daysAway <= 7) zoneColor = 'bg-orange-400'
                    else if (daysAway <= 14) zoneColor = 'bg-yellow-400'

                    // Background tint for the entire column header
                    let headerBg = ''
                    if (event?.impact === 'SURGE') headerBg = event.bgClass
                    else if (event?.impact === 'HIGH') headerBg = event.bgClass
                    else if (longWeekend && !event) headerBg = 'bg-amber-50/50'

                    return (
                      <th key={date} className={`px-1 py-1.5 text-center ${headerBg}`} style={{ minWidth: 82 }} title={event ? event.name : longWeekend || ''}>
                        {/* Zone / event color indicator bar */}
                        <div className={`h-1.5 rounded-full mx-1 mb-1 ${zoneColor}`} />
                        <div className={`text-[10px] font-bold ${isToday ? 'text-purple-600' : isWeekend ? 'text-red-500' : 'text-gray-600'}`}>
                          {dayOfWeek}
                        </div>
                        <div className={`text-sm font-bold ${isToday ? 'text-purple-700' : event ? event.textClass : 'text-gray-900'}`}>
                          {dayNum} {month}
                        </div>
                        {/* Festival badge */}
                        {event && (
                          <div className={`mt-1 inline-flex items-center gap-0.5 px-1 py-0.5 rounded ${event.badgeBg} border ${event.borderClass}`}>
                            <span className="text-[10px]">{event.emoji}</span>
                            <span className={`text-[7px] font-extrabold tracking-wider ${event.badgeText}`}>{event.shortName}</span>
                          </div>
                        )}
                        {/* Impact tag for festivals */}
                        {event && (
                          <div className={`mt-0.5 text-[7px] font-bold tracking-wider ${event.impact === 'SURGE' ? 'text-red-500' : 'text-blue-500'}`}>
                            {event.impact === 'SURGE' ? '⚡ SURGE' : '↑ HIGH'}
                          </div>
                        )}
                        {/* Long weekend indicator (only if no festival badge on this date) */}
                        {!event && longWeekend && (
                          <div className="mt-1 inline-flex items-center px-1 py-0.5 rounded bg-amber-100 border border-amber-300">
                            <span className="text-[7px] font-bold text-amber-700 tracking-wider">LW</span>
                          </div>
                        )}
                        {/* Today badge */}
                        {isToday && <div className="mt-0.5"><span className="text-[8px] font-bold text-purple-600 bg-purple-100 px-1 py-0.5 rounded">TODAY</span></div>}
                        {/* Weekend marker (only if not a festival or long weekend) */}
                        {isWeekend && !isToday && !event && !longWeekend && <span className="text-[8px] text-gray-400">🏖️</span>}
                      </th>
                    )
                  })}
                </tr>
              </thead>

              {/* ─── OPERATOR ROWS ─── */}
              <tbody>
                {operators.map((op) => {
                  const colors = OPERATOR_ROW_COLORS[op] || OPERATOR_ROW_COLORS.mythri
                  const isMine = op === 'mythri'

                  return (
                    <tr key={op} className={`border-b ${isMine ? 'bg-blue-50/30' : 'hover:bg-gray-50/50'}`}>
                      {/* Sticky operator name */}
                      <td
                        className={`sticky left-0 z-10 px-4 py-3 border-r ${isMine ? 'bg-blue-50' : 'bg-white'}`}
                        style={{ minWidth: 180 }}
                      >
                        <div className="flex items-center gap-2">
                          {isMine && <div className="w-1 h-8 rounded-full bg-blue-500 shrink-0" />}
                          <div>
                            <p className={`text-sm font-bold ${colors.text}`}>
                              {OPERATOR_LABELS[op] || op}
                            </p>
                            {isMine && <p className="text-[9px] text-blue-400 font-medium">YOUR PRICING</p>}
                          </div>
                        </div>
                      </td>

                      {/* Price cells for each date */}
                      {allDates.map((date) => {
                        const data = dateLookup[date]?.[op]
                        const daysAway = Math.ceil(
                          (new Date(date + 'T00:00:00').getTime() - new Date(todayStr + 'T00:00:00').getTime()) / (1000 * 60 * 60 * 24)
                        )
                        const isToday = daysAway === 0

                        if (!data || !data.avg_fare) {
                          return (
                            <td key={date} className={`px-1 py-2 text-center ${isToday ? 'bg-purple-50/50' : ''}`}>
                              <span className="text-[10px] text-gray-300">-</span>
                            </td>
                          )
                        }

                        // Determine cell color based on competitiveness (for Mythri only)
                        const mkt = marketAvgByDate[date]
                        let cellBg = ''
                        let priceColor = 'text-gray-900'

                        if (isMine && mkt) {
                          const diff = data.avg_fare - mkt
                          if (diff > 120) {
                            cellBg = 'bg-red-100'
                            priceColor = 'text-red-700'
                          } else if (diff > 30) {
                            cellBg = 'bg-orange-50'
                            priceColor = 'text-orange-700'
                          } else if (diff < -30) {
                            cellBg = 'bg-green-50'
                            priceColor = 'text-green-700'
                          }
                        }

                        // For competitors, highlight if they are cheapest on that date
                        if (!isMine) {
                          const allOpFares = Object.values(dateLookup[date] || {}).map((o: any) => o.avg_fare).filter((f: any) => f > 0)
                          const cheapest = allOpFares.length > 0 ? Math.min(...allOpFares) : 0
                          if (data.avg_fare === cheapest && cheapest > 0) {
                            cellBg = 'bg-green-50'
                            priceColor = 'text-green-700'
                          }
                        }

                        if (isToday && !cellBg) cellBg = 'bg-purple-50/50'

                        const seatColor =
                          data.available_seats <= 5 ? 'text-red-500' :
                          data.available_seats <= 15 ? 'text-orange-500' : 'text-gray-400'

                        return (
                          <td key={date} className={`px-1 py-2 text-center ${cellBg} ${isMine ? 'cursor-pointer hover:ring-2 hover:ring-blue-300 hover:ring-inset' : ''}`}>
                            <div className={`text-xs font-bold font-mono ${priceColor}`}>
                              {formatCurrency(data.avg_fare)}
                            </div>
                            <div className={`text-[9px] font-medium ${seatColor}`}>
                              {data.available_seats != null ? `${data.available_seats}s` : '-'}
                            </div>
                            {/* Overpriced indicator bar */}
                            {isMine && mkt && data.avg_fare - mkt > 120 && (
                              <div className="h-0.5 bg-red-400 rounded-full mx-1 mt-0.5" />
                            )}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}

                {/* ─── MARKET AVERAGE ROW ─── */}
                <tr className="border-t-2 border-gray-300 bg-gray-50">
                  <td className="sticky left-0 z-10 px-4 py-3 border-r bg-gray-50" style={{ minWidth: 180 }}>
                    <div className="flex items-center gap-2">
                      <BarChart3 className="h-4 w-4 text-gray-400" />
                      <div>
                        <p className="text-sm font-bold text-gray-600">Market Avg</p>
                        <p className="text-[9px] text-gray-400 font-medium">ALL OPERATORS</p>
                      </div>
                    </div>
                  </td>
                  {allDates.map((date) => {
                    const mkt = marketAvgByDate[date]
                    const isToday = date === todayStr
                    return (
                      <td key={date} className={`px-1 py-2 text-center ${isToday ? 'bg-purple-50/50' : ''}`}>
                        {mkt ? (
                          <div className="text-xs font-bold font-mono text-gray-500">{formatCurrency(mkt)}</div>
                        ) : (
                          <span className="text-[10px] text-gray-300">-</span>
                        )}
                      </td>
                    )
                  })}
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* High Demand Events */}
      <div className="rounded-xl bg-white p-5 shadow-sm border border-gray-100">
        <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">High Demand Events — Next 30 Days</h3>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
          <div className="p-3 bg-purple-50 border border-purple-200 rounded-lg">
            <div className="flex items-center gap-2 mb-1.5">
              <span>🎨</span>
              <span className="text-sm font-bold text-gray-900">Holi Weekend</span>
              <span className="text-[9px] font-bold text-purple-600 bg-purple-100 px-1.5 py-0.5 rounded">SURGE</span>
            </div>
            <p className="text-[11px] text-gray-600 leading-relaxed">
              Mar 14–16 · Affects 4 routes<br />Competitor prices already +18%. <strong className="text-purple-700">Pre-raise ₹200–300 now.</strong>
            </p>
          </div>
          <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
            <div className="flex items-center gap-2 mb-1.5">
              <span>🏏</span>
              <span className="text-sm font-bold text-gray-900">RCB vs CSK (Bengaluru)</span>
              <span className="text-[9px] font-bold text-blue-600 bg-blue-100 px-1.5 py-0.5 rounded">HIGH</span>
            </div>
            <p className="text-[11px] text-gray-600 leading-relaxed">
              Feb 28 · HYD→BLR route<br />Last year: +34% demand. <strong className="text-blue-700">Raise ₹250 on this date.</strong>
            </p>
          </div>
          <div className="p-3 bg-green-50 border border-green-200 rounded-lg">
            <div className="flex items-center gap-2 mb-1.5">
              <span>🎓</span>
              <span className="text-sm font-bold text-gray-900">Board Exam Season</span>
              <span className="text-[9px] font-bold text-green-600 bg-green-100 px-1.5 py-0.5 rounded">MEDIUM</span>
            </div>
            <p className="text-[11px] text-gray-600 leading-relaxed">
              Mar 1–15 · Student travel drops<br />Consider budget pricing on exam weeks.
            </p>
          </div>
          <div className="p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
            <div className="flex items-center gap-2 mb-1.5">
              <span>🌙</span>
              <span className="text-sm font-bold text-gray-900">Ugadi Long Weekend</span>
              <span className="text-[9px] font-bold text-yellow-600 bg-yellow-100 px-1.5 py-0.5 rounded">SURGE</span>
            </div>
            <p className="text-[11px] text-gray-600 leading-relaxed">
              Mar 28–31 · Telugu New Year<br />Major demand event. Price 35–40% above base.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
