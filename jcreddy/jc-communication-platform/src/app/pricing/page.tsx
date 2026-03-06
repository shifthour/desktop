'use client'

import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { formatCurrency } from '@/lib/utils'
import {
  Home,
  TrendingUp,
  TrendingDown,
  ArrowRight,
  CheckCircle,
  XCircle,
  Clock,
  AlertTriangle,
  Activity,
  Zap,
  Shield,
  ChevronDown,
  ChevronUp,
  RefreshCw,
  Settings,
  IndianRupee,
  Bus,
  Loader2,
} from 'lucide-react'
import Link from 'next/link'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'

// ─────────────────────────────────────────────────────────────────────────────
// Data fetching functions
// ─────────────────────────────────────────────────────────────────────────────

async function fetchRecommendations(status: string, date: string) {
  let url = `/api/pricing/recommendations?status=${status}&limit=100`
  if (date) url += `&date=${date}`
  const res = await fetch(url)
  if (!res.ok) throw new Error('Failed to fetch recommendations')
  return res.json()
}

async function fetchRules() {
  const res = await fetch('/api/pricing/rules')
  if (!res.ok) throw new Error('Failed to fetch rules')
  return res.json()
}

async function fetchConfig() {
  const res = await fetch('/api/pricing/config')
  if (!res.ok) throw new Error('Failed to fetch config')
  return res.json()
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function getDemandBadge(score: number | null) {
  if (score === null || score === undefined) return { label: 'N/A', bg: 'bg-gray-100', text: 'text-gray-600' }
  if (score >= 71) return { label: 'High', bg: 'bg-red-100', text: 'text-red-700' }
  if (score >= 41) return { label: 'Medium', bg: 'bg-yellow-100', text: 'text-yellow-700' }
  return { label: 'Low', bg: 'bg-green-100', text: 'text-green-700' }
}

function getChangeBadge(percent: number) {
  if (percent > 0) return { icon: TrendingUp, bg: 'bg-amber-100', text: 'text-amber-700', prefix: '+' }
  if (percent < 0) return { icon: TrendingDown, bg: 'bg-green-100', text: 'text-green-700', prefix: '' }
  return { icon: Activity, bg: 'bg-gray-100', text: 'text-gray-600', prefix: '' }
}

function formatHours(hours: number | null) {
  if (hours === null || hours === undefined) return '-'
  if (hours < 1) return `${Math.round(hours * 60)}m`
  if (hours < 24) return `${Math.round(hours)}h`
  const days = Math.floor(hours / 24)
  const remainingHours = Math.round(hours % 24)
  return `${days}d ${remainingHours}h`
}

function getRuleTypeBadge(type: string) {
  const badges: Record<string, { bg: string; text: string }> = {
    occupancy_threshold: { bg: 'bg-blue-100', text: 'text-blue-700' },
    time_to_departure: { bg: 'bg-purple-100', text: 'text-purple-700' },
    day_of_week: { bg: 'bg-indigo-100', text: 'text-indigo-700' },
    booking_velocity: { bg: 'bg-orange-100', text: 'text-orange-700' },
    last_seats: { bg: 'bg-red-100', text: 'text-red-700' },
    early_bird: { bg: 'bg-green-100', text: 'text-green-700' },
  }
  return badges[type] || { bg: 'bg-gray-100', text: 'text-gray-700' }
}

// ─────────────────────────────────────────────────────────────────────────────
// Domain navigation
// ─────────────────────────────────────────────────────────────────────────────

const domainNav = [
  { id: 'executive', label: 'Executive', href: '/executive' },
  { id: 'bookings', label: 'Bookings', href: '/bookings' },
  { id: 'revenue', label: 'Revenue', href: '/revenue' },
  { id: 'pricing', label: 'Dynamic Pricing', href: '/pricing', active: true },
  { id: 'trips', label: 'Trips', href: '/trips' },
]

// ─────────────────────────────────────────────────────────────────────────────
// Main Component
// ─────────────────────────────────────────────────────────────────────────────

export default function DynamicPricingPage() {
  const queryClient = useQueryClient()
  const [statusFilter, setStatusFilter] = useState('pending')
  const [dateFilter, setDateFilter] = useState('')
  const [showRules, setShowRules] = useState(false)
  const [generatingRecs, setGeneratingRecs] = useState(false)
  const [actionLoading, setActionLoading] = useState<string | null>(null)

  // Data fetching
  const { data: recData, isLoading: recsLoading } = useQuery({
    queryKey: ['pricing-recommendations', statusFilter, dateFilter],
    queryFn: () => fetchRecommendations(statusFilter, dateFilter),
    refetchInterval: 30000,
  })

  const { data: rulesData } = useQuery({
    queryKey: ['pricing-rules'],
    queryFn: fetchRules,
    refetchInterval: 60000,
  })

  const { data: configData } = useQuery({
    queryKey: ['pricing-config'],
    queryFn: fetchConfig,
  })

  const recommendations = recData?.recommendations || []
  const stats = recData?.stats || {}
  const rules = rulesData?.rules || []
  const globalConfig = configData?.configMap?.global_settings || {}

  // ─── Actions ───

  async function handleGenerateRecommendations() {
    setGeneratingRecs(true)
    try {
      const res = await fetch('/api/pricing/generate', { method: 'POST' })
      const data = await res.json()
      if (data.success) {
        queryClient.invalidateQueries({ queryKey: ['pricing-recommendations'] })
      } else {
        alert(data.message || 'Failed to generate recommendations')
      }
    } catch {
      alert('Failed to generate recommendations')
    } finally {
      setGeneratingRecs(false)
    }
  }

  async function handleAction(id: string, action: 'approve' | 'reject') {
    setActionLoading(id)
    try {
      const res = await fetch('/api/pricing/recommendations', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, action }),
      })
      const data = await res.json()
      if (data.success) {
        queryClient.invalidateQueries({ queryKey: ['pricing-recommendations'] })
      }
    } catch {
      alert(`Failed to ${action} recommendation`)
    } finally {
      setActionLoading(null)
    }
  }

  async function handleToggleRule(ruleId: string, currentActive: boolean) {
    try {
      await fetch('/api/pricing/rules', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: ruleId, is_active: !currentActive }),
      })
      queryClient.invalidateQueries({ queryKey: ['pricing-rules'] })
    } catch {
      alert('Failed to toggle rule')
    }
  }

  // ─── Chart data ───

  const demandChartData = recommendations.slice(0, 15).map((r: any) => ({
    name: r.service_number || 'N/A',
    score: r.demand_score || 0,
    change: r.fare_change_percent || 0,
    fill: (r.demand_score || 0) >= 71 ? '#ef4444' : (r.demand_score || 0) >= 41 ? '#f59e0b' : '#22c55e',
  }))

  // ─── Render ───

  return (
    <DashboardLayout>
      {/* Breadcrumb */}
      <div className="border-b bg-white px-6 py-3">
        <div className="flex items-center gap-2 text-sm text-gray-500">
          <Link href="/" className="hover:text-gray-700"><Home className="h-4 w-4" /></Link>
          <ArrowRight className="h-3 w-3" />
          <span className="font-medium text-gray-900">Dynamic Pricing</span>
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

      <div className="p-6 space-y-6">
        {/* Header with actions */}
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Dynamic Pricing</h1>
            <p className="text-sm text-gray-500 mt-1">
              AI-powered fare recommendations based on demand signals
            </p>
          </div>
          <div className="flex gap-3">
            <button
              onClick={handleGenerateRecommendations}
              disabled={generatingRecs}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {generatingRecs ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Zap className="h-4 w-4" />
              )}
              Generate Recommendations
            </button>
          </div>
        </div>

        {/* KPI Cards */}
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {/* Active Recommendations */}
          <div className="rounded-2xl bg-white p-5 shadow-lg border border-gray-100">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Active Recommendations</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{stats.total_pending || 0}</p>
              </div>
              <div className="rounded-xl bg-blue-100 p-3">
                <Activity className="h-5 w-5 text-blue-600" />
              </div>
            </div>
            <div className="mt-3 flex gap-3 text-xs">
              <span className="text-amber-600">{stats.increases || 0} increases</span>
              <span className="text-green-600">{stats.decreases || 0} decreases</span>
            </div>
          </div>

          {/* Avg Suggested Change */}
          <div className="rounded-2xl bg-white p-5 shadow-lg border border-gray-100">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Avg Suggested Change</p>
                <p className={`text-2xl font-bold mt-1 ${
                  (stats.avg_change_percent || 0) > 0 ? 'text-amber-600' : (stats.avg_change_percent || 0) < 0 ? 'text-green-600' : 'text-gray-900'
                }`}>
                  {(stats.avg_change_percent || 0) > 0 ? '+' : ''}{stats.avg_change_percent || 0}%
                </p>
              </div>
              <div className={`rounded-xl p-3 ${(stats.avg_change_percent || 0) >= 0 ? 'bg-amber-100' : 'bg-green-100'}`}>
                {(stats.avg_change_percent || 0) >= 0 ? (
                  <TrendingUp className="h-5 w-5 text-amber-600" />
                ) : (
                  <TrendingDown className="h-5 w-5 text-green-600" />
                )}
              </div>
            </div>
          </div>

          {/* Approved Today */}
          <div className="rounded-2xl bg-white p-5 shadow-lg border border-gray-100">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Approved Today</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{stats.approved_today || 0}</p>
              </div>
              <div className="rounded-xl bg-green-100 p-3">
                <CheckCircle className="h-5 w-5 text-green-600" />
              </div>
            </div>
          </div>

          {/* Revenue Impact */}
          <div className="rounded-2xl bg-white p-5 shadow-lg border border-gray-100">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Potential Revenue Uplift</p>
                <p className="text-2xl font-bold text-indigo-600 mt-1">
                  {formatCurrency(stats.total_revenue_impact || 0)}
                </p>
              </div>
              <div className="rounded-xl bg-indigo-100 p-3">
                <IndianRupee className="h-5 w-5 text-indigo-600" />
              </div>
            </div>
          </div>
        </div>

        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3">
          {/* Status Tabs */}
          <div className="flex rounded-lg border border-gray-200 bg-white overflow-hidden">
            {['pending', 'approved', 'rejected', 'all'].map((s) => (
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

          {/* Date filter */}
          <input
            type="date"
            value={dateFilter}
            onChange={(e) => setDateFilter(e.target.value)}
            className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          />
          {dateFilter && (
            <button
              onClick={() => setDateFilter('')}
              className="text-sm text-gray-500 hover:text-gray-700"
            >
              Clear date
            </button>
          )}

          {/* Rules toggle */}
          <button
            onClick={() => setShowRules(!showRules)}
            className="ml-auto flex items-center gap-2 px-4 py-2 rounded-lg border border-gray-200 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50 transition-colors"
          >
            <Settings className="h-4 w-4" />
            Pricing Rules
            {showRules ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </button>
        </div>

        {/* Pricing Rules Section (collapsible) */}
        {showRules && (
          <div className="rounded-xl border bg-white p-5">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-gray-900">Active Pricing Rules</h2>
              <span className="text-sm text-gray-500">{rules.filter((r: any) => r.is_active).length} active / {rules.length} total</span>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
              {rules.map((rule: any) => {
                const typeBadge = getRuleTypeBadge(rule.rule_type)
                const isIncrease = rule.multiplier > 1
                return (
                  <div
                    key={rule.id}
                    className={`rounded-xl border p-4 transition-all ${
                      rule.is_active ? 'border-gray-200 bg-white' : 'border-gray-100 bg-gray-50 opacity-60'
                    }`}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <h3 className="text-sm font-semibold text-gray-900 leading-tight">{rule.rule_name}</h3>
                      <button
                        onClick={() => handleToggleRule(rule.id, rule.is_active)}
                        className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors ${
                          rule.is_active ? 'bg-blue-600' : 'bg-gray-200'
                        }`}
                      >
                        <span className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition-transform ${
                          rule.is_active ? 'translate-x-4' : 'translate-x-0'
                        }`} />
                      </button>
                    </div>
                    <p className="text-xs text-gray-500 mt-1 line-clamp-2">{rule.description}</p>
                    <div className="mt-3 flex items-center gap-2">
                      <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${typeBadge.bg} ${typeBadge.text}`}>
                        {rule.rule_type.replace(/_/g, ' ')}
                      </span>
                      <span className={`text-xs font-bold ${isIncrease ? 'text-amber-600' : 'text-green-600'}`}>
                        {isIncrease ? '+' : ''}{((rule.multiplier - 1) * 100).toFixed(0)}%
                      </span>
                    </div>
                    <div className="mt-2 text-xs text-gray-400">Priority: {rule.priority}</div>
                  </div>
                )
              })}
            </div>
          </div>
        )}

        {/* Recommendations Table */}
        <div className="rounded-xl border bg-white shadow-sm">
          <div className="border-b px-5 py-4">
            <h2 className="text-lg font-semibold text-gray-900">Fare Recommendations</h2>
            <p className="text-sm text-gray-500 mt-0.5">
              {recommendations.length} recommendation{recommendations.length !== 1 ? 's' : ''} found
            </p>
          </div>

          {recsLoading ? (
            <div className="flex items-center justify-center py-20">
              <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
            </div>
          ) : recommendations.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-20 text-gray-400">
              <Shield className="h-12 w-12 mb-3" />
              <p className="text-lg font-medium">No recommendations found</p>
              <p className="text-sm mt-1">Generate recommendations or adjust filters</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b bg-gray-50/80">
                    <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-gray-600">Service</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-gray-600">Travel Date</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-gray-600">Departure</th>
                    <th className="px-4 py-3 text-right text-xs font-semibold uppercase tracking-wider text-gray-600">Current Fare</th>
                    <th className="px-4 py-3 text-right text-xs font-semibold uppercase tracking-wider text-gray-600">Recommended</th>
                    <th className="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-600">Change</th>
                    <th className="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-600">Demand</th>
                    <th className="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-600">Occupancy</th>
                    <th className="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-600">Departs In</th>
                    <th className="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-600">Rules</th>
                    {statusFilter === 'pending' && (
                      <th className="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-600">Actions</th>
                    )}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {recommendations.map((rec: any) => {
                    const trip = rec.jc_trips
                    const route = rec.jc_routes
                    const demandBadge = getDemandBadge(rec.demand_score)
                    const changeBadge = getChangeBadge(rec.fare_change_percent)
                    const ChangeIcon = changeBadge.icon
                    const matchedRules = (rec.applied_rules || []).length

                    return (
                      <tr key={rec.id} className="hover:bg-gray-50/50 transition-colors">
                        {/* Service */}
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-2">
                            <Bus className="h-4 w-4 text-gray-400" />
                            <div>
                              <div className="text-sm font-medium text-gray-900">{rec.service_number || '-'}</div>
                              <div className="text-xs text-gray-500">
                                {route?.origin || trip?.origin || '-'} → {route?.destination || trip?.destination || '-'}
                              </div>
                            </div>
                          </div>
                        </td>

                        {/* Travel Date */}
                        <td className="px-4 py-3 text-sm text-gray-700">
                          {rec.travel_date ? new Date(rec.travel_date + 'T00:00:00').toLocaleDateString('en-IN', {
                            day: '2-digit', month: 'short', year: 'numeric'
                          }) : '-'}
                        </td>

                        {/* Departure */}
                        <td className="px-4 py-3 text-sm text-gray-700">
                          {trip?.departure_time?.slice(0, 5) || '-'}
                        </td>

                        {/* Current Fare */}
                        <td className="px-4 py-3 text-right text-sm text-gray-700">
                          {formatCurrency(rec.current_base_fare)}
                        </td>

                        {/* Recommended Fare */}
                        <td className="px-4 py-3 text-right">
                          <span className={`text-sm font-bold ${
                            rec.fare_change_percent > 0 ? 'text-amber-600' : rec.fare_change_percent < 0 ? 'text-green-600' : 'text-gray-900'
                          }`}>
                            {formatCurrency(rec.recommended_fare)}
                          </span>
                        </td>

                        {/* Change % */}
                        <td className="px-4 py-3 text-center">
                          <span className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-semibold ${changeBadge.bg} ${changeBadge.text}`}>
                            <ChangeIcon className="h-3 w-3" />
                            {changeBadge.prefix}{rec.fare_change_percent}%
                          </span>
                        </td>

                        {/* Demand Score */}
                        <td className="px-4 py-3 text-center">
                          <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold ${demandBadge.bg} ${demandBadge.text}`}>
                            {demandBadge.label} ({rec.demand_score || 0})
                          </span>
                        </td>

                        {/* Occupancy */}
                        <td className="px-4 py-3">
                          <div className="flex flex-col items-center">
                            <span className="text-xs font-medium text-gray-700">{rec.occupancy_percent || 0}%</span>
                            <div className="mt-1 h-1.5 w-16 rounded-full bg-gray-200 overflow-hidden">
                              <div
                                className={`h-full rounded-full transition-all ${
                                  (rec.occupancy_percent || 0) >= 80 ? 'bg-red-500' :
                                  (rec.occupancy_percent || 0) >= 50 ? 'bg-yellow-500' : 'bg-green-500'
                                }`}
                                style={{ width: `${Math.min(rec.occupancy_percent || 0, 100)}%` }}
                              />
                            </div>
                          </div>
                        </td>

                        {/* Time to Departure */}
                        <td className="px-4 py-3 text-center">
                          <span className={`text-xs font-medium ${
                            (rec.hours_to_departure || 0) < 6 ? 'text-red-600' :
                            (rec.hours_to_departure || 0) < 24 ? 'text-amber-600' : 'text-gray-600'
                          }`}>
                            {formatHours(rec.hours_to_departure)}
                          </span>
                        </td>

                        {/* Matched Rules Count */}
                        <td className="px-4 py-3 text-center">
                          <span className="text-xs text-gray-500">{matchedRules} rule{matchedRules !== 1 ? 's' : ''}</span>
                        </td>

                        {/* Actions */}
                        {statusFilter === 'pending' && (
                          <td className="px-4 py-3 text-center">
                            <div className="flex items-center justify-center gap-2">
                              <button
                                onClick={() => handleAction(rec.id, 'approve')}
                                disabled={actionLoading === rec.id}
                                className="inline-flex items-center gap-1 rounded-lg bg-green-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-green-700 disabled:opacity-50 transition-colors"
                              >
                                {actionLoading === rec.id ? (
                                  <Loader2 className="h-3 w-3 animate-spin" />
                                ) : (
                                  <CheckCircle className="h-3 w-3" />
                                )}
                                Approve
                              </button>
                              <button
                                onClick={() => handleAction(rec.id, 'reject')}
                                disabled={actionLoading === rec.id}
                                className="inline-flex items-center gap-1 rounded-lg border border-gray-300 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50 transition-colors"
                              >
                                <XCircle className="h-3 w-3" />
                                Reject
                              </button>
                            </div>
                          </td>
                        )}
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Demand Score Chart */}
        {demandChartData.length > 0 && (
          <div className="rounded-xl border bg-white p-5 shadow-sm">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Demand Score by Service</h2>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={demandChartData}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis
                    dataKey="name"
                    axisLine={false}
                    tickLine={false}
                    tick={{ fontSize: 11 }}
                    angle={-45}
                    textAnchor="end"
                    height={60}
                  />
                  <YAxis
                    axisLine={false}
                    tickLine={false}
                    tick={{ fontSize: 12 }}
                    domain={[0, 100]}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#fff',
                      border: '1px solid #e2e8f0',
                      borderRadius: '12px',
                      fontSize: '12px',
                    }}
                    formatter={(value: number, name: string) => {
                      if (name === 'score') return [`${value}`, 'Demand Score']
                      return [`${value}%`, 'Fare Change']
                    }}
                  />
                  <Bar dataKey="score" name="score" radius={[4, 4, 0, 0]}>
                    {demandChartData.map((entry: any, index: number) => (
                      <Cell key={index} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-3 flex items-center justify-center gap-6 text-xs text-gray-500">
              <span className="flex items-center gap-1.5"><span className="h-3 w-3 rounded-full bg-green-500" /> Low (0-40)</span>
              <span className="flex items-center gap-1.5"><span className="h-3 w-3 rounded-full bg-yellow-500" /> Medium (41-70)</span>
              <span className="flex items-center gap-1.5"><span className="h-3 w-3 rounded-full bg-red-500" /> High (71-100)</span>
            </div>
          </div>
        )}

        {/* System Info */}
        <div className="rounded-xl border bg-gray-50 p-4">
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <AlertTriangle className="h-4 w-4" />
            <span>
              Recommendations are suggestions only. Approved recommendations must be manually applied in Bitla/ticketSimply.
              {globalConfig.pricing_enabled === false && (
                <span className="ml-2 font-medium text-red-600">Pricing engine is currently disabled.</span>
              )}
            </span>
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}
