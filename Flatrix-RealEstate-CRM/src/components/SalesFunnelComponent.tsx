'use client'

import { useState, useEffect } from 'react'
import {
  TrendingDown,
  Users,
  UserCheck,
  MapPin,
  CheckCircle,
  XCircle,
  Calendar,
  RefreshCw,
  Download,
  Filter
} from 'lucide-react'
import toast from 'react-hot-toast'

interface FunnelData {
  totalLeads: number
  qualifiedLeads: number
  disqualifiedLeads: number
  siteVisitsScheduled: number
  siteVisitsCompleted: number
  booked: number
  wonDeals: number
  notBooked: number
  conversionRates: {
    leadsToQualified: string
    qualifiedToSiteVisit: string
    siteVisitToBooked: string
    leadsToBooked: string
  }
  dateRange: {
    start: string
    end: string
    preset: string
  }
}

const dateRangeOptions = [
  { label: '7 Days', value: '7_DAYS' },
  { label: '15 Days', value: '15_DAYS' },
  { label: '1 Month', value: '1_MONTH' },
  { label: '3 Months', value: '3_MONTHS' },
  { label: '6 Months', value: '6_MONTHS' },
  { label: '1 Year', value: '1_YEAR' },
]

export default function SalesFunnelComponent() {
  const [funnelData, setFunnelData] = useState<FunnelData | null>(null)
  const [loading, setLoading] = useState(true)
  const [selectedRange, setSelectedRange] = useState('1_MONTH')
  const [showCustomDate, setShowCustomDate] = useState(false)
  const [customStartDate, setCustomStartDate] = useState('')
  const [customEndDate, setCustomEndDate] = useState('')

  const fetchFunnelData = async (dateRange: string = selectedRange, customDates?: { start: string, end: string }) => {
    try {
      setLoading(true)
      let url = `/api/analytics/funnel?dateRange=${dateRange}`

      if (customDates) {
        url = `/api/analytics/funnel?startDate=${customDates.start}&endDate=${customDates.end}`
      }

      const response = await fetch(url)
      const result = await response.json()

      if (result.success) {
        setFunnelData(result.data)
      } else {
        toast.error('Failed to fetch funnel data')
      }
    } catch (error) {
      console.error('Error fetching funnel data:', error)
      toast.error('Error loading funnel data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchFunnelData()
  }, [])

  const handleDateRangeChange = (range: string) => {
    setSelectedRange(range)
    setShowCustomDate(false)
    fetchFunnelData(range)
  }

  const handleCustomDateApply = () => {
    if (!customStartDate || !customEndDate) {
      toast.error('Please select both start and end dates')
      return
    }
    fetchFunnelData('custom', { start: customStartDate, end: customEndDate })
  }

  const FunnelStage = ({
    title,
    count,
    icon: Icon,
    color,
    percentage,
    conversionRate
  }: {
    title: string
    count: number
    icon: any
    color: string
    percentage: number
    conversionRate?: string
  }) => {
    const maxWidth = 100 // Full width for first item
    const width = percentage

    return (
      <div className="mb-4">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2">
            <div className={`p-2 rounded-lg ${color}`}>
              <Icon size={20} className="text-white" />
            </div>
            <div>
              <h3 className="font-semibold text-gray-900">{title}</h3>
              {conversionRate && (
                <p className="text-sm text-gray-500">{conversionRate}% conversion</p>
              )}
            </div>
          </div>
          <div className="text-right">
            <p className="text-2xl font-bold text-gray-900">{count.toLocaleString()}</p>
            {percentage < 100 && (
              <p className="text-sm text-gray-500">{percentage.toFixed(1)}% of total</p>
            )}
          </div>
        </div>
        <div className="bg-gray-200 rounded-full h-12 overflow-hidden">
          <div
            className={`h-full ${color} flex items-center justify-center text-white font-semibold transition-all duration-500`}
            style={{ width: `${width}%` }}
          >
            {width > 15 && `${count.toLocaleString()}`}
          </div>
        </div>
      </div>
    )
  }

  const StatCard = ({
    title,
    value,
    icon: Icon,
    color,
    bgColor
  }: {
    title: string
    value: string | number
    icon: any
    color: string
    bgColor: string
  }) => (
    <div className={`${bgColor} rounded-lg p-6 border border-gray-200`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-600">{title}</p>
          <p className={`text-3xl font-bold mt-2 ${color}`}>{value}</p>
        </div>
        <div className={`p-3 rounded-full ${color.replace('text-', 'bg-').replace('700', '100')}`}>
          <Icon size={24} className={color} />
        </div>
      </div>
    </div>
  )

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <RefreshCw className="animate-spin text-blue-600" size={40} />
      </div>
    )
  }

  if (!funnelData) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <p className="text-gray-500">Failed to load funnel data</p>
      </div>
    )
  }

  const totalLeads = funnelData.totalLeads || 1 // Prevent division by zero

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 mb-2">Sales Funnel</h1>
            <p className="text-gray-600">Track your lead conversion through each stage</p>
          </div>
          <button
            onClick={() => fetchFunnelData()}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <RefreshCw size={16} />
            Refresh
          </button>
        </div>

        {/* Date Range Filter */}
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-4">
            <Filter size={20} className="text-gray-600" />
            <h3 className="font-semibold text-gray-900">Date Range</h3>
          </div>

          <div className="flex flex-wrap gap-2 mb-4">
            {dateRangeOptions.map((option) => (
              <button
                key={option.value}
                onClick={() => handleDateRangeChange(option.value)}
                className={`px-4 py-2 rounded-lg border transition-colors ${
                  selectedRange === option.value && !showCustomDate
                    ? 'bg-blue-600 text-white border-blue-600'
                    : 'bg-white text-gray-700 border-gray-300 hover:border-blue-600'
                }`}
              >
                {option.label}
              </button>
            ))}
            <button
              onClick={() => setShowCustomDate(!showCustomDate)}
              className={`px-4 py-2 rounded-lg border transition-colors ${
                showCustomDate
                  ? 'bg-blue-600 text-white border-blue-600'
                  : 'bg-white text-gray-700 border-gray-300 hover:border-blue-600'
              }`}
            >
              Custom Range
            </button>
          </div>

          {showCustomDate && (
            <div className="flex gap-4 items-end">
              <div className="flex-1">
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Start Date
                </label>
                <input
                  type="date"
                  value={customStartDate}
                  onChange={(e) => setCustomStartDate(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
              <div className="flex-1">
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  End Date
                </label>
                <input
                  type="date"
                  value={customEndDate}
                  onChange={(e) => setCustomEndDate(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
              <button
                onClick={handleCustomDateApply}
                className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                Apply
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          title="Total Leads"
          value={funnelData.totalLeads.toLocaleString()}
          icon={Users}
          color="text-blue-700"
          bgColor="bg-blue-50"
        />
        <StatCard
          title="Qualified Leads"
          value={funnelData.qualifiedLeads.toLocaleString()}
          icon={UserCheck}
          color="text-green-700"
          bgColor="bg-green-50"
        />
        <StatCard
          title="Site Visits"
          value={funnelData.siteVisitsCompleted.toLocaleString()}
          icon={MapPin}
          color="text-purple-700"
          bgColor="bg-purple-50"
        />
        <StatCard
          title="Booked"
          value={funnelData.booked.toLocaleString()}
          icon={CheckCircle}
          color="text-emerald-700"
          bgColor="bg-emerald-50"
        />
      </div>

      {/* Funnel Visualization */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 mb-8">
        <h2 className="text-xl font-bold text-gray-900 mb-6">Conversion Funnel</h2>

        <FunnelStage
          title="Total Leads"
          count={funnelData.totalLeads}
          icon={Users}
          color="bg-blue-600"
          percentage={100}
        />

        <FunnelStage
          title="Qualified Leads"
          count={funnelData.qualifiedLeads}
          icon={UserCheck}
          color="bg-green-600"
          percentage={(funnelData.qualifiedLeads / totalLeads) * 100}
          conversionRate={funnelData.conversionRates.leadsToQualified}
        />

        <FunnelStage
          title="Site Visits Completed"
          count={funnelData.siteVisitsCompleted}
          icon={MapPin}
          color="bg-purple-600"
          percentage={(funnelData.siteVisitsCompleted / totalLeads) * 100}
          conversionRate={funnelData.conversionRates.qualifiedToSiteVisit}
        />

        <FunnelStage
          title="Booked"
          count={funnelData.booked}
          icon={CheckCircle}
          color="bg-emerald-600"
          percentage={(funnelData.booked / totalLeads) * 100}
          conversionRate={funnelData.conversionRates.siteVisitToBooked}
        />

        <FunnelStage
          title="Won Deals"
          count={funnelData.wonDeals}
          icon={TrendingDown}
          color="bg-teal-600"
          percentage={(funnelData.wonDeals / totalLeads) * 100}
        />
      </div>

      {/* Additional Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 rounded-lg bg-red-100">
              <XCircle size={20} className="text-red-600" />
            </div>
            <h3 className="font-semibold text-gray-900">Disqualified</h3>
          </div>
          <p className="text-3xl font-bold text-red-600">{funnelData.disqualifiedLeads.toLocaleString()}</p>
          <p className="text-sm text-gray-500 mt-1">
            {((funnelData.disqualifiedLeads / totalLeads) * 100).toFixed(1)}% of total leads
          </p>
        </div>

        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 rounded-lg bg-yellow-100">
              <Calendar size={20} className="text-yellow-600" />
            </div>
            <h3 className="font-semibold text-gray-900">Scheduled Visits</h3>
          </div>
          <p className="text-3xl font-bold text-yellow-600">{funnelData.siteVisitsScheduled.toLocaleString()}</p>
          <p className="text-sm text-gray-500 mt-1">Pending site visits</p>
        </div>

        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 rounded-lg bg-orange-100">
              <XCircle size={20} className="text-orange-600" />
            </div>
            <h3 className="font-semibold text-gray-900">Not Booked</h3>
          </div>
          <p className="text-3xl font-bold text-orange-600">{funnelData.notBooked.toLocaleString()}</p>
          <p className="text-sm text-gray-500 mt-1">
            After site visit
          </p>
        </div>
      </div>

      {/* Overall Conversion Rate */}
      <div className="mt-8 bg-gradient-to-r from-blue-600 to-purple-600 rounded-lg p-6 text-white">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold mb-1">Overall Conversion Rate</h3>
            <p className="text-blue-100">From lead to booking</p>
          </div>
          <div className="text-right">
            <p className="text-5xl font-bold">{funnelData.conversionRates.leadsToBooked}%</p>
            <p className="text-blue-100 mt-1">{funnelData.booked} / {funnelData.totalLeads} leads</p>
          </div>
        </div>
      </div>
    </div>
  )
}
