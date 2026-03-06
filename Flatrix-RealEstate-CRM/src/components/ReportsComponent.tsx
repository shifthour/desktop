'use client'

import { useState, useEffect } from 'react'
import { 
  BarChart3, 
  TrendingUp, 
  TrendingDown,
  Users, 
  Home,
  DollarSign,
  Calendar,
  MapPin,
  FileText,
  Download,
  Filter,
  RefreshCw,
  PieChart,
  Target,
  Clock,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Building,
  UserCheck
} from 'lucide-react'
import { useLeads, useDeals, useProperties, useCommissions, useChannelPartners } from '@/hooks/useDatabase'
import { supabase } from '@/lib/supabase'
import toast from 'react-hot-toast'

interface ReportStats {
  // Lead Analytics
  totalLeads: number
  newLeads: number
  qualifiedLeads: number
  convertedLeads: number
  lostLeads: number
  leadConversionRate: number
  
  // Deal Analytics
  totalDeals: number
  activeDeals: number
  wonDeals: number
  lostDeals: number
  totalRevenue: number
  averageDealValue: number
  dealConversionRate: number
  
  // Site Visit Analytics
  totalSiteVisits: number
  completedSiteVisits: number
  pendingSiteVisits: number
  scheduledSiteVisits: number
  siteVisitConversionRate: number
  
  // Commission Analytics
  totalCommissions: number
  pendingCommissions: number
  approvedCommissions: number
  paidCommissions: number
  
  // Performance Analytics
  topPerformingProjects: Array<{name: string, deals: number, revenue: number}>
  leadsBySource: Array<{source: string, count: number}>
  monthlyTrends: Array<{month: string, leads: number, deals: number, revenue: number}>
}

export default function ReportsComponent() {
  const [stats, setStats] = useState<ReportStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [dateRange, setDateRange] = useState('30') // Days
  
  const { data: leads } = useLeads()
  const { data: deals } = useDeals()
  const { data: properties } = useProperties()
  const { data: commissions } = useCommissions()
  const { data: channelPartners } = useChannelPartners()

  const fetchReportStats = async () => {
    try {
      setRefreshing(true)
      
      const dateFilter = new Date()
      dateFilter.setDate(dateFilter.getDate() - parseInt(dateRange))
      
      // Fetch leads data
      const { data: leadsData } = await supabase
        .from('flatrix_leads')
        .select('*')
        .gte('created_at', dateFilter.toISOString())

      // Fetch deals data
      const { data: dealsData } = await supabase
        .from('flatrix_deals')
        .select('*')
        .gte('created_at', dateFilter.toISOString())

      // Fetch commissions data
      const { data: commissionsData } = await supabase
        .from('flatrix_commissions')
        .select('*')

      // Fetch properties data with project info
      const { data: propertiesData } = await supabase
        .from('flatrix_properties')
        .select(`
          *,
          project:flatrix_projects!project_id(name, location)
        `)

      // Calculate lead analytics
      const totalLeads = leadsData?.length || 0
      const newLeads = leadsData?.filter(l => l.status === 'NEW').length || 0
      const qualifiedLeads = leadsData?.filter(l => l.status === 'QUALIFIED').length || 0
      const convertedLeads = leadsData?.filter(l => l.status === 'CONVERTED').length || 0
      const lostLeads = leadsData?.filter(l => l.status === 'LOST').length || 0
      const leadConversionRate = totalLeads > 0 ? (convertedLeads / totalLeads) * 100 : 0

      // Calculate deal analytics
      const totalDeals = dealsData?.length || 0
      const activeDeals = dealsData?.filter(d => d.status === 'ACTIVE').length || 0
      const wonDeals = dealsData?.filter(d => d.status === 'WON').length || 0
      const lostDealsCount = dealsData?.filter(d => d.status === 'LOST').length || 0
      const totalRevenue = dealsData?.filter(d => d.status === 'WON').reduce((sum, d) => sum + d.deal_value, 0) || 0
      const averageDealValue = wonDeals > 0 ? totalRevenue / wonDeals : 0
      const dealConversionRate = totalDeals > 0 ? (wonDeals / totalDeals) * 100 : 0

      // Calculate site visit analytics
      const dealsWithSiteVisit = dealsData?.filter(d => (d as any).site_visit_status) || []
      const totalSiteVisits = dealsWithSiteVisit.length
      const completedSiteVisits = dealsWithSiteVisit.filter(d => (d as any).site_visit_status === 'COMPLETED').length
      const pendingSiteVisits = dealsWithSiteVisit.filter(d => (d as any).site_visit_status === 'NOT_VISITED').length
      const scheduledSiteVisits = dealsWithSiteVisit.filter(d => (d as any).site_visit_status === 'SCHEDULED').length
      const bookedAfterVisit = dealsWithSiteVisit.filter(d => (d as any).site_visit_status === 'COMPLETED' && (d as any).conversion_status === 'BOOKED').length
      const siteVisitConversionRate = completedSiteVisits > 0 ? (bookedAfterVisit / completedSiteVisits) * 100 : 0

      // Calculate commission analytics
      const totalCommissions = commissionsData?.reduce((sum, c) => sum + c.amount, 0) || 0
      const pendingCommissions = commissionsData?.filter(c => c.status === 'PENDING').reduce((sum, c) => sum + c.amount, 0) || 0
      const approvedCommissions = commissionsData?.filter(c => c.status === 'APPROVED').reduce((sum, c) => sum + c.amount, 0) || 0
      const paidCommissions = commissionsData?.filter(c => c.status === 'PAID').reduce((sum, c) => sum + c.amount, 0) || 0

      // Calculate top performing projects
      const projectDeals = dealsData?.reduce((acc: any, deal) => {
        const property = propertiesData?.find(p => p.id === deal.property_id)
        const projectName = property?.project?.name || 'Unknown Project'
        
        if (!acc[projectName]) {
          acc[projectName] = { deals: 0, revenue: 0 }
        }
        
        acc[projectName].deals += 1
        if (deal.status === 'WON') {
          acc[projectName].revenue += deal.deal_value
        }
        
        return acc
      }, {})

      const topPerformingProjects = Object.entries(projectDeals || {})
        .map(([name, data]: [string, any]) => ({ name, deals: data.deals, revenue: data.revenue }))
        .sort((a, b) => b.revenue - a.revenue)
        .slice(0, 5)

      // Calculate leads by source
      const sourceData = leadsData?.reduce((acc: any, lead) => {
        const source = lead.source || 'Unknown'
        acc[source] = (acc[source] || 0) + 1
        return acc
      }, {})

      const leadsBySource = Object.entries(sourceData || {})
        .map(([source, count]: [string, any]) => ({ source, count }))
        .sort((a, b) => b.count - a.count)

      // Calculate monthly trends (last 6 months)
      const monthlyTrends = []
      for (let i = 5; i >= 0; i--) {
        const monthDate = new Date()
        monthDate.setMonth(monthDate.getMonth() - i)
        const monthStart = new Date(monthDate.getFullYear(), monthDate.getMonth(), 1)
        const monthEnd = new Date(monthDate.getFullYear(), monthDate.getMonth() + 1, 0)
        
        const monthLeads = leadsData?.filter(l => 
          new Date(l.created_at) >= monthStart && new Date(l.created_at) <= monthEnd
        ).length || 0
        
        const monthDeals = dealsData?.filter(d => 
          new Date(d.created_at) >= monthStart && new Date(d.created_at) <= monthEnd
        ).length || 0
        
        const monthRevenue = dealsData?.filter(d => 
          d.status === 'WON' && new Date(d.created_at) >= monthStart && new Date(d.created_at) <= monthEnd
        ).reduce((sum, d) => sum + d.deal_value, 0) || 0
        
        monthlyTrends.push({
          month: monthDate.toLocaleDateString('en-US', { month: 'short', year: '2-digit' }),
          leads: monthLeads,
          deals: monthDeals,
          revenue: monthRevenue
        })
      }

      setStats({
        totalLeads,
        newLeads,
        qualifiedLeads,
        convertedLeads,
        lostLeads,
        leadConversionRate,
        totalDeals,
        activeDeals,
        wonDeals,
        lostDeals: lostDealsCount,
        totalRevenue,
        averageDealValue,
        dealConversionRate,
        totalSiteVisits,
        completedSiteVisits,
        pendingSiteVisits,
        scheduledSiteVisits,
        siteVisitConversionRate,
        totalCommissions,
        pendingCommissions,
        approvedCommissions,
        paidCommissions,
        topPerformingProjects,
        leadsBySource,
        monthlyTrends
      })

    } catch (error) {
      console.error('Error fetching report stats:', error)
      toast.error('Failed to fetch report data')
    } finally {
      setRefreshing(false)
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchReportStats()
  }, [dateRange])

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(amount)
  }

  const formatPercentage = (value: number) => {
    return `${value.toFixed(1)}%`
  }

  const exportReport = () => {
    if (!stats) return
    
    const reportData = {
      generatedAt: new Date().toISOString(),
      dateRange: `Last ${dateRange} days`,
      stats
    }
    
    const blob = new Blob([JSON.stringify(reportData, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `flatrix-report-${new Date().toISOString().split('T')[0]}.json`
    a.click()
    URL.revokeObjectURL(url)
    
    toast.success('Report exported successfully!')
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    )
  }

  if (!stats) {
    return (
      <div className="text-center py-12">
        <AlertTriangle className="h-12 w-12 text-orange-500 mx-auto mb-4" />
        <p className="text-gray-500">Unable to load report data</p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Reports & Analytics</h1>
          <p className="text-gray-600 mt-2">Comprehensive business insights and performance metrics</p>
        </div>
        
        <div className="flex items-center space-x-4">
          <select
            value={dateRange}
            onChange={(e) => setDateRange(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            <option value="7">Last 7 days</option>
            <option value="30">Last 30 days</option>
            <option value="90">Last 90 days</option>
            <option value="365">Last year</option>
          </select>
          
          <button
            onClick={fetchReportStats}
            disabled={refreshing}
            className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
            <span>Refresh</span>
          </button>
          
          <button
            onClick={exportReport}
            className="flex items-center space-x-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
          >
            <Download className="h-4 w-4" />
            <span>Export</span>
          </button>
        </div>
      </div>

      {/* Key Metrics Overview */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Total Revenue</p>
              <p className="text-3xl font-bold text-green-600">{formatCurrency(stats.totalRevenue)}</p>
              <p className="text-sm text-gray-500 mt-1">From {stats.wonDeals} won deals</p>
            </div>
            <DollarSign className="h-12 w-12 text-green-500" />
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Total Leads</p>
              <p className="text-3xl font-bold text-blue-600">{stats.totalLeads}</p>
              <p className="text-sm text-gray-500 mt-1">{formatPercentage(stats.leadConversionRate)} conversion rate</p>
            </div>
            <Users className="h-12 w-12 text-blue-500" />
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Active Deals</p>
              <p className="text-3xl font-bold text-orange-600">{stats.activeDeals}</p>
              <p className="text-sm text-gray-500 mt-1">Avg: {formatCurrency(stats.averageDealValue)}</p>
            </div>
            <TrendingUp className="h-12 w-12 text-orange-500" />
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Site Visit Conversion</p>
              <p className="text-3xl font-bold text-purple-600">{formatPercentage(stats.siteVisitConversionRate)}</p>
              <p className="text-sm text-gray-500 mt-1">{stats.completedSiteVisits} completed visits</p>
            </div>
            <MapPin className="h-12 w-12 text-purple-500" />
          </div>
        </div>
      </div>

      {/* Lead Analytics */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
            <Users className="h-5 w-5 mr-2" />
            Lead Status Distribution
          </h3>
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <div className="w-3 h-3 bg-blue-500 rounded-full mr-3"></div>
                <span className="text-sm text-gray-600">New Leads</span>
              </div>
              <span className="font-semibold">{stats.newLeads}</span>
            </div>
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <div className="w-3 h-3 bg-yellow-500 rounded-full mr-3"></div>
                <span className="text-sm text-gray-600">Qualified</span>
              </div>
              <span className="font-semibold">{stats.qualifiedLeads}</span>
            </div>
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <div className="w-3 h-3 bg-green-500 rounded-full mr-3"></div>
                <span className="text-sm text-gray-600">Converted</span>
              </div>
              <span className="font-semibold">{stats.convertedLeads}</span>
            </div>
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <div className="w-3 h-3 bg-red-500 rounded-full mr-3"></div>
                <span className="text-sm text-gray-600">Lost</span>
              </div>
              <span className="font-semibold">{stats.lostLeads}</span>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
            <TrendingUp className="h-5 w-5 mr-2" />
            Deal Performance
          </h3>
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <CheckCircle className="h-4 w-4 text-green-500 mr-3" />
                <span className="text-sm text-gray-600">Won Deals</span>
              </div>
              <span className="font-semibold text-green-600">{stats.wonDeals}</span>
            </div>
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <Clock className="h-4 w-4 text-orange-500 mr-3" />
                <span className="text-sm text-gray-600">Active Deals</span>
              </div>
              <span className="font-semibold text-orange-600">{stats.activeDeals}</span>
            </div>
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <XCircle className="h-4 w-4 text-red-500 mr-3" />
                <span className="text-sm text-gray-600">Lost Deals</span>
              </div>
              <span className="font-semibold text-red-600">{stats.lostDeals}</span>
            </div>
            <div className="pt-2 border-t">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-600">Conversion Rate</span>
                <span className="font-bold text-blue-600">{formatPercentage(stats.dealConversionRate)}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Commission Analytics */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-6 flex items-center">
          <DollarSign className="h-5 w-5 mr-2" />
          Commission Overview
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="text-center p-4 bg-blue-50 rounded-lg">
            <p className="text-2xl font-bold text-blue-600">{formatCurrency(stats.totalCommissions)}</p>
            <p className="text-sm text-gray-600">Total Commissions</p>
          </div>
          <div className="text-center p-4 bg-yellow-50 rounded-lg">
            <p className="text-2xl font-bold text-yellow-600">{formatCurrency(stats.pendingCommissions)}</p>
            <p className="text-sm text-gray-600">Pending</p>
          </div>
          <div className="text-center p-4 bg-orange-50 rounded-lg">
            <p className="text-2xl font-bold text-orange-600">{formatCurrency(stats.approvedCommissions)}</p>
            <p className="text-sm text-gray-600">Approved</p>
          </div>
          <div className="text-center p-4 bg-green-50 rounded-lg">
            <p className="text-2xl font-bold text-green-600">{formatCurrency(stats.paidCommissions)}</p>
            <p className="text-sm text-gray-600">Paid</p>
          </div>
        </div>
      </div>

      {/* Top Performing Projects */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-6 flex items-center">
          <Building className="h-5 w-5 mr-2" />
          Top Performing Projects
        </h3>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Project</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Deals</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Revenue</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {stats.topPerformingProjects.map((project, index) => (
                <tr key={index}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {project.name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {project.deals}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {formatCurrency(project.revenue)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Lead Sources */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-6 flex items-center">
          <Target className="h-5 w-5 mr-2" />
          Lead Sources Performance
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {stats.leadsBySource.slice(0, 6).map((source, index) => (
            <div key={index} className="p-4 border rounded-lg">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-600">{source.source}</span>
                <span className="text-lg font-bold text-blue-600">{source.count}</span>
              </div>
              <div className="mt-2 bg-gray-200 rounded-full h-2">
                <div 
                  className="bg-blue-600 h-2 rounded-full" 
                  style={{ width: `${(source.count / stats.totalLeads) * 100}%` }}
                ></div>
              </div>
              <p className="text-xs text-gray-500 mt-1">
                {formatPercentage((source.count / stats.totalLeads) * 100)} of total leads
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Monthly Trends */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-6 flex items-center">
          <BarChart3 className="h-5 w-5 mr-2" />
          Monthly Trends (Last 6 Months)
        </h3>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Month</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Leads</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Deals</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Revenue</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {stats.monthlyTrends.map((month, index) => (
                <tr key={index}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {month.month}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {month.leads}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {month.deals}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {formatCurrency(month.revenue)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}