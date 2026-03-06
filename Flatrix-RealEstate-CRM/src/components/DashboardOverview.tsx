'use client'

import { 
  Users, 
  Home, 
  TrendingUp, 
  DollarSign,
  UserPlus,
  Calendar,
  Target,
  Award,
  Clock,
  MapPin,
  Building,
  BarChart3,
  TrendingDown,
  Eye,
  Phone,
  Mail,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Zap,
  Star,
  Activity,
  ArrowUpRight,
  ArrowDownRight,
  RefreshCw,
  Filter
} from 'lucide-react'
import { useState, useEffect } from 'react'
import { useLeads, useDeals, useProperties, useCommissions, useChannelPartners } from '@/hooks/useDatabase'
import { supabase } from '@/lib/supabase'

interface DashboardStats {
  // Financial KPIs
  totalRevenue: number
  monthlyRevenue: number
  avgDealValue: number
  revenueGrowth: number
  
  // Sales Performance
  totalLeads: number
  qualifiedLeads: number
  activeDeals: number
  wonDeals: number
  lostDeals: number
  leadConversionRate: number
  dealConversionRate: number
  
  // Site Visit Performance
  totalSiteVisits: number
  completedSiteVisits: number
  siteVisitConversionRate: number
  
  // Pipeline Health
  pipelineValue: number
  avgDealVelocity: number
  
  // Commission Tracking
  totalCommissions: number
  pendingCommissions: number
  approvedCommissions: number
  paidCommissions: number
  
  // Property Performance
  totalProperties: number
  availableProperties: number
  soldProperties: number
  
  // Team Performance
  activeChannelPartners: number
  topPerformingProject: string
  
  // Lead Sources
  leadSources: Array<{source: string, count: number, conversionRate: number}>
  
  // Recent Trends
  weeklyTrends: Array<{week: string, leads: number, deals: number, revenue: number}>
}

export default function DashboardOverview() {
  const [greeting, setGreeting] = useState('')
  const [stats, setStats] = useState<DashboardStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [timeRange, setTimeRange] = useState('30') // Days
  
  const { data: leads } = useLeads()
  const { data: deals } = useDeals()
  const { data: properties } = useProperties()
  const { data: commissions } = useCommissions()
  const { data: channelPartners } = useChannelPartners()

  useEffect(() => {
    const hour = new Date().getHours()
    if (hour < 12) setGreeting('Good Morning')
    else if (hour < 18) setGreeting('Good Afternoon')
    else setGreeting('Good Evening')
  }, [])

  const fetchDashboardStats = async () => {
    try {
      setRefreshing(true)
      
      const daysBack = parseInt(timeRange)
      const dateFilter = new Date()
      dateFilter.setDate(dateFilter.getDate() - daysBack)
      
      // Fetch recent data
      const { data: recentLeads } = await supabase
        .from('flatrix_leads')
        .select('*')
        .gte('created_at', dateFilter.toISOString())

      const { data: recentDeals } = await supabase
        .from('flatrix_deals')
        .select('*')
        .gte('created_at', dateFilter.toISOString())

      const { data: allDeals } = await supabase
        .from('flatrix_deals')
        .select('*')

      const { data: allLeads } = await supabase
        .from('flatrix_leads')
        .select('*')

      const { data: commissionsData } = await supabase
        .from('flatrix_commissions')
        .select('*')

      const { data: propertiesData } = await supabase
        .from('flatrix_properties')
        .select(`
          *,
          project:flatrix_projects!project_id(name, location)
        `)

      // Calculate financial KPIs
      const wonDeals = allDeals?.filter(d => d.status === 'WON') || []
      const totalRevenue = wonDeals.reduce((sum, d) => sum + d.deal_value, 0)
      const recentWonDeals = recentDeals?.filter(d => d.status === 'WON') || []
      const monthlyRevenue = recentWonDeals.reduce((sum, d) => sum + d.deal_value, 0)
      const avgDealValue = wonDeals.length > 0 ? totalRevenue / wonDeals.length : 0
      
      // Calculate growth
      const previousPeriodStart = new Date()
      previousPeriodStart.setDate(previousPeriodStart.getDate() - (daysBack * 2))
      const previousPeriodEnd = new Date()
      previousPeriodEnd.setDate(previousPeriodEnd.getDate() - daysBack)
      
      const { data: previousRevenue } = await supabase
        .from('flatrix_deals')
        .select('deal_value')
        .eq('status', 'WON')
        .gte('created_at', previousPeriodStart.toISOString())
        .lte('created_at', previousPeriodEnd.toISOString())
      
      const prevRevenue = previousRevenue?.reduce((sum, d) => sum + d.deal_value, 0) || 0
      const revenueGrowth = prevRevenue > 0 ? ((monthlyRevenue - prevRevenue) / prevRevenue) * 100 : 0

      // Sales performance
      const totalLeads = allLeads?.length || 0
      const qualifiedLeads = allLeads?.filter(l => l.status === 'QUALIFIED').length || 0
      const activeDeals = allDeals?.filter(d => d.status === 'ACTIVE').length || 0
      const lostDeals = allDeals?.filter(d => d.status === 'LOST').length || 0
      const convertedLeads = allLeads?.filter(l => l.status === 'CONVERTED').length || 0
      const leadConversionRate = totalLeads > 0 ? (convertedLeads / totalLeads) * 100 : 0
      const dealConversionRate = (allDeals?.length || 0) > 0 ? (wonDeals.length / (allDeals?.length || 1)) * 100 : 0

      // Site visit analytics
      const dealsWithSiteVisits = allDeals?.filter(d => (d as any).site_visit_status) || []
      const totalSiteVisits = dealsWithSiteVisits.length
      const completedSiteVisits = dealsWithSiteVisits.filter(d => (d as any).site_visit_status === 'COMPLETED').length
      const bookedAfterVisit = dealsWithSiteVisits.filter(d => 
        (d as any).site_visit_status === 'COMPLETED' && (d as any).conversion_status === 'BOOKED'
      ).length
      const siteVisitConversionRate = completedSiteVisits > 0 ? (bookedAfterVisit / completedSiteVisits) * 100 : 0

      // Pipeline health
      const pipelineValue = allDeals?.filter(d => d.status === 'ACTIVE').reduce((sum, d) => sum + d.deal_value, 0) || 0
      
      // Calculate average deal velocity (days from creation to won)
      const avgDealVelocity = wonDeals.length > 0 
        ? wonDeals.reduce((sum, deal) => {
            const created = new Date(deal.created_at)
            const won = new Date(deal.updated_at) // Assuming updated_at reflects when it was won
            const days = Math.floor((won.getTime() - created.getTime()) / (1000 * 60 * 60 * 24))
            return sum + days
          }, 0) / wonDeals.length
        : 0

      // Commission tracking
      const totalCommissions = commissionsData?.reduce((sum, c) => sum + c.amount, 0) || 0
      const pendingCommissions = commissionsData?.filter(c => c.status === 'PENDING').reduce((sum, c) => sum + c.amount, 0) || 0
      const approvedCommissions = commissionsData?.filter(c => c.status === 'APPROVED').reduce((sum, c) => sum + c.amount, 0) || 0
      const paidCommissions = commissionsData?.filter(c => c.status === 'PAID').reduce((sum, c) => sum + c.amount, 0) || 0

      // Property performance
      const totalProperties = propertiesData?.length || 0
      const availableProperties = propertiesData?.filter(p => p.status === 'AVAILABLE').length || 0
      const soldProperties = propertiesData?.filter(p => p.status === 'SOLD').length || 0

      // Team performance
      const activeChannelPartners = channelPartners?.filter(cp => cp.is_active).length || 0
      
      // Top performing project
      const projectPerformance = allDeals?.reduce((acc: any, deal) => {
        const property = propertiesData?.find(p => p.id === deal.property_id)
        const projectName = property?.project?.name || 'Unknown'
        acc[projectName] = (acc[projectName] || 0) + (deal.status === 'WON' ? deal.deal_value : 0)
        return acc
      }, {})
      
      const topPerformingProject = Object.entries(projectPerformance || {})
        .sort(([,a]: [string, any], [,b]: [string, any]) => b - a)[0]?.[0] || 'N/A'

      // Lead sources analysis
      const sourceAnalysis = allLeads?.reduce((acc: any, lead) => {
        const source = lead.source || 'Unknown'
        if (!acc[source]) {
          acc[source] = { total: 0, converted: 0 }
        }
        acc[source].total += 1
        if (lead.status === 'CONVERTED') {
          acc[source].converted += 1
        }
        return acc
      }, {})

      const leadSources = Object.entries(sourceAnalysis || {})
        .map(([source, data]: [string, any]) => ({
          source,
          count: data.total,
          conversionRate: data.total > 0 ? (data.converted / data.total) * 100 : 0
        }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 5)

      // Weekly trends (last 4 weeks)
      const weeklyTrends = []
      for (let i = 3; i >= 0; i--) {
        const weekStart = new Date()
        weekStart.setDate(weekStart.getDate() - (i * 7 + 7))
        const weekEnd = new Date()
        weekEnd.setDate(weekEnd.getDate() - (i * 7))
        
        const weekLeads = allLeads?.filter(l => {
          const created = new Date(l.created_at)
          return created >= weekStart && created <= weekEnd
        }).length || 0
        
        const weekDeals = allDeals?.filter(d => {
          const created = new Date(d.created_at)
          return created >= weekStart && created <= weekEnd
        }).length || 0
        
        const weekRevenue = allDeals?.filter(d => {
          const created = new Date(d.created_at)
          return d.status === 'WON' && created >= weekStart && created <= weekEnd
        }).reduce((sum, d) => sum + d.deal_value, 0) || 0
        
        weeklyTrends.push({
          week: `Week ${4 - i}`,
          leads: weekLeads,
          deals: weekDeals,
          revenue: weekRevenue
        })
      }

      setStats({
        totalRevenue,
        monthlyRevenue,
        avgDealValue,
        revenueGrowth,
        totalLeads,
        qualifiedLeads,
        activeDeals,
        wonDeals: wonDeals.length,
        lostDeals,
        leadConversionRate,
        dealConversionRate,
        totalSiteVisits,
        completedSiteVisits,
        siteVisitConversionRate,
        pipelineValue,
        avgDealVelocity,
        totalCommissions,
        pendingCommissions,
        approvedCommissions,
        paidCommissions,
        totalProperties,
        availableProperties,
        soldProperties,
        activeChannelPartners,
        topPerformingProject,
        leadSources,
        weeklyTrends
      })

    } catch (error) {
      console.error('Error fetching dashboard stats:', error)
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }

  useEffect(() => {
    fetchDashboardStats()
  }, [timeRange])

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(amount)
  }

  const formatCompactCurrency = (amount: number) => {
    if (amount >= 10000000) return `₹${(amount / 10000000).toFixed(1)}Cr`
    if (amount >= 100000) return `₹${(amount / 100000).toFixed(1)}L`
    return `₹${amount.toLocaleString()}`
  }

  const formatPercentage = (value: number) => {
    return `${value.toFixed(1)}%`
  }

  const getRecentActivities = () => {
    const activities = []
    
    // Recent leads
    const recentLeads = leads?.slice(0, 2).map(lead => ({
      id: lead.id,
      type: 'lead',
      message: `New lead: ${[lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown'} from ${lead.source || 'Direct'}`,
      time: new Date(lead.created_at).toLocaleDateString(),
      icon: UserPlus,
      color: 'bg-blue-100 text-blue-600'
    })) || []

    // Recent deals
    const recentDeals = deals?.slice(0, 2).map(deal => ({
      id: deal.id,
      type: 'deal',
      message: `Deal ${deal.deal_number} - ${formatCompactCurrency(deal.deal_value)}`,
      time: new Date(deal.created_at).toLocaleDateString(),
      icon: TrendingUp,
      color: 'bg-green-100 text-green-600'
    })) || []

    activities.push(...recentLeads, ...recentDeals)
    return activities.slice(0, 6)
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
        <p className="text-gray-500">Unable to load dashboard data</p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">{greeting}, Admin!</h1>
          <p className="text-gray-600 mt-2">Here's your real estate business performance overview</p>
        </div>
        
        <div className="flex items-center space-x-4">
          <select
            value={timeRange}
            onChange={(e) => setTimeRange(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            <option value="7">Last 7 days</option>
            <option value="30">Last 30 days</option>
            <option value="90">Last 90 days</option>
          </select>
          
          <button
            onClick={fetchDashboardStats}
            disabled={refreshing}
            className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
            <span>Refresh</span>
          </button>
        </div>
      </div>

      {/* Key Performance Indicators */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-gradient-to-r from-green-500 to-green-600 rounded-lg shadow p-6 text-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-green-100">Total Revenue</p>
              <p className="text-3xl font-bold">{formatCompactCurrency(stats.totalRevenue)}</p>
              <div className="flex items-center mt-2">
                {stats.revenueGrowth >= 0 ? (
                  <ArrowUpRight className="h-4 w-4 mr-1" />
                ) : (
                  <ArrowDownRight className="h-4 w-4 mr-1" />
                )}
                <span className="text-sm">{formatPercentage(Math.abs(stats.revenueGrowth))} vs previous period</span>
              </div>
            </div>
            <DollarSign className="h-12 w-12 text-green-200" />
          </div>
        </div>

        <div className="bg-gradient-to-r from-blue-500 to-blue-600 rounded-lg shadow p-6 text-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-blue-100">Active Pipeline</p>
              <p className="text-3xl font-bold">{formatCompactCurrency(stats.pipelineValue)}</p>
              <p className="text-sm text-blue-100 mt-2">{stats.activeDeals} active deals</p>
            </div>
            <Target className="h-12 w-12 text-blue-200" />
          </div>
        </div>

        <div className="bg-gradient-to-r from-purple-500 to-purple-600 rounded-lg shadow p-6 text-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-purple-100">Lead Conversion</p>
              <p className="text-3xl font-bold">{formatPercentage(stats.leadConversionRate)}</p>
              <p className="text-sm text-purple-100 mt-2">{stats.qualifiedLeads} qualified leads</p>
            </div>
            <Users className="h-12 w-12 text-purple-200" />
          </div>
        </div>

        <div className="bg-gradient-to-r from-orange-500 to-orange-600 rounded-lg shadow p-6 text-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-orange-100">Site Visit Conversion</p>
              <p className="text-3xl font-bold">{formatPercentage(stats.siteVisitConversionRate)}</p>
              <p className="text-sm text-orange-100 mt-2">{stats.completedSiteVisits} completed visits</p>
            </div>
            <MapPin className="h-12 w-12 text-orange-200" />
          </div>
        </div>
      </div>

      {/* Secondary KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <div className="bg-white rounded-lg shadow p-4 text-center">
          <div className="flex items-center justify-center mb-2">
            <TrendingUp className="h-8 w-8 text-green-500" />
          </div>
          <p className="text-2xl font-bold text-gray-900">{stats.wonDeals}</p>
          <p className="text-sm text-gray-600">Won Deals</p>
        </div>

        <div className="bg-white rounded-lg shadow p-4 text-center">
          <div className="flex items-center justify-center mb-2">
            <Clock className="h-8 w-8 text-blue-500" />
          </div>
          <p className="text-2xl font-bold text-gray-900">{Math.round(stats.avgDealVelocity)}</p>
          <p className="text-sm text-gray-600">Avg Deal Days</p>
        </div>

        <div className="bg-white rounded-lg shadow p-4 text-center">
          <div className="flex items-center justify-center mb-2">
            <Home className="h-8 w-8 text-purple-500" />
          </div>
          <p className="text-2xl font-bold text-gray-900">{stats.availableProperties}</p>
          <p className="text-sm text-gray-600">Available Units</p>
        </div>

        <div className="bg-white rounded-lg shadow p-4 text-center">
          <div className="flex items-center justify-center mb-2">
            <Building className="h-8 w-8 text-indigo-500" />
          </div>
          <p className="text-2xl font-bold text-gray-900">{stats.activeChannelPartners}</p>
          <p className="text-sm text-gray-600">Active Partners</p>
        </div>

        <div className="bg-white rounded-lg shadow p-4 text-center">
          <div className="flex items-center justify-center mb-2">
            <Award className="h-8 w-8 text-yellow-500" />
          </div>
          <p className="text-lg font-bold text-gray-900 truncate" title={stats.topPerformingProject}>
            {stats.topPerformingProject.length > 10 ? 
              stats.topPerformingProject.substring(0, 10) + '...' : 
              stats.topPerformingProject
            }
          </p>
          <p className="text-sm text-gray-600">Top Project</p>
        </div>

        <div className="bg-white rounded-lg shadow p-4 text-center">
          <div className="flex items-center justify-center mb-2">
            <BarChart3 className="h-8 w-8 text-red-500" />
          </div>
          <p className="text-2xl font-bold text-gray-900">{formatCompactCurrency(stats.avgDealValue)}</p>
          <p className="text-sm text-gray-600">Avg Deal Size</p>
        </div>
      </div>

      {/* Charts and Analytics */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Sales Funnel */}
        <div className="lg:col-span-2 bg-white rounded-lg shadow">
          <div className="p-6 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <BarChart3 className="h-5 w-5 mr-2" />
              Sales Funnel Performance
            </h2>
          </div>
          <div className="p-6">
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <div className="w-32 text-sm font-medium text-gray-700">Total Leads</div>
                  <div className="flex-1 bg-gray-200 rounded-full h-8">
                    <div className="bg-blue-500 h-8 rounded-full flex items-center justify-end pr-3" style={{width: '100%'}}>
                      <span className="text-white text-sm font-medium">{stats.totalLeads}</span>
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <div className="w-32 text-sm font-medium text-gray-700">Qualified</div>
                  <div className="flex-1 bg-gray-200 rounded-full h-8">
                    <div className="bg-yellow-500 h-8 rounded-full flex items-center justify-end pr-3" 
                         style={{width: `${stats.totalLeads > 0 ? (stats.qualifiedLeads / stats.totalLeads) * 100 : 0}%`}}>
                      <span className="text-white text-sm font-medium">{stats.qualifiedLeads}</span>
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <div className="w-32 text-sm font-medium text-gray-700">Active Deals</div>
                  <div className="flex-1 bg-gray-200 rounded-full h-8">
                    <div className="bg-orange-500 h-8 rounded-full flex items-center justify-end pr-3" 
                         style={{width: `${stats.totalLeads > 0 ? (stats.activeDeals / stats.totalLeads) * 100 : 0}%`}}>
                      <span className="text-white text-sm font-medium">{stats.activeDeals}</span>
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <div className="w-32 text-sm font-medium text-gray-700">Won Deals</div>
                  <div className="flex-1 bg-gray-200 rounded-full h-8">
                    <div className="bg-green-500 h-8 rounded-full flex items-center justify-end pr-3" 
                         style={{width: `${stats.totalLeads > 0 ? (stats.wonDeals / stats.totalLeads) * 100 : 0}%`}}>
                      <span className="text-white text-sm font-medium">{stats.wonDeals}</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Commission Overview */}
        <div className="bg-white rounded-lg shadow">
          <div className="p-6 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <DollarSign className="h-5 w-5 mr-2" />
              Commission Tracking
            </h2>
          </div>
          <div className="p-6">
            <div className="space-y-4">
              <div className="text-center p-4 bg-gray-50 rounded-lg">
                <p className="text-2xl font-bold text-gray-900">{formatCompactCurrency(stats.totalCommissions)}</p>
                <p className="text-sm text-gray-600">Total Commissions</p>
              </div>
              
              <div className="space-y-3">
                <div className="flex justify-between items-center">
                  <div className="flex items-center">
                    <div className="w-3 h-3 bg-yellow-500 rounded-full mr-2"></div>
                    <span className="text-sm text-gray-600">Pending</span>
                  </div>
                  <span className="text-sm font-medium">{formatCompactCurrency(stats.pendingCommissions)}</span>
                </div>
                
                <div className="flex justify-between items-center">
                  <div className="flex items-center">
                    <div className="w-3 h-3 bg-blue-500 rounded-full mr-2"></div>
                    <span className="text-sm text-gray-600">Approved</span>
                  </div>
                  <span className="text-sm font-medium">{formatCompactCurrency(stats.approvedCommissions)}</span>
                </div>
                
                <div className="flex justify-between items-center">
                  <div className="flex items-center">
                    <div className="w-3 h-3 bg-green-500 rounded-full mr-2"></div>
                    <span className="text-sm text-gray-600">Paid</span>
                  </div>
                  <span className="text-sm font-medium">{formatCompactCurrency(stats.paidCommissions)}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Lead Sources & Recent Activity */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Lead Sources Performance */}
        <div className="bg-white rounded-lg shadow">
          <div className="p-6 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <Target className="h-5 w-5 mr-2" />
              Top Lead Sources
            </h2>
          </div>
          <div className="p-6">
            <div className="space-y-4">
              {stats.leadSources.map((source, index) => (
                <div key={index} className="flex items-center justify-between">
                  <div className="flex-1">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-sm font-medium text-gray-700">{source.source}</span>
                      <span className="text-sm text-gray-500">{source.count} leads</span>
                    </div>
                    <div className="bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-blue-600 h-2 rounded-full" 
                        style={{ width: `${(source.count / stats.totalLeads) * 100}%` }}
                      ></div>
                    </div>
                    <div className="flex justify-between mt-1">
                      <span className="text-xs text-gray-500">
                        {formatPercentage((source.count / stats.totalLeads) * 100)} of total
                      </span>
                      <span className="text-xs text-green-600">
                        {formatPercentage(source.conversionRate)} conversion
                      </span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Recent Activities */}
        <div className="bg-white rounded-lg shadow">
          <div className="p-6 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <Activity className="h-5 w-5 mr-2" />
              Recent Activities
            </h2>
          </div>
          <div className="p-6">
            <div className="space-y-4">
              {getRecentActivities().map((activity, index) => (
                <div key={index} className="flex items-start space-x-3">
                  <div className={`p-2 rounded-lg ${activity.color}`}>
                    <activity.icon className="h-4 w-4" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-gray-900 truncate">{activity.message}</p>
                    <p className="text-xs text-gray-500 mt-1">{activity.time}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Weekly Trends */}
      <div className="bg-white rounded-lg shadow">
        <div className="p-6 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900 flex items-center">
            <TrendingUp className="h-5 w-5 mr-2" />
            Weekly Performance Trends
          </h2>
        </div>
        <div className="p-6">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Period</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Leads</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Deals</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Revenue</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {stats.weeklyTrends.map((week, index) => (
                  <tr key={index}>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                      {week.week}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {week.leads}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {week.deals}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {formatCompactCurrency(week.revenue)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}