'use client'

import { useEffect, useState } from 'react'
import { 
  Users, 
  Calendar, 
  TrendingUp, 
  DollarSign,
  Phone,
  MessageCircle,
  Clock
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'

interface DashboardStats {
  leadsToday: number
  siteVisitsToday: number
  bookingsThisMonth: number
  totalLeads: number
  overdueLeads: number
  averageCPL: number
  averageCPSV: number
  averageCPB: number
  conversionRate: number
  recentLeads: Array<{
    id: string
    name: string | null
    phone: string
    source: string
    stage: string
    score: number
    createdAt: string
  }>
}

export default function DashboardPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const fetchDashboardData = async () => {
      try {
        // Simulate API calls - in a real app, you'd have dedicated dashboard endpoints
        const today = new Date()
        
        // Fetch leads for today
        const leadsResponse = await fetch(`/api/leads?from=${today.toISOString().split('T')[0]}&limit=10`)
        const leadsData = await leadsResponse.json()
        
        // Fetch all leads for general stats
        const allLeadsResponse = await fetch('/api/leads?limit=1')
        const allLeadsData = await allLeadsResponse.json()
        
        // Simulate calculations (in real app, these would be separate endpoints)
        const mockStats: DashboardStats = {
          leadsToday: leadsData.leads?.length || 0,
          siteVisitsToday: 3, // Mock data
          bookingsThisMonth: 8, // Mock data
          totalLeads: allLeadsData.pagination?.total || 0,
          overdueLeads: 5, // Mock data
          averageCPL: 225,
          averageCPSV: 450,
          averageCPB: 2250,
          conversionRate: 12.5,
          recentLeads: leadsData.leads?.slice(0, 5) || []
        }
        
        setStats(mockStats)
      } catch (error) {
        console.error('Failed to fetch dashboard data:', error)
      } finally {
        setLoading(false)
      }
    }

    fetchDashboardData()
  }, [])

  if (loading) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold">Dashboard</h1>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {[...Array(4)].map((_, i) => (
            <Card key={i}>
              <CardContent className="p-6">
                <div className="animate-pulse">
                  <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
                  <div className="h-8 bg-gray-200 rounded w-1/2"></div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
          Dashboard
        </h1>
        <p className="text-gray-600 dark:text-gray-400">
          Welcome back! Here&apos;s what&apos;s happening with your leads today.
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              Leads Today
            </CardTitle>
            <Users className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.leadsToday || 0}</div>
            <p className="text-xs text-muted-foreground">
              +12% from yesterday
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              Site Visits Today
            </CardTitle>
            <Calendar className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.siteVisitsToday || 0}</div>
            <p className="text-xs text-muted-foreground">
              +5% from yesterday
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              Bookings MTD
            </CardTitle>
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.bookingsThisMonth || 0}</div>
            <p className="text-xs text-muted-foreground">
              +18% from last month
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              Conversion Rate
            </CardTitle>
            <DollarSign className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.conversionRate || 0}%</div>
            <p className="text-xs text-muted-foreground">
              +2.1% from last month
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Cost Per Metrics */}
      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Cost Per Lead</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">₹{stats?.averageCPL || 0}</div>
            <p className="text-xs text-muted-foreground">Average across all sources</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Cost Per Site Visit</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">₹{stats?.averageCPSV || 0}</div>
            <p className="text-xs text-muted-foreground">Average across all sources</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Cost Per Booking</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">₹{stats?.averageCPB || 0}</div>
            <p className="text-xs text-muted-foreground">Average across all sources</p>
          </CardContent>
        </Card>
      </div>

      {/* Recent Leads & Alerts */}
      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Recent Leads</CardTitle>
            <CardDescription>
              Latest leads that need your attention
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {stats?.recentLeads.map((lead) => (
                <div key={lead.id} className="flex items-center justify-between">
                  <div>
                    <div className="font-medium">{lead.name || 'Unknown'}</div>
                    <div className="text-sm text-gray-600 dark:text-gray-400">
                      {lead.source} • Score: {lead.score}
                    </div>
                  </div>
                  <div className="flex items-center space-x-2">
                    <Badge variant="secondary">{lead.stage}</Badge>
                    <div className="flex space-x-1">
                      <button className="p-1 hover:bg-gray-100 dark:hover:bg-gray-800 rounded">
                        <Phone className="w-4 h-4" />
                      </button>
                      <button className="p-1 hover:bg-gray-100 dark:hover:bg-gray-800 rounded">
                        <MessageCircle className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                </div>
              ))}
              {stats?.recentLeads.length === 0 && (
                <div className="text-center text-gray-500 py-4">
                  No recent leads
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Alerts & Reminders</CardTitle>
            <CardDescription>
              Important items requiring your attention
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {stats && stats.overdueLeads > 0 && (
                <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded">
                  <Clock className="w-4 h-4 text-red-500" />
                  <div>
                    <div className="font-medium text-red-700 dark:text-red-300">
                      {stats.overdueLeads} Overdue Leads
                    </div>
                    <div className="text-sm text-red-600 dark:text-red-400">
                      Follow up immediately to maintain SLA
                    </div>
                  </div>
                </div>
              )}
              
              <div className="flex items-center space-x-2 p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded">
                <Calendar className="w-4 h-4 text-blue-500" />
                <div>
                  <div className="font-medium text-blue-700 dark:text-blue-300">
                    Site Visit Reminders
                  </div>
                  <div className="text-sm text-blue-600 dark:text-blue-400">
                    3 visits scheduled for today
                  </div>
                </div>
              </div>

              <div className="flex items-center space-x-2 p-3 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded">
                <TrendingUp className="w-4 h-4 text-green-500" />
                <div>
                  <div className="font-medium text-green-700 dark:text-green-300">
                    Performance Update
                  </div>
                  <div className="text-sm text-green-600 dark:text-green-400">
                    Your conversion rate increased by 2.1%
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}