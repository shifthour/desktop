'use client'

import { useEffect, useState } from 'react'
import { BarChart3, TrendingUp, Users, Calendar, DollarSign, Target, Filter } from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { 
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

interface ReportData {
  totalLeads: number
  newLeads: number
  contactedLeads: number
  scheduledLeads: number
  visitedLeads: number
  bookedLeads: number
  conversionRate: number
  avgLeadScore: number
  sourceBreakdown: { [key: string]: number }
  stageBreakdown: { [key: string]: number }
  projectBreakdown?: { [key: string]: number }
}

interface Project {
  id: string
  name: string
  location: string | null
}

export default function ReportsPage() {
  const [reportData, setReportData] = useState<ReportData | null>(null)
  const [loading, setLoading] = useState(true)
  const [projects, setProjects] = useState<Project[]>([])
  const [timeFilter, setTimeFilter] = useState('all')
  const [projectFilter, setProjectFilter] = useState('all')

  const fetchReports = async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams()
      if (timeFilter !== 'all') params.set('timeFilter', timeFilter)
      if (projectFilter !== 'all') params.set('projectId', projectFilter)
      
      const response = await fetch(`/api/reports/overview?${params}`)
      if (response.ok) {
        const data = await response.json()
        setReportData(data)
      }
    } catch (error) {
      console.error('Failed to fetch reports:', error)
    } finally {
      setLoading(false)
    }
  }

  const fetchProjects = async () => {
    try {
      const response = await fetch('/api/projects')
      if (response.ok) {
        const data = await response.json()
        setProjects(data.projects || [])
      }
    } catch (error) {
      console.error('Failed to fetch projects:', error)
    }
  }

  useEffect(() => {
    fetchProjects()
  }, [])

  useEffect(() => {
    fetchReports()
  }, [timeFilter, projectFilter])

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-600"></div>
      </div>
    )
  }

  const conversionRate = reportData?.bookedLeads && reportData?.totalLeads 
    ? ((reportData.bookedLeads / reportData.totalLeads) * 100).toFixed(1)
    : '0.0'

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            Reports & Analytics
          </h1>
          <p className="text-gray-600 dark:text-gray-400">
            Track your sales performance and lead metrics
          </p>
        </div>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <Filter className="w-4 h-4 text-gray-500" />
              <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Filters:</span>
            </div>
            <Select value={timeFilter} onValueChange={setTimeFilter}>
              <SelectTrigger className="w-[180px]">
                <SelectValue placeholder="Time Period" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Time</SelectItem>
                <SelectItem value="weekly">This Week</SelectItem>
                <SelectItem value="monthly">This Month</SelectItem>
                <SelectItem value="quarterly">This Quarter</SelectItem>
                <SelectItem value="half_yearly">This Half Year</SelectItem>
                <SelectItem value="yearly">This Year</SelectItem>
              </SelectContent>
            </Select>
            <Select value={projectFilter} onValueChange={setProjectFilter}>
              <SelectTrigger className="w-[200px]">
                <SelectValue placeholder="Project" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Projects</SelectItem>
                {projects.map((project) => (
                  <SelectItem key={project.id} value={project.id}>
                    {project.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button 
              variant="outline" 
              size="sm"
              onClick={() => {
                setTimeFilter('all')
                setProjectFilter('all')
              }}
            >
              Clear Filters
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <Users className="w-5 h-5 text-blue-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Total Leads</p>
                <p className="text-2xl font-bold">{reportData?.totalLeads || 0}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <TrendingUp className="w-5 h-5 text-green-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Conversion Rate</p>
                <p className="text-2xl font-bold">{conversionRate}%</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <Target className="w-5 h-5 text-purple-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Avg Lead Score</p>
                <p className="text-2xl font-bold">{reportData?.avgLeadScore?.toFixed(0) || 0}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <DollarSign className="w-5 h-5 text-orange-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Bookings</p>
                <p className="text-2xl font-bold">{reportData?.bookedLeads || 0}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Detailed Breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Lead Sources */}
        <Card>
          <CardHeader>
            <CardTitle>Lead Sources</CardTitle>
            <CardDescription>Breakdown of leads by acquisition channel</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {reportData?.sourceBreakdown ? (
                Object.entries(reportData.sourceBreakdown).map(([source, count]) => {
                  const percentage = reportData.totalLeads > 0 
                    ? ((count / reportData.totalLeads) * 100).toFixed(1)
                    : '0.0'
                  
                  return (
                    <div key={source} className="flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <Badge variant="outline">
                          {source.replace('_', ' ').toUpperCase()}
                        </Badge>
                        <span className="text-sm text-gray-600">{count} leads</span>
                      </div>
                      <span className="font-medium">{percentage}%</span>
                    </div>
                  )
                })
              ) : (
                <p className="text-gray-500">No data available</p>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Lead Stages */}
        <Card>
          <CardHeader>
            <CardTitle>Lead Pipeline</CardTitle>
            <CardDescription>Current distribution of leads by stage</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {reportData?.stageBreakdown ? (
                Object.entries(reportData.stageBreakdown).map(([stage, count]) => {
                  const percentage = reportData.totalLeads > 0 
                    ? ((count / reportData.totalLeads) * 100).toFixed(1)
                    : '0.0'
                  
                  const stageColors: { [key: string]: string } = {
                    NEW: 'bg-blue-100 text-blue-800',
                    CONTACTED: 'bg-yellow-100 text-yellow-800',
                    SCHEDULED: 'bg-purple-100 text-purple-800',
                    VISITED: 'bg-green-100 text-green-800',
                    NEGOTIATION: 'bg-orange-100 text-orange-800',
                    BOOKED: 'bg-emerald-100 text-emerald-800',
                    DROPPED: 'bg-red-100 text-red-800',
                    RESCHEDULED: 'bg-amber-100 text-amber-800',
                    INVALID: 'bg-gray-100 text-gray-800'
                  }
                  
                  return (
                    <div key={stage} className="flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <Badge 
                          variant="secondary"
                          className={stageColors[stage] || 'bg-gray-100 text-gray-800'}
                        >
                          {stage}
                        </Badge>
                        <span className="text-sm text-gray-600">{count} leads</span>
                      </div>
                      <span className="font-medium">{percentage}%</span>
                    </div>
                  )
                })
              ) : (
                <p className="text-gray-500">No data available</p>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Project Breakdown */}
        <Card>
          <CardHeader>
            <CardTitle>Project Performance</CardTitle>
            <CardDescription>Lead distribution by project</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {reportData?.projectBreakdown ? (
                Object.entries(reportData.projectBreakdown).map(([projectName, count]) => {
                  const percentage = reportData.totalLeads > 0 
                    ? ((count / reportData.totalLeads) * 100).toFixed(1)
                    : '0.0'
                  
                  return (
                    <div key={projectName} className="flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <Badge variant="outline">
                          {projectName || 'No Project'}
                        </Badge>
                        <span className="text-sm text-gray-600">{count} leads</span>
                      </div>
                      <span className="font-medium">{percentage}%</span>
                    </div>
                  )
                })
              ) : (
                <p className="text-gray-500">No data available</p>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Performance Insights */}
      <Card>
        <CardHeader>
          <CardTitle>Performance Insights</CardTitle>
          <CardDescription>Key observations and recommendations</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="p-4 bg-blue-50 dark:bg-blue-950 rounded-lg">
              <h4 className="font-medium text-blue-900 dark:text-blue-100">Lead Quality</h4>
              <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">
                Average lead score is {reportData?.avgLeadScore?.toFixed(0) || 0} points. 
                Focus on higher-scoring leads for better conversion rates.
              </p>
            </div>
            
            <div className="p-4 bg-green-50 dark:bg-green-950 rounded-lg">
              <h4 className="font-medium text-green-900 dark:text-green-100">Conversion Performance</h4>
              <p className="text-sm text-green-700 dark:text-green-300 mt-1">
                Current conversion rate is {conversionRate}%. Industry benchmark is typically 2-5%.
              </p>
            </div>
            
            {reportData?.sourceBreakdown && (
              <div className="p-4 bg-purple-50 dark:bg-purple-950 rounded-lg">
                <h4 className="font-medium text-purple-900 dark:text-purple-100">Top Performing Channel</h4>
                <p className="text-sm text-purple-700 dark:text-purple-300 mt-1">
                  {Object.entries(reportData.sourceBreakdown)
                    .sort(([,a], [,b]) => b - a)[0]?.[0]?.replace('_', ' ').toUpperCase() || 'N/A'} 
                  is your highest volume lead source.
                </p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}