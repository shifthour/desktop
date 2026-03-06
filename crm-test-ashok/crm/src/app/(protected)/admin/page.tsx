'use client'

import { useEffect, useState } from 'react'
import { Shield, Users, Database, Settings, Activity, AlertTriangle } from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { toast } from 'sonner'

interface SystemStats {
  totalUsers: number
  totalLeads: number
  totalActivities: number
  totalVisits: number
  systemHealth: 'healthy' | 'warning' | 'error'
  lastBackup: string | null
}

export default function AdminPage() {
  const [stats, setStats] = useState<SystemStats | null>(null)
  const [loading, setLoading] = useState(true)

  const fetchSystemStats = async () => {
    setLoading(true)
    try {
      const response = await fetch('/api/admin/stats')
      if (response.ok) {
        const data = await response.json()
        setStats(data)
      }
    } catch (error) {
      console.error('Failed to fetch system stats:', error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchSystemStats()
  }, [])

  const handleDataBackup = async () => {
    try {
      toast.info('Starting data backup...')
      const response = await fetch('/api/admin/backup', { method: 'POST' })
      if (response.ok) {
        toast.success('Data backup completed successfully')
        fetchSystemStats() // Refresh stats
      } else {
        toast.error('Backup failed')
      }
    } catch (error) {
      toast.error('An error occurred during backup')
    }
  }

  const handleSystemCleanup = async () => {
    try {
      toast.info('Starting system cleanup...')
      const response = await fetch('/api/admin/cleanup', { method: 'POST' })
      if (response.ok) {
        toast.success('System cleanup completed')
        fetchSystemStats() // Refresh stats
      } else {
        toast.error('Cleanup failed')
      }
    } catch (error) {
      toast.error('An error occurred during cleanup')
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-600"></div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white flex items-center">
            <Shield className="w-6 h-6 mr-2" />
            System Administration
          </h1>
          <p className="text-gray-600 dark:text-gray-400">
            Manage system settings, users, and monitor application health
          </p>
        </div>
      </div>

      {/* System Health */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Activity className="w-5 h-5 mr-2" />
            System Health
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <Badge 
                variant={stats?.systemHealth === 'healthy' ? 'default' : 'destructive'}
                className={
                  stats?.systemHealth === 'healthy' 
                    ? 'bg-green-100 text-green-800' 
                    : 'bg-red-100 text-red-800'
                }
              >
                {stats?.systemHealth === 'healthy' ? 'All Systems Operational' : 'Issues Detected'}
              </Badge>
              {stats?.systemHealth !== 'healthy' && (
                <AlertTriangle className="w-5 h-5 text-red-500" />
              )}
            </div>
            {stats?.lastBackup && (
              <div className="text-sm text-gray-600">
                Last backup: {new Date(stats.lastBackup).toLocaleString()}
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* System Statistics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <Users className="w-5 h-5 text-blue-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Total Users</p>
                <p className="text-2xl font-bold">{stats?.totalUsers || 0}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <Database className="w-5 h-5 text-green-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Total Leads</p>
                <p className="text-2xl font-bold">{stats?.totalLeads || 0}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <Activity className="w-5 h-5 text-purple-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Activities</p>
                <p className="text-2xl font-bold">{stats?.totalActivities || 0}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <Settings className="w-5 h-5 text-orange-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Site Visits</p>
                <p className="text-2xl font-bold">{stats?.totalVisits || 0}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Administration Tools */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Data Management</CardTitle>
            <CardDescription>Backup and maintenance operations</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <Button 
              onClick={handleDataBackup}
              className="w-full justify-start"
              variant="outline"
            >
              <Database className="w-4 h-4 mr-2" />
              Create Data Backup
            </Button>
            
            <Button 
              onClick={handleSystemCleanup}
              className="w-full justify-start"
              variant="outline"
            >
              <Settings className="w-4 h-4 mr-2" />
              System Cleanup
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>System Information</CardTitle>
            <CardDescription>Application and environment details</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-600">Environment:</span>
                <Badge variant="outline">Development</Badge>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Database:</span>
                <Badge variant="outline">SQLite</Badge>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Version:</span>
                <Badge variant="outline">1.0.0</Badge>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Framework:</span>
                <Badge variant="outline">Next.js 14</Badge>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Maintenance Notes */}
      <Card>
        <CardHeader>
          <CardTitle>Maintenance & Security</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="p-4 bg-yellow-50 dark:bg-yellow-950 rounded-lg">
              <h4 className="font-medium text-yellow-900 dark:text-yellow-100">Production Checklist</h4>
              <ul className="text-sm text-yellow-700 dark:text-yellow-300 mt-2 space-y-1">
                <li>• Change default salt and secrets in environment variables</li>
                <li>• Set up PostgreSQL for production database</li>
                <li>• Configure proper RESEND_API_KEY for email functionality</li>
                <li>• Set up Cloudflare Workers for cron automation</li>
                <li>• Enable proper logging and monitoring</li>
              </ul>
            </div>
            
            <div className="p-4 bg-blue-50 dark:bg-blue-950 rounded-lg">
              <h4 className="font-medium text-blue-900 dark:text-blue-100">Regular Maintenance</h4>
              <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">
                Run data backups weekly and system cleanup monthly for optimal performance.
                Monitor lead scores and update scoring algorithms based on conversion data.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}