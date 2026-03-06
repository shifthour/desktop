'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { StatusBadge } from '@/components/ui/status-badge'
import { formatDateTime, timeAgo } from '@/lib/utils'
import {
  RefreshCw,
  Database,
  CheckCircle,
  XCircle,
  Clock,
  Download,
  Upload,
  AlertTriangle,
  Play,
} from 'lucide-react'

async function fetchSyncStatus() {
  const res = await fetch('/api/sync')
  if (!res.ok) throw new Error('Failed to fetch sync status')
  return res.json()
}

async function triggerSync() {
  const res = await fetch('/api/sync', { method: 'POST' })
  if (!res.ok) throw new Error('Failed to trigger sync')
  return res.json()
}

export default function SyncPage() {
  const [syncResult, setSyncResult] = useState<any>(null)
  const queryClient = useQueryClient()

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['sync-status'],
    queryFn: fetchSyncStatus,
    refetchInterval: 10000,
  })

  const syncMutation = useMutation({
    mutationFn: triggerSync,
    onSuccess: (result) => {
      setSyncResult(result)
      queryClient.invalidateQueries({ queryKey: ['sync-status'] })
    },
    onError: (error) => {
      setSyncResult({ status: 'error', message: error.message })
    },
  })

  const recentSyncs = data?.recent_syncs || []

  return (
    <DashboardLayout>
      <Header title="Data Sync" subtitle="Synchronize data from Bitla API" />

      <div className="p-6">
        {/* Sync Action Card */}
        <div className="mb-8 rounded-2xl border bg-gradient-to-br from-blue-50 to-indigo-50 p-8">
          <div className="flex items-start justify-between">
            <div>
              <h2 className="text-2xl font-bold text-gray-900">Sync Data from Bitla</h2>
              <p className="mt-2 text-gray-600">
                Pull the latest routes, trips, and passenger data from Bitla ticketSimply API.
                This will sync data for today and the next 2 days.
              </p>
            </div>
            <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-white shadow-lg">
              <Database className="h-8 w-8 text-blue-600" />
            </div>
          </div>

          <div className="mt-6 flex items-center gap-4">
            <button
              onClick={() => syncMutation.mutate()}
              disabled={syncMutation.isPending}
              className="flex items-center gap-2 rounded-xl bg-blue-600 px-6 py-3 text-sm font-semibold text-white transition-all hover:bg-blue-700 disabled:opacity-50"
            >
              {syncMutation.isPending ? (
                <>
                  <RefreshCw className="h-5 w-5 animate-spin" />
                  Syncing...
                </>
              ) : (
                <>
                  <Play className="h-5 w-5" />
                  Start Full Sync
                </>
              )}
            </button>
            <button
              onClick={() => refetch()}
              className="flex items-center gap-2 rounded-xl border border-gray-300 bg-white px-4 py-3 text-sm font-medium text-gray-700 transition-all hover:bg-gray-50"
            >
              <RefreshCw className="h-4 w-4" />
              Refresh Status
            </button>
          </div>

          {/* Sync Result */}
          {syncResult && (
            <div
              className={`mt-6 rounded-xl p-4 ${
                syncResult.status === 'success'
                  ? 'bg-green-100 text-green-800'
                  : syncResult.status === 'error'
                  ? 'bg-red-100 text-red-800'
                  : 'bg-blue-100 text-blue-800'
              }`}
            >
              {syncResult.status === 'success' ? (
                <div className="flex items-center gap-3">
                  <CheckCircle className="h-5 w-5" />
                  <div>
                    <p className="font-medium">Sync Completed Successfully!</p>
                    <p className="text-sm">
                      Fetched: {syncResult.records_fetched} |
                      Created: {syncResult.records_created} |
                      Updated: {syncResult.records_updated} |
                      Failed: {syncResult.records_failed}
                    </p>
                  </div>
                </div>
              ) : syncResult.status === 'error' ? (
                <div className="flex items-center gap-3">
                  <XCircle className="h-5 w-5" />
                  <div>
                    <p className="font-medium">Sync Failed</p>
                    <p className="text-sm">{syncResult.message}</p>
                  </div>
                </div>
              ) : null}
            </div>
          )}
        </div>

        {/* Info Cards */}
        <div className="mb-8 grid gap-6 sm:grid-cols-3">
          <div className="rounded-xl border bg-white p-6">
            <div className="flex items-center gap-4">
              <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-green-100">
                <Download className="h-6 w-6 text-green-600" />
              </div>
              <div>
                <p className="text-sm text-gray-500">Data Pulled</p>
                <p className="text-2xl font-bold text-gray-900">
                  {recentSyncs.reduce((sum: number, s: any) => sum + (s.records_fetched || 0), 0)}
                </p>
              </div>
            </div>
          </div>
          <div className="rounded-xl border bg-white p-6">
            <div className="flex items-center gap-4">
              <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-blue-100">
                <Upload className="h-6 w-6 text-blue-600" />
              </div>
              <div>
                <p className="text-sm text-gray-500">Records Created</p>
                <p className="text-2xl font-bold text-gray-900">
                  {recentSyncs.reduce((sum: number, s: any) => sum + (s.records_created || 0), 0)}
                </p>
              </div>
            </div>
          </div>
          <div className="rounded-xl border bg-white p-6">
            <div className="flex items-center gap-4">
              <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-red-100">
                <AlertTriangle className="h-6 w-6 text-red-600" />
              </div>
              <div>
                <p className="text-sm text-gray-500">Failed Records</p>
                <p className="text-2xl font-bold text-gray-900">
                  {recentSyncs.reduce((sum: number, s: any) => sum + (s.records_failed || 0), 0)}
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Sync History */}
        <div className="rounded-2xl border bg-white">
          <div className="border-b p-4">
            <h3 className="text-lg font-semibold">Sync History</h3>
            <p className="text-sm text-gray-500">Recent synchronization jobs</p>
          </div>

          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <RefreshCw className="h-8 w-8 animate-spin text-blue-600" />
            </div>
          ) : recentSyncs.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12">
              <Database className="mb-4 h-12 w-12 text-gray-300" />
              <p className="text-gray-500">No sync history yet</p>
              <p className="text-sm text-gray-400">Run your first sync to see data here</p>
            </div>
          ) : (
            <div className="divide-y">
              {recentSyncs.map((sync: any) => (
                <div
                  key={sync.id}
                  className="flex items-center justify-between p-4 transition-colors hover:bg-gray-50"
                >
                  <div className="flex items-center gap-4">
                    <div
                      className={`flex h-10 w-10 items-center justify-center rounded-xl ${
                        sync.status === 'completed'
                          ? 'bg-green-100'
                          : sync.status === 'failed'
                          ? 'bg-red-100'
                          : sync.status === 'running'
                          ? 'bg-blue-100'
                          : 'bg-yellow-100'
                      }`}
                    >
                      {sync.status === 'completed' ? (
                        <CheckCircle className="h-5 w-5 text-green-600" />
                      ) : sync.status === 'failed' ? (
                        <XCircle className="h-5 w-5 text-red-600" />
                      ) : sync.status === 'running' ? (
                        <RefreshCw className="h-5 w-5 animate-spin text-blue-600" />
                      ) : (
                        <Clock className="h-5 w-5 text-yellow-600" />
                      )}
                    </div>
                    <div>
                      <p className="font-medium text-gray-900 capitalize">{sync.sync_type} Sync</p>
                      <p className="text-sm text-gray-500">
                        {sync.target_date ? `Target: ${sync.target_date}` : 'All routes'}
                      </p>
                    </div>
                  </div>

                  <div className="flex items-center gap-8">
                    {/* Stats */}
                    <div className="hidden sm:flex items-center gap-6 text-sm">
                      <div className="text-center">
                        <p className="font-semibold text-gray-900">{sync.records_fetched || 0}</p>
                        <p className="text-gray-500">Fetched</p>
                      </div>
                      <div className="text-center">
                        <p className="font-semibold text-green-600">{sync.records_created || 0}</p>
                        <p className="text-gray-500">Created</p>
                      </div>
                      <div className="text-center">
                        <p className="font-semibold text-blue-600">{sync.records_updated || 0}</p>
                        <p className="text-gray-500">Updated</p>
                      </div>
                      {sync.records_failed > 0 && (
                        <div className="text-center">
                          <p className="font-semibold text-red-600">{sync.records_failed}</p>
                          <p className="text-gray-500">Failed</p>
                        </div>
                      )}
                    </div>

                    {/* Time */}
                    <div className="text-right">
                      <StatusBadge status={sync.status} size="sm" />
                      <p className="mt-1 text-xs text-gray-500">
                        {timeAgo(sync.created_at)}
                      </p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* API Configuration Info */}
        <div className="mt-8 rounded-2xl border bg-white p-6">
          <h3 className="mb-4 text-lg font-semibold">API Configuration</h3>
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="rounded-lg bg-gray-50 p-4">
              <p className="text-sm text-gray-500">Base URL</p>
              <p className="font-mono text-sm">http://myth.mythribus.com/api/</p>
            </div>
            <div className="rounded-lg bg-gray-50 p-4">
              <p className="text-sm text-gray-500">Status</p>
              <div className="flex items-center gap-2">
                <div className="pulse-dot green" />
                <span className="text-sm font-medium text-green-600">Connected</span>
              </div>
            </div>
          </div>
          <p className="mt-4 text-sm text-gray-500">
            Configure API credentials in your environment variables (.env.local)
          </p>
        </div>
      </div>
    </DashboardLayout>
  )
}
