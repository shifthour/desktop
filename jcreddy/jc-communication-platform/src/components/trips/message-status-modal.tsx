'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  X,
  Loader2,
  ChevronDown,
  ChevronRight,
  CheckCheck,
  Check,
  Send,
  XCircle,
  Phone,
  Clock,
  MessageSquare,
  RefreshCw,
} from 'lucide-react'
import { cn } from '@/lib/utils'

interface MessageStatusModalProps {
  isOpen: boolean
  onClose: () => void
  tripId: string
  tripInfo?: {
    origin: string
    destination: string
    travel_date: string
  }
}

interface MessageLog {
  id: string
  passenger_id: string
  passenger_name: string
  phone_number: string
  status: 'sent' | 'delivered' | 'read' | 'failed'
  sent_at: string
  delivered_at: string | null
  read_at: string | null
  failed_at: string | null
  error_message: string | null
}

interface MessageBatch {
  id: string
  template_name: string
  campaign_name: string
  sent_at: string
  total_count: number
  sent_count: number
  delivered_count: number
  read_count: number
  failed_count: number
  messages: MessageLog[]
}

async function fetchTripMessages(tripId: string) {
  console.log('[MessageStatusModal] Fetching messages for tripId:', tripId)
  const res = await fetch(`/api/trips/${tripId}/messages`)
  if (!res.ok) throw new Error('Failed to fetch messages')
  const data = await res.json()
  console.log('[MessageStatusModal] Response:', data)
  return data
}

export function MessageStatusModal({ isOpen, onClose, tripId, tripInfo }: MessageStatusModalProps) {
  const [expandedBatches, setExpandedBatches] = useState<Set<string>>(new Set())

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['tripMessages', tripId],
    queryFn: () => fetchTripMessages(tripId),
    enabled: isOpen && !!tripId,
    refetchInterval: 10000, // Auto-refresh every 10 seconds
    staleTime: 0, // Always consider data stale
    gcTime: 0, // Don't cache (formerly cacheTime)
    refetchOnWindowFocus: true,
  })

  if (!isOpen) return null

  const batches: MessageBatch[] = data?.batches || []
  const debug = data?.debug
  const stats = data?.stats || {}

  const toggleBatch = (batchId: string) => {
    const newExpanded = new Set(expandedBatches)
    if (newExpanded.has(batchId)) {
      newExpanded.delete(batchId)
    } else {
      newExpanded.add(batchId)
    }
    setExpandedBatches(newExpanded)
  }

  const formatDateTime = (dateStr: string | null) => {
    if (!dateStr) return '-'
    const date = new Date(dateStr)
    return date.toLocaleString('en-IN', {
      day: '2-digit',
      month: 'short',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
    })
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'read':
        return <CheckCheck className="h-4 w-4 text-blue-500" />
      case 'delivered':
        return <Check className="h-4 w-4 text-green-500" />
      case 'sent':
        return <Send className="h-4 w-4 text-gray-400" />
      case 'failed':
        return <XCircle className="h-4 w-4 text-red-500" />
      default:
        return <Clock className="h-4 w-4 text-gray-400" />
    }
  }

  const getStatusBadge = (status: string) => {
    const styles: Record<string, string> = {
      read: 'bg-blue-100 text-blue-700',
      delivered: 'bg-green-100 text-green-700',
      sent: 'bg-gray-100 text-gray-600',
      failed: 'bg-red-100 text-red-700',
    }
    return (
      <span className={cn('px-2 py-0.5 rounded text-xs font-medium capitalize', styles[status] || styles.sent)}>
        {status}
      </span>
    )
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />

      {/* Modal */}
      <div className="relative w-full max-w-4xl max-h-[90vh] bg-white rounded-xl shadow-2xl flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b bg-gradient-to-r from-purple-50 to-indigo-50 rounded-t-xl">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
              <MessageSquare className="h-5 w-5 text-purple-600" />
              Message Status
            </h2>
            {tripInfo && (
              <p className="text-sm text-gray-600">
                {tripInfo.origin} → {tripInfo.destination} | {tripInfo.travel_date}
              </p>
            )}
            <p className="text-xs text-gray-400">
              DB has {debug?.allBatchesInTable || 0} total batches, {debug?.batchesFoundForTrip || 0} for this trip
            </p>
            {debug?.directCheckTripIds && debug.directCheckTripIds.length > 0 && debug.batchesFoundForTrip === 0 && (
              <div className="text-xs text-red-500 mt-1">
                <p>Query tripId: "{tripId}" (len: {debug?.requestedTripIdLength})</p>
                <p>DB trip_ids:</p>
                {debug.directCheckTripIds.slice(0, 3).map((b: any, i: number) => (
                  <p key={i} className="ml-2">
                    - "{b.trip_id}" (len: {b.trip_id_length}) match: {b.matches ? 'YES' : 'NO'}
                  </p>
                ))}
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => refetch()}
              className="p-2 hover:bg-white/50 rounded-lg transition-colors"
              title="Refresh"
            >
              <RefreshCw className="h-5 w-5 text-gray-500" />
            </button>
            <button onClick={onClose} className="p-2 hover:bg-white/50 rounded-lg transition-colors">
              <X className="h-5 w-5 text-gray-500" />
            </button>
          </div>
        </div>

        {/* Overall Stats */}
        {stats.totalMessages > 0 && (
          <div className="p-4 border-b bg-gray-50">
            <div className="grid grid-cols-5 gap-4">
              <div className="text-center">
                <p className="text-2xl font-bold text-gray-900">{stats.totalMessages}</p>
                <p className="text-xs text-gray-500">Total</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-blue-600">{stats.totalRead}</p>
                <p className="text-xs text-gray-500 flex items-center justify-center gap-1">
                  <CheckCheck className="h-3 w-3" /> Read
                </p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-green-600">{stats.totalDelivered}</p>
                <p className="text-xs text-gray-500 flex items-center justify-center gap-1">
                  <Check className="h-3 w-3" /> Delivered
                </p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-gray-500">{stats.totalSent}</p>
                <p className="text-xs text-gray-500 flex items-center justify-center gap-1">
                  <Send className="h-3 w-3" /> Sent
                </p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-red-600">{stats.totalFailed}</p>
                <p className="text-xs text-gray-500 flex items-center justify-center gap-1">
                  <XCircle className="h-3 w-3" /> Failed
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4">
          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-purple-500" />
              <span className="ml-2 text-gray-600">Loading messages...</span>
            </div>
          ) : error ? (
            <div className="text-center py-12 text-red-500">Failed to load messages</div>
          ) : batches.length === 0 ? (
            <div className="text-center py-12">
              <MessageSquare className="h-12 w-12 text-gray-300 mx-auto mb-3" />
              <p className="text-gray-500">No messages sent for this trip yet</p>
              <p className="text-sm text-gray-400 mt-1">
                Use "View Passengers" to send messages
              </p>
            </div>
          ) : (
            <div className="space-y-4">
              {batches.map((batch) => {
                const isExpanded = expandedBatches.has(batch.id)
                return (
                  <div key={batch.id} className="border rounded-lg overflow-hidden">
                    {/* Batch Header */}
                    <div
                      className={cn(
                        'flex items-center justify-between p-4 cursor-pointer transition-colors',
                        isExpanded ? 'bg-purple-50' : 'bg-gray-50 hover:bg-gray-100'
                      )}
                      onClick={() => toggleBatch(batch.id)}
                    >
                      <div className="flex items-center gap-3">
                        {isExpanded ? (
                          <ChevronDown className="h-5 w-5 text-gray-400" />
                        ) : (
                          <ChevronRight className="h-5 w-5 text-gray-400" />
                        )}
                        <div>
                          <p className="font-semibold text-gray-900">{batch.template_name}</p>
                          <p className="text-xs text-gray-500">
                            Sent: {formatDateTime(batch.sent_at)} | {batch.total_count} messages
                          </p>
                        </div>
                      </div>
                      <div className="flex items-center gap-3">
                        <div className="flex items-center gap-2 text-sm">
                          <span className="flex items-center gap-1 text-blue-600">
                            <CheckCheck className="h-4 w-4" /> {batch.read_count}
                          </span>
                          <span className="flex items-center gap-1 text-green-600">
                            <Check className="h-4 w-4" /> {batch.delivered_count}
                          </span>
                          <span className="flex items-center gap-1 text-gray-500">
                            <Send className="h-4 w-4" /> {batch.sent_count}
                          </span>
                          {batch.failed_count > 0 && (
                            <span className="flex items-center gap-1 text-red-600">
                              <XCircle className="h-4 w-4" /> {batch.failed_count}
                            </span>
                          )}
                        </div>
                      </div>
                    </div>

                    {/* Message List */}
                    {isExpanded && (
                      <div className="border-t">
                        <table className="w-full">
                          <thead className="bg-gray-50">
                            <tr>
                              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Passenger</th>
                              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Phone</th>
                              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Status</th>
                              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Time</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y">
                            {batch.messages.map((msg) => (
                              <tr key={msg.id} className="hover:bg-gray-50">
                                <td className="px-4 py-3">
                                  <span className="font-medium text-gray-900">{msg.passenger_name}</span>
                                </td>
                                <td className="px-4 py-3">
                                  <span className="text-sm text-gray-600 flex items-center gap-1">
                                    <Phone className="h-3 w-3" />
                                    {msg.phone_number}
                                  </span>
                                </td>
                                <td className="px-4 py-3">
                                  <div className="flex items-center gap-2">
                                    {getStatusIcon(msg.status)}
                                    {getStatusBadge(msg.status)}
                                  </div>
                                  {msg.error_message && (
                                    <p className="text-xs text-red-500 mt-1">{msg.error_message}</p>
                                  )}
                                </td>
                                <td className="px-4 py-3 text-sm text-gray-500">
                                  {msg.status === 'read' && msg.read_at
                                    ? formatDateTime(msg.read_at)
                                    : msg.status === 'delivered' && msg.delivered_at
                                    ? formatDateTime(msg.delivered_at)
                                    : msg.status === 'failed' && msg.failed_at
                                    ? formatDateTime(msg.failed_at)
                                    : formatDateTime(msg.sent_at)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="border-t p-4 bg-gray-50 rounded-b-xl">
          <p className="text-xs text-gray-500 text-center">
            Status updates are received via webhook. Auto-refreshes every 30 seconds.
          </p>
        </div>
      </div>
    </div>
  )
}
