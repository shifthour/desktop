'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { DataTable } from '@/components/ui/data-table'
import { StatusBadge } from '@/components/ui/status-badge'
import { formatDateTime, timeAgo } from '@/lib/utils'
import {
  Webhook,
  RefreshCw,
  CheckCircle,
  XCircle,
  Clock,
  Eye,
  RotateCcw,
  Ticket,
  Ban,
  Users,
  Bus,
  AlertTriangle,
  Search,
} from 'lucide-react'

async function fetchWebhookEvents(page: number, type: string, processed: string, pnrSearch: string) {
  const params = new URLSearchParams({
    page: page.toString(),
    limit: '50',
    type,
    processed,
    pnr: pnrSearch,
  })
  const res = await fetch(`/api/webhooks/events?${params}`)
  if (!res.ok) throw new Error('Failed to fetch webhook events')
  return res.json()
}

async function retryWebhookEvent(eventId: string) {
  const res = await fetch('/api/webhooks/events', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ event_id: eventId }),
  })
  if (!res.ok) throw new Error('Failed to retry event')
  return res.json()
}

const eventTypeIcons: Record<string, any> = {
  booking: Ticket,
  cancellation: Ban,
  boarding_status: Users,
  'boarding-status': Users,
  boarding: Users,
  boarded: Users,
  vehicle_assign: Bus,
  'vehicle-assign': Bus,
  'bus-assign': Bus,
  service_update: AlertTriangle,
  'service-update': AlertTriangle,
  release: Ban,
  released: Ban,
  'ticket-release': Ban,
  cancel: Ban,
  cancelled: Ban,
}

const eventTypeColors: Record<string, string> = {
  booking: 'bg-blue-100 text-blue-600',
  cancellation: 'bg-red-100 text-red-600',
  boarding_status: 'bg-purple-100 text-purple-600',
  'boarding-status': 'bg-purple-100 text-purple-600',
  boarding: 'bg-purple-100 text-purple-600',
  boarded: 'bg-purple-100 text-purple-600',
  vehicle_assign: 'bg-green-100 text-green-600',
  'vehicle-assign': 'bg-green-100 text-green-600',
  'bus-assign': 'bg-green-100 text-green-600',
  service_update: 'bg-orange-100 text-orange-600',
  'service-update': 'bg-orange-100 text-orange-600',
  release: 'bg-red-100 text-red-600',
  released: 'bg-red-100 text-red-600',
  'ticket-release': 'bg-red-100 text-red-600',
  cancel: 'bg-red-100 text-red-600',
  cancelled: 'bg-red-100 text-red-600',
}

export default function WebhooksPage() {
  const [page, setPage] = useState(1)
  const [type, setType] = useState('')
  const [processed, setProcessed] = useState('')
  const [pnrSearch, setPnrSearch] = useState('')
  const [pnrInput, setPnrInput] = useState('')
  const [selectedEvent, setSelectedEvent] = useState<any>(null)

  const queryClient = useQueryClient()

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['webhook-events', page, type, processed, pnrSearch],
    queryFn: () => fetchWebhookEvents(page, type, processed, pnrSearch),
    refetchInterval: 5000, // Refresh every 5 seconds
  })

  const handlePnrSearch = () => {
    setPnrSearch(pnrInput.trim())
    setPage(1) // Reset to first page when searching
  }

  const clearPnrSearch = () => {
    setPnrInput('')
    setPnrSearch('')
    setPage(1)
  }

  const retryMutation = useMutation({
    mutationFn: retryWebhookEvent,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['webhook-events'] })
    },
  })

  const columns = [
    {
      key: 'event_type',
      header: 'Event Type',
      render: (event: any) => {
        const Icon = eventTypeIcons[event.event_type] || Webhook
        const colorClass = eventTypeColors[event.event_type] || 'bg-gray-100 text-gray-600'
        return (
          <div className="flex items-center gap-3">
            <div className={`flex h-10 w-10 items-center justify-center rounded-xl ${colorClass}`}>
              <Icon className="h-5 w-5" />
            </div>
            <div>
              <p className="font-medium text-gray-900 capitalize">
                {event.event_type.replace(/_/g, ' ')}
              </p>
              <p className="text-xs text-gray-500">{event.endpoint}</p>
            </div>
          </div>
        )
      },
    },
    {
      key: 'payload_preview',
      header: 'Payload Preview',
      render: (event: any) => {
        const payload = event.payload || {}
        // Check all possible PNR field names from Bitla
        const pnr = payload.travel_operator_pnr ||
                    payload.pnr_number ||
                    payload.ticket_number ||
                    payload.pnr ||
                    payload.ticketNumber ||
                    payload.PNR ||
                    payload.TicketNumber ||
                    '-'
        return (
          <div>
            <p className="text-sm font-medium">PNR: {pnr}</p>
            <p className="text-xs text-gray-500 truncate max-w-[200px]">
              {JSON.stringify(payload).slice(0, 50)}...
            </p>
          </div>
        )
      },
    },
    {
      key: 'source_ip',
      header: 'Source',
      render: (event: any) => (
        <span className="text-sm text-gray-600">{event.source_ip || '-'}</span>
      ),
    },
    {
      key: 'created_at',
      header: 'Received',
      render: (event: any) => (
        <div>
          <p className="text-sm">{timeAgo(event.created_at)}</p>
          <p className="text-xs text-gray-500">{formatDateTime(event.created_at)}</p>
        </div>
      ),
    },
    {
      key: 'processed',
      header: 'Status',
      render: (event: any) => {
        if (event.processing_error) {
          return <StatusBadge status="error" />
        }
        return <StatusBadge status={event.processed ? 'processed' : 'pending'} />
      },
    },
    {
      key: 'actions',
      header: '',
      render: (event: any) => (
        <div className="flex items-center gap-2">
          <button
            onClick={() => setSelectedEvent(event)}
            className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600"
            title="View details"
          >
            <Eye className="h-4 w-4" />
          </button>
          {!event.processed && (
            <button
              onClick={() => retryMutation.mutate(event.id)}
              disabled={retryMutation.isPending}
              className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 transition-colors hover:bg-blue-100 hover:text-blue-600 disabled:opacity-50"
              title="Retry"
            >
              <RotateCcw className={`h-4 w-4 ${retryMutation.isPending ? 'animate-spin' : ''}`} />
            </button>
          )}
        </div>
      ),
    },
  ]

  // Calculate stats
  const eventTypeCounts = data?.eventTypeCounts || {}

  return (
    <DashboardLayout>
      <Header
        title="Webhook Events"
        subtitle="Monitor real-time events from Bitla"
      />

      <div className="p-6">
        {/* Live Status Indicator */}
        <div className="mb-6 flex items-center gap-4 rounded-xl border bg-gradient-to-r from-green-50 to-emerald-50 p-4">
          <div className="pulse-dot green" />
          <div>
            <p className="font-medium text-gray-900">Webhook Receiver Active</p>
            <p className="text-sm text-gray-500">
              Listening for events at /api/webhooks/bitla/*
            </p>
          </div>
          <div className="ml-auto text-right">
            <p className="text-2xl font-bold text-green-600">{data?.pagination?.total || 0}</p>
            <p className="text-sm text-gray-500">Total Events</p>
          </div>
        </div>

        {/* Filters */}
        <div className="mb-6 flex flex-wrap items-center gap-4">
          {/* PNR Search */}
          <div className="relative flex items-center">
            <input
              type="text"
              placeholder="Search by PNR..."
              value={pnrInput}
              onChange={(e) => setPnrInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handlePnrSearch()}
              className="h-10 w-48 rounded-xl border border-gray-200 bg-white pl-10 pr-4 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
            />
            <Search className="absolute left-3 h-4 w-4 text-gray-400" />
            {pnrSearch && (
              <button
                onClick={clearPnrSearch}
                className="absolute right-3 text-gray-400 hover:text-gray-600"
              >
                ×
              </button>
            )}
          </div>
          <button
            onClick={handlePnrSearch}
            className="flex h-10 items-center gap-2 rounded-xl bg-gray-100 px-4 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-200"
          >
            Search
          </button>

          <select
            value={type}
            onChange={(e) => setType(e.target.value)}
            className="h-10 rounded-xl border border-gray-200 bg-white px-4 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            <option value="">All Types</option>
            <option value="booking">Booking</option>
            <option value="cancellation,cancel,cancelled">Cancellation</option>
            <option value="boarding,boarding_status,boarding-status,boarded">Boarding Status</option>
            <option value="vehicle_assign,vehicle-assign,bus-assign">Vehicle Assignment</option>
            <option value="service_update,service-update">Service Update</option>
            <option value="release,released,ticket-release">Release</option>
          </select>

          <select
            value={processed}
            onChange={(e) => setProcessed(e.target.value)}
            className="h-10 rounded-xl border border-gray-200 bg-white px-4 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            <option value="">All Status</option>
            <option value="true">Processed</option>
            <option value="false">Pending</option>
          </select>

          <button
            onClick={() => refetch()}
            className="flex h-10 items-center gap-2 rounded-xl bg-blue-600 px-4 text-sm font-medium text-white transition-colors hover:bg-blue-700"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        </div>

        {/* Active search indicator */}
        {pnrSearch && (
          <div className="mb-4 flex items-center gap-2 rounded-lg bg-blue-50 px-4 py-2 text-sm text-blue-700">
            <Search className="h-4 w-4" />
            Searching for PNR: <strong>{pnrSearch}</strong>
            <button
              onClick={clearPnrSearch}
              className="ml-2 text-blue-500 hover:text-blue-700 underline"
            >
              Clear search
            </button>
          </div>
        )}

        {/* Data Table */}
        <DataTable
          data={data?.events || []}
          columns={columns}
          loading={isLoading}
          emptyMessage="No webhook events received yet"
          pagination={{
            page,
            totalPages: data?.pagination?.totalPages || 1,
            total: data?.pagination?.total || 0,
            onPageChange: setPage,
          }}
        />
      </div>

      {/* Event Detail Modal */}
      {selectedEvent && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <div className="flex items-center gap-3">
                <div className={`flex h-10 w-10 items-center justify-center rounded-xl ${eventTypeColors[selectedEvent.event_type]}`}>
                  {(() => {
                    const Icon = eventTypeIcons[selectedEvent.event_type] || Webhook
                    return <Icon className="h-5 w-5" />
                  })()}
                </div>
                <div>
                  <h2 className="text-lg font-semibold capitalize">
                    {selectedEvent.event_type.replace(/_/g, ' ')} Event
                  </h2>
                  <p className="text-sm text-gray-500">{selectedEvent.id}</p>
                </div>
              </div>
              <button
                onClick={() => setSelectedEvent(null)}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100"
              >
                ×
              </button>
            </div>
            <div className="p-4 space-y-4">
              <div className="grid gap-4 sm:grid-cols-3">
                <div>
                  <p className="text-sm text-gray-500">Status</p>
                  <StatusBadge status={selectedEvent.processed ? 'processed' : 'pending'} />
                </div>
                <div>
                  <p className="text-sm text-gray-500">Received At</p>
                  <p className="font-medium">{formatDateTime(selectedEvent.created_at)}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Source IP</p>
                  <p className="font-medium">{selectedEvent.source_ip || '-'}</p>
                </div>
              </div>

              {/* Payload */}
              <div>
                <p className="mb-2 text-sm text-gray-500">Payload</p>
                <div className="rounded-lg bg-gray-900 p-4 overflow-x-auto">
                  <pre className="text-sm text-green-400">
                    {JSON.stringify(selectedEvent.payload, null, 2)}
                  </pre>
                </div>
              </div>

              {/* Error */}
              {selectedEvent.processing_error && (
                <div>
                  <p className="mb-2 text-sm text-red-500">Processing Error</p>
                  <div className="rounded-lg bg-red-50 p-4">
                    <p className="text-sm text-red-700">{selectedEvent.processing_error}</p>
                  </div>
                </div>
              )}

              {/* Actions */}
              {!selectedEvent.processed && (
                <div className="flex justify-end gap-2 pt-4 border-t">
                  <button
                    onClick={() => {
                      retryMutation.mutate(selectedEvent.id)
                      setSelectedEvent(null)
                    }}
                    className="flex items-center gap-2 rounded-xl bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
                  >
                    <RotateCcw className="h-4 w-4" />
                    Retry Processing
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </DashboardLayout>
  )
}
