'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { DataTable } from '@/components/ui/data-table'
import { StatusBadge } from '@/components/ui/status-badge'
import { formatDateTime, formatDateDDMMYYYY, formatPhoneNumber, timeAgo } from '@/lib/utils'
import {
  Search,
  Filter,
  RefreshCw,
  MessageSquare,
  Phone,
  Mail,
  Send,
  CheckCircle,
  Clock,
  AlertTriangle,
  Eye,
  RotateCcw,
  Calendar,
  User,
  Wifi,
  History,
  Activity,
  Smartphone,
  PhoneCall,
  Globe,
  MessageCircle,
  CheckCheck,
  X,
} from 'lucide-react'

async function fetchNotifications(page: number, status: string, type: string, channel: string, search: string, date: string) {
  const params = new URLSearchParams({
    page: page.toString(),
    limit: '20',
    status,
    type,
    channel,
    search,
    date,
  })
  const res = await fetch(`/api/notifications?${params}`)
  if (!res.ok) throw new Error('Failed to fetch notifications')
  return res.json()
}

async function resendNotification(notificationId: string) {
  const res = await fetch('/api/notifications', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ notification_id: notificationId, action: 'resend' }),
  })
  if (!res.ok) throw new Error('Failed to resend notification')
  return res.json()
}

const channelIcons: Record<string, any> = {
  whatsapp: MessageSquare,
  wap: Globe,
  rcs: MessageCircle,
  sms: Smartphone,
  email: Mail,
  ivr: PhoneCall,
}

const typeLabels: Record<string, string> = {
  booking_confirmation: 'Booking Confirmation',
  reminder_24h: '24h Reminder',
  reminder_2h: '2h Reminder',
  cancellation_ack: 'Cancellation',
  delay_alert: 'Delay Alert',
  wakeup_call: 'Wake-up Call',
}

export default function NotificationsPage() {
  const [page, setPage] = useState(1)
  const [status, setStatus] = useState('')
  const [type, setType] = useState('')
  const [channel, setChannel] = useState('')
  const [search, setSearch] = useState('')
  const [date, setDate] = useState('')
  const [selectedNotification, setSelectedNotification] = useState<any>(null)

  const queryClient = useQueryClient()

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['notifications', page, status, type, channel, search, date],
    queryFn: () => fetchNotifications(page, status, type, channel, search, date),
    refetchInterval: 10000, // Refresh every 10 seconds
  })

  const resendMutation = useMutation({
    mutationFn: resendNotification,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['notifications'] })
    },
  })

  const columns = [
    {
      key: 'recipient',
      header: 'Recipient',
      render: (notification: any) => (
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gray-100">
            {(() => {
              const Icon = channelIcons[notification.channel] || MessageSquare
              return <Icon className="h-5 w-5 text-gray-600" />
            })()}
          </div>
          <div>
            <p className="font-medium text-gray-900">
              {notification.recipient_name || 'Unknown'}
            </p>
            <p className="text-xs text-gray-500">
              {formatPhoneNumber(notification.recipient_mobile)}
            </p>
          </div>
        </div>
      ),
    },
    {
      key: 'repeat_count',
      header: 'Repeat',
      render: (notification: any) => {
        const repeatCount = notification.customer_repeat_count || 0
        return (
          <div className="text-center">
            {repeatCount > 0 ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-700">
                <History className="h-3 w-3" />
                {repeatCount}
              </span>
            ) : (
              <span className="rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700">New</span>
            )}
          </div>
        )
      },
    },
    {
      key: 'active_channel',
      header: 'Active Ch',
      render: (notification: any) => {
        const preferredChannel = notification.preferred_channel || notification.channel
        const channelColors: Record<string, string> = {
          whatsapp: 'bg-green-100 text-green-700',
          wap: 'bg-cyan-100 text-cyan-700',
          rcs: 'bg-indigo-100 text-indigo-700',
          sms: 'bg-blue-100 text-blue-700',
          email: 'bg-purple-100 text-purple-700',
          ivr: 'bg-orange-100 text-orange-700',
        }
        const Icon = channelIcons[preferredChannel] || MessageSquare
        return (
          <div className="flex items-center gap-1">
            <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${channelColors[preferredChannel] || 'bg-gray-100 text-gray-700'}`}>
              <Icon className="h-3 w-3" />
              {preferredChannel?.toUpperCase() || '--'}
            </span>
          </div>
        )
      },
    },
    {
      key: 'read_status',
      header: 'Read',
      render: (notification: any) => {
        const isRead = notification.is_read || notification.status === 'delivered'
        const readAt = notification.read_at
        return (
          <div className="flex items-center gap-1">
            {isRead ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700" title={readAt ? `Read at ${formatDateTime(readAt)}` : 'Read'}>
                <CheckCheck className="h-3 w-3" />
                Read
              </span>
            ) : (
              <span className="inline-flex items-center gap-1 rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-500">
                <X className="h-3 w-3" />
                Unread
              </span>
            )}
          </div>
        )
      },
    },
    {
      key: 'notification_type',
      header: 'Type',
      render: (notification: any) => (
        <span className="rounded-lg bg-blue-50 px-2 py-1 text-xs font-medium text-blue-700">
          {typeLabels[notification.notification_type] || notification.notification_type}
        </span>
      ),
    },
    {
      key: 'channel',
      header: 'Channel',
      render: (notification: any) => (
        <span className="capitalize">{notification.channel}</span>
      ),
    },
    {
      key: 'booking',
      header: 'Booking',
      render: (notification: any) => {
        const booking = notification.jc_bookings
        if (!booking) return <span className="text-gray-400">-</span>
        return (
          <div>
            <p className="text-sm font-medium">{booking.travel_operator_pnr}</p>
            <p className="text-xs text-gray-500">
              {booking.origin} → {booking.destination}
            </p>
            {booking.service_number && (
              <span className="mt-1 inline-block rounded bg-indigo-50 px-1.5 py-0.5 text-xs font-medium text-indigo-700">
                {booking.service_number}
              </span>
            )}
          </div>
        )
      },
    },
    {
      key: 'scheduled_at',
      header: 'Scheduled',
      render: (notification: any) => (
        <div>
          <p className="text-sm">{formatDateTime(notification.scheduled_at)}</p>
          <p className="text-xs text-gray-500">{timeAgo(notification.scheduled_at)}</p>
        </div>
      ),
    },
    {
      key: 'status',
      header: 'Status',
      render: (notification: any) => <StatusBadge status={notification.status} />,
    },
    {
      key: 'actions',
      header: '',
      render: (notification: any) => (
        <div className="flex items-center gap-2">
          <button
            onClick={() => setSelectedNotification(notification)}
            className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600"
            title="View details"
          >
            <Eye className="h-4 w-4" />
          </button>
          {notification.status === 'failed' && (
            <button
              onClick={() => resendMutation.mutate(notification.id)}
              disabled={resendMutation.isPending}
              className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 transition-colors hover:bg-blue-100 hover:text-blue-600 disabled:opacity-50"
              title="Resend"
            >
              <RotateCcw className={`h-4 w-4 ${resendMutation.isPending ? 'animate-spin' : ''}`} />
            </button>
          )}
        </div>
      ),
    },
  ]

  // Calculate stats
  const stats = {
    pending: data?.notifications?.filter((n: any) => n.status === 'pending').length || 0,
    sent: data?.notifications?.filter((n: any) => n.status === 'sent').length || 0,
    delivered: data?.notifications?.filter((n: any) => n.status === 'delivered').length || 0,
    failed: data?.notifications?.filter((n: any) => n.status === 'failed').length || 0,
  }

  // Channel-wise stats
  const channelStats = {
    whatsapp: data?.notifications?.filter((n: any) => n.channel === 'whatsapp').length || 0,
    wap: data?.notifications?.filter((n: any) => n.channel === 'wap').length || 0,
    rcs: data?.notifications?.filter((n: any) => n.channel === 'rcs').length || 0,
    sms: data?.notifications?.filter((n: any) => n.channel === 'sms').length || 0,
    email: data?.notifications?.filter((n: any) => n.channel === 'email').length || 0,
    ivr: data?.notifications?.filter((n: any) => n.channel === 'ivr').length || 0,
  }

  // Read stats
  const readStats = {
    read: data?.notifications?.filter((n: any) => n.is_read || n.status === 'delivered').length || 0,
    unread: data?.notifications?.filter((n: any) => !n.is_read && n.status !== 'delivered').length || 0,
  }

  return (
    <DashboardLayout>
      <Header
        title="Notifications"
        subtitle="Monitor and manage customer communications"
      />

      <div className="p-6">
        {/* Status Stats */}
        <div className="mb-4 grid gap-4 sm:grid-cols-4">
          <div className="flex items-center gap-4 rounded-xl border bg-white p-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-yellow-100">
              <Clock className="h-6 w-6 text-yellow-600" />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{stats.pending}</p>
              <p className="text-sm text-gray-500">Pending</p>
            </div>
          </div>
          <div className="flex items-center gap-4 rounded-xl border bg-white p-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-blue-100">
              <Send className="h-6 w-6 text-blue-600" />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{stats.sent}</p>
              <p className="text-sm text-gray-500">Sent</p>
            </div>
          </div>
          <div className="flex items-center gap-4 rounded-xl border bg-white p-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-green-100">
              <CheckCircle className="h-6 w-6 text-green-600" />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{stats.delivered}</p>
              <p className="text-sm text-gray-500">Delivered</p>
            </div>
          </div>
          <div className="flex items-center gap-4 rounded-xl border bg-white p-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-red-100">
              <AlertTriangle className="h-6 w-6 text-red-600" />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{stats.failed}</p>
              <p className="text-sm text-gray-500">Failed</p>
            </div>
          </div>
        </div>

        {/* Channel-wise Stats */}
        <div className="mb-6 grid gap-3 sm:grid-cols-6">
          <div className="flex items-center gap-3 rounded-xl border bg-white p-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-green-100">
              <MessageSquare className="h-5 w-5 text-green-600" />
            </div>
            <div>
              <p className="text-lg font-bold text-gray-900">{channelStats.whatsapp}</p>
              <p className="text-xs text-gray-500">WhatsApp</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border bg-white p-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-cyan-100">
              <Globe className="h-5 w-5 text-cyan-600" />
            </div>
            <div>
              <p className="text-lg font-bold text-gray-900">{channelStats.wap}</p>
              <p className="text-xs text-gray-500">WAP</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border bg-white p-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-indigo-100">
              <MessageCircle className="h-5 w-5 text-indigo-600" />
            </div>
            <div>
              <p className="text-lg font-bold text-gray-900">{channelStats.rcs}</p>
              <p className="text-xs text-gray-500">RCS</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border bg-white p-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-blue-100">
              <Smartphone className="h-5 w-5 text-blue-600" />
            </div>
            <div>
              <p className="text-lg font-bold text-gray-900">{channelStats.sms}</p>
              <p className="text-xs text-gray-500">SMS</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border bg-white p-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-purple-100">
              <Mail className="h-5 w-5 text-purple-600" />
            </div>
            <div>
              <p className="text-lg font-bold text-gray-900">{channelStats.email}</p>
              <p className="text-xs text-gray-500">Email</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border bg-white p-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-orange-100">
              <PhoneCall className="h-5 w-5 text-orange-600" />
            </div>
            <div>
              <p className="text-lg font-bold text-gray-900">{channelStats.ivr}</p>
              <p className="text-xs text-gray-500">IVR</p>
            </div>
          </div>
        </div>

        {/* Read Status Bar */}
        <div className="mb-6 rounded-xl border bg-white p-4">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm font-medium text-gray-700">Read Status</p>
            <p className="text-sm text-gray-500">
              <span className="font-medium text-green-600">{readStats.read}</span> read / <span className="font-medium text-gray-600">{readStats.unread}</span> unread
            </p>
          </div>
          <div className="h-2 overflow-hidden rounded-full bg-gray-100">
            <div
              className="h-full bg-green-500 transition-all"
              style={{ width: `${readStats.read + readStats.unread > 0 ? (readStats.read / (readStats.read + readStats.unread)) * 100 : 0}%` }}
            />
          </div>
        </div>

        {/* Filters */}
        <div className="mb-6 flex flex-wrap items-center gap-4">
          {/* Search */}
          <div className="relative flex-1 min-w-[250px]">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
            <input
              type="text"
              placeholder="Search by name, phone, service..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="h-10 w-full rounded-xl border border-gray-200 bg-white pl-10 pr-4 text-sm transition-all focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
            />
          </div>

          {/* Date Filter */}
          <div className="relative">
            <Calendar className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
            <input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
              className="h-10 rounded-xl border border-gray-200 bg-white pl-10 pr-4 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
            />
          </div>

          <select
            value={status}
            onChange={(e) => setStatus(e.target.value)}
            className="h-10 rounded-xl border border-gray-200 bg-white px-4 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            <option value="">All Status</option>
            <option value="pending">Pending</option>
            <option value="sent">Sent</option>
            <option value="delivered">Delivered</option>
            <option value="failed">Failed</option>
          </select>

          <select
            value={type}
            onChange={(e) => setType(e.target.value)}
            className="h-10 rounded-xl border border-gray-200 bg-white px-4 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            <option value="">All Types</option>
            <option value="booking_confirmation">Booking Confirmation</option>
            <option value="reminder_24h">24h Reminder</option>
            <option value="reminder_2h">2h Reminder</option>
            <option value="cancellation_ack">Cancellation</option>
            <option value="delay_alert">Delay Alert</option>
            <option value="wakeup_call">Wake-up Call</option>
          </select>

          <select
            value={channel}
            onChange={(e) => setChannel(e.target.value)}
            className="h-10 rounded-xl border border-gray-200 bg-white px-4 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
          >
            <option value="">All Channels</option>
            <option value="whatsapp">WhatsApp</option>
            <option value="wap">WAP</option>
            <option value="rcs">RCS</option>
            <option value="sms">SMS</option>
            <option value="email">Email</option>
            <option value="ivr">IVR</option>
          </select>

          <button
            onClick={() => refetch()}
            className="flex h-10 items-center gap-2 rounded-xl bg-blue-600 px-4 text-sm font-medium text-white transition-colors hover:bg-blue-700"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        </div>

        {/* Data Table */}
        <DataTable
          data={data?.notifications || []}
          columns={columns}
          loading={isLoading}
          emptyMessage="No notifications found"
          pagination={{
            page,
            totalPages: data?.pagination?.totalPages || 1,
            total: data?.pagination?.total || 0,
            onPageChange: setPage,
          }}
        />
      </div>

      {/* Notification Detail Modal */}
      {selectedNotification && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <h2 className="text-lg font-semibold">Notification Details</h2>
              <button
                onClick={() => setSelectedNotification(null)}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100"
              >
                ×
              </button>
            </div>
            <div className="p-4 space-y-4">
              {/* Customer Info */}
              <div className="flex items-center gap-4 p-4 rounded-xl bg-gray-50">
                <div className="flex h-14 w-14 items-center justify-center rounded-full bg-gradient-to-br from-blue-500 to-indigo-600 text-lg font-bold text-white">
                  {selectedNotification.recipient_name?.split(' ').map((n: string) => n[0]).join('').slice(0, 2).toUpperCase() || '??'}
                </div>
                <div className="flex-1">
                  <p className="font-semibold text-lg">{selectedNotification.recipient_name || 'Unknown'}</p>
                  <div className="flex items-center gap-4 text-sm text-gray-500">
                    <span className="flex items-center gap-1">
                      <Phone className="h-3 w-3" />
                      {formatPhoneNumber(selectedNotification.recipient_mobile)}
                    </span>
                    {(selectedNotification.customer_repeat_count || 0) > 0 && (
                      <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-700">
                        <History className="h-3 w-3" />
                        {selectedNotification.customer_repeat_count} repeat bookings
                      </span>
                    )}
                  </div>
                </div>
                <StatusBadge status={selectedNotification.status} />
              </div>

              {/* Notification Details */}
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="rounded-xl border p-4">
                  <p className="text-xs text-gray-500 mb-1">Type</p>
                  <p className="font-medium">
                    {typeLabels[selectedNotification.notification_type] || selectedNotification.notification_type}
                  </p>
                </div>
                <div className="rounded-xl border p-4">
                  <p className="text-xs text-gray-500 mb-1">Channel</p>
                  <div className="flex items-center gap-2">
                    {(() => {
                      const Icon = channelIcons[selectedNotification.channel] || MessageSquare
                      return <Icon className="h-4 w-4 text-gray-600" />
                    })()}
                    <p className="font-medium capitalize">{selectedNotification.channel}</p>
                  </div>
                </div>
                <div className="rounded-xl border p-4">
                  <p className="text-xs text-gray-500 mb-1">Scheduled At</p>
                  <p className="font-medium">{formatDateTime(selectedNotification.scheduled_at)}</p>
                </div>
                {selectedNotification.sent_at && (
                  <div className="rounded-xl border p-4">
                    <p className="text-xs text-gray-500 mb-1">Sent At</p>
                    <p className="font-medium">{formatDateTime(selectedNotification.sent_at)}</p>
                  </div>
                )}
              </div>

              {/* Preferred Channel */}
              {selectedNotification.preferred_channel && selectedNotification.preferred_channel !== selectedNotification.channel && (
                <div className="rounded-xl bg-yellow-50 border border-yellow-200 p-4">
                  <p className="text-sm text-yellow-700">
                    <Wifi className="h-4 w-4 inline mr-1" />
                    Customer's preferred channel: <strong className="capitalize">{selectedNotification.preferred_channel}</strong>
                  </p>
                </div>
              )}

              {/* Booking Info */}
              {selectedNotification.jc_bookings && (
                <div className="rounded-xl border p-4">
                  <p className="text-sm font-medium text-gray-500 mb-3">BOOKING DETAILS</p>
                  <div className="grid gap-3 sm:grid-cols-2">
                    <div>
                      <p className="text-xs text-gray-500">PNR</p>
                      <p className="font-medium">{selectedNotification.jc_bookings.travel_operator_pnr}</p>
                    </div>
                    <div>
                      <p className="text-xs text-gray-500">Route</p>
                      <p className="font-medium">
                        {selectedNotification.jc_bookings.origin} → {selectedNotification.jc_bookings.destination}
                      </p>
                    </div>
                    {selectedNotification.jc_bookings.service_number && (
                      <div>
                        <p className="text-xs text-gray-500">Service No</p>
                        <p className="font-medium">{selectedNotification.jc_bookings.service_number}</p>
                      </div>
                    )}
                    {selectedNotification.jc_bookings.travel_date && (
                      <div>
                        <p className="text-xs text-gray-500">Travel Date</p>
                        <p className="font-medium">{formatDateDDMMYYYY(selectedNotification.jc_bookings.travel_date)}</p>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Message Content */}
              {selectedNotification.message_content && (
                <div>
                  <p className="mb-2 text-sm font-medium text-gray-500">MESSAGE CONTENT</p>
                  <div className="rounded-lg bg-gray-50 p-4 border">
                    <pre className="whitespace-pre-wrap text-sm font-sans">
                      {selectedNotification.message_content}
                    </pre>
                  </div>
                </div>
              )}

              {/* Delivery Timeline */}
              <div>
                <p className="mb-2 text-sm font-medium text-gray-500">DELIVERY TIMELINE</p>
                <div className="rounded-lg border p-4">
                  <div className="flex items-center gap-4">
                    <div className={`flex h-8 w-8 items-center justify-center rounded-full ${
                      selectedNotification.status === 'pending' ? 'bg-yellow-100' :
                      selectedNotification.status === 'sent' ? 'bg-blue-100' :
                      selectedNotification.status === 'delivered' ? 'bg-green-100' : 'bg-red-100'
                    }`}>
                      {selectedNotification.status === 'pending' ? <Clock className="h-4 w-4 text-yellow-600" /> :
                       selectedNotification.status === 'sent' ? <Send className="h-4 w-4 text-blue-600" /> :
                       selectedNotification.status === 'delivered' ? <CheckCircle className="h-4 w-4 text-green-600" /> :
                       <AlertTriangle className="h-4 w-4 text-red-600" />}
                    </div>
                    <div className="flex-1">
                      <p className="font-medium capitalize">{selectedNotification.status}</p>
                      <p className="text-sm text-gray-500">{timeAgo(selectedNotification.sent_at || selectedNotification.scheduled_at)}</p>
                    </div>
                    {selectedNotification.retry_count > 0 && (
                      <span className="rounded bg-gray-100 px-2 py-1 text-xs text-gray-600">
                        {selectedNotification.retry_count} retries
                      </span>
                    )}
                  </div>
                </div>
              </div>

              {/* Error Message */}
              {selectedNotification.error_message && (
                <div>
                  <p className="mb-2 text-sm font-medium text-red-500">ERROR DETAILS</p>
                  <div className="rounded-lg bg-red-50 border border-red-200 p-4">
                    <p className="text-sm text-red-700">{selectedNotification.error_message}</p>
                  </div>
                </div>
              )}

              {/* Actions */}
              <div className="flex justify-end gap-2 pt-4 border-t">
                {selectedNotification.status === 'failed' && (
                  <button
                    onClick={() => {
                      resendMutation.mutate(selectedNotification.id)
                      setSelectedNotification(null)
                    }}
                    className="flex items-center gap-2 rounded-xl bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
                  >
                    <RotateCcw className="h-4 w-4" />
                    Resend Notification
                  </button>
                )}
                <button
                  onClick={() => setSelectedNotification(null)}
                  className="flex items-center gap-2 rounded-xl border border-gray-200 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </DashboardLayout>
  )
}
