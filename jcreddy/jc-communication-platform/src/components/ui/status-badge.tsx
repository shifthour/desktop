'use client'

import { cn } from '@/lib/utils'

interface StatusBadgeProps {
  status: string
  size?: 'sm' | 'md'
}

const statusConfig: Record<string, { bg: string; text: string; dot: string }> = {
  // Booking statuses
  confirmed: { bg: 'bg-green-100', text: 'text-green-700', dot: 'bg-green-500' },
  cancelled: { bg: 'bg-red-100', text: 'text-red-700', dot: 'bg-red-500' },
  partially_cancelled: { bg: 'bg-orange-100', text: 'text-orange-700', dot: 'bg-orange-500' },
  completed: { bg: 'bg-blue-100', text: 'text-blue-700', dot: 'bg-blue-500' },

  // Notification statuses
  pending: { bg: 'bg-yellow-100', text: 'text-yellow-700', dot: 'bg-yellow-500' },
  queued: { bg: 'bg-blue-100', text: 'text-blue-700', dot: 'bg-blue-500' },
  sent: { bg: 'bg-indigo-100', text: 'text-indigo-700', dot: 'bg-indigo-500' },
  delivered: { bg: 'bg-green-100', text: 'text-green-700', dot: 'bg-green-500' },
  read: { bg: 'bg-green-100', text: 'text-green-700', dot: 'bg-green-500' },
  failed: { bg: 'bg-red-100', text: 'text-red-700', dot: 'bg-red-500' },

  // Trip statuses
  scheduled: { bg: 'bg-blue-100', text: 'text-blue-700', dot: 'bg-blue-500' },
  departed: { bg: 'bg-purple-100', text: 'text-purple-700', dot: 'bg-purple-500' },
  in_transit: { bg: 'bg-indigo-100', text: 'text-indigo-700', dot: 'bg-indigo-500' },

  // Boarding statuses
  boarded: { bg: 'bg-green-100', text: 'text-green-700', dot: 'bg-green-500' },
  not_boarded: { bg: 'bg-yellow-100', text: 'text-yellow-700', dot: 'bg-yellow-500' },
  no_show: { bg: 'bg-red-100', text: 'text-red-700', dot: 'bg-red-500' },

  // Webhook statuses
  processed: { bg: 'bg-green-100', text: 'text-green-700', dot: 'bg-green-500' },
  unprocessed: { bg: 'bg-yellow-100', text: 'text-yellow-700', dot: 'bg-yellow-500' },
  error: { bg: 'bg-red-100', text: 'text-red-700', dot: 'bg-red-500' },

  // Generic
  active: { bg: 'bg-green-100', text: 'text-green-700', dot: 'bg-green-500' },
  inactive: { bg: 'bg-gray-100', text: 'text-gray-700', dot: 'bg-gray-500' },
}

export function StatusBadge({ status, size = 'md' }: StatusBadgeProps) {
  const config = statusConfig[status.toLowerCase()] || {
    bg: 'bg-gray-100',
    text: 'text-gray-700',
    dot: 'bg-gray-500',
  }

  const displayStatus = status.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())

  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full font-medium',
        config.bg,
        config.text,
        size === 'sm' ? 'px-2 py-0.5 text-xs' : 'px-2.5 py-1 text-xs'
      )}
    >
      <span className={cn('h-1.5 w-1.5 rounded-full', config.dot)} />
      {displayStatus}
    </span>
  )
}
