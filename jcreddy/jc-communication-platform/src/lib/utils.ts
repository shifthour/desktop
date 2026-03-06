import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDate(date: string | Date, options?: Intl.DateTimeFormatOptions): string {
  const d = typeof date === 'string' ? new Date(date) : date
  return d.toLocaleDateString('en-IN', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    ...options,
  })
}

// Format date as dd/mm/yyyy
export function formatDateDDMMYYYY(date: string | Date): string {
  const d = typeof date === 'string' ? new Date(date) : date
  const day = String(d.getDate()).padStart(2, '0')
  const month = String(d.getMonth() + 1).padStart(2, '0')
  const year = d.getFullYear()
  return `${day}/${month}/${year}`
}

// Format date with time as dd/mm/yyyy HH:MM
export function formatDateTimeDDMMYYYY(date: string | Date): string {
  const d = typeof date === 'string' ? new Date(date) : date
  const dateStr = formatDateDDMMYYYY(d)
  const time = d.toLocaleTimeString('en-IN', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
  return `${dateStr} ${time}`
}

export function formatTime(date: string | Date): string {
  const d = typeof date === 'string' ? new Date(date) : date
  return d.toLocaleTimeString('en-IN', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: true,
  })
}

export function formatDateTime(date: string | Date): string {
  const d = typeof date === 'string' ? new Date(date) : date
  return `${formatDate(d)} ${formatTime(d)}`
}

export function formatCurrency(amount: number): string {
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(amount)
}

export function formatPhoneNumber(phone: string): string {
  if (!phone) return ''
  const cleaned = phone.replace(/\D/g, '')
  if (cleaned.length === 10) {
    return `+91 ${cleaned.slice(0, 5)} ${cleaned.slice(5)}`
  }
  if (cleaned.length === 12 && cleaned.startsWith('91')) {
    return `+${cleaned.slice(0, 2)} ${cleaned.slice(2, 7)} ${cleaned.slice(7)}`
  }
  return phone
}

export function getStatusColor(status: string): string {
  const statusColors: Record<string, string> = {
    // Booking statuses
    confirmed: 'bg-green-100 text-green-800',
    cancelled: 'bg-red-100 text-red-800',
    partially_cancelled: 'bg-orange-100 text-orange-800',
    completed: 'bg-blue-100 text-blue-800',

    // Notification statuses
    pending: 'bg-yellow-100 text-yellow-800',
    queued: 'bg-blue-100 text-blue-800',
    sent: 'bg-indigo-100 text-indigo-800',
    delivered: 'bg-green-100 text-green-800',
    read: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800',

    // Trip statuses
    scheduled: 'bg-blue-100 text-blue-800',
    departed: 'bg-purple-100 text-purple-800',
    in_transit: 'bg-indigo-100 text-indigo-800',

    // Boarding statuses
    boarded: 'bg-green-100 text-green-800',
    not_boarded: 'bg-yellow-100 text-yellow-800',
    no_show: 'bg-red-100 text-red-800',

    // Default
    active: 'bg-green-100 text-green-800',
    inactive: 'bg-gray-100 text-gray-800',
  }

  return statusColors[status.toLowerCase()] || 'bg-gray-100 text-gray-800'
}

export function getInitials(name: string): string {
  if (!name) return '??'
  const parts = name.trim().split(' ')
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase()
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase()
}

export function generateGoogleMapsLink(lat: number, lng: number): string {
  return `https://www.google.com/maps?q=${lat},${lng}`
}

export function truncate(str: string, length: number): string {
  if (!str) return ''
  if (str.length <= length) return str
  return str.slice(0, length) + '...'
}

export function timeAgo(date: string | Date): string {
  const d = typeof date === 'string' ? new Date(date) : date
  const now = new Date()
  const seconds = Math.floor((now.getTime() - d.getTime()) / 1000)

  if (seconds < 60) return 'just now'
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`
  if (seconds < 604800) return `${Math.floor(seconds / 86400)}d ago`
  return formatDate(d)
}

export function calculatePercentage(value: number, total: number): number {
  if (total === 0) return 0
  return Math.round((value / total) * 100)
}
