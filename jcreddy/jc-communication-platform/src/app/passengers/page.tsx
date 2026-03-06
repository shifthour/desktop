'use client'

import { useQuery } from '@tanstack/react-query'
import { useState, useEffect } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { DataTable } from '@/components/ui/data-table'
import { StatusBadge } from '@/components/ui/status-badge'
import { formatDate, formatDateDDMMYYYY, formatPhoneNumber, formatCurrency } from '@/lib/utils'
import {
  Search,
  Users,
  Phone,
  Mail,
  Download,
  Eye,
  CheckCircle,
  XCircle,
  X,
  RefreshCw,
} from 'lucide-react'

async function fetchPassengers(page: number, search: string, status: string, routeId: string, dateFrom: string, dateTo: string) {
  const params = new URLSearchParams({
    page: page.toString(),
    limit: '20',
    search,
    status,
    routeId,
    dateFrom,
    dateTo,
  })
  const res = await fetch(`/api/passengers?${params}`)
  if (!res.ok) throw new Error('Failed to fetch passengers')
  return res.json()
}

// Helper to get date string in YYYY-MM-DD format (local timezone)
function getDateString(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

// Get today's date (local timezone)
function getToday(): string {
  return getDateString(new Date())
}

async function fetchRoutes() {
  const res = await fetch('/api/routes')
  if (!res.ok) throw new Error('Failed to fetch routes')
  return res.json()
}

export default function PassengersPage() {
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [status, setStatus] = useState('')
  const [routeId, setRouteId] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [datesInitialized, setDatesInitialized] = useState(false)
  const [selectedPassenger, setSelectedPassenger] = useState<any>(null)

  // Initialize dates on client side
  useEffect(() => {
    if (!datesInitialized) {
      const today = getToday()
      setDateFrom(today)
      setDateTo(today)
      setDatesInitialized(true)
    }
  }, [datesInitialized])

  const { data, isLoading } = useQuery({
    queryKey: ['passengers', page, search, status, routeId, dateFrom, dateTo],
    queryFn: () => fetchPassengers(page, search, status, routeId, dateFrom, dateTo),
    enabled: datesInitialized,
  })

  const { data: routesData } = useQuery({
    queryKey: ['routes'],
    queryFn: fetchRoutes,
  })

  const columns = [
    {
      key: 'passenger',
      header: 'Passenger',
      render: (passenger: any) => {
        const isRepeat = passenger.is_repeat || false
        const repeatCount = passenger.repeat_count || 0
        return (
          <div>
            <p className="font-medium text-gray-900">{passenger.full_name}</p>
            <div className="flex items-center gap-2">
              <span className="text-xs text-gray-500">PNR: {passenger.pnr_number}</span>
              {isRepeat ? (
                <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-1.5 py-0.5 text-xs font-medium text-blue-700">
                  <RefreshCw className="h-3 w-3" />
                  Repeat({repeatCount})
                </span>
              ) : (
                <span className="rounded-full bg-green-100 px-1.5 py-0.5 text-xs font-medium text-green-700">New</span>
              )}
            </div>
          </div>
        )
      },
    },
    {
      key: 'contact',
      header: 'Contact',
      render: (passenger: any) => (
        <div className="flex items-center gap-2">
          <div className="flex-1 min-w-0">
            <p className="text-sm truncate">{formatPhoneNumber(passenger.mobile)}</p>
          </div>
          <div className="flex items-center gap-1">
            <a
              href={`tel:+91${passenger.mobile}`}
              className="flex h-7 w-7 items-center justify-center rounded-lg text-blue-500 hover:bg-blue-100"
              title="Call"
            >
              <Phone className="h-4 w-4" />
            </a>
            <a
              href={`https://wa.me/91${passenger.mobile}`}
              target="_blank"
              rel="noopener noreferrer"
              className="flex h-7 w-7 items-center justify-center rounded-lg text-green-500 hover:bg-green-100"
              title="WhatsApp"
            >
              <svg className="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
                <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z"/>
              </svg>
            </a>
          </div>
        </div>
      ),
    },
    {
      key: 'booked_via',
      header: 'Booked Via',
      render: (passenger: any) => {
        const bookedBySources = passenger.booked_by_sources || []
        if (bookedBySources.length === 0) {
          return <span className="text-xs text-gray-400">--</span>
        }
        return (
          <span className="text-xs font-medium text-gray-700">
            {bookedBySources.join(', ')}
          </span>
        )
      },
    },
    {
      key: 'seat',
      header: 'Seat',
      render: (passenger: any) => (
        <span className="rounded bg-blue-100 px-2 py-0.5 text-xs font-semibold text-blue-700">
          {passenger.seat_number}
        </span>
      ),
    },
    {
      key: 'travel_date',
      header: 'Travel Date',
      render: (passenger: any) => {
        const booking = passenger.booking
        if (!booking) return <span className="text-gray-400">--</span>
        return (
          <div>
            <p className="text-sm text-gray-900">{formatDateDDMMYYYY(booking.travel_date)}</p>
            {booking.boarding_time && (
              <p className="text-xs text-gray-500">{booking.boarding_time}</p>
            )}
          </div>
        )
      },
    },
    {
      key: 'boarding_point',
      header: 'Boarding Point',
      render: (passenger: any) => {
        const booking = passenger.booking
        if (!booking?.boarding_point_name) return <span className="text-gray-400">--</span>
        return (
          <p className="text-sm text-gray-900">{booking.boarding_point_name}</p>
        )
      },
    },
    {
      key: 'route',
      header: 'Route',
      render: (passenger: any) => {
        const booking = passenger.booking
        if (!booking) return <span className="text-gray-400">--</span>
        return (
          <div>
            <p className="text-xs text-gray-900">
              {booking.origin} → {booking.destination}
            </p>
            {booking.service_number && (
              <span className="text-xs text-blue-600">({booking.service_number})</span>
            )}
          </div>
        )
      },
    },
    {
      key: 'boarding_status',
      header: 'Boarding Status',
      render: (passenger: any) => (
        <div className="flex items-center gap-1">
          {passenger.is_boarded ? (
            <>
              <CheckCircle className="h-4 w-4 text-green-500" />
              <span className="text-xs text-green-600">Boarded</span>
            </>
          ) : (
            <>
              <XCircle className="h-4 w-4 text-gray-300" />
              <span className="text-xs text-gray-500">Pending</span>
            </>
          )}
        </div>
      ),
    },
    {
      key: 'status',
      header: 'Status',
      render: (passenger: any) => (
        <StatusBadge
          status={passenger.is_cancelled ? 'cancelled' : passenger.is_confirmed ? 'confirmed' : 'pending'}
          size="sm"
        />
      ),
    },
  ]

  const passengers = data?.passengers || []

  return (
    <DashboardLayout>
      <Header title="Passengers" subtitle="Track and manage all passenger records across trips" />

      <div className="p-4 sm:p-6">
        {/* Section: Quick Stats */}
        <div className="mb-2">
          <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wide">Quick Stats</h3>
        </div>
        <div className="mb-4 sm:mb-6 grid gap-3 sm:gap-4 grid-cols-2 sm:grid-cols-4">
          <div className="rounded-xl border bg-white p-3 sm:p-4">
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="flex h-8 w-8 sm:h-10 sm:w-10 items-center justify-center rounded-lg sm:rounded-xl bg-blue-100">
                <Users className="h-4 w-4 sm:h-5 sm:w-5 text-blue-600" />
              </div>
              <div>
                <p className="text-lg sm:text-2xl font-bold text-gray-900">{data?.stats?.total || 0}</p>
                <p className="text-xs sm:text-sm text-gray-500">Total</p>
              </div>
            </div>
          </div>
          <div className="rounded-xl border bg-white p-3 sm:p-4">
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="flex h-8 w-8 sm:h-10 sm:w-10 items-center justify-center rounded-lg sm:rounded-xl bg-green-100">
                <CheckCircle className="h-4 w-4 sm:h-5 sm:w-5 text-green-600" />
              </div>
              <div>
                <p className="text-lg sm:text-2xl font-bold text-green-600">
                  {data?.stats?.boarded || 0}
                </p>
                <p className="text-xs sm:text-sm text-gray-500">Boarded</p>
              </div>
            </div>
          </div>
          <div className="rounded-xl border bg-white p-3 sm:p-4">
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="flex h-8 w-8 sm:h-10 sm:w-10 items-center justify-center rounded-lg sm:rounded-xl bg-yellow-100">
                <Users className="h-4 w-4 sm:h-5 sm:w-5 text-yellow-600" />
              </div>
              <div>
                <p className="text-lg sm:text-2xl font-bold text-yellow-600">
                  {data?.stats?.pending || 0}
                </p>
                <p className="text-xs sm:text-sm text-gray-500">Pending</p>
              </div>
            </div>
          </div>
          <div className="rounded-xl border bg-white p-3 sm:p-4">
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="flex h-8 w-8 sm:h-10 sm:w-10 items-center justify-center rounded-lg sm:rounded-xl bg-red-100">
                <XCircle className="h-4 w-4 sm:h-5 sm:w-5 text-red-600" />
              </div>
              <div>
                <p className="text-lg sm:text-2xl font-bold text-red-600">
                  {data?.stats?.cancelled || 0}
                </p>
                <p className="text-xs sm:text-sm text-gray-500">Cancelled</p>
              </div>
            </div>
          </div>
        </div>

        {/* Modern Compact Filters */}
        <div className="mb-4 rounded-xl border bg-white p-3">
          <div className="flex flex-wrap items-center gap-2">
            {/* Search - compact */}
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search PNR, phone, name..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="h-8 w-48 rounded-lg border border-gray-200 bg-gray-50 pl-8 pr-3 text-sm focus:border-blue-500 focus:bg-white focus:outline-none"
              />
            </div>

            {/* Divider */}
            <div className="h-6 w-px bg-gray-200" />

            {/* Quick Date Filters - Based on travel_date */}
            <div className="flex items-center gap-1">
              <button
                onClick={() => {
                  const today = getToday()
                  setDateFrom(today)
                  setDateTo(today)
                }}
                className={`h-8 px-3 rounded-lg text-xs font-medium transition-all ${
                  dateFrom === getToday() && dateTo === getToday()
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                Today
              </button>
              <button
                onClick={() => {
                  const yesterday = new Date()
                  yesterday.setDate(yesterday.getDate() - 1)
                  setDateFrom(getDateString(yesterday))
                  setDateTo(getToday())
                }}
                className={`h-8 px-3 rounded-lg text-xs font-medium transition-all ${
                  (() => {
                    const yesterday = new Date()
                    yesterday.setDate(yesterday.getDate() - 1)
                    return dateFrom === getDateString(yesterday) && dateTo === getToday()
                  })()
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                2 Days
              </button>
              <button
                onClick={() => {
                  const weekAgo = new Date()
                  weekAgo.setDate(weekAgo.getDate() - 6)
                  setDateFrom(getDateString(weekAgo))
                  setDateTo(getToday())
                }}
                className={`h-8 px-3 rounded-lg text-xs font-medium transition-all ${
                  (() => {
                    const weekAgo = new Date()
                    weekAgo.setDate(weekAgo.getDate() - 6)
                    return dateFrom === getDateString(weekAgo) && dateTo === getToday()
                  })()
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                7 Days
              </button>
              <button
                onClick={() => {
                  setDateFrom('')
                  setDateTo('')
                }}
                className={`h-8 px-3 rounded-lg text-xs font-medium transition-all ${
                  !dateFrom && !dateTo
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                All
              </button>
              {/* Custom Date */}
              <div className="flex items-center gap-1 ml-1">
                <input
                  type="date"
                  value={dateFrom}
                  onChange={(e) => setDateFrom(e.target.value)}
                  className="h-8 w-32 rounded-lg border border-gray-200 bg-gray-50 px-2 text-xs focus:border-blue-500 focus:bg-white focus:outline-none"
                />
                <span className="text-xs text-gray-400">-</span>
                <input
                  type="date"
                  value={dateTo}
                  onChange={(e) => setDateTo(e.target.value)}
                  className="h-8 w-32 rounded-lg border border-gray-200 bg-gray-50 px-2 text-xs focus:border-blue-500 focus:bg-white focus:outline-none"
                />
              </div>
            </div>

            {/* Divider */}
            <div className="h-6 w-px bg-gray-200" />

            {/* Status Filter */}
            <select
              value={status}
              onChange={(e) => setStatus(e.target.value)}
              className="h-8 rounded-lg border border-gray-200 bg-gray-50 px-2 text-xs focus:border-blue-500 focus:bg-white focus:outline-none"
            >
              <option value="">All Status</option>
              <option value="confirmed">Confirmed</option>
              <option value="boarded">Boarded</option>
              <option value="cancelled">Cancelled</option>
            </select>

            {/* Route Filter */}
            <select
              value={routeId}
              onChange={(e) => setRouteId(e.target.value)}
              className="h-8 rounded-lg border border-gray-200 bg-gray-50 px-2 text-xs focus:border-blue-500 focus:bg-white focus:outline-none"
            >
              <option value="">All Routes</option>
              {routesData?.routes?.map((route: any) => (
                <option key={route.id} value={route.id}>
                  [{route.bitla_route_id}] {route.origin}-{route.destination}
                </option>
              ))}
            </select>

            {/* Export */}
            <button className="ml-auto flex h-8 items-center gap-1.5 rounded-lg bg-gray-100 px-3 text-xs font-medium text-gray-600 hover:bg-gray-200">
              <Download className="h-3.5 w-3.5" />
              Export
            </button>
          </div>
        </div>

        {/* Section: Passenger Records */}
        <div className="mb-2 mt-4">
          <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wide">Passenger Records</h3>
        </div>

        {/* Mobile Card View */}
        <div className="block sm:hidden space-y-3">
          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <div className="h-8 w-8 animate-spin rounded-full border-4 border-blue-600 border-t-transparent" />
            </div>
          ) : passengers.length === 0 ? (
            <div className="flex flex-col items-center justify-center rounded-xl border bg-white py-12">
              <Users className="mb-4 h-12 w-12 text-gray-300" />
              <p className="text-lg font-medium text-gray-900">No passengers found</p>
            </div>
          ) : (
            passengers.map((passenger: any) => (
              <div key={passenger.id} className="rounded-xl border bg-white p-4">
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <p className="font-medium text-gray-900">{passenger.full_name}</p>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-gray-500">PNR: {passenger.pnr_number}</span>
                      {passenger.is_repeat ? (
                        <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-1.5 py-0.5 text-xs font-medium text-blue-700">
                          <RefreshCw className="h-3 w-3" />
                          Repeat({passenger.repeat_count})
                        </span>
                      ) : (
                        <span className="rounded-full bg-green-100 px-1.5 py-0.5 text-xs font-medium text-green-700">New</span>
                      )}
                    </div>
                  </div>
                  <StatusBadge
                    status={passenger.is_cancelled ? 'cancelled' : passenger.is_confirmed ? 'confirmed' : 'pending'}
                    size="sm"
                  />
                </div>

                <div className="grid grid-cols-2 gap-3 mb-3">
                  <div className="flex items-center gap-2 text-sm text-gray-600">
                    <Phone className="h-4 w-4 text-gray-400" />
                    <span className="truncate">{formatPhoneNumber(passenger.mobile)}</span>
                  </div>
                  <div className="flex items-center gap-2 text-sm">
                    <span className="rounded bg-blue-100 px-2 py-0.5 text-xs font-semibold text-blue-700">
                      Seat {passenger.seat_number}
                    </span>
                  </div>
                </div>

                {passenger.booking && (
                  <div className="mb-3 rounded-lg bg-gray-50 p-2">
                    <p className="text-sm font-medium text-gray-900">
                      {passenger.booking.origin} → {passenger.booking.destination}
                      {passenger.booking.service_number && (
                        <span className="ml-1 text-xs text-blue-600">({passenger.booking.service_number})</span>
                      )}
                    </p>
                    <div className="flex items-center gap-2 text-xs text-gray-500">
                      <span>{formatDateDDMMYYYY(passenger.booking.travel_date)}</span>
                      {passenger.booking.boarding_time && (
                        <span>{passenger.booking.boarding_time}</span>
                      )}
                    </div>
                    {passenger.booking.boarding_point_name && (
                      <p className="text-xs text-gray-600 mt-1">Boarding: {passenger.booking.boarding_point_name}</p>
                    )}
                  </div>
                )}

                <div className="flex items-center justify-between pt-3 border-t">
                  <div className="flex items-center gap-2">
                    {passenger.is_boarded ? (
                      <>
                        <CheckCircle className="h-4 w-4 text-green-500" />
                        <span className="text-sm text-green-600">Boarded</span>
                      </>
                    ) : (
                      <>
                        <XCircle className="h-4 w-4 text-gray-300" />
                        <span className="text-sm text-gray-500">Not Boarded</span>
                      </>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => setSelectedPassenger(passenger)}
                      className="flex h-8 w-8 items-center justify-center rounded-lg border text-gray-400 hover:bg-gray-50 hover:text-gray-600"
                    >
                      <Eye className="h-4 w-4" />
                    </button>
                    <a
                      href={`tel:+91${passenger.mobile}`}
                      className="flex h-8 w-8 items-center justify-center rounded-lg border text-gray-400 hover:bg-blue-50 hover:text-blue-600"
                    >
                      <Phone className="h-4 w-4" />
                    </a>
                    <a
                      href={`https://wa.me/91${passenger.mobile}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex h-8 w-8 items-center justify-center rounded-lg border text-green-500 hover:bg-green-50 hover:text-green-600"
                    >
                      <svg className="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z"/>
                      </svg>
                    </a>
                  </div>
                </div>
              </div>
            ))
          )}

          {/* Mobile Pagination */}
          {data?.pagination && data.pagination.totalPages > 1 && (
            <div className="flex items-center justify-between pt-4">
              <button
                onClick={() => setPage(Math.max(1, page - 1))}
                disabled={page === 1}
                className="rounded-lg border px-4 py-2 text-sm font-medium disabled:opacity-50"
              >
                Previous
              </button>
              <span className="text-sm text-gray-500">
                Page {page} of {data.pagination.totalPages}
              </span>
              <button
                onClick={() => setPage(Math.min(data.pagination.totalPages, page + 1))}
                disabled={page === data.pagination.totalPages}
                className="rounded-lg border px-4 py-2 text-sm font-medium disabled:opacity-50"
              >
                Next
              </button>
            </div>
          )}
        </div>

        {/* Desktop Data Table */}
        <div className="hidden sm:block">
          <DataTable
            data={passengers}
            columns={columns}
            loading={isLoading}
            emptyMessage="No passengers found"
            pagination={{
              page,
              totalPages: data?.pagination?.totalPages || 1,
              total: data?.pagination?.total || 0,
              onPageChange: setPage,
            }}
          />
        </div>
      </div>

      {/* Passenger Detail Modal */}
      {selectedPassenger && (
        <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 p-0 sm:p-4">
          <div className="max-h-[90vh] w-full sm:max-w-lg overflow-y-auto rounded-t-2xl sm:rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <h2 className="text-lg font-semibold">Passenger Details</h2>
              <button
                onClick={() => setSelectedPassenger(null)}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <div className="p-4 space-y-4">
              {/* Profile */}
              <div className="flex items-center gap-4">
                <div className="flex h-14 w-14 sm:h-16 sm:w-16 items-center justify-center rounded-full bg-gradient-to-br from-blue-500 to-indigo-600 text-lg sm:text-xl font-bold text-white">
                  {selectedPassenger.full_name?.split(' ').map((n: string) => n[0]).join('').slice(0, 2).toUpperCase()}
                </div>
                <div>
                  <p className="text-lg sm:text-xl font-semibold">{selectedPassenger.full_name}</p>
                  <p className="text-sm text-gray-500">
                    {selectedPassenger.title} • Age: {selectedPassenger.age || '-'}
                  </p>
                </div>
              </div>

              {/* Contact */}
              <div className="grid gap-3">
                <div className="flex items-center gap-3 rounded-lg border p-3">
                  <Phone className="h-5 w-5 text-gray-400" />
                  <div className="flex-1 min-w-0">
                    <p className="text-xs text-gray-500">Mobile</p>
                    <p className="font-medium truncate">{formatPhoneNumber(selectedPassenger.mobile)}</p>
                  </div>
                  <a
                    href={`tel:${selectedPassenger.mobile}`}
                    className="rounded-lg bg-green-100 px-3 py-1 text-sm font-medium text-green-700 hover:bg-green-200"
                  >
                    Call
                  </a>
                </div>
                {selectedPassenger.email && (
                  <div className="flex items-center gap-3 rounded-lg border p-3">
                    <Mail className="h-5 w-5 text-gray-400" />
                    <div className="flex-1 min-w-0">
                      <p className="text-xs text-gray-500">Email</p>
                      <p className="font-medium truncate">{selectedPassenger.email}</p>
                    </div>
                  </div>
                )}
              </div>

              {/* Booking Info */}
              <div className="rounded-lg bg-gray-50 p-4">
                <p className="mb-2 text-sm font-medium text-gray-500">BOOKING DETAILS</p>
                <div className="grid gap-2">
                  <div className="flex justify-between">
                    <span className="text-gray-500">PNR</span>
                    <span className="font-medium">{selectedPassenger.pnr_number}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">Seat</span>
                    <span className="font-medium">{selectedPassenger.seat_number}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">Fare</span>
                    <span className="font-medium">{formatCurrency(selectedPassenger.fare || 0)}</span>
                  </div>
                </div>
              </div>

              {/* Notification Preferences */}
              <div className="rounded-lg border p-4">
                <p className="mb-3 text-sm font-medium text-gray-500">NOTIFICATION PREFERENCES</p>
                <div className="flex flex-wrap gap-2">
                  {selectedPassenger.wake_up_call_applicable && (
                    <span className="rounded-full bg-blue-100 px-3 py-1 text-xs font-medium text-blue-700">
                      Wake-up Call
                    </span>
                  )}
                  {selectedPassenger.pre_boarding_applicable && (
                    <span className="rounded-full bg-green-100 px-3 py-1 text-xs font-medium text-green-700">
                      24h Reminder
                    </span>
                  )}
                  {selectedPassenger.welcome_call_applicable && (
                    <span className="rounded-full bg-purple-100 px-3 py-1 text-xs font-medium text-purple-700">
                      2h Reminder
                    </span>
                  )}
                  {selectedPassenger.is_sms_allowed && (
                    <span className="rounded-full bg-gray-100 px-3 py-1 text-xs font-medium text-gray-700">
                      SMS Allowed
                    </span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </DashboardLayout>
  )
}
