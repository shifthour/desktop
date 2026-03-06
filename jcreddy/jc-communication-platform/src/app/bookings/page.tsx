'use client'

import { useQuery } from '@tanstack/react-query'
import { useState, useEffect } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { DataTable } from '@/components/ui/data-table'
import { StatusBadge } from '@/components/ui/status-badge'
import { formatDate, formatDateDDMMYYYY, formatDateTimeDDMMYYYY, formatCurrency, formatPhoneNumber } from '@/lib/utils'
import { Search, Calendar, Download, Eye, Users, MapPin, RefreshCw, ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react'

async function fetchBookings(page: number, search: string, status: string, dateFrom: string, dateTo: string, dateType: string, routeId: string, passengerType: string, sortBy: string, sortOrder: string) {
  const params = new URLSearchParams({
    page: page.toString(),
    limit: '20',
    search,
    status,
    dateFrom,
    dateTo,
    dateType,
    routeId,
    passengerType,
    sortBy,
    sortOrder,
  })
  const res = await fetch(`/api/bookings?${params}`)
  if (!res.ok) throw new Error('Failed to fetch bookings')
  return res.json()
}

// Helper to get date string in YYYY-MM-DD format (local timezone)
function getDateString(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

// Get yesterday's date
function getYesterday(): string {
  const yesterday = new Date()
  yesterday.setDate(yesterday.getDate() - 1)
  return getDateString(yesterday)
}

// Get today's date
function getToday(): string {
  return getDateString(new Date())
}

async function fetchRoutes() {
  // Fetch routes from our database (jc_routes table)
  const res = await fetch('/api/routes')
  if (!res.ok) throw new Error('Failed to fetch routes')
  return res.json()
}

export default function BookingsPage() {
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [status, setStatus] = useState('')
  const [dateType, setDateType] = useState<'travel' | 'booked'>('travel')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [datesInitialized, setDatesInitialized] = useState(false)
  const [routeId, setRouteId] = useState('')
  const [passengerType, setPassengerType] = useState('')
  const [selectedBooking, setSelectedBooking] = useState<any>(null)
  const [sortBy, setSortBy] = useState('created_at')
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc')

  // Initialize dates on client side to avoid hydration issues
  // Default to today only
  useEffect(() => {
    if (!datesInitialized) {
      setDateFrom(getToday())
      setDateTo(getToday())
      setDatesInitialized(true)
    }
  }, [datesInitialized])

  const handleSort = (column: string) => {
    if (sortBy === column) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(column)
      setSortOrder('desc')
    }
    setPage(1)
  }

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['bookings', page, search, status, dateFrom, dateTo, dateType, routeId, passengerType, sortBy, sortOrder],
    queryFn: () => fetchBookings(page, search, status, dateFrom, dateTo, dateType, routeId, passengerType, sortBy, sortOrder),
    enabled: datesInitialized, // Only fetch after dates are initialized
  })

  const { data: routesData } = useQuery({
    queryKey: ['routes'],
    queryFn: fetchRoutes,
  })

  const columns = [
    {
      key: 'travel_operator_pnr',
      header: 'PNR',
      render: (booking: any) => (
        <p className="font-medium text-gray-900">{booking.travel_operator_pnr}</p>
      ),
    },
    {
      key: 'passenger_name',
      header: 'Passenger',
      render: (booking: any) => {
        const primaryPassenger = booking.jc_passengers?.[0]
        const passengerName = primaryPassenger?.full_name || '--'
        const mobile = primaryPassenger?.mobile || '--'
        const isRepeat = primaryPassenger?.is_repeat || false
        const repeatCount = primaryPassenger?.repeat_count || 0
        const tentativePhone = primaryPassenger?.tentative_phone || null
        const isMaskedPhone = mobile?.includes('x') || mobile?.includes('X')

        return (
          <div>
            <p className="font-medium text-gray-900">{passengerName}</p>
            <div className="flex flex-col gap-0.5">
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-500">{mobile}</span>
                {isRepeat ? (
                  <span className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-1.5 py-0.5 text-xs font-medium text-blue-700">
                    <RefreshCw className="h-3 w-3" />
                    Repeat({repeatCount})
                  </span>
                ) : (
                  <span className="rounded-full bg-green-100 px-1.5 py-0.5 text-xs font-medium text-green-700">New</span>
                )}
              </div>
              {isMaskedPhone && tentativePhone && (
                <span className="text-xs text-orange-600">
                  Tentative: {tentativePhone}
                </span>
              )}
            </div>
          </div>
        )
      },
    },
    {
      key: 'route',
      header: 'Route',
      render: (booking: any) => {
        // Try multiple sources for route ID - Bitla sends as 'route_id'
        const routeIdVal = booking.service_number ||
                          booking.raw_data?.route_id ||
                          booking.raw_data?.routeId ||
                          booking.jc_trips?.jc_routes?.bitla_route_id ||
                          null
        return (
          <div className="flex items-center gap-2">
            <MapPin className="h-4 w-4 text-gray-400" />
            <div>
              <p className="font-medium text-gray-900">
                {booking.origin} → {booking.destination}
                {routeIdVal && (
                  <span className="ml-1 text-xs text-blue-600">({routeIdVal})</span>
                )}
              </p>
            </div>
          </div>
        )
      },
    },
    {
      key: 'issue_date',
      header: (
        <button
          onClick={() => handleSort('booking_date')}
          className="flex items-center gap-1 hover:text-blue-600 transition-colors"
        >
          Booked Date
          {sortBy === 'booking_date' ? (
            sortOrder === 'asc' ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />
          ) : (
            <ArrowUpDown className="h-3 w-3 text-gray-400" />
          )}
        </button>
      ),
      render: (booking: any) => {
        // Use created_at for time since booking_date doesn't have time
        const createdAt = booking.created_at ? new Date(booking.created_at) : null
        const timeStr = createdAt ? createdAt.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }) : ''
        return (
          <div>
            <p className="text-sm text-gray-900">{booking.booking_date ? formatDateDDMMYYYY(booking.booking_date) : formatDateDDMMYYYY(booking.created_at)}</p>
            {timeStr && <p className="text-xs text-gray-500">{timeStr}</p>}
          </div>
        )
      },
    },
    {
      key: 'travel_date',
      header: (
        <button
          onClick={() => handleSort('travel_date')}
          className="flex items-center gap-1 hover:text-blue-600 transition-colors"
        >
          Travel Date
          {sortBy === 'travel_date' ? (
            sortOrder === 'asc' ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />
          ) : (
            <ArrowUpDown className="h-3 w-3 text-gray-400" />
          )}
        </button>
      ),
      render: (booking: any) => {
        // Get boarding time from dedicated column or raw_data
        const boardingTime = booking.boarding_time ||
                            booking.raw_data?.boarding_point_details_dep_time ||
                            booking.raw_data?.boarding_time ||
                            booking.raw_data?.departure_time ||
                            null
        return (
          <div>
            <p className="font-medium text-gray-900">{formatDateDDMMYYYY(booking.travel_date)}</p>
            {boardingTime && (
              <p className="text-xs text-gray-500">{boardingTime}</p>
            )}
          </div>
        )
      },
    },
    {
      key: 'seats',
      header: 'Seats',
      render: (booking: any) => (
        <div className="flex items-center gap-2">
          <Users className="h-4 w-4 text-gray-400" />
          <span>{booking.jc_passengers?.length || booking.total_seats || 1}</span>
        </div>
      ),
    },
    {
      key: 'total_fare',
      header: 'Fare + GST',
      render: (booking: any) => (
        <div>
          <p className="font-medium text-gray-900">
            {formatCurrency(booking.total_fare || 0)}
          </p>
          {(booking.cgst || booking.sgst) && (
            <p className="text-xs text-gray-500">
              GST: {formatCurrency((booking.cgst || 0) + (booking.sgst || 0))}
            </p>
          )}
        </div>
      ),
    },
    {
      key: 'booked_by',
      header: 'Booked Via',
      render: (booking: any) => {
        // Remove text after "/" (e.g., "Paytm /API Agent Booking" -> "Paytm")
        let bookedVia = booking.booked_by || 'Direct'
        if (bookedVia.includes('/')) {
          bookedVia = bookedVia.split('/')[0].trim()
        }
        return (
          <span className="rounded-lg bg-gray-100 px-2 py-1 text-xs font-medium text-gray-700">
            {bookedVia}
          </span>
        )
      },
    },
    {
      key: 'booking_status',
      header: 'Status',
      render: (booking: any) => <StatusBadge status={booking.booking_status} />,
    },
    {
      key: 'actions',
      header: '',
      render: (booking: any) => (
        <div className="flex items-center gap-1">
          <button
            onClick={() => setSelectedBooking(booking)}
            className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600"
            title="View Details"
          >
            <Eye className="h-4 w-4" />
          </button>
        </div>
      ),
    },
  ]

  return (
    <DashboardLayout>
      <Header title="Bookings" subtitle="Manage all booking records" />

      <div className="p-4 sm:p-6">
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

            {/* Date Type Toggle */}
            <div className="flex items-center gap-1">
              <span className="text-xs text-gray-500">By:</span>
              <div className="flex rounded-lg border bg-gray-50 p-0.5">
                <button
                  onClick={() => setDateType('travel')}
                  className={`px-2 py-1 rounded-md text-xs font-medium transition-colors ${
                    dateType === 'travel'
                      ? 'bg-white text-blue-600 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Travel Date
                </button>
                <button
                  onClick={() => setDateType('booked')}
                  className={`px-2 py-1 rounded-md text-xs font-medium transition-colors ${
                    dateType === 'booked'
                      ? 'bg-white text-blue-600 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Booked Date
                </button>
              </div>
            </div>

            {/* Quick Date Filters */}
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
                  setDateFrom(getYesterday())
                  setDateTo(getToday())
                }}
                className={`h-8 px-3 rounded-lg text-xs font-medium transition-all ${
                  dateFrom === getYesterday() && dateTo === getToday()
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                2 Days
              </button>
              <button
                onClick={() => {
                  const d = new Date()
                  d.setDate(d.getDate() - 6)
                  setDateFrom(getDateString(d))
                  setDateTo(getToday())
                }}
                className={`h-8 px-3 rounded-lg text-xs font-medium transition-all ${
                  (() => {
                    const d = new Date()
                    d.setDate(d.getDate() - 6)
                    return dateFrom === getDateString(d) && dateTo === getToday()
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
              <option value="pending">Pending</option>
              <option value="confirmed">Confirmed</option>
              <option value="released">Released</option>
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
                <option key={route.id} value={`${route.service_number}|${route.origin}`}>
                  {route.origin}-{route.destination} (ID: {route.bitla_route_id}) ({route.service_number})
                </option>
              ))}
            </select>

            {/* Passenger Type Filter */}
            <select
              value={passengerType}
              onChange={(e) => setPassengerType(e.target.value)}
              className="h-8 rounded-lg border border-gray-200 bg-gray-50 px-2 text-xs focus:border-blue-500 focus:bg-white focus:outline-none"
            >
              <option value="">All Type</option>
              <option value="new">New</option>
              <option value="repeat">Repeat</option>
            </select>

            {/* Export */}
            <button className="ml-auto flex h-8 items-center gap-1.5 rounded-lg bg-gray-100 px-3 text-xs font-medium text-gray-600 hover:bg-gray-200">
              <Download className="h-3.5 w-3.5" />
              Export
            </button>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="mb-6 grid gap-4 sm:grid-cols-6">
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Total</p>
            <p className="text-2xl font-bold text-gray-900">
              {data?.stats?.total || 0}
            </p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Pending</p>
            <p className="text-2xl font-bold text-yellow-600">
              {data?.stats?.pending || 0}
            </p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Confirmed</p>
            <p className="text-2xl font-bold text-green-600">
              {data?.stats?.confirmed || 0}
            </p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Released</p>
            <p className="text-2xl font-bold text-blue-600">
              {data?.stats?.released || 0}
            </p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Cancelled</p>
            <p className="text-2xl font-bold text-red-600">
              {data?.stats?.cancelled || 0}
            </p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Revenue</p>
            <p className="text-2xl font-bold text-gray-900">
              {formatCurrency(data?.stats?.revenue || 0)}
            </p>
          </div>
        </div>

        {/* Data Table */}
        <DataTable
          data={data?.bookings || []}
          columns={columns}
          loading={isLoading}
          emptyMessage="No bookings found"
          pagination={{
            page,
            totalPages: data?.pagination?.totalPages || 1,
            total: data?.pagination?.total || 0,
            onPageChange: setPage,
          }}
        />
      </div>

      {/* Booking Detail Modal */}
      {selectedBooking && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <h2 className="text-lg font-semibold">Booking Details</h2>
              <button
                onClick={() => setSelectedBooking(null)}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100"
              >
                ×
              </button>
            </div>
            <div className="p-4 space-y-4">
              <div className="grid gap-4 sm:grid-cols-2">
                <div>
                  <p className="text-sm text-gray-500">PNR</p>
                  <p className="font-medium">{selectedBooking.travel_operator_pnr}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Status</p>
                  <StatusBadge status={selectedBooking.booking_status} />
                </div>
                <div>
                  <p className="text-sm text-gray-500">Route</p>
                  <p className="font-medium">
                    {selectedBooking.origin} → {selectedBooking.destination}
                  </p>
                  {selectedBooking.jc_trips?.jc_routes && (
                    <div className="flex items-center gap-2 mt-1">
                      {selectedBooking.jc_trips.jc_routes.bitla_route_id && (
                        <span className="rounded bg-blue-50 px-1.5 py-0.5 text-xs text-blue-700">
                          ID: {selectedBooking.jc_trips.jc_routes.bitla_route_id}
                        </span>
                      )}
                      {selectedBooking.jc_trips.jc_routes.route_name && (
                        <span className="text-xs text-gray-500">
                          {selectedBooking.jc_trips.jc_routes.route_name}
                        </span>
                      )}
                    </div>
                  )}
                </div>
                <div>
                  <p className="text-sm text-gray-500">Travel Date</p>
                  <p className="font-medium">{formatDate(selectedBooking.travel_date)}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Boarding Point</p>
                  <p className="font-medium">{selectedBooking.boarding_point_name}</p>
                  <p className="text-xs text-gray-500">{selectedBooking.boarding_address}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Total Fare</p>
                  <p className="font-medium">{formatCurrency(selectedBooking.total_fare || 0)}</p>
                </div>
              </div>

              {/* Cancellation Details - Only show for cancelled bookings */}
              {selectedBooking.booking_status === 'cancelled' && (
                <div className="rounded-lg border border-red-200 bg-red-50 p-4">
                  <h3 className="mb-3 font-medium text-red-800">Cancellation Details</h3>
                  <div className="grid gap-3 sm:grid-cols-2">
                    {(() => {
                      const cancellation = selectedBooking.jc_cancellations?.[0] || null
                      const rawData = selectedBooking.raw_data || {}
                      const passengers = selectedBooking.jc_passengers || []

                      const cancelledAt = cancellation?.cancelled_at || selectedBooking.updated_at
                      const cancelledDate = cancelledAt ? formatDateDDMMYYYY(cancelledAt) : '--'
                      const cancelledBy = cancellation?.cancelled_by || rawData.cancelled_by || rawData.booked_by || '--'
                      const cancellationCharge = cancellation?.cancellation_charge ?? rawData.cancellation_charge ?? rawData.cancellation_charges ?? 0

                      // Calculate original fare: sum of all passengers' fares, or booking total_fare * passenger count
                      let originalFare = cancellation?.original_fare || 0
                      if (!originalFare && passengers.length > 0) {
                        // Try to sum passenger fares
                        const passengerFaresSum = passengers.reduce((sum: number, p: any) => sum + (p.fare || 0), 0)
                        if (passengerFaresSum > 0) {
                          originalFare = passengerFaresSum
                        } else if (selectedBooking.total_fare) {
                          // If total_fare is per-passenger, multiply by passenger count
                          originalFare = selectedBooking.total_fare * passengers.length
                        }
                      }
                      if (!originalFare) {
                        originalFare = selectedBooking.total_fare || 0
                      }

                      const refundAmount = cancellation?.refund_amount ?? (originalFare - cancellationCharge)
                      const passengerCount = passengers.length || 1

                      return (
                        <>
                          <div>
                            <p className="text-sm text-red-600">Cancelled Date</p>
                            <p className="font-medium">{cancelledDate}</p>
                          </div>
                          <div>
                            <p className="text-sm text-red-600">Cancelled By</p>
                            <p className="font-medium">{cancelledBy}</p>
                          </div>
                          <div>
                            <p className="text-sm text-red-600">Original Fare ({passengerCount} {passengerCount === 1 ? 'ticket' : 'tickets'})</p>
                            <p className="font-medium">{formatCurrency(originalFare)}</p>
                          </div>
                          <div>
                            <p className="text-sm text-red-600">Cancellation Charges</p>
                            <p className="font-medium text-orange-600">{formatCurrency(cancellationCharge)}</p>
                          </div>
                          <div className="sm:col-span-2">
                            <p className="text-sm text-red-600">Refund Amount</p>
                            <p className={`font-medium text-lg ${refundAmount >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {formatCurrency(refundAmount)}
                            </p>
                          </div>
                        </>
                      )
                    })()}
                  </div>
                </div>
              )}

              {/* Passengers */}
              {selectedBooking.jc_passengers?.length > 0 && (
                <div>
                  <h3 className="mb-2 font-medium">Passengers</h3>
                  <div className="space-y-2">
                    {selectedBooking.jc_passengers.map((passenger: any) => (
                      <div
                        key={passenger.id}
                        className="flex items-center justify-between rounded-lg border p-3"
                      >
                        <div>
                          <p className="font-medium">{passenger.full_name}</p>
                          <p className="text-sm text-gray-500">
                            {formatPhoneNumber(passenger.mobile)} • Seat: {passenger.seat_number}
                          </p>
                          {passenger.customer_id && (
                            <p className="text-xs text-gray-400 font-mono">ID: {passenger.customer_id}</p>
                          )}
                        </div>
                        <div className="flex flex-col items-end gap-1">
                          <StatusBadge
                            status={passenger.is_cancelled ? 'cancelled' : passenger.is_boarded ? 'boarded' : 'confirmed'}
                            size="sm"
                          />
                          {(passenger.repeat_count || 0) > 0 && (
                            <span className="inline-flex items-center gap-1 rounded bg-blue-100 px-1.5 py-0.5 text-xs text-blue-700">
                              <RefreshCw className="h-3 w-3" /> {passenger.repeat_count}x
                            </span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </DashboardLayout>
  )
}
