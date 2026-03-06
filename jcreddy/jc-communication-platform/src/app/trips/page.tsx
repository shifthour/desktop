'use client'

import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { StatusBadge } from '@/components/ui/status-badge'
import { formatDate, formatDateDDMMYYYY, formatPhoneNumber, formatCurrency, calculatePercentage } from '@/lib/utils'
import {
  Bus,
  Calendar,
  Clock,
  Users,
  MapPin,
  Phone,
  ChevronRight,
  ChevronLeft,
  Navigation,
  ArrowUpDown,
  TrendingUp,
  DollarSign,
  Truck,
  UserCircle,
  CreditCard,
  IndianRupee,
  Percent,
  Briefcase,
  Star,
  Info,
  Route,
  Building2,
  MapPinned,
  Sparkles,
  Timer,
  ArrowRight,
  LayoutGrid,
  List,
  XCircle,
  FileText,
  MessageSquare,
} from 'lucide-react'
import { PassengersModal } from '@/components/trips/passengers-modal'
import { MessageStatusModal } from '@/components/trips/message-status-modal'

async function fetchTrips(date: string, status: string) {
  const params = new URLSearchParams({ date, status })
  const res = await fetch(`/api/trips?${params}`)
  if (!res.ok) throw new Error('Failed to fetch trips')
  return res.json()
}

type SortOption = 'route_id' | 'departure' | 'occupancy' | 'route' | 'boarding_time'
type ViewMode = 'list' | 'grid'
type StatusTab = '' | 'assigned' | 'unassigned' | 'operational' | 'halted'

const sortOptions = [
  { value: 'route_id', label: 'Route ID' },
  { value: 'departure', label: 'Departure Time' },
  { value: 'occupancy', label: 'Occupancy %' },
  { value: 'route', label: 'Route (A-Z)' },
  { value: 'boarding_time', label: '1st BP Time' },
]

// Helper to calculate time until departure
function getTimeUntilDeparture(departureTime: string, travelDate: string): string {
  if (!departureTime) return ''
  const now = new Date()
  const [hours, minutes] = departureTime.split(':').map(Number)
  const departure = new Date(travelDate || now.toISOString().split('T')[0])
  departure.setHours(hours, minutes, 0, 0)

  const diff = departure.getTime() - now.getTime()
  if (diff < 0) return 'Departed'

  const totalHours = Math.floor(diff / (1000 * 60 * 60))
  const mins = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60))

  if (totalHours >= 24) {
    const days = Math.floor(totalHours / 24)
    return `${days}d ${totalHours % 24}h`
  }
  return `${totalHours}h ${mins}m`
}

// Helper to format duration
function formatDuration(departure: string, arrival: string): string {
  if (!departure || !arrival) return '--'
  const [dh, dm] = departure.split(':').map(Number)
  const [ah, am] = arrival.split(':').map(Number)

  let totalMins = (ah * 60 + am) - (dh * 60 + dm)
  if (totalMins < 0) totalMins += 24 * 60 // next day arrival

  const hours = Math.floor(totalMins / 60)
  const mins = totalMins % 60
  return `${hours}:${mins.toString().padStart(2, '0')} hrs`
}

// Helper to get dynamic trip status based on current time
function getTripStatus(departureTime: string, arrivalTime: string, travelDate: string): {
  status: 'scheduled' | 'boarding' | 'in_journey' | 'completed'
  label: string
  color: string
  borderColor: string
  bgColor: string
} {
  if (!departureTime || !travelDate) {
    return { status: 'scheduled', label: 'Trip Open', color: 'text-green-600', borderColor: 'border-green-500', bgColor: 'hover:bg-green-50' }
  }

  const now = new Date()
  const today = now.toISOString().split('T')[0]

  // Parse departure time
  const [dh, dm] = departureTime.split(':').map(Number)
  const departureDateTime = new Date(travelDate)
  departureDateTime.setHours(dh, dm, 0, 0)

  // Parse arrival time (could be next day)
  let arrivalDateTime: Date | null = null
  if (arrivalTime) {
    const [ah, am] = arrivalTime.split(':').map(Number)
    arrivalDateTime = new Date(travelDate)
    arrivalDateTime.setHours(ah, am, 0, 0)
    // If arrival is before departure, it's next day
    if (arrivalDateTime < departureDateTime) {
      arrivalDateTime.setDate(arrivalDateTime.getDate() + 1)
    }
  }

  // Calculate time difference
  const timeToDeparture = departureDateTime.getTime() - now.getTime()
  const minutesToDeparture = timeToDeparture / (1000 * 60)

  // If travel date is in the future
  if (travelDate > today) {
    return { status: 'scheduled', label: 'Trip Open', color: 'text-green-600', borderColor: 'border-green-500', bgColor: 'hover:bg-green-50' }
  }

  // If travel date is in the past
  if (travelDate < today) {
    return { status: 'completed', label: 'Completed', color: 'text-gray-500', borderColor: 'border-gray-300', bgColor: 'hover:bg-gray-50' }
  }

  // Travel date is today - check times
  if (minutesToDeparture > 60) {
    // More than 1 hour to departure
    return { status: 'scheduled', label: 'Trip Open', color: 'text-green-600', borderColor: 'border-green-500', bgColor: 'hover:bg-green-50' }
  } else if (minutesToDeparture > 0) {
    // Within 1 hour of departure - boarding phase
    return { status: 'boarding', label: 'Boarding', color: 'text-orange-600', borderColor: 'border-orange-500', bgColor: 'hover:bg-orange-50' }
  } else {
    // Departure time has passed
    if (arrivalDateTime && now < arrivalDateTime) {
      // Still before arrival - in journey
      return { status: 'in_journey', label: 'In Journey', color: 'text-blue-600', borderColor: 'border-blue-500', bgColor: 'hover:bg-blue-50' }
    } else if (arrivalDateTime && now >= arrivalDateTime) {
      // After arrival - completed
      return { status: 'completed', label: 'Completed', color: 'text-gray-500', borderColor: 'border-gray-300', bgColor: 'hover:bg-gray-50' }
    } else {
      // No arrival time, assume in journey
      return { status: 'in_journey', label: 'In Journey', color: 'text-blue-600', borderColor: 'border-blue-500', bgColor: 'hover:bg-blue-50' }
    }
  }
}

export default function TripsPage() {
  const today = new Date().toISOString().split('T')[0]
  const [date, setDate] = useState(today)
  const [status, setStatus] = useState('')
  const [sortBy, setSortBy] = useState<SortOption>('route_id')
  const [selectedTrip, setSelectedTrip] = useState<any>(null)
  const [showFaresModal, setShowFaresModal] = useState(false)
  const [faresTrip, setFaresTrip] = useState<any>(null)
  const [viewMode, setViewMode] = useState<ViewMode>('list')
  const [statusTab, setStatusTab] = useState<StatusTab>('')
  const [showPassengersModal, setShowPassengersModal] = useState(false)
  const [passengersTripId, setPassengersTripId] = useState<string | null>(null)
  const [showMessageStatusModal, setShowMessageStatusModal] = useState(false)
  const [messageStatusTripId, setMessageStatusTripId] = useState<string | null>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['trips', date, status],
    queryFn: () => fetchTrips(date, status),
    refetchInterval: 30000,
  })

  // Generate date options for quick navigation
  const getDateOptions = () => {
    const dates = []
    for (let i = -2; i <= 4; i++) {
      const d = new Date()
      d.setDate(d.getDate() + i)
      dates.push({
        date: d.toISOString().split('T')[0],
        label: i === 0 ? 'Today' : d.toLocaleDateString('en-US', { weekday: 'short', day: '2-digit', month: 'short' })
      })
    }
    return dates
  }

  // Sort and filter trips
  const allTrips = data?.trips || []

  // Filter by status tab
  const filteredByTab = allTrips.filter((trip: any) => {
    if (!statusTab) return true
    switch (statusTab) {
      case 'assigned': return trip.captain1_name || trip.vehicle_number
      case 'unassigned': return !trip.captain1_name && !trip.vehicle_number
      case 'operational': return trip.trip_status === 'in_transit' || trip.trip_status === 'scheduled'
      case 'halted': return trip.trip_status === 'cancelled' || trip.trip_status === 'halted'
      default: return true
    }
  })

  const trips = filteredByTab.sort((a: any, b: any) => {
    switch (sortBy) {
      case 'route_id':
        // Sort by bitla_route_id (numeric), then by departure_time
        const routeIdA = a.jc_routes?.bitla_route_id || 0
        const routeIdB = b.jc_routes?.bitla_route_id || 0
        if (routeIdA !== routeIdB) {
          return routeIdA - routeIdB
        }
        return (a.departure_time || '').localeCompare(b.departure_time || '')
      case 'departure':
        return (a.departure_time || '').localeCompare(b.departure_time || '')
      case 'occupancy':
        const occA = calculatePercentage(a.booked_seats || 0, a.total_seats || 1)
        const occB = calculatePercentage(b.booked_seats || 0, b.total_seats || 1)
        return occB - occA
      case 'route':
        const routeA = a.origin || a.jc_routes?.origin || ''
        const routeB = b.origin || b.jc_routes?.origin || ''
        return routeA.localeCompare(routeB)
      case 'boarding_time':
        return (a.first_boarding_time || a.departure_time || '').localeCompare(b.first_boarding_time || b.departure_time || '')
      default:
        return 0
    }
  })

  // Calculate tab counts
  const tabCounts = {
    all: allTrips.length,
    assigned: allTrips.filter((t: any) => t.captain1_name || t.vehicle_number).length,
    unassigned: allTrips.filter((t: any) => !t.captain1_name && !t.vehicle_number).length,
    operational: allTrips.filter((t: any) => t.trip_status === 'in_transit' || t.trip_status === 'scheduled').length,
    halted: allTrips.filter((t: any) => t.trip_status === 'cancelled' || t.trip_status === 'halted').length,
  }

  return (
    <DashboardLayout>
      <Header title="Trips" subtitle="Monitor active and scheduled trips" />

      <div className="p-4 sm:p-6">
        {/* Top Filters Row */}
        <div className="mb-4 rounded-xl border bg-white p-4">
          <div className="flex flex-wrap items-center gap-3">
            {/* Sort Dropdown */}
            <div className="flex items-center gap-2">
              <ArrowUpDown className="h-4 w-4 text-gray-400" />
              <select
                value={sortBy}
                onChange={(e) => setSortBy(e.target.value as SortOption)}
                className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm focus:border-blue-500 focus:outline-none"
              >
                {sortOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    Sort: {option.label}
                  </option>
                ))}
              </select>
            </div>

            {/* More Filters Button */}
            <button className="flex items-center gap-2 rounded-lg border border-gray-200 px-3 py-2 text-sm text-gray-600 hover:bg-gray-50">
              <span>More Filters</span>
            </button>

            {/* Date Selector */}
            <div className="relative ml-auto">
              <Calendar className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
              <input
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
                className="h-9 rounded-lg border border-gray-200 bg-white pl-10 pr-4 text-sm focus:border-blue-500 focus:outline-none"
              />
            </div>

            {/* Search Button */}
            <button className="flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700">
              Search
            </button>

            <button className="rounded-lg border border-gray-200 px-4 py-2 text-sm text-gray-600 hover:bg-gray-50">
              Clear
            </button>
          </div>
        </div>

        {/* Date Navigation & Tabs */}
        <div className="mb-4 flex flex-wrap items-center justify-between gap-4">
          {/* Status Tabs */}
          <div className="flex items-center gap-1 rounded-lg bg-gray-100 p-1">
            <button
              onClick={() => setStatusTab('')}
              className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                statusTab === '' ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              All <span className="ml-1 text-blue-600">{tabCounts.all}</span>
            </button>
            <button
              onClick={() => setStatusTab('assigned')}
              className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                statusTab === 'assigned' ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Assigned <span className="ml-1 text-gray-400">{tabCounts.assigned}</span>
            </button>
            <button
              onClick={() => setStatusTab('unassigned')}
              className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                statusTab === 'unassigned' ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Unassigned <span className="ml-1 text-gray-400">{tabCounts.unassigned}</span>
            </button>
            <button
              onClick={() => setStatusTab('operational')}
              className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                statusTab === 'operational' ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Operational <span className="ml-1 text-gray-400">{tabCounts.operational}</span>
            </button>
            <button
              onClick={() => setStatusTab('halted')}
              className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                statusTab === 'halted' ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Halted <span className="ml-1 text-gray-400">{tabCounts.halted}</span>
            </button>
          </div>

          {/* Quick Date Navigation */}
          <div className="flex items-center gap-2">
            <button className="p-1.5 rounded-lg hover:bg-gray-100">
              <ChevronLeft className="h-4 w-4 text-gray-500" />
            </button>
            <div className="flex gap-1">
              {getDateOptions().map((d) => (
                <button
                  key={d.date}
                  onClick={() => setDate(d.date)}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                    date === d.date
                      ? 'bg-blue-600 text-white'
                      : 'text-gray-600 hover:bg-gray-100'
                  }`}
                >
                  {d.label}
                </button>
              ))}
            </div>
            <button className="p-1.5 rounded-lg hover:bg-gray-100">
              <ChevronRight className="h-4 w-4 text-gray-500" />
            </button>
          </div>

          {/* Trip Count & View Toggle */}
          <div className="flex items-center gap-3">
            <span className="text-sm text-gray-500">
              <span className="font-medium text-gray-900">{trips.length}</span> Trips Found
            </span>
            <div className="flex items-center gap-1 rounded-lg border p-1">
              <button
                onClick={() => setViewMode('list')}
                className={`p-1.5 rounded ${viewMode === 'list' ? 'bg-gray-100' : ''}`}
              >
                <List className="h-4 w-4 text-gray-600" />
              </button>
              <button
                onClick={() => setViewMode('grid')}
                className={`p-1.5 rounded ${viewMode === 'grid' ? 'bg-gray-100' : ''}`}
              >
                <LayoutGrid className="h-4 w-4 text-gray-600" />
              </button>
            </div>
          </div>
        </div>

        {/* Trips List */}
        {isLoading ? (
          <div className="flex items-center justify-center py-12">
            <div className="h-8 w-8 animate-spin rounded-full border-4 border-blue-600 border-t-transparent" />
          </div>
        ) : trips.length === 0 ? (
          <div className="flex flex-col items-center justify-center rounded-2xl border bg-white py-12">
            <Bus className="mb-4 h-12 w-12 text-gray-300" />
            <p className="text-gray-500">No trips found for this date</p>
          </div>
        ) : viewMode === 'list' ? (
          /* List View - matching screenshot */
          <div className="space-y-3">
            {trips.map((trip: any) => {
              const occupancy = calculatePercentage(trip.booked_seats || 0, trip.total_seats || 1)
              // Use available_seats from API directly, or calculate: total - booked - blocked
              const availableSeats = trip.available_seats ?? ((trip.total_seats || 0) - (trip.booked_seats || 0) - (trip.blocked_seats || 0))
              const minFare = trip.min_fare || trip.jc_routes?.base_fare || 800
              const maxFare = trip.max_fare || minFare * 1.4
              const timeUntil = getTimeUntilDeparture(trip.departure_time, trip.travel_date)

              return (
                <div
                  key={trip.id}
                  onClick={() => setSelectedTrip(trip)}
                  className="cursor-pointer rounded-xl border bg-white hover:border-blue-300 hover:shadow-md transition-all overflow-hidden"
                >
                  {/* View Passengers & Message Status Buttons - Top Bar */}
                  <div className="flex items-center justify-between px-4 py-2 bg-gray-50 border-b">
                    <span className="text-xs text-gray-500">
                      Route ID: {trip.jc_routes?.bitla_route_id || trip.service_number || '--'}
                    </span>
                    <div className="flex items-center gap-2">
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          setMessageStatusTripId(trip.id)
                          setSelectedTrip(trip)
                          setShowMessageStatusModal(true)
                        }}
                        className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-purple-600 text-white text-xs font-medium hover:bg-purple-700 transition-colors"
                      >
                        <MessageSquare className="h-3.5 w-3.5" />
                        Message Status
                      </button>
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          setPassengersTripId(trip.id)
                          setSelectedTrip(trip)
                          setShowPassengersModal(true)
                        }}
                        className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-green-600 text-white text-xs font-medium hover:bg-green-700 transition-colors"
                      >
                        <Users className="h-3.5 w-3.5" />
                        View Passengers
                      </button>
                    </div>
                  </div>
                  <div className="flex flex-col lg:flex-row">
                    {/* Left Section - Route Info */}
                    <div className="flex-1 p-4 border-b lg:border-b-0 lg:border-r">
                      <div className="flex items-start gap-3">
                        <div className="flex flex-col items-center">
                          <div className="w-2 h-2 rounded-full bg-blue-600"></div>
                          <div className="w-0.5 h-8 bg-gray-200"></div>
                          <div className="w-2 h-2 rounded-full bg-green-600"></div>
                        </div>
                        <div className="flex-1">
                          <h3 className="font-semibold text-gray-900 text-lg">
                            {trip.origin || trip.jc_routes?.origin || 'Origin'} - {trip.destination || trip.jc_routes?.destination || 'Destination'}
                          </h3>

                          {/* Route Name */}
                          {trip.jc_routes?.route_name && (
                            <p className="text-sm text-blue-600 mt-0.5">
                              {trip.jc_routes.route_name}
                            </p>
                          )}

                          {/* Depart countdown badge */}
                          {timeUntil && timeUntil !== 'Departed' && (
                            <span className="inline-flex items-center gap-1 mt-2 px-2 py-1 rounded-full bg-orange-100 text-orange-700 text-xs font-medium">
                              <Timer className="h-3 w-3" />
                              Depart in {timeUntil}
                            </span>
                          )}

                          {/* Bus Type */}
                          <p className="text-sm text-gray-600 mt-2">
                            {trip.bus_type || 'Sleeper A/C'}
                          </p>

                          {/* AC Badge */}
                          <span className="inline-flex items-center mt-2 px-2 py-0.5 rounded bg-cyan-100 text-cyan-700 text-xs font-medium">
                            AC
                          </span>
                        </div>
                      </div>
                    </div>

                    {/* Center Section - Timing & Service Info */}
                    <div className="flex-1 p-4 border-b lg:border-b-0 lg:border-r">
                      <div className="flex items-center justify-between">
                        {/* Departure */}
                        <div className="text-center">
                          <p className="text-xs text-gray-500 flex items-center gap-1">
                            <span className="text-green-600">↑</span> Departure
                          </p>
                          <p className="text-xl font-bold text-gray-900">{trip.departure_time || '00:00'}</p>
                        </div>

                        {/* Duration */}
                        <div className="text-center px-4">
                          <p className="text-xs text-gray-400">
                            ⏱ {formatDuration(trip.departure_time, trip.arrival_time)}
                          </p>
                          <div className="flex items-center gap-2 my-1">
                            <div className="h-px flex-1 bg-gray-300"></div>
                            <div className="flex gap-0.5">
                              {[...Array(5)].map((_, i) => (
                                <div key={i} className="w-1 h-1 rounded-full bg-gray-400"></div>
                              ))}
                            </div>
                            <div className="h-px flex-1 bg-gray-300"></div>
                          </div>
                        </div>

                        {/* Arrival */}
                        <div className="text-center">
                          <p className="text-xs text-gray-500 flex items-center gap-1">
                            <span className="text-red-600">↓</span> Arrival
                          </p>
                          <p className="text-xl font-bold text-gray-900">{trip.arrival_time || '00:00'}</p>
                        </div>

                        {/* Distance */}
                        <div className="text-center ml-4">
                          <p className="text-xs text-gray-500">🚌 Distance</p>
                          <p className="text-sm font-medium text-gray-700">{trip.distance || trip.jc_routes?.distance_km || 0} km</p>
                        </div>
                      </div>

                      {/* Service Code & Trip Status */}
                      <div className="flex items-center justify-between mt-4 pt-3 border-t">
                        <span className="px-2 py-1 rounded bg-blue-100 text-blue-700 text-sm font-medium">
                          {trip.service_number || trip.jc_routes?.service_number || 'SVC-001'}
                        </span>
                        {(() => {
                          const tripStatus = getTripStatus(trip.departure_time, trip.arrival_time, trip.travel_date)
                          return (
                            <span className={`px-3 py-1.5 rounded-lg border-2 ${tripStatus.borderColor} ${tripStatus.color} text-sm font-medium ${tripStatus.bgColor}`}>
                              {tripStatus.label}
                            </span>
                          )
                        })()}
                      </div>

                      {/* Coach & Crew Info */}
                      <div className="flex items-center flex-wrap gap-2 mt-3">
                        {trip.coach_num && (
                          <span className="px-2 py-0.5 rounded-full bg-gray-100 text-gray-700 text-xs flex items-center gap-1">
                            <Truck className="h-3 w-3" />
                            {trip.coach_num}
                          </span>
                        )}
                        {trip.captain1_name && (
                          <span className="px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 text-xs flex items-center gap-1">
                            <UserCircle className="h-3 w-3" />
                            {trip.captain1_name}
                          </span>
                        )}
                        {trip.attendant_name && (
                          <span className="px-2 py-0.5 rounded-full bg-purple-100 text-purple-700 text-xs flex items-center gap-1">
                            <UserCircle className="h-3 w-3" />
                            {trip.attendant_name}
                          </span>
                        )}
                      </div>
                    </div>

                    {/* Right Section - Seat Layout, Fare & Availability */}
                    <div className="w-full lg:w-80 p-4">
                      <div className="flex items-start gap-4">
                        {/* Seat Stats Visual */}
                        <div className="flex-shrink-0">
                          <p className="text-[10px] text-gray-400 mb-1 text-center">Seat Status</p>
                          <div className="p-2 bg-gray-50 rounded-lg text-center min-w-[80px]">
                            <div className="text-2xl font-bold text-blue-600">{trip.booked_seats || 0}</div>
                            <div className="text-[10px] text-gray-500">Booked</div>
                            <div className="mt-1 text-lg font-semibold text-green-600">{trip.available_seats || availableSeats}</div>
                            <div className="text-[10px] text-green-500">Available</div>
                            {(trip.blocked_seats || 0) > 0 && (
                              <>
                                <div className="mt-1 text-sm font-medium text-orange-500">{trip.blocked_seats}</div>
                                <div className="text-[10px] text-orange-400">Blocked</div>
                              </>
                            )}
                            {(trip.phone_booking_count || 0) > 0 && (
                              <>
                                <div className="mt-1 text-sm font-medium text-purple-500">{trip.phone_booking_count}</div>
                                <div className="text-[10px] text-purple-400">Phone</div>
                              </>
                            )}
                            {trip.cancelled_seats > 0 && (
                              <>
                                <div className="mt-1 text-sm font-medium text-red-500">{trip.cancelled_seats}</div>
                                <div className="text-[10px] text-red-400">Cancelled</div>
                              </>
                            )}
                          </div>
                        </div>

                        {/* Fare & Stats */}
                        <div className="flex-1">
                          {/* Fare Info */}
                          <div className="flex items-center gap-2 mb-2">
                            <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-green-100">
                              <span className="text-xs font-bold text-green-600">{occupancy}%</span>
                            </div>
                            {/* Revenue */}
                            <div>
                              <p className="text-xs text-gray-500">Revenue</p>
                              <p className="text-lg font-bold text-gray-900">
                                ₹ {(trip.total_revenue || 0).toLocaleString()}
                              </p>
                            </div>
                          </div>

                          {/* Booking Stats */}
                          <div className="flex items-center gap-3 text-xs text-gray-500 mb-2">
                            <span className="flex items-center gap-1" title="Booked">
                              <Users className="h-3 w-3 text-blue-500" />
                              {trip.booked_seats || 0}
                            </span>
                            <span className="flex items-center gap-1" title="Boarded">
                              <Bus className="h-3 w-3 text-green-500" />
                              {trip.boarded_count || 0}
                            </span>
                            <span className="flex items-center gap-1" title="Cancelled">
                              <XCircle className="h-3 w-3 text-red-500" />
                              {trip.cancelled_seats || 0}
                            </span>
                          </div>

                          {/* Occupancy indicator */}
                          <div className="flex items-center gap-2 mb-3">
                            <div className="relative w-8 h-8">
                              <svg className="w-8 h-8 transform -rotate-90">
                                <circle cx="16" cy="16" r="12" fill="none" stroke="#e5e7eb" strokeWidth="3" />
                                <circle
                                  cx="16" cy="16" r="12" fill="none"
                                  stroke={occupancy >= 80 ? '#22c55e' : occupancy >= 50 ? '#eab308' : '#ef4444'}
                                  strokeWidth="3"
                                  strokeDasharray={`${occupancy * 0.75} 100`}
                                />
                              </svg>
                            </div>
                            {/* Available Seats Button */}
                            <div className="flex-1 rounded-lg bg-orange-500 text-white text-center py-2 hover:bg-orange-600">
                              <div className="text-sm font-semibold">{availableSeats} Available</div>
                              {trip.phone_booking_count > 0 && (
                                <div className="text-[10px] opacity-90 flex items-center justify-center gap-1">
                                  <Phone className="h-3 w-3" /> {trip.phone_booking_count} Phone
                                </div>
                              )}
                            </div>
                          </div>

                                                  </div>
                      </div>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        ) : (
          /* Grid View - original card layout */
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {trips.map((trip: any) => (
              <div
                key={trip.id}
                onClick={() => setSelectedTrip(trip)}
                className="cursor-pointer rounded-2xl border bg-white p-6 transition-all hover:border-blue-200 hover:shadow-lg"
              >
                {/* Header */}
                <div className="mb-4 flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-blue-100">
                      <Bus className="h-6 w-6 text-blue-600" />
                    </div>
                    <div>
                      <p className="font-semibold text-gray-900">
                        {trip.origin || trip.jc_routes?.origin || 'Origin'} → {trip.destination || trip.jc_routes?.destination || 'Destination'}
                        <span className="ml-1 text-sm font-normal text-gray-500">
                          (Route ID: {trip.jc_routes?.bitla_route_id || trip.service_number || '--'})
                        </span>
                      </p>
                      {trip.jc_routes?.route_name && (
                        <p className="text-sm text-gray-500">
                          {trip.jc_routes.route_name}
                        </p>
                      )}
                    </div>
                  </div>
                  {(() => {
                    const tripStatus = getTripStatus(trip.departure_time, trip.arrival_time, trip.travel_date)
                    return (
                      <span className={`px-2 py-1 rounded-lg text-xs font-medium ${tripStatus.color} ${
                        tripStatus.status === 'scheduled' ? 'bg-green-100' :
                        tripStatus.status === 'boarding' ? 'bg-orange-100' :
                        tripStatus.status === 'in_journey' ? 'bg-blue-100' :
                        'bg-gray-100'
                      }`}>
                        {tripStatus.label}
                      </span>
                    )
                  })()}
                </div>

                {/* Info Grid */}
                <div className="mb-4 grid grid-cols-2 gap-4">
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 text-gray-400" />
                    <div>
                      <p className="text-xs text-gray-500">Departure</p>
                      <p className="text-sm font-medium">{trip.departure_time || '--:--'}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <Users className="h-4 w-4 text-gray-400" />
                    <div>
                      <p className="text-xs text-gray-500">Passengers</p>
                      <p className="text-sm font-medium">{trip.booked_seats || 0} booked</p>
                    </div>
                  </div>
                </div>

                {/* Occupancy Bar */}
                <div className="mb-4">
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs text-gray-500">Occupancy</span>
                    <span className={`text-sm font-bold ${
                      calculatePercentage(trip.booked_seats || 0, trip.total_seats || 1) >= 80
                        ? 'text-green-600'
                        : calculatePercentage(trip.booked_seats || 0, trip.total_seats || 1) >= 50
                          ? 'text-yellow-600'
                          : 'text-red-600'
                    }`}>
                      {calculatePercentage(trip.booked_seats || 0, trip.total_seats || 1)}%
                    </span>
                  </div>
                  <div className="h-2 overflow-hidden rounded-full bg-gray-100">
                    <div
                      className={`h-full transition-all ${
                        calculatePercentage(trip.booked_seats || 0, trip.total_seats || 1) >= 80
                          ? 'bg-green-500'
                          : calculatePercentage(trip.booked_seats || 0, trip.total_seats || 1) >= 50
                            ? 'bg-yellow-500'
                            : 'bg-red-500'
                      }`}
                      style={{
                        width: `${calculatePercentage(trip.booked_seats || 0, trip.total_seats || 1)}%`,
                      }}
                    />
                  </div>
                </div>

                {/* Fare Info */}
                <div className="grid grid-cols-2 gap-2">
                  <div className="rounded-lg bg-green-50 p-2">
                    <p className="text-xs text-green-600">Revenue</p>
                    <p className="text-sm font-bold text-green-700">
                      {formatCurrency(trip.total_revenue || trip.booked_seats * (trip.avg_fare || 500) || 0)}
                    </p>
                  </div>
                  <div className="rounded-lg bg-blue-50 p-2">
                    <p className="text-xs text-blue-600">Avg Fare</p>
                    <p className="text-sm font-bold text-blue-700">
                      {formatCurrency(trip.avg_fare || trip.jc_routes?.base_fare || 0)}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Trip Detail Modal */}
      {selectedTrip && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <div>
                <h2 className="text-lg font-semibold">Trip Details</h2>
                <p className="text-sm text-gray-500">{formatDate(selectedTrip.travel_date)}</p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => {
                    setPassengersTripId(selectedTrip.id)
                    setShowPassengersModal(true)
                  }}
                  className="flex items-center gap-2 rounded-lg bg-green-600 px-4 py-2 text-sm font-medium text-white hover:bg-green-700"
                >
                  <MessageSquare className="h-4 w-4" />
                  View Passengers & Send Message
                </button>
                <button
                  onClick={() => setSelectedTrip(null)}
                  className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100"
                >
                  ×
                </button>
              </div>
            </div>
            <div className="p-4 space-y-6">
              {/* Route */}
              <div className="flex items-center gap-4">
                <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-blue-100">
                  <Bus className="h-7 w-7 text-blue-600" />
                </div>
                <div className="flex-1">
                  <p className="text-xl font-semibold text-gray-900">
                    {selectedTrip.origin || selectedTrip.jc_routes?.origin} → {selectedTrip.destination || selectedTrip.jc_routes?.destination}
                    <span className="ml-2 text-base font-normal text-gray-500">
                      (Route ID: {selectedTrip.jc_routes?.bitla_route_id || selectedTrip.service_number || '--'})
                    </span>
                  </p>
                  {selectedTrip.jc_routes?.route_name && (
                    <p className="text-gray-500">{selectedTrip.jc_routes.route_name}</p>
                  )}
                  {selectedTrip.route_duration && (
                    <p className="text-sm text-gray-400">Duration: {selectedTrip.route_duration}</p>
                  )}
                </div>
                {(() => {
                  const tripStatus = getTripStatus(selectedTrip.departure_time, selectedTrip.arrival_time, selectedTrip.travel_date)
                  return (
                    <span className={`px-3 py-1.5 rounded-lg text-sm font-medium ${tripStatus.color} ${
                      tripStatus.status === 'scheduled' ? 'bg-green-100' :
                      tripStatus.status === 'boarding' ? 'bg-orange-100' :
                      tripStatus.status === 'in_journey' ? 'bg-blue-100' :
                      'bg-gray-100'
                    }`}>
                      {tripStatus.label}
                    </span>
                  )
                })()}
              </div>

              {/* Times */}
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="rounded-xl border p-4">
                  <p className="text-sm text-gray-500">Departure</p>
                  <p className="text-xl font-bold">{selectedTrip.departure_time || '--:--'}</p>
                </div>
                <div className="rounded-xl border p-4">
                  <p className="text-sm text-gray-500">Arrival</p>
                  <p className="text-xl font-bold">{selectedTrip.arrival_time || '--:--'}</p>
                </div>
              </div>

              {/* Seat Details - Comprehensive */}
              <div className="rounded-xl border p-4">
                <p className="mb-3 text-sm font-medium text-gray-700">Seat Breakdown</p>
                <div className="grid gap-3 grid-cols-2 sm:grid-cols-5">
                  <div className="text-center p-2 rounded-lg bg-gray-50">
                    <p className="text-2xl font-bold text-gray-900">{selectedTrip.total_seats || 40}</p>
                    <p className="text-xs text-gray-500">Total</p>
                  </div>
                  <div className="text-center p-2 rounded-lg bg-blue-50">
                    <p className="text-2xl font-bold text-blue-600">{selectedTrip.booked_seats || 0}</p>
                    <p className="text-xs text-blue-600">Booked</p>
                  </div>
                  <div className="text-center p-2 rounded-lg bg-green-50">
                    <p className="text-2xl font-bold text-green-600">{selectedTrip.available_seats || 0}</p>
                    <p className="text-xs text-green-600">Available</p>
                  </div>
                  <div className="text-center p-2 rounded-lg bg-orange-50">
                    <p className="text-2xl font-bold text-orange-600">{selectedTrip.blocked_seats || 0}</p>
                    <p className="text-xs text-orange-600">Blocked</p>
                  </div>
                  <div className="text-center p-2 rounded-lg bg-purple-50">
                    <p className="text-2xl font-bold text-purple-600">{selectedTrip.phone_booking_count || 0}</p>
                    <p className="text-xs text-purple-600">Phone</p>
                  </div>
                </div>
                {/* Blocked Seat Numbers */}
                {selectedTrip.blocked_seat_numbers && (
                  <div className="mt-3 pt-3 border-t">
                    <p className="text-xs text-orange-600 mb-1">Blocked Seats:</p>
                    <p className="text-sm font-mono text-gray-700">{selectedTrip.blocked_seat_numbers}</p>
                  </div>
                )}
                {/* Phone Booking Seat Numbers */}
                {selectedTrip.phone_booking_seat_numbers && (
                  <div className="mt-2">
                    <p className="text-xs text-purple-600 mb-1">Phone Booked Seats:</p>
                    <p className="text-sm font-mono text-gray-700">{selectedTrip.phone_booking_seat_numbers}</p>
                  </div>
                )}
                {/* Occupancy Bar */}
                <div className="mt-4">
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs text-gray-500">Occupancy</span>
                    <span className="text-sm font-bold">{selectedTrip.occupancy_percent || 0}%</span>
                  </div>
                  <div className="h-3 overflow-hidden rounded-full bg-gray-100">
                    <div
                      className={`h-full transition-all ${
                        (selectedTrip.occupancy_percent || 0) >= 80
                          ? 'bg-green-500'
                          : (selectedTrip.occupancy_percent || 0) >= 50
                            ? 'bg-yellow-500'
                            : 'bg-red-500'
                      }`}
                      style={{ width: `${selectedTrip.occupancy_percent || 0}%` }}
                    />
                  </div>
                </div>
              </div>

              {/* Revenue */}
              <div className="grid gap-3 sm:grid-cols-3">
                <div className="rounded-xl bg-green-50 p-4">
                  <p className="text-sm text-green-600">Total Revenue</p>
                  <p className="text-xl font-bold text-green-700">
                    {formatCurrency(selectedTrip.total_revenue || 0)}
                  </p>
                </div>
                <div className="rounded-xl bg-blue-50 p-4">
                  <p className="text-sm text-blue-600">Avg Fare</p>
                  <p className="text-xl font-bold text-blue-700">
                    {formatCurrency(selectedTrip.avg_fare || selectedTrip.jc_routes?.base_fare || 0)}
                  </p>
                </div>
                <div className="rounded-xl bg-orange-50 p-4">
                  <p className="text-sm text-orange-600">Bookings</p>
                  <p className="text-xl font-bold text-orange-700">
                    {selectedTrip.booking_count || 0}
                  </p>
                </div>
              </div>

              {/* Vehicle/Coach Details */}
              <div className="rounded-xl border p-4">
                <p className="mb-3 text-sm font-medium text-gray-700">Coach/Bus Details</p>
                <div className="flex items-center gap-3">
                  <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-gray-100">
                    <Truck className="h-6 w-6 text-gray-600" />
                  </div>
                  <div className="flex-1">
                    <p className="font-semibold text-gray-900">{selectedTrip.bus_type || 'Sleeper A/C'}</p>
                    {selectedTrip.coach_num && (
                      <p className="text-sm font-mono text-blue-600">{selectedTrip.coach_num}</p>
                    )}
                  </div>
                  {selectedTrip.coach_mobile_number && (
                    <a
                      href={`tel:${selectedTrip.coach_mobile_number}`}
                      className="flex items-center gap-2 rounded-lg bg-green-100 px-3 py-2 text-sm font-medium text-green-700 hover:bg-green-200"
                    >
                      <Phone className="h-4 w-4" />
                      {selectedTrip.coach_mobile_number}
                    </a>
                  )}
                </div>
              </div>

              {/* Crew Details */}
              <div className="rounded-xl border p-4">
                <p className="mb-3 text-sm font-medium text-gray-700">Crew Details</p>
                <div className="space-y-3">
                  {/* Captain 1 */}
                  {selectedTrip.captain1_name && (
                    <div className="flex items-center justify-between rounded-lg bg-blue-50 p-3">
                      <div className="flex items-center gap-3">
                        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-blue-100">
                          <UserCircle className="h-5 w-5 text-blue-600" />
                        </div>
                        <div>
                          <p className="font-medium">{selectedTrip.captain1_name}</p>
                          <p className="text-sm text-blue-600">Driver 1</p>
                        </div>
                      </div>
                      {selectedTrip.captain1_phone && (
                        <a
                          href={`tel:${selectedTrip.captain1_phone}`}
                          className="flex items-center gap-2 rounded-lg bg-green-100 px-3 py-2 text-sm font-medium text-green-700 hover:bg-green-200"
                        >
                          <Phone className="h-4 w-4" />
                          {selectedTrip.captain1_phone}
                        </a>
                      )}
                    </div>
                  )}
                  {/* Captain 2 */}
                  {selectedTrip.captain2_name && (
                    <div className="flex items-center justify-between rounded-lg bg-blue-50 p-3">
                      <div className="flex items-center gap-3">
                        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-blue-100">
                          <UserCircle className="h-5 w-5 text-blue-600" />
                        </div>
                        <div>
                          <p className="font-medium">{selectedTrip.captain2_name}</p>
                          <p className="text-sm text-blue-600">Driver 2</p>
                        </div>
                      </div>
                      {selectedTrip.captain2_phone && (
                        <a
                          href={`tel:${selectedTrip.captain2_phone}`}
                          className="flex items-center gap-2 rounded-lg bg-green-100 px-3 py-2 text-sm font-medium text-green-700 hover:bg-green-200"
                        >
                          <Phone className="h-4 w-4" />
                          {selectedTrip.captain2_phone}
                        </a>
                      )}
                    </div>
                  )}
                  {/* Attendant */}
                  {selectedTrip.attendant_name && (
                    <div className="flex items-center justify-between rounded-lg bg-purple-50 p-3">
                      <div className="flex items-center gap-3">
                        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-purple-100">
                          <UserCircle className="h-5 w-5 text-purple-600" />
                        </div>
                        <div>
                          <p className="font-medium">{selectedTrip.attendant_name}</p>
                          <p className="text-sm text-purple-600">Attendant</p>
                        </div>
                      </div>
                      {selectedTrip.attendant_phone && (
                        <a
                          href={`tel:${selectedTrip.attendant_phone}`}
                          className="flex items-center gap-2 rounded-lg bg-green-100 px-3 py-2 text-sm font-medium text-green-700 hover:bg-green-200"
                        >
                          <Phone className="h-4 w-4" />
                          {selectedTrip.attendant_phone}
                        </a>
                      )}
                    </div>
                  )}
                  {/* Helpline */}
                  {selectedTrip.helpline_number && (
                    <div className="flex items-center justify-between rounded-lg bg-gray-50 p-3">
                      <div className="flex items-center gap-3">
                        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-gray-100">
                          <Phone className="h-5 w-5 text-gray-600" />
                        </div>
                        <div>
                          <p className="font-medium">Helpline</p>
                          <p className="text-sm text-gray-500">{selectedTrip.helpline_number}</p>
                        </div>
                      </div>
                      <a
                        href={`tel:${selectedTrip.helpline_number}`}
                        className="flex items-center gap-2 rounded-lg bg-green-100 px-3 py-2 text-sm font-medium text-green-700 hover:bg-green-200"
                      >
                        <Phone className="h-4 w-4" />
                        Call
                      </a>
                    </div>
                  )}
                  {/* No crew assigned */}
                  {!selectedTrip.captain1_name && !selectedTrip.captain2_name && !selectedTrip.attendant_name && (
                    <p className="text-center text-gray-500 py-4">No crew assigned yet</p>
                  )}
                </div>
              </div>

              {/* Pickup Van Details */}
              {selectedTrip.pickup_van_details && selectedTrip.pickup_van_details.length > 0 && (
                <div className="rounded-xl border p-4">
                  <p className="mb-3 text-sm font-medium text-gray-700">Pickup Van Details</p>
                  <div className="space-y-2">
                    {selectedTrip.pickup_van_details.map((van: any, idx: number) => (
                      <div key={idx} className="flex items-center justify-between rounded-lg bg-gray-50 p-3">
                        <div>
                          <p className="font-medium">{van.van_number || van.vehicle_number || `Van ${idx + 1}`}</p>
                          {van.driver_name && <p className="text-sm text-gray-500">{van.driver_name}</p>}
                        </div>
                        {van.driver_phone && (
                          <a
                            href={`tel:${van.driver_phone}`}
                            className="text-sm text-blue-600 hover:underline"
                          >
                            {van.driver_phone}
                          </a>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Last Synced */}
              {selectedTrip.last_synced_at && (
                <div className="text-center text-xs text-gray-400 pt-4 border-t">
                  Last synced: {formatDate(selectedTrip.last_synced_at)}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Current Fares Modal */}
      {showFaresModal && faresTrip && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <div>
                <h2 className="text-lg font-semibold">Current Fares Info</h2>
                <p className="text-sm text-gray-500">
                  {faresTrip.origin || faresTrip.jc_routes?.origin || 'Origin'} → {faresTrip.destination || faresTrip.jc_routes?.destination || 'Destination'}
                </p>
              </div>
              <button
                onClick={() => {
                  setShowFaresModal(false)
                  setFaresTrip(null)
                }}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100"
              >
                ×
              </button>
            </div>
            <div className="p-4 space-y-6">
              <div className="grid gap-4 sm:grid-cols-3">
                <div className="rounded-xl bg-green-50 p-4">
                  <p className="text-sm text-green-600">Base Fare</p>
                  <p className="text-2xl font-bold text-green-700">
                    {formatCurrency(faresTrip.jc_routes?.base_fare || faresTrip.avg_fare || 500)}
                  </p>
                </div>
                <div className="rounded-xl bg-blue-50 p-4">
                  <p className="text-sm text-blue-600">Current Avg</p>
                  <p className="text-2xl font-bold text-blue-700">
                    {formatCurrency(faresTrip.avg_fare || faresTrip.jc_routes?.base_fare || 500)}
                  </p>
                </div>
                <div className="rounded-xl bg-purple-50 p-4">
                  <p className="text-sm text-purple-600">Max Fare</p>
                  <p className="text-2xl font-bold text-purple-700">
                    {formatCurrency(faresTrip.max_fare || (faresTrip.jc_routes?.base_fare || 500) * 1.5)}
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Passengers Modal for WhatsApp Messaging */}
      <PassengersModal
        isOpen={showPassengersModal}
        onClose={() => {
          setShowPassengersModal(false)
          setPassengersTripId(null)
        }}
        tripId={passengersTripId || ''}
        tripInfo={selectedTrip ? {
          service_number: selectedTrip.service_number,
          origin: selectedTrip.origin || selectedTrip.jc_routes?.origin,
          destination: selectedTrip.destination || selectedTrip.jc_routes?.destination,
          travel_date: selectedTrip.travel_date,
          departure_time: selectedTrip.departure_time,
        } : undefined}
      />

      {/* Message Status Modal */}
      <MessageStatusModal
        isOpen={showMessageStatusModal}
        onClose={() => {
          setShowMessageStatusModal(false)
          setMessageStatusTripId(null)
        }}
        tripId={messageStatusTripId || ''}
        tripInfo={selectedTrip ? {
          origin: selectedTrip.origin || selectedTrip.jc_routes?.origin,
          destination: selectedTrip.destination || selectedTrip.jc_routes?.destination,
          travel_date: selectedTrip.travel_date,
        } : undefined}
      />
    </DashboardLayout>
  )
}
