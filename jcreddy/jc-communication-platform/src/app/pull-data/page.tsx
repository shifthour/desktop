'use client'

import { useState, useEffect } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { formatCurrency } from '@/lib/utils'
import {
  Download,
  RefreshCw,
  CheckCircle,
  AlertCircle,
  Route,
  Bus,
  Users,
  Ticket,
  Clock,
  ArrowRight,
  Database,
  Filter,
  Search,
  Phone,
  MapPin,
  ChevronDown,
  ChevronUp,
  Plus,
  Edit3,
  Minus,
  FileJson,
  Eye,
  EyeOff,
} from 'lucide-react'

type RecordStatus = 'inserted' | 'updated' | 'not_modified'

interface RouteReconciliation {
  route_id: number
  route_num: string
  route_from: string
  route_to: string
  status: RecordStatus
  raw_data: any
}

interface PassengerReconciliation {
  pnr_number: string
  name: string
  seat_number: string
  mobile: string
  origin: string
  destination: string
  fare: number
  booked_by: string
  is_boarded: boolean
  booking_status: RecordStatus
  passenger_status: RecordStatus
  raw_data: any
}

interface TripReconciliation {
  route_id: number
  route_num: string
  origin: string
  destination: string
  travel_date: string
  departure_time: string
  arrival_time: string
  available_seats: number
  booked_seats: number
  blocked_seats: number
  phone_booking_count: number
  total_seats: number
  passenger_count: number
  status: RecordStatus
  passengers: PassengerReconciliation[]
  raw_data: any
}

interface SyncSummary {
  routes: {
    total: number
    active: number
    synced: number
    updated: number
    not_modified?: number
  }
  trips: {
    processed: number
  }
  bookings: {
    total: number
    inserted: number
    updated: number
    not_modified?: number
  }
  passengers: {
    total: number
    inserted: number
    updated: number
    not_modified?: number
  }
  errors?: string[]
}

interface RawData {
  routes: RouteReconciliation[]
  trips: TripReconciliation[]
}

interface PullResult {
  status: string
  message: string
  summary: SyncSummary
  raw_data?: RawData
  sync_job_id: string
  latency_ms: number
}

interface PassengerData {
  id: string
  pnr_number: string
  full_name: string
  mobile: string
  seat_number: string
  is_boarded: boolean
  booking: {
    travel_operator_pnr: string
    travel_date: string
    origin: string
    destination: string
    boarding_point_name: string
    dropoff_point_name: string
    total_fare: number
    booked_by: string
    booking_status: string
  }
  trip: {
    service_number: string
    bus_type: string
    departure_time: string
  }
  route: {
    id: string
    route_name: string
    origin: string
    destination: string
  }
}

interface RouteOption {
  id: string
  route_name: string
  origin: string
  destination: string
  service_number: string
}

// Status badge component
function StatusBadge({ status }: { status: RecordStatus }) {
  const config = {
    inserted: { bg: 'bg-green-100', text: 'text-green-700', icon: Plus, label: 'Inserted' },
    updated: { bg: 'bg-orange-100', text: 'text-orange-700', icon: Edit3, label: 'Updated' },
    not_modified: { bg: 'bg-gray-100', text: 'text-gray-600', icon: Minus, label: 'No Change' },
  }
  const { bg, text, icon: Icon, label } = config[status]
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${bg} ${text}`}>
      <Icon className="h-3 w-3" />
      {label}
    </span>
  )
}

interface BatchInfo {
  batch_number: number
  routes: { id: number; route_num: string; route_from: string; route_to: string }[]
}

export default function PullDataPage() {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<PullResult | null>(null)
  const [progress, setProgress] = useState<string>('')

  // Date selector - default to today
  const [selectedDate, setSelectedDate] = useState(() => {
    return new Date().toISOString().split('T')[0]
  })

  // Batch controls
  const [batchMode, setBatchMode] = useState<'all' | 'single'>('all')
  const [selectedBatch, setSelectedBatch] = useState<number>(1)
  const [availableBatches, setAvailableBatches] = useState<BatchInfo[]>([])
  const [totalBatches, setTotalBatches] = useState<number>(0)
  const [batchSize] = useState<number>(3) // 3 routes per batch

  // Data display state
  const [passengers, setPassengers] = useState<PassengerData[]>([])
  const [routes, setRoutes] = useState<RouteOption[]>([])
  const [loadingPassengers, setLoadingPassengers] = useState(false)
  const [selectedRoute, setSelectedRoute] = useState<string>('all')
  const [selectedStatus, setSelectedStatus] = useState<string>('all')
  const [searchQuery, setSearchQuery] = useState('')
  const [showFilters, setShowFilters] = useState(true)

  // Raw data display state
  const [showRawRoutes, setShowRawRoutes] = useState(true)
  const [showRawTrips, setShowRawTrips] = useState(true)
  const [expandedRoutes, setExpandedRoutes] = useState<Set<number>>(new Set())
  const [expandedTrips, setExpandedTrips] = useState<Set<number>>(new Set())
  const [expandedPassengers, setExpandedPassengers] = useState<Set<string>>(new Set())
  const [routeStatusFilter, setRouteStatusFilter] = useState<RecordStatus | 'all'>('all')
  const [passengerStatusFilter, setPassengerStatusFilter] = useState<RecordStatus | 'all'>('all')

  // Fetch batch information
  const fetchBatchInfo = async () => {
    try {
      const response = await fetch('/api/pull-data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ routes_only: true, batch_size: batchSize })
      })
      const data = await response.json()
      if (data.status === 'success' && data.batches) {
        setAvailableBatches(data.batches)
        setTotalBatches(data.total_batches)
      }
    } catch (err) {
      console.error('Failed to fetch batch info:', err)
    }
  }

  // Load batch info on mount
  useEffect(() => {
    fetchBatchInfo()
  }, [])

  const pullData = async () => {
    setLoading(true)
    setError(null)
    setResult(null)
    setProgress('Connecting to Bitla API...')

    try {
      // If single batch mode, use the new batch parameter
      if (batchMode === 'single') {
        setProgress(`Processing batch ${selectedBatch}/${totalBatches}...`)

        const response = await fetch('/api/pull-data', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            target_date: selectedDate,
            batch: selectedBatch,
            batch_size: batchSize
          })
        })

        const data = await response.json()

        if (data.status !== 'success') {
          throw new Error(data.message || 'Failed to pull data')
        }

        setResult({
          status: 'success',
          message: data.message,
          summary: data.summary,
          raw_data: data.raw_data,
          sync_job_id: data.sync_job_id || '',
          latency_ms: data.latency_ms
        })
        setProgress('')

        // Fetch passenger data after successful pull
        await fetchPassengerData(selectedDate)
        return
      }

      // Step 1: First fetch the list of active routes
      setProgress('Step 1/2: Fetching active routes list...')
      const routesResponse = await fetch('/api/pull-data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ routes_only: true })
      })

      const routesData = await routesResponse.json()

      if (routesData.status !== 'success' || !routesData.routes) {
        throw new Error(routesData.message || 'Failed to fetch routes')
      }

      const activeRoutes = routesData.routes
      const totalRoutes = activeRoutes.length

      setProgress(`Step 2/2: Processing ${totalRoutes} routes (0/${totalRoutes})...`)

      // Step 2: Process each route individually
      // Initialize aggregated results
      const allRouteReconciliations: RouteReconciliation[] = []
      const allTripReconciliations: TripReconciliation[] = []
      const aggregatedSummary: SyncSummary = {
        routes: { total: totalRoutes, active: totalRoutes, synced: 0, updated: 0, not_modified: 0 },
        trips: { processed: 0 },
        bookings: { total: 0, inserted: 0, updated: 0, not_modified: 0 },
        passengers: { total: 0, inserted: 0, updated: 0, not_modified: 0 },
        errors: []
      }

      let totalLatency = 0

      // Process routes in parallel batches of 5 for speed
      const BATCH_SIZE = 5
      const batches = []
      for (let i = 0; i < activeRoutes.length; i += BATCH_SIZE) {
        batches.push(activeRoutes.slice(i, i + BATCH_SIZE))
      }

      let processedCount = 0

      for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
        const batch = batches[batchIdx]
        setProgress(`Step 2/2: Processing batch ${batchIdx + 1}/${batches.length} (routes ${processedCount + 1}-${Math.min(processedCount + batch.length, totalRoutes)} of ${totalRoutes})...`)

        // Process all routes in this batch in parallel
        const batchPromises = batch.map(async (route: any) => {
          try {
            const routeResponse = await fetch('/api/pull-data', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ route_id: route.id, target_date: selectedDate })
            })

            // Check if response is OK before parsing JSON
            if (!routeResponse.ok) {
              const errorText = await routeResponse.text()
              return { route, routeData: null, error: `HTTP ${routeResponse.status}: ${errorText.substring(0, 100)}` }
            }

            // Try to parse JSON, handle non-JSON responses
            const responseText = await routeResponse.text()
            try {
              const routeData = JSON.parse(responseText)
              return { route, routeData, error: null }
            } catch {
              // Response is not valid JSON (might be error page from Bitla or server)
              return { route, routeData: null, error: `Invalid response: ${responseText.substring(0, 50)}...` }
            }
          } catch (routeErr: any) {
            return { route, routeData: null, error: routeErr.message || 'Network error' }
          }
        })

        // Wait for all routes in this batch to complete
        const batchResults = await Promise.all(batchPromises)

        // Process results
        for (const { route, routeData, error } of batchResults) {
          if (error) {
            aggregatedSummary.errors!.push(`Route ${route.route_num}: ${error}`)
            continue
          }

          if (routeData?.status === 'success') {
            // Aggregate the results
            totalLatency += routeData.latency_ms || 0

            // Add route reconciliations
            if (routeData.raw_data?.routes) {
              allRouteReconciliations.push(...routeData.raw_data.routes)
            }

            // Add trip reconciliations
            if (routeData.raw_data?.trips) {
              allTripReconciliations.push(...routeData.raw_data.trips)
            }

            // Aggregate summary
            if (routeData.summary) {
              aggregatedSummary.routes.synced += routeData.summary.routes?.synced || 0
              aggregatedSummary.routes.updated += routeData.summary.routes?.updated || 0
              aggregatedSummary.routes.not_modified = (aggregatedSummary.routes.not_modified || 0) + (routeData.summary.routes?.not_modified || 0)
              aggregatedSummary.trips.processed += routeData.summary.trips?.processed || 0
              aggregatedSummary.bookings.total += routeData.summary.bookings?.total || 0
              aggregatedSummary.bookings.inserted += routeData.summary.bookings?.inserted || 0
              aggregatedSummary.bookings.updated += routeData.summary.bookings?.updated || 0
              aggregatedSummary.bookings.not_modified = (aggregatedSummary.bookings.not_modified || 0) + (routeData.summary.bookings?.not_modified || 0)
              aggregatedSummary.passengers.total += routeData.summary.passengers?.total || 0
              aggregatedSummary.passengers.inserted += routeData.summary.passengers?.inserted || 0
              aggregatedSummary.passengers.updated += routeData.summary.passengers?.updated || 0
              aggregatedSummary.passengers.not_modified = (aggregatedSummary.passengers.not_modified || 0) + (routeData.summary.passengers?.not_modified || 0)

              if (routeData.summary.errors) {
                aggregatedSummary.errors!.push(...routeData.summary.errors)
              }
            }
          } else {
            // Add error for this route
            aggregatedSummary.errors!.push(`Route ${route.route_num}: ${routeData?.message || 'Unknown error'}`)
          }
        }

        processedCount += batch.length
      }

      // Build final result
      const finalResult: PullResult = {
        status: 'success',
        message: `Data pull completed for ${totalRoutes} routes`,
        summary: aggregatedSummary,
        raw_data: {
          routes: allRouteReconciliations,
          trips: allTripReconciliations
        },
        sync_job_id: '',
        latency_ms: totalLatency
      }

      setResult(finalResult)
      setProgress('')

      // After successful pull, fetch the passenger data to display
      await fetchPassengerData(selectedDate)

    } catch (err: any) {
      setError(err.message || 'Failed to pull data')
      setProgress('')
    } finally {
      setLoading(false)
    }
  }

  const fetchPassengerData = async (date?: string) => {
    setLoadingPassengers(true)
    try {
      const targetDate = date || selectedDate

      // Fetch passengers and routes in parallel
      const [passengersRes, routesRes] = await Promise.all([
        fetch(`/api/passengers?date=${targetDate}&limit=10000`),
        fetch('/api/routes')
      ])

      const passengersData = await passengersRes.json()
      const routesData = await routesRes.json()

      if (passengersData.passengers) {
        setPassengers(passengersData.passengers)
      }

      if (routesData.routes) {
        // Filter to only show routes that have passengers today
        const routeIdsWithPassengers = new Set(
          passengersData.passengers
            ?.map((p: PassengerData) => p.route?.id)
            .filter(Boolean) || []
        )

        const routesWithPassengers = routesData.routes
          .filter((r: any) => routeIdsWithPassengers.has(r.id))
          .map((r: any) => ({
            id: r.id,
            route_name: r.route_name || `${r.origin} - ${r.destination}`,
            origin: r.origin,
            destination: r.destination,
            service_number: r.service_number || '',
          }))

        setRoutes(routesWithPassengers)
      }
    } catch (err) {
      console.error('Failed to fetch passengers:', err)
    } finally {
      setLoadingPassengers(false)
    }
  }

  // Load passenger data on mount if we have existing data
  useEffect(() => {
    fetchPassengerData()
  }, [])

  // Filter passengers
  const filteredPassengers = passengers.filter((p) => {
    // Route filter
    if (selectedRoute !== 'all' && p.route?.id !== selectedRoute) {
      return false
    }

    // Status filter
    if (selectedStatus === 'boarded' && !p.is_boarded) {
      return false
    }
    if (selectedStatus === 'not_boarded' && p.is_boarded) {
      return false
    }
    if (selectedStatus === 'confirmed' && p.booking?.booking_status !== 'confirmed') {
      return false
    }
    if (selectedStatus === 'cancelled' && p.booking?.booking_status !== 'cancelled') {
      return false
    }

    // Search filter
    if (searchQuery) {
      const query = searchQuery.toLowerCase()
      return (
        p.full_name?.toLowerCase().includes(query) ||
        p.pnr_number?.toLowerCase().includes(query) ||
        p.mobile?.includes(query) ||
        p.seat_number?.toLowerCase().includes(query)
      )
    }

    return true
  })

  // Get status counts
  const statusCounts = {
    all: passengers.length,
    boarded: passengers.filter(p => p.is_boarded).length,
    not_boarded: passengers.filter(p => !p.is_boarded).length,
    confirmed: passengers.filter(p => p.booking?.booking_status === 'confirmed').length,
    cancelled: passengers.filter(p => p.booking?.booking_status === 'cancelled').length,
  }

  return (
    <DashboardLayout>
      <Header title="Pull Data" subtitle="Sync passenger and trip data from Bitla API" />

      <div className="p-4 sm:p-6">
        {/* Main Card */}
        <div className="mb-6 rounded-xl border bg-white p-6">
          <div className="text-center">
            {/* Icon */}
            <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-blue-100">
              <Database className="h-8 w-8 text-blue-600" />
            </div>

            {/* Title */}
            <h2 className="text-xl font-semibold text-gray-900 mb-2">
              Pull Data for Date
            </h2>

            {/* Date Selector */}
            <div className="mb-4 flex items-center justify-center gap-3">
              <label htmlFor="pullDate" className="text-sm font-medium text-gray-600">
                Select Date:
              </label>
              <input
                type="date"
                id="pullDate"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                className="px-4 py-2 rounded-lg border border-gray-300 text-gray-900 font-medium focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>

            {/* Selected Date Display */}
            <p className="text-gray-500 mb-1">
              {new Date(selectedDate + 'T00:00:00').toLocaleDateString('en-IN', {
                weekday: 'long',
                year: 'numeric',
                month: 'long',
                day: 'numeric',
              })}
            </p>

            {/* Batch Mode Toggle */}
            <div className="mb-4 flex items-center justify-center gap-3">
              <span className="text-sm font-medium text-gray-600">Mode:</span>
              <div className="flex rounded-lg border bg-gray-50 p-0.5">
                <button
                  onClick={() => setBatchMode('all')}
                  className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                    batchMode === 'all'
                      ? 'bg-white text-blue-600 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  All Routes
                </button>
                <button
                  onClick={() => setBatchMode('single')}
                  className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                    batchMode === 'single'
                      ? 'bg-white text-blue-600 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Single Batch
                </button>
              </div>
            </div>

            {/* Batch Selector (only shown in single batch mode) */}
            {batchMode === 'single' && totalBatches > 0 && (
              <div className="mb-4 p-4 rounded-xl border bg-gray-50 max-w-lg mx-auto">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-sm font-medium text-gray-700">Select Batch:</span>
                  <span className="text-xs text-gray-500">{totalBatches} batches ({batchSize} routes each)</span>
                </div>
                <div className="flex flex-wrap gap-2 justify-center">
                  {Array.from({ length: totalBatches }, (_, i) => i + 1).map((batchNum) => (
                    <button
                      key={batchNum}
                      onClick={() => setSelectedBatch(batchNum)}
                      className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                        selectedBatch === batchNum
                          ? 'bg-blue-600 text-white'
                          : 'bg-white border text-gray-700 hover:bg-gray-100'
                      }`}
                    >
                      Batch {batchNum}
                    </button>
                  ))}
                </div>
                {/* Show routes in selected batch */}
                {availableBatches[selectedBatch - 1] && (
                  <div className="mt-3 pt-3 border-t">
                    <p className="text-xs text-gray-500 mb-2">Routes in Batch {selectedBatch}:</p>
                    <div className="flex flex-wrap gap-1">
                      {availableBatches[selectedBatch - 1].routes.map((route) => (
                        <span
                          key={route.id}
                          className="px-2 py-1 bg-blue-100 text-blue-700 text-xs rounded"
                          title={`${route.route_from} → ${route.route_to}`}
                        >
                          {route.route_num} (ID: {route.id})
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Description */}
            <p className="text-sm text-gray-500 mb-6 max-w-md mx-auto">
              {batchMode === 'all'
                ? 'Click the button below to fetch all routes and passenger data for the selected date from Bitla API.'
                : `Click the button below to fetch only Batch ${selectedBatch} (${batchSize} routes) for the selected date.`}
            </p>

            {/* Pull Button */}
            <button
              onClick={pullData}
              disabled={loading}
              className="inline-flex items-center justify-center gap-2 rounded-xl bg-blue-600 px-8 py-3 text-base font-medium text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {loading ? (
                <>
                  <RefreshCw className="h-5 w-5 animate-spin" />
                  Pulling Data...
                </>
              ) : (
                <>
                  <Download className="h-5 w-5" />
                  {batchMode === 'all'
                    ? `Pull All Data for ${selectedDate}`
                    : `Pull Batch ${selectedBatch} for ${selectedDate}`}
                </>
              )}
            </button>

            {/* Progress */}
            {loading && progress && (
              <div className="mt-4 text-sm text-gray-500">
                {progress}
              </div>
            )}
          </div>
        </div>

        {/* Error Display */}
        {error && (
          <div className="mb-6 flex items-center gap-3 rounded-xl bg-red-50 border border-red-200 p-4">
            <AlertCircle className="h-5 w-5 text-red-600 flex-shrink-0" />
            <div>
              <p className="font-medium text-red-800">Error pulling data</p>
              <p className="text-sm text-red-600">{error}</p>
            </div>
          </div>
        )}

        {/* Success Result */}
        {result && result.status === 'success' && (
          <div className="mb-6 space-y-4">
            {/* Success Header */}
            <div className="flex items-center gap-3 rounded-xl bg-green-50 border border-green-200 p-4">
              <CheckCircle className="h-5 w-5 text-green-600 flex-shrink-0" />
              <div className="flex-1">
                <p className="font-medium text-green-800">{result.message}</p>
                <p className="text-sm text-green-600">
                  Completed in {(result.latency_ms / 1000).toFixed(1)} seconds
                </p>
              </div>
            </div>

            {/* Summary Cards */}
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              {/* Routes */}
              <div className="rounded-xl border bg-white p-4">
                <div className="flex items-center gap-3 mb-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-purple-100">
                    <Route className="h-5 w-5 text-purple-600" />
                  </div>
                  <h3 className="font-medium text-gray-900">Routes</h3>
                </div>
                <div className="space-y-1">
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500">Total</span>
                    <span className="font-medium text-gray-900">{result.summary.routes.total}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Plus className="h-3 w-3" /> Inserted</span>
                    <span className="font-medium text-green-600">{result.summary.routes.synced}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Edit3 className="h-3 w-3" /> Updated</span>
                    <span className="font-medium text-orange-600">{result.summary.routes.updated}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Minus className="h-3 w-3" /> No Change</span>
                    <span className="font-medium text-gray-500">{result.summary.routes.not_modified || 0}</span>
                  </div>
                </div>
              </div>

              {/* Trips */}
              <div className="rounded-xl border bg-white p-4">
                <div className="flex items-center gap-3 mb-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-blue-100">
                    <Bus className="h-5 w-5 text-blue-600" />
                  </div>
                  <h3 className="font-medium text-gray-900">Trips</h3>
                </div>
                <div className="space-y-1">
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500">Processed</span>
                    <span className="font-medium text-gray-900">{result.summary.trips.processed}</span>
                  </div>
                </div>
              </div>

              {/* Bookings */}
              <div className="rounded-xl border bg-white p-4">
                <div className="flex items-center gap-3 mb-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-green-100">
                    <Ticket className="h-5 w-5 text-green-600" />
                  </div>
                  <h3 className="font-medium text-gray-900">Bookings</h3>
                </div>
                <div className="space-y-1">
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500">Total</span>
                    <span className="font-medium text-gray-900">{result.summary.bookings.total}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Plus className="h-3 w-3" /> Inserted</span>
                    <span className="font-medium text-green-600">{result.summary.bookings.inserted}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Edit3 className="h-3 w-3" /> Updated</span>
                    <span className="font-medium text-orange-600">{result.summary.bookings.updated}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Minus className="h-3 w-3" /> No Change</span>
                    <span className="font-medium text-gray-500">{result.summary.bookings.not_modified || 0}</span>
                  </div>
                </div>
              </div>

              {/* Passengers */}
              <div className="rounded-xl border bg-white p-4">
                <div className="flex items-center gap-3 mb-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-orange-100">
                    <Users className="h-5 w-5 text-orange-600" />
                  </div>
                  <h3 className="font-medium text-gray-900">Passengers</h3>
                </div>
                <div className="space-y-1">
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500">Total</span>
                    <span className="font-medium text-gray-900">{result.summary.passengers.total}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Plus className="h-3 w-3" /> Inserted</span>
                    <span className="font-medium text-green-600">{result.summary.passengers.inserted}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Edit3 className="h-3 w-3" /> Updated</span>
                    <span className="font-medium text-orange-600">{result.summary.passengers.updated}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-500 flex items-center gap-1"><Minus className="h-3 w-3" /> No Change</span>
                    <span className="font-medium text-gray-500">{result.summary.passengers.not_modified || 0}</span>
                  </div>
                </div>
              </div>
            </div>

            {/* Errors (if any) */}
            {result.summary.errors && result.summary.errors.length > 0 && (
              <div className="rounded-xl border border-yellow-200 bg-yellow-50 p-4">
                <div className="flex items-center gap-2 mb-2">
                  <AlertCircle className="h-5 w-5 text-yellow-600" />
                  <h3 className="font-medium text-yellow-800">
                    {result.summary.errors.length} error(s) occurred
                  </h3>
                </div>
                <ul className="text-sm text-yellow-700 space-y-1 ml-7">
                  {result.summary.errors.slice(0, 5).map((err, idx) => (
                    <li key={idx}>{err}</li>
                  ))}
                  {result.summary.errors.length > 5 && (
                    <li>...and {result.summary.errors.length - 5} more</li>
                  )}
                </ul>
              </div>
            )}

            {/* RAW ROUTES DATA */}
            {result.raw_data?.routes && result.raw_data.routes.length > 0 && (
              <div className="rounded-xl border bg-white overflow-hidden">
                <div
                  className="p-4 border-b bg-purple-50 cursor-pointer flex items-center justify-between"
                  onClick={() => setShowRawRoutes(!showRawRoutes)}
                >
                  <div className="flex items-center gap-3">
                    <Route className="h-5 w-5 text-purple-600" />
                    <h3 className="font-semibold text-gray-900">
                      Step 1: Raw Routes Data ({result.raw_data.routes.length} routes)
                    </h3>
                  </div>
                  <div className="flex items-center gap-3">
                    {/* Status filter */}
                    <div className="flex gap-1">
                      {(['all', 'inserted', 'updated', 'not_modified'] as const).map((s) => (
                        <button
                          key={s}
                          onClick={(e) => { e.stopPropagation(); setRouteStatusFilter(s) }}
                          className={`px-2 py-1 text-xs rounded ${
                            routeStatusFilter === s
                              ? s === 'all' ? 'bg-gray-600 text-white' :
                                s === 'inserted' ? 'bg-green-600 text-white' :
                                s === 'updated' ? 'bg-orange-600 text-white' :
                                'bg-gray-500 text-white'
                              : 'bg-gray-100 text-gray-600'
                          }`}
                        >
                          {s === 'all' ? 'All' : s === 'inserted' ? 'New' : s === 'updated' ? 'Updated' : 'No Change'}
                        </button>
                      ))}
                    </div>
                    {showRawRoutes ? <ChevronUp className="h-5 w-5" /> : <ChevronDown className="h-5 w-5" />}
                  </div>
                </div>
                {showRawRoutes && (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50 border-b">
                        <tr>
                          <th className="px-4 py-3 text-left font-medium text-gray-500">Route ID</th>
                          <th className="px-4 py-3 text-left font-medium text-gray-500">Route Number</th>
                          <th className="px-4 py-3 text-left font-medium text-gray-500">From</th>
                          <th className="px-4 py-3 text-left font-medium text-gray-500">To</th>
                          <th className="px-4 py-3 text-center font-medium text-gray-500">Status</th>
                          <th className="px-4 py-3 text-center font-medium text-gray-500">Raw Data</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y">
                        {result.raw_data.routes
                          .filter(r => routeStatusFilter === 'all' || r.status === routeStatusFilter)
                          .map((route) => (
                          <tr key={route.route_id} className="hover:bg-gray-50">
                            <td className="px-4 py-3 font-mono text-gray-600">{route.route_id}</td>
                            <td className="px-4 py-3 font-medium text-gray-900">{route.route_num}</td>
                            <td className="px-4 py-3 text-gray-700">{route.route_from}</td>
                            <td className="px-4 py-3 text-gray-700">{route.route_to}</td>
                            <td className="px-4 py-3 text-center">
                              <StatusBadge status={route.status} />
                            </td>
                            <td className="px-4 py-3 text-center">
                              <button
                                onClick={() => {
                                  const newSet = new Set(expandedRoutes)
                                  if (newSet.has(route.route_id)) {
                                    newSet.delete(route.route_id)
                                  } else {
                                    newSet.add(route.route_id)
                                  }
                                  setExpandedRoutes(newSet)
                                }}
                                className="p-1 rounded hover:bg-gray-100"
                                title="View raw JSON"
                              >
                                {expandedRoutes.has(route.route_id) ? (
                                  <EyeOff className="h-4 w-4 text-gray-500" />
                                ) : (
                                  <Eye className="h-4 w-4 text-gray-500" />
                                )}
                              </button>
                            </td>
                          </tr>
                        ))}
                        {result.raw_data.routes
                          .filter(r => routeStatusFilter === 'all' || r.status === routeStatusFilter)
                          .map((route) => expandedRoutes.has(route.route_id) && (
                          <tr key={`${route.route_id}-raw`} className="bg-gray-50">
                            <td colSpan={6} className="px-4 py-3">
                              <pre className="text-xs bg-gray-900 text-green-400 p-3 rounded-lg overflow-x-auto max-h-48">
                                {JSON.stringify(route.raw_data, null, 2)}
                              </pre>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {/* RAW TRIPS & PASSENGERS DATA */}
            {result.raw_data?.trips && result.raw_data.trips.length > 0 && (
              <div className="rounded-xl border bg-white overflow-hidden">
                <div
                  className="p-4 border-b bg-blue-50 cursor-pointer flex items-center justify-between"
                  onClick={() => setShowRawTrips(!showRawTrips)}
                >
                  <div className="flex items-center gap-3">
                    <Bus className="h-5 w-5 text-blue-600" />
                    <h3 className="font-semibold text-gray-900">
                      Step 2: Raw Trips & Passengers Data ({result.raw_data.trips.length} trips, {result.raw_data.trips.reduce((sum, t) => sum + t.passenger_count, 0)} passengers)
                    </h3>
                  </div>
                  <div className="flex items-center gap-3">
                    {/* Status filter */}
                    <div className="flex gap-1">
                      {(['all', 'inserted', 'updated', 'not_modified'] as const).map((s) => (
                        <button
                          key={s}
                          onClick={(e) => { e.stopPropagation(); setPassengerStatusFilter(s) }}
                          className={`px-2 py-1 text-xs rounded ${
                            passengerStatusFilter === s
                              ? s === 'all' ? 'bg-gray-600 text-white' :
                                s === 'inserted' ? 'bg-green-600 text-white' :
                                s === 'updated' ? 'bg-orange-600 text-white' :
                                'bg-gray-500 text-white'
                              : 'bg-gray-100 text-gray-600'
                          }`}
                        >
                          {s === 'all' ? 'All' : s === 'inserted' ? 'New' : s === 'updated' ? 'Updated' : 'No Change'}
                        </button>
                      ))}
                    </div>
                    {showRawTrips ? <ChevronUp className="h-5 w-5" /> : <ChevronDown className="h-5 w-5" />}
                  </div>
                </div>
                {showRawTrips && (
                  <div className="divide-y">
                    {result.raw_data.trips.map((trip, tripIdx) => (
                      <div key={tripIdx} className="border-b last:border-b-0">
                        {/* Trip Header */}
                        <div
                          className="p-4 bg-gray-50 cursor-pointer flex items-center justify-between"
                          onClick={() => {
                            const newSet = new Set(expandedTrips)
                            if (newSet.has(tripIdx)) {
                              newSet.delete(tripIdx)
                            } else {
                              newSet.add(tripIdx)
                            }
                            setExpandedTrips(newSet)
                          }}
                        >
                          <div className="flex items-center gap-4">
                            <div className="flex items-center gap-2">
                              <span className="px-2 py-1 bg-blue-100 text-blue-700 text-xs font-medium rounded">
                                {trip.route_num}
                              </span>
                              <StatusBadge status={trip.status} />
                            </div>
                            <div>
                              <p className="font-medium text-gray-900">
                                {trip.origin} → {trip.destination}
                              </p>
                              <p className="text-xs text-gray-500">
                                {trip.departure_time} - {trip.arrival_time} |
                                <span className="text-green-600 font-medium"> {trip.booked_seats || trip.passenger_count} booked</span> |
                                <span className="text-blue-600"> {trip.available_seats} available</span> |
                                <span className="text-red-600"> {trip.blocked_seats || 0} blocked</span> |
                                <span className="text-purple-600"> {trip.phone_booking_count || 0} phone</span> |
                                Total: {trip.total_seats || ((trip.booked_seats || trip.passenger_count) + trip.available_seats + (trip.blocked_seats || 0))}
                              </p>
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            <span className="text-xs text-gray-500">
                              {trip.passengers.filter(p => p.passenger_status === 'inserted').length} new,{' '}
                              {trip.passengers.filter(p => p.passenger_status === 'updated').length} updated,{' '}
                              {trip.passengers.filter(p => p.passenger_status === 'not_modified').length} unchanged
                            </span>
                            {expandedTrips.has(tripIdx) ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                          </div>
                        </div>

                        {/* Passengers Table */}
                        {expandedTrips.has(tripIdx) && (
                          <div className="overflow-x-auto">
                            <table className="w-full text-sm">
                              <thead className="bg-gray-100 border-b">
                                <tr>
                                  <th className="px-4 py-2 text-left font-medium text-gray-500">PNR</th>
                                  <th className="px-4 py-2 text-left font-medium text-gray-500">Name</th>
                                  <th className="px-4 py-2 text-left font-medium text-gray-500">Seat</th>
                                  <th className="px-4 py-2 text-left font-medium text-gray-500">Mobile</th>
                                  <th className="px-4 py-2 text-left font-medium text-gray-500">Route</th>
                                  <th className="px-4 py-2 text-right font-medium text-gray-500">Fare</th>
                                  <th className="px-4 py-2 text-left font-medium text-gray-500">Booked By</th>
                                  <th className="px-4 py-2 text-center font-medium text-gray-500">Boarded</th>
                                  <th className="px-4 py-2 text-center font-medium text-gray-500">Booking Status</th>
                                  <th className="px-4 py-2 text-center font-medium text-gray-500">Passenger Status</th>
                                  <th className="px-4 py-2 text-center font-medium text-gray-500">Raw JSON</th>
                                </tr>
                              </thead>
                              <tbody className="divide-y">
                                {trip.passengers
                                  .filter(p => passengerStatusFilter === 'all' || p.passenger_status === passengerStatusFilter)
                                  .map((passenger, pIdx) => {
                                    const passengerKey = `${tripIdx}-${pIdx}`
                                    return (
                                    <>
                                      <tr key={pIdx} className="hover:bg-gray-50">
                                        <td className="px-4 py-2 font-mono text-xs text-gray-600">{passenger.pnr_number}</td>
                                        <td className="px-4 py-2 font-medium text-gray-900">{passenger.name}</td>
                                        <td className="px-4 py-2">
                                          <span className="px-2 py-0.5 bg-blue-100 text-blue-700 text-xs font-semibold rounded">
                                            {passenger.seat_number}
                                          </span>
                                        </td>
                                        <td className="px-4 py-2 text-gray-600">{passenger.mobile}</td>
                                        <td className="px-4 py-2 text-gray-600 text-xs">
                                          {passenger.origin} → {passenger.destination}
                                        </td>
                                        <td className="px-4 py-2 text-right font-medium text-green-600">
                                          ₹{passenger.fare}
                                        </td>
                                        <td className="px-4 py-2">
                                          <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                            passenger.booked_by === 'Redbus' ? 'bg-red-100 text-red-700' :
                                            passenger.booked_by === 'Paytm' ? 'bg-blue-100 text-blue-700' :
                                            passenger.booked_by === 'Abhi' ? 'bg-green-100 text-green-700' :
                                            'bg-gray-100 text-gray-700'
                                          }`}>
                                            {passenger.booked_by}
                                          </span>
                                        </td>
                                        <td className="px-4 py-2 text-center">
                                          {passenger.is_boarded ? (
                                            <span className="text-green-600">✓</span>
                                          ) : (
                                            <span className="text-gray-400">-</span>
                                          )}
                                        </td>
                                        <td className="px-4 py-2 text-center">
                                          <StatusBadge status={passenger.booking_status} />
                                        </td>
                                        <td className="px-4 py-2 text-center">
                                          <StatusBadge status={passenger.passenger_status} />
                                        </td>
                                        <td className="px-4 py-2 text-center">
                                          <button
                                            onClick={() => {
                                              const newSet = new Set(expandedPassengers)
                                              if (newSet.has(passengerKey)) {
                                                newSet.delete(passengerKey)
                                              } else {
                                                newSet.add(passengerKey)
                                              }
                                              setExpandedPassengers(newSet)
                                            }}
                                            className="p-1 rounded hover:bg-gray-100"
                                            title="View raw JSON"
                                          >
                                            {expandedPassengers.has(passengerKey) ? (
                                              <EyeOff className="h-4 w-4 text-gray-500" />
                                            ) : (
                                              <Eye className="h-4 w-4 text-gray-500" />
                                            )}
                                          </button>
                                        </td>
                                      </tr>
                                      {expandedPassengers.has(passengerKey) && (
                                        <tr key={`${pIdx}-raw`} className="bg-gray-50">
                                          <td colSpan={11} className="px-4 py-3">
                                            <pre className="text-xs bg-gray-900 text-green-400 p-3 rounded-lg overflow-x-auto max-h-48">
                                              {JSON.stringify(passenger.raw_data, null, 2)}
                                            </pre>
                                          </td>
                                        </tr>
                                      )}
                                    </>
                                  )})}
                              </tbody>
                            </table>

                            {/* Raw Trip Data Toggle */}
                            <div className="p-3 border-t bg-gray-50">
                              <button
                                onClick={() => {
                                  const newSet = new Set(expandedRoutes)
                                  const rawKey = 10000 + tripIdx
                                  if (newSet.has(rawKey)) {
                                    newSet.delete(rawKey)
                                  } else {
                                    newSet.add(rawKey)
                                  }
                                  setExpandedRoutes(newSet)
                                }}
                                className="flex items-center gap-2 text-xs text-gray-600 hover:text-gray-900"
                              >
                                <FileJson className="h-4 w-4" />
                                {expandedRoutes.has(10000 + tripIdx) ? 'Hide' : 'View'} Raw Trip JSON
                              </button>
                              {expandedRoutes.has(10000 + tripIdx) && (
                                <pre className="mt-2 text-xs bg-gray-900 text-green-400 p-3 rounded-lg overflow-x-auto max-h-64">
                                  {JSON.stringify(trip.raw_data, null, 2)}
                                </pre>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Today's Passengers section removed - data now shown in Raw Trips & Passengers section above */}

        {/* Empty State - Only show when not loading and no passengers */}
        {!loading && !loadingPassengers && passengers.length === 0 && !error && (
          <div className="rounded-xl border bg-gray-50 p-6">
            <h3 className="font-medium text-gray-900 mb-4">How it works</h3>
            <div className="space-y-3">
              <div className="flex items-start gap-3">
                <div className="flex h-6 w-6 items-center justify-center rounded-full bg-blue-100 text-xs font-medium text-blue-600">
                  1
                </div>
                <div>
                  <p className="font-medium text-gray-900">Fetch Active Routes</p>
                  <p className="text-sm text-gray-500">
                    Connects to Bitla API and fetches all active routes
                  </p>
                </div>
              </div>
              <div className="flex items-start gap-3">
                <div className="flex h-6 w-6 items-center justify-center rounded-full bg-blue-100 text-xs font-medium text-blue-600">
                  2
                </div>
                <div>
                  <p className="font-medium text-gray-900">Fetch Passengers for Selected Date</p>
                  <p className="text-sm text-gray-500">
                    For each active route, fetches passenger details for the selected date
                  </p>
                </div>
              </div>
              <div className="flex items-start gap-3">
                <div className="flex h-6 w-6 items-center justify-center rounded-full bg-blue-100 text-xs font-medium text-blue-600">
                  3
                </div>
                <div>
                  <p className="font-medium text-gray-900">Reconcile Data</p>
                  <p className="text-sm text-gray-500">
                    Compares with existing data - inserts new records, updates changed records, skips unchanged
                  </p>
                </div>
              </div>
              <div className="flex items-start gap-3">
                <div className="flex h-6 w-6 items-center justify-center rounded-full bg-green-100 text-xs font-medium text-green-600">
                  ✓
                </div>
                <div>
                  <p className="font-medium text-gray-900">View & Filter Data</p>
                  <p className="text-sm text-gray-500">
                    Passengers are displayed below with filters for route and status
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </DashboardLayout>
  )
}
