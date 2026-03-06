'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { DataTable } from '@/components/ui/data-table'
import { StatusBadge } from '@/components/ui/status-badge'
import {
  Search, Plus, Bus, Settings, MapPin, Fuel, Calendar,
  Edit2, Trash2, Eye, X, ChevronDown, ChevronUp
} from 'lucide-react'

interface Coach {
  id: string
  coach_number: string
  coach_name: string | null
  coach_type: string
  bus_type: string | null
  permit_type: string | null
  make: string | null
  model: string | null
  color: string | null
  year_of_manufacture: number | null
  chassis_number: string | null
  engine_number: string | null
  motor_serial_number: string | null
  licence_plate: string | null
  total_seats: number
  seating_capacity: number | null
  sleeping_capacity: number | null
  fuel_capacity: number | null
  fuel_type: string
  master_odometer: number
  current_odometer: number
  home_city: string | null
  current_location: string | null
  coach_mobile_number: string | null
  status: string
  rto_status: string
  insurance_expiry: string | null
  permit_expiry: string | null
  fitness_expiry: string | null
  puc_expiry: string | null
  tax_expiry: string | null
  amenities: string[] | null
  remarks: string | null
  created_at: string
}

async function fetchCoaches(page: number, search: string, status: string, coachType: string, city: string) {
  const params = new URLSearchParams({
    page: page.toString(),
    limit: '20',
    search,
    status,
    coachType,
    city,
  })
  const res = await fetch(`/api/coaches?${params}`)
  if (!res.ok) throw new Error('Failed to fetch coaches')
  return res.json()
}

async function createCoach(data: Partial<Coach>) {
  const res = await fetch('/api/coaches', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!res.ok) {
    const error = await res.json()
    throw new Error(error.error || 'Failed to create coach')
  }
  return res.json()
}

async function updateCoach({ id, data }: { id: string; data: Partial<Coach> }) {
  const res = await fetch(`/api/coaches/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!res.ok) {
    const error = await res.json()
    throw new Error(error.error || 'Failed to update coach')
  }
  return res.json()
}

async function deleteCoach(id: string) {
  const res = await fetch(`/api/coaches/${id}`, { method: 'DELETE' })
  if (!res.ok) throw new Error('Failed to delete coach')
  return res.json()
}

const COACH_TYPES = ['Regular', 'Pickup', 'Luxury', 'Mini', 'Multi-Axle']
const BUS_TYPES = ['A/C Sleeper', 'Non-A/C Sleeper', 'A/C Seater', 'Non-A/C Seater', 'Semi-Sleeper', 'Volvo Multi-Axle']
const PERMIT_TYPES = ['Route Permit', 'Inter State Permit', 'National Permit', 'Tourist Permit', 'Personal Car']
const FUEL_TYPES = ['Diesel', 'Petrol', 'CNG', 'Electric', 'Hybrid']
const STATUS_OPTIONS = ['available', 'in_service', 'maintenance', 'out_of_service', 'breakdown']
const RTO_STATUS_OPTIONS = ['operational', 'pending', 'expired']
const MAKES = ['Ashok Leyland', 'Volvo', 'TATA', 'Bharat Benz', 'Eicher', 'Mercedes', 'Scania', 'MAN']
const AMENITIES_OPTIONS = ['AC', 'WiFi', 'Charging Points', 'TV', 'Blankets', 'Water Bottle', 'Reading Light', 'GPS', 'CCTV', 'Emergency Exit']

export default function CoachesPage() {
  const queryClient = useQueryClient()
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [status, setStatus] = useState('')
  const [coachType, setCoachType] = useState('')
  const [city, setCity] = useState('')
  const [showModal, setShowModal] = useState(false)
  const [editingCoach, setEditingCoach] = useState<Coach | null>(null)
  const [viewingCoach, setViewingCoach] = useState<Coach | null>(null)
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [formData, setFormData] = useState<Partial<Coach>>({})
  const [formError, setFormError] = useState('')

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['coaches', page, search, status, coachType, city],
    queryFn: () => fetchCoaches(page, search, status, coachType, city),
  })

  const createMutation = useMutation({
    mutationFn: createCoach,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['coaches'] })
      setShowModal(false)
      setFormData({})
      setFormError('')
    },
    onError: (error: Error) => setFormError(error.message),
  })

  const updateMutation = useMutation({
    mutationFn: updateCoach,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['coaches'] })
      setShowModal(false)
      setEditingCoach(null)
      setFormData({})
      setFormError('')
    },
    onError: (error: Error) => setFormError(error.message),
  })

  const deleteMutation = useMutation({
    mutationFn: deleteCoach,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['coaches'] })
    },
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    setFormError('')

    if (!formData.coach_number) {
      setFormError('Coach number is required')
      return
    }

    if (editingCoach) {
      updateMutation.mutate({ id: editingCoach.id, data: formData })
    } else {
      createMutation.mutate(formData)
    }
  }

  const openAddModal = () => {
    setEditingCoach(null)
    setFormData({
      coach_type: 'Regular',
      status: 'available',
      rto_status: 'operational',
      fuel_type: 'Diesel',
      total_seats: 40,
    })
    setFormError('')
    setShowAdvanced(false)
    setShowModal(true)
  }

  const openEditModal = (coach: Coach) => {
    setEditingCoach(coach)
    setFormData({ ...coach })
    setFormError('')
    setShowAdvanced(false)
    setShowModal(true)
  }

  const handleDelete = (coach: Coach) => {
    if (confirm(`Are you sure you want to delete coach ${coach.coach_number}?`)) {
      deleteMutation.mutate(coach.id)
    }
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'available': return 'bg-green-100 text-green-700'
      case 'in_service': return 'bg-blue-100 text-blue-700'
      case 'maintenance': return 'bg-yellow-100 text-yellow-700'
      case 'out_of_service': return 'bg-gray-100 text-gray-700'
      case 'breakdown': return 'bg-red-100 text-red-700'
      default: return 'bg-gray-100 text-gray-700'
    }
  }

  const columns = [
    {
      key: 'coach_number',
      header: 'Coach Number',
      render: (coach: Coach) => (
        <div>
          <p className="font-semibold text-gray-900">{coach.coach_number}</p>
          {coach.coach_name && (
            <p className="text-xs text-gray-500">{coach.coach_name}</p>
          )}
        </div>
      ),
    },
    {
      key: 'type',
      header: 'Type',
      render: (coach: Coach) => (
        <div>
          <span className="rounded bg-purple-100 px-2 py-0.5 text-xs font-medium text-purple-700">
            {coach.coach_type}
          </span>
          {coach.bus_type && (
            <p className="mt-1 text-xs text-gray-500">{coach.bus_type}</p>
          )}
        </div>
      ),
    },
    {
      key: 'make_model',
      header: 'Make & Model',
      render: (coach: Coach) => (
        <div>
          <p className="font-medium text-gray-900">{coach.make || '--'}</p>
          {coach.model && <p className="text-xs text-gray-500">{coach.model}</p>}
        </div>
      ),
    },
    {
      key: 'city',
      header: 'Location',
      render: (coach: Coach) => (
        <div className="flex items-center gap-1">
          <MapPin className="h-3.5 w-3.5 text-gray-400" />
          <span className="text-sm">{coach.home_city || '--'}</span>
        </div>
      ),
    },
    {
      key: 'odometer',
      header: 'Odometer',
      render: (coach: Coach) => (
        <div>
          <p className="font-medium">{(coach.current_odometer || 0).toLocaleString()} km</p>
          <p className="text-xs text-gray-500">Master: {(coach.master_odometer || 0).toLocaleString()}</p>
        </div>
      ),
    },
    {
      key: 'seats',
      header: 'Seats',
      render: (coach: Coach) => (
        <span className="font-medium">{coach.total_seats}</span>
      ),
    },
    {
      key: 'status',
      header: 'Status',
      render: (coach: Coach) => (
        <div className="space-y-1">
          <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium capitalize ${getStatusColor(coach.status)}`}>
            {coach.status.replace('_', ' ')}
          </span>
          <p className={`text-xs ${coach.rto_status === 'operational' ? 'text-green-600' : coach.rto_status === 'expired' ? 'text-red-600' : 'text-yellow-600'}`}>
            RTO: {coach.rto_status}
          </p>
        </div>
      ),
    },
    {
      key: 'actions',
      header: '',
      render: (coach: Coach) => (
        <div className="flex items-center gap-1">
          <button
            onClick={() => setViewingCoach(coach)}
            className="rounded p-1.5 text-gray-400 hover:bg-gray-100 hover:text-gray-600"
            title="View Details"
          >
            <Eye className="h-4 w-4" />
          </button>
          <button
            onClick={() => openEditModal(coach)}
            className="rounded p-1.5 text-gray-400 hover:bg-blue-50 hover:text-blue-600"
            title="Edit"
          >
            <Edit2 className="h-4 w-4" />
          </button>
          <button
            onClick={() => handleDelete(coach)}
            className="rounded p-1.5 text-gray-400 hover:bg-red-50 hover:text-red-600"
            title="Delete"
          >
            <Trash2 className="h-4 w-4" />
          </button>
        </div>
      ),
    },
  ]

  return (
    <DashboardLayout>
      <Header title="Coaches" subtitle="Manage bus and coach fleet details" />

      <div className="p-4 sm:p-6">
        {/* Stats Cards */}
        <div className="mb-6 grid gap-4 sm:grid-cols-6">
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2">
              <Bus className="h-5 w-5 text-gray-400" />
              <p className="text-sm text-gray-500">Total</p>
            </div>
            <p className="mt-1 text-2xl font-bold text-gray-900">{data?.stats?.total || 0}</p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Available</p>
            <p className="mt-1 text-2xl font-bold text-green-600">{data?.stats?.available || 0}</p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">In Service</p>
            <p className="mt-1 text-2xl font-bold text-blue-600">{data?.stats?.in_service || 0}</p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Maintenance</p>
            <p className="mt-1 text-2xl font-bold text-yellow-600">{data?.stats?.maintenance || 0}</p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Out of Service</p>
            <p className="mt-1 text-2xl font-bold text-gray-600">{data?.stats?.out_of_service || 0}</p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <p className="text-sm text-gray-500">Breakdown</p>
            <p className="mt-1 text-2xl font-bold text-red-600">{data?.stats?.breakdown || 0}</p>
          </div>
        </div>

        {/* Filters */}
        <div className="mb-4 rounded-xl border bg-white p-3">
          <div className="flex flex-wrap items-center gap-2">
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search coach number, make..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="h-8 w-56 rounded-lg border border-gray-200 bg-gray-50 pl-8 pr-3 text-sm focus:border-blue-500 focus:bg-white focus:outline-none"
              />
            </div>

            <select
              value={status}
              onChange={(e) => setStatus(e.target.value)}
              className="h-8 rounded-lg border border-gray-200 bg-gray-50 px-2 text-xs focus:border-blue-500 focus:outline-none"
            >
              <option value="">All Status</option>
              {STATUS_OPTIONS.map(s => (
                <option key={s} value={s}>{s.replace('_', ' ')}</option>
              ))}
            </select>

            <select
              value={coachType}
              onChange={(e) => setCoachType(e.target.value)}
              className="h-8 rounded-lg border border-gray-200 bg-gray-50 px-2 text-xs focus:border-blue-500 focus:outline-none"
            >
              <option value="">All Types</option>
              {COACH_TYPES.map(t => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>

            <input
              type="text"
              placeholder="Filter by city..."
              value={city}
              onChange={(e) => setCity(e.target.value)}
              className="h-8 w-32 rounded-lg border border-gray-200 bg-gray-50 px-2 text-xs focus:border-blue-500 focus:outline-none"
            />

            <button
              onClick={openAddModal}
              className="ml-auto flex h-8 items-center gap-1.5 rounded-lg bg-blue-600 px-4 text-xs font-medium text-white hover:bg-blue-700"
            >
              <Plus className="h-4 w-4" />
              Add Coach
            </button>
          </div>
        </div>

        {/* Data Table */}
        <DataTable
          data={data?.coaches || []}
          columns={columns}
          loading={isLoading}
          emptyMessage="No coaches found"
          pagination={{
            page,
            totalPages: data?.pagination?.totalPages || 1,
            total: data?.pagination?.total || 0,
            onPageChange: setPage,
          }}
        />
      </div>

      {/* Add/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <h2 className="text-lg font-semibold">
                {editingCoach ? 'Edit Coach' : 'Add New Coach'}
              </h2>
              <button
                onClick={() => { setShowModal(false); setEditingCoach(null); setFormData({}); }}
                className="rounded-lg p-1 text-gray-400 hover:bg-gray-100"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <form onSubmit={handleSubmit} className="p-4">
              {formError && (
                <div className="mb-4 rounded-lg bg-red-50 p-3 text-sm text-red-600">
                  {formError}
                </div>
              )}

              {/* Basic Info */}
              <div className="mb-6">
                <h3 className="mb-3 font-medium text-gray-900">Basic Information</h3>
                <div className="grid gap-4 sm:grid-cols-3">
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">
                      Coach Number <span className="text-red-500">*</span>
                    </label>
                    <input
                      type="text"
                      value={formData.coach_number || ''}
                      onChange={(e) => setFormData({ ...formData, coach_number: e.target.value })}
                      placeholder="e.g., KA 01 AB 1234"
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                      required
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Coach Name</label>
                    <input
                      type="text"
                      value={formData.coach_name || ''}
                      onChange={(e) => setFormData({ ...formData, coach_name: e.target.value })}
                      placeholder="Optional friendly name"
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Coach Type</label>
                    <select
                      value={formData.coach_type || 'Regular'}
                      onChange={(e) => setFormData({ ...formData, coach_type: e.target.value })}
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    >
                      {COACH_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                    </select>
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Bus Type</label>
                    <select
                      value={formData.bus_type || ''}
                      onChange={(e) => setFormData({ ...formData, bus_type: e.target.value })}
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    >
                      <option value="">Select Bus Type</option>
                      {BUS_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                    </select>
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Permit Type</label>
                    <select
                      value={formData.permit_type || ''}
                      onChange={(e) => setFormData({ ...formData, permit_type: e.target.value })}
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    >
                      <option value="">Select Permit Type</option>
                      {PERMIT_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                    </select>
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Total Seats</label>
                    <input
                      type="number"
                      value={formData.total_seats || 40}
                      onChange={(e) => setFormData({ ...formData, total_seats: parseInt(e.target.value) })}
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    />
                  </div>
                </div>
              </div>

              {/* Vehicle Details */}
              <div className="mb-6">
                <h3 className="mb-3 font-medium text-gray-900">Vehicle Details</h3>
                <div className="grid gap-4 sm:grid-cols-3">
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Make</label>
                    <select
                      value={formData.make || ''}
                      onChange={(e) => setFormData({ ...formData, make: e.target.value })}
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    >
                      <option value="">Select Make</option>
                      {MAKES.map(m => <option key={m} value={m}>{m}</option>)}
                    </select>
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Model</label>
                    <input
                      type="text"
                      value={formData.model || ''}
                      onChange={(e) => setFormData({ ...formData, model: e.target.value })}
                      placeholder="e.g., B11R, Nexon"
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Color</label>
                    <input
                      type="text"
                      value={formData.color || ''}
                      onChange={(e) => setFormData({ ...formData, color: e.target.value })}
                      placeholder="e.g., White, Red"
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Year of Manufacture</label>
                    <input
                      type="number"
                      value={formData.year_of_manufacture || ''}
                      onChange={(e) => setFormData({ ...formData, year_of_manufacture: parseInt(e.target.value) || undefined })}
                      placeholder="e.g., 2022"
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Home City</label>
                    <input
                      type="text"
                      value={formData.home_city || ''}
                      onChange={(e) => setFormData({ ...formData, home_city: e.target.value })}
                      placeholder="e.g., Hyderabad"
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Coach Mobile</label>
                    <input
                      type="tel"
                      value={formData.coach_mobile_number || ''}
                      onChange={(e) => setFormData({ ...formData, coach_mobile_number: e.target.value })}
                      placeholder="e.g., 9876543210"
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    />
                  </div>
                </div>
              </div>

              {/* Status */}
              <div className="mb-6">
                <h3 className="mb-3 font-medium text-gray-900">Status</h3>
                <div className="grid gap-4 sm:grid-cols-3">
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Status</label>
                    <select
                      value={formData.status || 'available'}
                      onChange={(e) => setFormData({ ...formData, status: e.target.value })}
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    >
                      {STATUS_OPTIONS.map(s => (
                        <option key={s} value={s}>{s.replace('_', ' ')}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">RTO Status</label>
                    <select
                      value={formData.rto_status || 'operational'}
                      onChange={(e) => setFormData({ ...formData, rto_status: e.target.value })}
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    >
                      {RTO_STATUS_OPTIONS.map(s => (
                        <option key={s} value={s}>{s}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-gray-700">Fuel Type</label>
                    <select
                      value={formData.fuel_type || 'Diesel'}
                      onChange={(e) => setFormData({ ...formData, fuel_type: e.target.value })}
                      className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    >
                      {FUEL_TYPES.map(f => <option key={f} value={f}>{f}</option>)}
                    </select>
                  </div>
                </div>
              </div>

              {/* Advanced Section (Collapsible) */}
              <div className="mb-6">
                <button
                  type="button"
                  onClick={() => setShowAdvanced(!showAdvanced)}
                  className="flex w-full items-center justify-between rounded-lg bg-gray-50 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100"
                >
                  <span>Advanced Details</span>
                  {showAdvanced ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                </button>

                {showAdvanced && (
                  <div className="mt-4 space-y-6 rounded-lg border p-4">
                    {/* Technical Details */}
                    <div>
                      <h4 className="mb-3 text-sm font-medium text-gray-700">Technical Details</h4>
                      <div className="grid gap-4 sm:grid-cols-3">
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Chassis Number</label>
                          <input
                            type="text"
                            value={formData.chassis_number || ''}
                            onChange={(e) => setFormData({ ...formData, chassis_number: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Engine Number</label>
                          <input
                            type="text"
                            value={formData.engine_number || ''}
                            onChange={(e) => setFormData({ ...formData, engine_number: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Motor Serial Number</label>
                          <input
                            type="text"
                            value={formData.motor_serial_number || ''}
                            onChange={(e) => setFormData({ ...formData, motor_serial_number: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Licence Plate</label>
                          <input
                            type="text"
                            value={formData.licence_plate || ''}
                            onChange={(e) => setFormData({ ...formData, licence_plate: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Fuel Capacity (L)</label>
                          <input
                            type="number"
                            value={formData.fuel_capacity || ''}
                            onChange={(e) => setFormData({ ...formData, fuel_capacity: parseFloat(e.target.value) || undefined })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                      </div>
                    </div>

                    {/* Odometer */}
                    <div>
                      <h4 className="mb-3 text-sm font-medium text-gray-700">Odometer</h4>
                      <div className="grid gap-4 sm:grid-cols-2">
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Master Odometer (km)</label>
                          <input
                            type="number"
                            value={formData.master_odometer || 0}
                            onChange={(e) => setFormData({ ...formData, master_odometer: parseFloat(e.target.value) })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Current Odometer (km)</label>
                          <input
                            type="number"
                            value={formData.current_odometer || 0}
                            onChange={(e) => setFormData({ ...formData, current_odometer: parseFloat(e.target.value) })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                      </div>
                    </div>

                    {/* Document Expiry Dates */}
                    <div>
                      <h4 className="mb-3 text-sm font-medium text-gray-700">Document Expiry Dates</h4>
                      <div className="grid gap-4 sm:grid-cols-3">
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Insurance Expiry</label>
                          <input
                            type="date"
                            value={formData.insurance_expiry || ''}
                            onChange={(e) => setFormData({ ...formData, insurance_expiry: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Permit Expiry</label>
                          <input
                            type="date"
                            value={formData.permit_expiry || ''}
                            onChange={(e) => setFormData({ ...formData, permit_expiry: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Fitness Expiry</label>
                          <input
                            type="date"
                            value={formData.fitness_expiry || ''}
                            onChange={(e) => setFormData({ ...formData, fitness_expiry: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">PUC Expiry</label>
                          <input
                            type="date"
                            value={formData.puc_expiry || ''}
                            onChange={(e) => setFormData({ ...formData, puc_expiry: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                        <div>
                          <label className="mb-1 block text-xs font-medium text-gray-700">Tax Expiry</label>
                          <input
                            type="date"
                            value={formData.tax_expiry || ''}
                            onChange={(e) => setFormData({ ...formData, tax_expiry: e.target.value })}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                          />
                        </div>
                      </div>
                    </div>

                    {/* Remarks */}
                    <div>
                      <label className="mb-1 block text-xs font-medium text-gray-700">Remarks</label>
                      <textarea
                        value={formData.remarks || ''}
                        onChange={(e) => setFormData({ ...formData, remarks: e.target.value })}
                        rows={3}
                        className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                        placeholder="Any additional notes..."
                      />
                    </div>
                  </div>
                )}
              </div>

              {/* Submit Buttons */}
              <div className="flex justify-end gap-3 border-t pt-4">
                <button
                  type="button"
                  onClick={() => { setShowModal(false); setEditingCoach(null); setFormData({}); }}
                  className="rounded-lg border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={createMutation.isPending || updateMutation.isPending}
                  className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
                >
                  {createMutation.isPending || updateMutation.isPending ? 'Saving...' : editingCoach ? 'Update Coach' : 'Add Coach'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* View Details Modal */}
      {viewingCoach && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <h2 className="text-lg font-semibold">Coach Details</h2>
              <button
                onClick={() => setViewingCoach(null)}
                className="rounded-lg p-1 text-gray-400 hover:bg-gray-100"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="p-4 space-y-6">
              {/* Basic Info */}
              <div className="grid gap-4 sm:grid-cols-2">
                <div>
                  <p className="text-sm text-gray-500">Coach Number</p>
                  <p className="font-semibold text-lg">{viewingCoach.coach_number}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Status</p>
                  <span className={`inline-block rounded-full px-3 py-1 text-sm font-medium capitalize ${getStatusColor(viewingCoach.status)}`}>
                    {viewingCoach.status.replace('_', ' ')}
                  </span>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Make & Model</p>
                  <p className="font-medium">{viewingCoach.make || '--'} {viewingCoach.model || ''}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Type</p>
                  <p className="font-medium">{viewingCoach.coach_type} - {viewingCoach.bus_type || '--'}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Home City</p>
                  <p className="font-medium">{viewingCoach.home_city || '--'}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Total Seats</p>
                  <p className="font-medium">{viewingCoach.total_seats}</p>
                </div>
              </div>

              {/* Technical */}
              <div className="border-t pt-4">
                <h3 className="mb-3 font-medium text-gray-900">Technical Details</h3>
                <div className="grid gap-4 sm:grid-cols-2">
                  <div>
                    <p className="text-sm text-gray-500">Chassis Number</p>
                    <p className="font-mono text-sm">{viewingCoach.chassis_number || '--'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">Engine Number</p>
                    <p className="font-mono text-sm">{viewingCoach.engine_number || '--'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">Current Odometer</p>
                    <p className="font-medium">{(viewingCoach.current_odometer || 0).toLocaleString()} km</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">Fuel Type</p>
                    <p className="font-medium">{viewingCoach.fuel_type}</p>
                  </div>
                </div>
              </div>

              {/* Documents */}
              <div className="border-t pt-4">
                <h3 className="mb-3 font-medium text-gray-900">Document Expiry</h3>
                <div className="grid gap-4 sm:grid-cols-3">
                  <div>
                    <p className="text-sm text-gray-500">Insurance</p>
                    <p className={`font-medium ${viewingCoach.insurance_expiry && new Date(viewingCoach.insurance_expiry) < new Date() ? 'text-red-600' : ''}`}>
                      {viewingCoach.insurance_expiry || '--'}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">Permit</p>
                    <p className={`font-medium ${viewingCoach.permit_expiry && new Date(viewingCoach.permit_expiry) < new Date() ? 'text-red-600' : ''}`}>
                      {viewingCoach.permit_expiry || '--'}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">Fitness</p>
                    <p className={`font-medium ${viewingCoach.fitness_expiry && new Date(viewingCoach.fitness_expiry) < new Date() ? 'text-red-600' : ''}`}>
                      {viewingCoach.fitness_expiry || '--'}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">PUC</p>
                    <p className={`font-medium ${viewingCoach.puc_expiry && new Date(viewingCoach.puc_expiry) < new Date() ? 'text-red-600' : ''}`}>
                      {viewingCoach.puc_expiry || '--'}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">Tax</p>
                    <p className={`font-medium ${viewingCoach.tax_expiry && new Date(viewingCoach.tax_expiry) < new Date() ? 'text-red-600' : ''}`}>
                      {viewingCoach.tax_expiry || '--'}
                    </p>
                  </div>
                </div>
              </div>

              {viewingCoach.remarks && (
                <div className="border-t pt-4">
                  <h3 className="mb-2 font-medium text-gray-900">Remarks</h3>
                  <p className="text-sm text-gray-600">{viewingCoach.remarks}</p>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </DashboardLayout>
  )
}
