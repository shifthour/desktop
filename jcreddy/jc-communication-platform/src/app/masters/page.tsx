'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import {
  Search, Plus, Edit2, Trash2, X, Globe, Map, MapPin, Bus, Route,
  ChevronDown, ChevronUp, Building2
} from 'lucide-react'

type TabType = 'countries' | 'states' | 'cities' | 'coaches' | 'routes'

// ============ COUNTRIES TAB ============
function CountriesTab() {
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [showModal, setShowModal] = useState(false)
  const [editing, setEditing] = useState<any>(null)
  const [formData, setFormData] = useState<any>({})
  const [error, setError] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['countries', search],
    queryFn: async () => {
      const res = await fetch(`/api/masters/countries?search=${search}`)
      return res.json()
    },
  })

  const saveMutation = useMutation({
    mutationFn: async (data: any) => {
      const url = editing ? `/api/masters/countries/${editing.id}` : '/api/masters/countries'
      const res = await fetch(url, {
        method: editing ? 'PUT' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      })
      if (!res.ok) throw new Error((await res.json()).error)
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['countries'] })
      setShowModal(false)
      setEditing(null)
      setFormData({})
    },
    onError: (e: Error) => setError(e.message),
  })

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/masters/countries/${id}`, { method: 'DELETE' })
      if (!res.ok) throw new Error('Delete failed')
      return res.json()
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['countries'] }),
  })

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search countries..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-9 w-full rounded-lg border bg-gray-50 pl-9 pr-3 text-sm focus:border-blue-500 focus:bg-white focus:outline-none"
          />
        </div>
        <button
          onClick={() => { setEditing(null); setFormData({}); setError(''); setShowModal(true); }}
          className="flex h-9 items-center gap-1.5 rounded-lg bg-blue-600 px-4 text-sm font-medium text-white hover:bg-blue-700"
        >
          <Plus className="h-4 w-4" /> Add Country
        </button>
      </div>

      <div className="rounded-xl border bg-white">
        <table className="w-full text-sm">
          <thead className="border-b bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Code</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Country Name</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Phone Code</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Currency</th>
              <th className="px-4 py-3 text-right font-medium text-gray-600">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y">
            {isLoading ? (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-500">Loading...</td></tr>
            ) : data?.countries?.length === 0 ? (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-500">No countries found</td></tr>
            ) : (
              data?.countries?.map((item: any) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 font-mono font-medium">{item.country_code}</td>
                  <td className="px-4 py-3 font-medium">{item.country_name}</td>
                  <td className="px-4 py-3 text-gray-600">{item.phone_code || '--'}</td>
                  <td className="px-4 py-3 text-gray-600">{item.currency_code || '--'}</td>
                  <td className="px-4 py-3 text-right">
                    <button onClick={() => { setEditing(item); setFormData(item); setError(''); setShowModal(true); }} className="rounded p-1 text-gray-400 hover:bg-blue-50 hover:text-blue-600"><Edit2 className="h-4 w-4" /></button>
                    <button onClick={() => confirm('Delete this country?') && deleteMutation.mutate(item.id)} className="rounded p-1 text-gray-400 hover:bg-red-50 hover:text-red-600"><Trash2 className="h-4 w-4" /></button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {showModal && (
        <Modal title={editing ? 'Edit Country' : 'Add Country'} onClose={() => setShowModal(false)}>
          <form onSubmit={(e) => { e.preventDefault(); saveMutation.mutate(formData); }} className="space-y-4">
            {error && <div className="rounded bg-red-50 p-2 text-sm text-red-600">{error}</div>}
            <div className="grid gap-4 sm:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium">Country Code *</label>
                <input type="text" value={formData.country_code || ''} onChange={(e) => setFormData({ ...formData, country_code: e.target.value })} maxLength={3} placeholder="IN" className="w-full rounded-lg border px-3 py-2 text-sm" required />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Country Name *</label>
                <input type="text" value={formData.country_name || ''} onChange={(e) => setFormData({ ...formData, country_name: e.target.value })} placeholder="India" className="w-full rounded-lg border px-3 py-2 text-sm" required />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Phone Code</label>
                <input type="text" value={formData.phone_code || ''} onChange={(e) => setFormData({ ...formData, phone_code: e.target.value })} placeholder="+91" className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Currency Code</label>
                <input type="text" value={formData.currency_code || ''} onChange={(e) => setFormData({ ...formData, currency_code: e.target.value })} placeholder="INR" maxLength={3} className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
            </div>
            <div className="flex justify-end gap-2 pt-2">
              <button type="button" onClick={() => setShowModal(false)} className="rounded-lg border px-4 py-2 text-sm">Cancel</button>
              <button type="submit" disabled={saveMutation.isPending} className="rounded-lg bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">
                {saveMutation.isPending ? 'Saving...' : 'Save'}
              </button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  )
}

// ============ STATES TAB ============
function StatesTab() {
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [countryFilter, setCountryFilter] = useState('')
  const [showModal, setShowModal] = useState(false)
  const [editing, setEditing] = useState<any>(null)
  const [formData, setFormData] = useState<any>({})
  const [error, setError] = useState('')

  const { data: countriesData } = useQuery({
    queryKey: ['countries'],
    queryFn: async () => (await fetch('/api/masters/countries')).json(),
  })

  const { data, isLoading } = useQuery({
    queryKey: ['states', search, countryFilter],
    queryFn: async () => {
      const params = new URLSearchParams({ search, countryId: countryFilter })
      const res = await fetch(`/api/masters/states?${params}`)
      return res.json()
    },
  })

  const saveMutation = useMutation({
    mutationFn: async (data: any) => {
      const url = editing ? `/api/masters/states/${editing.id}` : '/api/masters/states'
      const res = await fetch(url, {
        method: editing ? 'PUT' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      })
      if (!res.ok) throw new Error((await res.json()).error)
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['states'] })
      setShowModal(false)
      setEditing(null)
      setFormData({})
    },
    onError: (e: Error) => setError(e.message),
  })

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      await fetch(`/api/masters/states/${id}`, { method: 'DELETE' })
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['states'] }),
  })

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
          <input type="text" placeholder="Search states..." value={search} onChange={(e) => setSearch(e.target.value)} className="h-9 w-full rounded-lg border bg-gray-50 pl-9 pr-3 text-sm focus:border-blue-500 focus:bg-white focus:outline-none" />
        </div>
        <select value={countryFilter} onChange={(e) => setCountryFilter(e.target.value)} className="h-9 rounded-lg border bg-gray-50 px-3 text-sm">
          <option value="">All Countries</option>
          {countriesData?.countries?.map((c: any) => <option key={c.id} value={c.id}>{c.country_name}</option>)}
        </select>
        <button onClick={() => { setEditing(null); setFormData({}); setError(''); setShowModal(true); }} className="flex h-9 items-center gap-1.5 rounded-lg bg-blue-600 px-4 text-sm font-medium text-white hover:bg-blue-700">
          <Plus className="h-4 w-4" /> Add State
        </button>
      </div>

      <div className="rounded-xl border bg-white">
        <table className="w-full text-sm">
          <thead className="border-b bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Code</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">State Name</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Country</th>
              <th className="px-4 py-3 text-right font-medium text-gray-600">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y">
            {isLoading ? (
              <tr><td colSpan={4} className="px-4 py-8 text-center text-gray-500">Loading...</td></tr>
            ) : data?.states?.length === 0 ? (
              <tr><td colSpan={4} className="px-4 py-8 text-center text-gray-500">No states found</td></tr>
            ) : (
              data?.states?.map((item: any) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 font-mono font-medium">{item.state_code}</td>
                  <td className="px-4 py-3 font-medium">{item.state_name}</td>
                  <td className="px-4 py-3 text-gray-600">{item.jc_countries?.country_name || '--'}</td>
                  <td className="px-4 py-3 text-right">
                    <button onClick={() => { setEditing(item); setFormData({ ...item, country_id: item.country_id }); setError(''); setShowModal(true); }} className="rounded p-1 text-gray-400 hover:bg-blue-50 hover:text-blue-600"><Edit2 className="h-4 w-4" /></button>
                    <button onClick={() => confirm('Delete this state?') && deleteMutation.mutate(item.id)} className="rounded p-1 text-gray-400 hover:bg-red-50 hover:text-red-600"><Trash2 className="h-4 w-4" /></button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {showModal && (
        <Modal title={editing ? 'Edit State' : 'Add State'} onClose={() => setShowModal(false)}>
          <form onSubmit={(e) => { e.preventDefault(); saveMutation.mutate(formData); }} className="space-y-4">
            {error && <div className="rounded bg-red-50 p-2 text-sm text-red-600">{error}</div>}
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="sm:col-span-2">
                <label className="mb-1 block text-xs font-medium">Country *</label>
                <select value={formData.country_id || ''} onChange={(e) => setFormData({ ...formData, country_id: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm" required>
                  <option value="">Select Country</option>
                  {countriesData?.countries?.map((c: any) => <option key={c.id} value={c.id}>{c.country_name}</option>)}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">State Code *</label>
                <input type="text" value={formData.state_code || ''} onChange={(e) => setFormData({ ...formData, state_code: e.target.value })} placeholder="TS" className="w-full rounded-lg border px-3 py-2 text-sm" required />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">State Name *</label>
                <input type="text" value={formData.state_name || ''} onChange={(e) => setFormData({ ...formData, state_name: e.target.value })} placeholder="Telangana" className="w-full rounded-lg border px-3 py-2 text-sm" required />
              </div>
            </div>
            <div className="flex justify-end gap-2 pt-2">
              <button type="button" onClick={() => setShowModal(false)} className="rounded-lg border px-4 py-2 text-sm">Cancel</button>
              <button type="submit" disabled={saveMutation.isPending} className="rounded-lg bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">{saveMutation.isPending ? 'Saving...' : 'Save'}</button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  )
}

// ============ CITIES TAB ============
function CitiesTab() {
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [stateFilter, setStateFilter] = useState('')
  const [page, setPage] = useState(1)
  const [showModal, setShowModal] = useState(false)
  const [editing, setEditing] = useState<any>(null)
  const [formData, setFormData] = useState<any>({})
  const [error, setError] = useState('')

  const { data: statesData } = useQuery({
    queryKey: ['states'],
    queryFn: async () => (await fetch('/api/masters/states')).json(),
  })

  const { data, isLoading } = useQuery({
    queryKey: ['cities', search, stateFilter, page],
    queryFn: async () => {
      const params = new URLSearchParams({ search, stateId: stateFilter, page: page.toString(), limit: '20' })
      return (await fetch(`/api/masters/cities?${params}`)).json()
    },
  })

  const saveMutation = useMutation({
    mutationFn: async (data: any) => {
      const url = editing ? `/api/masters/cities/${editing.id}` : '/api/masters/cities'
      const res = await fetch(url, { method: editing ? 'PUT' : 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) })
      if (!res.ok) throw new Error((await res.json()).error)
      return res.json()
    },
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['cities'] }); setShowModal(false); setEditing(null); setFormData({}); },
    onError: (e: Error) => setError(e.message),
  })

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => { await fetch(`/api/masters/cities/${id}`, { method: 'DELETE' }) },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['cities'] }),
  })

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
          <input type="text" placeholder="Search cities..." value={search} onChange={(e) => setSearch(e.target.value)} className="h-9 w-full rounded-lg border bg-gray-50 pl-9 pr-3 text-sm focus:border-blue-500 focus:bg-white focus:outline-none" />
        </div>
        <select value={stateFilter} onChange={(e) => setStateFilter(e.target.value)} className="h-9 rounded-lg border bg-gray-50 px-3 text-sm">
          <option value="">All States</option>
          {statesData?.states?.map((s: any) => <option key={s.id} value={s.id}>{s.state_name}</option>)}
        </select>
        <button onClick={() => { setEditing(null); setFormData({}); setError(''); setShowModal(true); }} className="flex h-9 items-center gap-1.5 rounded-lg bg-blue-600 px-4 text-sm font-medium text-white hover:bg-blue-700">
          <Plus className="h-4 w-4" /> Add City
        </button>
      </div>

      <div className="rounded-xl border bg-white">
        <table className="w-full text-sm">
          <thead className="border-b bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left font-medium text-gray-600">City Name</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Code</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">State</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Metro</th>
              <th className="px-4 py-3 text-right font-medium text-gray-600">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y">
            {isLoading ? (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-500">Loading...</td></tr>
            ) : data?.cities?.length === 0 ? (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-500">No cities found</td></tr>
            ) : (
              data?.cities?.map((item: any) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 font-medium">{item.city_name}</td>
                  <td className="px-4 py-3 font-mono text-gray-600">{item.city_code || '--'}</td>
                  <td className="px-4 py-3 text-gray-600">{item.jc_states?.state_name || '--'}</td>
                  <td className="px-4 py-3">{item.is_metro ? <span className="rounded bg-blue-100 px-2 py-0.5 text-xs text-blue-700">Metro</span> : '--'}</td>
                  <td className="px-4 py-3 text-right">
                    <button onClick={() => { setEditing(item); setFormData(item); setError(''); setShowModal(true); }} className="rounded p-1 text-gray-400 hover:bg-blue-50 hover:text-blue-600"><Edit2 className="h-4 w-4" /></button>
                    <button onClick={() => confirm('Delete this city?') && deleteMutation.mutate(item.id)} className="rounded p-1 text-gray-400 hover:bg-red-50 hover:text-red-600"><Trash2 className="h-4 w-4" /></button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
        {data?.pagination?.totalPages > 1 && (
          <div className="flex items-center justify-between border-t px-4 py-3">
            <span className="text-sm text-gray-500">Page {page} of {data.pagination.totalPages} ({data.pagination.total} total)</span>
            <div className="flex gap-2">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="rounded border px-3 py-1 text-sm disabled:opacity-50">Prev</button>
              <button onClick={() => setPage(p => p + 1)} disabled={page >= data.pagination.totalPages} className="rounded border px-3 py-1 text-sm disabled:opacity-50">Next</button>
            </div>
          </div>
        )}
      </div>

      {showModal && (
        <Modal title={editing ? 'Edit City' : 'Add City'} onClose={() => setShowModal(false)}>
          <form onSubmit={(e) => { e.preventDefault(); saveMutation.mutate(formData); }} className="space-y-4">
            {error && <div className="rounded bg-red-50 p-2 text-sm text-red-600">{error}</div>}
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="sm:col-span-2">
                <label className="mb-1 block text-xs font-medium">State *</label>
                <select value={formData.state_id || ''} onChange={(e) => setFormData({ ...formData, state_id: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm" required>
                  <option value="">Select State</option>
                  {statesData?.states?.map((s: any) => <option key={s.id} value={s.id}>{s.state_name}</option>)}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">City Name *</label>
                <input type="text" value={formData.city_name || ''} onChange={(e) => setFormData({ ...formData, city_name: e.target.value })} placeholder="Hyderabad" className="w-full rounded-lg border px-3 py-2 text-sm" required />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">City Code</label>
                <input type="text" value={formData.city_code || ''} onChange={(e) => setFormData({ ...formData, city_code: e.target.value })} placeholder="HYD" className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
              <div className="flex items-center gap-2">
                <input type="checkbox" id="is_metro" checked={formData.is_metro || false} onChange={(e) => setFormData({ ...formData, is_metro: e.target.checked })} className="rounded" />
                <label htmlFor="is_metro" className="text-sm">Is Metro City</label>
              </div>
            </div>
            <div className="flex justify-end gap-2 pt-2">
              <button type="button" onClick={() => setShowModal(false)} className="rounded-lg border px-4 py-2 text-sm">Cancel</button>
              <button type="submit" disabled={saveMutation.isPending} className="rounded-lg bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">{saveMutation.isPending ? 'Saving...' : 'Save'}</button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  )
}

// ============ COACHES TAB ============
function CoachesTab() {
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [status, setStatus] = useState('')
  const [page, setPage] = useState(1)
  const [showModal, setShowModal] = useState(false)
  const [editing, setEditing] = useState<any>(null)
  const [formData, setFormData] = useState<any>({})
  const [error, setError] = useState('')

  const COACH_TYPES = ['Regular', 'Pickup', 'Luxury', 'Mini', 'Multi-Axle']
  const BUS_TYPES = ['A/C Sleeper', 'Non-A/C Sleeper', 'A/C Seater', 'Non-A/C Seater', 'Semi-Sleeper']
  const STATUS_OPTIONS = ['available', 'in_service', 'maintenance', 'out_of_service', 'breakdown']
  const MAKES = ['Ashok Leyland', 'Volvo', 'TATA', 'Bharat Benz', 'Eicher', 'Mercedes', 'Scania']

  const { data, isLoading } = useQuery({
    queryKey: ['coaches', search, status, page],
    queryFn: async () => {
      const params = new URLSearchParams({ search, status, page: page.toString(), limit: '20' })
      return (await fetch(`/api/coaches?${params}`)).json()
    },
  })

  const saveMutation = useMutation({
    mutationFn: async (data: any) => {
      const url = editing ? `/api/coaches/${editing.id}` : '/api/coaches'
      const res = await fetch(url, { method: editing ? 'PUT' : 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) })
      if (!res.ok) throw new Error((await res.json()).error)
      return res.json()
    },
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['coaches'] }); setShowModal(false); setEditing(null); setFormData({}); },
    onError: (e: Error) => setError(e.message),
  })

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => { await fetch(`/api/coaches/${id}`, { method: 'DELETE' }) },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['coaches'] }),
  })

  const getStatusColor = (s: string) => {
    const colors: Record<string, string> = { available: 'bg-green-100 text-green-700', in_service: 'bg-blue-100 text-blue-700', maintenance: 'bg-yellow-100 text-yellow-700', out_of_service: 'bg-gray-100 text-gray-700', breakdown: 'bg-red-100 text-red-700' }
    return colors[s] || 'bg-gray-100 text-gray-700'
  }

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
          <input type="text" placeholder="Search coaches..." value={search} onChange={(e) => setSearch(e.target.value)} className="h-9 w-full rounded-lg border bg-gray-50 pl-9 pr-3 text-sm focus:border-blue-500 focus:bg-white focus:outline-none" />
        </div>
        <select value={status} onChange={(e) => setStatus(e.target.value)} className="h-9 rounded-lg border bg-gray-50 px-3 text-sm">
          <option value="">All Status</option>
          {STATUS_OPTIONS.map(s => <option key={s} value={s}>{s.replace('_', ' ')}</option>)}
        </select>
        <button onClick={() => { setEditing(null); setFormData({ coach_type: 'Regular', status: 'available', fuel_type: 'Diesel', total_seats: 40 }); setError(''); setShowModal(true); }} className="flex h-9 items-center gap-1.5 rounded-lg bg-blue-600 px-4 text-sm font-medium text-white hover:bg-blue-700">
          <Plus className="h-4 w-4" /> Add Coach
        </button>
      </div>

      {/* Stats */}
      <div className="mb-4 grid grid-cols-6 gap-3">
        <div className="rounded-lg border bg-white p-3"><p className="text-xs text-gray-500">Total</p><p className="text-xl font-bold">{data?.stats?.total || 0}</p></div>
        <div className="rounded-lg border bg-white p-3"><p className="text-xs text-gray-500">Available</p><p className="text-xl font-bold text-green-600">{data?.stats?.available || 0}</p></div>
        <div className="rounded-lg border bg-white p-3"><p className="text-xs text-gray-500">In Service</p><p className="text-xl font-bold text-blue-600">{data?.stats?.in_service || 0}</p></div>
        <div className="rounded-lg border bg-white p-3"><p className="text-xs text-gray-500">Maintenance</p><p className="text-xl font-bold text-yellow-600">{data?.stats?.maintenance || 0}</p></div>
        <div className="rounded-lg border bg-white p-3"><p className="text-xs text-gray-500">Out of Service</p><p className="text-xl font-bold text-gray-600">{data?.stats?.out_of_service || 0}</p></div>
        <div className="rounded-lg border bg-white p-3"><p className="text-xs text-gray-500">Breakdown</p><p className="text-xl font-bold text-red-600">{data?.stats?.breakdown || 0}</p></div>
      </div>

      <div className="rounded-xl border bg-white">
        <table className="w-full text-sm">
          <thead className="border-b bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Coach Number</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Type</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Make & Model</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">City</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Seats</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Status</th>
              <th className="px-4 py-3 text-right font-medium text-gray-600">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y">
            {isLoading ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-500">Loading...</td></tr>
            ) : data?.coaches?.length === 0 ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-500">No coaches found</td></tr>
            ) : (
              data?.coaches?.map((item: any) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3"><p className="font-semibold">{item.coach_number}</p>{item.coach_name && <p className="text-xs text-gray-500">{item.coach_name}</p>}</td>
                  <td className="px-4 py-3"><span className="rounded bg-purple-100 px-2 py-0.5 text-xs text-purple-700">{item.coach_type}</span>{item.bus_type && <p className="mt-1 text-xs text-gray-500">{item.bus_type}</p>}</td>
                  <td className="px-4 py-3"><p className="font-medium">{item.make || '--'}</p>{item.model && <p className="text-xs text-gray-500">{item.model}</p>}</td>
                  <td className="px-4 py-3 text-gray-600">{item.home_city || '--'}</td>
                  <td className="px-4 py-3 font-medium">{item.total_seats}</td>
                  <td className="px-4 py-3"><span className={`rounded-full px-2 py-0.5 text-xs font-medium ${getStatusColor(item.status)}`}>{item.status.replace('_', ' ')}</span></td>
                  <td className="px-4 py-3 text-right">
                    <button onClick={() => { setEditing(item); setFormData(item); setError(''); setShowModal(true); }} className="rounded p-1 text-gray-400 hover:bg-blue-50 hover:text-blue-600"><Edit2 className="h-4 w-4" /></button>
                    <button onClick={() => confirm('Delete this coach?') && deleteMutation.mutate(item.id)} className="rounded p-1 text-gray-400 hover:bg-red-50 hover:text-red-600"><Trash2 className="h-4 w-4" /></button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
        {data?.pagination?.totalPages > 1 && (
          <div className="flex items-center justify-between border-t px-4 py-3">
            <span className="text-sm text-gray-500">Page {page} of {data.pagination.totalPages}</span>
            <div className="flex gap-2">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="rounded border px-3 py-1 text-sm disabled:opacity-50">Prev</button>
              <button onClick={() => setPage(p => p + 1)} disabled={page >= data.pagination.totalPages} className="rounded border px-3 py-1 text-sm disabled:opacity-50">Next</button>
            </div>
          </div>
        )}
      </div>

      {showModal && (
        <Modal title={editing ? 'Edit Coach' : 'Add Coach'} onClose={() => setShowModal(false)} size="lg">
          <form onSubmit={(e) => { e.preventDefault(); saveMutation.mutate(formData); }} className="space-y-4">
            {error && <div className="rounded bg-red-50 p-2 text-sm text-red-600">{error}</div>}
            <div className="grid gap-4 sm:grid-cols-3">
              <div>
                <label className="mb-1 block text-xs font-medium">Coach Number *</label>
                <input type="text" value={formData.coach_number || ''} onChange={(e) => setFormData({ ...formData, coach_number: e.target.value })} placeholder="KA 01 AB 1234" className="w-full rounded-lg border px-3 py-2 text-sm" required />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Coach Name</label>
                <input type="text" value={formData.coach_name || ''} onChange={(e) => setFormData({ ...formData, coach_name: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Coach Type</label>
                <select value={formData.coach_type || 'Regular'} onChange={(e) => setFormData({ ...formData, coach_type: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm">
                  {COACH_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Bus Type</label>
                <select value={formData.bus_type || ''} onChange={(e) => setFormData({ ...formData, bus_type: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm">
                  <option value="">Select</option>
                  {BUS_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Make</label>
                <select value={formData.make || ''} onChange={(e) => setFormData({ ...formData, make: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm">
                  <option value="">Select</option>
                  {MAKES.map(m => <option key={m} value={m}>{m}</option>)}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Model</label>
                <input type="text" value={formData.model || ''} onChange={(e) => setFormData({ ...formData, model: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Home City</label>
                <input type="text" value={formData.home_city || ''} onChange={(e) => setFormData({ ...formData, home_city: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Total Seats</label>
                <input type="number" value={formData.total_seats || 40} onChange={(e) => setFormData({ ...formData, total_seats: parseInt(e.target.value) })} className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Status</label>
                <select value={formData.status || 'available'} onChange={(e) => setFormData({ ...formData, status: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm">
                  {STATUS_OPTIONS.map(s => <option key={s} value={s}>{s.replace('_', ' ')}</option>)}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Chassis Number</label>
                <input type="text" value={formData.chassis_number || ''} onChange={(e) => setFormData({ ...formData, chassis_number: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Engine Number</label>
                <input type="text" value={formData.engine_number || ''} onChange={(e) => setFormData({ ...formData, engine_number: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium">Coach Mobile</label>
                <input type="tel" value={formData.coach_mobile_number || ''} onChange={(e) => setFormData({ ...formData, coach_mobile_number: e.target.value })} className="w-full rounded-lg border px-3 py-2 text-sm" />
              </div>
            </div>
            <div className="flex justify-end gap-2 pt-2">
              <button type="button" onClick={() => setShowModal(false)} className="rounded-lg border px-4 py-2 text-sm">Cancel</button>
              <button type="submit" disabled={saveMutation.isPending} className="rounded-lg bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">{saveMutation.isPending ? 'Saving...' : 'Save'}</button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  )
}

// ============ ROUTES TAB ============
function RoutesTab() {
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [showModal, setShowModal] = useState(false)
  const [editing, setEditing] = useState<any>(null)
  const [formData, setFormData] = useState<any>({})
  const [error, setError] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['routes', search],
    queryFn: async () => (await fetch(`/api/routes?search=${search}`)).json(),
  })

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
          <input type="text" placeholder="Search routes..." value={search} onChange={(e) => setSearch(e.target.value)} className="h-9 w-full rounded-lg border bg-gray-50 pl-9 pr-3 text-sm focus:border-blue-500 focus:bg-white focus:outline-none" />
        </div>
        <span className="text-sm text-gray-500">Routes are synced from Bitla</span>
      </div>

      <div className="rounded-xl border bg-white">
        <table className="w-full text-sm">
          <thead className="border-b bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Route ID</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Service Number</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Route</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Distance</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Base Fare</th>
            </tr>
          </thead>
          <tbody className="divide-y">
            {isLoading ? (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-500">Loading...</td></tr>
            ) : data?.routes?.length === 0 ? (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-500">No routes found. Pull data from Bitla to sync routes.</td></tr>
            ) : (
              data?.routes?.map((item: any) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 font-mono text-blue-600">{item.bitla_route_id}</td>
                  <td className="px-4 py-3 font-medium">{item.service_number || '--'}</td>
                  <td className="px-4 py-3">
                    <p className="font-medium">{item.origin} → {item.destination}</p>
                    {item.route_name && <p className="text-xs text-gray-500">{item.route_name}</p>}
                  </td>
                  <td className="px-4 py-3 text-gray-600">{item.distance_km ? `${item.distance_km} km` : '--'}</td>
                  <td className="px-4 py-3 text-gray-600">{item.base_fare ? `₹${item.base_fare}` : '--'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ============ MODAL COMPONENT ============
function Modal({ title, children, onClose, size = 'md' }: { title: string; children: React.ReactNode; onClose: () => void; size?: 'sm' | 'md' | 'lg' }) {
  const sizeClass = size === 'lg' ? 'max-w-3xl' : size === 'sm' ? 'max-w-md' : 'max-w-xl'
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className={`w-full ${sizeClass} rounded-2xl bg-white`}>
        <div className="flex items-center justify-between border-b p-4">
          <h2 className="text-lg font-semibold">{title}</h2>
          <button onClick={onClose} className="rounded-lg p-1 text-gray-400 hover:bg-gray-100"><X className="h-5 w-5" /></button>
        </div>
        <div className="p-4">{children}</div>
      </div>
    </div>
  )
}

// ============ MAIN PAGE ============
export default function MastersPage() {
  const [activeTab, setActiveTab] = useState<TabType>('countries')

  const tabs = [
    { id: 'countries' as TabType, label: 'Countries', icon: Globe },
    { id: 'states' as TabType, label: 'States', icon: Map },
    { id: 'cities' as TabType, label: 'Cities', icon: Building2 },
    { id: 'coaches' as TabType, label: 'Coaches', icon: Bus },
    { id: 'routes' as TabType, label: 'Routes', icon: Route },
  ]

  return (
    <DashboardLayout>
      <Header title="Masters" subtitle="Manage master data for countries, states, cities, coaches and routes" />

      <div className="p-4 sm:p-6">
        {/* Tabs */}
        <div className="mb-6 flex gap-1 rounded-xl border bg-white p-1">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors ${
                activeTab === tab.id
                  ? 'bg-blue-600 text-white'
                  : 'text-gray-600 hover:bg-gray-100'
              }`}
            >
              <tab.icon className="h-4 w-4" />
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab Content */}
        {activeTab === 'countries' && <CountriesTab />}
        {activeTab === 'states' && <StatesTab />}
        {activeTab === 'cities' && <CitiesTab />}
        {activeTab === 'coaches' && <CoachesTab />}
        {activeTab === 'routes' && <RoutesTab />}
      </div>
    </DashboardLayout>
  )
}
