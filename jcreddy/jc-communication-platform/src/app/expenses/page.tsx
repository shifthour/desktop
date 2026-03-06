'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState, useMemo } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { formatCurrency } from '@/lib/utils'
import {
  Search, Plus, Edit2, Trash2, X, Receipt, Calculator,
  Calendar, TrendingUp, IndianRupee
} from 'lucide-react'

const FREQUENCY_OPTIONS = [
  { value: 'daily', label: 'Daily', days: 1 },
  { value: 'weekly', label: 'Weekly', days: 7 },
  { value: 'monthly', label: 'Monthly', days: 30.44 },
  { value: 'quarterly', label: 'Quarterly (3 months)', days: 91.31 },
  { value: 'half_yearly', label: 'Half Yearly (6 months)', days: 182.62 },
  { value: 'annually', label: 'Annually', days: 365 },
]

const CATEGORY_OPTIONS = [
  'Fuel', 'Maintenance', 'Salary', 'Insurance', 'Permit & Tax',
  'Toll', 'Office Rent', 'Utilities', 'Marketing', 'Software',
  'Communication', 'Miscellaneous', 'Other'
]

// Calculate daily cost based on frequency
function calculateDailyCost(amount: number, frequency: string): number {
  const freq = FREQUENCY_OPTIONS.find(f => f.value === frequency)
  return freq ? amount / freq.days : amount / 30.44
}

// Get frequency label
function getFrequencyLabel(value: string): string {
  return FREQUENCY_OPTIONS.find(f => f.value === value)?.label || value
}

export default function ExpensesPage() {
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [frequencyFilter, setFrequencyFilter] = useState('')
  const [categoryFilter, setCategoryFilter] = useState('')
  const [showModal, setShowModal] = useState(false)
  const [editing, setEditing] = useState<any>(null)
  const [formData, setFormData] = useState<any>({})
  const [error, setError] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['expenses', search, frequencyFilter, categoryFilter],
    queryFn: async () => {
      const params = new URLSearchParams({ search, frequency: frequencyFilter, category: categoryFilter })
      const res = await fetch(`/api/masters/expenses?${params}`)
      return res.json()
    },
  })

  const saveMutation = useMutation({
    mutationFn: async (data: any) => {
      const url = editing ? `/api/masters/expenses/${editing.id}` : '/api/masters/expenses'
      const res = await fetch(url, {
        method: editing ? 'PUT' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      })
      if (!res.ok) throw new Error((await res.json()).error)
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['expenses'] })
      setShowModal(false)
      setEditing(null)
      setFormData({})
      setError('')
    },
    onError: (e: Error) => setError(e.message),
  })

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/masters/expenses/${id}`, { method: 'DELETE' })
      if (!res.ok) throw new Error('Delete failed')
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['expenses'] }),
  })

  // Calculate preview daily cost when form changes
  const previewDailyCost = useMemo(() => {
    if (formData.amount && formData.frequency) {
      return calculateDailyCost(parseFloat(formData.amount), formData.frequency)
    }
    return 0
  }, [formData.amount, formData.frequency])

  const openAddModal = () => {
    setEditing(null)
    setFormData({ frequency: 'monthly' })
    setError('')
    setShowModal(true)
  }

  const openEditModal = (expense: any) => {
    setEditing(expense)
    setFormData({ ...expense })
    setError('')
    setShowModal(true)
  }

  const getFrequencyColor = (freq: string) => {
    const colors: Record<string, string> = {
      daily: 'bg-red-100 text-red-700',
      weekly: 'bg-orange-100 text-orange-700',
      monthly: 'bg-blue-100 text-blue-700',
      quarterly: 'bg-purple-100 text-purple-700',
      half_yearly: 'bg-indigo-100 text-indigo-700',
      annually: 'bg-green-100 text-green-700',
    }
    return colors[freq] || 'bg-gray-100 text-gray-700'
  }

  return (
    <DashboardLayout>
      <Header title="Expenses" subtitle="Manage recurring expenses with automatic daily cost calculation" />

      <div className="p-4 sm:p-6">
        {/* Summary Stats */}
        <div className="mb-6 grid gap-4 sm:grid-cols-4">
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 text-gray-500">
              <Receipt className="h-4 w-4" />
              <span className="text-sm">Total Expenses</span>
            </div>
            <p className="mt-1 text-2xl font-bold">{data?.stats?.total_expenses || 0}</p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 text-gray-500">
              <Calculator className="h-4 w-4" />
              <span className="text-sm">Daily Cost</span>
            </div>
            <p className="mt-1 text-2xl font-bold text-blue-600">
              {formatCurrency(data?.stats?.total_daily_cost || 0)}
            </p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 text-gray-500">
              <Calendar className="h-4 w-4" />
              <span className="text-sm">Monthly Cost</span>
            </div>
            <p className="mt-1 text-2xl font-bold text-purple-600">
              {formatCurrency(data?.stats?.total_monthly_cost || 0)}
            </p>
          </div>
          <div className="rounded-xl border bg-white p-4">
            <div className="flex items-center gap-2 text-gray-500">
              <TrendingUp className="h-4 w-4" />
              <span className="text-sm">Annual Cost</span>
            </div>
            <p className="mt-1 text-2xl font-bold text-green-600">
              {formatCurrency(data?.stats?.total_annual_cost || 0)}
            </p>
          </div>
        </div>

        {/* Filters */}
        <div className="mb-4 rounded-xl border bg-white p-3">
          <div className="flex flex-wrap items-center gap-3">
            <div className="relative flex-1 max-w-xs">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search expenses..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="h-9 w-full rounded-lg border bg-gray-50 pl-9 pr-3 text-sm focus:border-blue-500 focus:bg-white focus:outline-none"
              />
            </div>

            <select
              value={frequencyFilter}
              onChange={(e) => setFrequencyFilter(e.target.value)}
              className="h-9 rounded-lg border bg-gray-50 px-3 text-sm focus:border-blue-500 focus:outline-none"
            >
              <option value="">All Frequencies</option>
              {FREQUENCY_OPTIONS.map(f => (
                <option key={f.value} value={f.value}>{f.label}</option>
              ))}
            </select>

            <select
              value={categoryFilter}
              onChange={(e) => setCategoryFilter(e.target.value)}
              className="h-9 rounded-lg border bg-gray-50 px-3 text-sm focus:border-blue-500 focus:outline-none"
            >
              <option value="">All Categories</option>
              {CATEGORY_OPTIONS.map(c => (
                <option key={c} value={c}>{c}</option>
              ))}
            </select>

            <button
              onClick={openAddModal}
              className="ml-auto flex h-9 items-center gap-1.5 rounded-lg bg-blue-600 px-4 text-sm font-medium text-white hover:bg-blue-700"
            >
              <Plus className="h-4 w-4" /> Add Expense
            </button>
          </div>
        </div>

        {/* Expenses Table */}
        <div className="rounded-xl border bg-white">
          <table className="w-full text-sm">
            <thead className="border-b bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Expense Name</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Category</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Frequency</th>
                <th className="px-4 py-3 text-right font-medium text-gray-600">Amount</th>
                <th className="px-4 py-3 text-right font-medium text-gray-600">
                  <span className="flex items-center justify-end gap-1">
                    <Calculator className="h-3.5 w-3.5" />
                    Daily Cost
                  </span>
                </th>
                <th className="px-4 py-3 text-right font-medium text-gray-600">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {isLoading ? (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-gray-500">
                    Loading...
                  </td>
                </tr>
              ) : data?.expenses?.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-gray-500">
                    No expenses found. Add your first expense to get started.
                  </td>
                </tr>
              ) : (
                data?.expenses?.map((expense: any) => (
                  <tr key={expense.id} className="hover:bg-gray-50">
                    <td className="px-4 py-3">
                      <p className="font-medium text-gray-900">{expense.expense_name}</p>
                      {expense.description && (
                        <p className="text-xs text-gray-500 line-clamp-1">{expense.description}</p>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      {expense.category ? (
                        <span className="rounded bg-gray-100 px-2 py-0.5 text-xs text-gray-700">
                          {expense.category}
                        </span>
                      ) : (
                        <span className="text-gray-400">--</span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${getFrequencyColor(expense.frequency)}`}>
                        {getFrequencyLabel(expense.frequency)}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right font-medium">
                      {formatCurrency(expense.amount)}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <span className="font-semibold text-blue-600">
                        {formatCurrency(expense.daily_cost)}
                      </span>
                      <span className="text-xs text-gray-500">/day</span>
                    </td>
                    <td className="px-4 py-3 text-right">
                      <button
                        onClick={() => openEditModal(expense)}
                        className="rounded p-1.5 text-gray-400 hover:bg-blue-50 hover:text-blue-600"
                      >
                        <Edit2 className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => confirm('Delete this expense?') && deleteMutation.mutate(expense.id)}
                        className="rounded p-1.5 text-gray-400 hover:bg-red-50 hover:text-red-600"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
            {data?.expenses?.length > 0 && (
              <tfoot className="border-t bg-gray-50">
                <tr>
                  <td colSpan={3} className="px-4 py-3 text-right font-medium text-gray-600">
                    Total:
                  </td>
                  <td className="px-4 py-3 text-right font-bold">
                    {formatCurrency(data?.stats?.total_amount || 0)}
                  </td>
                  <td className="px-4 py-3 text-right font-bold text-blue-600">
                    {formatCurrency(data?.stats?.total_daily_cost || 0)}/day
                  </td>
                  <td></td>
                </tr>
              </tfoot>
            )}
          </table>
        </div>
      </div>

      {/* Add/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="w-full max-w-lg rounded-2xl bg-white">
            <div className="flex items-center justify-between border-b p-4">
              <h2 className="text-lg font-semibold">
                {editing ? 'Edit Expense' : 'Add New Expense'}
              </h2>
              <button
                onClick={() => setShowModal(false)}
                className="rounded-lg p-1 text-gray-400 hover:bg-gray-100"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <form
              onSubmit={(e) => {
                e.preventDefault()
                saveMutation.mutate(formData)
              }}
              className="p-4 space-y-4"
            >
              {error && (
                <div className="rounded-lg bg-red-50 p-3 text-sm text-red-600">
                  {error}
                </div>
              )}

              <div>
                <label className="mb-1 block text-sm font-medium text-gray-700">
                  Expense Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={formData.expense_name || ''}
                  onChange={(e) => setFormData({ ...formData, expense_name: e.target.value })}
                  placeholder="e.g., Office Rent, Driver Salary"
                  className="w-full rounded-lg border px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                  required
                />
              </div>

              <div>
                <label className="mb-1 block text-sm font-medium text-gray-700">
                  Description
                </label>
                <textarea
                  value={formData.description || ''}
                  onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                  placeholder="Optional description..."
                  rows={2}
                  className="w-full rounded-lg border px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                />
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <div>
                  <label className="mb-1 block text-sm font-medium text-gray-700">
                    Amount (₹) <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="number"
                    step="0.01"
                    min="0"
                    value={formData.amount || ''}
                    onChange={(e) => setFormData({ ...formData, amount: e.target.value })}
                    placeholder="0.00"
                    className="w-full rounded-lg border px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    required
                  />
                </div>

                <div>
                  <label className="mb-1 block text-sm font-medium text-gray-700">
                    Frequency <span className="text-red-500">*</span>
                  </label>
                  <select
                    value={formData.frequency || 'monthly'}
                    onChange={(e) => setFormData({ ...formData, frequency: e.target.value })}
                    className="w-full rounded-lg border px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                    required
                  >
                    {FREQUENCY_OPTIONS.map(f => (
                      <option key={f.value} value={f.value}>{f.label}</option>
                    ))}
                  </select>
                </div>
              </div>

              <div>
                <label className="mb-1 block text-sm font-medium text-gray-700">
                  Category
                </label>
                <select
                  value={formData.category || ''}
                  onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                  className="w-full rounded-lg border px-3 py-2 text-sm focus:border-blue-500 focus:outline-none"
                >
                  <option value="">Select Category</option>
                  {CATEGORY_OPTIONS.map(c => (
                    <option key={c} value={c}>{c}</option>
                  ))}
                </select>
              </div>

              {/* Daily Cost Preview */}
              {formData.amount && formData.frequency && (
                <div className="rounded-lg bg-blue-50 p-4">
                  <div className="flex items-center gap-2 text-blue-700">
                    <Calculator className="h-5 w-5" />
                    <span className="font-medium">Daily Cost Calculation</span>
                  </div>
                  <div className="mt-2 grid grid-cols-3 gap-4 text-center">
                    <div>
                      <p className="text-xs text-blue-600">Per Day</p>
                      <p className="text-lg font-bold text-blue-700">
                        {formatCurrency(previewDailyCost)}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs text-blue-600">Per Month</p>
                      <p className="text-lg font-bold text-blue-700">
                        {formatCurrency(previewDailyCost * 30.44)}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs text-blue-600">Per Year</p>
                      <p className="text-lg font-bold text-blue-700">
                        {formatCurrency(previewDailyCost * 365)}
                      </p>
                    </div>
                  </div>
                  <p className="mt-2 text-xs text-blue-600">
                    {formatCurrency(parseFloat(formData.amount))} {getFrequencyLabel(formData.frequency).toLowerCase()} = {formatCurrency(previewDailyCost)} per day
                  </p>
                </div>
              )}

              <div className="flex justify-end gap-3 pt-2">
                <button
                  type="button"
                  onClick={() => setShowModal(false)}
                  className="rounded-lg border px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={saveMutation.isPending}
                  className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
                >
                  {saveMutation.isPending ? 'Saving...' : editing ? 'Update Expense' : 'Add Expense'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </DashboardLayout>
  )
}
