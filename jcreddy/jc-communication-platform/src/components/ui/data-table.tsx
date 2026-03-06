'use client'

import { cn } from '@/lib/utils'
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from 'lucide-react'

interface Column<T> {
  key: string
  header: string | React.ReactNode
  render?: (item: T) => React.ReactNode
  className?: string
}

interface DataTableProps<T> {
  data: T[]
  columns: Column<T>[]
  loading?: boolean
  emptyMessage?: string
  pagination?: {
    page: number
    totalPages: number
    total: number
    onPageChange: (page: number) => void
  }
}

export function DataTable<T extends Record<string, any>>({
  data,
  columns,
  loading,
  emptyMessage = 'No data available',
  pagination,
}: DataTableProps<T>) {
  if (loading) {
    return (
      <div className="rounded-2xl border bg-white">
        <div className="p-8">
          <div className="flex items-center justify-center">
            <div className="h-8 w-8 animate-spin rounded-full border-4 border-blue-600 border-t-transparent" />
          </div>
        </div>
      </div>
    )
  }

  if (!data.length) {
    return (
      <div className="rounded-2xl border bg-white">
        <div className="flex flex-col items-center justify-center py-12">
          <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-gray-100">
            <svg
              className="h-8 w-8 text-gray-400"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={1.5}
                d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"
              />
            </svg>
          </div>
          <p className="text-gray-500">{emptyMessage}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="overflow-hidden rounded-2xl border bg-white">
      <div className="overflow-x-auto">
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.key} className={column.className}>
                  {column.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((item, index) => (
              <tr key={index} className="transition-colors">
                {columns.map((column) => (
                  <td key={column.key} className={column.className}>
                    {column.render
                      ? column.render(item)
                      : item[column.key]}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {pagination && pagination.totalPages > 1 && (
        <div className="flex items-center justify-between border-t px-4 py-3">
          <p className="text-sm text-gray-500">
            Showing page {pagination.page} of {pagination.totalPages} ({pagination.total} total)
          </p>
          <div className="flex items-center gap-1">
            <button
              onClick={() => pagination.onPageChange(1)}
              disabled={pagination.page === 1}
              className="flex h-8 w-8 items-center justify-center rounded-lg border transition-colors hover:bg-gray-50 disabled:opacity-50"
            >
              <ChevronsLeft className="h-4 w-4" />
            </button>
            <button
              onClick={() => pagination.onPageChange(pagination.page - 1)}
              disabled={pagination.page === 1}
              className="flex h-8 w-8 items-center justify-center rounded-lg border transition-colors hover:bg-gray-50 disabled:opacity-50"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            {/* Page numbers */}
            {Array.from({ length: Math.min(5, pagination.totalPages) }, (_, i) => {
              const pageNum = Math.max(1, Math.min(pagination.page - 2, pagination.totalPages - 4)) + i
              if (pageNum > pagination.totalPages) return null
              return (
                <button
                  key={pageNum}
                  onClick={() => pagination.onPageChange(pageNum)}
                  className={cn(
                    'flex h-8 w-8 items-center justify-center rounded-lg text-sm font-medium transition-colors',
                    pageNum === pagination.page
                      ? 'bg-blue-600 text-white'
                      : 'border hover:bg-gray-50'
                  )}
                >
                  {pageNum}
                </button>
              )
            })}
            <button
              onClick={() => pagination.onPageChange(pagination.page + 1)}
              disabled={pagination.page === pagination.totalPages}
              className="flex h-8 w-8 items-center justify-center rounded-lg border transition-colors hover:bg-gray-50 disabled:opacity-50"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
            <button
              onClick={() => pagination.onPageChange(pagination.totalPages)}
              disabled={pagination.page === pagination.totalPages}
              className="flex h-8 w-8 items-center justify-center rounded-lg border transition-colors hover:bg-gray-50 disabled:opacity-50"
            >
              <ChevronsRight className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
