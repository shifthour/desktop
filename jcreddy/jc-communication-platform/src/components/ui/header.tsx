'use client'

import { Search, RefreshCw } from 'lucide-react'
import { useState } from 'react'

interface HeaderProps {
  title: string
  subtitle?: string
  onSync?: () => void
  syncing?: boolean
}

export function Header({ title, subtitle, onSync, syncing }: HeaderProps) {
  const [searchQuery, setSearchQuery] = useState('')

  return (
    <header className="sticky top-0 z-20 flex flex-col gap-4 border-b bg-white/80 px-4 py-3 backdrop-blur-xl sm:flex-row sm:h-16 sm:items-center sm:justify-between sm:px-6 sm:py-0 lg:top-0">
      <div className="min-w-0">
        <h1 className="text-lg font-semibold text-gray-900 sm:text-xl truncate">{title}</h1>
        {subtitle && (
          <p className="text-xs text-gray-500 sm:text-sm truncate">{subtitle}</p>
        )}
      </div>

      <div className="flex items-center gap-2 sm:gap-4">
        {/* Search - Hidden on small mobile, visible on larger screens */}
        <div className="relative flex-1 sm:flex-none">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="h-9 w-full rounded-xl border border-gray-200 bg-gray-50 pl-10 pr-4 text-sm transition-all focus:border-blue-500 focus:bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 sm:h-10 sm:w-48 md:w-64"
          />
        </div>

        {/* Sync Button */}
        {onSync && (
          <button
            onClick={onSync}
            disabled={syncing}
            className="flex h-9 items-center gap-2 rounded-xl bg-blue-600 px-3 text-sm font-medium text-white transition-all hover:bg-blue-700 disabled:opacity-50 sm:h-10 sm:px-4"
          >
            <RefreshCw className={`h-4 w-4 ${syncing ? 'animate-spin' : ''}`} />
            <span className="hidden sm:inline">{syncing ? 'Syncing...' : 'Sync'}</span>
          </button>
        )}
      </div>
    </header>
  )
}
