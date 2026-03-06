'use client'

import { useState } from 'react'
import Image from 'next/image'
import { Sidebar } from '@/components/ui/sidebar'
import { Menu } from 'lucide-react'

interface DashboardLayoutProps {
  children: React.ReactNode
}

export function DashboardLayout({ children }: DashboardLayoutProps) {
  const [sidebarOpen, setSidebarOpen] = useState(false)

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Mobile header with hamburger */}
      <div className="sticky top-0 z-30 flex h-14 items-center gap-4 border-b bg-white px-4 lg:hidden">
        <button
          onClick={() => setSidebarOpen(true)}
          className="flex h-10 w-10 items-center justify-center rounded-xl border border-gray-200 text-gray-600 hover:bg-gray-50"
        >
          <Menu className="h-5 w-5" />
        </button>
        <div className="flex items-center gap-2">
          <Image
            src="/suraksha-ride-logo.jpeg"
            alt="SurakshaRide Logo"
            width={32}
            height={32}
            className="rounded-lg"
          />
          <span className="font-semibold text-gray-900">SurakshaRide</span>
        </div>
      </div>

      <Sidebar isOpen={sidebarOpen} onClose={() => setSidebarOpen(false)} />

      <main className="min-h-screen lg:ml-64">
        {children}
      </main>
    </div>
  )
}
