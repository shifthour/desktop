'use client'

import { usePathname } from 'next/navigation'
import Sidebar from '@/components/Sidebar'

interface DashboardLayoutProps {
  children: React.ReactNode
}

export default function DashboardLayout({ children }: DashboardLayoutProps) {
  const pathname = usePathname()
  
  const getPageTitle = () => {
    switch (pathname) {
      case '/dashboard':
        return 'Dashboard'
      case '/users':
        return 'Users'
      case '/leads':
        return 'Leads'
      case '/properties':
        return 'Properties'
      case '/partners':
        return 'Partners'
      case '/deals':
        return 'Deals'
      case '/site-visits':
        return 'Site Visits'
      case '/funnel':
        return 'Sales Funnel'
      case '/activities':
        return 'Activities'
      case '/commissions':
        return 'Commissions'
      case '/reports':
        return 'Reports'
      case '/settings':
        return 'Settings'
      default:
        return 'Dashboard'
    }
  }

  const getActiveSection = () => {
    const path = pathname.replace('/', '')
    return path || 'dashboard'
  }

  return (
    <div className="flex h-screen bg-gray-100">
      <Sidebar activeSection={getActiveSection()} />
      <main className="flex-1 overflow-y-auto">
        <div className="bg-white border-b border-gray-200 px-8 py-4">
          <nav className="flex" aria-label="Breadcrumb">
            <ol className="flex items-center space-x-4">
              <li>
                <div className="flex items-center">
                  <a href="#" className="text-gray-400 hover:text-gray-500">
                    Flatrix CRM
                  </a>
                </div>
              </li>
              <li>
                <div className="flex items-center">
                  <svg className="flex-shrink-0 h-5 w-5 text-gray-300" fill="currentColor" viewBox="0 0 20 20" aria-hidden="true">
                    <path d="m5.555 17.776 4-16 .894.448-4 16-.894-.448z" />
                  </svg>
                  <span className="ml-4 text-sm font-medium text-gray-900">{getPageTitle()}</span>
                </div>
              </li>
            </ol>
          </nav>
        </div>
        <div className="p-8">
          {children}
        </div>
      </main>
    </div>
  )
}