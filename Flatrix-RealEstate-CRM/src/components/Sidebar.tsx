'use client'

import {
  LayoutDashboard,
  Users,
  Building2,
  Home,
  DollarSign,
  UserCheck,
  FileText,
  Settings,
  LogOut,
  TrendingUp,
  MapPin,
  Calendar,
  TrendingDown
} from 'lucide-react'
import { useRouter } from 'next/navigation'
import { useAuth } from '@/contexts/AuthContext'

const allNavigation = [
  { name: 'Dashboard', id: 'dashboard', icon: LayoutDashboard, roles: ['super_admin'] },
  { name: 'Users', id: 'users', icon: UserCheck, roles: ['super_admin'] },
  { name: 'Projects', id: 'properties', icon: Home, roles: ['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT'] },
  { name: 'Activities', id: 'activities', icon: Calendar, roles: ['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT'] },
  { name: 'Leads', id: 'leads', icon: Users, roles: ['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT'] },
  { name: 'Deals', id: 'deals', icon: TrendingUp, roles: ['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT'] },
  { name: 'Site Visits', id: 'site-visits', icon: MapPin, roles: ['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT'] },
  { name: 'Sales Funnel', id: 'funnel', icon: TrendingDown, roles: ['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT'] },
  { name: 'Commissions', id: 'commissions', icon: DollarSign, roles: ['super_admin'] },
  { name: 'Reports', id: 'reports', icon: FileText, roles: ['super_admin'] },
  { name: 'Settings', id: 'settings', icon: Settings, roles: ['super_admin', 'ADMIN'] },
]

interface SidebarProps {
  activeSection: string
}

export default function Sidebar({ activeSection }: SidebarProps) {
  const router = useRouter()
  const { user, logout } = useAuth()

  // Filter navigation based on user role
  const navigation = allNavigation.filter(item => 
    user && item.roles.includes(user.role)
  )

  const handleNavigation = (sectionId: string) => {
    if (sectionId === 'dashboard') {
      router.push('/dashboard')
    } else {
      router.push(`/${sectionId}`)
    }
  }

  return (
    <div className="flex h-screen w-64 flex-col bg-gray-900">
      <div className="flex h-16 items-center justify-center border-b border-gray-800">
        <div className="flex items-center space-x-2">
          <Building2 className="h-8 w-8 text-blue-500" />
          <span className="text-xl font-bold text-white">Flatrix CRM</span>
        </div>
      </div>

      <nav className="flex-1 space-y-1 px-2 py-4">
        {navigation.map((item) => {
          const isActive = activeSection === item.id
          return (
            <button
              key={item.name}
              onClick={() => handleNavigation(item.id)}
              className={`
                w-full group flex items-center px-2 py-2 text-sm font-medium rounded-md transition-colors
                ${isActive 
                  ? 'bg-gray-800 text-white' 
                  : 'text-gray-300 hover:bg-gray-700 hover:text-white'}
              `}
            >
              <item.icon className="mr-3 h-5 w-5 flex-shrink-0" />
              {item.name}
            </button>
          )
        })}
      </nav>

      <div className="border-t border-gray-800 p-4">
        <div className="flex items-center">
          <div className="flex-1">
            <p className="text-sm font-medium text-white">{user?.name || 'User'}</p>
            <p className="text-xs text-gray-400">{user?.email}</p>
            <p className="text-xs text-blue-400 capitalize">{user?.role.replace('_', ' ')}</p>
          </div>
          <button 
            onClick={() => {
              logout()
              router.push('/')
            }}
            className="ml-3 p-2 text-gray-400 hover:text-white transition-colors"
          >
            <LogOut className="h-5 w-5" />
          </button>
        </div>
      </div>
    </div>
  )
}