'use client'

import { useState } from 'react'
import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { 
  Users, 
  BarChart3, 
  Calendar, 
  Settings, 
  LogOut,
  Menu,
  X,
  Home
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { toast } from 'sonner'

const navigation = [
  { name: 'Dashboard', href: '/dashboard', icon: Home },
  { name: 'Leads', href: '/leads', icon: Users },
  { name: 'Site Visits', href: '/visits', icon: Calendar },
  { name: 'Reports', href: '/reports', icon: BarChart3 },
  { name: 'Admin', href: '/admin', icon: Settings },
]

interface MainNavProps {
  user: {
    name: string
    email: string
    role: string
  }
}

export function MainNav({ user }: MainNavProps) {
  const pathname = usePathname()
  const router = useRouter()
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  const handleLogout = async () => {
    try {
      const response = await fetch('/api/auth/logout', {
        method: 'POST'
      })
      
      if (response.ok) {
        toast.success('Logged out successfully')
        router.push('/login')
        router.refresh()
      } else {
        toast.error('Failed to log out')
      }
    } catch (error) {
      toast.error('An error occurred')
    }
  }

  return (
    <nav className="bg-white dark:bg-gray-900 shadow-sm border-b">
      <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          <div className="flex">
            <div className="flex-shrink-0 flex items-center">
              <Link href="/leads" className="text-xl font-bold text-primary">
                CRM
              </Link>
            </div>
            <div className="hidden sm:ml-6 sm:flex sm:space-x-8">
              {navigation.map((item) => {
                const Icon = item.icon
                const isActive = pathname.startsWith(item.href)
                
                return (
                  <Link
                    key={item.name}
                    href={item.href}
                    className={cn(
                      'inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors',
                      isActive
                        ? 'border-primary text-primary'
                        : 'border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300'
                    )}
                  >
                    <Icon className="w-4 h-4 mr-2" />
                    {item.name}
                  </Link>
                )
              })}
            </div>
          </div>

          <div className="hidden sm:ml-6 sm:flex sm:items-center space-x-4">
            <div className="text-sm text-gray-700 dark:text-gray-300">
              <div className="font-medium">{user.name}</div>
              <div className="text-xs text-gray-500">{user.role}</div>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={handleLogout}
              className="flex items-center"
            >
              <LogOut className="w-4 h-4 mr-2" />
              Logout
            </Button>
          </div>

          {/* Mobile menu button */}
          <div className="sm:hidden flex items-center">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
            >
              {isMobileMenuOpen ? (
                <X className="w-6 h-6" />
              ) : (
                <Menu className="w-6 h-6" />
              )}
            </Button>
          </div>
        </div>
      </div>

      {/* Mobile menu */}
      {isMobileMenuOpen && (
        <div className="sm:hidden">
          <div className="pt-2 pb-3 space-y-1">
            {navigation.map((item) => {
              const Icon = item.icon
              const isActive = pathname.startsWith(item.href)
              
              return (
                <Link
                  key={item.name}
                  href={item.href}
                  className={cn(
                    'flex items-center px-3 py-2 text-base font-medium',
                    isActive
                      ? 'bg-primary/10 border-r-2 border-primary text-primary'
                      : 'text-gray-600 hover:bg-gray-50 dark:text-gray-300 dark:hover:bg-gray-800'
                  )}
                  onClick={() => setIsMobileMenuOpen(false)}
                >
                  <Icon className="w-5 h-5 mr-3" />
                  {item.name}
                </Link>
              )
            })}
          </div>
          <div className="pt-4 pb-3 border-t border-gray-200 dark:border-gray-700">
            <div className="flex items-center px-4">
              <div>
                <div className="text-base font-medium text-gray-800 dark:text-gray-200">
                  {user.name}
                </div>
                <div className="text-sm text-gray-500">{user.role}</div>
              </div>
            </div>
            <div className="mt-3 space-y-1">
              <Button
                variant="ghost"
                className="w-full justify-start px-4 py-2 text-base font-medium"
                onClick={handleLogout}
              >
                <LogOut className="w-5 h-5 mr-3" />
                Logout
              </Button>
            </div>
          </div>
        </div>
      )}
    </nav>
  )
}