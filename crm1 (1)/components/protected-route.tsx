"use client"

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { Card, CardContent } from '@/components/ui/card'
import { Shield, Loader2 } from 'lucide-react'

interface ProtectedRouteProps {
  children: React.ReactNode
  requireSuperAdmin?: boolean
  requireAdmin?: boolean
}

export function ProtectedRoute({ 
  children, 
  requireSuperAdmin = false,
  requireAdmin = false 
}: ProtectedRouteProps) {
  const router = useRouter()

  useEffect(() => {
    const checkAuth = () => {
      const storedUser = localStorage.getItem('user')
      
      if (!storedUser) {
        router.push('/login')
        return
      }

      try {
        const user = JSON.parse(storedUser)
        
        // Check super admin requirement
        if (requireSuperAdmin && !user.is_super_admin) {
          router.push('/unauthorized')
          return
        }

        // Check admin requirement (allow both super admins and company admins)
        if (requireAdmin && !user.is_admin && !user.is_super_admin) {
          router.push('/unauthorized')
          return
        }

        // Check if user is active
        if (!user.is_active) {
          router.push('/account-disabled')
          return
        }
      } catch (error) {
        console.error('Invalid user data')
        localStorage.removeItem('user')
        router.push('/login')
      }
    }

    checkAuth()
  }, [router, requireSuperAdmin, requireAdmin])

  // Check if we have a valid user
  const storedUser = typeof window !== 'undefined' ? localStorage.getItem('user') : null
  if (!storedUser) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <Card className="w-96">
          <CardContent className="p-6">
            <div className="flex flex-col items-center space-y-4">
              <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
              <p className="text-gray-600">Checking authentication...</p>
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  return <>{children}</>
}