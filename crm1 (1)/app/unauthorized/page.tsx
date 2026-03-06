"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Shield, ArrowLeft, Home } from "lucide-react"

export default function UnauthorizedPage() {
  const router = useRouter()
  const [user, setUser] = useState<any>(null)

  useEffect(() => {
    const storedUser = localStorage.getItem('user')
    if (storedUser) {
      setUser(JSON.parse(storedUser))
    } else {
      router.push('/login')
    }
  }, [router])

  const handleGoHome = () => {
    if (user?.is_super_admin || user?.is_admin) {
      router.push('/admin')
    } else {
      router.push('/')
    }
  }

  const handleGoBack = () => {
    router.back()
  }

  if (!user) {
    return null // Will redirect to login
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-red-50 via-orange-50 to-yellow-50 flex items-center justify-center p-4">
      <div className="w-full max-w-md space-y-6">
        {/* Header */}
        <div className="text-center">
          <div className="inline-flex items-center justify-center w-20 h-20 bg-gradient-to-r from-red-600 to-orange-600 rounded-2xl mb-4">
            <Shield className="w-10 h-10 text-white" />
          </div>
          <h1 className="text-3xl font-bold text-gray-900">Access Denied</h1>
          <p className="text-gray-600 mt-2">You don't have permission to access this page</p>
        </div>

        {/* Error Card */}
        <Card className="shadow-xl border-red-200">
          <CardHeader>
            <CardTitle className="text-red-700">Unauthorized Access</CardTitle>
            <CardDescription>
              This page is restricted to administrators only.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* User Info */}
            <Alert className="bg-orange-50 border-orange-200">
              <AlertDescription>
                <div className="space-y-1">
                  <p><strong>Current User:</strong> {user.full_name}</p>
                  <p><strong>Email:</strong> {user.email}</p>
                  <p><strong>Role:</strong> {
                    user.is_super_admin ? 'Super Admin' :
                    user.is_admin ? 'Company Admin' :
                    user.role?.name ? user.role.name.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase()) :
                    'User'
                  }</p>
                  <p><strong>Company:</strong> {user.company?.name || 'N/A'}</p>
                </div>
              </AlertDescription>
            </Alert>

            {/* Access Requirements */}
            <div className="bg-gray-50 p-4 rounded-lg">
              <h4 className="font-medium text-gray-900 mb-2">Admin Page Access Requirements:</h4>
              <ul className="text-sm text-gray-600 space-y-1">
                <li>• Super Admin privileges, OR</li>
                <li>• Company Admin privileges</li>
              </ul>
            </div>

            {/* Action Buttons */}
            <div className="flex space-x-2">
              <Button
                onClick={handleGoBack}
                variant="outline"
                className="flex-1"
              >
                <ArrowLeft className="w-4 h-4 mr-2" />
                Go Back
              </Button>
              <Button
                onClick={handleGoHome}
                className="flex-1 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700"
              >
                <Home className="w-4 h-4 mr-2" />
                Dashboard
              </Button>
            </div>

            {/* Help Text */}
            <div className="text-center text-sm text-gray-500 pt-4 border-t">
              <p>Need admin access? Contact your system administrator.</p>
            </div>
          </CardContent>
        </Card>

        <div className="text-center text-sm text-gray-500">
          © 2025 LabGig CRM. Built for Instrumental Companies.
        </div>
      </div>
    </div>
  )
}