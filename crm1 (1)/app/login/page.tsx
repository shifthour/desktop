"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Lock, Mail, Loader2, Users, Building2, Settings } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

export default function UserLoginPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [loading, setLoading] = useState(false)
  const [credentials, setCredentials] = useState({
    email: "",
    password: ""
  })
  const [error, setError] = useState("")

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setError("")
    setLoading(true)

    try {
      // Validate credentials
      if (!credentials.email || !credentials.password) {
        setError("Please enter both email and password")
        setLoading(false)
        return
      }

      // Check credentials for company users
      const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(credentials)
      })

      const data = await response.json()

      if (response.ok && data.success) {
        // Store user session
        localStorage.setItem('user', JSON.stringify(data.user))
        
        // Check if password change is required (first time login)
        if (data.requirePasswordChange) {
          toast({
            title: "Password change required",
            description: "You must change your default password before accessing the system."
          })
          router.push('/change-password')
          return
        }
        
        toast({
          title: "Login successful",
          description: `Welcome back, ${data.user.full_name}!`
        })

        // Redirect based on user role and admin status
        if (data.user.is_super_admin) {
          // Super admins go to the main admin dashboard
          router.push('/admin')
        } else if (data.user.is_admin) {
          // Company admins go to the company admin dashboard with Company Settings
          router.push('/company-admin')
        } else {
          // Regular company users (sales manager, field engineer, etc.) go to main dashboard
          router.push('/')
        }
      } else {
        setError(data.error || "Invalid credentials")
      }
    } catch (error) {
      console.error('Login error:', error)
      setError("An error occurred. Please try again.")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-green-50 via-blue-50 to-indigo-50 flex items-center justify-center p-4">
      <div className="w-full max-w-md space-y-6">
        {/* Logo/Header */}
        <div className="text-center">
          <div className="inline-flex items-center justify-center w-20 h-20 bg-gradient-to-r from-green-600 to-blue-600 rounded-2xl mb-4">
            <Building2 className="w-10 h-10 text-white" />
          </div>
          <h1 className="text-3xl font-bold text-gray-900">LabGig CRM</h1>
          <p className="text-gray-600 mt-2">Company User Access</p>
        </div>

        {/* Login Card */}
        <Card className="shadow-xl">
          <CardHeader>
            <CardTitle className="text-2xl">Welcome to your CRM</CardTitle>
            <CardDescription>Sign in to access your company dashboard</CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleLogin} className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="email">Company Email Address</Label>
                <div className="relative">
                  <Mail className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                  <Input
                    id="email"
                    type="email"
                    value={credentials.email}
                    onChange={(e) => setCredentials({...credentials, email: e.target.value})}
                    placeholder="your.name@company.com"
                    className="pl-10"
                    disabled={loading}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                  <Input
                    id="password"
                    type="password"
                    value={credentials.password}
                    onChange={(e) => setCredentials({...credentials, password: e.target.value})}
                    placeholder="Enter your password"
                    className="pl-10"
                    disabled={loading}
                  />
                </div>
              </div>

              {error && (
                <Alert variant="destructive">
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}

              <Button
                type="submit"
                className="w-full bg-gradient-to-r from-green-600 to-blue-600 hover:from-green-700 hover:to-blue-700"
                disabled={loading}
              >
                {loading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Signing in...
                  </>
                ) : (
                  "Sign in to CRM"
                )}
              </Button>
            </form>
          </CardContent>
        </Card>

        <div className="text-center text-sm text-gray-500">
          © 2025 LabGig CRM. Built for Instrumental Companies.
        </div>
      </div>
    </div>
  )
}