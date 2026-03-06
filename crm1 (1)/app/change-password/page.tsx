"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Lock, Eye, EyeOff, CheckCircle, AlertTriangle } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

export default function ChangePasswordPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [loading, setLoading] = useState(false)
  const [user, setUser] = useState<any>(null)
  const [passwords, setPasswords] = useState({
    currentPassword: "",
    newPassword: "",
    confirmPassword: ""
  })
  const [showPasswords, setShowPasswords] = useState({
    current: false,
    new: false,
    confirm: false
  })
  const [passwordStrength, setPasswordStrength] = useState({
    score: 0,
    feedback: ""
  })
  const [error, setError] = useState("")

  useEffect(() => {
    // Get user from localStorage
    const storedUser = localStorage.getItem('user')
    if (!storedUser) {
      router.push('/login')
      return
    }
    
    const userData = JSON.parse(storedUser)
    setUser(userData)

    // Pre-fill current password if it's the default one
    if (userData.password_changed === false) {
      // Check if user is admin or regular user to set appropriate default password
      const defaultPassword = userData.is_admin || userData.is_super_admin ? "Admin@123" : "User@123"
      setPasswords(prev => ({ ...prev, currentPassword: defaultPassword }))
    }
  }, [router])

  useEffect(() => {
    // Check password strength
    const checkPasswordStrength = (password: string) => {
      if (password.length < 8) {
        return { score: 1, feedback: "Password must be at least 8 characters long" }
      }
      
      let score = 0
      let feedback = "Weak password"
      
      if (password.length >= 8) score++
      if (/[A-Z]/.test(password)) score++
      if (/[a-z]/.test(password)) score++
      if (/\d/.test(password)) score++
      if (/[!@#$%^&*(),.?":{}|<>]/.test(password)) score++
      
      if (score >= 4) {
        feedback = "Strong password"
      } else if (score >= 3) {
        feedback = "Good password"
      } else if (score >= 2) {
        feedback = "Fair password"
      }
      
      return { score, feedback }
    }

    if (passwords.newPassword) {
      const strength = checkPasswordStrength(passwords.newPassword)
      setPasswordStrength(strength)
    } else {
      setPasswordStrength({ score: 0, feedback: "" })
    }
  }, [passwords.newPassword])

  const handlePasswordChange = async (e: React.FormEvent) => {
    e.preventDefault()
    setError("")
    setLoading(true)

    try {
      // Validate passwords
      if (!passwords.currentPassword || !passwords.newPassword || !passwords.confirmPassword) {
        setError("Please fill in all password fields")
        setLoading(false)
        return
      }

      if (passwords.newPassword !== passwords.confirmPassword) {
        setError("New passwords do not match")
        setLoading(false)
        return
      }

      if (passwords.newPassword.length < 8) {
        setError("New password must be at least 8 characters long")
        setLoading(false)
        return
      }

      if (passwordStrength.score < 3) {
        setError("Please choose a stronger password")
        setLoading(false)
        return
      }

      // Call API to change password
      const response = await fetch('/api/auth/change-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: user.id,
          currentPassword: passwords.currentPassword,
          newPassword: passwords.newPassword
        })
      })

      const data = await response.json()

      if (response.ok && data.success) {
        // Update user session to mark password as changed
        const updatedUser = { ...user, password_changed: true }
        localStorage.setItem('user', JSON.stringify(updatedUser))
        
        toast({
          title: "Password changed successfully",
          description: isFirstTimeLogin 
            ? "Your password has been updated. You will now be redirected to your dashboard."
            : "Your password has been changed successfully."
        })

        // For voluntary password changes, redirect back to where they came from
        // For first-time logins, redirect to appropriate dashboard
        if (isFirstTimeLogin) {
          setTimeout(() => {
            if (user.is_super_admin) {
              router.push('/admin')
            } else if (user.is_admin) {
              router.push('/company-admin')
            } else {
              router.push('/')
            }
          }, 2000)
        } else {
          // For voluntary password changes, just go back after a short delay
          setTimeout(() => {
            router.back()
          }, 1500)
        }
      } else {
        setError(data.error || "Failed to change password")
      }
    } catch (error) {
      console.error('Password change error:', error)
      setError("An error occurred. Please try again.")
    } finally {
      setLoading(false)
    }
  }

  const getPasswordStrengthColor = (score: number) => {
    if (score >= 4) return "text-green-600"
    if (score >= 3) return "text-yellow-600" 
    if (score >= 2) return "text-orange-600"
    return "text-red-600"
  }

  const getPasswordStrengthBg = (score: number) => {
    if (score >= 4) return "bg-green-100 border-green-300"
    if (score >= 3) return "bg-yellow-100 border-yellow-300"
    if (score >= 2) return "bg-orange-100 border-orange-300"
    return "bg-red-100 border-red-300"
  }

  if (!user) {
    return null // Will redirect to login
  }

  // Check if this is first-time login or voluntary password change
  const isFirstTimeLogin = user.password_changed === false
  const isVoluntaryChange = !isFirstTimeLogin

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 flex items-center justify-center p-4">
      <div className="w-full max-w-md space-y-6">
        {/* Header */}
        <div className="text-center">
          <div className="inline-flex items-center justify-center w-20 h-20 bg-gradient-to-r from-blue-600 to-purple-600 rounded-2xl mb-4">
            <Lock className="w-10 h-10 text-white" />
          </div>
          <h1 className="text-3xl font-bold text-gray-900">
            {isFirstTimeLogin ? "Set New Password" : "Change Password"}
          </h1>
          <p className="text-gray-600 mt-2">
            {isFirstTimeLogin 
              ? "Please set a new password for your account"
              : "Update your current password"
            }
          </p>
        </div>

        {/* First time login notice */}
        {isFirstTimeLogin && (
          <Alert className="bg-blue-50 border-blue-200">
            <AlertTriangle className="h-4 w-4 text-blue-600" />
            <AlertDescription className="text-blue-800">
              <strong>First time login detected.</strong> For security reasons, you must change your default password before accessing the system.
            </AlertDescription>
          </Alert>
        )}

        {/* Password Change Form */}
        <Card className="shadow-xl">
          <CardHeader>
            <CardTitle>
              {isFirstTimeLogin ? "Password Setup" : "Change Password"}
            </CardTitle>
            <CardDescription>
              {isFirstTimeLogin 
                ? "Create a secure password to protect your account"
                : "Enter your current password and choose a new one"
              }
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={handlePasswordChange} className="space-y-4">
              {/* Current Password */}
              <div className="space-y-2">
                <Label htmlFor="currentPassword">
                  {isFirstTimeLogin ? "Current Password (provided to you)" : "Current Password"}
                </Label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                  <Input
                    id="currentPassword"
                    type={showPasswords.current ? "text" : "password"}
                    value={passwords.currentPassword}
                    onChange={(e) => setPasswords({...passwords, currentPassword: e.target.value})}
                    placeholder={isFirstTimeLogin ? (user.is_admin || user.is_super_admin ? "Admin@123" : "User@123") : "Enter current password"}
                    className="pl-10 pr-10"
                    disabled={loading}
                    readOnly={isFirstTimeLogin}
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => setShowPasswords({...showPasswords, current: !showPasswords.current})}
                    className="absolute right-2 top-1/2 transform -translate-y-1/2 p-1 h-auto"
                  >
                    {showPasswords.current ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                  </Button>
                </div>
                {isFirstTimeLogin ? (
                  <p className="text-xs text-gray-500">
                    Your current password is: <strong className="text-blue-600">{user.is_admin || user.is_super_admin ? "Admin@123" : "User@123"}</strong>
                  </p>
                ) : (
                  <p className="text-xs text-gray-500">
                    Enter your current password to verify your identity
                  </p>
                )}
              </div>

              {/* New Password */}
              <div className="space-y-2">
                <Label htmlFor="newPassword">New Password</Label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                  <Input
                    id="newPassword"
                    type={showPasswords.new ? "text" : "password"}
                    value={passwords.newPassword}
                    onChange={(e) => setPasswords({...passwords, newPassword: e.target.value})}
                    placeholder="Enter new password"
                    className="pl-10 pr-10"
                    disabled={loading}
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => setShowPasswords({...showPasswords, new: !showPasswords.new})}
                    className="absolute right-2 top-1/2 transform -translate-y-1/2 p-1 h-auto"
                  >
                    {showPasswords.new ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                  </Button>
                </div>
                
                {/* Password strength indicator */}
                {passwords.newPassword && (
                  <div className={`text-xs p-2 rounded border ${getPasswordStrengthBg(passwordStrength.score)}`}>
                    <span className={`font-medium ${getPasswordStrengthColor(passwordStrength.score)}`}>
                      {passwordStrength.feedback}
                    </span>
                  </div>
                )}
              </div>

              {/* Confirm Password */}
              <div className="space-y-2">
                <Label htmlFor="confirmPassword">Confirm New Password</Label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                  <Input
                    id="confirmPassword"
                    type={showPasswords.confirm ? "text" : "password"}
                    value={passwords.confirmPassword}
                    onChange={(e) => setPasswords({...passwords, confirmPassword: e.target.value})}
                    placeholder="Confirm new password"
                    className="pl-10 pr-10"
                    disabled={loading}
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => setShowPasswords({...showPasswords, confirm: !showPasswords.confirm})}
                    className="absolute right-2 top-1/2 transform -translate-y-1/2 p-1 h-auto"
                  >
                    {showPasswords.confirm ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                  </Button>
                </div>
                
                {/* Password match indicator */}
                {passwords.confirmPassword && (
                  <div className="flex items-center space-x-2 text-xs">
                    {passwords.newPassword === passwords.confirmPassword ? (
                      <>
                        <CheckCircle className="w-3 h-3 text-green-600" />
                        <span className="text-green-600">Passwords match</span>
                      </>
                    ) : (
                      <>
                        <AlertTriangle className="w-3 h-3 text-red-600" />
                        <span className="text-red-600">Passwords do not match</span>
                      </>
                    )}
                  </div>
                )}
              </div>

              {/* Password requirements */}
              <div className="text-xs text-gray-600 bg-gray-50 p-3 rounded">
                <p className="font-medium mb-1">Password requirements:</p>
                <ul className="space-y-1">
                  <li>• At least 8 characters long</li>
                  <li>• Mix of uppercase and lowercase letters</li>
                  <li>• At least one number</li>
                  <li>• At least one special character (!@#$%^&*)</li>
                </ul>
              </div>

              {error && (
                <Alert variant="destructive">
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}

              <div className="flex space-x-3">
                {isVoluntaryChange && (
                  <Button
                    type="button"
                    variant="outline"
                    className="flex-1"
                    onClick={() => router.back()}
                    disabled={loading}
                  >
                    Cancel
                  </Button>
                )}
                <Button
                  type="submit"
                  className={`${isVoluntaryChange ? 'flex-1' : 'w-full'} bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700`}
                  disabled={loading || passwordStrength.score < 3 || passwords.newPassword !== passwords.confirmPassword}
                >
                  {loading ? "Updating Password..." : "Update Password"}
                </Button>
              </div>
            </form>
          </CardContent>
        </Card>

        <div className="text-center text-sm text-gray-500">
          <p>Logged in as: <strong>{user.full_name}</strong> ({user.email})</p>
        </div>
      </div>
    </div>
  )
}