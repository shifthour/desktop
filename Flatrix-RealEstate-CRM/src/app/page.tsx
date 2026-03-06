'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import toast from 'react-hot-toast'
import { Building2, Users, TrendingUp, FileText, BarChart3, Shield } from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'

export default function LoginPage() {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const router = useRouter()
  const { login } = useAuth()

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)
    
    try {
      const success = await login(email, password)
      
      if (success) {
        // Get user info from auth context to determine redirect
        const storedUser = localStorage.getItem('flatrix_user')
        if (storedUser) {
          const user = JSON.parse(storedUser)
          toast.success('Login successful!')
          
          // Redirect based on role
          if (user.role === 'super_admin') {
            router.push('/dashboard')
          } else {
            router.push('/leads') // Default page for admins
          }
        }
      } else {
        toast.error('Invalid email or password')
      }
    } catch (error) {
      console.error('Login error:', error)
      toast.error('Login failed: ' + String(error))
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex">
      <div className="flex-1 flex items-center justify-center p-8">
        <div className="max-w-md w-full">
          <div className="bg-white rounded-2xl shadow-xl p-8">
            <div className="text-center mb-8">
              <div className="inline-flex items-center justify-center w-16 h-16 bg-blue-600 rounded-full mb-4">
                <Building2 className="w-8 h-8 text-white" />
              </div>
              <h1 className="text-2xl font-bold text-gray-900">Flatrix CRM</h1>
              <p className="text-gray-600 mt-2">Real Estate Channel Partner Management</p>
            </div>

            <form onSubmit={handleLogin} className="space-y-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Email Address
                </label>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition"
                  placeholder="admin@flatrix.com"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Password
                </label>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition"
                  placeholder="Enter your password"
                  required
                />
              </div>

              <button
                type="submit"
                disabled={isLoading}
                className="w-full bg-blue-600 text-white py-3 rounded-lg font-medium hover:bg-blue-700 transition disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isLoading ? 'Signing in...' : 'Sign In'}
              </button>
            </form>

          </div>
        </div>
      </div>

      <div className="hidden lg:flex flex-1 bg-gradient-to-br from-blue-600 to-indigo-700 p-12 items-center justify-center">
        <div className="max-w-lg text-white">
          <h2 className="text-4xl font-bold mb-6">
            Streamline Your Real Estate Business
          </h2>
          <p className="text-blue-100 mb-8 text-lg">
            Complete CRM solution designed specifically for real estate channel partners. 
            Manage leads, properties, commissions, and more in one place.
          </p>

          <div className="grid grid-cols-2 gap-6">
            <div className="flex items-center space-x-3">
              <Users className="w-8 h-8 text-blue-200" />
              <div>
                <p className="font-semibold">Lead Management</p>
                <p className="text-sm text-blue-200">Track & convert leads</p>
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <TrendingUp className="w-8 h-8 text-blue-200" />
              <div>
                <p className="font-semibold">Sales Pipeline</p>
                <p className="text-sm text-blue-200">Monitor deal progress</p>
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <FileText className="w-8 h-8 text-blue-200" />
              <div>
                <p className="font-semibold">Commission Tracking</p>
                <p className="text-sm text-blue-200">Automated calculations</p>
              </div>
            </div>

            <div className="flex items-center space-x-3">
              <BarChart3 className="w-8 h-8 text-blue-200" />
              <div>
                <p className="font-semibold">Analytics</p>
                <p className="text-sm text-blue-200">Real-time insights</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}