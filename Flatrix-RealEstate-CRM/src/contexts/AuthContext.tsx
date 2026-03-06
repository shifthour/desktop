'use client'

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { supabase } from '@/lib/supabase'
import { verifyPassword } from '@/lib/auth'

interface User {
  id: string
  email: string
  role: 'super_admin' | 'ADMIN' | 'SALES_MANAGER' | 'AGENT'
  name: string
}

interface AuthContextType {
  user: User | null
  login: (email: string, password: string) => Promise<boolean>
  logout: () => void
  isLoading: boolean
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export const useAuth = () => {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}

interface AuthProviderProps {
  children: ReactNode
}

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    // Check for stored user session
    const storedUser = localStorage.getItem('flatrix_user')
    if (storedUser) {
      try {
        setUser(JSON.parse(storedUser))
      } catch (error) {
        console.error('Error parsing stored user:', error)
        localStorage.removeItem('flatrix_user')
      }
    }
    setIsLoading(false)
  }, [])

  const login = async (email: string, password: string): Promise<boolean> => {
    try {
      console.log('[LOGIN] Attempt for:', email)

      const { data, error } = await supabase
        .from('flatrix_users')
        .select('*')
        .eq('email', email)
        .single()

      console.log('[LOGIN] Database query result:', {
        found: !!data,
        error: error?.message,
        hasPassword: !!data?.password,
        passwordLength: data?.password?.length || 0,
        isBcryptHash: (data?.password?.startsWith('$2a$') || data?.password?.startsWith('$2b$')) || false
      })

      if (error || !data) {
        console.error('[LOGIN] User not found:', error)
        return false
      }

      console.log('[LOGIN] User found:', {
        email: data.email,
        role: data.role,
        hasPassword: !!data.password,
        passwordFormat: data.password?.substring(0, 10)
      })

      // Check if password exists
      if (!data.password) {
        console.error('[LOGIN] No password set for user')
        return false
      }

      // Check if password is hashed
      const isBcryptHash = data.password.startsWith('$2a$') || data.password.startsWith('$2b$')
      console.log('[LOGIN] Is password hashed?', isBcryptHash)

      let isValidPassword = false

      if (isBcryptHash) {
        // Password is hashed, use bcrypt compare
        console.log('[LOGIN] Verifying with bcrypt...')
        isValidPassword = await verifyPassword(password, data.password)
      } else {
        // Password might be plain text (legacy), do direct comparison
        console.log('[LOGIN] Password not hashed, doing direct comparison...')
        isValidPassword = password === data.password
      }

      console.log('[LOGIN] Password verification result:', isValidPassword)

      if (!isValidPassword) {
        console.error('[LOGIN] Invalid password')
        return false
      }

      const userData: User = {
        id: data.id,
        email: data.email,
        role: data.role,
        name: data.name || data.email
      }

      console.log('[LOGIN] Success! Setting user:', userData)
      setUser(userData)
      localStorage.setItem('flatrix_user', JSON.stringify(userData))
      return true
    } catch (error) {
      console.error('[LOGIN] Error:', error)
      return false
    }
  }

  const logout = () => {
    setUser(null)
    localStorage.removeItem('flatrix_user')
  }

  const value: AuthContextType = {
    user,
    login,
    logout,
    isLoading
  }

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  )
}