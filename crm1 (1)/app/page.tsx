"use client"

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { RoleBasedSidebar } from "@/components/role-based-sidebar"
import { Header } from "@/components/header"
import { DashboardContent } from "@/components/dashboard-content"
import { ProtectedRoute } from "@/components/protected-route"

export default function Home() {
  const router = useRouter()

  useEffect(() => {
    // Check if user is authenticated, but don't redirect admins
    const storedUser = localStorage.getItem('user')
    
    if (!storedUser) {
      router.push('/login')
      return
    }

    try {
      const user = JSON.parse(storedUser)
      // All authenticated users (including admins) can view the dashboard
      // No automatic redirects - let users navigate where they want
    } catch (error) {
      router.push('/login')
    }
  }, [router])

  return (
    <ProtectedRoute>
      <div className="flex h-screen bg-gray-50">
        <RoleBasedSidebar />
        <div className="flex-1 flex flex-col overflow-hidden">
          <Header />
          <main className="flex-1 overflow-x-hidden overflow-y-auto">
            <DashboardContent />
          </main>
        </div>
      </div>
    </ProtectedRoute>
  )
}