"use client"

import { RoleBasedSidebar } from "@/components/role-based-sidebar"
import { Header } from "@/components/header"
import { AdminDashboard } from "@/components/admin-dashboard"
import { ProtectedRoute } from "@/components/protected-route"

export default function AdminPage() {
  return (
    <ProtectedRoute requireAdmin={true}>
      <div className="flex h-screen bg-gray-50">
        <RoleBasedSidebar />
        <div className="flex-1 flex flex-col overflow-hidden">
          <Header />
          <main className="flex-1 overflow-x-hidden overflow-y-auto">
            <AdminDashboard />
          </main>
        </div>
      </div>
    </ProtectedRoute>
  )
}
