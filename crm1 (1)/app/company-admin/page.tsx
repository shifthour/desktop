"use client"

import { RoleBasedSidebar } from "@/components/role-based-sidebar"
import { Header } from "@/components/header"
import { ProtectedRoute } from "@/components/protected-route"
import { CompanyAdminDashboard } from "@/components/company-admin-dashboard"

export default function CompanyAdminPage() {
  return (
    <ProtectedRoute requireAdmin={true}>
      <div className="flex h-screen bg-gray-50">
        <RoleBasedSidebar />
        <div className="flex-1 flex flex-col overflow-hidden">
          <Header />
          <main className="flex-1 overflow-x-hidden overflow-y-auto">
            <CompanyAdminDashboard />
          </main>
        </div>
      </div>
    </ProtectedRoute>
  )
}