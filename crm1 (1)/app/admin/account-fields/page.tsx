import { RoleBasedSidebar } from "@/components/role-based-sidebar"
import { Header } from "@/components/header"
import { ProtectedRoute } from "@/components/protected-route"

export default function AccountFieldsPage() {
  return (
    <ProtectedRoute requireAdmin={true}>
      <div className="flex h-screen bg-gray-50">
        <RoleBasedSidebar />
        <div className="flex-1 flex flex-col overflow-hidden">
          <Header />
          <main className="flex-1 overflow-x-hidden overflow-y-auto p-6">
            <div className="text-center">
              <h1 className="text-2xl font-bold text-gray-900 mb-4">Account Fields Manager</h1>
              <p className="text-gray-600">This feature is temporarily unavailable due to maintenance.</p>
              <p className="text-sm text-gray-500 mt-2">Please contact support for assistance.</p>
            </div>
          </main>
        </div>
      </div>
    </ProtectedRoute>
  )
}
