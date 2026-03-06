import { RoleBasedSidebar } from "@/components/role-based-sidebar"
import { Header } from "@/components/header"
import { SalesOrdersContent } from "@/components/sales-orders-content"

export default function SalesOrdersPage() {
  return (
    <div className="flex h-screen bg-gray-50">
      <RoleBasedSidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <Header />
        <main className="flex-1 overflow-x-hidden overflow-y-auto">
          <SalesOrdersContent />
        </main>
      </div>
    </div>
  )
}
