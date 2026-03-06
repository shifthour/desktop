import { RoleBasedSidebar } from "@/components/role-based-sidebar"
import { Header } from "@/components/header"
import { OpportunitiesContent } from "@/components/opportunities-content"

export const metadata = {
  title: "Deals",
}

export default function OpportunitiesPage() {
  return (
    <div className="flex h-screen bg-gray-50">
      <RoleBasedSidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <Header />
        <main className="flex-1 overflow-x-hidden overflow-y-auto">
          <OpportunitiesContent />
        </main>
      </div>
    </div>
  )
}
