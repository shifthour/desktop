import { RoleBasedSidebar } from "@/components/role-based-sidebar"
import { Header } from "@/components/header"
import { DynamicEditContactContent } from "@/components/dynamic-edit-contact-content"

export default async function EditContactPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params

  return (
    <div className="flex h-screen bg-gray-50">
      <RoleBasedSidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <Header />
        <main className="flex-1 overflow-x-hidden overflow-y-auto">
          <DynamicEditContactContent contactId={id} />
        </main>
      </div>
    </div>
  )
}
