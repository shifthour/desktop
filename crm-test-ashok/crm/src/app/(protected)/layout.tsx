import { redirect } from 'next/navigation'
import { getSession } from '@/lib/auth'
import { MainNav } from '@/components/layout/main-nav'

export default async function ProtectedLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const session = await getSession()

  if (!session) {
    redirect('/login')
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <MainNav 
        user={{
          name: session.name,
          email: session.email,
          role: session.role
        }}
      />
      <main className="py-8">
        <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
          {children}
        </div>
      </main>
    </div>
  )
}