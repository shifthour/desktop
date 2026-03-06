import DashboardLayout from '@/components/DashboardLayout'
import DashboardOverview from '@/components/DashboardOverview'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function DashboardPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin']}>
      <DashboardLayout>
        <DashboardOverview />
      </DashboardLayout>
    </ProtectedRoute>
  )
}