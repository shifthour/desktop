import DashboardLayout from '@/components/DashboardLayout'
import CommissionsComponent from '@/components/CommissionsComponent'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function CommissionsPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin']}>
      <DashboardLayout>
        <CommissionsComponent />
      </DashboardLayout>
    </ProtectedRoute>
  )
}