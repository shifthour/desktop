import DashboardLayout from '@/components/DashboardLayout'
import UsersManagementComponent from '@/components/UsersManagementComponent'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function UsersPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin']}>
      <DashboardLayout>
        <UsersManagementComponent />
      </DashboardLayout>
    </ProtectedRoute>
  )
}
