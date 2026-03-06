import DashboardLayout from '@/components/DashboardLayout'
import ReportsComponent from '@/components/ReportsComponent'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function ReportsPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin']}>
      <DashboardLayout>
        <ReportsComponent />
      </DashboardLayout>
    </ProtectedRoute>
  )
}