import DashboardLayout from '@/components/DashboardLayout'
import PropertiesComponent from '@/components/PropertiesComponent'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function PropertiesPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT']}>
      <DashboardLayout>
        <PropertiesComponent />
      </DashboardLayout>
    </ProtectedRoute>
  )
}