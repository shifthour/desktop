import DashboardLayout from '@/components/DashboardLayout'
import SalesFunnelComponent from '@/components/SalesFunnelComponent'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function FunnelPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT']}>
      <DashboardLayout>
        <SalesFunnelComponent />
      </DashboardLayout>
    </ProtectedRoute>
  )
}
