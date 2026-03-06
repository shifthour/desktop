import { Suspense } from 'react'
import DashboardLayout from '@/components/DashboardLayout'
import LeadsComponent from '@/components/LeadsComponent'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function LeadsPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT']}>
      <DashboardLayout>
        <Suspense fallback={<div className="flex items-center justify-center h-64"><div className="text-lg text-gray-600">Loading...</div></div>}>
          <LeadsComponent />
        </Suspense>
      </DashboardLayout>
    </ProtectedRoute>
  )
}