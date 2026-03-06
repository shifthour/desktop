import { Suspense } from 'react'
import DashboardLayout from '@/components/DashboardLayout'
import SiteVisitsComponent from '@/components/SiteVisitsComponent'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function SiteVisitsPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT']}>
      <DashboardLayout>
        <Suspense fallback={<div className="flex items-center justify-center h-64"><div className="text-lg text-gray-600">Loading...</div></div>}>
          <SiteVisitsComponent />
        </Suspense>
      </DashboardLayout>
    </ProtectedRoute>
  )
}