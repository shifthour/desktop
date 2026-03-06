'use client'

import DashboardLayout from '@/components/DashboardLayout'
import ActivitiesComponent from '@/components/ActivitiesComponent'
import ProtectedRoute from '@/components/ProtectedRoute'

export default function ActivitiesPage() {
  return (
    <ProtectedRoute allowedRoles={['super_admin', 'ADMIN', 'SALES_MANAGER', 'AGENT']}>
      <DashboardLayout>
        <ActivitiesComponent />
      </DashboardLayout>
    </ProtectedRoute>
  )
}