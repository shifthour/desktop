'use client'

import { useChannelPartners } from '@/hooks/useDatabase'

export default function PartnersComponent() {
  const { data: partners, loading } = useChannelPartners()

  if (loading) {
    return <div className="text-center py-8">Loading partners...</div>
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Channel Partners</h1>
        <p className="text-gray-600 mt-2">Manage your channel partner network</p>
      </div>

      <div className="bg-white rounded-lg shadow p-8 text-center">
        <p className="text-gray-500 text-lg">
          Channel Partners section - {partners?.length || 0} partners found
        </p>
        <p className="text-sm text-gray-400 mt-2">
          This section will show partner management interface
        </p>
      </div>
    </div>
  )
}