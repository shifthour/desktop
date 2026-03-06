'use client'

import { useAuth } from '@/contexts/AuthContext'
import FollowupNotificationPopup from './FollowupNotificationPopup'
import NewLeadNotificationPopup from './NewLeadNotificationPopup'

export default function ClientLayoutWrapper() {
  const { user } = useAuth()

  // Only show notification popup if user is logged in and is an AGENT or SALES_MANAGER
  if (!user || (user.role !== 'AGENT' && user.role !== 'SALES_MANAGER')) {
    return null
  }

  return (
    <>
      <NewLeadNotificationPopup />
      <FollowupNotificationPopup />
    </>
  )
}
