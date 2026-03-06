'use client'

import { useState, useEffect, useRef } from 'react'
import { X, Phone, Mail, User, Clock, Bell, CheckCheck } from 'lucide-react'
import { supabase } from '@/lib/supabase'
import { useAuth } from '@/contexts/AuthContext'

interface FollowupLead {
  id: string
  name: string
  phone: string
  email?: string
  next_followup_date: string
  notes?: string
  source?: string
  status?: string
  assigned_to_id?: string
}

const FIFTEEN_MINUTES = 15 * 60 * 1000

export default function FollowupNotificationPopup() {
  const { user } = useAuth()
  const [dueFollowups, setDueFollowups] = useState<FollowupLead[]>([])
  const [loading, setLoading] = useState(true)
  const [hidden, setHidden] = useState(false)
  const hiddenUntilRef = useRef<number>(0)

  useEffect(() => {
    if (!user) return

    const checkFollowups = async () => {
      // If user closed the popup, don't show until 15 minutes have passed
      if (Date.now() < hiddenUntilRef.current) {
        return
      }

      // If popup was hidden, un-hide it now (15 min has passed)
      setHidden(false)

      try {
        const now = new Date()
        const fiveMinutesFromNow = new Date(now.getTime() + 5 * 60 * 1000)

        const { data: leads, error: leadsError } = await supabase
          .from('flatrix_leads')
          .select('*')
          .eq('assigned_to_id', user.id)
          .not('next_followup_date', 'is', null)
          .lte('next_followup_date', fiveMinutesFromNow.toISOString())
          .in('status', ['NEW', 'CONTACTED', 'QUALIFIED'])

        if (leadsError) throw leadsError

        if (!leads || leads.length === 0) {
          setDueFollowups([])
          setLoading(false)
          return
        }

        // Check which ones haven't been dismissed
        const { data: dismissed } = await supabase
          .from('flatrix_dismissed_followup_notifications')
          .select('lead_id, followup_datetime')
          .eq('user_id', user.id)
          .in('lead_id', leads.map(l => l.id))

        const dismissedSet = new Set(
          (dismissed || []).map(d => {
            const normalizedDate = new Date(d.followup_datetime).toISOString()
            return `${d.lead_id}-${normalizedDate}`
          })
        )

        const undismissedLeads = leads.filter(lead => {
          const normalizedLeadDate = new Date(lead.next_followup_date).toISOString()
          const key = `${lead.id}-${normalizedLeadDate}`
          return !dismissedSet.has(key)
        })

        setDueFollowups(undismissedLeads as FollowupLead[])
        setLoading(false)
      } catch (error) {
        console.error('Error checking followups:', error)
        setLoading(false)
      }
    }

    // Initial check
    checkFollowups()

    // Check every 15 minutes only
    const interval = setInterval(checkFollowups, FIFTEEN_MINUTES)

    return () => clearInterval(interval)
  }, [user])

  // When user closes the popup (X, Close All, or Mark as Done),
  // hide it for 15 minutes regardless of pending followups
  const hideFor15Minutes = () => {
    hiddenUntilRef.current = Date.now() + FIFTEEN_MINUTES
    setHidden(true)
  }

  const handleDismiss = async (lead: FollowupLead) => {
    if (!user) return

    try {
      const normalizedDatetime = new Date(lead.next_followup_date).toISOString()

      await supabase
        .from('flatrix_dismissed_followup_notifications')
        .insert({
          user_id: user.id,
          lead_id: lead.id,
          followup_datetime: normalizedDatetime
        })

      // Remove from local state
      const remaining = dueFollowups.filter(l => l.id !== lead.id)
      setDueFollowups(remaining)

      // If that was the last one, hide popup for 15 minutes
      if (remaining.length === 0) {
        hideFor15Minutes()
      }
    } catch (error) {
      console.error('[FOLLOWUP] Error dismissing notification:', error)
    }
  }

  const handleDismissAll = async () => {
    if (!user || dueFollowups.length === 0) return

    try {
      const dismissals = dueFollowups.map(lead => ({
        user_id: user.id,
        lead_id: lead.id,
        followup_datetime: new Date(lead.next_followup_date).toISOString()
      }))

      await supabase
        .from('flatrix_dismissed_followup_notifications')
        .upsert(dismissals, { onConflict: 'user_id,lead_id,followup_datetime' })

      setDueFollowups([])
      hideFor15Minutes()
    } catch (error) {
      console.error('[FOLLOWUP] Error dismissing all notifications:', error)
    }
  }

  const handleClosePopup = () => {
    hideFor15Minutes()
  }

  const handleCall = (phone: string) => {
    window.location.href = `tel:${phone}`
  }

  const handleEmail = (email: string) => {
    window.location.href = `mailto:${email}`
  }

  if (loading || hidden || dueFollowups.length === 0) {
    return null
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-[9999] p-4">
      <div className="bg-white rounded-lg shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="bg-gradient-to-r from-blue-600 to-blue-700 text-white px-6 py-4 rounded-t-lg">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <div className="bg-white bg-opacity-20 p-2 rounded-full">
                <Bell className="h-6 w-6 animate-pulse" />
              </div>
              <div>
                <h2 className="text-xl font-bold">Followup Reminder</h2>
                <p className="text-sm text-blue-100">
                  You have {dueFollowups.length} followup{dueFollowups.length !== 1 ? 's' : ''} due now
                </p>
              </div>
            </div>
            <div className="flex items-center space-x-2">
              {dueFollowups.length > 1 && (
                <button
                  onClick={handleDismissAll}
                  className="flex items-center space-x-2 bg-white bg-opacity-20 hover:bg-opacity-30 text-white px-4 py-2 rounded-lg transition-colors text-sm font-medium"
                  title="Close all followup reminders"
                >
                  <CheckCheck className="h-4 w-4" />
                  <span>Close All</span>
                </button>
              )}
              <button
                onClick={handleClosePopup}
                className="p-2 hover:bg-white hover:bg-opacity-20 rounded-lg transition-colors"
                title="Close (will reappear in 15 minutes)"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
          </div>
        </div>

        {/* Followup List */}
        <div className="divide-y divide-gray-200">
          {dueFollowups.map((lead) => (
            <div key={lead.id} className="p-6 hover:bg-gray-50 transition-colors">
              {/* Lead Info */}
              <div className="flex items-start justify-between mb-4">
                <div className="flex items-start space-x-4">
                  <div className="bg-blue-100 rounded-full p-3">
                    <User className="h-6 w-6 text-blue-600" />
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900">{lead.name}</h3>
                    {lead.source && (
                      <span className="inline-block px-2 py-1 text-xs bg-gray-100 text-gray-600 rounded mt-1">
                        {lead.source}
                      </span>
                    )}
                  </div>
                </div>
              </div>

              {/* Contact Details */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-4">
                <div className="flex items-center space-x-2 text-sm">
                  <Phone className="h-4 w-4 text-gray-400" />
                  <span className="text-gray-900">{lead.phone}</span>
                  <button
                    onClick={() => handleCall(lead.phone)}
                    className="ml-2 text-blue-600 hover:text-blue-700 font-medium"
                  >
                    Call
                  </button>
                </div>
                {lead.email && (
                  <div className="flex items-center space-x-2 text-sm">
                    <Mail className="h-4 w-4 text-gray-400" />
                    <span className="text-gray-900 truncate">{lead.email}</span>
                    <button
                      onClick={() => handleEmail(lead.email!)}
                      className="ml-2 text-blue-600 hover:text-blue-700 font-medium"
                    >
                      Email
                    </button>
                  </div>
                )}
              </div>

              {/* Followup Time */}
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-3 mb-4">
                <div className="flex items-center space-x-2 text-sm">
                  <Clock className="h-4 w-4 text-yellow-600" />
                  <span className="font-medium text-yellow-900">Scheduled for:</span>
                  <span className="text-yellow-700">
                    {new Date(lead.next_followup_date).toLocaleString('en-US', {
                      month: 'short',
                      day: 'numeric',
                      year: 'numeric',
                      hour: 'numeric',
                      minute: '2-digit',
                      hour12: true
                    })}
                  </span>
                </div>
              </div>

              {/* Notes */}
              {lead.notes && (
                <div className="bg-gray-50 rounded-lg p-3">
                  <p className="text-xs font-medium text-gray-700 mb-1">Notes:</p>
                  <p className="text-sm text-gray-600 line-clamp-3">{lead.notes}</p>
                </div>
              )}

              {/* Action Buttons */}
              <div className="flex space-x-3 mt-4">
                <button
                  onClick={() => handleCall(lead.phone)}
                  className="flex-1 bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors font-medium flex items-center justify-center space-x-2"
                >
                  <Phone className="h-4 w-4" />
                  <span>Call Now</span>
                </button>
                <button
                  onClick={() => handleDismiss(lead)}
                  className="flex-1 bg-gray-100 text-gray-700 px-4 py-2 rounded-lg hover:bg-gray-200 transition-colors font-medium"
                >
                  Mark as Done
                </button>
              </div>
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className="bg-gray-50 px-6 py-4 rounded-b-lg border-t border-gray-200">
          <p className="text-xs text-gray-500 text-center">
            This notification will reappear in 15 minutes if not marked as done
          </p>
        </div>
      </div>
    </div>
  )
}
