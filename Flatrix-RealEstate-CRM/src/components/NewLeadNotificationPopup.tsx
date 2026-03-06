'use client'

import { useState, useEffect, useRef } from 'react'
import { X, Phone, Mail, User, Calendar, UserPlus, Sparkles } from 'lucide-react'
import { supabase } from '@/lib/supabase'
import { useAuth } from '@/contexts/AuthContext'

interface NewLead {
  id: string
  name: string
  phone: string
  email?: string
  source?: string
  status: string
  created_at: string
  assigned_to_id?: string
  notes?: string
  preferred_location?: string
  budget_min?: number
}

const FIFTEEN_MINUTES = 15 * 60 * 1000

export default function NewLeadNotificationPopup() {
  const { user } = useAuth()
  const [newLeads, setNewLeads] = useState<NewLead[]>([])
  const [loading, setLoading] = useState(true)
  const [showPopup, setShowPopup] = useState(false)
  const hiddenUntilRef = useRef<number>(0)

  useEffect(() => {
    if (!user) return

    const checkNewLeads = async () => {
      // If user closed the popup, don't show until 15 minutes have passed
      if (Date.now() < hiddenUntilRef.current) {
        return
      }

      try {
        const { data: leads, error: leadsError } = await supabase
          .from('flatrix_leads')
          .select('*')
          .eq('assigned_to_id', user.id)
          .eq('status', 'NEW')
          .order('created_at', { ascending: false })

        if (leadsError) throw leadsError

        if (!leads || leads.length === 0) {
          setNewLeads([])
          setShowPopup(false)
          setLoading(false)
          return
        }

        setNewLeads(leads as NewLead[])
        setShowPopup(true)
        setLoading(false)
      } catch (error) {
        console.error('[NEW LEADS] Error checking new leads:', error)
        setLoading(false)
      }
    }

    // Initial check
    checkNewLeads()

    // Check every 15 minutes only
    const interval = setInterval(checkNewLeads, FIFTEEN_MINUTES)

    return () => clearInterval(interval)
  }, [user])

  const hideFor15Minutes = () => {
    hiddenUntilRef.current = Date.now() + FIFTEEN_MINUTES
    setShowPopup(false)
    setNewLeads([])
  }

  const handleDismiss = (lead: NewLead) => {
    const remaining = newLeads.filter(l => l.id !== lead.id)
    setNewLeads(remaining)

    if (remaining.length === 0) {
      hideFor15Minutes()
    }
  }

  const handleDismissAll = () => {
    hideFor15Minutes()
  }

  const handleCall = (phone: string) => {
    window.location.href = `tel:${phone}`
  }

  const handleEmail = (email: string) => {
    window.location.href = `mailto:${email}`
  }

  const getTimeAgo = (dateString: string) => {
    const date = new Date(dateString)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffMins = Math.floor(diffMs / 60000)
    const diffHours = Math.floor(diffMs / 3600000)

    if (diffMins < 1) return 'Just now'
    if (diffMins < 60) return `${diffMins} minute${diffMins !== 1 ? 's' : ''} ago`
    if (diffHours < 24) return `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
  }

  if (loading || !showPopup || newLeads.length === 0) {
    return null
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-[9999] p-4">
      <div className="bg-white rounded-lg shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="bg-gradient-to-r from-green-600 to-green-700 text-white px-6 py-4 rounded-t-lg">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <div className="bg-white bg-opacity-20 p-2 rounded-full">
                <UserPlus className="h-6 w-6 animate-bounce" />
              </div>
              <div>
                <h2 className="text-xl font-bold">New Lead{newLeads.length !== 1 ? 's' : ''} Assigned!</h2>
                <p className="text-sm text-green-100">
                  You have {newLeads.length} new lead{newLeads.length !== 1 ? 's' : ''} to follow up
                </p>
              </div>
            </div>
            <button
              onClick={handleDismissAll}
              className="p-2 hover:bg-white hover:bg-opacity-20 rounded-lg transition-colors"
              title="Close (will reappear in 15 minutes)"
            >
              <X className="h-6 w-6" />
            </button>
          </div>
        </div>

        {/* New Leads List */}
        <div className="divide-y divide-gray-200">
          {newLeads.map((lead) => (
            <div key={lead.id} className="p-6 hover:bg-gray-50 transition-colors">
              {/* Lead Info */}
              <div className="flex items-start justify-between mb-4">
                <div className="flex items-start space-x-4">
                  <div className="bg-green-100 rounded-full p-3">
                    <User className="h-6 w-6 text-green-600" />
                  </div>
                  <div>
                    <div className="flex items-center space-x-2">
                      <h3 className="text-lg font-semibold text-gray-900">{lead.name}</h3>
                      <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                        <Sparkles className="h-3 w-3 mr-1" />
                        NEW
                      </span>
                    </div>
                    <p className="text-xs text-gray-500 mt-1">
                      <Calendar className="h-3 w-3 inline mr-1" />
                      {getTimeAgo(lead.created_at)}
                    </p>
                    {lead.source && (
                      <span className="inline-block px-2 py-1 text-xs bg-gray-100 text-gray-600 rounded mt-1">
                        Source: {lead.source}
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
                    className="ml-2 text-green-600 hover:text-green-700 font-medium"
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
                      className="ml-2 text-green-600 hover:text-green-700 font-medium"
                    >
                      Email
                    </button>
                  </div>
                )}
              </div>

              {/* Lead Details */}
              <div className="bg-green-50 border border-green-200 rounded-lg p-3 mb-4">
                <div className="grid grid-cols-2 gap-2 text-sm">
                  {lead.preferred_location && (
                    <div>
                      <span className="font-medium text-green-900">Location:</span>
                      <span className="text-green-700 ml-2">{lead.preferred_location}</span>
                    </div>
                  )}
                  {lead.budget_min && (
                    <div>
                      <span className="font-medium text-green-900">Budget:</span>
                      <span className="text-green-700 ml-2">₹{lead.budget_min.toLocaleString()}+</span>
                    </div>
                  )}
                </div>
              </div>

              {/* Notes */}
              {lead.notes && (
                <div className="bg-gray-50 rounded-lg p-3 mb-4">
                  <p className="text-xs font-medium text-gray-700 mb-1">Notes:</p>
                  <p className="text-sm text-gray-600 line-clamp-3">{lead.notes}</p>
                </div>
              )}

              {/* Action Buttons */}
              <div className="flex space-x-3">
                <button
                  onClick={() => handleCall(lead.phone)}
                  className="flex-1 bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors font-medium flex items-center justify-center space-x-2"
                >
                  <Phone className="h-4 w-4" />
                  <span>Call Now</span>
                </button>
                <button
                  onClick={() => handleDismiss(lead)}
                  className="flex-1 bg-gray-100 text-gray-700 px-4 py-2 rounded-lg hover:bg-gray-200 transition-colors font-medium"
                >
                  Acknowledge
                </button>
              </div>
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className="bg-gray-50 px-6 py-4 rounded-b-lg border-t border-gray-200">
          <p className="text-xs text-gray-500 text-center">
            This notification will reappear in 15 minutes until you change the lead status from NEW
          </p>
        </div>
      </div>
    </div>
  )
}
