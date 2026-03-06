'use client'

import { useState, useRef, useEffect } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { 
  Search, 
  Plus, 
  Phone, 
  Mail, 
  Calendar,
  User,
  MapPin,
  ChevronDown,
  X,
  Edit2,
  Building,
  UserCheck,
  Users,
  TrendingUp,
  CheckCircle,
  Download,
  Upload,
  MessageSquare,
  FileText,
  Car,
  BarChart3
} from 'lucide-react'
import { useDeals, useLeads } from '@/hooks/useDatabase'
import { supabase } from '@/lib/supabase'
import toast from 'react-hot-toast'
import { useAuth } from '@/contexts/AuthContext'

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M12.031 2c-5.523 0-10.031 4.507-10.031 10.031 0 1.74.443 3.387 1.288 4.816l-1.288 4.153 4.232-1.245c1.389.761 2.985 1.214 4.661 1.214h.004c5.522 0 10.031-4.507 10.031-10.031 0-2.672-1.041-5.183-2.93-7.071-1.892-1.892-4.405-2.937-7.071-2.937zm.004 18.375h-.003c-1.465 0-2.899-.394-4.148-1.14l-.297-.177-3.085.808.821-2.998-.193-.307c-.821-1.302-1.253-2.807-1.253-4.358 0-4.516 3.677-8.192 8.193-8.192 2.186 0 4.239.852 5.784 2.397 1.545 1.545 2.397 3.598 2.397 5.783-.003 4.518-3.68 8.194-8.196 8.194zm4.5-6.143c-.247-.124-1.463-.722-1.69-.805-.227-.083-.392-.124-.558.124-.166.248-.642.805-.786.969-.145.165-.29.186-.537.062-.247-.124-1.045-.385-1.99-1.227-.736-.657-1.233-1.468-1.378-1.715-.145-.247-.016-.381.109-.504.112-.111.247-.29.371-.434.124-.145.166-.248.248-.413.083-.166.042-.31-.021-.434-.062-.124-.558-1.343-.765-1.839-.201-.479-.407-.414-.558-.422-.145-.007-.31-.009-.476-.009-.166 0-.435.062-.662.31-.227.248-.866.847-.866 2.067 0 1.22.889 2.395 1.013 2.56.124.166 1.75 2.667 4.24 3.74.592.256 1.055.408 1.415.523.594.189 1.135.162 1.563.098.476-.071 1.463-.598 1.669-1.175.207-.577.207-1.071.145-1.175-.062-.104-.227-.165-.476-.289z"/>
  </svg>
)

const siteVisitStatuses = ['NOT_VISITED', 'SCHEDULED', 'COMPLETED', 'CANCELLED']
const conversionStatuses = ['PENDING', 'BOOKED', 'NOT_BOOKED', 'CANCELLED', 'LOST']

const statusColors = {
  NOT_VISITED: 'bg-gray-100 text-gray-800 border-gray-200',
  SCHEDULED: 'bg-blue-100 text-blue-800 border-blue-200',
  COMPLETED: 'bg-green-100 text-green-800 border-green-200',
  CANCELLED: 'bg-red-100 text-red-800 border-red-200'
}

const conversionColors = {
  PENDING: 'bg-yellow-100 text-yellow-800 border-yellow-200',
  BOOKED: 'bg-green-100 text-green-800 border-green-200',
  NOT_BOOKED: 'bg-red-100 text-red-800 border-red-200',
  CANCELLED: 'bg-gray-100 text-gray-800 border-gray-200',
  LOST: 'bg-red-100 text-red-800 border-red-200'
}

// Helper function to map source names for display
const getDisplaySource = (source: string) => {
  if (!source) return 'Unknown'
  
  const googleAdsSources = ['brochure_download', 'site_visit', 'floor_plans', 'gallery', 'location']
  if (googleAdsSources.includes(source.toLowerCase())) {
    return 'Google Ads'
  }
  
  return source
}

// Helper function to parse notes and extract individual entries
const parseNotes = (notes: string, leadCreatedAt?: string) => {
  if (!notes) return []
  
  let referralEntry = null
  let remainingNotes = notes
  
  if (notes.startsWith('Referral Name:')) {
    const referralMatch = notes.match(/^Referral Name: ([^\n]+)(?:\n\n)?(.*)$/)
    if (referralMatch) {
      referralEntry = {
        timestamp: leadCreatedAt || null,
        content: `Referral Name: ${referralMatch[1]}`,
        date: leadCreatedAt ? new Date(leadCreatedAt) : null,
        isReferral: true
      }
      remainingNotes = referralMatch[2] || ''
    }
  }
  
  const entries = remainingNotes ? remainingNotes.split(/\n\n(?=\[)/g) : []
  
  const parsedEntries = entries.map(entry => {
    const trimmed = entry.trim()
    if (!trimmed) return null

    const timestampMatch = trimmed.match(/^\[([^\]]+)\]:?\s*(.*)$/)
    if (timestampMatch) {
      const timestampStr = timestampMatch[1]
      let parsedDate = new Date(timestampStr)

      // If the date is invalid, try parsing it as a US locale date string
      if (isNaN(parsedDate.getTime())) {
        // Try to parse formats like "10/16/25, 11:23 PM" or "10/16/2025, 11:23 PM"
        const usDateMatch = timestampStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4}),?\s+(\d{1,2}):(\d{2})\s+(AM|PM)$/i)
        if (usDateMatch) {
          const [, month, day, year, hours, minutes, period] = usDateMatch
          const fullYear = year.length === 2 ? `20${year}` : year
          let hour24 = parseInt(hours, 10)
          if (period.toUpperCase() === 'PM' && hour24 !== 12) hour24 += 12
          if (period.toUpperCase() === 'AM' && hour24 === 12) hour24 = 0
          parsedDate = new Date(`${fullYear}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T${hour24.toString().padStart(2, '0')}:${minutes}:00`)
        }
      }

      return {
        timestamp: timestampStr,
        content: timestampMatch[2].trim(),
        date: parsedDate,
        isReferral: false
      }
    } else {
      return {
        timestamp: leadCreatedAt || null,
        content: trimmed,
        date: leadCreatedAt ? new Date(leadCreatedAt) : null,
        isReferral: false
      }
    }
  }).filter(Boolean)
  
  if (referralEntry) {
    parsedEntries.unshift(referralEntry)
  }
  
  return parsedEntries.sort((a, b) => {
    if (!a?.date && !b?.date) return 0
    if (!a?.date) return 1
    if (!b?.date) return -1
    return (b?.date?.getTime() || 0) - (a?.date?.getTime() || 0)
  })
}

// Helper function to get the latest note
const getLatestNote = (notes: string, leadCreatedAt?: string) => {
  const parsedNotes = parseNotes(notes, leadCreatedAt)
  if (parsedNotes.length === 0) return null
  
  const latest = parsedNotes[0]
  return (latest?.content?.length || 0) > 50 ? latest?.content?.substring(0, 50) + '...' : (latest?.content || '')
}

export default function DealsComponent() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const { user } = useAuth()
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedStatus, setSelectedStatus] = useState('NOT_VISITED')
  const [selectedAssignee, setSelectedAssignee] = useState('All Assigned')
  const [sortBy, setSortBy] = useState('qualified_date')
  const [showEditModal, setShowEditModal] = useState(false)
  const [editingDeal, setEditingDeal] = useState<any>(null)
  const [showNotesHistory, setShowNotesHistory] = useState(false)
  const [selectedDealNotes, setSelectedDealNotes] = useState<any>(null)
  const [notesHistoryType, setNotesHistoryType] = useState<'regular' | 'management'>('regular')
  const [appendNote, setAppendNote] = useState('')
  const [showWhatsAppModal, setShowWhatsAppModal] = useState(false)
  const [selectedDeal, setSelectedDeal] = useState<any>(null)
  const [siteVisitDate, setSiteVisitDate] = useState('')

  const { data: deals, loading: dealsLoading, refetch: refetchDeals } = useDeals()
  const { data: leads, loading: leadsLoading, refetch: refetchLeads } = useLeads()

  // Debug logging for deals data structure
  useEffect(() => {
    if (deals && deals.length > 0) {
      console.log('[DEALS DEBUG] Sample deal structure:', deals[0])
      const dealWithSuresh = deals.find((d: any) => {
        const leadPhone = (d as any).lead?.phone || d.lead_phone
        return leadPhone === '9398446636'
      })
      if (dealWithSuresh) {
        console.log('[DEALS DEBUG] Suresh deal from useDeals:', dealWithSuresh)
        console.log('[DEALS DEBUG] Suresh deal.lead:', (dealWithSuresh as any).lead)
      }
    }
  }, [deals])

  // Handle URL parameters for filtering from Activities page
  useEffect(() => {
    const searchParam = searchParams.get('search')
    if (searchParam) {
      setSearchTerm(searchParam)
    }
  }, [searchParams])

  // Auto-assign unassigned leads when page loads
  useEffect(() => {
    const autoAssignUnassignedLeads = async () => {
      console.log('[AUTO-ASSIGN] Starting auto-assignment check...')
      console.log('[AUTO-ASSIGN] Leads loaded:', leads?.length || 0)

      if (!leads || leads.length === 0) {
        console.log('[AUTO-ASSIGN] No leads data available yet')
        return
      }

      // Find all unassigned leads
      const unassignedLeads = leads.filter((lead: any) => !lead.assigned_to_id)
      console.log('[AUTO-ASSIGN] Total leads:', leads.length)
      console.log('[AUTO-ASSIGN] Unassigned leads:', unassignedLeads.length)

      if (unassignedLeads.length === 0) {
        console.log('[AUTO-ASSIGN] All leads are already assigned')
        return
      }

      console.log(`[AUTO-ASSIGN] Found ${unassignedLeads.length} unassigned leads, assigning to agents...`)
      console.log('[AUTO-ASSIGN] Sample unassigned leads:', unassignedLeads.slice(0, 3).map((l: any) => ({
        id: l.id,
        name: (l as any).name || [l.first_name, l.last_name].filter(Boolean).join(' ') || 'Unknown',
        phone: l.phone,
        assigned_to_id: l.assigned_to_id
      })))

      // Assign each unassigned lead
      let successCount = 0
      let failCount = 0

      for (const lead of unassignedLeads) {
        try {
          const leadName = (lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown'
          console.log(`[AUTO-ASSIGN] Processing lead: ${leadName} (${lead.phone})`)

          // Get next agent for round-robin assignment
          const { data: nextUserId, error: rotationError } = await supabase
            .rpc('get_next_assignee')

          console.log(`[AUTO-ASSIGN] get_next_assignee() returned:`, { nextUserId, error: rotationError })

          if (rotationError) {
            console.error('[AUTO-ASSIGN] Error getting next assignee:', rotationError)
            failCount++
            continue
          }

          if (!nextUserId) {
            console.error('[AUTO-ASSIGN] No agent ID returned from get_next_assignee(). Check if you have active AGENT role users.')
            failCount++
            continue
          }

          // Update the lead with assigned agent
          const { error: updateError } = await supabase
            .from('flatrix_leads')
            .update({
              assigned_to_id: nextUserId,
              updated_at: new Date().toISOString()
            })
            .eq('id', lead.id)

          if (updateError) {
            console.error('[AUTO-ASSIGN] Error updating lead assignment:', updateError)
            failCount++
          } else {
            console.log(`[AUTO-ASSIGN] ✓ Assigned lead ${lead.id} (${leadName}) to agent ${nextUserId}`)
            successCount++
          }
        } catch (error) {
          console.error('[AUTO-ASSIGN] Exception in auto-assignment:', error)
          failCount++
        }
      }

      console.log(`[AUTO-ASSIGN] Completed: ${successCount} successful, ${failCount} failed`)

      // Refresh the leads list after assignments
      if (successCount > 0) {
        console.log('[AUTO-ASSIGN] Refreshing data...')
        refetchLeads()
        refetchDeals() // Also refresh deals to show updated assignments
      }
    }

    // Run auto-assignment after a short delay to ensure leads are loaded
    const timeoutId = setTimeout(() => {
      autoAssignUnassignedLeads()
    }, 1000)

    return () => clearTimeout(timeoutId)
  }, [leads, refetchLeads, refetchDeals])

  // Debug logging for data
  console.log('Deals data:', deals?.length || 0, 'deals loaded')
  console.log('Leads data:', leads?.length || 0, 'leads loaded')
  console.log('Qualified leads:', leads?.filter(lead => lead.status === 'QUALIFIED')?.length || 0)
  if ((leads?.length || 0) > 0) {
    console.log('Sample lead statuses:', leads?.slice(0, 3).map(l => ({ id: l.id, name: [l.first_name, l.last_name].filter(Boolean).join(' '), status: l.status })))
  }

  // Get qualified leads and join with deals data
  const qualifiedLeadsWithDeals = leads?.filter(lead => lead.status === 'QUALIFIED')?.map(lead => {
    const deal = deals?.find(d => d.lead_id === lead.id)
    const combined = {
      ...lead,
      ...deal,
      lead_id: lead.id,
      lead_name: (lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown',
      lead_email: lead.email,
      lead_phone: lead.phone,
      lead_source: lead.source,
      lead_notes: lead.notes,
      management_notes: (lead as any).management_notes,
      lead_created_at: lead.created_at,
      lead_preferred_type: (lead as any)?.preferred_type,
      lead_budget_min: (lead as any)?.budget_min,
      site_visit_status: (deal as any)?.site_visit_status || 'NOT_VISITED',
      site_visit_date: (deal as any)?.site_visit_date,
      conversion_status: (deal as any)?.conversion_status || 'PENDING',
      conversion_date: (deal as any)?.conversion_date,
      deal_id: deal?.id,
      deal_created_at: deal?.created_at, // When lead was qualified
      deal_value: deal?.deal_value || (lead as any)?.budget_max,
      registered_date: (lead as any)?.registered_date // When lead was registered
    }
    
    // Debug logging for first few records
    if (lead.id && Math.random() < 0.1) { // Log ~10% of records to avoid spam
      console.log('Lead-Deal join debug:', {
        leadId: lead.id,
        leadName: (lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown',
        dealFound: !!deal,
        dealId: deal?.id,
        siteVisitStatus: (deal as any)?.site_visit_status,
        combinedStatus: combined.site_visit_status
      })
    }
    
    return combined
  }) || []
  
  console.log('Total qualified leads with deals:', qualifiedLeadsWithDeals.length)
  console.log('Deals with site visit status:', qualifiedLeadsWithDeals.filter(d => d.site_visit_status !== 'NOT_VISITED').length)

  // Calculate stats
  const totalQualifiedLeads = qualifiedLeadsWithDeals.length
  const siteVisitCompleted = qualifiedLeadsWithDeals.filter(deal => deal.site_visit_status === 'COMPLETED').length
  const flatsBooked = qualifiedLeadsWithDeals.filter(deal => deal.conversion_status === 'BOOKED').length
  const conversionRate = siteVisitCompleted > 0 ? Math.round((flatsBooked / siteVisitCompleted) * 100) : 0
  const noSiteVisitLeads = qualifiedLeadsWithDeals.filter(deal => deal.site_visit_status === 'NOT_VISITED').length

  // Mock percentage changes
  const statsChange = {
    totalQualified: '+15.3%',
    siteVisits: '+22.4%',
    conversionRate: '+5.2%',
    noSiteVisits: '-12.8%'
  }

  // Get unique agents for filter
  const uniqueAgents = Array.from(
    new Set(
      qualifiedLeadsWithDeals
        .map(deal => (deal as any).assigned_to?.name)
        .filter(Boolean)
    )
  ).sort()

  const filteredAndSortedDeals = qualifiedLeadsWithDeals.filter(deal => {
    if (!deal) return false

    const searchMatches = !searchTerm ||
      deal.lead_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      deal.lead_email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      deal.lead_phone?.includes(searchTerm) ||
      deal.preferred_location?.toLowerCase().includes(searchTerm.toLowerCase())

    let statusMatches = false
    if (selectedStatus === 'Active Site Visits') {
      statusMatches = deal.site_visit_status === 'NOT_VISITED' || deal.site_visit_status === 'SCHEDULED'
    } else if (selectedStatus === 'All Statuses') {
      statusMatches = true
    } else {
      statusMatches = deal.site_visit_status === selectedStatus
    }

    // Agent filter logic
    let assigneeMatches = true
    const dealAssignedTo = (deal as any).assigned_to?.name
    if (selectedAssignee === 'Assigned to Me') {
      assigneeMatches = deal.assigned_to_id === user?.id
    } else if (selectedAssignee === 'Unassigned') {
      assigneeMatches = !deal.assigned_to_id
    } else if (selectedAssignee !== 'All Assigned') {
      // Filter by specific agent name
      assigneeMatches = dealAssignedTo === selectedAssignee
    }

    return searchMatches && statusMatches && assigneeMatches
  })?.sort((a, b) => {
    switch (sortBy) {
      case 'qualified_date':
        // Use deal created_at (when lead was qualified) or fall back to lead updated_at
        const aQualifiedDate = (a as any).deal_created_at || (a as any).lead_created_at || (a as any).created_at || a.updated_at
        const bQualifiedDate = (b as any).deal_created_at || (b as any).lead_created_at || (b as any).created_at || b.updated_at
        return new Date(bQualifiedDate || 0).getTime() - new Date(aQualifiedDate || 0).getTime()
      case 'next_followup':
        const aFollowupDate = a.next_followup_date ? new Date(a.next_followup_date).getTime() : 0
        const bFollowupDate = b.next_followup_date ? new Date(b.next_followup_date).getTime() : 0
        return bFollowupDate - aFollowupDate
      default:
        const aDefaultDate = (a as any).deal_created_at || a.updated_at
        const bDefaultDate = (b as any).deal_created_at || b.updated_at
        return new Date(bDefaultDate || 0).getTime() - new Date(aDefaultDate || 0).getTime()
    }
  })

  const handleEditDeal = (deal: any) => {
    console.log('Opening edit modal for deal:', deal)
    const editData = {
      ...deal,
      site_visit_date: deal.site_visit_date ? deal.site_visit_date.split('T')[0] : '',
      next_followup_date: deal.next_followup_date
        ? new Date(deal.next_followup_date).toISOString().slice(0, 16)
        : '',
      conversion_date: deal.conversion_date ? deal.conversion_date.split('T')[0] : ''
    }
    console.log('Edit data prepared:', editData)
    setEditingDeal(editData)
    setAppendNote('')
    setShowEditModal(true)
  }

  const handleUpdateDeal = async (e: React.FormEvent) => {
    e.preventDefault()
    
    console.log('Starting deal update...', editingDeal)
    console.log('editingDeal fields:', Object.keys(editingDeal))
    console.log('site_visit_status:', editingDeal.site_visit_status)
    console.log('deal_id:', editingDeal.deal_id)
    console.log('lead_id:', editingDeal.lead_id)
    
    try {
      let updatedNotes = editingDeal.lead_notes || ''
      
      if (appendNote.trim()) {
        const timestamp = new Date().toLocaleString('en-US', { 
          dateStyle: 'short', 
          timeStyle: 'short' 
        })
        updatedNotes = updatedNotes 
          ? `${updatedNotes}\n\n[${timestamp}]:\n${appendNote.trim()}`
          : `[${timestamp}]:\n${appendNote.trim()}`
      }

      // Handle management notes update
      let updatedManagementNotes = (editingDeal as any).management_notes || ''
      if ((editingDeal as any).appendManagementNote?.trim()) {
        const timestamp = new Date().toLocaleString('en-US', { 
          dateStyle: 'short', 
          timeStyle: 'short' 
        })
        updatedManagementNotes = updatedManagementNotes 
          ? `${updatedManagementNotes}\n\n[${timestamp}]:\n${(editingDeal as any).appendManagementNote.trim()}`
          : `[${timestamp}]:\n${(editingDeal as any).appendManagementNote.trim()}`
      }

      // Update the lead notes and next_followup_date
      console.log('Updating lead with:', {
        notes: updatedNotes,
        management_notes: updatedManagementNotes,
        next_followup_date: editingDeal.next_followup_date || null,
        lead_id: editingDeal.lead_id
      })
      
      const { error: leadError } = await supabase
        .from('flatrix_leads')
        .update({
          notes: updatedNotes,
          management_notes: updatedManagementNotes,
          next_followup_date: editingDeal.next_followup_date
            ? new Date(editingDeal.next_followup_date).toISOString()
            : null
        })
        .eq('id', editingDeal.lead_id)

      if (leadError) {
        console.error('Lead update error:', leadError)
        throw leadError
      }
      console.log('Lead updated successfully')

      // Update or create deal record with site visit status
      if (editingDeal.deal_id) {
        // Update existing deal
        console.log('Updating existing deal:', editingDeal.deal_id, {
          site_visit_status: editingDeal.site_visit_status,
          site_visit_date: editingDeal.site_visit_date || null
        })
        
        const { error: dealError } = await supabase
          .from('flatrix_deals')
          .update({
            site_visit_status: editingDeal.site_visit_status,
            site_visit_date: editingDeal.site_visit_date || null,
            conversion_status: editingDeal.conversion_status,
            conversion_date: editingDeal.conversion_date || null
          })
          .eq('id', editingDeal.deal_id)

        if (dealError) {
          console.error('Deal update error:', dealError)
          throw dealError
        }
        console.log('Deal updated successfully')
      } else {
        // Create new deal record if it doesn't exist
        console.log('Creating new deal for lead:', editingDeal.lead_id)
        
        const dealData = {
          lead_id: editingDeal.lead_id,
          user_id: editingDeal.assigned_to_id || editingDeal.assigned_to?.id || null,
          property_id: editingDeal.property_id || null, // Add property_id field
          deal_value: editingDeal.deal_value || editingDeal.budget_max || 0,
          site_visit_status: editingDeal.site_visit_status,
          site_visit_date: editingDeal.site_visit_date || null,
          conversion_status: editingDeal.conversion_status || 'PENDING',
          conversion_date: editingDeal.conversion_date || null,
          status: 'ACTIVE'
        }
        
        console.log('Deal data to insert:', dealData)
        console.log('Available assigned_to data:', editingDeal.assigned_to)
        console.log('assigned_to_id:', editingDeal.assigned_to_id)
        
        const { error: dealError } = await supabase
          .from('flatrix_deals')
          .insert(dealData)

        if (dealError) {
          console.error('Deal create error:', dealError)
          throw dealError
        }
        console.log('Deal created successfully')
      }

      toast.success('Deal updated successfully!')
      setShowEditModal(false)
      
      // Navigate to Site Visits page if site visit status was changed to COMPLETED
      const shouldNavigate = editingDeal.site_visit_status === 'COMPLETED'
      
      setEditingDeal(null)
      setAppendNote('')
      
      console.log('Refetching data...')
      
      // Wait a bit for database transaction to complete
      setTimeout(async () => {
        console.log('First refetch...')
        await refetchDeals()
        await refetchLeads()
        
        // Second refetch after another delay
        setTimeout(async () => {
          console.log('Final refetch...')
          await refetchDeals()
          await refetchLeads()
          console.log('All refetches completed')
          
          // Navigate to Site Visits page after data is updated
          if (shouldNavigate) {
            router.push('/site-visits')
          }
        }, 1500)
      }, 500)
    } catch (error) {
      console.error('Full error object:', error)
      console.error('Error message:', (error as any)?.message)
      console.error('Error details:', JSON.stringify(error, null, 2))
      toast.error(`Failed to update deal: ${(error as any)?.message || error}`)
    }
  }

  const handleWhatsAppGroup = (deal: any) => {
    setSelectedDeal(deal)
    setSiteVisitDate('')
    setShowWhatsAppModal(true)
  }

  const sendToWhatsAppGroup = async () => {
    if (!selectedDeal || !siteVisitDate) {
      toast.error('Please enter site visit date')
      return
    }

    const message = `Hi Team, please find Client details below

Client Name: ${selectedDeal.lead_name || selectedDeal.name || 'N/A'}
Phone Number: ${selectedDeal.lead_phone || selectedDeal.phone || 'N/A'}
Email: ${selectedDeal.lead_email || selectedDeal.email || 'None'}
Site Visit Date: ${siteVisitDate}

Regards,
${user?.name || 'Flatrix Team'}`

    try {
      // Copy message to clipboard
      await navigator.clipboard.writeText(message)
      
      // Open WhatsApp group
      const whatsappUrl = `https://chat.whatsapp.com/GtZCDIzfHfKAg3ZDwqJZHx`
      window.open(whatsappUrl, '_blank')
      
      toast.success('Message copied to clipboard! Paste it in the WhatsApp group.')
    } catch (err) {
      // Fallback if clipboard API doesn't work
      const whatsappUrl = `https://chat.whatsapp.com/GtZCDIzfHfKAg3ZDwqJZHx`
      window.open(whatsappUrl, '_blank')
      
      // Show the message in an alert as fallback
      alert(`Message to copy and paste:\n\n${message}`)
    }
    
    setShowWhatsAppModal(false)
    setSiteVisitDate('')
    setSelectedDeal(null)
  }

  if (dealsLoading || leadsLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-lg text-gray-600">Loading deals...</div>
      </div>
    )
  }

  return (
    <div>
      <div className="mb-6 flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Deals Management</h1>
          <p className="text-gray-600 mt-2">Track and manage your qualified leads and site visits</p>
        </div>
        <div className="flex gap-3">
          <button className="flex items-center space-x-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition">
            <Download className="h-4 w-4" />
            <span>Export</span>
          </button>
          <button className="flex items-center space-x-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition">
            <Upload className="h-4 w-4" />
            <span>Import Data</span>
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {/* Total Qualified Leads */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Total Qualified Leads</p>
              <h3 className="text-3xl font-bold text-gray-900">{totalQualifiedLeads}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.totalQualified}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-blue-100 p-3 rounded-lg">
              <Users className="h-6 w-6 text-blue-600" />
            </div>
          </div>
        </div>

        {/* Site Visits Completed */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Site Visits Completed</p>
              <h3 className="text-3xl font-bold text-gray-900">{siteVisitCompleted}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.siteVisits}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-green-100 p-3 rounded-lg">
              <Car className="h-6 w-6 text-green-600" />
            </div>
          </div>
        </div>

        {/* Site Visit Conversion Rate */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Conversion Rate</p>
              <h3 className="text-3xl font-bold text-gray-900">{conversionRate}%</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.conversionRate}</span>
                <span className="text-xs text-gray-500 ml-2">improvement</span>
              </div>
            </div>
            <div className="bg-purple-100 p-3 rounded-lg">
              <BarChart3 className="h-6 w-6 text-purple-600" />
            </div>
          </div>
        </div>

        {/* No Site Visit Leads */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">No Site Visit Leads</p>
              <h3 className="text-3xl font-bold text-gray-900">{noSiteVisitLeads}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-red-600 mr-1 rotate-180" />
                <span className="text-sm text-red-600 font-medium">{statsChange.noSiteVisits}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-orange-100 p-3 rounded-lg">
              <X className="h-6 w-6 text-orange-600" />
            </div>
          </div>
        </div>
      </div>

      {/* Search and Filters Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 mb-6">
        <div className="p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Deal Search & Filters</h2>
          <p className="text-sm text-gray-600 mb-4">Filter and search through your deals</p>
          
          <div className="flex flex-col gap-4">
            <div className="w-full">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search deals by name, contact, or city..."
                  className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>
            </div>
            
            <div className="flex flex-col sm:flex-row gap-3">
              <div className="relative flex-1">
                <select
                  value={selectedStatus}
                  onChange={(e) => setSelectedStatus(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option>Active Site Visits</option>
                  <option>All Statuses</option>
                  {siteVisitStatuses.map(status => (
                    <option key={status} value={status}>{status.replace('_', ' ')}</option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <select
                  value={selectedAssignee}
                  onChange={(e) => setSelectedAssignee(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option>All Assigned</option>
                  <option>Assigned to Me</option>
                  <option>Unassigned</option>
                  {uniqueAgents.map(agent => (
                    <option key={agent} value={agent}>{agent}</option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <select
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option value="qualified_date">Sort by Qualified Date</option>
                  <option value="next_followup">Sort by Next Followup</option>
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Deals List Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Deals List</h2>
            <p className="text-sm text-gray-600 mt-1">Qualified leads and their site visit status</p>
          </div>
        </div>

        <div>
          {filteredAndSortedDeals.length === 0 ? (
            <div className="text-center text-gray-500 py-12">
              <CheckCircle className="h-12 w-12 text-gray-300 mx-auto mb-3" />
              <p className="text-lg font-medium mb-1">No deals found</p>
              <p className="text-sm text-gray-400">
                {searchTerm || selectedStatus !== 'Active Site Visits' 
                  ? 'Try adjusting your filters' 
                  : 'Qualified leads will appear here as deals'}
              </p>
            </div>
          ) : (
            <div className="divide-y divide-gray-200">
              {filteredAndSortedDeals.map((deal: any) => {
                // Debug logging for assigned_to data
                if (deal.lead_phone === '9398446636') { // Suresh's number
                  console.log('[DEBUG SURESH] lead_phone:', deal.lead_phone)
                  console.log('[DEBUG SURESH] lead_name:', deal.lead_name)
                  console.log('[DEBUG SURESH] deal.assigned_to:', (deal as any).assigned_to)
                  console.log('[DEBUG SURESH] deal.assigned_to?.name:', (deal as any).assigned_to?.name)
                  console.log('[DEBUG SURESH] deal.assigned_to_id:', (deal as any).assigned_to_id)
                  console.log('[DEBUG SURESH] What will display:', (deal as any).assigned_to?.name || 'Unassigned')
                }
                return (
                <div key={deal.lead_id} className="px-4 sm:px-6 py-4 hover:bg-gray-50 transition-colors">
                  <div className="hidden md:flex items-center justify-between gap-2">
                    <div className="flex items-center space-x-3 flex-1 min-w-0 overflow-hidden">
                      {/* Profile Icon */}
                      <div className="flex-shrink-0">
                        <div className="w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center">
                          <User className="h-5 w-5 text-blue-600" />
                        </div>
                      </div>

                      {/* Name Column */}
                      <div className="min-w-[120px] max-w-[180px] flex-shrink">
                        <h3 className="text-base font-semibold text-gray-900">
                          {deal.lead_name || 'No Name'}
                        </h3>
                        <div className="flex gap-3 mt-1">
                          {deal.lead_preferred_type && (
                            <div className="text-xs text-gray-600">
                              <span className="font-medium">Preferred Unit:</span> {deal.lead_preferred_type}
                            </div>
                          )}
                          {deal.lead_budget_min && (
                            <div className="text-xs text-gray-600">
                              <span className="font-medium">Budget:</span> ₹{deal.lead_budget_min.toLocaleString()}
                            </div>
                          )}
                        </div>
                        {deal.preferred_location && (
                          <div className="text-xs text-gray-500 mt-1">
                            {deal.preferred_location}
                          </div>
                        )}
                      </div>

                      {/* Contact Details Column */}
                      <div className="min-w-[120px] max-w-[160px] flex-shrink">
                        <div className="text-sm text-gray-900">
                          {deal.lead_phone || 'No phone'}
                        </div>
                        <div className="text-xs text-gray-500 truncate">
                          {deal.lead_email || 'No email'}
                        </div>
                        <div className="text-xs text-gray-500 mt-1 truncate">
                          Source: {getDisplaySource(deal.lead_source)}
                          {deal.lead_source === 'Referral' && deal.lead_notes?.includes('Referral Name:') && (
                            <span className="text-blue-600 font-medium">
                              {' • ' + deal.lead_notes.split('Referral Name:')[1]?.split('\n')[0]?.trim()}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Site Visit Status Column */}
                      <div className="min-w-[120px] max-w-[150px] flex-shrink">
                        <div className="text-sm text-gray-600 mb-1">
                          Site Visit: <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${statusColors[deal.site_visit_status as keyof typeof statusColors] || 'bg-gray-100 text-gray-800'}`}>
                            {deal.site_visit_status?.replace('_', ' ') || 'NOT VISITED'}
                          </span>
                        </div>
                        <div className="text-xs text-gray-500 mb-1">
                          Site Visit Date: <span className="text-gray-900 font-medium">
                            {deal.site_visit_date 
                              ? new Date(deal.site_visit_date).toLocaleDateString('en-US', {
                                  month: 'short',
                                  day: 'numeric',
                                  year: 'numeric'
                                })
                              : 'Not set'}
                          </span>
                        </div>
                        <div className="text-sm text-gray-600">
                          Assigned to: <span className="text-gray-900 font-medium">{(deal as any).assigned_to?.name || 'Unassigned'}</span>
                        </div>
                      </div>

                      {/* Dates Column */}
                      <div className="min-w-[120px] max-w-[150px] flex-shrink">
                        <div className="text-xs text-gray-700 mb-1">
                          Qualified Date: <span className="text-gray-600">
                            {(() => {
                              const qualifiedDate = (deal as any).deal_created_at || (deal as any).lead_created_at || (deal as any).created_at
                              return qualifiedDate 
                                ? new Date(qualifiedDate).toLocaleDateString('en-US', { 
                                    month: 'short', 
                                    day: 'numeric',
                                    year: 'numeric' 
                                  })
                                : '-'
                            })()}
                          </span>
                        </div>
                        <div className="text-xs text-gray-700 mb-1">
                          Lead Expiry Date: <span className="text-orange-600 font-medium">
                            {(() => {
                              // Use registered_date if available, otherwise fall back to qualified date
                              const registeredDate = (deal as any).registered_date
                              const qualifiedDate = (deal as any).deal_created_at || (deal as any).lead_created_at || (deal as any).created_at
                              const baseDate = registeredDate || qualifiedDate
                              
                              if (baseDate) {
                                const expiryDate = new Date(baseDate)
                                expiryDate.setDate(expiryDate.getDate() + 60)
                                return expiryDate.toLocaleDateString('en-US', { 
                                  month: 'short', 
                                  day: 'numeric',
                                  year: 'numeric' 
                                })
                              }
                              return '-'
                            })()}
                          </span>
                        </div>
                        <div className="text-xs text-gray-700">
                          Next Followup: <span className="text-gray-900 font-medium">
                            {deal.next_followup_date
                              ? new Date(deal.next_followup_date).toLocaleString('en-US', {
                                  month: 'short',
                                  day: 'numeric',
                                  year: 'numeric',
                                  hour: 'numeric',
                                  minute: '2-digit',
                                  hour12: true
                                })
                              : 'Not set'}
                          </span>
                        </div>
                        <div className="text-xs text-gray-700 mt-1">
                          Registered: <span className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${
                            (deal as any).registered === 'Yes' ? 'bg-green-100 text-green-800' :
                            (deal as any).registered === 'Existing Lead' ? 'bg-blue-100 text-blue-800' :
                            (deal as any).registered === 'Accompany client' ? 'bg-purple-100 text-purple-800' :
                            'bg-red-100 text-red-800'
                          }`}>
                            {(deal as any).registered || 'No'}
                          </span>
                        </div>
                      </div>
                    </div>

                    {/* Actions */}
                    <div className="flex items-center space-x-1 ml-2 flex-shrink-0">
                      {deal.lead_phone && (
                        <>
                          <button
                            onClick={() => handleWhatsAppGroup(deal)}
                            className="p-2 text-purple-600 hover:text-purple-700 hover:bg-purple-50 rounded-lg transition-colors border border-purple-200"
                            title="Send to WhatsApp Group"
                          >
                            <Users className="h-4 w-4" />
                          </button>
                          <a
                            href={`tel:${deal.lead_phone}`}
                            className="p-2 text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded-lg transition-colors border border-blue-200"
                            title="Call"
                          >
                            <Phone className="h-4 w-4" />
                          </a>
                          <a
                            href={`https://wa.me/${deal.lead_phone.replace(/[^0-9]/g, '')}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-2 text-green-600 hover:text-green-700 hover:bg-green-50 rounded-lg transition-colors border border-green-200"
                            title="WhatsApp"
                          >
                            <WhatsAppIcon className="h-4 w-4" />
                          </a>
                        </>
                      )}
                      {deal.lead_email && (
                        <a
                          href={`mailto:${deal.lead_email}`}
                          className="p-2 text-gray-600 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
                          title="Email"
                        >
                          <Mail className="h-4 w-4" />
                        </a>
                      )}
                      <button
                        onClick={() => handleEditDeal(deal)}
                        className="p-2 text-gray-600 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
                        title="Edit"
                      >
                        <Edit2 className="h-4 w-4" />
                      </button>
                    </div>
                  </div>

                  {/* Notes Row */}
                  <div className="mt-3 pt-3 border-t border-gray-100">
                    <div className="flex items-start space-x-2">
                      <button
                        onClick={() => {
                          setSelectedDealNotes(deal)
                          setNotesHistoryType('regular')
                          setShowNotesHistory(true)
                        }}
                        className="p-1 hover:bg-gray-100 rounded transition-colors"
                        title="View notes history"
                      >
                        <MessageSquare className="h-4 w-4 text-gray-500" />
                      </button>
                      <div className="flex-1">
                        <span className="text-xs font-medium text-gray-700">Latest Note: </span>
                        <span className="text-xs text-gray-600">
                          {getLatestNote(deal.lead_notes, deal.lead_created_at) || 'No notes added'}
                        </span>
                        {deal.lead_notes && parseNotes(deal.lead_notes, deal.lead_created_at).length > 1 && (
                          <button
                            onClick={() => {
                              setSelectedDealNotes(deal)
                              setNotesHistoryType('regular')
                              setShowNotesHistory(true)
                            }}
                            className="text-xs text-blue-600 hover:text-blue-700 ml-1 hover:underline"
                          >
                            (+{parseNotes(deal.lead_notes, deal.lead_created_at).length - 1} more)
                          </button>
                        )}
                      </div>
                    </div>

                    {/* Management Notes */}
                    <div className="flex items-start space-x-2 pt-2 border-t border-gray-100">
                      <button
                        onClick={() => {
                          setSelectedDealNotes(deal)
                          setNotesHistoryType('management')
                          setShowNotesHistory(true)
                        }}
                        className="p-1 hover:bg-gray-100 rounded transition-colors"
                        title="View management notes history"
                      >
                        <FileText className="h-4 w-4 text-purple-500" />
                      </button>
                      <div className="flex-1">
                        <span className="text-xs font-medium text-purple-700">Management Note: </span>
                        <span className="text-xs text-gray-600">
                          {getLatestNote((deal as any).management_notes, deal.lead_created_at) || 'No management notes'}
                        </span>
                        {(deal as any).management_notes && parseNotes((deal as any).management_notes, deal.lead_created_at).length > 1 && (
                          <button
                            onClick={() => {
                              setSelectedDealNotes(deal)
                              setNotesHistoryType('management')
                              setShowNotesHistory(true)
                            }}
                            className="text-xs text-purple-600 hover:text-purple-700 ml-1 hover:underline"
                          >
                            (+{parseNotes((deal as any).management_notes, deal.lead_created_at).length - 1} more)
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              )})}
            </div>
          )}
        </div>

        {filteredAndSortedDeals.length > 0 && (
          <div className="px-6 py-3 bg-gray-50 border-t text-sm text-gray-600">
            Showing {filteredAndSortedDeals.length} of {qualifiedLeadsWithDeals.length} deals
          </div>
        )}
      </div>

      {/* Edit Deal Modal */}
      {showEditModal && editingDeal && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {setShowEditModal(false); setEditingDeal(null); setAppendNote('')}}
        >
          <div 
            className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-6 border-b border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900">Update Site Visit Status</h2>
              <button
                onClick={() => {setShowEditModal(false); setEditingDeal(null); setAppendNote('')}}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={handleUpdateDeal} className="p-6">
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Lead Name</label>
                  <input 
                    type="text" 
                    value={editingDeal.lead_name || ''}
                    disabled
                    className="w-full px-3 py-2 bg-gray-50 border border-gray-300 rounded-lg" 
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Site Visit Status</label>
                  <select 
                    value={editingDeal.site_visit_status || 'NOT_VISITED'}
                    onChange={(e) => setEditingDeal({...editingDeal, site_visit_status: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    {siteVisitStatuses.map(status => (
                      <option key={status} value={status}>{status.replace('_', ' ')}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <Calendar className="h-4 w-4" />
                      <span>Site Visit Date</span>
                    </div>
                  </label>
                  <input 
                    type="date" 
                    value={editingDeal.site_visit_date || ''}
                    onChange={(e) => setEditingDeal({...editingDeal, site_visit_date: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Set the date when the site visit is scheduled or was completed.
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <CheckCircle className="h-4 w-4" />
                      <span>Conversion Status</span>
                    </div>
                  </label>
                  <select 
                    value={editingDeal.conversion_status || 'PENDING'}
                    onChange={(e) => setEditingDeal({...editingDeal, conversion_status: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    {conversionStatuses.map(status => (
                      <option key={status} value={status}>{status.replace('_', ' ')}</option>
                    ))}
                  </select>
                  <p className="text-xs text-gray-500 mt-1">
                    Track whether the customer booked a flat after the site visit.
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <Calendar className="h-4 w-4" />
                      <span>Conversion Date</span>
                    </div>
                  </label>
                  <input 
                    type="date" 
                    value={editingDeal.conversion_date || ''}
                    onChange={(e) => setEditingDeal({...editingDeal, conversion_date: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Set the date when the booking decision was made.
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <Calendar className="h-4 w-4" />
                      <span>Next Followup Date & Time</span>
                    </div>
                  </label>
                  <input
                    type="datetime-local"
                    value={editingDeal.next_followup_date || ''}
                    onChange={(e) => setEditingDeal({...editingDeal, next_followup_date: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Set the next date and time to follow up with this lead.
                  </p>
                </div>

                {/* Previous Notes - Read Only */}
                {editingDeal.lead_notes && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <div className="flex items-center space-x-2">
                        <FileText className="h-4 w-4" />
                        <span>Previous Notes History</span>
                      </div>
                    </label>
                    <div className="w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg max-h-32 overflow-y-auto">
                      <pre className="text-sm text-gray-600 whitespace-pre-wrap font-sans">{editingDeal.lead_notes}</pre>
                    </div>
                  </div>
                )}

                {/* Add New Note */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <MessageSquare className="h-4 w-4" />
                      <span>Add New Note</span>
                      <span className="text-xs text-gray-500">(will be appended with timestamp)</span>
                    </div>
                  </label>
                  <textarea 
                    rows={3} 
                    value={appendNote}
                    onChange={(e) => setAppendNote(e.target.value)}
                    placeholder="Enter your new note here..."
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  ></textarea>
                  <p className="text-xs text-gray-500 mt-1">
                    Note: Previous notes cannot be edited or deleted. New notes will be added with a timestamp.
                  </p>
                </div>

                {/* Management Notes Section */}
                <div className="pt-4 border-t border-gray-200">
                  <h3 className="text-sm font-semibold text-purple-700 mb-3 flex items-center space-x-2">
                    <FileText className="h-4 w-4" />
                    <span>Management Notes</span>
                  </h3>
                  
                  {/* Previous Management Notes */}
                  {(editingDeal as any).management_notes && (
                    <div className="mb-4">
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        <div className="flex items-center space-x-2">
                          <FileText className="h-4 w-4" />
                          <span>Previous Management Notes History</span>
                        </div>
                      </label>
                      <div className="w-full px-3 py-2 bg-purple-50 border border-purple-200 rounded-lg max-h-32 overflow-y-auto">
                        <pre className="text-sm text-gray-600 whitespace-pre-wrap font-sans">{(editingDeal as any).management_notes}</pre>
                      </div>
                    </div>
                  )}

                  {/* Append New Management Notes */}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <div className="flex items-center space-x-2">
                        <MessageSquare className="h-4 w-4" />
                        <span>Add New Management Note</span>
                        <span className="text-xs text-gray-500">(will be appended with timestamp)</span>
                      </div>
                    </label>
                    <textarea 
                      rows={3} 
                      value={(editingDeal as any).appendManagementNote || ''}
                      onChange={(e) => setEditingDeal({...editingDeal, appendManagementNote: e.target.value})}
                      placeholder="Enter management guidance note here..."
                      className="w-full px-3 py-2 border border-purple-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent outline-none"
                    ></textarea>
                    <p className="text-xs text-gray-500 mt-1">
                      Management notes help guide CRM users on next steps.
                    </p>
                  </div>
                </div>
              </div>

              <div className="flex justify-end space-x-3 mt-6">
                <button
                  type="button"
                  onClick={() => {setShowEditModal(false); setEditingDeal(null); setAppendNote('')}}
                  className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition"
                >
                  Cancel
                </button>
                <button 
                  type="submit"
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
                >
                  Update Deal
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Notes History Modal */}
      {showNotesHistory && selectedDealNotes && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {
            setShowNotesHistory(false)
            setSelectedDealNotes(null)
          }}
        >
          <div 
            className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-6 border-b border-gray-200">
              <div>
                <h2 className="text-xl font-semibold text-gray-900">
                  {notesHistoryType === 'management' ? 'Management Notes History' : 'Notes History'}
                </h2>
                <p className="text-sm text-gray-600 mt-1">
                  {selectedDealNotes.lead_name || 'Deal'}
                </p>
              </div>
              <button
                onClick={() => {
                  setShowNotesHistory(false)
                  setSelectedDealNotes(null)
                }}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            
            <div className="p-6 max-h-96 overflow-y-auto">
              {(() => {
                const notesToShow = notesHistoryType === 'management' 
                  ? (selectedDealNotes as any).management_notes 
                  : selectedDealNotes.lead_notes
                const parsedNotesToShow = parseNotes(notesToShow, selectedDealNotes.lead_created_at)
                const borderColor = notesHistoryType === 'management' ? 'border-purple-200' : 'border-blue-200'
                const dotColor = notesHistoryType === 'management' ? 'bg-purple-500' : 'bg-blue-500'
                const noNotesIcon = notesHistoryType === 'management' ? FileText : MessageSquare
                const NoNotesIcon = noNotesIcon
                
                return parsedNotesToShow.length === 0 ? (
                  <div className="text-center text-gray-500 py-8">
                    <NoNotesIcon className="h-12 w-12 text-gray-300 mx-auto mb-3" />
                    <p className="text-lg font-medium mb-1">No {notesHistoryType === 'management' ? 'management ' : ''}notes available</p>
                    <p className="text-sm">No {notesHistoryType === 'management' ? 'management ' : ''}notes have been added to this deal yet.</p>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {parsedNotesToShow.map((note, index) => (
                      <div key={index} className={`border-l-4 ${borderColor} pl-4 pb-4`}>
                        <div className="flex items-start justify-between mb-2">
                          <div className="flex items-center space-x-2">
                            <div className={`w-2 h-2 ${dotColor} rounded-full`}></div>
                            <span className="text-sm font-medium text-gray-900">
                              {note?.date ? note.date.toLocaleDateString('en-US', {
                                year: 'numeric',
                                month: 'short',
                                day: 'numeric',
                                hour: '2-digit',
                                minute: '2-digit'
                              }) : 'No date'}
                            </span>
                          </div>
                          {index === 0 && (
                            <span className="bg-green-100 text-green-800 text-xs px-2 py-1 rounded-full">
                              Latest
                            </span>
                          )}
                        </div>
                        <div className="text-sm text-gray-700 leading-relaxed whitespace-pre-wrap ml-4">
                          {note?.content}
                        </div>
                      </div>
                    ))}
                  </div>
                )
              })()}
            </div>
            
            <div className="px-6 py-4 bg-gray-50 border-t border-gray-200">
              <div className="flex justify-between items-center text-xs text-gray-500">
                <span>Total notes: {parseNotes(
                  notesHistoryType === 'management' ? (selectedDealNotes as any).management_notes : selectedDealNotes.lead_notes, 
                  selectedDealNotes.lead_created_at
                ).length}</span>
                <span>Click outside to close</span>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* WhatsApp Group Modal */}
      {showWhatsAppModal && selectedDeal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-md w-full mx-4">
            <div className="px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">Send to WhatsApp Group</h3>
            </div>
            
            <div className="px-6 py-4">
              <div className="mb-4">
                <p className="text-sm text-gray-600 mb-2">Client Details:</p>
                <div className="bg-gray-50 p-3 rounded-md text-sm">
                  <p><strong>Name:</strong> {selectedDeal.lead_name || selectedDeal.name || 'N/A'}</p>
                  <p><strong>Phone:</strong> {selectedDeal.lead_phone || selectedDeal.phone || 'N/A'}</p>
                  <p><strong>Email:</strong> {selectedDeal.lead_email || selectedDeal.email || 'None'}</p>
                </div>
              </div>
              
              <div className="mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Site Visit Date *
                </label>
                <input
                  type="date"
                  value={siteVisitDate}
                  onChange={(e) => setSiteVisitDate(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent outline-none"
                  required
                />
              </div>

              {siteVisitDate && (
                <div className="mb-4">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Message Preview:
                  </label>
                  <div className="bg-gray-50 p-3 rounded-md text-sm border">
                    <pre className="whitespace-pre-wrap font-sans">{`Hi Team, please find Client details below

Client Name: ${selectedDeal.lead_name || selectedDeal.name || 'N/A'}
Phone Number: ${selectedDeal.lead_phone || selectedDeal.phone || 'N/A'}
Email: ${selectedDeal.lead_email || selectedDeal.email || 'None'}
Site Visit Date: ${siteVisitDate}

Regards,
${user?.name || 'Flatrix Team'}`}</pre>
                  </div>
                  <button
                    onClick={async () => {
                      const message = `Hi Team, please find Client details below

Client Name: ${selectedDeal.lead_name || selectedDeal.name || 'N/A'}
Phone Number: ${selectedDeal.lead_phone || selectedDeal.phone || 'N/A'}
Email: ${selectedDeal.lead_email || selectedDeal.email || 'None'}
Site Visit Date: ${siteVisitDate}

Regards,
${user?.name || 'Flatrix Team'}`
                      
                      try {
                        await navigator.clipboard.writeText(message)
                        toast.success('Message copied to clipboard!')
                      } catch (err) {
                        toast.error('Could not copy message')
                      }
                    }}
                    className="mt-2 px-3 py-1 text-xs bg-gray-200 text-gray-700 rounded hover:bg-gray-300 transition-colors"
                  >
                    Copy Message
                  </button>
                </div>
              )}
            </div>
            
            <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 flex justify-end space-x-3">
              <button
                onClick={() => {
                  setShowWhatsAppModal(false)
                  setSiteVisitDate('')
                  setSelectedDeal(null)
                }}
                className="px-4 py-2 text-gray-700 bg-gray-200 rounded-lg hover:bg-gray-300 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={sendToWhatsAppGroup}
                className="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors flex items-center space-x-2"
              >
                <Users className="h-4 w-4" />
                <span>Send to Group</span>
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}