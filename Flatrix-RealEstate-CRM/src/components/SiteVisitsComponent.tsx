'use client'

import { useState, useRef, useEffect } from 'react'
import { useSearchParams } from 'next/navigation'
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
  BarChart3,
  MapPin as SiteIcon,
  Clock,
  History
} from 'lucide-react'
import { useDeals, useLeads } from '@/hooks/useDatabase'
import { supabase } from '@/lib/supabase'
import toast from 'react-hot-toast'
import { useAuth } from '@/contexts/AuthContext'
import * as XLSX from 'xlsx'

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M12.031 2c-5.523 0-10.031 4.507-10.031 10.031 0 1.74.443 3.387 1.288 4.816l-1.288 4.153 4.232-1.245c1.389.761 2.985 1.214 4.661 1.214h.004c5.522 0 10.031-4.507 10.031-10.031 0-2.672-1.041-5.183-2.93-7.071-1.892-1.892-4.405-2.937-7.071-2.937zm.004 18.375h-.003c-1.465 0-2.899-.394-4.148-1.14l-.297-.177-3.085.808.821-2.998-.193-.307c-.821-1.302-1.253-2.807-1.253-4.358 0-4.516 3.677-8.192 8.193-8.192 2.186 0 4.239.852 5.784 2.397 1.545 1.545 2.397 3.598 2.397 5.783-.003 4.518-3.68 8.194-8.196 8.194zm4.5-6.143c-.247-.124-1.463-.722-1.69-.805-.227-.083-.392-.124-.558.124-.166.248-.642.805-.786.969-.145.165-.29.186-.537.062-.247-.124-1.045-.385-1.99-1.227-.736-.657-1.233-1.468-1.378-1.715-.145-.247-.016-.381.109-.504.112-.111.247-.29.371-.434.124-.145.166-.248.248-.413.083-.166.042-.31-.021-.434-.062-.124-.558-1.343-.765-1.839-.201-.479-.407-.414-.558-.422-.145-.007-.31-.009-.476-.009-.166 0-.435.062-.662.31-.227.248-.866.847-.866 2.067 0 1.22.889 2.395 1.013 2.56.124.166 1.75 2.667 4.24 3.74.592.256 1.055.408 1.415.523.594.189 1.135.162 1.563.098.476-.071 1.463-.598 1.669-1.175.207-.577.207-1.071.145-1.175-.062-.104-.227-.165-.476-.289z"/>
  </svg>
)

const siteVisitStatuses = ['NOT_VISITED', 'SCHEDULED', 'COMPLETED', 'RESCHEDULED', 'REVISIT', 'CANCELLED']
const conversionStatuses = ['PENDING', 'BOOKED', 'NOT_BOOKED', 'CANCELLED', 'LOST']

const statusColors = {
  NOT_VISITED: 'bg-gray-100 text-gray-800 border-gray-200',
  SCHEDULED: 'bg-blue-100 text-blue-800 border-blue-200',
  COMPLETED: 'bg-green-100 text-green-800 border-green-200',
  RESCHEDULED: 'bg-orange-100 text-orange-800 border-orange-200',
  REVISIT: 'bg-purple-100 text-purple-800 border-purple-200',
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
    return b.date.getTime() - a.date.getTime()
  })
}

// Helper function to get the latest note
const getLatestNote = (notes: string, leadCreatedAt?: string) => {
  const parsedNotes = parseNotes(notes, leadCreatedAt)
  if (parsedNotes.length === 0) return null
  
  const latest = parsedNotes[0]
  if (!latest) return null
  return latest.content.length > 50 ? latest.content.substring(0, 50) + '...' : latest.content
}

export default function SiteVisitsComponent() {
  const searchParams = useSearchParams()
  const { user } = useAuth()

  // Check if user is admin or super admin
  const isAdminOrSuperAdmin = user?.role === 'ADMIN' || user?.role === 'super_admin'

  const [searchTerm, setSearchTerm] = useState('')
  const [selectedSiteVisitStatus, setSelectedSiteVisitStatus] = useState('All Site Visit Statuses')
  const [selectedConversionStatus, setSelectedConversionStatus] = useState('PENDING')
  const [selectedAssignee, setSelectedAssignee] = useState('All Assigned')
  const [selectedSource, setSelectedSource] = useState('All Sources')
  const [sortBy, setSortBy] = useState('site_visit_date')
  const [showEditModal, setShowEditModal] = useState(false)
  const [editingVisit, setEditingVisit] = useState<any>(null)
  const [showNotesHistory, setShowNotesHistory] = useState(false)
  const [selectedVisitNotes, setSelectedVisitNotes] = useState<any>(null)
  const [notesHistoryType, setNotesHistoryType] = useState<'regular' | 'management'>('regular')
  const [appendNote, setAppendNote] = useState('')
  const [showConversionHistory, setShowConversionHistory] = useState(false)
  const [selectedConversionHistory, setSelectedConversionHistory] = useState<any>(null)
  const [conversionHistoryData, setConversionHistoryData] = useState<any[]>([])
  const [editingAttendedBy, setEditingAttendedBy] = useState<string | null>(null)
  const [attendedByValue, setAttendedByValue] = useState('')
  const [exportFromDate, setExportFromDate] = useState('')
  const [exportToDate, setExportToDate] = useState('')
  const [showExportModal, setShowExportModal] = useState(false)
  const [filterFromDate, setFilterFromDate] = useState('')
  const [filterToDate, setFilterToDate] = useState('')

  const { data: deals, loading: dealsLoading, refetch: refetchDeals } = useDeals()
  const { data: leads, loading: leadsLoading, refetch: refetchLeads } = useLeads()

  // Handle URL parameters for filtering from Activities page
  useEffect(() => {
    const searchParam = searchParams.get('search')
    if (searchParam) {
      setSearchTerm(searchParam)
    }
  }, [searchParams])

  // Function to fetch conversion history for a specific deal
  const fetchConversionHistory = async (dealId: string) => {
    try {
      const { data, error } = await supabase
        .from('flatrix_conversion_history')
        .select('*')
        .eq('deal_id', dealId)
        .order('changed_at', { ascending: false })
      
      if (error) throw error
      return data || []
    } catch (error) {
      console.error('Error fetching conversion history:', error)
      return []
    }
  }

  // Function to handle showing conversion history
  const handleShowConversionHistory = async (visit: any) => {
    if (!visit.deal_id) {
      toast.error('No deal found for this site visit')
      return
    }
    
    const history = await fetchConversionHistory(visit.deal_id)
    setConversionHistoryData(history)
    setSelectedConversionHistory(visit)
    setShowConversionHistory(true)
  }

  // Get all site visits (deals with any site visit activity)
  const siteVisitsData = leads?.filter(lead => lead.status === 'QUALIFIED')?.map(lead => {
    const deal = deals?.find(d => d.lead_id === lead.id)
    return {
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
      lead_project_name: (lead as any)?.project_name,
      site_visit_status: (deal as any)?.site_visit_status || 'NOT_VISITED',
      site_visit_date: (deal as any)?.site_visit_date,
      conversion_status: (deal as any)?.conversion_status || 'PENDING',
      conversion_date: (deal as any)?.conversion_date,
      attended_by: (deal as any)?.attended_by,
      deal_id: deal?.id,
      deal_value: deal?.deal_value || (lead as any)?.budget_max
    }
  }).filter(visit =>
    // Only show visits that have some site visit activity (not just NOT_VISITED)
    visit.site_visit_status && visit.site_visit_status !== 'NOT_VISITED'
  ) || []

  // Calculate stats
  const totalSiteVisits = siteVisitsData.length
  const siteVisitsConverted = siteVisitsData.filter(visit => visit.conversion_status === 'BOOKED').length
  const conversionRate = totalSiteVisits > 0 ? Math.round((siteVisitsConverted / totalSiteVisits) * 100) : 0
  const siteVisitsNotConverted = siteVisitsData.filter(visit => 
    visit.conversion_status === 'NOT_BOOKED' || visit.conversion_status === 'CANCELLED' || visit.conversion_status === 'PENDING'
  ).length

  // Mock percentage changes
  const statsChange = {
    totalVisits: '+18.7%',
    converted: '+25.3%',
    conversionRate: '+6.8%',
    notConverted: '-15.2%'
  }

  // Get unique agents for filter
  const uniqueAgents = Array.from(
    new Set(
      siteVisitsData
        .map(visit => (visit as any).assigned_to?.name)
        .filter(Boolean)
    )
  ).sort()

  // Get unique sources for filter
  const uniqueSources = Array.from(
    new Set(
      siteVisitsData
        .map(visit => getDisplaySource(visit.lead_source || ''))
        .filter(source => source && source !== 'Unknown')
    )
  ).sort()

  const filteredAndSortedVisits = siteVisitsData.filter(visit => {
    if (!visit) return false

    const searchMatches = !searchTerm ||
      visit.lead_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      visit.lead_email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      visit.lead_phone?.includes(searchTerm) ||
      visit.preferred_location?.toLowerCase().includes(searchTerm.toLowerCase())

    const siteVisitStatusMatches = selectedSiteVisitStatus === 'All Site Visit Statuses' || visit.site_visit_status === selectedSiteVisitStatus
    const conversionStatusMatches = selectedConversionStatus === 'All Conversion Statuses' || visit.conversion_status === selectedConversionStatus
    const sourceMatches = selectedSource === 'All Sources' || getDisplaySource(visit.lead_source || '') === selectedSource

    // Agent filter logic
    let assigneeMatches = true
    const visitAssignedTo = (visit as any).assigned_to?.name
    if (selectedAssignee === 'Assigned to Me') {
      assigneeMatches = visit.assigned_to_id === user?.id
    } else if (selectedAssignee === 'Unassigned') {
      assigneeMatches = !visit.assigned_to_id
    } else if (selectedAssignee !== 'All Assigned') {
      // Filter by specific agent name
      assigneeMatches = visitAssignedTo === selectedAssignee
    }

    // Date range filter logic
    let dateMatches = true
    if (filterFromDate || filterToDate) {
      if (!visit.site_visit_date) {
        dateMatches = false
      } else {
        const visitDate = new Date(visit.site_visit_date)

        if (filterFromDate) {
          const fromDate = new Date(filterFromDate)
          fromDate.setHours(0, 0, 0, 0)
          if (visitDate < fromDate) {
            dateMatches = false
          }
        }

        if (filterToDate) {
          const toDate = new Date(filterToDate)
          toDate.setHours(23, 59, 59, 999)
          if (visitDate > toDate) {
            dateMatches = false
          }
        }
      }
    }

    return searchMatches && siteVisitStatusMatches && conversionStatusMatches && sourceMatches && assigneeMatches && dateMatches
  })?.sort((a, b) => {
    switch (sortBy) {
      case 'site_visit_date':
        const aSiteVisitDate = a.site_visit_date ? new Date(a.site_visit_date).getTime() : 0
        const bSiteVisitDate = b.site_visit_date ? new Date(b.site_visit_date).getTime() : 0
        return bSiteVisitDate - aSiteVisitDate
      case 'next_followup':
        const aFollowupDate = a.next_followup_date ? new Date(a.next_followup_date).getTime() : 0
        const bFollowupDate = b.next_followup_date ? new Date(b.next_followup_date).getTime() : 0
        return bFollowupDate - aFollowupDate
      default:
        const aDefaultDate = a.site_visit_date ? new Date(a.site_visit_date).getTime() : 0
        const bDefaultDate = b.site_visit_date ? new Date(b.site_visit_date).getTime() : 0
        return bDefaultDate - aDefaultDate
    }
  })

  const handleEditVisit = (visit: any) => {
    setEditingVisit({
      ...visit,
      site_visit_date: visit.site_visit_date
        ? new Date(visit.site_visit_date).toISOString().slice(0, 16)
        : '',
      next_followup_date: visit.next_followup_date
        ? new Date(visit.next_followup_date).toISOString().slice(0, 16)
        : '',
      conversion_date: visit.conversion_date ? visit.conversion_date.split('T')[0] : ''
    })
    setAppendNote('')
    setShowEditModal(true)
  }

  const handleAttendedByEdit = (visitId: string, currentValue: string) => {
    setEditingAttendedBy(visitId)
    setAttendedByValue(currentValue || '')
  }

  const handleAttendedBySave = async (dealId: string) => {
    if (!dealId) {
      toast.error('No deal found for this site visit')
      return
    }

    try {
      const { error } = await supabase
        .from('flatrix_deals')
        .update({
          attended_by: attendedByValue.trim() || null,
          updated_at: new Date().toISOString()
        })
        .eq('id', dealId)

      if (error) throw error

      toast.success('Attended by updated successfully!')
      setEditingAttendedBy(null)
      setAttendedByValue('')
      refetchDeals()
    } catch (error) {
      console.error('Error updating attended by:', error)
      toast.error('Failed to update attended by')
    }
  }

  const handleAttendedByCancel = () => {
    setEditingAttendedBy(null)
    setAttendedByValue('')
  }

  const handleExportSiteVisits = () => {
    if (!exportFromDate || !exportToDate) {
      toast.error('Please select both from and to dates')
      return
    }

    // Filter site visits by date range
    const fromDate = new Date(exportFromDate)
    const toDate = new Date(exportToDate)
    toDate.setHours(23, 59, 59, 999) // Include the entire end date

    const visitsInRange = siteVisitsData.filter(visit => {
      if (!visit.site_visit_date) return false
      const visitDate = new Date(visit.site_visit_date)
      return visitDate >= fromDate && visitDate <= toDate
    })

    if (visitsInRange.length === 0) {
      toast.error('No site visits found in the selected date range')
      return
    }

    // Prepare data for Excel export
    const exportData = visitsInRange.map(visit => ({
      'Name': visit.lead_name || 'Unknown',
      'Phone Number': visit.lead_phone || 'N/A',
      'Project': visit.lead_project_name || 'N/A',
      'Date & Time of Site Visit': visit.site_visit_date
        ? new Date(visit.site_visit_date).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            hour12: true
          })
        : 'Not set',
      'Site Visit Status': visit.site_visit_status?.replace('_', ' ') || 'NOT VISITED',
      'Conversion Status': visit.conversion_status?.replace('_', ' ') || 'PENDING',
      'Attended By': visit.attended_by || 'N/A',
      'Assigned To': (visit as any).assigned_to?.name || 'Unassigned'
    }))

    // Create workbook and worksheet
    const ws = XLSX.utils.json_to_sheet(exportData)
    const wb = XLSX.utils.book_new()
    XLSX.utils.book_append_sheet(wb, ws, 'Site Visits')

    // Generate filename with date range
    const fromDateStr = fromDate.toLocaleDateString('en-US').replace(/\//g, '-')
    const toDateStr = toDate.toLocaleDateString('en-US').replace(/\//g, '-')
    const filename = `Site_Visits_${fromDateStr}_to_${toDateStr}.xlsx`

    // Download file
    XLSX.writeFile(wb, filename)

    toast.success(`Exported ${visitsInRange.length} site visits to Excel`)
    setShowExportModal(false)
    setExportFromDate('')
    setExportToDate('')
  }

  const handleUpdateVisit = async (e: React.FormEvent) => {
    e.preventDefault()
    
    try {
      let updatedNotes = editingVisit.lead_notes || ''
      
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
      let updatedManagementNotes = (editingVisit as any).management_notes || ''
      if ((editingVisit as any).appendManagementNote?.trim()) {
        const timestamp = new Date().toLocaleString('en-US', { 
          dateStyle: 'short', 
          timeStyle: 'short' 
        })
        updatedManagementNotes = updatedManagementNotes 
          ? `${updatedManagementNotes}\n\n[${timestamp}]:\n${(editingVisit as any).appendManagementNote.trim()}`
          : `[${timestamp}]:\n${(editingVisit as any).appendManagementNote.trim()}`
      }

      // Update the lead notes and next_followup_date
      const { error: leadError } = await supabase
        .from('flatrix_leads')
        .update({
          notes: updatedNotes,
          management_notes: updatedManagementNotes,
          next_followup_date: editingVisit.next_followup_date
            ? new Date(editingVisit.next_followup_date).toISOString()
            : null
        })
        .eq('id', editingVisit.lead_id)

      if (leadError) throw leadError

      // Update deal record with site visit status
      if (editingVisit.deal_id) {
        const { error: dealError } = await supabase
          .from('flatrix_deals')
          .update({
            site_visit_status: editingVisit.site_visit_status,
            site_visit_date: editingVisit.site_visit_date
              ? new Date(editingVisit.site_visit_date).toISOString()
              : null,
            conversion_status: editingVisit.conversion_status,
            conversion_date: editingVisit.conversion_date || null,
            attended_by: editingVisit.attended_by?.trim() || null,
            updated_at: new Date().toISOString()
          })
          .eq('id', editingVisit.deal_id)

        if (dealError) throw dealError
      }

      toast.success('Site visit updated successfully!')
      setShowEditModal(false)
      setEditingVisit(null)
      setAppendNote('')
      refetchDeals()
      refetchLeads()
    } catch (error) {
      console.error('Error updating site visit:', error)
      toast.error('Failed to update site visit')
    }
  }

  if (dealsLoading || leadsLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-lg text-gray-600">Loading site visits...</div>
      </div>
    )
  }

  return (
    <div>
      <div className="mb-6 flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Site Visits Management</h1>
          <p className="text-gray-600 mt-2">Track and manage all scheduled and completed site visits</p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => setShowExportModal(true)}
            className="flex items-center space-x-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition"
          >
            <Download className="h-4 w-4" />
            <span>Export Site Visits</span>
          </button>
          <button className="flex items-center space-x-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition">
            <Upload className="h-4 w-4" />
            <span>Import Data</span>
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {/* Total Site Visits */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Total Site Visits</p>
              <h3 className="text-3xl font-bold text-gray-900">{totalSiteVisits}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.totalVisits}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-blue-100 p-3 rounded-lg">
              <SiteIcon className="h-6 w-6 text-blue-600" />
            </div>
          </div>
        </div>

        {/* Site Visits Converted */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Site Visits Converted</p>
              <h3 className="text-3xl font-bold text-gray-900">{siteVisitsConverted}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.converted}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-green-100 p-3 rounded-lg">
              <CheckCircle className="h-6 w-6 text-green-600" />
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

        {/* Site Visits Not Converted */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Not Converted</p>
              <h3 className="text-3xl font-bold text-gray-900">{siteVisitsNotConverted}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-red-600 mr-1 rotate-180" />
                <span className="text-sm text-red-600 font-medium">{statsChange.notConverted}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-orange-100 p-3 rounded-lg">
              <Clock className="h-6 w-6 text-orange-600" />
            </div>
          </div>
        </div>
      </div>

      {/* Search and Filters Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 mb-6">
        <div className="p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Site Visit Search & Filters</h2>
          <p className="text-sm text-gray-600 mb-4">Filter and search through your site visits</p>
          
          <div className="flex flex-col gap-4">
            {/* Date Range Filter */}
            <div className="flex flex-col sm:flex-row gap-3 items-end">
              <div className="flex-1">
                <label className="block text-sm font-medium text-gray-700 mb-1">From Date</label>
                <input
                  type="date"
                  value={filterFromDate}
                  onChange={(e) => setFilterFromDate(e.target.value)}
                  className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                />
              </div>
              <div className="flex-1">
                <label className="block text-sm font-medium text-gray-700 mb-1">To Date</label>
                <input
                  type="date"
                  value={filterToDate}
                  onChange={(e) => setFilterToDate(e.target.value)}
                  className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                />
              </div>
              {(filterFromDate || filterToDate) && (
                <button
                  onClick={() => {
                    setFilterFromDate('')
                    setFilterToDate('')
                  }}
                  className="px-4 py-2.5 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors font-medium flex items-center space-x-2"
                >
                  <X className="h-4 w-4" />
                  <span>Clear Dates</span>
                </button>
              )}
            </div>

            <div className="w-full">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search site visits by name, contact, or city..."
                  className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>
            </div>

            <div className="flex flex-col sm:flex-row gap-3">
              <div className="relative flex-1">
                <select
                  value={selectedSiteVisitStatus}
                  onChange={(e) => setSelectedSiteVisitStatus(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option>All Site Visit Statuses</option>
                  {siteVisitStatuses.map(status => (
                    <option key={status} value={status}>{status.replace('_', ' ')}</option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <select
                  value={selectedConversionStatus}
                  onChange={(e) => setSelectedConversionStatus(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option>All Conversion Statuses</option>
                  {conversionStatuses.map(status => (
                    <option key={status} value={status}>{status.replace('_', ' ')}</option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <select
                  value={selectedSource}
                  onChange={(e) => setSelectedSource(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option>All Sources</option>
                  {uniqueSources.map(source => (
                    <option key={source} value={source}>{source}</option>
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
                  <option value="site_visit_date">Sort by Site Visit Date</option>
                  <option value="next_followup">Sort by Next Followup</option>
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Site Visits List Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Site Visits List</h2>
            <p className="text-sm text-gray-600 mt-1">All scheduled and completed site visits</p>
          </div>
        </div>

        <div>
          {filteredAndSortedVisits.length === 0 ? (
            <div className="text-center text-gray-500 py-12">
              <Car className="h-12 w-12 text-gray-300 mx-auto mb-3" />
              <p className="text-lg font-medium mb-1">No site visits found</p>
              <p className="text-sm text-gray-400">
                {searchTerm || selectedSiteVisitStatus !== 'All Site Visit Statuses' || selectedConversionStatus !== 'All Conversion Statuses' || filterFromDate || filterToDate
                  ? 'Try adjusting your filters'
                  : 'Site visits will appear here when scheduled'}
              </p>
            </div>
          ) : (
            <div className="divide-y divide-gray-200">
              {filteredAndSortedVisits.map((visit: any) => (
                <div key={visit.lead_id} className="px-4 sm:px-6 py-4 hover:bg-gray-50 transition-colors">
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
                          {visit.lead_name || 'No Name'}
                        </h3>
                        {visit.lead_project_name && (
                          <div className="text-xs text-blue-600 font-medium mt-0.5 flex items-center gap-1">
                            <Building className="h-3 w-3" />
                            {visit.lead_project_name}
                          </div>
                        )}
                        <div className="flex gap-3 mt-1">
                          {visit.lead_preferred_type && (
                            <div className="text-xs text-gray-600">
                              <span className="font-medium">Preferred Unit:</span> {visit.lead_preferred_type}
                            </div>
                          )}
                          {visit.lead_budget_min && (
                            <div className="text-xs text-gray-600">
                              <span className="font-medium">Budget:</span> ₹{visit.lead_budget_min.toLocaleString()}
                            </div>
                          )}
                        </div>
                        {visit.preferred_location && (
                          <div className="text-xs text-gray-500 mt-1">
                            {visit.preferred_location}
                          </div>
                        )}
                      </div>

                      {/* Contact Details Column */}
                      <div className="min-w-[120px] max-w-[160px] flex-shrink">
                        <div className="text-sm text-gray-900">
                          {visit.lead_phone || 'No phone'}
                        </div>
                        <div className="text-xs text-gray-500 truncate">
                          {visit.lead_email || 'No email'}
                        </div>
                        <div className="text-xs text-gray-500 mt-1 truncate">
                          Source: {getDisplaySource(visit.lead_source)}
                          {visit.lead_source === 'Referral' && visit.lead_notes?.includes('Referral Name:') && (
                            <span className="text-blue-600 font-medium">
                              {' • ' + visit.lead_notes.split('Referral Name:')[1]?.split('\n')[0]?.trim()}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Site Visit Status Column */}
                      <div className="min-w-[120px] max-w-[150px] flex-shrink">
                        <div className="text-sm text-gray-600 mb-1">
                          Site Visit: <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${statusColors[visit.site_visit_status as keyof typeof statusColors] || 'bg-gray-100 text-gray-800'}`}>
                            {visit.site_visit_status?.replace('_', ' ') || 'NOT VISITED'}
                          </span>
                        </div>
                        <div className="text-xs text-gray-500 mb-1">
                          Site Visit Date: <span className="text-gray-900 font-medium">
                            {visit.site_visit_date
                              ? new Date(visit.site_visit_date).toLocaleString('en-US', {
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
                        <div className="text-sm text-gray-600">
                          Assigned to: <span className="text-gray-900 font-medium">{(visit as any).lead?.assigned_to?.name || 'Unassigned'}</span>
                        </div>
                      </div>

                      {/* Dates Column */}
                      <div className="min-w-[120px] max-w-[150px] flex-shrink">
                        <div className="text-xs text-gray-700 mb-1">
                          Conversion: <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${conversionColors[visit.conversion_status as keyof typeof conversionColors] || 'bg-gray-100 text-gray-800'}`}>
                            {visit.conversion_status?.replace('_', ' ') || 'PENDING'}
                          </span>
                        </div>
                        {visit.conversion_status === 'BOOKED' && visit.conversion_date && (
                          <div className="text-xs text-gray-500">
                            Booked on: <span className="text-gray-900 font-medium">
                              {new Date(visit.conversion_date).toLocaleDateString('en-US', {
                                month: 'short',
                                day: 'numeric',
                                year: 'numeric'
                              })}
                            </span>
                          </div>
                        )}
                        <div className="text-xs text-gray-700">
                          Next Followup: <span className="text-gray-900 font-medium">
                            {visit.next_followup_date
                              ? new Date(visit.next_followup_date).toLocaleString('en-US', {
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
                          <div className="flex items-center gap-2">
                            <span>Attended by:</span>
                            {editingAttendedBy === visit.lead_id ? (
                              <div className="flex items-center gap-1">
                                <input
                                  type="text"
                                  value={attendedByValue}
                                  onChange={(e) => setAttendedByValue(e.target.value)}
                                  className="px-2 py-0.5 text-xs border border-blue-300 rounded focus:ring-1 focus:ring-blue-500 focus:border-transparent outline-none"
                                  placeholder="Enter name"
                                  autoFocus
                                  onKeyDown={(e) => {
                                    if (e.key === 'Enter') {
                                      handleAttendedBySave(visit.deal_id)
                                    } else if (e.key === 'Escape') {
                                      handleAttendedByCancel()
                                    }
                                  }}
                                />
                                <button
                                  onClick={() => handleAttendedBySave(visit.deal_id)}
                                  className="p-1 text-green-600 hover:text-green-700 hover:bg-green-50 rounded transition-colors"
                                  title="Save"
                                >
                                  <CheckCircle className="h-3 w-3" />
                                </button>
                                <button
                                  onClick={handleAttendedByCancel}
                                  className="p-1 text-red-600 hover:text-red-700 hover:bg-red-50 rounded transition-colors"
                                  title="Cancel"
                                >
                                  <X className="h-3 w-3" />
                                </button>
                              </div>
                            ) : (
                              <div className="flex items-center gap-1">
                                <span className="text-gray-900 font-medium">
                                  {visit.attended_by || 'Not set'}
                                </span>
                                <button
                                  onClick={() => handleAttendedByEdit(visit.lead_id, visit.attended_by)}
                                  className="p-0.5 text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded transition-colors"
                                  title="Edit attended by"
                                >
                                  <Edit2 className="h-3 w-3" />
                                </button>
                              </div>
                            )}
                          </div>
                        </div>
                        <div className="text-xs text-gray-700 mt-1">
                          Registered: <span className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${
                            (visit as any).registered === 'Yes' ? 'bg-green-100 text-green-800' :
                            (visit as any).registered === 'Existing Lead' ? 'bg-blue-100 text-blue-800' :
                            (visit as any).registered === 'Accompany client' ? 'bg-purple-100 text-purple-800' :
                            'bg-red-100 text-red-800'
                          }`}>
                            {(visit as any).registered || 'No'}
                          </span>
                        </div>
                      </div>
                    </div>

                    {/* Actions */}
                    <div className="flex items-center space-x-1 ml-2 flex-shrink-0">
                      <button
                        onClick={() => handleShowConversionHistory(visit)}
                        className="p-2 text-purple-600 hover:text-purple-700 hover:bg-purple-50 rounded-lg transition-colors border border-purple-200"
                        title="View conversion history"
                      >
                        <History className="h-4 w-4" />
                      </button>
                      {visit.lead_phone && (
                        <>
                          <a
                            href={`tel:${visit.lead_phone}`}
                            className="p-2 text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded-lg transition-colors border border-blue-200"
                            title="Call"
                          >
                            <Phone className="h-4 w-4" />
                          </a>
                          <a
                            href={`https://wa.me/${visit.lead_phone.replace(/[^0-9]/g, '')}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-2 text-green-600 hover:text-green-700 hover:bg-green-50 rounded-lg transition-colors border border-green-200"
                            title="WhatsApp"
                          >
                            <WhatsAppIcon className="h-4 w-4" />
                          </a>
                        </>
                      )}
                      {visit.lead_email && (
                        <a
                          href={`mailto:${visit.lead_email}`}
                          className="p-2 text-gray-600 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
                          title="Email"
                        >
                          <Mail className="h-4 w-4" />
                        </a>
                      )}
                      <button
                        onClick={() => handleEditVisit(visit)}
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
                          setSelectedVisitNotes(visit)
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
                          {getLatestNote(visit.lead_notes, visit.lead_created_at) || 'No notes added'}
                        </span>
                        {visit.lead_notes && parseNotes(visit.lead_notes, visit.lead_created_at).length > 1 && (
                          <button
                            onClick={() => {
                              setSelectedVisitNotes(visit)
                              setNotesHistoryType('regular')
                              setShowNotesHistory(true)
                            }}
                            className="text-xs text-blue-600 hover:text-blue-700 ml-1 hover:underline"
                          >
                            (+{parseNotes(visit.lead_notes, visit.lead_created_at).length - 1} more)
                          </button>
                        )}
                      </div>
                    </div>

                    {/* Management Notes */}
                    <div className="flex items-start space-x-2 pt-2 border-t border-gray-100">
                      <button
                        onClick={() => {
                          setSelectedVisitNotes(visit)
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
                          {getLatestNote((visit as any).management_notes, visit.lead_created_at) || 'No management notes'}
                        </span>
                        {(visit as any).management_notes && parseNotes((visit as any).management_notes, visit.lead_created_at).length > 1 && (
                          <button
                            onClick={() => {
                              setSelectedVisitNotes(visit)
                              setNotesHistoryType('management')
                              setShowNotesHistory(true)
                            }}
                            className="text-xs text-purple-600 hover:text-purple-700 ml-1 hover:underline"
                          >
                            (+{parseNotes((visit as any).management_notes, visit.lead_created_at).length - 1} more)
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {filteredAndSortedVisits.length > 0 && (
          <div className="px-6 py-3 bg-gray-50 border-t text-sm text-gray-600">
            Showing {filteredAndSortedVisits.length} of {siteVisitsData.length} site visits
          </div>
        )}
      </div>

      {/* Edit Site Visit Modal */}
      {showEditModal && editingVisit && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {setShowEditModal(false); setEditingVisit(null); setAppendNote('')}}
        >
          <div 
            className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-6 border-b border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900">Update Site Visit</h2>
              <button
                onClick={() => {setShowEditModal(false); setEditingVisit(null); setAppendNote('')}}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={handleUpdateVisit} className="p-6">
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Lead Name</label>
                  <input 
                    type="text" 
                    value={editingVisit.lead_name || ''}
                    disabled
                    className="w-full px-3 py-2 bg-gray-50 border border-gray-300 rounded-lg" 
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Site Visit Status</label>
                  <select 
                    value={editingVisit.site_visit_status || 'SCHEDULED'}
                    onChange={(e) => setEditingVisit({...editingVisit, site_visit_status: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    {siteVisitStatuses.filter(status => status !== 'NOT_VISITED').map(status => (
                      <option key={status} value={status}>{status.replace('_', ' ')}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <Calendar className="h-4 w-4" />
                      <span>Site Visit Date & Time</span>
                    </div>
                  </label>
                  <input
                    type="datetime-local"
                    value={editingVisit.site_visit_date || ''}
                    onChange={(e) => setEditingVisit({...editingVisit, site_visit_date: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Set the date and time when the site visit is scheduled or was completed.
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
                    value={editingVisit.next_followup_date || ''}
                    onChange={(e) => setEditingVisit({...editingVisit, next_followup_date: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Set the next date and time to follow up with this lead.
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <UserCheck className="h-4 w-4" />
                      <span>Attended By</span>
                    </div>
                  </label>
                  <input
                    type="text"
                    value={editingVisit.attended_by || ''}
                    onChange={(e) => setEditingVisit({...editingVisit, attended_by: e.target.value})}
                    placeholder="Enter name of person who attended"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Name of the person who attended the site visit with the customer.
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
                    value={editingVisit.conversion_status || 'PENDING'}
                    onChange={(e) => setEditingVisit({...editingVisit, conversion_status: e.target.value})}
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
                    value={editingVisit.conversion_date || ''}
                    onChange={(e) => setEditingVisit({...editingVisit, conversion_date: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Set the date when the booking decision was made.
                  </p>
                </div>

                {/* Previous Notes - Read Only */}
                {editingVisit.lead_notes && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <div className="flex items-center space-x-2">
                        <FileText className="h-4 w-4" />
                        <span>Previous Notes History</span>
                      </div>
                    </label>
                    <div className="w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg max-h-32 overflow-y-auto">
                      <pre className="text-sm text-gray-600 whitespace-pre-wrap font-sans">{editingVisit.lead_notes}</pre>
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
                  {(editingVisit as any).management_notes && (
                    <div className="mb-4">
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        <div className="flex items-center space-x-2">
                          <FileText className="h-4 w-4" />
                          <span>Previous Management Notes History</span>
                        </div>
                      </label>
                      <div className="w-full px-3 py-2 bg-purple-50 border border-purple-200 rounded-lg max-h-32 overflow-y-auto">
                        <pre className="text-sm text-gray-600 whitespace-pre-wrap font-sans">{(editingVisit as any).management_notes}</pre>
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
                      value={(editingVisit as any).appendManagementNote || ''}
                      onChange={(e) => setEditingVisit({...editingVisit, appendManagementNote: e.target.value})}
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
                  onClick={() => {setShowEditModal(false); setEditingVisit(null); setAppendNote('')}}
                  className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition"
                >
                  Cancel
                </button>
                <button 
                  type="submit"
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
                >
                  Update Site Visit
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Notes History Modal */}
      {showNotesHistory && selectedVisitNotes && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {
            setShowNotesHistory(false)
            setSelectedVisitNotes(null)
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
                  {selectedVisitNotes.lead_name || 'Site Visit'}
                </p>
              </div>
              <button
                onClick={() => {
                  setShowNotesHistory(false)
                  setSelectedVisitNotes(null)
                }}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            
            <div className="p-6 max-h-96 overflow-y-auto">
              {(() => {
                const notesToShow = notesHistoryType === 'management' 
                  ? (selectedVisitNotes as any).management_notes 
                  : selectedVisitNotes.lead_notes
                const parsedNotesToShow = parseNotes(notesToShow, selectedVisitNotes.lead_created_at)
                const borderColor = notesHistoryType === 'management' ? 'border-purple-200' : 'border-blue-200'
                const dotColor = notesHistoryType === 'management' ? 'bg-purple-500' : 'bg-blue-500'
                const noNotesIcon = notesHistoryType === 'management' ? FileText : MessageSquare
                const NoNotesIcon = noNotesIcon
                
                return parsedNotesToShow.length === 0 ? (
                  <div className="text-center text-gray-500 py-8">
                    <NoNotesIcon className="h-12 w-12 text-gray-300 mx-auto mb-3" />
                    <p className="text-lg font-medium mb-1">No {notesHistoryType === 'management' ? 'management ' : ''}notes available</p>
                    <p className="text-sm">No {notesHistoryType === 'management' ? 'management ' : ''}notes have been added to this site visit yet.</p>
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
                  notesHistoryType === 'management' ? (selectedVisitNotes as any).management_notes : selectedVisitNotes.lead_notes, 
                  selectedVisitNotes.lead_created_at
                ).length}</span>
                <span>Click outside to close</span>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Conversion History Modal */}
      {showConversionHistory && selectedConversionHistory && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {
            setShowConversionHistory(false)
            setSelectedConversionHistory(null)
            setConversionHistoryData([])
          }}
        >
          <div 
            className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-6 border-b border-gray-200">
              <div>
                <h2 className="text-xl font-semibold text-gray-900">Conversion Status History</h2>
                <p className="text-sm text-gray-600 mt-1">
                  {selectedConversionHistory.lead_name || 'Site Visit'} - Status Change Timeline
                </p>
              </div>
              <button
                onClick={() => {
                  setShowConversionHistory(false)
                  setSelectedConversionHistory(null)
                  setConversionHistoryData([])
                }}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            
            <div className="p-6 max-h-96 overflow-y-auto">
              {conversionHistoryData.length === 0 ? (
                <div className="text-center text-gray-500 py-8">
                  <History className="h-12 w-12 text-gray-300 mx-auto mb-3" />
                  <p className="text-lg font-medium mb-1">No conversion history available</p>
                  <p className="text-sm">No status changes have been recorded for this conversion yet.</p>
                </div>
              ) : (
                <div className="space-y-4">
                  {conversionHistoryData.map((record, index) => (
                    <div key={record.id} className="border-l-4 border-purple-200 pl-4 pb-4">
                      <div className="flex items-start justify-between mb-2">
                        <div className="flex items-center space-x-2">
                          <div className="w-2 h-2 bg-purple-500 rounded-full"></div>
                          <span className="text-sm font-medium text-gray-900">
                            {new Date(record.changed_at).toLocaleDateString('en-US', {
                              year: 'numeric',
                              month: 'short',
                              day: 'numeric',
                              hour: '2-digit',
                              minute: '2-digit'
                            })}
                          </span>
                        </div>
                        {index === 0 && (
                          <span className="bg-green-100 text-green-800 text-xs px-2 py-1 rounded-full">
                            Latest
                          </span>
                        )}
                      </div>
                      <div className="ml-4">
                        <div className="flex items-center space-x-2 mb-1">
                          {record.previous_status ? (
                            <>
                              <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${
                                record.previous_status === 'PENDING' ? 'bg-yellow-100 text-yellow-800' :
                                record.previous_status === 'BOOKED' ? 'bg-green-100 text-green-800' :
                                record.previous_status === 'NOT_BOOKED' ? 'bg-red-100 text-red-800' :
                                record.previous_status === 'CANCELLED' ? 'bg-gray-100 text-gray-800' :
                                'bg-gray-100 text-gray-800'
                              }`}>
                                {record.previous_status.replace('_', ' ')}
                              </span>
                              <span className="text-gray-400">→</span>
                            </>
                          ) : (
                            <span className="text-xs text-gray-500">Initial Status:</span>
                          )}
                          <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${
                            record.new_status === 'PENDING' ? 'bg-yellow-100 text-yellow-800' :
                            record.new_status === 'BOOKED' ? 'bg-green-100 text-green-800' :
                            record.new_status === 'NOT_BOOKED' ? 'bg-red-100 text-red-800' :
                            record.new_status === 'CANCELLED' ? 'bg-gray-100 text-gray-800' :
                            'bg-gray-100 text-gray-800'
                          }`}>
                            {record.new_status.replace('_', ' ')}
                          </span>
                        </div>
                        {record.notes && (
                          <p className="text-sm text-gray-600 mb-1">{record.notes}</p>
                        )}
                        {record.conversion_date && record.new_status === 'BOOKED' && (
                          <p className="text-xs text-gray-500">
                            Conversion Date: {new Date(record.conversion_date).toLocaleDateString('en-US', {
                              year: 'numeric',
                              month: 'short',
                              day: 'numeric'
                            })}
                          </p>
                        )}
                        <p className="text-xs text-gray-500">
                          Changed by: {record.changed_by}
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
            
            <div className="px-6 py-4 bg-gray-50 border-t border-gray-200">
              <div className="flex justify-between items-center text-xs text-gray-500">
                <span>Total changes: {conversionHistoryData.length}</span>
                <span>Click outside to close</span>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Export Site Visits Modal */}
      {showExportModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-md w-full mx-4">
            <div className="px-6 py-4 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold text-gray-900">Export Site Visits</h3>
                <button
                  onClick={() => {
                    setShowExportModal(false)
                    setExportFromDate('')
                    setExportToDate('')
                  }}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>
            </div>

            <div className="p-6">
              <p className="text-sm text-gray-600 mb-4">
                Select the date range to export site visits. The Excel file will include Name, Phone Number, Date & Time of Site Visit, and other details.
              </p>

              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    From Date
                  </label>
                  <input
                    type="date"
                    value={exportFromDate}
                    onChange={(e) => setExportFromDate(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    To Date
                  </label>
                  <input
                    type="date"
                    value={exportToDate}
                    onChange={(e) => setExportToDate(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                </div>
              </div>
            </div>

            <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 flex justify-end space-x-3">
              <button
                onClick={() => {
                  setShowExportModal(false)
                  setExportFromDate('')
                  setExportToDate('')
                }}
                className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-100 transition"
              >
                Cancel
              </button>
              <button
                onClick={handleExportSiteVisits}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition flex items-center space-x-2"
              >
                <Download className="h-4 w-4" />
                <span>Export to Excel</span>
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}