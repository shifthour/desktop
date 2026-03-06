'use client'

import { useState, useRef, useEffect } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { 
  Search, 
  Plus, 
  Filter, 
  Phone, 
  Mail, 
  Calendar,
  MoreVertical,
  User,
  MapPin,
  DollarSign,
  Tag,
  Clock,
  ChevronDown,
  X,
  Edit2,
  Trash2,
  Building,
  UserCheck,
  Users,
  TrendingUp,
  CheckCircle,
  Download,
  Upload,
  FileSpreadsheet,
  MessageSquare,
  ChevronRight,
  FileText
} from 'lucide-react'
import { useLeads } from '@/hooks/useDatabase'
import { supabase } from '@/lib/supabase'
import toast from 'react-hot-toast'
import { useAuth } from '@/contexts/AuthContext'

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M12.031 2c-5.523 0-10.031 4.507-10.031 10.031 0 1.74.443 3.387 1.288 4.816l-1.288 4.153 4.232-1.245c1.389.761 2.985 1.214 4.661 1.214h.004c5.522 0 10.031-4.507 10.031-10.031 0-2.672-1.041-5.183-2.93-7.071-1.892-1.892-4.405-2.937-7.071-2.937zm.004 18.375h-.003c-1.465 0-2.899-.394-4.148-1.14l-.297-.177-3.085.808.821-2.998-.193-.307c-.821-1.302-1.253-2.807-1.253-4.358 0-4.516 3.677-8.192 8.193-8.192 2.186 0 4.239.852 5.784 2.397 1.545 1.545 2.397 3.598 2.397 5.783-.003 4.518-3.68 8.194-8.196 8.194zm4.5-6.143c-.247-.124-1.463-.722-1.69-.805-.227-.083-.392-.124-.558.124-.166.248-.642.805-.786.969-.145.165-.29.186-.537.062-.247-.124-1.045-.385-1.99-1.227-.736-.657-1.233-1.468-1.378-1.715-.145-.247-.016-.381.109-.504.112-.111.247-.29.371-.434.124-.145.166-.248.248-.413.083-.166.042-.31-.021-.434-.062-.124-.558-1.343-.765-1.839-.201-.479-.407-.414-.558-.422-.145-.007-.31-.009-.476-.009-.166 0-.435.062-.662.31-.227.248-.866.847-.866 2.067 0 1.22.889 2.395 1.013 2.56.124.166 1.75 2.667 4.24 3.74.592.256 1.055.408 1.415.523.594.189 1.135.162 1.563.098.476-.071 1.463-.598 1.669-1.175.207-.577.207-1.071.145-1.175-.062-.104-.227-.165-.476-.289z"/>
  </svg>
)

const leadStatuses = ['NEW', 'CONTACTED', 'QUALIFIED', 'LOST']
const leadSources = ['99acres', 'Direct Call', 'Facebook', 'Google Ads', 'Housing.com', 'Instagram', 'Magic Bricks', 'Referral', 'Walk-in', 'WhatsApp'].sort()

// Display labels for status (user-friendly names)
const statusDisplayLabels = {
  NEW: 'New',
  CONTACTED: 'Contacted',
  QUALIFIED: 'Qualified',
  LOST: 'Disqualified'
}

const statusColors = {
  NEW: 'bg-blue-100 text-blue-800 border-blue-200',
  CONTACTED: 'bg-yellow-100 text-yellow-800 border-yellow-200',
  QUALIFIED: 'bg-green-100 text-green-800 border-green-200',
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

// Project name normalization and mapping
const projectNameMapping: { [key: string]: string } = {
  // Add your project variations here - map variations to canonical name
  // Example: 'ryth of rains': 'Rhythm of Rain',
  // Example: 'rythm of rain': 'Rhythm of Rain',
  // Add more mappings as you discover variations
}

// Helper function to normalize project names
const normalizeProjectName = (projectName: string | null | undefined): string => {
  if (!projectName) return ''

  // Convert to lowercase and trim for comparison
  const normalized = projectName.toLowerCase().trim()

  // Check if there's a known mapping
  if (projectNameMapping[normalized]) {
    return projectNameMapping[normalized]
  }

  // Basic normalization: remove extra spaces, standardize capitalization
  const cleaned = projectName
    .trim()
    .replace(/\s+/g, ' ') // Replace multiple spaces with single space
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ')

  return cleaned
}

// Helper function to check if two project names match
const projectNamesMatch = (name1: string | null | undefined, name2: string | null | undefined): boolean => {
  if (!name1 || !name2) return false

  const normalized1 = normalizeProjectName(name1)
  const normalized2 = normalizeProjectName(name2)

  return normalized1 === normalized2
}

// Helper function to parse notes and extract individual entries
const parseNotes = (notes: string, leadCreatedAt?: string) => {
  if (!notes) return []
  
  // First check if notes contain "Referral Name:" at the beginning
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
  
  // Split by timestamp pattern [date/time]:
  const entries = remainingNotes ? remainingNotes.split(/\n\n(?=\[)/g) : []
  
  const parsedEntries = entries.map(entry => {
    const trimmed = entry.trim()
    if (!trimmed) return null

    // Extract timestamp and content
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
      // Handle notes without timestamp (legacy notes)
      // If it's the first entry and no timestamp, use lead creation date
      return {
        timestamp: leadCreatedAt || null,
        content: trimmed,
        date: leadCreatedAt ? new Date(leadCreatedAt) : null,
        isReferral: false
      }
    }
  }).filter(Boolean)
  
  // Add referral entry if exists
  if (referralEntry) {
    parsedEntries.unshift(referralEntry)
  }
  
  // Sort by date (newest first)
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

export default function LeadsComponent() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const { user } = useAuth()
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedStatus, setSelectedStatus] = useState('All Stages (Active Leads)')
  const [selectedAssignee, setSelectedAssignee] = useState('All Assigned')
  const [selectedSource, setSelectedSource] = useState('All Sources')
  const [selectedProject, setSelectedProject] = useState('All Projects')
  const [sortBy, setSortBy] = useState('created_at')
  const [showAddModal, setShowAddModal] = useState(false)
  const [showEditModal, setShowEditModal] = useState(false)
  const [editingLead, setEditingLead] = useState<any>(null)
  const [expandedNotes, setExpandedNotes] = useState<Set<string>>(new Set())
  const [hoveredNote, setHoveredNote] = useState<string | null>(null)
  const [selectedLead, setSelectedLead] = useState<any>(null)
  const [appendNote, setAppendNote] = useState('')
  const [showNotesHistory, setShowNotesHistory] = useState(false)
  const [selectedLeadNotes, setSelectedLeadNotes] = useState<any>(null)
  const [notesHistoryType, setNotesHistoryType] = useState<'regular' | 'management'>('regular')
  const [showWhatsAppModal, setShowWhatsAppModal] = useState(false)
  const [siteVisitDate, setSiteVisitDate] = useState('')
  const [showImportModal, setShowImportModal] = useState(false)
  const [importFile, setImportFile] = useState<File | null>(null)
  const [importing, setImporting] = useState(false)
  const [inlineEditingLeadId, setInlineEditingLeadId] = useState<string | null>(null)
  const [inlineRegisteredValue, setInlineRegisteredValue] = useState<string>('')
  const [newLeadNotifications, setNewLeadNotifications] = useState<any[]>([])
  const [lastLeadCount, setLastLeadCount] = useState<number>(0)
  const [dateRange, setDateRange] = useState<string>('All Time')
  const [customFromDate, setCustomFromDate] = useState<string>('')
  const [customToDate, setCustomToDate] = useState<string>('')
  const [newLead, setNewLead] = useState({
    first_name: '',
    last_name: '',
    email: '',
    phone: '',
    project_name: '',
    source: 'Direct Call',
    referral_name: '',
    notes: '',
    registered: 'No' as string,
    assigned_to_id: '' as string
  })
  const [agents, setAgents] = useState<any[]>([])

  const { data: leads, loading, refetch } = useLeads()

  // Handle URL parameters for filtering from Activities page
  useEffect(() => {
    const searchParam = searchParams.get('search')
    if (searchParam) {
      setSearchTerm(searchParam)
    }
  }, [searchParams])

  // Fetch agents for assignment dropdown
  useEffect(() => {
    const fetchAgents = async () => {
      try {
        const { data, error } = await supabase
          .from('flatrix_users')
          .select('id, name, email')
          .eq('role', 'AGENT')
          .eq('is_active', true)
          .order('name', { ascending: true })

        if (error) throw error
        setAgents(data || [])
      } catch (error) {
        console.error('Error fetching agents:', error)
      }
    }

    fetchAgents()
  }, [])

  // Auto-assign unassigned leads when page loads
  useEffect(() => {
    const autoAssignUnassignedLeads = async () => {
      if (!leads || leads.length === 0) return

      // Find all unassigned leads
      const unassignedLeads = leads.filter((lead: any) => !lead.assigned_to_id)

      if (unassignedLeads.length === 0) return

      console.log(`Found ${unassignedLeads.length} unassigned leads, assigning to agents...`)

      // Assign each unassigned lead
      for (const lead of unassignedLeads) {
        try {
          // Get next agent for round-robin assignment
          const { data: nextUserId, error: rotationError } = await supabase
            .rpc('get_next_assignee')

          if (rotationError) {
            console.error('Error getting next assignee:', rotationError)
            continue
          }

          if (nextUserId) {
            // Update the lead with assigned agent
            const { error: updateError } = await supabase
              .from('flatrix_leads')
              .update({
                assigned_to_id: nextUserId,
                updated_at: new Date().toISOString()
              })
              .eq('id', lead.id)

            if (updateError) {
              console.error('Error updating lead assignment:', updateError)
            } else {
              console.log(`Assigned lead ${lead.id} to agent ${nextUserId}`)
            }
          }
        } catch (error) {
          console.error('Error in auto-assignment:', error)
        }
      }

      // Refresh the leads list after assignments
      if (unassignedLeads.length > 0) {
        refetch()
      }
    }

    // Run auto-assignment after a short delay to ensure leads are loaded
    const timeoutId = setTimeout(() => {
      autoAssignUnassignedLeads()
    }, 1000)

    return () => clearTimeout(timeoutId)
  }, [leads, refetch])

  // Detect new leads and show notification with sound
  useEffect(() => {
    if (!leads || leads.length === 0) {
      setLastLeadCount(0)
      return
    }

    // Initialize on first load
    if (lastLeadCount === 0) {
      setLastLeadCount(leads.length)
      return
    }

    // Check if new leads were added
    if (leads.length > lastLeadCount) {
      const newLeadsCount = leads.length - lastLeadCount
      const latestLeads = leads.slice(0, newLeadsCount)

      // Play alarm sound
      playAlarmSound()

      // Add notifications for new leads
      const notifications = latestLeads.map(lead => ({
        id: lead.id,
        name: (lead as any).name || lead.first_name || 'Unknown',
        phone: lead.phone,
        source: lead.source || 'Unknown',
        timestamp: new Date().toLocaleTimeString('en-US', {
          hour: '2-digit',
          minute: '2-digit'
        })
      }))

      setNewLeadNotifications(prev => [...prev, ...notifications])
      setLastLeadCount(leads.length)
    } else if (leads.length < lastLeadCount) {
      // Lead count decreased (deleted), update count
      setLastLeadCount(leads.length)
    }
  }, [leads, lastLeadCount])

  // Play alarm sound function
  const playAlarmSound = () => {
    try {
      // Create an alarm sound using Web Audio API
      const audioContext = new (window.AudioContext || (window as any).webkitAudioContext)()
      const oscillator = audioContext.createOscillator()
      const gainNode = audioContext.createGain()

      oscillator.connect(gainNode)
      gainNode.connect(audioContext.destination)

      // Set alarm sound properties (beep pattern)
      oscillator.type = 'sine'
      oscillator.frequency.setValueAtTime(800, audioContext.currentTime) // High pitch

      gainNode.gain.setValueAtTime(0.3, audioContext.currentTime)
      gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.2)

      oscillator.start(audioContext.currentTime)
      oscillator.stop(audioContext.currentTime + 0.2)

      // Second beep
      setTimeout(() => {
        const oscillator2 = audioContext.createOscillator()
        const gainNode2 = audioContext.createGain()

        oscillator2.connect(gainNode2)
        gainNode2.connect(audioContext.destination)

        oscillator2.type = 'sine'
        oscillator2.frequency.setValueAtTime(1000, audioContext.currentTime)
        gainNode2.gain.setValueAtTime(0.3, audioContext.currentTime)
        gainNode2.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.2)

        oscillator2.start(audioContext.currentTime)
        oscillator2.stop(audioContext.currentTime + 0.2)
      }, 250)

      // Third beep
      setTimeout(() => {
        const oscillator3 = audioContext.createOscillator()
        const gainNode3 = audioContext.createGain()

        oscillator3.connect(gainNode3)
        gainNode3.connect(audioContext.destination)

        oscillator3.type = 'sine'
        oscillator3.frequency.setValueAtTime(800, audioContext.currentTime)
        gainNode3.gain.setValueAtTime(0.3, audioContext.currentTime)
        gainNode3.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.2)

        oscillator3.start(audioContext.currentTime)
        oscillator3.stop(audioContext.currentTime + 0.2)
      }, 500)
    } catch (error) {
      console.error('Error playing alarm sound:', error)
    }
  }

  // Close notification
  const closeNotification = (notificationId: string) => {
    setNewLeadNotifications(prev => prev.filter(n => n.id !== notificationId))
  }


  // Get unique project names from leads (normalized)
  const uniqueProjects = Array.from(
    new Set(
      leads
        ?.map(lead => normalizeProjectName(lead.project_name))
        .filter((name): name is string => Boolean(name))
    )
  ).sort()

  // Helper function to get date range
  const getDateRangeFilter = () => {
    const now = new Date()
    let fromDate: Date | null = null
    let toDate: Date | null = null

    switch (dateRange) {
      case 'Last 7 Days':
        fromDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)
        toDate = now
        break
      case 'Last 30 Days':
        fromDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)
        toDate = now
        break
      case 'Last 3 Months':
        fromDate = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000)
        toDate = now
        break
      case 'Last 6 Months':
        fromDate = new Date(now.getTime() - 180 * 24 * 60 * 60 * 1000)
        toDate = now
        break
      case 'Last 1 Year':
        fromDate = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000)
        toDate = now
        break
      case 'Custom Range':
        if (customFromDate) fromDate = new Date(customFromDate)
        if (customToDate) {
          toDate = new Date(customToDate)
          toDate.setHours(23, 59, 59, 999) // Include the entire end date
        }
        break
      case 'All Time':
      default:
        return { fromDate: null, toDate: null }
    }

    return { fromDate, toDate }
  }

  const filteredAndSortedLeads = leads?.filter(lead => {
    if (!lead) return false
    
    const searchMatches = !searchTerm || 
      ((lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown')?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      lead.first_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      lead.last_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      lead.email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      lead.phone?.includes(searchTerm) ||
      lead.preferred_location?.toLowerCase().includes(searchTerm.toLowerCase())
    
    // Default filtering: only show NEW and CONTACTED unless explicitly selected
    let statusMatches = false
    if (selectedStatus === 'All Leads') {
      // Show all leads regardless of status
      statusMatches = true
    } else if (selectedStatus === 'All Stages (Active Leads)') {
      // Show NEW, CONTACTED, and QUALIFIED leads (active pipeline)
      statusMatches = lead.status === 'NEW' || lead.status === 'CONTACTED' || lead.status === 'QUALIFIED'
    } else {
      // If user explicitly selected a status, show that status
      statusMatches = lead.status === selectedStatus
    }
    
    const sourceMatches = selectedSource === 'All Sources' || lead.source === selectedSource
    const projectMatches = selectedProject === 'All Projects' || projectNamesMatch(lead.project_name, selectedProject)

    // Assignee filtering
    let assigneeMatches = true
    if (selectedAssignee === 'Unassigned') {
      assigneeMatches = !lead.assigned_to_id
    } else if (selectedAssignee !== 'All Assigned') {
      // Filter by specific agent ID
      assigneeMatches = lead.assigned_to_id === selectedAssignee
    }

    // Date range filtering
    const { fromDate, toDate } = getDateRangeFilter()
    let dateMatches = true
    if (fromDate || toDate) {
      // Use next_followup_date when sorting by next_followup, otherwise use created_at
      const dateField = sortBy === 'next_followup' ? lead.next_followup_date : lead.created_at
      const leadDate = dateField ? new Date(dateField) : null
      if (leadDate) {
        // Normalize to start of day for comparison (ignore time component)
        const leadDateOnly = new Date(leadDate.getFullYear(), leadDate.getMonth(), leadDate.getDate())
        const fromDateOnly = fromDate ? new Date(fromDate.getFullYear(), fromDate.getMonth(), fromDate.getDate()) : null
        const toDateOnly = toDate ? new Date(toDate.getFullYear(), toDate.getMonth(), toDate.getDate()) : null

        if (fromDateOnly && leadDateOnly < fromDateOnly) dateMatches = false
        if (toDateOnly && leadDateOnly > toDateOnly) dateMatches = false
      } else {
        // If sorting by next_followup but no date set, exclude from filtered results when date range is active
        dateMatches = sortBy !== 'next_followup'
      }
    }

    return searchMatches && statusMatches && sourceMatches && projectMatches && assigneeMatches && dateMatches
  })?.sort((a, b) => {
    switch (sortBy) {
      case 'created_at':
        return new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime()
      case 'contacted_date':
        const aContactedDate = a.last_contacted_at ? new Date(a.last_contacted_at).getTime() : 0
        const bContactedDate = b.last_contacted_at ? new Date(b.last_contacted_at).getTime() : 0
        return bContactedDate - aContactedDate
      case 'next_followup':
        const aFollowupDate = a.next_followup_date ? new Date(a.next_followup_date).getTime() : 0
        const bFollowupDate = b.next_followup_date ? new Date(b.next_followup_date).getTime() : 0
        return bFollowupDate - aFollowupDate
      default:
        return new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime()
    }
  }) || []

  // Calculate stats based on FILTERED leads (respects date range)
  const totalLeads = filteredAndSortedLeads?.length || 0
  const qualifiedLeads = filteredAndSortedLeads?.filter(lead => lead.status === 'QUALIFIED').length || 0
  const unqualifiedLeads = filteredAndSortedLeads?.filter(lead => lead.status === 'LOST').length || 0
  const qualificationRate = totalLeads > 0 ? Math.round((qualifiedLeads / totalLeads) * 100) : 0

  // Mock percentage changes (in real app, would compare with previous period)
  const statsChange = {
    totalLeads: '+18.6%',
    qualifiedLeads: '+12.4%',
    qualificationRate: '+2.3%',
    unqualifiedLeads: '-8.5%'
  }

  const handleAddLead = async (e: React.FormEvent) => {
    e.preventDefault()

    try {
      // Determine which agent to assign to
      let assignedUserId = null

      if (newLead.assigned_to_id) {
        // Manual assignment - use the selected agent
        assignedUserId = newLead.assigned_to_id
      } else {
        // Auto-assignment - get next agent for round-robin
        const { data: nextUserId, error: rotationError } = await supabase
          .rpc('get_next_assignee')

        if (rotationError) {
          console.error('Error getting next assignee:', rotationError)
        }
        assignedUserId = nextUserId
      }

      // Append referral name to notes if source is Referral
      let finalNotes = newLead.notes || ''
      if (newLead.source === 'Referral' && newLead.referral_name) {
        finalNotes = finalNotes
          ? `Referral Name: ${newLead.referral_name}\n\n${finalNotes}`
          : `Referral Name: ${newLead.referral_name}`
      }

      const leadData: any = {
        name: newLead.first_name,
        email: newLead.email || null,
        phone: newLead.phone,
        project_name: newLead.project_name || null,
        source: newLead.source || null,
        notes: finalNotes || null,
        registered: newLead.registered,
        status: 'NEW' as const,
        status_of_save: 'CRM create',
        assigned_to_id: assignedUserId || null
      }

      // If registered is 'Yes', set registered_date
      if (newLead.registered === 'Yes') {
        leadData.registered_date = new Date().toISOString()
      }

      console.log('Attempting to add lead with data:', leadData)

      const { data, error } = await supabase
        .from('flatrix_leads')
        .insert([leadData])
        .select()

      if (error) {
        console.error('Supabase error details:', error)
        throw error
      }

      console.log('Lead added successfully:', data)
      toast.success('Lead added successfully!')
      setShowAddModal(false)
      setNewLead({
        first_name: '',
        last_name: '',
        email: '',
        phone: '',
        project_name: '',
        source: 'Website',
        referral_name: '',
        notes: '',
        registered: 'No',
        assigned_to_id: ''
      })
      refetch()
    } catch (error) {
      console.error('Error adding lead:', error)
      console.error('Error type:', typeof error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      toast.error(`Failed to add lead: ${(error as any)?.message || (error as any)?.details || JSON.stringify(error)}`)
    }
  }

  const handleEditLead = (lead: any) => {
    // Extract referral name if the source is Referral
    let referralName = ''
    if (lead.source === 'Referral' && lead.notes) {
      const referralMatch = lead.notes.match(/^Referral Name:\s*([^\n]+)/i)
      if (referralMatch) {
        referralName = referralMatch[1].trim()
      }
    }
    
    setEditingLead({
      ...lead,
      referral_name: referralName,
      // Handle registered as string with three possible values
      registered: (lead as any).registered || 'No',
      last_contacted_at: lead.last_contacted_at ? lead.last_contacted_at.split('T')[0] : '',
      next_followup_date: lead.next_followup_date
        ? new Date(lead.next_followup_date).toISOString().slice(0, 16)
        : ''
    })
    setAppendNote('') // Reset append note field
    setShowEditModal(true)
  }

  const handleUpdateLead = async (e: React.FormEvent) => {
    e.preventDefault()
    
    try {
      // Handle referral name update
      let updatedNotes = editingLead.notes || ''
      
      // If source is Referral, update or add referral name
      if (editingLead.source === 'Referral' && editingLead.referral_name) {
        // Remove existing referral name from notes if present
        updatedNotes = updatedNotes.replace(/^Referral Name:\s*[^\n]+\n?\n?/i, '')
        // Add new referral name at the beginning
        updatedNotes = `Referral Name: ${editingLead.referral_name}${updatedNotes ? '\n\n' + updatedNotes : ''}`
      }
      
      // Add new append note with timestamp if provided
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
      let updatedManagementNotes = editingLead.management_notes || ''
      if ((editingLead as any).appendManagementNote?.trim()) {
        const timestamp = new Date().toLocaleString('en-US', { 
          dateStyle: 'short', 
          timeStyle: 'short' 
        })
        updatedManagementNotes = updatedManagementNotes 
          ? `${updatedManagementNotes}\n\n[${timestamp}]:\n${(editingLead as any).appendManagementNote.trim()}`
          : `[${timestamp}]:\n${(editingLead as any).appendManagementNote.trim()}`
      }

      // Handle registered_date logic
      console.log('[LEAD UPDATE] editingLead.next_followup_date:', editingLead.next_followup_date)

      const leadData: any = {
        name: editingLead.name || editingLead.first_name,
        email: editingLead.email || null,
        phone: editingLead.phone,
        source: editingLead.source || null,
        preferred_type: editingLead.preferred_type || null,
        budget_min: editingLead.budget_min || null,
        notes: updatedNotes,
        management_notes: updatedManagementNotes,
        registered: editingLead.registered,
        status: editingLead.status,
        last_contacted_at: editingLead.last_contacted_at || null,
        next_followup_date: editingLead.next_followup_date
          ? new Date(editingLead.next_followup_date).toISOString()
          : null,
        assigned_to_id: editingLead.assigned_to_id || null,
        updated_at: new Date().toISOString()
      }

      console.log('[LEAD UPDATE] leadData.next_followup_date:', leadData.next_followup_date)
      console.log('[LEAD UPDATE] leadData.updated_at:', leadData.updated_at)

      // If registered is being set to 'Yes' and it wasn't 'Yes' before, set registered_date
      if (editingLead.registered === 'Yes') {
        // Check if the lead wasn't previously registered or if registered_date is not set
        const currentLead = leads?.find(lead => lead.id === editingLead.id)
        if ((currentLead as any)?.registered !== 'Yes' || !(currentLead as any)?.registered_date) {
          leadData.registered_date = new Date().toISOString()
        }
      } else {
        // If setting registered to 'No', 'Existing Lead', or 'Accompany client', clear the registered_date
        leadData.registered_date = null
      }

      console.log('[LEAD UPDATE] Updating lead ID:', editingLead.id)
      console.log('[LEAD UPDATE] Data being sent:', leadData)

      const { data: updatedData, error } = await supabase
        .from('flatrix_leads')
        .update(leadData)
        .eq('id', editingLead.id)
        .select()

      if (error) {
        console.error('[LEAD UPDATE] Error:', error)
        throw error
      }

      console.log('[LEAD UPDATE] Update successful, returned data:', updatedData)

      toast.success('Lead updated successfully!')
      setShowEditModal(false)
      setEditingLead(null)
      setAppendNote('')
      refetch()
      
      // Navigate to Deals page if status was changed to QUALIFIED
      if (editingLead.status === 'QUALIFIED') {
        router.push('/deals')
      }
    } catch (error) {
      console.error('Error updating lead:', error)
      const errorMessage = (error as any)?.message || 'Failed to update lead'
      toast.error(errorMessage)
    }
  }

  const formatBudget = (min: number | null, max: number | null) => {
    const formatAmount = (amount: number) => {
      if (amount >= 10000000) return `${(amount / 10000000).toFixed(1)}Cr`
      if (amount >= 100000) return `${(amount / 100000).toFixed(0)}L`
      return amount.toLocaleString()
    }

    if (min && max) return `₹${formatAmount(min)} - ${formatAmount(max)}`
    if (min) return `₹${formatAmount(min)}+`
    if (max) return `Up to ₹${formatAmount(max)}`
    return '-'
  }

  const handleWhatsAppGroup = (lead: any) => {
    setSelectedLead(lead)
    setSiteVisitDate('')
    setShowWhatsAppModal(true)
  }

  const handleInlineRegisteredUpdate = async (leadId: string) => {
    try {
      // Prepare the update data
      const updateData: any = {
        registered: inlineRegisteredValue
      }

      // Handle registered_date logic
      const currentLead = leads?.find(lead => lead.id === leadId)
      if (inlineRegisteredValue === 'Yes') {
        // Set registered_date if not already set
        if ((currentLead as any)?.registered !== 'Yes' || !(currentLead as any)?.registered_date) {
          updateData.registered_date = new Date().toISOString()
        }
      } else {
        // Clear registered_date for 'No', 'Existing Lead', or 'Accompany client'
        updateData.registered_date = null
      }

      const { error } = await supabase
        .from('flatrix_leads')
        .update(updateData)
        .eq('id', leadId)

      if (error) {
        throw error
      }

      toast.success('Registration status updated successfully!')
      setInlineEditingLeadId(null)
      setInlineRegisteredValue('')
      refetch()
    } catch (error) {
      console.error('Error updating registered status:', error)
      const errorMessage = (error as any)?.message || (error as any)?.details || 'Failed to update registration status'
      toast.error(`Failed to update: ${errorMessage}`)
    }
  }

  const sendToWhatsAppGroup = async () => {
    if (!selectedLead || !siteVisitDate) {
      toast.error('Please enter site visit date')
      return
    }

    const message = `Hi Team, please find Client details below

Client Name: ${selectedLead.name || [selectedLead.first_name, selectedLead.last_name].filter(Boolean).join(' ') || 'N/A'}
Phone Number: ${selectedLead.phone || 'N/A'}
Email: ${selectedLead.email || 'None'}
Preferred Unit: ${selectedLead.preferred_type || ''}
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
      toast.error('WhatsApp group opened. Please copy the message manually.')
    }

    // Close modal after sending
    setShowWhatsAppModal(false)
    setSiteVisitDate('')
    setSelectedLead(null)
  }

  const downloadTemplate = () => {
    // Create sample data for the template
    const templateData = [
      {
        first_name: 'John',
        last_name: 'Doe',
        email: 'john.doe@example.com',
        phone: '9876543210',
        source: 'Website',
        referral_name: '',
        preferred_type: '2 BHK',
        budget_min: 5000000,
        budget_max: 7000000,
        preferred_location: 'Whitefield',
        registered: 'FALSE',
        notes: 'Interested in premium apartments'
      },
      {
        first_name: 'Jane',
        last_name: 'Smith',
        email: 'jane.smith@example.com',
        phone: '8765432109',
        source: 'Referral',
        referral_name: 'Kumar Sharma',
        preferred_type: '3 BHK',
        budget_min: 8000000,
        budget_max: 12000000,
        preferred_location: 'HSR Layout',
        registered: 'TRUE',
        notes: 'Looking for ready to move properties'
      }
    ]

    // Create CSV content
    const headers = [
      'first_name',
      'last_name', 
      'email',
      'phone',
      'source',
      'referral_name',
      'preferred_type',
      'budget_min',
      'budget_max',
      'preferred_location',
      'registered',
      'notes'
    ]
    
    const csvContent = [
      headers.join(','),
      ...templateData.map(row => 
        headers.map(header => {
          const value = row[header as keyof typeof row] || ''
          // Escape commas and quotes in values
          return typeof value === 'string' && (value.includes(',') || value.includes('"')) 
            ? `"${value.replace(/"/g, '""')}"` 
            : value
        }).join(',')
      )
    ].join('\n')

    // Create and download file
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    const url = URL.createObjectURL(blob)
    link.setAttribute('href', url)
    link.setAttribute('download', 'leads_import_template.csv')
    link.style.visibility = 'hidden'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    
    toast.success('Template downloaded successfully!')
  }

  const handleImportFile = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (file) {
      // Check file type
      if (!file.name.toLowerCase().endsWith('.csv') && 
          !file.name.toLowerCase().endsWith('.xlsx') && 
          !file.name.toLowerCase().endsWith('.xls')) {
        toast.error('Please upload a CSV or Excel file')
        return
      }
      setImportFile(file)
    }
  }

  const processImportData = async () => {
    if (!importFile) {
      toast.error('Please select a file to import')
      return
    }

    setImporting(true)
    try {
      let data: any[] = []

      if (importFile.name.toLowerCase().endsWith('.csv')) {
        // Process CSV file
        const text = await importFile.text()
        const lines = text.split('\n').filter(line => line.trim())
        
        if (lines.length < 2) {
          throw new Error('File must contain header row and at least one data row')
        }

        const headers = lines[0].split(',').map(h => h.trim().replace(/['"]/g, ''))
        
        for (let i = 1; i < lines.length; i++) {
          const values = lines[i].split(',').map(v => v.trim().replace(/['"]/g, ''))
          const row: any = {}
          
          headers.forEach((header, index) => {
            row[header] = values[index] || ''
          })
          
          // Validate required fields
          if (row.first_name || row.last_name || row.phone) {
            data.push(row)
          }
        }
      } else {
        // For Excel files, we'll show an error for now since we need a library
        toast.error('Excel file processing requires additional setup. Please use CSV format for now.')
        setImporting(false)
        return
      }

      if (data.length === 0) {
        throw new Error('No valid lead data found in file')
      }

      // Insert data into database
      let successCount = 0
      let errorCount = 0

      for (const row of data) {
        try {
          // Get next agent for round-robin assignment
          const { data: nextUserId, error: rotationError } = await supabase
            .rpc('get_next_assignee')

          if (rotationError) {
            console.error('Error getting next assignee:', rotationError)
          }

          // Handle referral name in notes if source is Referral
          let finalNotes = row.notes || ''
          if (row.source === 'Referral' && row.referral_name) {
            finalNotes = finalNotes
              ? `Referral Name: ${row.referral_name}\n\n${finalNotes}`
              : `Referral Name: ${row.referral_name}`
          }

          const leadData = {
            name: row.first_name || '',
            email: row.email || null,
            phone: row.phone || '',
            source: row.source || 'Import',
            preferred_type: row.preferred_type || null,
            budget_min: row.budget_min ? parseInt(row.budget_min) : null,
            budget_max: row.budget_max ? parseInt(row.budget_max) : null,
            preferred_location: row.preferred_location || null,
            notes: finalNotes,
            registered: row.registered === 'true' || row.registered === 'TRUE' || row.registered === true || false,
            status: 'NEW',
            assigned_to_id: nextUserId || null,  // Auto-assign to next agent in rotation
            created_at: new Date().toISOString()
          }

          const { error } = await supabase
            .from('flatrix_leads')
            .insert(leadData)

          if (error) {
            console.error('Error inserting lead:', error)
            errorCount++
          } else {
            successCount++
          }
        } catch (err) {
          console.error('Error processing lead row:', err)
          errorCount++
        }
      }

      if (successCount > 0) {
        toast.success(`Successfully imported ${successCount} leads!`)
        refetch() // Refresh the leads list
      }
      
      if (errorCount > 0) {
        toast.error(`Failed to import ${errorCount} leads. Please check the data format.`)
      }

      // Close modal and reset state
      setShowImportModal(false)
      setImportFile(null)

    } catch (error) {
      console.error('Import error:', error)
      toast.error((error as Error).message || 'Failed to import leads')
    } finally {
      setImporting(false)
    }
  }


  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-lg text-gray-600">Loading leads...</div>
      </div>
    )
  }

  return (
    <div>
      <div className="mb-6 flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Leads Management</h1>
          <p className="text-gray-600 mt-2">Track and manage your sales leads effectively</p>
        </div>
        <div className="flex gap-3">
          <button className="flex items-center space-x-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition">
            <Download className="h-4 w-4" />
            <span>Export</span>
          </button>
          <button 
            onClick={() => setShowImportModal(true)}
            className="flex items-center space-x-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition"
          >
            <Upload className="h-4 w-4" />
            <span>Import Data</span>
          </button>
          <button
            onClick={() => setShowAddModal(true)}
            className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
          >
            <Plus className="h-4 w-4" />
            <span>Add Lead</span>
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {/* Total Leads */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Total Leads</p>
              <h3 className="text-3xl font-bold text-gray-900">{totalLeads}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.totalLeads}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-blue-100 p-3 rounded-lg">
              <Users className="h-6 w-6 text-blue-600" />
            </div>
          </div>
        </div>

        {/* Qualified Leads */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Qualified Leads</p>
              <h3 className="text-3xl font-bold text-gray-900">{qualifiedLeads}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.qualifiedLeads}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-green-100 p-3 rounded-lg">
              <CheckCircle className="h-6 w-6 text-green-600" />
            </div>
          </div>
        </div>

        {/* Qualification Rate */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Qualification Rate</p>
              <h3 className="text-3xl font-bold text-gray-900">{qualificationRate}%</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.qualificationRate}</span>
                <span className="text-xs text-gray-500 ml-2">improvement</span>
              </div>
            </div>
            <div className="bg-purple-100 p-3 rounded-lg">
              <TrendingUp className="h-6 w-6 text-purple-600" />
            </div>
          </div>
        </div>

        {/* Unqualified Leads */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Unqualified Leads</p>
              <h3 className="text-3xl font-bold text-gray-900">{unqualifiedLeads}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-red-600 mr-1 rotate-180" />
                <span className="text-sm text-red-600 font-medium">{statsChange.unqualifiedLeads}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-red-100 p-3 rounded-lg">
              <X className="h-6 w-6 text-red-600" />
            </div>
          </div>
        </div>
      </div>

      {/* Search and Filters Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 mb-6">
        <div className="p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Lead Search & Filters</h2>
          <p className="text-sm text-gray-600 mb-4">Filter and search through your leads</p>
          
          <div className="flex flex-col gap-4">
            {/* Search Bar */}
            <div className="w-full">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search leads by name, contact, or city..."
                  className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>
            </div>
            
            {/* Filter Dropdowns */}
            <div className="flex flex-col sm:flex-row gap-3">
              <div className="relative flex-1">
                <select
                  value={selectedStatus}
                  onChange={(e) => setSelectedStatus(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option>All Leads</option>
                  <option>All Stages (Active Leads)</option>
                  {leadStatuses.map(status => (
                    <option key={status} value={status}>{statusDisplayLabels[status as keyof typeof statusDisplayLabels]}</option>
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
                  {leadSources.map(source => (
                    <option key={source} value={source}>{source}</option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <select
                  value={selectedProject}
                  onChange={(e) => setSelectedProject(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option>All Projects</option>
                  {uniqueProjects.map(project => (
                    <option key={project} value={project}>{project}</option>
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
                  <option value="All Assigned">All Agents</option>
                  <option value="Unassigned">Unassigned</option>
                  {agents.map(agent => (
                    <option key={agent.id} value={agent.id}>
                      {agent.name}
                    </option>
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
                  <option value="created_at">Sort by Created Date</option>
                  <option value="contacted_date">Sort by Contacted Date</option>
                  <option value="next_followup">Sort by Next Followup</option>
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>
            </div>

            {/* Date Range Filter */}
            <div className="flex flex-col sm:flex-row gap-3">
              <div className="relative flex-1">
                <select
                  value={dateRange}
                  onChange={(e) => {
                    setDateRange(e.target.value)
                    if (e.target.value !== 'Custom Range') {
                      setCustomFromDate('')
                      setCustomToDate('')
                    }
                  }}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option value="All Time">All Time</option>
                  <option value="Last 7 Days">Last 7 Days</option>
                  <option value="Last 30 Days">Last 30 Days</option>
                  <option value="Last 3 Months">Last 3 Months</option>
                  <option value="Last 6 Months">Last 6 Months</option>
                  <option value="Last 1 Year">Last 1 Year</option>
                  <option value="Custom Range">Custom Range</option>
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              {/* Custom Date Range Inputs */}
              {dateRange === 'Custom Range' && (
                <>
                  <div className="relative flex-1">
                    <input
                      type="date"
                      value={customFromDate}
                      onChange={(e) => setCustomFromDate(e.target.value)}
                      className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                      placeholder="From Date"
                    />
                  </div>
                  <div className="relative flex-1">
                    <input
                      type="date"
                      value={customToDate}
                      onChange={(e) => setCustomToDate(e.target.value)}
                      className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                      placeholder="To Date"
                    />
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Leads List Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Leads List</h2>
            <p className="text-sm text-gray-600 mt-1">Latest leads and their current status</p>
          </div>
        </div>

        {/* Modern Row-based Leads Display */}
        <div>
          {filteredAndSortedLeads.length === 0 ? (
            <div className="text-center text-gray-500 py-12">
              <User className="h-12 w-12 text-gray-300 mx-auto mb-3" />
              <p className="text-lg font-medium mb-1">No leads found</p>
              <p className="text-sm text-gray-400">
                {searchTerm || selectedStatus !== 'All Stages' ? 'Try adjusting your filters' : 'Add your first lead to get started'}
              </p>
            </div>
          ) : (
            <div className="divide-y divide-gray-200">
              {filteredAndSortedLeads.map((lead: any) => (
                <div key={lead.id} className="px-4 sm:px-6 py-4 hover:bg-gray-50 transition-colors">
                  {/* Desktop Layout */}
                  <div className="hidden md:flex items-center justify-between gap-2">
                    {/* Left Section - Lead Info */}
                    <div className="flex items-center space-x-3 flex-1 min-w-0 overflow-hidden">
                      {/* Profile Icon */}
                      <div className="flex-shrink-0">
                        <div className="w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center">
                          <User className="h-5 w-5 text-blue-600" />
                        </div>
                      </div>

                      {/* Name Column */}
                      <div className="min-w-[120px] max-w-[160px] flex-shrink">
                        {(lead as any).project_name && (
                          <div className="text-xs font-medium text-blue-600 mb-1">
                            {(lead as any).project_name}
                          </div>
                        )}
                        <h3 className="text-base font-semibold text-gray-900">
                          {(lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || `${lead.first_name || ''} ${lead.last_name || ''}`.trim() || 'Unknown'}
                        </h3>
                        <div className="flex gap-3 mt-0">
                          {lead.preferred_type && (
                            <div className="text-xs text-gray-600">
                              <span className="font-medium">Unit:</span> {lead.preferred_type}
                            </div>
                          )}
                          {lead.budget_min && (
                            <div className="text-xs text-gray-600">
                              <span className="font-medium">Budget:</span> ₹{lead.budget_min.toLocaleString()}
                            </div>
                          )}
                        </div>
                        {lead.preferred_location && (
                          <div className="text-xs text-gray-500 mt-0">
                            {lead.preferred_location}
                          </div>
                        )}
                      </div>

                      {/* Contact Details Column */}
                      <div className="min-w-[120px] max-w-[160px] flex-shrink">
                        <div className="text-sm text-gray-900">
                          {lead.phone || 'No phone'}
                        </div>
                        <div className="text-xs text-gray-500 truncate">
                          {lead.email || 'No email'}
                        </div>
                        {/* Show source and referral name if applicable */}
                        <div className="text-xs text-gray-500 mt-0.5">
                          Source: {getDisplaySource(lead.source)}
                          {lead.source === 'Referral' && lead.notes?.includes('Referral Name:') && (
                            <span className="text-blue-600 font-medium">
                              {' • ' + lead.notes.split('Referral Name:')[1]?.split('\n')[0]?.trim()}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Stage Column */}
                      <div className="min-w-[100px] max-w-[140px] flex-shrink">
                        <div className="text-sm text-gray-600 mb-1">
                          Stage: <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${statusColors[lead.status as keyof typeof statusColors] || 'bg-gray-100 text-gray-800'}`}>
                            {statusDisplayLabels[lead.status as keyof typeof statusDisplayLabels] || 'New'}
                          </span>
                        </div>
                        <div className="text-sm text-gray-600">
                          Assigned to: <span className="text-gray-900 font-medium">{(lead as any).assigned_to?.name || 'Unassigned'}</span>
                        </div>
                      </div>

                      {/* Registered Column */}
                      <div className="min-w-[100px] max-w-[140px] flex-shrink">
                        <div className="text-sm text-gray-600">
                          <span className="text-xs">Registered:</span>
                          {inlineEditingLeadId === lead.id ? (
                            <div className="flex items-center gap-2 mt-1 flex-wrap" onClick={(e) => e.stopPropagation()}>
                              <select
                                value={inlineRegisteredValue}
                                onChange={(e) => setInlineRegisteredValue(e.target.value)}
                                className="text-xs border rounded px-1 py-0.5"
                                autoFocus
                              >
                                <option value="No">No</option>
                                <option value="Yes">Yes</option>
                                <option value="Existing Lead">Existing Lead</option>
                                <option value="Accompany client">Accompany client</option>
                              </select>
                              <button
                                onClick={(e) => {
                                  e.stopPropagation()
                                  handleInlineRegisteredUpdate(lead.id)
                                }}
                                className="text-xs bg-blue-500 text-white px-2 py-0.5 rounded hover:bg-blue-600 cursor-pointer"
                              >
                                Update
                              </button>
                              <button
                                onClick={(e) => {
                                  e.stopPropagation()
                                  setInlineEditingLeadId(null)
                                  setInlineRegisteredValue('')
                                }}
                                className="text-xs bg-gray-400 text-white px-2 py-0.5 rounded hover:bg-gray-500 cursor-pointer"
                              >
                                Cancel
                              </button>
                            </div>
                          ) : (
                            <div className="flex items-center gap-1 mt-1">
                              <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${
                                (lead as any).registered === 'Yes' ? 'bg-green-100 text-green-800' :
                                (lead as any).registered === 'Existing Lead' ? 'bg-blue-100 text-blue-800' :
                                (lead as any).registered === 'Accompany client' ? 'bg-purple-100 text-purple-800' :
                                'bg-red-100 text-red-800'
                              }`}>
                                {(lead as any).registered || 'No'}
                              </span>
                              <button
                                onClick={(e) => {
                                  e.stopPropagation()
                                  setInlineEditingLeadId(lead.id)
                                  setInlineRegisteredValue((lead as any).registered || 'No')
                                }}
                                className="text-xs text-gray-500 hover:text-gray-700 ml-1"
                                title="Edit registration status"
                              >
                                <Edit2 className="h-3 w-3" />
                              </button>
                            </div>
                          )}
                        </div>
                      </div>

                      {/* Combined Dates Column */}
                      <div className="min-w-[130px] max-w-[160px] flex-shrink">
                        <div className="text-xs text-gray-700 mb-1">
                          Created Date: <span className="text-gray-600 text-[10px] whitespace-nowrap">
                            {lead.created_at
                              ? new Date(lead.created_at).toLocaleString('en-US', {
                                  month: 'short',
                                  day: 'numeric',
                                  year: 'numeric',
                                  hour: '2-digit',
                                  minute: '2-digit'
                                })
                              : '-'}
                          </span>
                        </div>
                        <div className="text-xs text-gray-700 mb-1">
                          Contacted Date: <span className="text-gray-600">
                            {lead.last_contacted_at
                              ? new Date(lead.last_contacted_at).toLocaleDateString('en-US', {
                                  month: 'short',
                                  day: 'numeric',
                                  year: 'numeric'
                                })
                              : 'Never'}
                          </span>
                        </div>
                        <div className="text-xs text-gray-700">
                          Next Followup: <span className="text-gray-600">
                            {lead.next_followup_date
                              ? new Date(lead.next_followup_date).toLocaleString('en-US', {
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
                      </div>
                    </div>

                    {/* Right Section - Actions */}
                    <div className="flex items-center space-x-1 ml-2 flex-shrink-0">
                      {lead.phone && (
                        <>
                          <button
                            onClick={() => handleWhatsAppGroup(lead)}
                            className="p-2 text-purple-600 hover:text-purple-700 hover:bg-purple-50 rounded-lg transition-colors border border-purple-200"
                            title="Send to WhatsApp Group"
                          >
                            <Users className="h-4 w-4" />
                          </button>
                          <a
                            href={`tel:${lead.phone}`}
                            className="p-2 text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded-lg transition-colors border border-blue-200"
                            title="Call"
                          >
                            <Phone className="h-4 w-4" />
                          </a>
                          <a
                            href={`https://wa.me/${lead.phone.replace(/[^0-9]/g, '')}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-2 text-green-600 hover:text-green-700 hover:bg-green-50 rounded-lg transition-colors border border-green-200"
                            title="WhatsApp"
                          >
                            <WhatsAppIcon className="h-4 w-4" />
                          </a>
                        </>
                      )}
                      {lead.email && (
                        <a
                          href={`mailto:${lead.email}`}
                          className="p-2 text-gray-600 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
                          title="Email"
                        >
                          <Mail className="h-4 w-4" />
                        </a>
                      )}
                      <button
                        onClick={() => handleEditLead(lead)}
                        className="p-2 text-gray-600 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
                        title="Edit"
                      >
                        <Edit2 className="h-4 w-4" />
                      </button>
                    </div>
                  </div>

                  {/* Mobile Layout */}
                  <div className="md:hidden">
                    <div className="flex items-start justify-between">
                      <div className="flex items-start space-x-3 flex-1">
                        {/* Profile Icon */}
                        <div className="flex-shrink-0">
                          <div className="w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center">
                            <User className="h-5 w-5 text-blue-600" />
                          </div>
                        </div>
                        <div className="flex-1">
                          {(lead as any).project_name && (
                            <div className="text-xs font-medium text-blue-600 mb-1">
                              {(lead as any).project_name}
                            </div>
                          )}
                          <h3 className="text-lg font-semibold text-gray-900">
                            {(lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || `${lead.first_name || ''} ${lead.last_name || ''}`.trim() || 'Unknown'}
                          </h3>
                          <div className="flex flex-wrap gap-2 mt-0">
                            {lead.preferred_type && (
                              <div className="text-xs text-gray-600">
                                <span className="font-medium">Preferred Unit:</span> {lead.preferred_type}
                              </div>
                            )}
                            {lead.budget_min && (
                              <div className="text-xs text-gray-600">
                                <span className="font-medium">Budget:</span> ₹{lead.budget_min.toLocaleString()}
                              </div>
                            )}
                          </div>
                          <div className="mt-1 space-y-0.5">
                            <div className="flex items-center text-sm text-gray-600">
                              <Phone className="h-4 w-4 mr-2" />
                              {lead.phone || 'No phone'}
                            </div>
                            {lead.email && (
                              <div className="flex items-center text-sm text-gray-600">
                                <Mail className="h-4 w-4 mr-2" />
                                <span className="truncate">{lead.email}</span>
                              </div>
                            )}
                            {lead.preferred_location && (
                              <div className="flex items-center text-sm text-gray-600">
                                <MapPin className="h-4 w-4 mr-2" />
                                {lead.preferred_location}
                              </div>
                            )}
                          </div>
                          
                          <div className="mt-1 flex flex-wrap gap-2">
                            <span className={`inline-flex px-2 py-1 rounded text-xs font-medium ${statusColors[lead.status as keyof typeof statusColors] || 'bg-gray-50 text-gray-700'}`}>
                              {statusDisplayLabels[lead.status as keyof typeof statusDisplayLabels] || 'New'}
                            </span>
                            <span className={`inline-flex px-2 py-1 rounded text-xs font-medium ${
                              (lead as any).registered === 'Yes' ? 'bg-green-100 text-green-800' :
                              (lead as any).registered === 'Existing Lead' ? 'bg-blue-100 text-blue-800' :
                              (lead as any).registered === 'Accompany client' ? 'bg-purple-100 text-purple-800' :
                              'bg-red-100 text-red-800'
                            }`}>
                              Registered: {(lead as any).registered || 'No'}
                            </span>
                            <span className="inline-flex px-2 py-1 rounded text-xs bg-gray-100 text-gray-600">
                              Assigned to: {(lead as any).assigned_to?.name || 'Unassigned'}
                            </span>
                          </div>

                          <div className="mt-1.5 text-xs text-gray-500 space-y-1">
                            <div className="whitespace-nowrap">Created: <span className="text-[10px]">{lead.created_at
                              ? new Date(lead.created_at).toLocaleString('en-US', {
                                  month: 'short',
                                  day: 'numeric',
                                  year: 'numeric',
                                  hour: '2-digit',
                                  minute: '2-digit'
                                })
                              : '-'}</span></div>
                            <div>Contacted: {lead.last_contacted_at 
                              ? new Date(lead.last_contacted_at).toLocaleDateString('en-US', { 
                                  month: 'short', 
                                  day: 'numeric',
                                  year: 'numeric' 
                                })
                              : 'Never'}</div>
                            <div>Next Followup: {lead.next_followup_date
                              ? new Date(lead.next_followup_date).toLocaleString('en-US', {
                                  month: 'short',
                                  day: 'numeric',
                                  year: 'numeric',
                                  hour: 'numeric',
                                  minute: '2-digit',
                                  hour12: true
                                })
                              : 'Not set'}</div>
                          </div>
                        </div>
                      </div>

                      {/* Mobile Actions */}
                      <div className="flex flex-col space-y-2 ml-4">
                        {lead.phone && (
                          <>
                            <button
                              onClick={() => handleWhatsAppGroup(lead)}
                              className="p-2 text-purple-600 hover:text-purple-700 hover:bg-purple-50 rounded-lg transition-colors border border-purple-200"
                              title="Send to WhatsApp Group"
                            >
                              <Users className="h-5 w-5" />
                            </button>
                            <a
                              href={`tel:${lead.phone}`}
                              className="p-2 text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded-lg transition-colors border border-blue-200"
                              title="Call"
                            >
                              <Phone className="h-5 w-5" />
                            </a>
                            <a
                              href={`https://wa.me/${lead.phone.replace(/[^0-9]/g, '')}`}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="p-2 text-green-600 hover:text-green-700 hover:bg-green-50 rounded-lg transition-colors border border-green-200"
                              title="WhatsApp"
                            >
                              <WhatsAppIcon className="h-5 w-5" />
                            </a>
                          </>
                        )}
                        {lead.email && (
                          <a
                            href={`mailto:${lead.email}`}
                            className="p-2 text-gray-600 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
                            title="Email"
                          >
                            <Mail className="h-5 w-5" />
                          </a>
                        )}
                        <button
                          onClick={() => handleEditLead(lead)}
                          className="p-2 text-gray-600 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
                          title="Edit"
                        >
                          <Edit2 className="h-5 w-5" />
                        </button>
                      </div>
                    </div>
                  </div>
                  
                  {/* Notes Row - Below all columns (both desktop and mobile) */}
                  <div className="mt-3 pt-3 border-t border-gray-100">
                    <div className="flex items-start space-x-2">
                      <button
                        onClick={() => {
                          setSelectedLeadNotes(lead)
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
                          {getLatestNote(lead.notes, lead.created_at) || 'No notes added'}
                        </span>
                        {lead.notes && parseNotes(lead.notes, lead.created_at).length > 1 && (
                          <button
                            onClick={() => {
                              setSelectedLeadNotes(lead)
                              setShowNotesHistory(true)
                            }}
                            className="text-xs text-blue-600 hover:text-blue-700 ml-1 hover:underline"
                          >
                            (+{parseNotes(lead.notes, lead.created_at).length - 1} more)
                          </button>
                        )}
                      </div>
                    </div>

                    {/* Management Notes */}
                    <div className="flex items-start space-x-2 pt-2 border-t border-gray-100">
                      <button
                        onClick={() => {
                          setSelectedLeadNotes(lead)
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
                          {getLatestNote((lead as any).management_notes, lead.created_at) || 'No management notes'}
                        </span>
                        {(lead as any).management_notes && parseNotes((lead as any).management_notes, lead.created_at).length > 1 && (
                          <button
                            onClick={() => {
                              setSelectedLeadNotes(lead)
                              setNotesHistoryType('management')
                              setShowNotesHistory(true)
                            }}
                            className="text-xs text-purple-600 hover:text-purple-700 ml-1 hover:underline"
                          >
                            (+{parseNotes((lead as any).management_notes, lead.created_at).length - 1} more)
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

        {/* Results Summary */}
        {filteredAndSortedLeads.length > 0 && (
          <div className="px-6 py-3 bg-gray-50 border-t text-sm text-gray-600">
            Showing {filteredAndSortedLeads.length} of {leads?.length || 0} leads
          </div>
        )}
      </div>

      {/* Add Lead Modal */}
      {showAddModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between p-6 border-b border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900">Add New Lead</h2>
              <button
                onClick={() => setShowAddModal(false)}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={handleAddLead} className="p-6">
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Project Name</label>
                  <input
                    type="text"
                    value={newLead.project_name || ''}
                    onChange={(e) => setNewLead({...newLead, project_name: e.target.value})}
                    placeholder="Enter project name"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Contact Name *</label>
                  <input
                    type="text"
                    required
                    value={newLead.first_name}
                    onChange={(e) => setNewLead({...newLead, first_name: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
                    <input 
                      type="email" 
                      value={newLead.email}
                      onChange={(e) => setNewLead({...newLead, email: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Phone *</label>
                    <input 
                      type="tel" 
                      required
                      value={newLead.phone}
                      onChange={(e) => setNewLead({...newLead, phone: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                    />
                  </div>
                </div>



                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Lead Source</label>
                  <select 
                    value={newLead.source}
                    onChange={(e) => setNewLead({...newLead, source: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    {leadSources.map(source => (
                      <option key={source} value={source}>{source}</option>
                    ))}
                  </select>
                </div>

                {/* Conditional Referral Name Field */}
                {newLead.source === 'Referral' && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <div className="flex items-center space-x-2">
                        <UserCheck className="h-4 w-4" />
                        <span>Referral Name *</span>
                      </div>
                    </label>
                    <input 
                      type="text" 
                      required
                      value={newLead.referral_name}
                      onChange={(e) => setNewLead({...newLead, referral_name: e.target.value})}
                      placeholder="Enter the name of the person who referred this lead"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      This will be automatically added to the lead notes for tracking.
                    </p>
                  </div>
                )}

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Registered</label>
                  <select
                    value={newLead.registered || 'No'}
                    onChange={(e) => setNewLead({...newLead, registered: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    <option value="No">No</option>
                    <option value="Yes">Yes</option>
                    <option value="Existing Lead">Existing Lead</option>
                    <option value="Accompany client">Accompany client</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <User className="h-4 w-4" />
                      <span>Assigned To (Agent)</span>
                    </div>
                  </label>
                  <select
                    value={newLead.assigned_to_id || ''}
                    onChange={(e) => setNewLead({...newLead, assigned_to_id: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    <option value="">Auto-assign (Round Robin)</option>
                    {agents.map(agent => (
                      <option key={agent.id} value={agent.id}>
                        {agent.name} {agent.email ? `(${agent.email})` : ''}
                      </option>
                    ))}
                  </select>
                  <p className="text-xs text-gray-500 mt-1">
                    Leave as "Auto-assign" to automatically assign to the next agent in rotation, or select a specific agent
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Notes</label>
                  <textarea
                    rows={3}
                    value={newLead.notes}
                    onChange={(e) => setNewLead({...newLead, notes: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  ></textarea>
                </div>
              </div>

              <div className="flex justify-end space-x-3 mt-6">
                <button
                  type="button"
                  onClick={() => setShowAddModal(false)}
                  className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition"
                >
                  Cancel
                </button>
                <button 
                  type="submit"
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
                >
                  Add Lead
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Edit Lead Modal */}
      {showEditModal && editingLead && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between p-6 border-b border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900">Edit Lead</h2>
              <button
                onClick={() => {setShowEditModal(false); setEditingLead(null); setAppendNote('')}}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={handleUpdateLead} className="p-6">
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Contact Name *</label>
                  <input 
                    type="text" 
                    required
                    value={editingLead.name || editingLead.first_name || ''}
                    onChange={(e) => setEditingLead({...editingLead, name: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                  />
                </div>
                
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
                    <input 
                      type="email" 
                      value={editingLead.email || ''}
                      onChange={(e) => setEditingLead({...editingLead, email: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Phone *</label>
                    <input 
                      type="tel" 
                      required
                      value={editingLead.phone || ''}
                      onChange={(e) => setEditingLead({...editingLead, phone: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Contacted Date</label>
                    <input 
                      type="date" 
                      value={editingLead.last_contacted_at || ''}
                      onChange={(e) => setEditingLead({...editingLead, last_contacted_at: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Next Followup Date & Time</label>
                    <input
                      type="datetime-local"
                      value={editingLead.next_followup_date || ''}
                      onChange={(e) => {
                        console.log('[FOLLOWUP INPUT] New value:', e.target.value)
                        setEditingLead({...editingLead, next_followup_date: e.target.value})
                      }}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    />
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Lead Source</label>
                  <select 
                    value={editingLead.source || 'Website'}
                    onChange={(e) => setEditingLead({...editingLead, source: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    {leadSources.map(source => (
                      <option key={source} value={source}>{source}</option>
                    ))}
                  </select>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Preferred Type</label>
                  <select 
                    value={editingLead.preferred_type || ''}
                    onChange={(e) => setEditingLead({...editingLead, preferred_type: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    <option value="">Select Type</option>
                    <option value="1BHK">1BHK</option>
                    <option value="2BHK">2BHK</option>
                    <option value="3BHK">3BHK</option>
                    <option value="4BHK">4BHK</option>
                    <option value="Villa">Villa</option>
                    <option value="Plot">Plot</option>
                    <option value="Commercial">Commercial</option>
                  </select>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Budget (₹)</label>
                  <input 
                    type="number" 
                    value={editingLead.budget_min || ''}
                    onChange={(e) => setEditingLead({...editingLead, budget_min: parseFloat(e.target.value) || 0})}
                    placeholder="Enter budget"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                  />
                </div>

                {/* Conditional Referral Name Field for Edit */}
                {editingLead.source === 'Referral' && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <div className="flex items-center space-x-2">
                        <UserCheck className="h-4 w-4" />
                        <span>Referral Name</span>
                      </div>
                    </label>
                    <input 
                      type="text" 
                      value={editingLead.referral_name || ''}
                      onChange={(e) => setEditingLead({...editingLead, referral_name: e.target.value})}
                      placeholder="Enter the name of the person who referred this lead"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" 
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Update the referral name if needed. This will replace the existing referral name.
                    </p>
                  </div>
                )}

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Status</label>
                    <select
                      value={editingLead.status || 'NEW'}
                      onChange={(e) => setEditingLead({...editingLead, status: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    >
                      {leadStatuses.map(status => (
                        <option key={status} value={status}>{statusDisplayLabels[status as keyof typeof statusDisplayLabels]}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Registered</label>
                    <select
                      value={editingLead.registered || 'No'}
                      onChange={(e) => setEditingLead({...editingLead, registered: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    >
                      <option value="No">No</option>
                      <option value="Yes">Yes</option>
                      <option value="Existing Lead">Existing Lead</option>
                      <option value="Accompany client">Accompany client</option>
                    </select>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <User className="h-4 w-4" />
                      <span>Assigned To (Agent)</span>
                    </div>
                  </label>
                  <select
                    value={editingLead.assigned_to_id || ''}
                    onChange={(e) => setEditingLead({...editingLead, assigned_to_id: e.target.value || null})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    <option value="">Unassigned</option>
                    {agents.map(agent => (
                      <option key={agent.id} value={agent.id}>
                        {agent.name} {agent.email ? `(${agent.email})` : ''}
                      </option>
                    ))}
                  </select>
                  <p className="text-xs text-gray-500 mt-1">
                    Select an agent to assign this lead to
                  </p>
                </div>

                {/* Existing Notes - Read Only */}
                {editingLead.notes && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <div className="flex items-center space-x-2">
                        <FileText className="h-4 w-4" />
                        <span>Previous Notes History</span>
                      </div>
                    </label>
                    <div className="w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg max-h-32 overflow-y-auto">
                      <pre className="text-sm text-gray-600 whitespace-pre-wrap font-sans">{editingLead.notes}</pre>
                    </div>
                  </div>
                )}

                {/* Append New Notes */}
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
                    Note: Previous notes cannot be edited or deleted. New notes will be added below existing notes with a timestamp.
                  </p>
                </div>

                {/* Management Notes Section */}
                <div className="pt-4 border-t border-gray-200">
                  <h3 className="text-sm font-semibold text-purple-700 mb-3 flex items-center space-x-2">
                    <FileText className="h-4 w-4" />
                    <span>Management Notes</span>
                  </h3>
                  
                  {/* Previous Management Notes */}
                  {editingLead.management_notes && (
                    <div className="mb-4">
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        <div className="flex items-center space-x-2">
                          <FileText className="h-4 w-4" />
                          <span>Previous Management Notes History</span>
                        </div>
                      </label>
                      <div className="w-full px-3 py-2 bg-purple-50 border border-purple-200 rounded-lg max-h-32 overflow-y-auto">
                        <pre className="text-sm text-gray-600 whitespace-pre-wrap font-sans">{editingLead.management_notes}</pre>
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
                      value={(editingLead as any).appendManagementNote || ''}
                      onChange={(e) => setEditingLead({...editingLead, appendManagementNote: e.target.value})}
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
                  onClick={() => {setShowEditModal(false); setEditingLead(null); setAppendNote('')}}
                  className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition"
                >
                  Cancel
                </button>
                <button 
                  type="submit"
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
                >
                  Update Lead
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Notes History Modal */}
      {showNotesHistory && selectedLeadNotes && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {
            setShowNotesHistory(false)
            setSelectedLeadNotes(null)
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
                  {selectedLeadNotes.name || `${selectedLeadNotes.first_name || ''} ${selectedLeadNotes.last_name || ''}`.trim() || 'Lead'}
                </p>
              </div>
              <button
                onClick={() => {
                  setShowNotesHistory(false)
                  setSelectedLeadNotes(null)
                }}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            
            <div className="p-6 max-h-96 overflow-y-auto">
              {(() => {
                const notesToShow = notesHistoryType === 'management' 
                  ? (selectedLeadNotes as any).management_notes 
                  : selectedLeadNotes.notes
                const parsedNotesToShow = parseNotes(notesToShow, selectedLeadNotes.created_at)
                const borderColor = notesHistoryType === 'management' ? 'border-purple-200' : 'border-blue-200'
                const dotColor = notesHistoryType === 'management' ? 'bg-purple-500' : 'bg-blue-500'
                const noNotesIcon = notesHistoryType === 'management' ? FileText : MessageSquare
                const NoNotesIcon = noNotesIcon
                
                return parsedNotesToShow.length === 0 ? (
                  <div className="text-center text-gray-500 py-8">
                    <NoNotesIcon className="h-12 w-12 text-gray-300 mx-auto mb-3" />
                    <p className="text-lg font-medium mb-1">No {notesHistoryType === 'management' ? 'management ' : ''}notes available</p>
                    <p className="text-sm">No {notesHistoryType === 'management' ? 'management ' : ''}notes have been added to this lead yet.</p>
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
                        {note?.isReferral && (
                          <span className="bg-blue-100 text-blue-800 text-xs px-2 py-1 rounded-full">
                            Referral
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
                  notesHistoryType === 'management' ? (selectedLeadNotes as any).management_notes : selectedLeadNotes.notes, 
                  selectedLeadNotes.created_at
                ).length}</span>
                <span>Click outside to close</span>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Import Data Modal */}
      {showImportModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-md w-full mx-4">
            <div className="px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">Import Leads Data</h3>
            </div>
            
            <div className="px-6 py-4">
              <div className="space-y-4">
                {/* Step 1: Download Template */}
                <div>
                  <h4 className="text-sm font-medium text-gray-700 mb-2">Step 1: Download Template</h4>
                  <p className="text-sm text-gray-600 mb-3">
                    Download the sample template to see the correct format for your data.
                  </p>
                  <button
                    onClick={downloadTemplate}
                    className="w-full px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center justify-center space-x-2"
                  >
                    <Download className="h-4 w-4" />
                    <span>Download Template</span>
                  </button>
                </div>

                {/* Step 2: Upload File */}
                <div>
                  <h4 className="text-sm font-medium text-gray-700 mb-2">Step 2: Upload Your File</h4>
                  <p className="text-sm text-gray-600 mb-3">
                    Upload your CSV file with lead data in the same format as the template.
                  </p>
                  <div className="border-2 border-dashed border-gray-300 rounded-lg p-4">
                    <input
                      type="file"
                      accept=".csv,.xlsx,.xls"
                      onChange={handleImportFile}
                      className="hidden"
                      id="import-file-input"
                    />
                    <label
                      htmlFor="import-file-input"
                      className="cursor-pointer flex flex-col items-center space-y-2"
                    >
                      <Upload className="h-8 w-8 text-gray-400" />
                      <span className="text-sm text-gray-600">
                        {importFile ? importFile.name : 'Click to select file or drag and drop'}
                      </span>
                      <span className="text-xs text-gray-500">
                        Supports: CSV, Excel (.xlsx, .xls)
                      </span>
                    </label>
                  </div>
                </div>

                {/* File Preview */}
                {importFile && (
                  <div className="bg-gray-50 p-3 rounded-lg">
                    <p className="text-sm text-gray-700">
                      <strong>Selected file:</strong> {importFile.name}
                    </p>
                    <p className="text-sm text-gray-500">
                      Size: {(importFile.size / 1024).toFixed(1)} KB
                    </p>
                  </div>
                )}

                {/* Instructions */}
                <div className="bg-yellow-50 p-3 rounded-lg">
                  <h5 className="text-sm font-medium text-yellow-800 mb-1">Important Notes:</h5>
                  <ul className="text-xs text-yellow-700 space-y-1">
                    <li>• Make sure your file follows the template format exactly</li>
                    <li>• Required fields: first_name OR phone number</li>
                    <li>• Budget amounts should be in numbers (e.g., 5000000 for 50 lakhs)</li>
                    <li>• Use the exact source values: Website, Referral, Facebook, etc.</li>
                    <li>• If source is 'Referral', add referral_name for proper tracking</li>
                    <li>• Registered field should be TRUE or FALSE</li>
                  </ul>
                </div>
              </div>
            </div>
            
            <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 flex justify-end space-x-3">
              <button
                onClick={() => {
                  setShowImportModal(false)
                  setImportFile(null)
                }}
                className="px-4 py-2 text-gray-700 bg-gray-200 rounded-lg hover:bg-gray-300 transition-colors"
                disabled={importing}
              >
                Cancel
              </button>
              <button
                onClick={processImportData}
                disabled={!importFile || importing}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors flex items-center space-x-2 disabled:bg-gray-400 disabled:cursor-not-allowed"
              >
                {importing ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    <span>Importing...</span>
                  </>
                ) : (
                  <>
                    <Upload className="h-4 w-4" />
                    <span>Import Data</span>
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* WhatsApp Group Modal */}
      {showWhatsAppModal && selectedLead && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-md w-full mx-4">
            <div className="px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">Send to WhatsApp Group</h3>
            </div>
            
            <div className="px-6 py-4">
              <div className="mb-4">
                <p className="text-sm text-gray-600 mb-2">Client Details:</p>
                <div className="bg-gray-50 p-3 rounded-md text-sm">
                  <p><strong>Name:</strong> {selectedLead.name || [selectedLead.first_name, selectedLead.last_name].filter(Boolean).join(' ') || 'N/A'}</p>
                  <p><strong>Phone:</strong> {selectedLead.phone || 'N/A'}</p>
                  <p><strong>Email:</strong> {selectedLead.email || 'None'}</p>
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

Client Name: ${selectedLead.name || [selectedLead.first_name, selectedLead.last_name].filter(Boolean).join(' ') || 'N/A'}
Phone Number: ${selectedLead.phone || 'N/A'}
Email: ${selectedLead.email || 'None'}
Preferred Unit: ${selectedLead.preferred_type || ''}
Site Visit Date: ${siteVisitDate}

Regards,
${user?.name || 'Flatrix Team'}`}</pre>
                  </div>
                  <button
                    onClick={async () => {
                      const message = `Hi Team, please find Client details below

Client Name: ${selectedLead.name || [selectedLead.first_name, selectedLead.last_name].filter(Boolean).join(' ') || 'N/A'}
Phone Number: ${selectedLead.phone || 'N/A'}
Email: ${selectedLead.email || 'None'}
Preferred Unit: ${selectedLead.preferred_type || ''}
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
                  setSelectedLead(null)
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

      {/* Persistent New Lead Notifications */}
      {newLeadNotifications.length > 0 && (
        <div className="fixed top-4 right-4 z-50 space-y-2 max-w-sm">
          {newLeadNotifications.map(notification => (
            <div
              key={notification.id}
              className="bg-gradient-to-r from-orange-500 to-red-500 text-white p-4 rounded-lg shadow-2xl border-2 border-white animate-bounce"
            >
              <div className="flex justify-between items-start mb-2">
                <div className="flex items-center gap-2">
                  <div className="text-2xl">🔔</div>
                  <h3 className="font-bold text-lg">New Lead Arrived!</h3>
                </div>
                <button
                  onClick={() => closeNotification(notification.id)}
                  className="ml-2 text-white hover:text-gray-200 text-xl font-bold transition-colors"
                  title="Close notification"
                >
                  ✕
                </button>
              </div>
              <div className="space-y-1 text-sm">
                <p><strong>Name:</strong> {notification.name}</p>
                <p><strong>Phone:</strong> {notification.phone}</p>
                <p><strong>Source:</strong> {notification.source}</p>
                <p className="text-xs opacity-90 mt-2 flex items-center gap-1">
                  ⏰ {notification.timestamp}
                </p>
              </div>
            </div>
          ))}
        </div>
      )}

    </div>
  )
}