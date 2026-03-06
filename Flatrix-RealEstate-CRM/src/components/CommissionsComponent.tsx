'use client'

import { useState, useRef, useEffect } from 'react'
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
  DollarSign,
  CreditCard
} from 'lucide-react'
import { useDeals, useLeads, useCommissions } from '@/hooks/useDatabase'
import { supabase } from '@/lib/supabase'
import toast from 'react-hot-toast'

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M12.031 2c-5.523 0-10.031 4.507-10.031 10.031 0 1.74.443 3.387 1.288 4.816l-1.288 4.153 4.232-1.245c1.389.761 2.985 1.214 4.661 1.214h.004c5.522 0 10.031-4.507 10.031-10.031 0-2.672-1.041-5.183-2.93-7.071-1.892-1.892-4.405-2.937-7.071-2.937zm.004 18.375h-.003c-1.465 0-2.899-.394-4.148-1.14l-.297-.177-3.085.808.821-2.998-.193-.307c-.821-1.302-1.253-2.807-1.253-4.358 0-4.516 3.677-8.192 8.193-8.192 2.186 0 4.239.852 5.784 2.397 1.545 1.545 2.397 3.598 2.397 5.783-.003 4.518-3.68 8.194-8.196 8.194zm4.5-6.143c-.247-.124-1.463-.722-1.69-.805-.227-.083-.392-.124-.558.124-.166.248-.642.805-.786.969-.145.165-.29.186-.537.062-.247-.124-1.045-.385-1.99-1.227-.736-.657-1.233-1.468-1.378-1.715-.145-.247-.016-.381.109-.504.112-.111.247-.29.371-.434.124-.145.166-.248.248-.413.083-.166.042-.31-.021-.434-.062-.124-.558-1.343-.765-1.839-.201-.479-.407-.414-.558-.422-.145-.007-.31-.009-.476-.009-.166 0-.435.062-.662.31-.227.248-.866.847-.866 2.067 0 1.22.889 2.395 1.013 2.56.124.166 1.75 2.667 4.24 3.74.592.256 1.055.408 1.415.523.594.189 1.135.162 1.563.098.476-.071 1.463-.598 1.669-1.175.207-.577.207-1.071.145-1.175-.062-.104-.227-.165-.476-.289z"/>
  </svg>
)

const paymentStatuses = ['PENDING', 'RECEIVED', 'PARTIAL', 'OVERDUE']

const paymentColors = {
  PENDING: 'bg-yellow-100 text-yellow-800 border-yellow-200',
  RECEIVED: 'bg-green-100 text-green-800 border-green-200',
  PARTIAL: 'bg-blue-100 text-blue-800 border-blue-200',
  OVERDUE: 'bg-red-100 text-red-800 border-red-200'
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
      return {
        timestamp: timestampMatch[1],
        content: timestampMatch[2].trim(),
        date: new Date(timestampMatch[1]),
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
  
  return parsedEntries.filter(entry => entry !== null).sort((a, b) => {
    if (!a!.date && !b!.date) return 0
    if (!a!.date) return 1
    if (!b!.date) return -1
    return b!.date!.getTime() - a!.date!.getTime()
  })
}

// Helper function to get the latest note
const getLatestNote = (notes: string, leadCreatedAt?: string) => {
  const parsedNotes = parseNotes(notes, leadCreatedAt)
  if (parsedNotes.length === 0) return null
  
  const latest = parsedNotes[0]
  return latest.content.length > 50 ? latest.content.substring(0, 50) + '...' : latest.content
}

export default function CommissionsComponent() {
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedStatus, setSelectedStatus] = useState('All Payment Status')
  const [selectedAssignee, setSelectedAssignee] = useState('All Assigned')
  const [sortBy, setSortBy] = useState('booked_date')
  const [showEditModal, setShowEditModal] = useState(false)
  const [editingCommission, setEditingCommission] = useState<any>(null)
  const [showNotesHistory, setShowNotesHistory] = useState(false)
  const [selectedCommissionNotes, setSelectedCommissionNotes] = useState<any>(null)
  const [notesHistoryType, setNotesHistoryType] = useState<'regular' | 'management'>('regular')
  const [appendNote, setAppendNote] = useState('')

  const { data: deals, loading: dealsLoading, refetch: refetchDeals } = useDeals()
  const { data: leads, loading: leadsLoading, refetch: refetchLeads } = useLeads()
  const { data: commissions, loading: commissionsLoading, refetch: refetchCommissions } = useCommissions()
  
  // Debug logging
  console.log('All commissions data:', commissions)
  if (commissions && commissions.length > 0) {
    console.log('Sample commission:', commissions[0])
  }
  
  // State to store commission amounts mapped by deal_id
  const [commissionAmounts, setCommissionAmounts] = useState<Record<string, number>>({})
  
  // Fetch fresh commission data on mount and map by deal_id
  useEffect(() => {
    const fetchFreshCommissions = async () => {
      const { data: freshData } = await supabase
        .from('flatrix_commissions')
        .select('*')
      console.log('Fresh commissions from DB:', freshData)
      
      // Create a map of deal_id to amount
      if (freshData) {
        const amountMap: Record<string, number> = {}
        freshData.forEach(comm => {
          if (comm.deal_id && comm.amount) {
            amountMap[comm.deal_id] = comm.amount
          }
        })
        setCommissionAmounts(amountMap)
        console.log('Commission amounts map:', amountMap)
      }
    }
    fetchFreshCommissions()
  }, [])

  // Get all booked conversions (deals with conversion_status = 'BOOKED')
  console.log('[COMMISSIONS] Total leads:', leads?.length);
  console.log('[COMMISSIONS] QUALIFIED leads:', leads?.filter(l => l.status === 'QUALIFIED').length);
  console.log('[COMMISSIONS] Total deals:', deals?.length);
  console.log('[COMMISSIONS] BOOKED deals:', deals?.filter(d => (d as any).conversion_status === 'BOOKED').length);

  const bookedConversionsData = leads?.filter(lead => lead.status === 'QUALIFIED')?.map(lead => {
    const deal = deals?.find(d => d.lead_id === lead.id && (d as any).conversion_status === 'BOOKED')
    const commission = commissions?.find(c => {
      // Check both deal_id as string and as object reference
      return c.deal_id === deal?.id
    })

    if (!deal) {
      // Log which QUALIFIED leads don't have BOOKED deals
      const leadName = (lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ');
      if (leadName?.includes('Naresh') || leadName?.includes('Aparna')) {
        console.log(`[COMMISSIONS] ${leadName} - NO BOOKED DEAL FOUND. Lead ID: ${lead.id}`);
      }
      return null; // Only show booked conversions
    }
    
    // Enhanced debug logging
    if (deal) {
      console.log(`Deal ${deal.id}:`, {
        deal_id: deal.id,
        commission: commission,
        commission_amount: commission?.amount,
        commission_id: commission?.id
      })
    }
    
    // Get commission amount from our direct map
    let commissionAmount = 0
    if (deal && deal.id && commissionAmounts[deal.id]) {
      commissionAmount = commissionAmounts[deal.id]
      console.log(`Using commission amount for deal ${deal.id}: ${commissionAmount}`)
    }
    
    return {
      ...lead,
      ...deal,
      lead_id: lead.id,
      lead_name: (lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown Contact',
      lead_email: lead.email,
      lead_phone: lead.phone,
      lead_source: lead.source,
      lead_notes: lead.notes,
      management_notes: (lead as any).management_notes,
      lead_created_at: lead.created_at,
      lead_preferred_type: (lead as any).preferred_type,
      lead_budget_min: (lead as any).budget_min,
      site_visit_status: (deal as any)?.site_visit_status || 'COMPLETED',
      site_visit_date: (deal as any)?.site_visit_date,
      conversion_status: (deal as any)?.conversion_status,
      conversion_date: (deal as any)?.conversion_date,
      deal_id: deal?.id,
      deal_value: deal?.deal_value || lead.budget_max,
      commission_id: commission?.id,
      payment_status: commission?.status || 'PENDING',
      commission_amount: commissionAmount
    }
  }).filter(Boolean) || []

  // Calculate stats
  const totalCommissions = bookedConversionsData.length
  const totalCommissionAmount = bookedConversionsData.reduce((sum, item) => sum + (item?.commission_amount || 0), 0)
  const paidCommissions = bookedConversionsData.filter(item => (item as any)?.payment_status === 'RECEIVED').length
  const pendingCommissions = bookedConversionsData.filter(item => 
    (item as any)?.payment_status === 'PENDING' || (item as any)?.payment_status === 'PARTIAL'
  ).length

  // Mock percentage changes
  const statsChange = {
    total: '+22.5%',
    amount: '+18.7%',
    paid: '+15.3%',
    pending: '-8.2%'
  }

  const filteredAndSortedCommissions = bookedConversionsData.filter(commission => {
    if (!commission) return false
    
    const searchMatches = !searchTerm || 
      commission.lead_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      commission.lead_email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      commission.lead_phone?.includes(searchTerm) ||
      commission.preferred_location?.toLowerCase().includes(searchTerm.toLowerCase())
    
    const statusMatches = selectedStatus === 'All Payment Status' || commission.payment_status === selectedStatus
    
    return searchMatches && statusMatches
  })?.sort((a, b) => {
    if (!a || !b) return 0
    switch (sortBy) {
      case 'booked_date':
        const aBookedDate = a.conversion_date ? new Date(a.conversion_date).getTime() : 0
        const bBookedDate = b.conversion_date ? new Date(b.conversion_date).getTime() : 0
        return bBookedDate - aBookedDate
      case 'site_visit':
        const aSiteVisitDate = a.site_visit_date ? new Date(a.site_visit_date).getTime() : 0
        const bSiteVisitDate = b.site_visit_date ? new Date(b.site_visit_date).getTime() : 0
        return bSiteVisitDate - aSiteVisitDate
      default:
        const aDefaultDate = a.conversion_date ? new Date(a.conversion_date).getTime() : 0
        const bDefaultDate = b.conversion_date ? new Date(b.conversion_date).getTime() : 0
        return bDefaultDate - aDefaultDate
    }
  })

  const handleEditCommission = async (commission: any) => {
    console.log('Opening edit modal with commission data:', commission)
    
    // If there's a deal_id but no commission_id, try to fetch the commission
    if (commission.deal_id && !commission.commission_id) {
      const { data: existingCommission } = await supabase
        .from('flatrix_commissions')
        .select('*')
        .eq('deal_id', commission.deal_id)
        .single()
      
      if (existingCommission) {
        console.log('Found existing commission:', existingCommission)
        commission.commission_id = existingCommission.id
        commission.commission_amount = existingCommission.amount
        commission.payment_status = existingCommission.status
      }
    }
    
    setEditingCommission({
      ...commission,
      conversion_date: commission.conversion_date ? commission.conversion_date.split('T')[0] : ''
    })
    setAppendNote('')
    setShowEditModal(true)
  }

  const handleUpdateCommission = async (e: React.FormEvent) => {
    e.preventDefault()
    
    try {
      console.log('Updating commission with data:', editingCommission)
      
      let updatedNotes = editingCommission.lead_notes || ''
      
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
      let updatedManagementNotes = (editingCommission as any).management_notes || ''
      if ((editingCommission as any).appendManagementNote?.trim()) {
        const timestamp = new Date().toLocaleString('en-US', { 
          dateStyle: 'short', 
          timeStyle: 'short' 
        })
        updatedManagementNotes = updatedManagementNotes 
          ? `${updatedManagementNotes}\n\n[${timestamp}]:\n${(editingCommission as any).appendManagementNote.trim()}`
          : `[${timestamp}]:\n${(editingCommission as any).appendManagementNote.trim()}`
      }

      // Update the lead notes
      if (editingCommission.lead_id) {
        const { error: leadError } = await supabase
          .from('flatrix_leads')
          .update({ 
            notes: updatedNotes,
            management_notes: updatedManagementNotes
          })
          .eq('id', editingCommission.lead_id)

        if (leadError) {
          console.error('Lead update error:', leadError)
          throw leadError
        }
      }

      // Update deal value
      if (editingCommission.deal_id) {
        const { error: dealError } = await supabase
          .from('flatrix_deals')
          .update({
            deal_value: Number(editingCommission.deal_value) || 0,
            updated_at: new Date().toISOString()
          })
          .eq('id', editingCommission.deal_id)

        if (dealError) {
          console.error('Deal update error:', dealError)
          throw dealError
        }
      }

      // Update or create commission record
      if (editingCommission.commission_id) {
        // Update existing commission
        console.log('Updating commission with ID:', editingCommission.commission_id)
        console.log('Update data:', {
          amount: Number(editingCommission.commission_amount) || 0,
          status: editingCommission.payment_status || 'PENDING'
        })
        
        const { data: updateResult, error: commissionError } = await supabase
          .from('flatrix_commissions')
          .update({
            amount: Number(editingCommission.commission_amount) || 0,
            status: editingCommission.payment_status || 'PENDING',
            updated_at: new Date().toISOString()
          })
          .eq('id', editingCommission.commission_id)
          .select()

        console.log('Update result:', updateResult)
        
        if (commissionError) {
          console.error('Commission update error:', commissionError)
          throw commissionError
        }
      } else if (editingCommission.deal_id && Number(editingCommission.commission_amount) > 0) {
        // Check if commission record already exists for this deal
        const { data: existingCommission } = await supabase
          .from('flatrix_commissions')
          .select('id')
          .eq('deal_id', editingCommission.deal_id)
          .single()

        if (existingCommission) {
          // Update existing commission
          console.log('Updating existing commission with ID:', existingCommission.id)
          console.log('Update data:', {
            amount: Number(editingCommission.commission_amount) || 0,
            status: editingCommission.payment_status || 'PENDING'
          })
          
          const { data: updateResult, error: commissionError } = await supabase
            .from('flatrix_commissions')
            .update({
              amount: Number(editingCommission.commission_amount) || 0,
              status: editingCommission.payment_status || 'PENDING',
              updated_at: new Date().toISOString()
            })
            .eq('id', existingCommission.id)
            .select()

          console.log('Update result:', updateResult)

          if (commissionError) {
            console.error('Commission update error:', commissionError)
            throw commissionError
          }
        } else {
          // Get a default channel partner (first active one)
          const { data: defaultPartner } = await supabase
            .from('flatrix_channel_partners')
            .select('id')
            .eq('is_active', true)
            .limit(1)
            .single()

          if (defaultPartner) {
            // Create new commission record if it doesn't exist
            const { error: commissionError } = await supabase
              .from('flatrix_commissions')
              .insert({
                deal_id: editingCommission.deal_id,
                channel_partner_id: defaultPartner.id,
                amount: Number(editingCommission.commission_amount) || 0,
                percentage: 5.0, // Default 5% commission
                status: editingCommission.payment_status || 'PENDING',
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
              })

            if (commissionError) {
              console.error('Commission insert error:', commissionError)
              throw commissionError
            }
          }
        }
      }

      toast.success('Commission updated successfully!')
      setShowEditModal(false)
      setEditingCommission(null)
      setAppendNote('')
      
      // Refresh the commission amounts map
      const { data: freshData } = await supabase
        .from('flatrix_commissions')
        .select('*')
      
      if (freshData) {
        const amountMap: Record<string, number> = {}
        freshData.forEach(comm => {
          if (comm.deal_id && comm.amount) {
            amountMap[comm.deal_id] = comm.amount
          }
        })
        setCommissionAmounts(amountMap)
      }
      
      // Also refresh the main data
      refetchCommissions()
      refetchLeads()
      refetchDeals()
    } catch (error) {
      console.error('Error updating commission:', error)
      toast.error(`Failed to update commission: ${error instanceof Error ? error.message : 'Unknown error'}`)
    }
  }

  if (dealsLoading || leadsLoading || commissionsLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-lg text-gray-600">Loading commissions...</div>
      </div>
    )
  }

  return (
    <div>
      <div className="mb-6 flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Commission Management</h1>
          <p className="text-gray-600 mt-2">Track and manage commission payments for booked conversions</p>
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
        {/* Total Commissions */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Total Commissions</p>
              <h3 className="text-3xl font-bold text-gray-900">{totalCommissions}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.total}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-blue-100 p-3 rounded-lg">
              <DollarSign className="h-6 w-6 text-blue-600" />
            </div>
          </div>
        </div>

        {/* Total Commission Amount */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Total Amount</p>
              <h3 className="text-3xl font-bold text-gray-900">₹{totalCommissionAmount.toLocaleString()}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.amount}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-green-100 p-3 rounded-lg">
              <CreditCard className="h-6 w-6 text-green-600" />
            </div>
          </div>
        </div>

        {/* Paid Commissions */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Paid Commissions</p>
              <h3 className="text-3xl font-bold text-gray-900">{paidCommissions}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">{statsChange.paid}</span>
                <span className="text-xs text-gray-500 ml-2">from last month</span>
              </div>
            </div>
            <div className="bg-purple-100 p-3 rounded-lg">
              <CheckCircle className="h-6 w-6 text-purple-600" />
            </div>
          </div>
        </div>

        {/* Pending Commissions */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Pending Commissions</p>
              <h3 className="text-3xl font-bold text-gray-900">{pendingCommissions}</h3>
              <div className="flex items-center mt-2">
                <TrendingUp className="h-4 w-4 text-red-600 mr-1 rotate-180" />
                <span className="text-sm text-red-600 font-medium">{statsChange.pending}</span>
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
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Commission Search & Filters</h2>
          <p className="text-sm text-gray-600 mb-4">Filter and search through your commission records</p>
          
          <div className="flex flex-col gap-4">
            <div className="w-full">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search commissions by name, contact, or city..."
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
                  <option>All Payment Status</option>
                  {paymentStatuses.map(status => (
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
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <select
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option value="booked_date">Sort by Booked Date</option>
                  <option value="site_visit">Sort by Site Visit</option>
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Commission List Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Commission List</h2>
            <p className="text-sm text-gray-600 mt-1">All booked conversion commissions</p>
          </div>
        </div>

        <div>
          {filteredAndSortedCommissions.length === 0 ? (
            <div className="text-center text-gray-500 py-12">
              <DollarSign className="h-12 w-12 text-gray-300 mx-auto mb-3" />
              <p className="text-lg font-medium mb-1">No commissions found</p>
              <p className="text-sm text-gray-400">
                {searchTerm || selectedStatus !== 'All Payment Status' 
                  ? 'Try adjusting your filters' 
                  : 'Commissions will appear here when conversions are booked'}
              </p>
            </div>
          ) : (
            <div className="divide-y divide-gray-200">
              {filteredAndSortedCommissions.map((commission: any) => (
                <div key={commission.lead_id} className="px-4 sm:px-6 py-4 hover:bg-gray-50 transition-colors">
                  <div className="hidden md:flex items-center justify-between">
                    <div className="flex items-center space-x-8 flex-1">
                      {/* Profile Icon */}
                      <div className="flex-shrink-0">
                        <div className="w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center">
                          <User className="h-5 w-5 text-blue-600" />
                        </div>
                      </div>

                      {/* Name Column */}
                      <div className="min-w-[200px]">
                        <h3 className="text-base font-semibold text-gray-900">
                          {commission.lead_name || 'No Name'}
                        </h3>
                        <div className="flex gap-3 mt-1">
                          {commission.lead_preferred_type && (
                            <div className="text-xs text-gray-600">
                              <span className="font-medium">Preferred Unit:</span> {commission.lead_preferred_type}
                            </div>
                          )}
                          {commission.lead_budget_min && (
                            <div className="text-xs text-gray-600">
                              <span className="font-medium">Budget:</span> ₹{commission.lead_budget_min.toLocaleString()}
                            </div>
                          )}
                        </div>
                        {commission.preferred_location && (
                          <div className="text-xs text-gray-500 mt-1">
                            {commission.preferred_location}
                          </div>
                        )}
                      </div>

                      {/* Contact Details Column */}
                      <div className="min-w-[200px]">
                        <div className="text-sm text-gray-900">
                          {commission.lead_phone || 'No phone'}
                        </div>
                        <div className="text-xs text-gray-500 truncate">
                          {commission.lead_email || 'No email'}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">
                          Source: {getDisplaySource(commission.lead_source)}
                          {commission.lead_source === 'Referral' && commission.lead_notes?.includes('Referral Name:') && (
                            <span className="text-blue-600 font-medium">
                              {' • ' + commission.lead_notes.split('Referral Name:')[1]?.split('\n')[0]?.trim()}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Commission Details Column */}
                      <div className="min-w-[150px]">
                        <div className="text-sm text-gray-600 mb-1">
                          Deal Value: <span className="text-gray-900 font-medium">
                            ₹{commission.deal_value?.toLocaleString() || 'Not set'}
                          </span>
                        </div>
                        <div className="text-sm text-gray-600 mb-1">
                          Commission: <span className="text-gray-900 font-medium">
                            ₹{commission.commission_amount?.toLocaleString() || '0'}
                          </span>
                        </div>
                        <div className="text-sm text-gray-600">
                          Assigned to: <span className="text-gray-900 font-medium">Dinki</span>
                        </div>
                      </div>

                      {/* Payment Status Column */}
                      <div className="min-w-[140px]">
                        <div className="text-xs text-gray-700 mb-1">
                          Payment Status: <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${paymentColors[commission.payment_status as keyof typeof paymentColors] || 'bg-gray-100 text-gray-800'}`}>
                            {commission.payment_status?.replace('_', ' ') || 'PENDING'}
                          </span>
                        </div>
                        {commission.conversion_date && (
                          <div className="text-xs text-gray-500 mb-1">
                            Booked on: <span className="text-gray-900 font-medium">
                              {new Date(commission.conversion_date).toLocaleDateString('en-US', {
                                month: 'short',
                                day: 'numeric',
                                year: 'numeric'
                              })}
                            </span>
                          </div>
                        )}
                        <div className="text-xs text-gray-700">
                          Site Visit: <span className="text-gray-900 font-medium">
                            {commission.site_visit_date 
                              ? new Date(commission.site_visit_date).toLocaleDateString('en-US', { 
                                  month: 'short', 
                                  day: 'numeric',
                                  year: 'numeric' 
                                })
                              : 'Not set'}
                          </span>
                        </div>
                      </div>
                    </div>

                    {/* Actions */}
                    <div className="flex items-center space-x-2 ml-4">
                      {commission.lead_phone && (
                        <>
                          <a
                            href={`tel:${commission.lead_phone}`}
                            className="p-2 text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded-lg transition-colors border border-blue-200"
                            title="Call"
                          >
                            <Phone className="h-4 w-4" />
                          </a>
                          <a
                            href={`https://wa.me/${commission.lead_phone.replace(/[^0-9]/g, '')}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-2 text-green-600 hover:text-green-700 hover:bg-green-50 rounded-lg transition-colors border border-green-200"
                            title="WhatsApp"
                          >
                            <WhatsAppIcon className="h-4 w-4" />
                          </a>
                        </>
                      )}
                      {commission.lead_email && (
                        <a
                          href={`mailto:${commission.lead_email}`}
                          className="p-2 text-gray-600 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors border border-gray-200"
                          title="Email"
                        >
                          <Mail className="h-4 w-4" />
                        </a>
                      )}
                      <button
                        onClick={() => handleEditCommission(commission)}
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
                          setSelectedCommissionNotes(commission)
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
                          {getLatestNote(commission.lead_notes, commission.lead_created_at) || 'No notes added'}
                        </span>
                        {commission.lead_notes && parseNotes(commission.lead_notes, commission.lead_created_at).length > 1 && (
                          <button
                            onClick={() => {
                              setSelectedCommissionNotes(commission)
                              setNotesHistoryType('regular')
                              setShowNotesHistory(true)
                            }}
                            className="text-xs text-blue-600 hover:text-blue-700 ml-1 hover:underline"
                          >
                            (+{parseNotes(commission.lead_notes, commission.lead_created_at).length - 1} more)
                          </button>
                        )}
                      </div>
                    </div>

                    {/* Management Notes */}
                    <div className="flex items-start space-x-2 pt-2 border-t border-gray-100">
                      <button
                        onClick={() => {
                          setSelectedCommissionNotes(commission)
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
                          {getLatestNote((commission as any).management_notes, commission.lead_created_at) || 'No management notes'}
                        </span>
                        {(commission as any).management_notes && parseNotes((commission as any).management_notes, commission.lead_created_at).length > 1 && (
                          <button
                            onClick={() => {
                              setSelectedCommissionNotes(commission)
                              setNotesHistoryType('management')
                              setShowNotesHistory(true)
                            }}
                            className="text-xs text-purple-600 hover:text-purple-700 ml-1 hover:underline"
                          >
                            (+{parseNotes((commission as any).management_notes, commission.lead_created_at).length - 1} more)
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

        {filteredAndSortedCommissions.length > 0 && (
          <div className="px-6 py-3 bg-gray-50 border-t text-sm text-gray-600">
            Showing {filteredAndSortedCommissions.length} of {bookedConversionsData.length} commissions
          </div>
        )}
      </div>

      {/* Edit Commission Modal */}
      {showEditModal && editingCommission && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {setShowEditModal(false); setEditingCommission(null); setAppendNote('')}}
        >
          <div 
            className="bg-white rounded-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-6 border-b border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900">Update Commission Payment</h2>
              <button
                onClick={() => {setShowEditModal(false); setEditingCommission(null); setAppendNote('')}}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={handleUpdateCommission} className="p-6">
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Lead Name</label>
                  <input 
                    type="text" 
                    value={editingCommission.lead_name || ''}
                    disabled
                    className="w-full px-3 py-2 bg-gray-50 border border-gray-300 rounded-lg" 
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Deal Value</label>
                  <input 
                    type="number" 
                    value={editingCommission.deal_value || ''}
                    onChange={(e) => setEditingCommission({...editingCommission, deal_value: parseFloat(e.target.value) || 0})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter deal value"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Commission Amount</label>
                  <input 
                    type="number" 
                    value={editingCommission.commission_amount || ''}
                    onChange={(e) => setEditingCommission({...editingCommission, commission_amount: parseFloat(e.target.value) || 0})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter commission amount"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    <div className="flex items-center space-x-2">
                      <CreditCard className="h-4 w-4" />
                      <span>Payment Status</span>
                    </div>
                  </label>
                  <select 
                    value={editingCommission.payment_status || 'PENDING'}
                    onChange={(e) => setEditingCommission({...editingCommission, payment_status: e.target.value})}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    {paymentStatuses.map(status => (
                      <option key={status} value={status}>{status.replace('_', ' ')}</option>
                    ))}
                  </select>
                  <p className="text-xs text-gray-500 mt-1">
                    Track whether the commission payment has been received.
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
                    type="text" 
                    value={editingCommission.conversion_date ? new Date(editingCommission.conversion_date).toLocaleDateString('en-US') : 'Not set'}
                    disabled
                    className="w-full px-3 py-2 bg-gray-50 border border-gray-300 rounded-lg" 
                  />
                </div>

                {/* Previous Notes - Read Only */}
                {editingCommission.lead_notes && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      <div className="flex items-center space-x-2">
                        <FileText className="h-4 w-4" />
                        <span>Previous Notes History</span>
                      </div>
                    </label>
                    <div className="w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg max-h-32 overflow-y-auto">
                      <pre className="text-sm text-gray-600 whitespace-pre-wrap font-sans">{editingCommission.lead_notes}</pre>
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
                  {(editingCommission as any).management_notes && (
                    <div className="mb-4">
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        <div className="flex items-center space-x-2">
                          <FileText className="h-4 w-4" />
                          <span>Previous Management Notes History</span>
                        </div>
                      </label>
                      <div className="w-full px-3 py-2 bg-purple-50 border border-purple-200 rounded-lg max-h-32 overflow-y-auto">
                        <pre className="text-sm text-gray-600 whitespace-pre-wrap font-sans">{(editingCommission as any).management_notes}</pre>
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
                      value={(editingCommission as any).appendManagementNote || ''}
                      onChange={(e) => setEditingCommission({...editingCommission, appendManagementNote: e.target.value})}
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
                  onClick={() => {setShowEditModal(false); setEditingCommission(null); setAppendNote('')}}
                  className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition"
                >
                  Cancel
                </button>
                <button 
                  type="submit"
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
                >
                  Update Commission
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Notes History Modal */}
      {showNotesHistory && selectedCommissionNotes && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {
            setShowNotesHistory(false)
            setSelectedCommissionNotes(null)
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
                  {selectedCommissionNotes.lead_name || 'Commission'}
                </p>
              </div>
              <button
                onClick={() => {
                  setShowNotesHistory(false)
                  setSelectedCommissionNotes(null)
                }}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            
            <div className="p-6 max-h-96 overflow-y-auto">
              {(() => {
                const notesToShow = notesHistoryType === 'management' 
                  ? (selectedCommissionNotes as any).management_notes 
                  : selectedCommissionNotes.lead_notes
                const parsedNotesToShow = parseNotes(notesToShow, selectedCommissionNotes.lead_created_at)
                const borderColor = notesHistoryType === 'management' ? 'border-purple-200' : 'border-blue-200'
                const dotColor = notesHistoryType === 'management' ? 'bg-purple-500' : 'bg-blue-500'
                const noNotesIcon = notesHistoryType === 'management' ? FileText : MessageSquare
                const NoNotesIcon = noNotesIcon
                
                return parsedNotesToShow.length === 0 ? (
                  <div className="text-center text-gray-500 py-8">
                    <NoNotesIcon className="h-12 w-12 text-gray-300 mx-auto mb-3" />
                    <p className="text-lg font-medium mb-1">No {notesHistoryType === 'management' ? 'management ' : ''}notes available</p>
                    <p className="text-sm">No {notesHistoryType === 'management' ? 'management ' : ''}notes have been added to this commission yet.</p>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {parsedNotesToShow.map((note, index) => (
                      <div key={index} className={`border-l-4 ${borderColor} pl-4 pb-4`}>
                        <div className="flex items-start justify-between mb-2">
                          <div className="flex items-center space-x-2">
                            <div className={`w-2 h-2 ${dotColor} rounded-full`}></div>
                            <span className="text-sm font-medium text-gray-900">
                              {note.date ? note.date.toLocaleDateString('en-US', {
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
                          {note.content}
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
                  notesHistoryType === 'management' ? (selectedCommissionNotes as any).management_notes : selectedCommissionNotes.lead_notes, 
                  selectedCommissionNotes.lead_created_at
                ).length}</span>
                <span>Click outside to close</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}