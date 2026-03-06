'use client'

import { useState, useRef, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { 
  Search, 
  Calendar,
  Clock,
  User,
  Phone,
  Mail,
  MapPin,
  TrendingUp,
  CheckCircle,
  AlertTriangle,
  Zap,
  Brain,
  Target,
  Filter,
  ChevronDown,
  Sparkles,
  Users,
  BarChart3,
  TrendingDown,
  Bell,
  Star,
  ArrowRight,
  Lightbulb,
  MessageSquare,
  RefreshCw,
  MessageCircle,
  Send,
  Copy,
  Edit3,
  X
} from 'lucide-react'
import { useDeals, useLeads } from '@/hooks/useDatabase'
import { supabase } from '@/lib/supabase'
import toast from 'react-hot-toast'

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M12.031 2c-5.523 0-10.031 4.507-10.031 10.031 0 1.74.443 3.387 1.288 4.816l-1.288 4.153 4.232-1.245c1.389.761 2.985 1.214 4.661 1.214h.004c5.522 0 10.031-4.507 10.031-10.031 0-2.672-1.041-5.183-2.93-7.071-1.892-1.892-4.405-2.937-7.071-2.937zm.004 18.375h-.003c-1.465 0-2.899-.394-4.148-1.14l-.297-.177-3.085.808.821-2.998-.193-.307c-.821-1.302-1.253-2.807-1.253-4.358 0-4.516 3.677-8.192 8.193-8.192 2.186 0 4.239.852 5.784 2.397 1.545 1.545 2.397 3.598 2.397 5.783-.003 4.518-3.68 8.194-8.196 8.194zm4.5-6.143c-.247-.124-1.463-.722-1.69-.805-.227-.083-.392-.124-.558.124-.166.248-.642.805-.786.969-.145.165-.29.186-.537.062-.247-.124-1.045-.385-1.99-1.227-.736-.657-1.233-1.468-1.378-1.715-.145-.247-.016-.381.109-.504.112-.111.247-.29.371-.434.124-.145.166-.248.248-.413.083-.166.042-.31-.021-.434-.062-.124-.558-1.343-.765-1.839-.201-.479-.407-.414-.558-.422-.145-.007-.31-.009-.476-.009-.166 0-.435.062-.662.31-.227.248-.866.847-.866 2.067 0 1.22.889 2.395 1.013 2.56.124.166 1.75 2.667 4.24 3.74.592.256 1.055.408 1.415.523.594.189 1.135.162 1.563.098.476-.071 1.463-.598 1.669-1.175.207-.577.207-1.071.145-1.175-.062-.104-.227-.165-.476-.289z"/>
  </svg>
)

const priorityColors: { [key: string]: string } = {
  HIGH: 'bg-red-100 text-red-800 border-red-200',
  MEDIUM: 'bg-yellow-100 text-yellow-800 border-yellow-200',
  LOW: 'bg-green-100 text-green-800 border-green-200'
}

const activityTypeColors: { [key: string]: string } = {
  LEADS: 'bg-blue-100 text-blue-800 border-blue-200',
  DEALS: 'bg-purple-100 text-purple-800 border-purple-200',
  'SITE VISITS': 'bg-green-100 text-green-800 border-green-200'
}

// AI-powered insights and recommendations
const generateAIInsights = (activities: any[]) => {
  const insights = []
  
  const overdueCounts = activities.filter(a => a.isOverdue).length
  const todayCount = activities.filter(a => a.isToday).length
  const highPriorityCount = activities.filter(a => a.priority === 'HIGH').length
  
  // Overdue analysis
  if (overdueCounts > 0) {
    insights.push({
      type: 'warning',
      icon: AlertTriangle,
      title: 'Overdue Follow-ups Detected',
      message: `You have ${overdueCounts} overdue follow-ups that need immediate attention. Prioritizing these could recover potential lost opportunities.`,
      action: 'Review Overdue Items',
      color: 'text-red-600'
    })
  }
  
  // Today's focus
  if (todayCount > 0) {
    insights.push({
      type: 'info',
      icon: Target,
      title: 'Today\'s Focus Areas',
      message: `${todayCount} follow-ups scheduled for today. Completing these on time increases conversion probability by 40%.`,
      action: 'View Today\'s Tasks',
      color: 'text-blue-600'
    })
  }
  
  // High priority recommendations
  if (highPriorityCount > 0) {
    insights.push({
      type: 'priority',
      icon: Star,
      title: 'High-Value Opportunities',
      message: `${highPriorityCount} high-priority leads need follow-up. These represent your highest conversion potential.`,
      action: 'Focus on High Priority',
      color: 'text-purple-600'
    })
  }
  
  // Smart recommendations based on patterns
  const leadActivities = activities.filter(a => a.type === 'LEADS')
  const dealActivities = activities.filter(a => a.type === 'DEALS')
  
  if (leadActivities.length > dealActivities.length * 2) {
    insights.push({
      type: 'strategy',
      icon: Lightbulb,
      title: 'Lead Qualification Opportunity',
      message: 'High lead volume detected. Consider implementing lead scoring to focus on qualified prospects.',
      action: 'Optimize Lead Process',
      color: 'text-green-600'
    })
  }
  
  return insights
}

// AI-powered activity prioritization
const calculatePriority = (activity: any) => {
  let score = 0
  
  // Recency scoring
  const daysSinceCreated = Math.floor((new Date().getTime() - new Date(activity.created_at).getTime()) / (1000 * 60 * 60 * 24))
  if (daysSinceCreated > 7) score += 30
  else if (daysSinceCreated > 3) score += 20
  else score += 10
  
  // Lead source scoring
  const highValueSources = ['referral', 'google_ads', 'website']
  if (highValueSources.includes(activity.source?.toLowerCase())) score += 25
  
  // Budget scoring
  if (activity.budget_max > 5000000) score += 30 // 50L+
  else if (activity.budget_max > 3000000) score += 20 // 30L+
  else if (activity.budget_max > 1000000) score += 10 // 10L+
  
  // Overdue scoring
  if (activity.isOverdue) score += 40
  
  // Deal stage scoring
  if (activity.type === 'DEALS') score += 20
  if (activity.type === 'SITE VISITS') score += 25
  
  // Return priority level
  if (score >= 60) return 'HIGH'
  if (score >= 35) return 'MEDIUM'
  return 'LOW'
}

// AI-powered message generation based on activity context
const generateFollowUpMessage = (activity: any) => {
  if (!activity) {
    return 'Error: No activity data available'
  }
  
  const { contact_name, type, isOverdue, priority, budget_max, location, source } = activity
  
  const name = contact_name || 'there'
  const currentHour = new Date().getHours()
  const timeOfDay = currentHour < 12 ? 'Good morning' : currentHour < 17 ? 'Good afternoon' : 'Good evening'
  
  let baseMessage = `${timeOfDay} ${name},\n\n`
  
  // Context-based message generation
  try {
    if (type === 'LEADS') {
      if (isOverdue) {
        baseMessage += `I hope you're doing well. I wanted to follow up on your interest in properties in ${location || 'your preferred area'}. `
        baseMessage += `I understand that finding the right property takes time, and I'm here to help make the process easier for you.\n\n`
        
        if (budget_max && typeof budget_max === 'number') {
          baseMessage += `Based on your budget of ₹${budget_max.toLocaleString()}, I have some excellent options that might interest you. `
        }
        
        baseMessage += `Would you be available for a quick call today to discuss some new properties that match your requirements? `
        baseMessage += `I can also share detailed brochures and floor plans via WhatsApp if that's more convenient.\n\n`
      } else {
        baseMessage += `Thank you for your interest in our properties. I wanted to reach out and see if you have any questions about the options we discussed.\n\n`
        
        if (source === 'referral') {
          baseMessage += `Since you came through a referral, I'd love to ensure you receive the best possible service and find exactly what you're looking for.\n\n`
        }
        
        baseMessage += `Would you like to schedule a site visit to see some properties firsthand? I can arrange a convenient time that works for your schedule.\n\n`
      }
    } else if (type === 'DEALS') {
      baseMessage += `I hope you're excited about the property options we've been discussing. `
      
      if (activity.site_visit_status === 'SCHEDULED') {
        baseMessage += `I wanted to confirm our upcoming site visit and see if you have any specific requirements or questions before we meet.\n\n`
        baseMessage += `Please let me know if the scheduled time still works for you, or if you'd prefer to reschedule. `
        baseMessage += `I'll make sure to show you properties that align perfectly with your preferences and budget.\n\n`
      } else {
        baseMessage += `I'd love to schedule a site visit so you can experience the properties and amenities firsthand. `
        baseMessage += `Seeing the actual space, layout, and surroundings often helps in making the final decision.\n\n`
        baseMessage += `When would be a good time for you this week? I can arrange visits for multiple properties in one go to save your time.\n\n`
      }
    } else if (type === 'SITE VISITS') {
      baseMessage += `I hope you enjoyed our site visit and found the properties impressive. `
      baseMessage += `I wanted to follow up on your thoughts and see if you have any questions about what we saw.\n\n`
      
      if (priority === 'HIGH') {
        baseMessage += `The properties we visited are in high demand, and I'd hate for you to miss out on a great opportunity. `
        baseMessage += `If any of them caught your interest, I'd be happy to discuss the next steps and help you secure your dream home.\n\n`
      }
      
      baseMessage += `I'm here to assist with any additional information you might need - be it pricing details, payment plans, loan assistance, or legal documentation.\n\n`
    } else {
      // Default message for unknown types
      baseMessage += `Thank you for your interest in our properties. I wanted to reach out and see how we can assist you further.\n\n`
      baseMessage += `Please feel free to contact me if you have any questions or would like to schedule a meeting.\n\n`
    }
  } catch (error) {
    console.error('Error in message generation logic:', error)
    baseMessage += `Thank you for your interest in our properties. I wanted to reach out and see how we can assist you further.\n\n`
    baseMessage += `Please feel free to contact me if you have any questions or would like to schedule a meeting.\n\n`
  }
  
  // Closing based on priority
  if (priority === 'HIGH') {
    baseMessage += `Please feel free to call me directly at your convenience. I'm committed to helping you find the perfect property.\n\n`
  } else {
    baseMessage += `Looking forward to hearing from you soon.\n\n`
  }
  
  baseMessage += `Best regards,\nDinki\nFlatrix Real Estate\n📞 Available for calls and WhatsApp`
  
  return baseMessage
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

export default function ActivitiesComponent() {
  const router = useRouter()
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedFilter, setSelectedFilter] = useState('ALL')
  const [selectedPriority, setSelectedPriority] = useState('ALL')
  const [selectedAgent, setSelectedAgent] = useState('ALL')
  const [selectedDate, setSelectedDate] = useState('')
  const [showAIInsights, setShowAIInsights] = useState(true)
  const [showMessageModal, setShowMessageModal] = useState(false)
  const [selectedActivity, setSelectedActivity] = useState<any>(null)
  const [generatedMessage, setGeneratedMessage] = useState('')
  const [isGenerating, setIsGenerating] = useState(false)
  const [showNotesHistory, setShowNotesHistory] = useState(false)
  const [selectedActivityNotes, setSelectedActivityNotes] = useState<any>(null)
  const [expandedNotes, setExpandedNotes] = useState<Set<string>>(new Set())
  const [hoveredNote, setHoveredNote] = useState<string | null>(null)

  const { data: deals, loading: dealsLoading, refetch: refetchDeals } = useDeals()
  const { data: leads, loading: leadsLoading, refetch: refetchLeads } = useLeads()

  // Handle AI insight actions
  const handleInsightAction = (action: string) => {
    switch (action) {
      case 'Review Overdue Items':
        // Filter to show only overdue activities
        setSelectedFilter('ALL')
        setSelectedPriority('ALL')
        setSearchTerm('')
        setSelectedDate('')
        // Scroll to activities list
        setTimeout(() => {
          const activitiesSection = document.getElementById('activities-list')
          if (activitiesSection) {
            activitiesSection.scrollIntoView({ behavior: 'smooth' })
          }
        }, 100)
        toast.success('Showing overdue follow-ups that need immediate attention')
        break
        
      case 'Focus on High Priority':
        // Filter to show only high priority activities
        setSelectedFilter('ALL')
        setSelectedPriority('HIGH')
        setSearchTerm('')
        setSelectedDate('')
        setTimeout(() => {
          const activitiesSection = document.getElementById('activities-list')
          if (activitiesSection) {
            activitiesSection.scrollIntoView({ behavior: 'smooth' })
          }
        }, 100)
        toast.success('Filtering to show high-priority opportunities')
        break
        
      case 'Optimize Lead Process':
        // Filter to show lead activities
        setSelectedFilter('LEADS')
        setSelectedPriority('ALL')
        setSearchTerm('')
        setSelectedDate('')
        setTimeout(() => {
          const activitiesSection = document.getElementById('activities-list')
          if (activitiesSection) {
            activitiesSection.scrollIntoView({ behavior: 'smooth' })
          }
        }, 100)
        toast.success('Showing lead activities to help optimize your process')
        break
        
      default:
        toast('This insight action is coming soon!', { icon: 'ℹ️' })
    }
  }

  // Aggregate all activities from different sources
  const allActivities: any[] = []
  
  // Add leads with follow-up dates (exclude disqualified)
  if (leads) {
    leads.forEach(lead => {
      if (lead.next_followup_date && lead.status !== 'DISQUALIFIED' && (lead.status === 'NEW' || lead.status === 'CONTACTED')) {
        const followupDate = new Date(lead.next_followup_date)
        const today = new Date()
        const isOverdue = followupDate < today
        const isToday = followupDate.toDateString() === today.toDateString()
        
        allActivities.push({
          id: `lead-${lead.id}`,
          type: 'LEADS',
          title: `Follow up with ${(lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Lead'}`,
          subtitle: `Lead in ${lead.status} status`,
          followup_date: lead.next_followup_date,
          contact_name: (lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown Contact',
          contact_phone: lead.phone,
          contact_email: lead.email,
          source: lead.source,
          location: lead.preferred_location,
          budget_max: lead.budget_max,
          created_at: lead.created_at,
          notes: lead.notes,
          assigned_to: (lead as any).assigned_to?.name || 'Unassigned',
          assigned_to_id: lead.assigned_to_id,
          isOverdue,
          isToday,
          priority: calculatePriority({ ...lead, isOverdue, type: 'LEADS' }),
          record_id: lead.id,
          tab_location: 'leads'
        })
      }
    })
  }
  
  // Add deals with follow-up dates
  if (deals && leads) {
    deals.forEach(deal => {
      const dealAny = deal as any
      const lead = leads.find(l => l.id === deal.lead_id)
      if (lead && lead.status !== 'DISQUALIFIED' && lead.next_followup_date && (dealAny.site_visit_status === 'NOT_VISITED' || dealAny.site_visit_status === 'SCHEDULED')) {
        const followupDate = new Date(lead.next_followup_date)
        const today = new Date()
        const isOverdue = followupDate < today
        const isToday = followupDate.toDateString() === today.toDateString()
        
        allActivities.push({
          id: `deal-${deal.id}`,
          type: 'DEALS',
          title: `Deal follow-up: ${(lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Contact'}`,
          subtitle: `Site visit ${dealAny.site_visit_status?.toLowerCase().replace('_', ' ')}`,
          followup_date: lead.next_followup_date,
          contact_name: (lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown Contact',
          contact_phone: lead.phone,
          contact_email: lead.email,
          source: lead.source,
          location: lead.preferred_location,
          budget_max: lead.budget_max,
          deal_value: deal.deal_value,
          site_visit_status: dealAny.site_visit_status,
          created_at: deal.created_at,
          notes: lead.notes,
          assigned_to: (lead as any).assigned_to?.name || 'Unassigned',
          assigned_to_id: lead.assigned_to_id,
          isOverdue,
          isToday,
          priority: calculatePriority({ ...lead, ...deal, isOverdue, type: 'DEALS' }),
          record_id: deal.id,
          tab_location: 'deals'
        })
      }
    })
  }
  
  // Add site visits with follow-up dates (completed site visits needing conversion follow-up)
  if (deals && leads) {
    deals.forEach(deal => {
      const dealAny = deal as any
      const lead = leads.find(l => l.id === deal.lead_id)
      if (lead && lead.status !== 'DISQUALIFIED' && lead.next_followup_date && dealAny.site_visit_status === 'COMPLETED' && dealAny.conversion_status === 'PENDING') {
        const followupDate = new Date(lead.next_followup_date)
        const today = new Date()
        const isOverdue = followupDate < today
        const isToday = followupDate.toDateString() === today.toDateString()
        
        allActivities.push({
          id: `sitevisit-${deal.id}`,
          type: 'SITE VISITS',
          title: `Conversion follow-up: ${(lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Contact'}`,
          subtitle: 'Site visit completed, pending booking decision',
          followup_date: lead.next_followup_date,
          contact_name: (lead as any).name || [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown Contact',
          contact_phone: lead.phone,
          contact_email: lead.email,
          source: lead.source,
          location: lead.preferred_location,
          budget_max: lead.budget_max,
          deal_value: deal.deal_value,
          site_visit_date: dealAny.site_visit_date,
          conversion_status: dealAny.conversion_status,
          created_at: deal.created_at,
          notes: lead.notes,
          assigned_to: (lead as any).assigned_to?.name || 'Unassigned',
          assigned_to_id: lead.assigned_to_id,
          isOverdue,
          isToday,
          priority: calculatePriority({ ...lead, ...deal, isOverdue, type: 'SITE VISITS' }),
          record_id: deal.id,
          tab_location: 'site-visits'
        })
      }
    })
  }
  
  // Sort activities by priority and date
  const sortedActivities = allActivities.sort((a, b) => {
    // First sort by priority
    const priorityOrder: { [key: string]: number } = { HIGH: 3, MEDIUM: 2, LOW: 1 }
    const priorityDiff = (priorityOrder[b.priority] || 0) - (priorityOrder[a.priority] || 0)
    if (priorityDiff !== 0) return priorityDiff
    
    // Then by date (overdue first, then today, then future)
    if (a.isOverdue && !b.isOverdue) return -1
    if (!a.isOverdue && b.isOverdue) return 1
    if (a.isToday && !b.isToday) return -1
    if (!a.isToday && b.isToday) return 1
    
    // Finally by follow-up date
    return new Date(a.followup_date).getTime() - new Date(b.followup_date).getTime()
  })
  
  // Generate AI insights
  const aiInsights = generateAIInsights(sortedActivities)

  // Get unique agents for filter
  const uniqueAgents = Array.from(new Set(sortedActivities.map(a => a.assigned_to).filter(Boolean)))
    .sort()

  // Filter activities
  const filteredActivities = sortedActivities.filter(activity => {
    const searchMatches = !searchTerm ||
      activity.contact_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      activity.title?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      activity.location?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      activity.assigned_to?.toLowerCase().includes(searchTerm.toLowerCase())

    const filterMatches = selectedFilter === 'ALL' || activity.type === selectedFilter
    const priorityMatches = selectedPriority === 'ALL' || activity.priority === selectedPriority
    const agentMatches = selectedAgent === 'ALL' || activity.assigned_to === selectedAgent

    let dateMatches = true
    if (selectedDate) {
      const activityDate = new Date(activity.followup_date).toDateString()
      const filterDate = new Date(selectedDate).toDateString()
      dateMatches = activityDate === filterDate
    }

    return searchMatches && filterMatches && priorityMatches && agentMatches && dateMatches
  })
  
  // Calculate stats
  const totalActivities = sortedActivities.length
  const overdueActivities = sortedActivities.filter(a => a.isOverdue).length
  const todayActivities = sortedActivities.filter(a => a.isToday).length
  const highPriorityActivities = sortedActivities.filter(a => a.priority === 'HIGH').length
  
  const handleActivityClick = (activity: any) => {
    // Navigate to the appropriate tab with contact filtering
    const searchParam = encodeURIComponent(activity.contact_name || '')
    const phoneParam = encodeURIComponent(activity.contact_phone || '')
    
    // Create query parameters for filtering
    const queryParams = new URLSearchParams()
    if (activity.contact_name) {
      queryParams.set('search', activity.contact_name)
    }
    if (activity.contact_phone) {
      queryParams.set('phone', activity.contact_phone)
    }
    
    const url = `/${activity.tab_location}?${queryParams.toString()}`
    router.push(url)
  }
  
  const markAsCompleted = async (activity: any) => {
    try {
      // Update the next_followup_date to null or set a future date
      const { error } = await supabase
        .from('flatrix_leads')
        .update({ 
          next_followup_date: null,
          updated_at: new Date().toISOString()
        })
        .eq('id', activity.type === 'LEADS' ? activity.record_id : 
              deals?.find(d => d.id === activity.record_id)?.lead_id)
      
      if (error) throw error
      
      toast.success('Activity marked as completed!')
      refetchLeads()
      refetchDeals()
    } catch (error) {
      console.error('Error marking activity as completed:', error)
      toast.error('Failed to mark activity as completed')
    }
  }

  const handleGenerateMessage = async (activity: any) => {
    try {
      if (!activity) {
        toast.error('No activity data available')
        return
      }
      
      setSelectedActivity(activity)
      setIsGenerating(true)
      setShowMessageModal(true)
      
      // Simulate AI generation delay for better UX
      await new Promise(resolve => setTimeout(resolve, 1500))
      
      const message = generateFollowUpMessage(activity)
      setGeneratedMessage(message)
    } catch (error) {
      console.error('Error generating message:', error)
      toast.error('Failed to generate message')
      setGeneratedMessage('Error generating message. Please try again.')
    } finally {
      setIsGenerating(false)
    }
  }

  const handleCopyMessage = () => {
    try {
      if (!generatedMessage) {
        toast.error('No message to copy')
        return
      }
      navigator.clipboard.writeText(generatedMessage)
      toast.success('Message copied to clipboard!')
    } catch (error) {
      console.error('Error copying message:', error)
      toast.error('Failed to copy message')
    }
  }

  const handleSendWhatsApp = () => {
    try {
      if (!selectedActivity?.contact_phone) {
        toast.error('No phone number available')
        return
      }
      if (!generatedMessage) {
        toast.error('No message to send')
        return
      }
      
      const encodedMessage = encodeURIComponent(generatedMessage)
      const cleanPhone = selectedActivity.contact_phone.replace(/[^0-9]/g, '')
      const whatsappUrl = `https://wa.me/${cleanPhone}?text=${encodedMessage}`
      window.open(whatsappUrl, '_blank')
      toast.success('Opening WhatsApp...')
    } catch (error) {
      console.error('Error opening WhatsApp:', error)
      toast.error('Failed to open WhatsApp')
    }
  }

  const handleSendSMS = () => {
    try {
      if (!selectedActivity?.contact_phone) {
        toast.error('No phone number available')
        return
      }
      if (!generatedMessage) {
        toast.error('No message to send')
        return
      }
      
      const encodedMessage = encodeURIComponent(generatedMessage)
      const smsUrl = `sms:${selectedActivity.contact_phone}?body=${encodedMessage}`
      window.open(smsUrl, '_blank')
      toast.success('Opening SMS app...')
    } catch (error) {
      console.error('Error opening SMS:', error)
      toast.error('Failed to open SMS app')
    }
  }

  if (dealsLoading || leadsLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-lg text-gray-600">Loading activities...</div>
      </div>
    )
  }

  return (
    <div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {/* Total Activities */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Total Activities</p>
              <h3 className="text-3xl font-bold text-gray-900">{totalActivities}</h3>
              <div className="flex items-center mt-2">
                <Calendar className="h-4 w-4 text-blue-600 mr-1" />
                <span className="text-sm text-blue-600 font-medium">All follow-ups</span>
              </div>
            </div>
            <div className="bg-blue-100 p-3 rounded-lg">
              <BarChart3 className="h-6 w-6 text-blue-600" />
            </div>
          </div>
        </div>

        {/* Overdue Activities */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Overdue</p>
              <h3 className="text-3xl font-bold text-red-900">{overdueActivities}</h3>
              <div className="flex items-center mt-2">
                <AlertTriangle className="h-4 w-4 text-red-600 mr-1" />
                <span className="text-sm text-red-600 font-medium">Need attention</span>
              </div>
            </div>
            <div className="bg-red-100 p-3 rounded-lg">
              <Clock className="h-6 w-6 text-red-600" />
            </div>
          </div>
        </div>

        {/* Today's Activities */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">Due Today</p>
              <h3 className="text-3xl font-bold text-green-900">{todayActivities}</h3>
              <div className="flex items-center mt-2">
                <Target className="h-4 w-4 text-green-600 mr-1" />
                <span className="text-sm text-green-600 font-medium">Focus areas</span>
              </div>
            </div>
            <div className="bg-green-100 p-3 rounded-lg">
              <CheckCircle className="h-6 w-6 text-green-600" />
            </div>
          </div>
        </div>

        {/* High Priority */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <p className="text-sm text-gray-600 mb-1">High Priority</p>
              <h3 className="text-3xl font-bold text-purple-900">{highPriorityActivities}</h3>
              <div className="flex items-center mt-2">
                <Star className="h-4 w-4 text-purple-600 mr-1" />
                <span className="text-sm text-purple-600 font-medium">Top opportunities</span>
              </div>
            </div>
            <div className="bg-purple-100 p-3 rounded-lg">
              <Zap className="h-6 w-6 text-purple-600" />
            </div>
          </div>
        </div>
      </div>

      {/* Filters Section */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 mb-6">
        <div className="p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Smart Activity Filters</h2>
          
          <div className="flex flex-col gap-4">
            <div className="w-full">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search activities by contact name, location..."
                  className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>
            </div>
            
            <div className="flex flex-col sm:flex-row gap-3">
              <div className="relative flex-1">
                <select
                  value={selectedFilter}
                  onChange={(e) => setSelectedFilter(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option value="ALL">All Activities</option>
                  <option value="LEADS">Leads</option>
                  <option value="DEALS">Deals</option>
                  <option value="SITE VISITS">Site Visits</option>
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <select
                  value={selectedPriority}
                  onChange={(e) => setSelectedPriority(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option value="ALL">All Priorities</option>
                  <option value="HIGH">High Priority</option>
                  <option value="MEDIUM">Medium Priority</option>
                  <option value="LOW">Low Priority</option>
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <select
                  value={selectedAgent}
                  onChange={(e) => setSelectedAgent(e.target.value)}
                  className="appearance-none w-full px-4 py-2.5 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none bg-white cursor-pointer hover:bg-gray-50"
                >
                  <option value="ALL">All Agents</option>
                  {uniqueAgents.map(agent => (
                    <option key={agent} value={agent}>{agent}</option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" />
              </div>

              <div className="relative flex-1">
                <input
                  type="date"
                  value={selectedDate}
                  onChange={(e) => setSelectedDate(e.target.value)}
                  className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                />
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Activities List */}
      <div id="activities-list" className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold text-gray-900">Follow-up Activities</h2>
              <p className="text-sm text-gray-600 mt-1">AI-prioritized tasks for maximum impact</p>
            </div>
            <div className="flex items-center space-x-2">
              <button
                onClick={() => {refetchLeads(); refetchDeals()}}
                className="flex items-center space-x-2 px-3 py-2 text-sm border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition"
              >
                <RefreshCw className="h-4 w-4" />
                <span>Refresh</span>
              </button>
            </div>
          </div>
        </div>

        <div>
          {filteredActivities.length === 0 ? (
            <div className="text-center text-gray-500 py-12">
              <Calendar className="h-12 w-12 text-gray-300 mx-auto mb-3" />
              <p className="text-lg font-medium mb-1">No activities found</p>
              <p className="text-sm text-gray-400">
                {searchTerm || selectedFilter !== 'ALL' || selectedPriority !== 'ALL' || selectedDate
                  ? 'Try adjusting your filters' 
                  : 'All follow-ups are up to date!'}
              </p>
            </div>
          ) : (
            <div className="divide-y divide-gray-200">
              {filteredActivities.map((activity: any) => (
                <div key={activity.id} className="px-4 sm:px-6 py-4 hover:bg-gray-50 transition-colors">
                  <div className="flex items-start justify-between">
                    <div className="flex items-start space-x-4 flex-1">
                      {/* Priority & Status Indicators */}
                      <div className="flex flex-col items-center space-y-2">
                        <div className={`w-3 h-3 rounded-full ${
                          activity.isOverdue ? 'bg-red-500' :
                          activity.isToday ? 'bg-yellow-500' :
                          'bg-green-500'
                        }`}></div>
                        <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${priorityColors[activity.priority]}`}>
                          {activity.priority}
                        </span>
                      </div>

                      {/* Activity Content */}
                      <div className="flex-1">
                        <div className="flex items-start justify-between mb-2">
                          <div>
                            <h3 className="text-base font-semibold text-gray-900 mb-1">
                              {activity.title}
                            </h3>
                            <p className="text-sm text-gray-600 mb-2">{activity.subtitle}</p>
                            
                            <div className="flex items-center space-x-4 text-xs text-gray-500">
                              <div className="flex items-center space-x-1">
                                <Calendar className="h-3 w-3" />
                                <span>
                                  {activity.isOverdue ? 'Overdue: ' : activity.isToday ? 'Today: ' : 'Due: '}
                                  {new Date(activity.followup_date).toLocaleString('en-US', {
                                    month: 'short',
                                    day: 'numeric',
                                    year: 'numeric',
                                    hour: 'numeric',
                                    minute: '2-digit',
                                    hour12: true
                                  })}
                                </span>
                              </div>
                              
                              <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${activityTypeColors[activity.type]}`}>
                                {activity.type}
                              </span>
                              
                              {activity.location && (
                                <div className="flex items-center space-x-1">
                                  <MapPin className="h-3 w-3" />
                                  <span>{activity.location}</span>
                                </div>
                              )}
                              
                              {activity.budget_max && (
                                <div className="flex items-center space-x-1">
                                  <span>Budget: ₹{activity.budget_max.toLocaleString()}</span>
                                </div>
                              )}
                            </div>
                          </div>
                        </div>

                        {/* Contact Information */}
                        <div className="flex items-center space-x-6 text-sm">
                          <div className="flex items-center space-x-2">
                            <User className="h-4 w-4 text-gray-400" />
                            <span className="font-medium text-gray-900">
                              {activity.contact_name || 'Unknown Contact'}
                            </span>
                          </div>

                          {activity.contact_phone && (
                            <div className="flex items-center space-x-2 text-gray-600">
                              <Phone className="h-4 w-4 text-gray-400" />
                              <span>{activity.contact_phone}</span>
                            </div>
                          )}

                          {activity.contact_email && (
                            <div className="flex items-center space-x-2 text-gray-600">
                              <Mail className="h-4 w-4 text-gray-400" />
                              <span className="truncate max-w-48">{activity.contact_email}</span>
                            </div>
                          )}

                          <div className="flex items-center space-x-2 text-gray-600">
                            <Users className="h-4 w-4 text-gray-400" />
                            <span className="font-medium">
                              Assigned to: <span className="text-gray-900">{activity.assigned_to || 'Unassigned'}</span>
                            </span>
                          </div>
                        </div>

                        {/* Notes Display */}
                        {activity.notes && (
                          <div className="mt-3 text-sm">
                            <div className="flex items-start space-x-2">
                              <MessageSquare className="h-4 w-4 text-gray-400 mt-0.5 flex-shrink-0" />
                              <div className="flex-1">
                                <div className="flex items-center space-x-2 mb-1">
                                  <span className="font-medium text-gray-700">Latest Note:</span>
                                  {(() => {
                                    const parsedNotes = parseNotes(activity.notes, activity.created_at)
                                    const totalNotes = parsedNotes.length
                                    const isExpanded = expandedNotes.has(activity.id)
                                    const latestNote = getLatestNote(activity.notes, activity.created_at)
                                    
                                    return (
                                      <div className="flex items-center space-x-2">
                                        {totalNotes > 1 && !isExpanded && (
                                          <button
                                            onClick={() => {
                                              setSelectedActivityNotes({
                                                ...activity,
                                                parsedNotes: parsedNotes
                                              })
                                              setShowNotesHistory(true)
                                            }}
                                            className="text-xs text-blue-600 hover:text-blue-800 font-medium bg-blue-50 px-2 py-1 rounded-full border border-blue-200 hover:bg-blue-100 transition-colors"
                                            title="View all notes"
                                          >
                                            +{totalNotes - 1} more
                                          </button>
                                        )}
                                        <button
                                          onClick={() => {
                                            setSelectedActivityNotes({
                                              ...activity,
                                              parsedNotes: parsedNotes
                                            })
                                            setShowNotesHistory(true)
                                          }}
                                          className="text-gray-400 hover:text-gray-600 transition-colors"
                                          title="View notes history"
                                        >
                                          <MessageCircle className="h-3 w-3" />
                                        </button>
                                      </div>
                                    )
                                  })()}
                                </div>
                                <p className="text-gray-600 text-xs leading-relaxed">
                                  {expandedNotes.has(activity.id) ? 
                                    parseNotes(activity.notes, activity.created_at)[0]?.content || 'No notes available' :
                                    getLatestNote(activity.notes, activity.created_at) || 'No notes available'
                                  }
                                </p>
                                {!expandedNotes.has(activity.id) && getLatestNote(activity.notes, activity.created_at) && getLatestNote(activity.notes, activity.created_at)?.endsWith('...') && (
                                  <button
                                    onClick={() => {
                                      const newExpanded = new Set(expandedNotes)
                                      newExpanded.add(activity.id)
                                      setExpandedNotes(newExpanded)
                                    }}
                                    className="text-xs text-blue-600 hover:text-blue-800 mt-1"
                                  >
                                    Show more
                                  </button>
                                )}
                                {expandedNotes.has(activity.id) && (
                                  <button
                                    onClick={() => {
                                      const newExpanded = new Set(expandedNotes)
                                      newExpanded.delete(activity.id)
                                      setExpandedNotes(newExpanded)
                                    }}
                                    className="text-xs text-gray-600 hover:text-gray-800 mt-1"
                                  >
                                    Show less
                                  </button>
                                )}
                              </div>
                            </div>
                          </div>
                        )}

                        {/* AI Recommendations */}
                        {activity.priority === 'HIGH' && (
                          <div className="mt-3 p-3 bg-purple-50 border border-purple-200 rounded-lg">
                            <div className="flex items-start space-x-2">
                              <Sparkles className="h-4 w-4 text-purple-600 mt-0.5" />
                              <div>
                                <p className="text-sm font-medium text-purple-900">AI Recommendation</p>
                                <p className="text-xs text-purple-700 mt-1">
                                  {activity.isOverdue 
                                    ? 'This lead is overdue and high-value. Immediate follow-up recommended to prevent loss.'
                                    : activity.type === 'SITE VISITS'
                                    ? 'Site visit completed - optimal time for booking conversion. Call within 24 hours.'
                                    : 'High-priority lead with strong conversion potential. Personalized follow-up suggested.'
                                  }
                                </p>
                              </div>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Action Buttons */}
                    <div className="flex items-center space-x-2 ml-4">
                      <button
                        onClick={() => handleGenerateMessage(activity)}
                        className="p-2 text-indigo-600 hover:text-indigo-700 hover:bg-indigo-50 rounded-lg transition-colors border border-indigo-200"
                        title="Generate AI Message"
                      >
                        <Brain className="h-4 w-4" />
                      </button>
                      
                      {activity.contact_phone && (
                        <>
                          <a
                            href={`tel:${activity.contact_phone}`}
                            className="p-2 text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded-lg transition-colors border border-blue-200"
                            title="Call"
                          >
                            <Phone className="h-4 w-4" />
                          </a>
                          <a
                            href={`https://wa.me/${activity.contact_phone.replace(/[^0-9]/g, '')}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-2 text-green-600 hover:text-green-700 hover:bg-green-50 rounded-lg transition-colors border border-green-200"
                            title="WhatsApp"
                          >
                            <WhatsAppIcon className="h-4 w-4" />
                          </a>
                        </>
                      )}
                      
                      <button
                        onClick={() => handleActivityClick(activity)}
                        className="p-2 text-purple-600 hover:text-purple-700 hover:bg-purple-50 rounded-lg transition-colors border border-purple-200"
                        title="View Details"
                      >
                        <ArrowRight className="h-4 w-4" />
                      </button>
                      
                      <button
                        onClick={() => markAsCompleted(activity)}
                        className="p-2 text-green-600 hover:text-green-700 hover:bg-green-50 rounded-lg transition-colors border border-green-200"
                        title="Mark Complete"
                      >
                        <CheckCircle className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {filteredActivities.length > 0 && (
          <div className="px-6 py-3 bg-gray-50 border-t text-sm text-gray-600">
            Showing {filteredActivities.length} of {sortedActivities.length} activities
            {filteredActivities.filter(a => a.isOverdue).length > 0 && (
              <span className="ml-4 text-red-600 font-medium">
                ({filteredActivities.filter(a => a.isOverdue).length} overdue)
              </span>
            )}
          </div>
        )}
      </div>

      {/* AI Message Generation Modal */}
      {showMessageModal && selectedActivity && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
          onClick={() => {
            setShowMessageModal(false)
            setSelectedActivity(null)
            setGeneratedMessage('')
          }}
        >
          <div 
            className="bg-white rounded-lg max-w-3xl w-full max-h-[90vh] overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-6 border-b border-gray-200 bg-gradient-to-r from-indigo-50 to-purple-50">
              <div className="flex items-center space-x-3">
                <div className="p-2 bg-indigo-100 rounded-lg">
                  <Brain className="h-6 w-6 text-indigo-600" />
                </div>
                <div>
                  <h2 className="text-xl font-semibold text-gray-900">AI Message Generator</h2>
                  <p className="text-sm text-gray-600">
                    Personalized follow-up for {selectedActivity.contact_name || 'Contact'}
                  </p>
                </div>
                <span className="bg-indigo-100 text-indigo-800 text-xs px-2 py-1 rounded-full">
                  Smart CRM
                </span>
              </div>
              <button
                onClick={() => {
                  setShowMessageModal(false)
                  setSelectedActivity(null)
                  setGeneratedMessage('')
                }}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="p-6">
              {/* Activity Context */}
              <div className="mb-6 p-4 bg-gray-50 rounded-lg">
                <h3 className="text-sm font-medium text-gray-900 mb-2">Context Information</h3>
                <div className="grid grid-cols-2 gap-4 text-xs text-gray-600">
                  <div>
                    <span className="font-medium">Type:</span> <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${activityTypeColors[selectedActivity.type]}`}>
                      {selectedActivity.type}
                    </span>
                  </div>
                  <div>
                    <span className="font-medium">Priority:</span> <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${priorityColors[selectedActivity.priority]}`}>
                      {selectedActivity.priority}
                    </span>
                  </div>
                  <div>
                    <span className="font-medium">Status:</span> 
                    <span className={`ml-1 ${selectedActivity.isOverdue ? 'text-red-600' : selectedActivity.isToday ? 'text-yellow-600' : 'text-green-600'}`}>
                      {selectedActivity.isOverdue ? 'Overdue' : selectedActivity.isToday ? 'Due Today' : 'Upcoming'}
                    </span>
                  </div>
                  <div>
                    <span className="font-medium">Location:</span> {selectedActivity.location || 'Not specified'}
                  </div>
                  {selectedActivity.budget_max && (
                    <div>
                      <span className="font-medium">Budget:</span> ₹{selectedActivity.budget_max.toLocaleString()}
                    </div>
                  )}
                  <div>
                    <span className="font-medium">Source:</span> {selectedActivity.source || 'Unknown'}
                  </div>
                </div>
              </div>

              {/* Generated Message */}
              <div className="mb-6">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-sm font-medium text-gray-900">Generated Message</h3>
                  {!isGenerating && generatedMessage && (
                    <div className="flex items-center space-x-2">
                      <button
                        onClick={handleCopyMessage}
                        className="flex items-center space-x-1 px-3 py-1 text-xs border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition"
                      >
                        <Copy className="h-3 w-3" />
                        <span>Copy</span>
                      </button>
                    </div>
                  )}
                </div>
                
                <div className="relative">
                  {isGenerating ? (
                    <div className="flex items-center justify-center py-12 bg-gray-50 rounded-lg border border-gray-200">
                      <div className="text-center">
                        <div className="inline-flex items-center space-x-2 text-indigo-600">
                          <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-600"></div>
                          <span className="text-sm font-medium">AI is crafting your message...</span>
                        </div>
                        <p className="text-xs text-gray-500 mt-2">
                          Analyzing context and personalizing content
                        </p>
                      </div>
                    </div>
                  ) : generatedMessage ? (
                    <div className="relative">
                      <textarea
                        value={generatedMessage}
                        onChange={(e) => setGeneratedMessage(e.target.value)}
                        className="w-full h-64 p-4 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none resize-none"
                        placeholder="Your AI-generated message will appear here..."
                      />
                      <div className="absolute top-2 right-2">
                        <span className="bg-green-100 text-green-800 text-xs px-2 py-1 rounded-full">
                          AI Generated
                        </span>
                      </div>
                    </div>
                  ) : (
                    <div className="flex items-center justify-center py-12 bg-gray-50 rounded-lg border border-gray-200 border-dashed">
                      <div className="text-center">
                        <Brain className="h-8 w-8 text-gray-400 mx-auto mb-2" />
                        <p className="text-sm text-gray-500">
                          Click "Generate Message" to create a personalized follow-up
                        </p>
                      </div>
                    </div>
                  )}
                </div>
                
                {generatedMessage && !isGenerating && (
                  <div className="mt-3 p-3 bg-blue-50 border border-blue-200 rounded-lg">
                    <div className="flex items-start space-x-2">
                      <Sparkles className="h-4 w-4 text-blue-600 mt-0.5" />
                      <div>
                        <p className="text-sm font-medium text-blue-900">AI Enhancement Tip</p>
                        <p className="text-xs text-blue-700 mt-1">
                          This message is personalized based on the lead's context, priority, and activity type. 
                          You can edit it before sending to add more specific details.
                        </p>
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {/* Action Buttons */}
              {generatedMessage && !isGenerating && (
                <div className="flex flex-col sm:flex-row gap-3">
                  <button
                    onClick={() => {
                      if (!generatedMessage.trim()) {
                        toast.error('Please generate a message first')
                        return
                      }
                      handleGenerateMessage(selectedActivity)
                    }}
                    className="flex items-center justify-center space-x-2 px-4 py-2 border border-indigo-300 text-indigo-700 rounded-lg hover:bg-indigo-50 transition"
                  >
                    <RefreshCw className="h-4 w-4" />
                    <span>Regenerate</span>
                  </button>
                  
                  {selectedActivity.contact_phone && (
                    <>
                      <button
                        onClick={handleSendWhatsApp}
                        className="flex items-center justify-center space-x-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition"
                      >
                        <WhatsAppIcon className="h-4 w-4" />
                        <span>Send via WhatsApp</span>
                      </button>
                      
                      <button
                        onClick={handleSendSMS}
                        className="flex items-center justify-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
                      >
                        <MessageSquare className="h-4 w-4" />
                        <span>Send SMS</span>
                      </button>
                    </>
                  )}
                  
                  <button
                    onClick={() => {
                      setShowMessageModal(false)
                      setSelectedActivity(null)
                      setGeneratedMessage('')
                    }}
                    className="flex items-center justify-center space-x-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition"
                  >
                    <span>Close</span>
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Notes History Modal */}
      {showNotesHistory && selectedActivityNotes && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full max-h-[80vh] overflow-hidden">
            <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
              <div>
                <h3 className="text-lg font-semibold text-gray-900">Notes History</h3>
                <p className="text-sm text-gray-600">
                  {selectedActivityNotes.contact_name || 'Unknown Contact'} - {selectedActivityNotes.type}
                </p>
              </div>
              <button
                onClick={() => {
                  setShowNotesHistory(false)
                  setSelectedActivityNotes(null)
                }}
                className="text-gray-400 hover:text-gray-600 transition-colors"
              >
                <X className="h-6 w-6" />
              </button>
            </div>
            
            <div className="px-6 py-4 max-h-96 overflow-y-auto">
              {selectedActivityNotes.parsedNotes && selectedActivityNotes.parsedNotes.length > 0 ? (
                <div className="space-y-4">
                  {selectedActivityNotes.parsedNotes.map((note: any, index: number) => (
                    <div key={index} className={`p-4 rounded-lg border ${
                      index === 0 ? 'bg-blue-50 border-blue-200' : 'bg-gray-50 border-gray-200'
                    }`}>
                      <div className="flex items-start justify-between mb-2">
                        <div className="flex items-center space-x-2">
                          {note.isReferral ? (
                            <div className="flex items-center space-x-1">
                              <User className="h-4 w-4 text-purple-600" />
                              <span className="text-xs font-medium text-purple-800 bg-purple-100 px-2 py-1 rounded-full">
                                Referral Info
                              </span>
                            </div>
                          ) : (
                            <MessageCircle className="h-4 w-4 text-blue-600" />
                          )}
                          {index === 0 && (
                            <span className="text-xs font-medium text-blue-800 bg-blue-100 px-2 py-1 rounded-full">
                              Latest
                            </span>
                          )}
                        </div>
                        <span className="text-xs text-gray-500">
                          {note.timestamp ? new Date(note.timestamp).toLocaleString('en-US', {
                            month: 'short',
                            day: 'numeric',
                            year: 'numeric',
                            hour: '2-digit',
                            minute: '2-digit'
                          }) : 'No timestamp'}
                        </span>
                      </div>
                      <p className="text-sm text-gray-700 whitespace-pre-wrap leading-relaxed">
                        {note.content}
                      </p>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center text-gray-500 py-8">
                  <MessageCircle className="h-12 w-12 text-gray-300 mx-auto mb-3" />
                  <p className="text-lg font-medium mb-1">No notes found</p>
                  <p className="text-sm text-gray-400">This activity doesn't have any notes yet.</p>
                </div>
              )}
            </div>
            
            <div className="px-6 py-4 border-t border-gray-200 flex justify-end">
              <button
                onClick={() => {
                  setShowNotesHistory(false)
                  setSelectedActivityNotes(null)
                }}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 border border-gray-300 rounded-lg hover:bg-gray-200 transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}