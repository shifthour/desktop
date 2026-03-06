'use client'

import { useEffect, useState } from 'react'
import { useParams, useRouter } from 'next/navigation'
import Link from 'next/link'
import { 
  ArrowLeft,
  Phone, 
  MessageCircle, 
  Mail,
  MapPin,
  Calendar,
  User,
  TrendingUp,
  Clock,
  Plus,
  Edit
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Separator } from '@/components/ui/separator'
import { Textarea } from '@/components/ui/textarea'
import { 
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { toast } from 'sonner'
import { waLink } from '@/lib/whatsapp'

interface Lead {
  id: string
  name: string | null
  phone: string
  email: string | null
  source: string
  campaign: string | null
  stage: string
  score: number
  bedroomsPref: number | null
  budgetMin: number | null
  budgetMax: number | null
  locationPref: string | null
  createdAt: string
  updatedAt: string
  project: { id: string; name: string; location: string } | null
  assignedTo: { id: string; name: string; email: string } | null
  activities: Array<{
    id: string
    type: string
    note: string | null
    at: string
    user: { id: string; name: string } | null
  }>
  siteVisits: Array<{
    id: string
    scheduledAt: string
    status: string
    outcomeNote: string | null
    project: { id: string; name: string; location: string } | null
  }>
}

const STAGE_COLORS = {
  NEW: 'bg-blue-500',
  CONTACTED: 'bg-yellow-500',
  SCHEDULED: 'bg-purple-500',
  VISITED: 'bg-green-500',
  NEGOTIATION: 'bg-orange-500',
  BOOKED: 'bg-emerald-500',
  DROPPED: 'bg-red-500'
}

const ACTIVITY_ICONS = {
  CALL: Phone,
  WHATSAPP: MessageCircle,
  EMAIL: Mail,
  NOTE: Edit,
  STATUS_CHANGE: TrendingUp,
  ASSIGNMENT: User
}

export default function LeadDetailPage() {
  const params = useParams()
  const router = useRouter()
  const [lead, setLead] = useState<Lead | null>(null)
  const [loading, setLoading] = useState(true)
  const [newNote, setNewNote] = useState('')
  const [addingNote, setAddingNote] = useState(false)
  const [newStage, setNewStage] = useState('')

  const fetchLead = async () => {
    setLoading(true)
    try {
      const response = await fetch(`/api/leads/${params.id}`)
      if (response.ok) {
        const data = await response.json()
        setLead(data)
        setNewStage(data.stage)
      } else if (response.status === 404) {
        toast.error('Lead not found')
        router.push('/leads')
      } else {
        toast.error('Failed to fetch lead details')
      }
    } catch (error) {
      toast.error('An error occurred')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchLead()
  }, [params.id])

  const handleCall = () => {
    if (lead) {
      window.open(`tel:${lead.phone}`, '_self')
    }
  }

  const handleWhatsApp = () => {
    if (lead) {
      const message = `Hi ${lead.name || 'there'}, thank you for your interest in our properties. I'm reaching out to assist you with your requirements.`
      window.open(waLink(lead.phone, message), '_blank')
    }
  }

  const handleEmail = () => {
    if (lead?.email) {
      window.open(`mailto:${lead.email}`, '_self')
    }
  }

  const addNote = async () => {
    if (!lead || !newNote.trim()) return

    setAddingNote(true)
    try {
      const response = await fetch(`/api/leads/${lead.id}/activities`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: 'NOTE',
          note: newNote.trim()
        })
      })

      if (response.ok) {
        setNewNote('')
        toast.success('Note added successfully')
        await fetchLead() // Refresh to show new note
      } else {
        toast.error('Failed to add note')
      }
    } catch (error) {
      toast.error('An error occurred')
    } finally {
      setAddingNote(false)
    }
  }

  const updateStage = async (stage: string) => {
    if (!lead || stage === lead.stage) return

    try {
      const response = await fetch(`/api/leads/${lead.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stage })
      })

      if (response.ok) {
        toast.success('Stage updated successfully')
        await fetchLead() // Refresh to show changes
      } else {
        toast.error('Failed to update stage')
      }
    } catch (error) {
      toast.error('An error occurred')
    }
  }

  const formatCurrency = (amount: number) => {
    return `₹${(amount / 100000).toFixed(0)}L`
  }

  const formatDateTime = (dateString: string) => {
    return new Intl.DateTimeFormat('en-IN', {
      dateStyle: 'medium',
      timeStyle: 'short',
      timeZone: 'Asia/Kolkata'
    }).format(new Date(dateString))
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-600"></div>
      </div>
    )
  }

  if (!lead) {
    return (
      <div className="text-center py-12">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Lead not found</h2>
        <Link href="/leads">
          <Button className="mt-4">Back to Leads</Button>
        </Link>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link href="/leads">
            <Button variant="ghost" size="sm">
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back to Queue
            </Button>
          </Link>
          <div>
            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
              {lead.name || 'Unknown Lead'}
            </h1>
            <p className="text-gray-600 dark:text-gray-400">
              Lead #{lead.id.slice(-8)} • Score: {lead.score}
            </p>
          </div>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline" onClick={handleCall}>
            <Phone className="w-4 h-4 mr-2" />
            Call
          </Button>
          <Button variant="outline" onClick={handleWhatsApp}>
            <MessageCircle className="w-4 h-4 mr-2" />
            WhatsApp
          </Button>
          {lead.email && (
            <Button variant="outline" onClick={handleEmail}>
              <Mail className="w-4 h-4 mr-2" />
              Email
            </Button>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left Column - Lead Details */}
        <div className="lg:col-span-2 space-y-6">
          {/* Contact Information */}
          <Card>
            <CardHeader>
              <CardTitle>Contact Information</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Name</label>
                  <p className="mt-1">{lead.name || 'Not provided'}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Phone</label>
                  <p className="mt-1 font-mono">{lead.phone}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Email</label>
                  <p className="mt-1">{lead.email || 'Not provided'}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Source</label>
                  <div className="mt-1">
                    <Badge variant="secondary">{lead.source.replace('_', ' ')}</Badge>
                    {lead.campaign && (
                      <p className="text-sm text-gray-500 mt-1">{lead.campaign}</p>
                    )}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Preferences */}
          <Card>
            <CardHeader>
              <CardTitle>Property Preferences</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Bedrooms</label>
                  <p className="mt-1">{lead.bedroomsPref ? `${lead.bedroomsPref} BHK` : 'Not specified'}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Budget</label>
                  <p className="mt-1">
                    {lead.budgetMin || lead.budgetMax
                      ? `${lead.budgetMin ? formatCurrency(lead.budgetMin) : 'Any'} - ${lead.budgetMax ? formatCurrency(lead.budgetMax) : 'Any'}`
                      : 'Not specified'
                    }
                  </p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Location</label>
                  <p className="mt-1">{lead.locationPref || 'Not specified'}</p>
                </div>
              </div>
              {lead.project && (
                <div className="mt-4">
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Interested Project</label>
                  <p className="mt-1 font-medium">{lead.project.name}</p>
                  {lead.project.location && (
                    <p className="text-sm text-gray-500">{lead.project.location}</p>
                  )}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Add Note */}
          <Card>
            <CardHeader>
              <CardTitle>Add Note</CardTitle>
              <CardDescription>Record call outcomes, follow-up notes, or any important information</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <Textarea
                  placeholder="Enter your note here..."
                  value={newNote}
                  onChange={(e) => setNewNote(e.target.value)}
                  rows={3}
                />
                <Button
                  onClick={addNote}
                  disabled={!newNote.trim() || addingNote}
                  size="sm"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  {addingNote ? 'Adding...' : 'Add Note'}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Right Column - Status & Activities */}
        <div className="space-y-6">
          {/* Status & Actions */}
          <Card>
            <CardHeader>
              <CardTitle>Lead Status</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Current Stage</label>
                <div className="mt-2">
                  <Select value={newStage} onValueChange={updateStage}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="NEW">New</SelectItem>
                      <SelectItem value="CONTACTED">Contacted</SelectItem>
                      <SelectItem value="SCHEDULED">Scheduled</SelectItem>
                      <SelectItem value="VISITED">Visited</SelectItem>
                      <SelectItem value="NEGOTIATION">Negotiation</SelectItem>
                      <SelectItem value="BOOKED">Booked</SelectItem>
                      <SelectItem value="DROPPED">Dropped</SelectItem>
                      <SelectItem value="RESCHEDULED">Rescheduled</SelectItem>
                      <SelectItem value="INVALID">Invalid Lead</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <Separator />

              <div className="space-y-3">
                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Score</label>
                  <div className="mt-1 flex items-center space-x-2">
                    <div className={`w-3 h-6 rounded ${STAGE_COLORS[lead.stage as keyof typeof STAGE_COLORS] || 'bg-gray-300'}`} />
                    <span className="text-2xl font-bold">{lead.score}</span>
                    <TrendingUp className="w-4 h-4 text-gray-400" />
                  </div>
                </div>
                
                {lead.assignedTo && (
                  <div>
                    <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Assigned To</label>
                    <p className="mt-1">{lead.assignedTo.name}</p>
                  </div>
                )}

                <div>
                  <label className="text-sm font-medium text-gray-500 dark:text-gray-400">Created</label>
                  <p className="mt-1 text-sm">{formatDateTime(lead.createdAt)}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Site Visits */}
          {lead.siteVisits.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle>Site Visits</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {lead.siteVisits.map((visit) => (
                    <div key={visit.id} className="border rounded-lg p-3">
                      <div className="flex items-center justify-between mb-2">
                        <Badge variant={visit.status === 'DONE' ? 'default' : 'secondary'}>
                          {visit.status}
                        </Badge>
                        <span className="text-sm text-gray-500">
                          {formatDateTime(visit.scheduledAt)}
                        </span>
                      </div>
                      {visit.project && (
                        <p className="text-sm font-medium">{visit.project.name}</p>
                      )}
                      {visit.outcomeNote && (
                        <p className="text-sm text-gray-600 mt-1">{visit.outcomeNote}</p>
                      )}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      {/* Activities Timeline */}
      <Card>
        <CardHeader>
          <CardTitle>Activity Timeline</CardTitle>
          <CardDescription>Chronological history of all interactions with this lead</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {lead.activities.length > 0 ? (
              lead.activities.map((activity, index) => {
                const IconComponent = ACTIVITY_ICONS[activity.type as keyof typeof ACTIVITY_ICONS] || Clock
                return (
                  <div key={activity.id} className="flex items-start space-x-4">
                    <div className="flex-shrink-0">
                      <div className="w-8 h-8 bg-indigo-100 dark:bg-indigo-900 rounded-full flex items-center justify-center">
                        <IconComponent className="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
                      </div>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <p className="text-sm font-medium text-gray-900 dark:text-white">
                          {activity.type.replace('_', ' ')}
                        </p>
                        <p className="text-sm text-gray-500">
                          {formatDateTime(activity.at)}
                        </p>
                      </div>
                      {activity.note && (
                        <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                          {activity.note}
                        </p>
                      )}
                      {activity.user && (
                        <p className="text-xs text-gray-500 mt-1">
                          by {activity.user.name}
                        </p>
                      )}
                    </div>
                    {index < lead.activities.length - 1 && (
                      <div className="absolute left-4 mt-8 w-0.5 h-4 bg-gray-200 dark:bg-gray-700" />
                    )}
                  </div>
                )
              })
            ) : (
              <div className="text-center py-8">
                <Clock className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                <p className="text-gray-500">No activities recorded yet</p>
                <p className="text-sm text-gray-400">Add a note or update the lead stage to start building the timeline</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}