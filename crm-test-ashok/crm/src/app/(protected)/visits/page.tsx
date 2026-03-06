'use client'

import { useEffect, useState } from 'react'
import { Calendar, MapPin, Clock, User, CheckCircle, XCircle, AlertCircle, TrendingUp, Phone, MessageCircle, ChevronDown, Edit } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from '@/components/ui/table'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { toast } from 'sonner'
import { waLink } from '@/lib/whatsapp'

interface SiteVisit {
  id: string
  scheduledAt: string
  status: 'SCHEDULED' | 'DONE' | 'NO_SHOW' | 'RESCHEDULED'
  outcomeNote: string | null
  createdAt: string
  lead: {
    id: string
    name: string | null
    phone: string
    email: string | null
    source: string
    stage: string
    score: number
  }
  project: {
    id: string
    name: string
    location: string | null
  } | null
}

const STATUS_COLORS = {
  SCHEDULED: 'bg-blue-100 text-blue-800',
  DONE: 'bg-green-100 text-green-800',
  NO_SHOW: 'bg-red-100 text-red-800',
  RESCHEDULED: 'bg-yellow-100 text-yellow-800'
}

const STAGE_COLORS = {
  NEW: 'bg-blue-500',
  CONTACTED: 'bg-yellow-500',
  SCHEDULED: 'bg-purple-500',
  VISITED: 'bg-green-500',
  NEGOTIATION: 'bg-orange-500',
  BOOKED: 'bg-emerald-500',
  DROPPED: 'bg-red-500',
  RESCHEDULED: 'bg-amber-500',
  INVALID: 'bg-gray-500'
}

const SOURCE_COLORS = {
  google_ads: 'bg-blue-100 text-blue-800',
  meta: 'bg-purple-100 text-purple-800',
  website: 'bg-green-100 text-green-800',
  csv: 'bg-gray-100 text-gray-800'
}

const STATUS_ICONS = {
  SCHEDULED: Clock,
  DONE: CheckCircle,
  NO_SHOW: XCircle,
  RESCHEDULED: AlertCircle
}

export default function SiteVisitsPage() {
  const [visits, setVisits] = useState<SiteVisit[]>([])
  const [loading, setLoading] = useState(true)

  const fetchVisits = async () => {
    setLoading(true)
    try {
      const response = await fetch('/api/visits')
      if (response.ok) {
        const data = await response.json()
        setVisits(data.visits || [])
      } else {
        toast.error('Failed to fetch site visits')
      }
    } catch (error) {
      toast.error('An error occurred')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchVisits()
  }, [])

  const formatDateTime = (dateString: string) => {
    return new Intl.DateTimeFormat('en-IN', {
      dateStyle: 'medium',
      timeStyle: 'short',
      timeZone: 'Asia/Kolkata'
    }).format(new Date(dateString))
  }

  const handleRowClick = (leadId: string) => {
    window.location.href = `/leads/${leadId}`
  }

  const handleCall = (phone: string) => {
    window.open(`tel:${phone}`, '_self')
  }

  const handleWhatsApp = (visit: SiteVisit) => {
    const message = `Hi ${visit.lead.name || 'there'}, regarding your scheduled site visit for ${visit.project?.name || 'our property'} on ${formatDateTime(visit.scheduledAt)}.`
    window.open(waLink(visit.lead.phone, message), '_blank')
  }

  const handleStageChange = async (leadId: string, newStage: string) => {
    try {
      const response = await fetch(`/api/leads/${leadId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stage: newStage })
      })

      if (response.ok) {
        toast.success(`Stage updated to ${newStage}`)
        fetchVisits() // Refresh to show changes
      } else {
        toast.error('Failed to update stage')
      }
    } catch (error) {
      toast.error('An error occurred')
    }
  }

  const upcomingVisits = visits.filter(v => v.status === 'SCHEDULED' && new Date(v.scheduledAt) > new Date())
  const todayVisits = visits.filter(v => {
    const visitDate = new Date(v.scheduledAt).toDateString()
    const today = new Date().toDateString()
    return visitDate === today
  })

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            Site Visits
          </h1>
          <p className="text-gray-600 dark:text-gray-400">
            Manage and track property site visits
          </p>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <Clock className="w-5 h-5 text-blue-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Upcoming</p>
                <p className="text-2xl font-bold">{upcomingVisits.length}</p>
              </div>
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <Calendar className="w-5 h-5 text-purple-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Today</p>
                <p className="text-2xl font-bold">{todayVisits.length}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <CheckCircle className="w-5 h-5 text-green-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">Completed</p>
                <p className="text-2xl font-bold">{visits.filter(v => v.status === 'DONE').length}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center space-x-2">
              <XCircle className="w-5 h-5 text-red-500" />
              <div>
                <p className="text-sm text-gray-600 dark:text-gray-400">No Shows</p>
                <p className="text-2xl font-bold">{visits.filter(v => v.status === 'NO_SHOW').length}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Site Visits Table */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>All Site Visits ({visits.length})</span>
            {loading && <span className="text-sm text-gray-500">Loading...</span>}
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Score</TableHead>
                  <TableHead>Lead & Contact</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead>Project</TableHead>
                  <TableHead>Stage</TableHead>
                  <TableHead>Visit Status</TableHead>
                  <TableHead>Scheduled</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {visits.length > 0 ? (
                  visits.map((visit) => {
                    const StatusIcon = STATUS_ICONS[visit.status]
                    return (
                      <TableRow 
                        key={visit.id}
                        className="cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800"
                        onClick={() => handleRowClick(visit.lead.id)}
                      >
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <div className={`w-2 h-8 rounded ${STAGE_COLORS[visit.lead.stage as keyof typeof STAGE_COLORS] || 'bg-gray-300'}`} />
                            <div className="text-center">
                              <div className="font-bold text-lg">{visit.lead.score}</div>
                              <TrendingUp className="w-3 h-3 text-gray-400 mx-auto" />
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div>
                            <div className="font-medium">
                              {visit.lead.name || 'Unknown'}
                            </div>
                            <div className="text-sm text-gray-600 dark:text-gray-400">
                              {visit.lead.phone}
                            </div>
                            {visit.lead.email && (
                              <div className="text-sm text-gray-500">
                                {visit.lead.email}
                              </div>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge 
                            variant="secondary"
                            className={SOURCE_COLORS[visit.lead.source as keyof typeof SOURCE_COLORS]}
                          >
                            {visit.lead.source.replace('_', ' ')}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="text-sm">
                            {visit.project ? (
                              <div>
                                <div className="font-medium">{visit.project.name}</div>
                                {visit.project.location && (
                                  <div className="text-sm text-gray-500 flex items-center">
                                    <MapPin className="w-3 h-3 mr-1" />
                                    {visit.project.location}
                                  </div>
                                )}
                              </div>
                            ) : (
                              <span className="text-gray-400 italic">No project</span>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <Badge 
                              variant={visit.lead.stage === 'NEW' ? 'default' : 'secondary'}
                              className={visit.lead.stage === 'NEW' ? 'bg-blue-500' : ''}
                            >
                              {visit.lead.stage}
                            </Badge>
                            <DropdownMenu>
                              <DropdownMenuTrigger asChild>
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  className="h-6 w-6 p-0"
                                  onClick={(e) => e.stopPropagation()}
                                  title="Change Stage"
                                >
                                  <ChevronDown className="w-3 h-3" />
                                </Button>
                              </DropdownMenuTrigger>
                              <DropdownMenuContent align="start">
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'NEW')
                                  }}
                                  disabled={visit.lead.stage === 'NEW'}
                                >
                                  New
                                </DropdownMenuItem>
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'CONTACTED')
                                  }}
                                  disabled={visit.lead.stage === 'CONTACTED'}
                                >
                                  Contacted
                                </DropdownMenuItem>
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'SCHEDULED')
                                  }}
                                  disabled={visit.lead.stage === 'SCHEDULED'}
                                >
                                  Scheduled
                                </DropdownMenuItem>
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'VISITED')
                                  }}
                                  disabled={visit.lead.stage === 'VISITED'}
                                >
                                  Visited
                                </DropdownMenuItem>
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'NEGOTIATION')
                                  }}
                                  disabled={visit.lead.stage === 'NEGOTIATION'}
                                >
                                  Negotiation
                                </DropdownMenuItem>
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'BOOKED')
                                  }}
                                  disabled={visit.lead.stage === 'BOOKED'}
                                >
                                  Booked
                                </DropdownMenuItem>
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'DROPPED')
                                  }}
                                  disabled={visit.lead.stage === 'DROPPED'}
                                >
                                  Dropped
                                </DropdownMenuItem>
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'RESCHEDULED')
                                  }}
                                  disabled={visit.lead.stage === 'RESCHEDULED'}
                                >
                                  Rescheduled
                                </DropdownMenuItem>
                                <DropdownMenuItem 
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleStageChange(visit.lead.id, 'INVALID')
                                  }}
                                  disabled={visit.lead.stage === 'INVALID'}
                                >
                                  Invalid Lead
                                </DropdownMenuItem>
                              </DropdownMenuContent>
                            </DropdownMenu>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <StatusIcon className="w-4 h-4" />
                            <Badge 
                              variant="secondary"
                              className={STATUS_COLORS[visit.status]}
                            >
                              {visit.status.replace('_', ' ')}
                            </Badge>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="text-sm">
                            <div className="font-medium">
                              {new Date(visit.scheduledAt).toLocaleDateString('en-IN')}
                            </div>
                            <div className="text-gray-500">
                              {new Date(visit.scheduledAt).toLocaleTimeString('en-IN', { 
                                hour: '2-digit', 
                                minute: '2-digit' 
                              })}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex space-x-1">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={(e) => {
                                e.stopPropagation()
                                handleCall(visit.lead.phone)
                              }}
                              title="Call"
                            >
                              <Phone className="w-4 h-4" />
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={(e) => {
                                e.stopPropagation()
                                handleWhatsApp(visit)
                              }}
                              title="WhatsApp"
                            >
                              <MessageCircle className="w-4 h-4" />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    )
                  })
                ) : (
                  <TableRow>
                    <TableCell colSpan={8} className="text-center py-8">
                      <div className="flex flex-col items-center space-y-2">
                        <Calendar className="w-12 h-12 text-gray-400" />
                        <p className="text-gray-500">No site visits scheduled</p>
                      </div>
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}