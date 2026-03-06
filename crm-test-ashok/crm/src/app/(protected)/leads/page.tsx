'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { 
  Phone, 
  MessageCircle, 
  Plus, 
  Filter,
  Download,
  Search,
  Clock,
  TrendingUp,
  ChevronDown,
  RefreshCw,
  ArrowUpDown,
  ArrowUp,
  ArrowDown
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { toast } from 'sonner'
import { waLink } from '@/lib/whatsapp'
import { AddLeadDialog } from '@/components/add-lead-dialog'

interface Lead {
  id: string
  name: string | null
  phone: string
  email: string | null
  source: string
  campaign: string | null
  stage: string
  score: number
  createdAt: string
  project: { id: string; name: string } | null
  assignedTo: { id: string; name: string } | null
  lastActivity: { type: string; at: string; note: string | null; user: { name: string } | null } | null
}

interface Project {
  id: string
  name: string
  location: string | null
}

interface LeadsResponse {
  leads: Lead[]
  pagination: {
    page: number
    limit: number
    total: number
    totalPages: number
    hasNext: boolean
    hasPrev: boolean
  }
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

export default function LeadsPage() {
  const [leads, setLeads] = useState<Lead[]>([])
  const [loading, setLoading] = useState(true)
  const [pagination, setPagination] = useState<LeadsResponse['pagination']>()
  const [search, setSearch] = useState('')
  const [stageFilter, setStageFilter] = useState('all')
  const [sourceFilter, setSourceFilter] = useState('all')
  const [assignedToFilter, setAssignedToFilter] = useState('me')
  const [projectFilter, setProjectFilter] = useState('all')
  const [projects, setProjects] = useState<Project[]>([])
  const [sortField, setSortField] = useState<'score' | 'createdAt' | 'name' | 'source' | 'project'>('score')
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')

  const sortLeads = (leadsToSort: Lead[], field: string, direction: string) => {
    return [...leadsToSort].sort((a, b) => {
      let aValue: any, bValue: any
      
      switch (field) {
        case 'score':
          aValue = a.score
          bValue = b.score
          break
        case 'createdAt':
          aValue = new Date(a.createdAt)
          bValue = new Date(b.createdAt)
          break
        case 'name':
          aValue = a.name || 'zzz' // Put null names at the end
          bValue = b.name || 'zzz'
          break
        case 'source':
          aValue = a.source
          bValue = b.source
          break
        case 'project':
          aValue = a.project?.name || 'zzz' // Put null projects at the end
          bValue = b.project?.name || 'zzz'
          break
        default:
          return 0
      }
      
      if (direction === 'asc') {
        return aValue < bValue ? -1 : aValue > bValue ? 1 : 0
      } else {
        return aValue > bValue ? -1 : aValue < bValue ? 1 : 0
      }
    })
  }

  const handleSort = (field: 'score' | 'createdAt' | 'name' | 'source' | 'project') => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDirection('desc')
    }
  }

  const handleRowClick = (leadId: string) => {
    window.location.href = `/leads/${leadId}`
  }

  const fetchLeads = async (page = 1) => {
    setLoading(true)
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        limit: '50'
      })

      if (search) params.set('q', search)
      if (stageFilter !== 'all') params.set('stage', stageFilter)
      if (sourceFilter !== 'all') params.set('source', sourceFilter)
      if (projectFilter !== 'all') params.set('project', projectFilter)
      if (assignedToFilter !== 'all') params.set('assignedTo', assignedToFilter)

      const response = await fetch(`/api/leads?${params}`)
      if (response.ok) {
        const data: LeadsResponse = await response.json()
        const sortedLeads = sortLeads(data.leads, sortField, sortDirection)
        setLeads(sortedLeads)
        setPagination(data.pagination)
      } else {
        toast.error('Failed to fetch leads')
      }
    } catch (error) {
      toast.error('An error occurred')
    } finally {
      setLoading(false)
    }
  }

  const fetchProjects = async () => {
    try {
      const response = await fetch('/api/projects')
      if (response.ok) {
        const data = await response.json()
        setProjects(data.projects || [])
      }
    } catch (error) {
      console.error('Failed to fetch projects:', error)
    }
  }

  useEffect(() => {
    fetchProjects()
  }, [])

  useEffect(() => {
    fetchLeads()
  }, [search, stageFilter, sourceFilter, projectFilter, assignedToFilter, sortField, sortDirection])

  const handleCall = (phone: string) => {
    window.open(`tel:${phone}`, '_self')
  }

  const handleWhatsApp = (lead: Lead) => {
    const message = `Hi ${lead.name || 'there'}, thank you for your interest in our properties. I'm reaching out to assist you with your requirements.`
    window.open(waLink(lead.phone, message), '_blank')
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
        // Update the local state immediately
        setLeads(prevLeads => 
          prevLeads.map(lead => 
            lead.id === leadId 
              ? { ...lead, stage: newStage }
              : lead
          )
        )
      } else {
        toast.error('Failed to update stage')
      }
    } catch (error) {
      toast.error('An error occurred')
    }
  }

  const isOverdue = (createdAt: string, stage: string) => {
    if (stage !== 'NEW') return false
    const created = new Date(createdAt)
    const now = new Date()
    const diffMinutes = (now.getTime() - created.getTime()) / (1000 * 60)
    return diffMinutes > 15
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            Telecaller Queue
          </h1>
          <p className="text-gray-600 dark:text-gray-400">
            Manage and follow up on your leads
          </p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline" size="sm">
            <Download className="w-4 h-4 mr-2" />
            Export CSV
          </Button>
          <AddLeadDialog 
            projects={projects}
            onLeadAdded={() => fetchLeads()}
          />
        </div>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="p-4">
          <div className="flex flex-wrap gap-4">
            <div className="flex-1 min-w-[200px]">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <Input
                  placeholder="Search leads..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
            <Select value={assignedToFilter} onValueChange={setAssignedToFilter}>
              <SelectTrigger className="w-[180px]">
                <SelectValue placeholder="Assigned to" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Agents</SelectItem>
                <SelectItem value="me">Assigned to Me</SelectItem>
              </SelectContent>
            </Select>
            <Select value={stageFilter} onValueChange={setStageFilter}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="Stage" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Stages</SelectItem>
                <SelectItem value="NEW">New</SelectItem>
                <SelectItem value="CONTACTED">Contacted</SelectItem>
                <SelectItem value="SCHEDULED">Scheduled</SelectItem>
                <SelectItem value="VISITED">Visited</SelectItem>
                <SelectItem value="NEGOTIATION">Negotiation</SelectItem>
                <SelectItem value="BOOKED">Booked</SelectItem>
                <SelectItem value="DROPPED">Dropped</SelectItem>
                <SelectItem value="RESCHEDULED">Rescheduled</SelectItem>
                <SelectItem value="INVALID">Invalid</SelectItem>
              </SelectContent>
            </Select>
            <Select value={sourceFilter} onValueChange={setSourceFilter}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="Source" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Sources</SelectItem>
                <SelectItem value="google_ads">Google Ads</SelectItem>
                <SelectItem value="meta">Meta</SelectItem>
                <SelectItem value="website">Website</SelectItem>
                <SelectItem value="csv">CSV Import</SelectItem>
              </SelectContent>
            </Select>
            <Select value={projectFilter} onValueChange={setProjectFilter}>
              <SelectTrigger className="w-[200px]">
                <SelectValue placeholder="Project" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Projects</SelectItem>
                {projects.map((project) => (
                  <SelectItem key={project.id} value={project.id}>
                    {project.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>


      {/* Leads Table */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>Leads ({pagination?.total || 0})</span>
            {loading && <span className="text-sm text-gray-500">Loading...</span>}
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>
                    <Button 
                      variant="ghost" 
                      size="sm"
                      onClick={() => handleSort('score')}
                      className="font-medium hover:bg-transparent p-0 h-auto"
                    >
                      Score
                      {sortField === 'score' && (
                        sortDirection === 'desc' ? <ArrowDown className="w-4 h-4 ml-1" /> : <ArrowUp className="w-4 h-4 ml-1" />
                      )}
                      {sortField !== 'score' && <ArrowUpDown className="w-4 h-4 ml-1 text-gray-400" />}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button 
                      variant="ghost" 
                      size="sm"
                      onClick={() => handleSort('name')}
                      className="font-medium hover:bg-transparent p-0 h-auto"
                    >
                      Lead & Contact
                      {sortField === 'name' && (
                        sortDirection === 'desc' ? <ArrowDown className="w-4 h-4 ml-1" /> : <ArrowUp className="w-4 h-4 ml-1" />
                      )}
                      {sortField !== 'name' && <ArrowUpDown className="w-4 h-4 ml-1 text-gray-400" />}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button 
                      variant="ghost" 
                      size="sm"
                      onClick={() => handleSort('source')}
                      className="font-medium hover:bg-transparent p-0 h-auto"
                    >
                      Source
                      {sortField === 'source' && (
                        sortDirection === 'desc' ? <ArrowDown className="w-4 h-4 ml-1" /> : <ArrowUp className="w-4 h-4 ml-1" />
                      )}
                      {sortField !== 'source' && <ArrowUpDown className="w-4 h-4 ml-1 text-gray-400" />}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button 
                      variant="ghost" 
                      size="sm"
                      onClick={() => handleSort('project')}
                      className="font-medium hover:bg-transparent p-0 h-auto"
                    >
                      Project
                      {sortField === 'project' && (
                        sortDirection === 'desc' ? <ArrowDown className="w-4 h-4 ml-1" /> : <ArrowUp className="w-4 h-4 ml-1" />
                      )}
                      {sortField !== 'project' && <ArrowUpDown className="w-4 h-4 ml-1 text-gray-400" />}
                    </Button>
                  </TableHead>
                  <TableHead>Stage</TableHead>
                  <TableHead>Latest Note</TableHead>
                  <TableHead>
                    <Button 
                      variant="ghost" 
                      size="sm"
                      onClick={() => handleSort('createdAt')}
                      className="font-medium hover:bg-transparent p-0 h-auto"
                    >
                      Created
                      {sortField === 'createdAt' && (
                        sortDirection === 'desc' ? <ArrowDown className="w-4 h-4 ml-1" /> : <ArrowUp className="w-4 h-4 ml-1" />
                      )}
                      {sortField !== 'createdAt' && <ArrowUpDown className="w-4 h-4 ml-1 text-gray-400" />}
                    </Button>
                  </TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {leads.map((lead) => (
                  <TableRow 
                    key={lead.id}
                    className="cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800"
                    onClick={() => handleRowClick(lead.id)}
                  >
                    <TableCell>
                      <div className="flex items-center space-x-2">
                        <div className={`w-2 h-8 rounded ${STAGE_COLORS[lead.stage as keyof typeof STAGE_COLORS] || 'bg-gray-300'}`} />
                        <div className="text-center">
                          <div className="font-bold text-lg">{lead.score}</div>
                          <TrendingUp className="w-3 h-3 text-gray-400 mx-auto" />
                        </div>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div>
                        <div className="font-medium">
                          {lead.name || 'Unknown'}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-400">
                          {lead.phone}
                        </div>
                        {lead.email && (
                          <div className="text-sm text-gray-500">
                            {lead.email}
                          </div>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        <Badge 
                          variant="secondary"
                          className={SOURCE_COLORS[lead.source as keyof typeof SOURCE_COLORS]}
                        >
                          {lead.source.replace('_', ' ')}
                        </Badge>
                        {lead.campaign && (
                          <div className="text-xs text-gray-500">
                            {lead.campaign}
                          </div>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="text-sm">
                        {lead.project ? (
                          <div className="font-medium">{lead.project.name}</div>
                        ) : (
                          <span className="text-gray-400 italic">No project</span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center space-x-2">
                        <Badge 
                          variant={lead.stage === 'NEW' ? 'default' : 'secondary'}
                          className={lead.stage === 'NEW' ? 'bg-blue-500' : ''}
                        >
                          {lead.stage}
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
                                handleStageChange(lead.id, 'NEW')
                              }}
                              disabled={lead.stage === 'NEW'}
                            >
                              New
                            </DropdownMenuItem>
                            <DropdownMenuItem 
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStageChange(lead.id, 'CONTACTED')
                              }}
                              disabled={lead.stage === 'CONTACTED'}
                            >
                              Contacted
                            </DropdownMenuItem>
                            <DropdownMenuItem 
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStageChange(lead.id, 'SCHEDULED')
                              }}
                              disabled={lead.stage === 'SCHEDULED'}
                            >
                              Scheduled
                            </DropdownMenuItem>
                            <DropdownMenuItem 
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStageChange(lead.id, 'VISITED')
                              }}
                              disabled={lead.stage === 'VISITED'}
                            >
                              Visited
                            </DropdownMenuItem>
                            <DropdownMenuItem 
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStageChange(lead.id, 'NEGOTIATION')
                              }}
                              disabled={lead.stage === 'NEGOTIATION'}
                            >
                              Negotiation
                            </DropdownMenuItem>
                            <DropdownMenuItem 
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStageChange(lead.id, 'BOOKED')
                              }}
                              disabled={lead.stage === 'BOOKED'}
                            >
                              Booked
                            </DropdownMenuItem>
                            <DropdownMenuItem 
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStageChange(lead.id, 'DROPPED')
                              }}
                              disabled={lead.stage === 'DROPPED'}
                            >
                              Dropped
                            </DropdownMenuItem>
                            <DropdownMenuItem 
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStageChange(lead.id, 'RESCHEDULED')
                              }}
                              disabled={lead.stage === 'RESCHEDULED'}
                            >
                              Rescheduled
                            </DropdownMenuItem>
                            <DropdownMenuItem 
                              onClick={(e) => {
                                e.stopPropagation()
                                handleStageChange(lead.id, 'INVALID')
                              }}
                              disabled={lead.stage === 'INVALID'}
                            >
                              Invalid Lead
                            </DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="text-sm max-w-[200px]">
                        {lead.lastActivity?.note ? (
                          <div>
                            <div className="font-medium text-gray-900 dark:text-white truncate">
                              {lead.lastActivity.note.length > 50 
                                ? `${lead.lastActivity.note.substring(0, 50)}...`
                                : lead.lastActivity.note
                              }
                            </div>
                            <div className="text-xs text-gray-500 mt-1">
                              {new Date(lead.lastActivity.at).toLocaleDateString('en-IN')} • {lead.lastActivity.user?.name || 'System'}
                            </div>
                          </div>
                        ) : (
                          <span className="text-gray-400 italic">No notes</span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="text-sm">
                        <div className="font-medium">
                          {new Date(lead.createdAt).toLocaleDateString('en-IN')}
                        </div>
                        <div className="text-gray-500">
                          {new Date(lead.createdAt).toLocaleTimeString('en-IN', { 
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
                            handleCall(lead.phone)
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
                            handleWhatsApp(lead)
                          }}
                          title="WhatsApp"
                        >
                          <MessageCircle className="w-4 h-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          {/* Pagination */}
          {pagination && pagination.totalPages > 1 && (
            <div className="flex items-center justify-between p-4 border-t">
              <div className="text-sm text-gray-600 dark:text-gray-400">
                Page {pagination.page} of {pagination.totalPages} 
                ({pagination.total} total leads)
              </div>
              <div className="flex space-x-2">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!pagination.hasPrev}
                  onClick={() => fetchLeads(pagination.page - 1)}
                >
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!pagination.hasNext}
                  onClick={() => fetchLeads(pagination.page + 1)}
                >
                  Next
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}