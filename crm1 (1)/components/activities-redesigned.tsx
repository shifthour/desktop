"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Calendar } from "@/components/ui/calendar"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { 
  Plus, Phone, Mail, MessageSquare, Calendar as CalendarIcon, Clock, 
  CheckCircle, AlertTriangle, Target, Users, FileText, Video,
  Filter, Search, MoreHorizontal, Edit, Trash2, Bot, Play
} from "lucide-react"
import { format } from "date-fns"
import { AIHubDashboard } from "@/components/ai-hub-dashboard"

interface Activity {
  id: string
  activity_type: string
  title: string
  description?: string
  entity_type?: string
  entity_id?: string
  entity_name?: string
  contact_name?: string
  contact_phone?: string
  contact_email?: string
  contact_whatsapp?: string
  scheduled_date?: string
  due_date?: string
  completed_date?: string
  status: string
  priority: string
  assigned_to?: string
  outcome?: string
  outcome_notes?: string
  next_action?: string
  follow_up_required?: boolean
  follow_up_date?: string
  created_at: string
}

const activityTypes = [
  { value: 'call', label: 'Phone Call', icon: Phone },
  { value: 'email', label: 'Email', icon: Mail },
  { value: 'meeting', label: 'Meeting', icon: Users },
  { value: 'demo', label: 'Product Demo', icon: Video },
  { value: 'follow-up', label: 'Follow-up', icon: Clock },
  { value: 'task', label: 'Task', icon: FileText },
  { value: 'quote', label: 'Quote Preparation', icon: Target }
]

const statusOptions = [
  { value: 'pending', label: 'Pending', color: 'bg-yellow-100 text-yellow-800' },
  { value: 'in-progress', label: 'In Progress', color: 'bg-blue-100 text-blue-800' },
  { value: 'completed', label: 'Completed', color: 'bg-green-100 text-green-800' },
  { value: 'overdue', label: 'Overdue', color: 'bg-red-100 text-red-800' },
  { value: 'cancelled', label: 'Cancelled', color: 'bg-gray-100 text-gray-800' }
]

const priorityOptions = [
  { value: 'low', label: 'Low', color: 'bg-green-100 text-green-800' },
  { value: 'medium', label: 'Medium', color: 'bg-yellow-100 text-yellow-800' },
  { value: 'high', label: 'High', color: 'bg-orange-100 text-orange-800' },
  { value: 'urgent', label: 'Urgent', color: 'bg-red-100 text-red-800' }
]

const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
  </svg>
)

export function ActivitiesRedesigned() {
  const [activities, setActivities] = useState<Activity[]>([])
  const [dashboard, setDashboard] = useState<any>({})
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'today' | 'ai-hub' | 'all'>('today')
  const [filters, setFilters] = useState({
    search: '',
    type: 'all',
    status: 'all',
    assignedTo: 'all'
  })
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [selectedActivity, setSelectedActivity] = useState<Activity | null>(null)
  const [isUpdateModalOpen, setIsUpdateModalOpen] = useState(false)

  useEffect(() => {
    loadActivities()
  }, [])

  const loadActivities = async () => {
    try {
      setLoading(true)
      const userStr = localStorage.getItem('user')
      if (!userStr) return
      
      const user = JSON.parse(userStr)
      const companyId = user.company_id

      const response = await fetch(`/api/activities?companyId=${companyId}&limit=200`)
      const data = await response.json()
      
      setActivities(data.activities || [])
      setDashboard(data.dashboard || {})
    } catch (error) {
      console.error('Error loading activities:', error)
    } finally {
      setLoading(false)
    }
  }

  const createActivity = async (activityData: any) => {
    try {
      const userStr = localStorage.getItem('user')
      if (!userStr) return
      
      const user = JSON.parse(userStr)
      
      const response = await fetch('/api/activities', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...activityData,
          companyId: user.company_id,
          userId: user.id
        })
      })
      
      if (response.ok) {
        loadActivities()
        setIsCreateModalOpen(false)
      }
    } catch (error) {
      console.error('Error creating activity:', error)
    }
  }

  const updateActivityStatus = async (activityId: string, status: string, outcome?: string) => {
    try {
      const response = await fetch('/api/activities', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: activityId,
          status,
          outcome,
          completedDate: status === 'completed' ? new Date().toISOString() : undefined
        })
      })
      
      if (response.ok) {
        loadActivities()
      }
    } catch (error) {
      console.error('Error updating activity:', error)
    }
  }

  const handleContactAction = (activity: Activity, actionType: 'call' | 'email' | 'whatsapp') => {
    switch (actionType) {
      case 'call':
        if (activity.contact_phone) {
          window.open(`tel:${activity.contact_phone}`)
          updateActivityStatus(activity.id, 'in-progress', 'call-made')
        }
        break
      case 'email':
        if (activity.contact_email) {
          const subject = encodeURIComponent(`Re: ${activity.title}`)
          const body = encodeURIComponent(`Dear ${activity.contact_name},\n\nRegarding ${activity.description || activity.title}.\n\nBest regards`)
          window.open(`mailto:${activity.contact_email}?subject=${subject}&body=${body}`)
          updateActivityStatus(activity.id, 'in-progress', 'email-sent')
        }
        break
      case 'whatsapp':
        const whatsappNumber = activity.contact_whatsapp || activity.contact_phone
        if (whatsappNumber) {
          const message = encodeURIComponent(`Hi ${activity.contact_name?.split(' ')[0]}, regarding ${activity.title}`)
          window.open(`https://wa.me/${whatsappNumber.replace(/[^0-9]/g, '')}?text=${message}`)
          updateActivityStatus(activity.id, 'in-progress', 'whatsapp-sent')
        }
        break
    }
  }

  const getActivityIcon = (type: string) => {
    const activityType = activityTypes.find(t => t.value === type)
    return activityType?.icon || FileText
  }

  const getStatusColor = (status: string) => {
    const statusOption = statusOptions.find(s => s.value === status)
    return statusOption?.color || 'bg-gray-100 text-gray-800'
  }

  const getPriorityColor = (priority: string) => {
    const priorityOption = priorityOptions.find(p => p.value === priority)
    return priorityOption?.color || 'bg-gray-100 text-gray-800'
  }

  const filteredActivities = activities.filter(activity => {
    const matchesSearch = !filters.search || 
      activity.title.toLowerCase().includes(filters.search.toLowerCase()) ||
      activity.entity_name?.toLowerCase().includes(filters.search.toLowerCase()) ||
      activity.contact_name?.toLowerCase().includes(filters.search.toLowerCase())
    
    const matchesType = filters.type === 'all' || activity.activity_type === filters.type
    const matchesStatus = filters.status === 'all' || activity.status === filters.status
    const matchesAssigned = filters.assignedTo === 'all' || activity.assigned_to === filters.assignedTo
    
    return matchesSearch && matchesType && matchesStatus && matchesAssigned
  })

  const todayActivities = filteredActivities.filter(activity => {
    const today = new Date().toDateString()
    const scheduledDate = activity.scheduled_date ? new Date(activity.scheduled_date).toDateString() : null
    const dueDate = activity.due_date ? new Date(activity.due_date).toDateString() : null
    return scheduledDate === today || dueDate === today
  })

  const overdueActivities = filteredActivities.filter(activity => {
    const now = new Date()
    const dueDate = activity.due_date ? new Date(activity.due_date) : null
    return dueDate && dueDate < now && activity.status !== 'completed'
  })

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Activities Center</h1>
          <p className="text-gray-600">Comprehensive activity management powered by real CRM data</p>
        </div>
        <Button onClick={() => setIsCreateModalOpen(true)}>
          <Plus className="w-4 h-4 mr-2" />
          Create Activity
        </Button>
      </div>

      {/* Dashboard Stats */}
      <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
        <Card>
          <CardContent className="p-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-blue-600">{loading ? '...' : dashboard.total_activities || 0}</div>
              <div className="text-sm text-gray-600">Total</div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-green-600">{loading ? '...' : dashboard.completed_today || 0}</div>
              <div className="text-sm text-gray-600">Done Today</div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-orange-600">{loading ? '...' : dashboard.today_scheduled || 0}</div>
              <div className="text-sm text-gray-600">Scheduled</div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-red-600">{loading ? '...' : dashboard.overdue || 0}</div>
              <div className="text-sm text-gray-600">Overdue</div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-purple-600">{loading ? '...' : dashboard.pending || 0}</div>
              <div className="text-sm text-gray-600">Pending</div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-indigo-600">{loading ? '...' : dashboard.follow_ups_due || 0}</div>
              <div className="text-sm text-gray-600">Follow-ups</div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Tab Navigation */}
      <Tabs value={activeTab} onValueChange={(value: any) => setActiveTab(value)} className="w-full">
        <TabsList className="grid w-full grid-cols-3">
          <TabsTrigger value="today" className="flex items-center">
            <CalendarIcon className="w-4 h-4 mr-2" />
            Today's Focus
          </TabsTrigger>
          <TabsTrigger value="ai-hub" className="flex items-center">
            <Bot className="w-4 h-4 mr-2" />
            AI Command Center
          </TabsTrigger>
          <TabsTrigger value="all" className="flex items-center">
            <FileText className="w-4 h-4 mr-2" />
            All Activities
          </TabsTrigger>
        </TabsList>

        {/* Today's Focus Tab */}
        <TabsContent value="today" className="space-y-6">
          {/* Overdue Section */}
          {overdueActivities.length > 0 && (
            <Card className="border-l-4 border-l-red-500">
              <CardHeader>
                <CardTitle className="flex items-center text-red-700">
                  <AlertTriangle className="w-5 h-5 mr-2" />
                  Overdue Activities ({overdueActivities.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {overdueActivities.map(activity => {
                    const Icon = getActivityIcon(activity.activity_type)
                    return (
                      <div key={activity.id} className="flex items-center justify-between p-3 bg-red-50 rounded-lg border border-red-200">
                        <div className="flex items-center space-x-3">
                          <Icon className="w-5 h-5 text-red-600" />
                          <div>
                            <h4 className="font-medium text-gray-900">{activity.title}</h4>
                            <p className="text-sm text-gray-600">{activity.entity_name} • {activity.contact_name}</p>
                            <p className="text-xs text-red-600">Due: {activity.due_date ? format(new Date(activity.due_date), 'MMM dd') : 'No date'}</p>
                          </div>
                        </div>
                        <div className="flex space-x-2">
                          <Button size="sm" onClick={() => updateActivityStatus(activity.id, 'completed')}>
                            <CheckCircle className="w-3 h-3" />
                          </Button>
                        </div>
                      </div>
                    )
                  })}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Today's Activities */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <CalendarIcon className="w-5 h-5 mr-2 text-blue-600" />
                Today's Activities ({todayActivities.length})
              </CardTitle>
            </CardHeader>
            <CardContent>
              {todayActivities.length === 0 ? (
                <div className="text-center py-8">
                  <CalendarIcon className="w-12 h-12 mx-auto mb-4 text-gray-400" />
                  <h3 className="text-lg font-medium text-gray-900 mb-2">No activities scheduled for today</h3>
                  <p className="text-gray-600">Create a new activity to get started</p>
                </div>
              ) : (
                <div className="space-y-3">
                  {todayActivities.map(activity => {
                    const Icon = getActivityIcon(activity.activity_type)
                    return (
                      <div key={activity.id} className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50">
                        <div className="flex items-center space-x-4">
                          <div className="p-2 rounded-lg bg-blue-100">
                            <Icon className="w-5 h-5 text-blue-600" />
                          </div>
                          <div>
                            <h4 className="font-medium text-gray-900">{activity.title}</h4>
                            <p className="text-sm text-gray-600">{activity.description}</p>
                            <div className="flex items-center space-x-4 text-xs text-gray-500 mt-1">
                              <span>{activity.entity_type}: {activity.entity_name}</span>
                              <span>Contact: {activity.contact_name}</span>
                              <Badge className={getPriorityColor(activity.priority)} variant="outline">
                                {activity.priority}
                              </Badge>
                            </div>
                          </div>
                        </div>
                        <div className="flex items-center space-x-2">
                          <Badge className={getStatusColor(activity.status)}>
                            {activity.status}
                          </Badge>
                          {activity.status === 'pending' && (
                            <Button size="sm" onClick={() => updateActivityStatus(activity.id, 'in-progress')}>
                              <Play className="w-3 h-3 mr-1" />
                              Start
                            </Button>
                          )}
                          {activity.status === 'in-progress' && (
                            <Button size="sm" variant="outline" onClick={() => {
                              setSelectedActivity(activity)
                              setIsUpdateModalOpen(true)
                            }}>
                              <CheckCircle className="w-3 h-3 mr-1" />
                              Complete
                            </Button>
                          )}
                          {activity.contact_phone && (
                            <Button size="sm" variant="outline" onClick={() => handleContactAction(activity, 'call')}>
                              <Phone className="w-3 h-3" />
                            </Button>
                          )}
                          {(activity.contact_whatsapp || activity.contact_phone) && (
                            <Button size="sm" variant="outline" onClick={() => handleContactAction(activity, 'whatsapp')} className="text-green-600 hover:text-green-800">
                              <WhatsAppIcon className="w-3 h-3" />
                            </Button>
                          )}
                          {activity.contact_email && (
                            <Button size="sm" variant="outline" onClick={() => handleContactAction(activity, 'email')}>
                              <Mail className="w-3 h-3" />
                            </Button>
                          )}
                          <Button size="sm" variant="ghost" onClick={() => {
                            setSelectedActivity(activity)
                            setIsUpdateModalOpen(true)
                          }}>
                            <Edit className="w-3 h-3" />
                          </Button>
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* AI Hub Tab */}
        <TabsContent value="ai-hub">
          <AIHubDashboard />
        </TabsContent>

        {/* All Activities Tab */}
        <TabsContent value="all" className="space-y-6">
          {/* Filters */}
          <Card>
            <CardContent className="p-4">
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                  <Input
                    placeholder="Search activities..."
                    value={filters.search}
                    onChange={(e) => setFilters({...filters, search: e.target.value})}
                    className="pl-10"
                  />
                </div>
                <Select value={filters.type} onValueChange={(value) => setFilters({...filters, type: value})}>
                  <SelectTrigger>
                    <SelectValue placeholder="Activity Type" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Types</SelectItem>
                    {activityTypes.map(type => (
                      <SelectItem key={type.value} value={type.value}>{type.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Select value={filters.status} onValueChange={(value) => setFilters({...filters, status: value})}>
                  <SelectTrigger>
                    <SelectValue placeholder="Status" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Status</SelectItem>
                    {statusOptions.map(status => (
                      <SelectItem key={status.value} value={status.value}>{status.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button variant="outline" onClick={loadActivities}>
                  <Filter className="w-4 h-4 mr-2" />
                  Refresh
                </Button>
              </div>
            </CardContent>
          </Card>

          {/* Activities List */}
          <Card>
            <CardHeader>
              <CardTitle>All Activities ({filteredActivities.length})</CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="text-center py-8">Loading activities...</div>
              ) : filteredActivities.length === 0 ? (
                <div className="text-center py-8">
                  <FileText className="w-12 h-12 mx-auto mb-4 text-gray-400" />
                  <h3 className="text-lg font-medium text-gray-900 mb-2">No activities found</h3>
                  <p className="text-gray-600">Create your first activity to get started</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {filteredActivities.map(activity => {
                    const Icon = getActivityIcon(activity.activity_type)
                    return (
                      <div key={activity.id} className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50">
                        <div className="flex items-center space-x-4">
                          <Icon className="w-5 h-5 text-gray-600" />
                          <div>
                            <h4 className="font-medium">{activity.title}</h4>
                            <p className="text-sm text-gray-600">{activity.description}</p>
                            <div className="flex items-center space-x-3 text-xs text-gray-500 mt-1">
                              <span>{activity.entity_type}: {activity.entity_name}</span>
                              <span>Assigned: {activity.assigned_to}</span>
                              {activity.scheduled_date && (
                                <span>Scheduled: {format(new Date(activity.scheduled_date), 'MMM dd, HH:mm')}</span>
                              )}
                            </div>
                          </div>
                        </div>
                        <div className="flex items-center space-x-2">
                          <Badge className={getStatusColor(activity.status)}>
                            {activity.status}
                          </Badge>
                          <Badge className={getPriorityColor(activity.priority)} variant="outline">
                            {activity.priority}
                          </Badge>
                          {activity.contact_phone && (
                            <Button size="sm" variant="outline" onClick={() => handleContactAction(activity, 'call')}>
                              <Phone className="w-3 h-3" />
                            </Button>
                          )}
                          {(activity.contact_whatsapp || activity.contact_phone) && (
                            <Button size="sm" variant="outline" onClick={() => handleContactAction(activity, 'whatsapp')} className="text-green-600 hover:text-green-800">
                              <WhatsAppIcon className="w-3 h-3" />
                            </Button>
                          )}
                          {activity.contact_email && (
                            <Button size="sm" variant="outline" onClick={() => handleContactAction(activity, 'email')}>
                              <Mail className="w-3 h-3" />
                            </Button>
                          )}
                          <Button size="sm" variant="ghost" onClick={() => {
                            setSelectedActivity(activity)
                            setIsUpdateModalOpen(true)
                          }}>
                            <Edit className="w-4 h-4" />
                          </Button>
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Create Activity Modal */}
      <CreateActivityModal 
        isOpen={isCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onCreate={createActivity}
      />

      {/* Update Activity Modal */}
      <UpdateActivityModal
        activity={selectedActivity}
        isOpen={isUpdateModalOpen}
        onClose={() => {
          setIsUpdateModalOpen(false)
          setSelectedActivity(null)
        }}
        onUpdate={loadActivities}
      />
    </div>
  )
}

function CreateActivityModal({ isOpen, onClose, onCreate }: any) {
  const [formData, setFormData] = useState({
    activityType: '',
    title: '',
    description: '',
    entityType: '',
    entityId: '',
    contactName: '',
    contactPhone: '',
    contactEmail: '',
    scheduledDate: '',
    dueDate: '',
    priority: 'medium',
    assignedTo: ''
  })

  const handleSubmit = () => {
    if (!formData.activityType || !formData.title) return
    onCreate(formData)
    setFormData({
      activityType: '',
      title: '',
      description: '',
      entityType: '',
      entityId: '',
      contactName: '',
      contactPhone: '',
      contactEmail: '',
      scheduledDate: '',
      dueDate: '',
      priority: 'medium',
      assignedTo: ''
    })
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Create New Activity</DialogTitle>
        </DialogHeader>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <Label>Activity Type</Label>
            <Select value={formData.activityType} onValueChange={(value) => setFormData({...formData, activityType: value})}>
              <SelectTrigger>
                <SelectValue placeholder="Select type" />
              </SelectTrigger>
              <SelectContent>
                {activityTypes.map(type => (
                  <SelectItem key={type.value} value={type.value}>{type.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label>Priority</Label>
            <Select value={formData.priority} onValueChange={(value) => setFormData({...formData, priority: value})}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {priorityOptions.map(priority => (
                  <SelectItem key={priority.value} value={priority.value}>{priority.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="col-span-2">
            <Label>Title</Label>
            <Input 
              value={formData.title}
              onChange={(e) => setFormData({...formData, title: e.target.value})}
              placeholder="Activity title"
            />
          </div>
          <div className="col-span-2">
            <Label>Description</Label>
            <Textarea 
              value={formData.description}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
              placeholder="Activity description"
            />
          </div>
          <div>
            <Label>Contact Name</Label>
            <Input 
              value={formData.contactName}
              onChange={(e) => setFormData({...formData, contactName: e.target.value})}
              placeholder="Contact person"
            />
          </div>
          <div>
            <Label>Contact Phone</Label>
            <Input 
              value={formData.contactPhone}
              onChange={(e) => setFormData({...formData, contactPhone: e.target.value})}
              placeholder="Phone number"
            />
          </div>
          <div>
            <Label>Contact Email</Label>
            <Input 
              value={formData.contactEmail}
              onChange={(e) => setFormData({...formData, contactEmail: e.target.value})}
              placeholder="Email address"
            />
          </div>
          <div>
            <Label>Scheduled Date & Time</Label>
            <Input 
              type="datetime-local"
              value={formData.scheduledDate}
              onChange={(e) => setFormData({...formData, scheduledDate: e.target.value})}
            />
          </div>
          <div>
            <Label>Due Date</Label>
            <Input 
              type="date"
              value={formData.dueDate}
              onChange={(e) => setFormData({...formData, dueDate: e.target.value})}
            />
          </div>
          <div>
            <Label>Assigned To</Label>
            <Input 
              value={formData.assignedTo}
              onChange={(e) => setFormData({...formData, assignedTo: e.target.value})}
              placeholder="Person responsible"
            />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={handleSubmit}>Create Activity</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function UpdateActivityModal({ activity, isOpen, onClose, onUpdate }: any) {
  const [outcome, setOutcome] = useState('')
  const [notes, setNotes] = useState('')
  const [nextAction, setNextAction] = useState('')
  const [followUpRequired, setFollowUpRequired] = useState(false)

  const handleComplete = async () => {
    try {
      const response = await fetch('/api/activities', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: activity.id,
          status: 'completed',
          outcome,
          outcomeNotes: notes,
          nextAction,
          followUpRequired,
          followUpDate: followUpRequired ? new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0] : null
        })
      })
      
      if (response.ok) {
        onUpdate()
        onClose()
      }
    } catch (error) {
      console.error('Error completing activity:', error)
    }
  }

  if (!activity) return null

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Complete Activity</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          <div>
            <h4 className="font-medium">{activity.title}</h4>
            <p className="text-sm text-gray-600">{activity.description}</p>
          </div>
          <div>
            <Label>Outcome</Label>
            <Select value={outcome} onValueChange={setOutcome}>
              <SelectTrigger>
                <SelectValue placeholder="Select outcome" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="successful">Successful</SelectItem>
                <SelectItem value="no-answer">No Answer</SelectItem>
                <SelectItem value="callback-requested">Callback Requested</SelectItem>
                <SelectItem value="meeting-scheduled">Meeting Scheduled</SelectItem>
                <SelectItem value="not-interested">Not Interested</SelectItem>
                <SelectItem value="need-more-info">Need More Info</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label>Notes</Label>
            <Textarea 
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="Activity outcome notes"
            />
          </div>
          <div>
            <Label>Next Action</Label>
            <Input 
              value={nextAction}
              onChange={(e) => setNextAction(e.target.value)}
              placeholder="What should happen next?"
            />
          </div>
          <div className="flex items-center space-x-2">
            <input 
              type="checkbox" 
              checked={followUpRequired}
              onChange={(e) => setFollowUpRequired(e.target.checked)}
            />
            <Label>Schedule follow-up in 7 days</Label>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={handleComplete}>Complete Activity</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}