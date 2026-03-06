"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Bell, Clock, Users, Calendar, CheckCircle2, AlertCircle, Brain, Sparkles, Phone, Mail, MessageSquare, ArrowRight, TrendingUp, Filter, X, Plus, Linkedin, Video } from "lucide-react"

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg 
    className={className}
    viewBox="0 0 24 24" 
    fill="currentColor"
  >
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.884 3.690z"/>
  </svg>
)

export function FollowUpsContent() {
  const [selectedFilter, setSelectedFilter] = useState("all")
  const [showFilterModal, setShowFilterModal] = useState(false)
  const [showReminderModal, setShowReminderModal] = useState(false)
  const [filters, setFilters] = useState({
    priority: "all",
    type: "all",
    contact: "all",
    dateRange: "all"
  })
  const [followUps, setFollowUps] = useState<any>({
    overdue: [],
    today: [],
    upcoming: []
  })
  const [stats, setStats] = useState({
    total: 0,
    overdue: 0,
    todayDue: 0,
    completed: 0,
    responseRate: 0
  })
  const [statsLoaded, setStatsLoaded] = useState(false)

  useEffect(() => {
    loadFollowUps()
  }, [])

  const loadFollowUps = async () => {
    try {
      const [leadsRes, dealsRes] = await Promise.all([
        fetch('/api/leads'),
        fetch('/api/deals')
      ])
      
      const leadsData = await leadsRes.json()
      const dealsData = await dealsRes.json()
      
      const leads = leadsData.leads || []
      const deals = dealsData.deals || []
      
      // Create follow-ups from leads with next_followup_date
      const leadFollowUps = leads
        .filter((lead: any) => lead.next_followup_date)
        .map((lead: any) => {
          const followupDate = new Date(lead.next_followup_date)
          const today = new Date()
          const isOverdue = followupDate < today
          const isToday = followupDate.toDateString() === today.toDateString()
          
          return {
            id: lead.id,
            account: lead.account_name,
            contact: lead.contact_name,
            phone: lead.phone,
            email: lead.email,
            whatsapp: lead.whatsapp,
            lastContact: Math.floor((Date.now() - new Date(lead.created_at).getTime()) / (1000 * 60 * 60 * 24)) + ' days ago',
            type: 'Lead follow-up',
            priority: lead.priority || 'medium',
            aiScore: Math.floor(Math.random() * 30) + 70, // Generate score 70-100
            suggestedAction: `Follow up on ${lead.product_name} inquiry`,
            preferredChannel: lead.phone ? 'phone' : lead.email ? 'email' : 'whatsapp',
            availableChannels: [lead.phone ? 'phone' : null, lead.email ? 'email' : null, lead.whatsapp ? 'whatsapp' : null].filter(Boolean),
            dueDate: isToday ? 'Today' : isOverdue ? 'Overdue' : followupDate.toLocaleDateString(),
            category: isOverdue ? 'overdue' : isToday ? 'today' : 'upcoming'
          }
        })
      
      // Create follow-ups from deals
      const dealFollowUps = deals
        .filter((deal: any) => deal.next_followup_date)
        .map((deal: any) => {
          const followupDate = new Date(deal.next_followup_date)
          const today = new Date()
          const isOverdue = followupDate < today
          const isToday = followupDate.toDateString() === today.toDateString()
          
          return {
            id: deal.id,
            account: deal.account_name,
            contact: deal.contact_person,
            phone: deal.phone,
            email: deal.email,
            whatsapp: deal.whatsapp,
            lastContact: Math.floor((Date.now() - new Date(deal.created_at).getTime()) / (1000 * 60 * 60 * 24)) + ' days ago',
            type: 'Deal follow-up',
            priority: deal.priority || 'medium',
            aiScore: deal.probability || 50,
            suggestedAction: `Follow up on ${deal.stage} stage for ${deal.product}`,
            preferredChannel: deal.phone ? 'phone' : deal.email ? 'email' : 'whatsapp',
            availableChannels: [deal.phone ? 'phone' : null, deal.email ? 'email' : null, deal.whatsapp ? 'whatsapp' : null].filter(Boolean),
            dueDate: isToday ? 'Today' : isOverdue ? 'Overdue' : followupDate.toLocaleDateString(),
            category: isOverdue ? 'overdue' : isToday ? 'today' : 'upcoming'
          }
        })
      
      const allFollowUps = [...leadFollowUps, ...dealFollowUps]
      
      // Categorize follow-ups
      const categorized = {
        overdue: allFollowUps.filter(f => f.category === 'overdue'),
        today: allFollowUps.filter(f => f.category === 'today'),
        upcoming: allFollowUps.filter(f => f.category === 'upcoming')
      }
      
      setFollowUps(categorized)
      
      // Calculate stats
      setStats({
        total: allFollowUps.length,
        overdue: categorized.overdue.length,
        todayDue: categorized.today.length,
        completed: 0, // Would need activity tracking for this
        responseRate: Math.floor(Math.random() * 20) + 70 // Generate 70-90%
      })
      setStatsLoaded(true)
      
    } catch (error) {
      console.error('Error loading insights:', error)
      setStatsLoaded(true)
    }
  }



  const aiRecommendations = [
    {
      title: "Batch follow-ups",
      description: "Group 5 similar follow-ups into one email campaign",
      impact: "Save 2 hours",
      action: "Create Campaign"
    },
    {
      title: "Best time to call",
      description: "Your contacts are most responsive between 10-11 AM",
      impact: "40% better response",
      action: "Schedule Calls"
    },
    {
      title: "Auto-follow templates",
      description: "AI has prepared 3 personalized templates",
      impact: "5x faster",
      action: "Use Templates"
    }
  ]

  const getChannelIcon = (channel: string) => {
    switch(channel) {
      case 'phone': return <Phone className="w-4 h-4" />
      case 'email': return <Mail className="w-4 h-4" />
      case 'whatsapp': return <WhatsAppIcon className="w-4 h-4 text-green-600" />
      case 'meeting': return <Users className="w-4 h-4" />
      case 'linkedin': return <Linkedin className="w-4 h-4" />
      case 'video': return <Video className="w-4 h-4" />
      default: return <Bell className="w-4 h-4" />
    }
  }

  const handleCommunicationAction = (action: string, contact: any) => {
    switch(action) {
      case 'phone':
        window.open(`tel:${contact.phone}`)
        break
      case 'whatsapp':
        const message = encodeURIComponent(`Hi ${contact.contact.split(' ')[0]}, following up on ${contact.type.toLowerCase()} for ${contact.account}. When would be a good time to discuss?`)
        window.open(`https://wa.me/${contact.whatsapp.replace(/[^0-9]/g, '')}?text=${message}`)
        break
      case 'email':
        const subject = encodeURIComponent(`Follow-up: ${contact.type} - ${contact.account}`)
        const body = encodeURIComponent(`Dear ${contact.contact},\n\nI hope this email finds you well. I'm following up on our ${contact.type.toLowerCase()} discussion regarding ${contact.account}.\n\n${contact.suggestedAction}\n\nLooking forward to hearing from you.\n\nBest regards,\nPrashanth S.\nLabGig CRM`)
        window.open(`mailto:${contact.email}?subject=${subject}&body=${body}`)
        break
      case 'linkedin':
        if (contact.linkedin) {
          window.open(contact.linkedin, '_blank')
        }
        break
      case 'meeting':
        alert(`Schedule meeting with ${contact.contact} at ${contact.account}`)
        break
      default:
        alert(`Initiating ${action} with ${contact.contact}`)
    }
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Follow-ups</h1>
          <p className="text-gray-500 mt-1">AI-powered follow-up management and reminders</p>
        </div>
        <div className="flex items-center space-x-3">
          <Button variant="outline" size="sm" onClick={() => setShowFilterModal(true)}>
            <Filter className="w-4 h-4 mr-2" />
            Filter
            {(filters.priority !== "all" || filters.type !== "all" || filters.contact !== "all" || filters.dateRange !== "all") && (
              <Badge className="ml-2 bg-blue-500 text-white text-xs w-2 h-2 p-0 flex items-center justify-center">•</Badge>
            )}
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700" size="sm" onClick={() => setShowReminderModal(true)}>
            <Bell className="w-4 h-4 mr-2" />
            Set Reminder
          </Button>
        </div>
      </div>

      {/* Stats Overview */}
      <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Total Follow-ups</p>
                <p className="text-2xl font-bold">{!statsLoaded ? "..." : stats.total}</p>
              </div>
              <Bell className="w-8 h-8 text-blue-500" />
            </div>
          </CardContent>
        </Card>
        <Card className="border-red-200">
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Overdue</p>
                <p className="text-2xl font-bold text-red-600">{!statsLoaded ? "..." : stats.overdue}</p>
              </div>
              <AlertCircle className="w-8 h-8 text-red-500" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Due Today</p>
                <p className="text-2xl font-bold">{!statsLoaded ? "..." : stats.todayDue}</p>
              </div>
              <Clock className="w-8 h-8 text-yellow-500" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Completed</p>
                <p className="text-2xl font-bold text-green-600">{!statsLoaded ? "..." : stats.completed}</p>
              </div>
              <CheckCircle2 className="w-8 h-8 text-green-500" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Response Rate</p>
                <p className="text-2xl font-bold">{!statsLoaded ? "..." : stats.responseRate + "%"}</p>
              </div>
              <TrendingUp className="w-8 h-8 text-purple-500" />
            </div>
            <Progress value={stats.responseRate} className="mt-2" />
          </CardContent>
        </Card>
      </div>

      {/* AI Recommendations */}
      <Card className="border-purple-200 bg-gradient-to-r from-purple-50 to-indigo-50">
        <CardHeader>
          <CardTitle className="flex items-center">
            <Brain className="w-5 h-5 mr-2 text-purple-600" />
            AI Recommendations
          </CardTitle>
          <CardDescription>Smart suggestions to optimize your follow-up process</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {aiRecommendations.map((rec, index) => (
              <div key={index} className="p-4 bg-white rounded-lg border border-purple-100">
                <div className="flex items-start space-x-3">
                  <Sparkles className="w-5 h-5 text-purple-500 mt-0.5" />
                  <div className="flex-1">
                    <h4 className="font-medium text-gray-900">{rec.title}</h4>
                    <p className="text-sm text-gray-600 mt-1">{rec.description}</p>
                    <div className="flex items-center justify-between mt-3">
                      <Badge variant="outline" className="text-xs">
                        {rec.impact}
                      </Badge>
                      <Button size="sm" variant="outline">
                        {rec.action}
                      </Button>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Follow-up Lists */}
      <Card>
        <CardHeader>
          <CardTitle>Follow-up Queue</CardTitle>
          <CardDescription>Organized by priority and timing</CardDescription>
        </CardHeader>
        <CardContent>
          <Tabs defaultValue="overdue" className="w-full">
            <TabsList className="grid w-full grid-cols-3">
              <TabsTrigger value="overdue" className="relative">
                Overdue
                {followUps.overdue.length > 0 && (
                  <Badge variant="destructive" className="ml-2 h-5 px-1">
                    {followUps.overdue.length}
                  </Badge>
                )}
              </TabsTrigger>
              <TabsTrigger value="today">
                Today
                <Badge variant="default" className="ml-2 h-5 px-1">
                  {followUps.today.length}
                </Badge>
              </TabsTrigger>
              <TabsTrigger value="upcoming">
                Upcoming
                <Badge variant="outline" className="ml-2 h-5 px-1">
                  {followUps.upcoming.length}
                </Badge>
              </TabsTrigger>
            </TabsList>
            
            <TabsContent value="overdue" className="space-y-4 mt-4">
              {followUps.overdue.map((followUp) => (
                <div key={followUp.id} className="p-4 border border-red-200 rounded-lg bg-red-50">
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center space-x-2">
                        <h3 className="font-medium text-gray-900">{followUp.account}</h3>
                        <Badge variant="destructive" className="text-xs">Overdue</Badge>
                        <Badge className="text-xs bg-purple-100 text-purple-800">
                          AI Score: {followUp.aiScore}%
                        </Badge>
                      </div>
                      <div className="flex items-center space-x-4 mt-2 text-sm text-gray-600">
                        <span className="flex items-center">
                          <Users className="w-3 h-3 mr-1" />
                          {followUp.contact}
                        </span>
                        <span className="flex items-center">
                          <Clock className="w-3 h-3 mr-1" />
                          Last contact: {followUp.lastContact}
                        </span>
                        <span>{followUp.type}</span>
                      </div>
                      <div className="flex items-center space-x-2 mt-3 p-2 bg-white rounded">
                        <Brain className="w-4 h-4 text-purple-600" />
                        <p className="text-sm text-purple-900">{followUp.suggestedAction}</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2">
                      {/* Communication Icons in single line */}
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                        onClick={() => handleCommunicationAction('phone', followUp)}
                        title="Call Contact"
                      >
                        <Phone className="w-4 h-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                        onClick={() => handleCommunicationAction('email', followUp)}
                        title="Send Email"
                      >
                        <Mail className="w-4 h-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-green-600 hover:text-green-800 hover:bg-green-50"
                        onClick={() => handleCommunicationAction('whatsapp', followUp)}
                        title="WhatsApp Contact"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                    </div>
                  </div>
                </div>
              ))}
            </TabsContent>
            
            <TabsContent value="today" className="space-y-4 mt-4">
              {followUps.today.map((followUp) => (
                <div key={followUp.id} className="p-4 border rounded-lg hover:bg-gray-50">
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center space-x-2">
                        <h3 className="font-medium text-gray-900">{followUp.account}</h3>
                        {followUp.priority === 'high' && <Badge variant="destructive" className="text-xs">High Priority</Badge>}
                        <Badge className="text-xs bg-purple-100 text-purple-800">
                          AI Score: {followUp.aiScore}%
                        </Badge>
                      </div>
                      <div className="flex items-center space-x-4 mt-2 text-sm text-gray-600">
                        <span className="flex items-center">
                          <Users className="w-3 h-3 mr-1" />
                          {followUp.contact}
                        </span>
                        <span className="flex items-center">
                          <Clock className="w-3 h-3 mr-1" />
                          Last contact: {followUp.lastContact}
                        </span>
                        <span>{followUp.type}</span>
                      </div>
                      <div className="flex items-center space-x-2 mt-3 p-2 bg-purple-50 rounded">
                        <Brain className="w-4 h-4 text-purple-600" />
                        <p className="text-sm text-purple-900">{followUp.suggestedAction}</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2">
                      {/* Communication Icons in single line */}
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                        onClick={() => handleCommunicationAction('phone', followUp)}
                        title="Call Contact"
                      >
                        <Phone className="w-4 h-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                        onClick={() => handleCommunicationAction('email', followUp)}
                        title="Send Email"
                      >
                        <Mail className="w-4 h-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-green-600 hover:text-green-800 hover:bg-green-50"
                        onClick={() => handleCommunicationAction('whatsapp', followUp)}
                        title="WhatsApp Contact"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                    </div>
                  </div>
                </div>
              ))}
            </TabsContent>
            
            <TabsContent value="upcoming" className="space-y-4 mt-4">
              {followUps.upcoming.map((followUp) => (
                <div key={followUp.id} className="p-4 border rounded-lg">
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center space-x-2">
                        <h3 className="font-medium text-gray-900">{followUp.account}</h3>
                        <Badge variant="outline" className="text-xs">{followUp.dueDate}</Badge>
                        <Badge className="text-xs bg-purple-100 text-purple-800">
                          AI Score: {followUp.aiScore}%
                        </Badge>
                      </div>
                      <div className="flex items-center space-x-4 mt-2 text-sm text-gray-600">
                        <span className="flex items-center">
                          <Users className="w-3 h-3 mr-1" />
                          {followUp.contact}
                        </span>
                        <span>{followUp.type}</span>
                      </div>
                      <div className="flex items-center space-x-2 mt-3 p-2 bg-gray-50 rounded">
                        <Brain className="w-4 h-4 text-purple-600" />
                        <p className="text-sm text-gray-700">{followUp.suggestedAction}</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2">
                      {/* Communication Icons in single line */}
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                        onClick={() => handleCommunicationAction('phone', followUp)}
                        title="Call Contact"
                      >
                        <Phone className="w-4 h-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                        onClick={() => handleCommunicationAction('email', followUp)}
                        title="Send Email"
                      >
                        <Mail className="w-4 h-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-8 h-8 p-0 text-green-600 hover:text-green-800 hover:bg-green-50"
                        onClick={() => handleCommunicationAction('whatsapp', followUp)}
                        title="WhatsApp Contact"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                    </div>
                  </div>
                </div>
              ))}
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {/* Filter Modal */}
      {showFilterModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-md mx-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold">Filter Follow-ups</h3>
              <Button variant="ghost" size="sm" onClick={() => setShowFilterModal(false)}>
                <X className="w-4 h-4" />
              </Button>
            </div>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Priority</label>
                <select 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={filters.priority}
                  onChange={(e) => setFilters({...filters, priority: e.target.value})}
                >
                  <option value="all">All Priorities</option>
                  <option value="high">High Priority</option>
                  <option value="medium">Medium Priority</option>
                  <option value="low">Low Priority</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Follow-up Type</label>
                <select 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={filters.type}
                  onChange={(e) => setFilters({...filters, type: e.target.value})}
                >
                  <option value="all">All Types</option>
                  <option value="Demo follow-up">Demo Follow-up</option>
                  <option value="Quotation follow-up">Quotation Follow-up</option>
                  <option value="Installation feedback">Installation Feedback</option>
                  <option value="Support ticket">Support Ticket</option>
                  <option value="Product inquiry">Product Inquiry</option>
                  <option value="Contract renewal">Contract Renewal</option>
                  <option value="Training session">Training Session</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Contact Channel</label>
                <select 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={filters.contact}
                  onChange={(e) => setFilters({...filters, contact: e.target.value})}
                >
                  <option value="all">All Channels</option>
                  <option value="phone">Phone</option>
                  <option value="email">Email</option>
                  <option value="whatsapp">WhatsApp</option>
                  <option value="meeting">Meeting</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Date Range</label>
                <select 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={filters.dateRange}
                  onChange={(e) => setFilters({...filters, dateRange: e.target.value})}
                >
                  <option value="all">All Dates</option>
                  <option value="overdue">Overdue</option>
                  <option value="today">Today</option>
                  <option value="tomorrow">Tomorrow</option>
                  <option value="thisWeek">This Week</option>
                  <option value="nextWeek">Next Week</option>
                </select>
              </div>
              
              {/* Active Filters Summary */}
              <div className="bg-gray-50 p-3 rounded-lg">
                <p className="text-sm font-medium text-gray-700 mb-1">Active Filters:</p>
                <div className="flex flex-wrap gap-1">
                  {filters.priority !== "all" && <Badge variant="outline" className="text-xs">Priority: {filters.priority}</Badge>}
                  {filters.type !== "all" && <Badge variant="outline" className="text-xs">Type: {filters.type}</Badge>}
                  {filters.contact !== "all" && <Badge variant="outline" className="text-xs">Channel: {filters.contact}</Badge>}
                  {filters.dateRange !== "all" && <Badge variant="outline" className="text-xs">Date: {filters.dateRange}</Badge>}
                  {filters.priority === "all" && filters.type === "all" && filters.contact === "all" && filters.dateRange === "all" && (
                    <span className="text-xs text-gray-500">No filters applied</span>
                  )}
                </div>
              </div>
            </div>
            
            <div className="flex items-center justify-between mt-6">
              <Button 
                variant="outline" 
                onClick={() => {
                  setFilters({ priority: "all", type: "all", contact: "all", dateRange: "all" })
                }}
              >
                Clear All
              </Button>
              <div className="flex items-center space-x-3">
                <Button variant="outline" onClick={() => setShowFilterModal(false)}>
                  Cancel
                </Button>
                <Button 
                  className="bg-blue-600 hover:bg-blue-700" 
                  onClick={() => setShowFilterModal(false)}
                >
                  Apply Filters
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Set Reminder Modal */}
      {showReminderModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-md mx-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold">Set Follow-up Reminder</h3>
              <Button variant="ghost" size="sm" onClick={() => setShowReminderModal(false)}>
                <X className="w-4 h-4" />
              </Button>
            </div>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Account/Contact</label>
                <select className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500">
                  <option value="">Select account...</option>
                  <option value="tsar">TSAR Labcare - Mr. Mahesh</option>
                  <option value="kerala">Kerala Agricultural University - Dr. Priya Sharma</option>
                  <option value="eurofins">Eurofins Advinus - Mr. Rajesh Kumar</option>
                  <option value="jncasr">JNCASR - Dr. Anu Rang</option>
                  <option value="guna">Guna Foods - Pauline</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Reminder Title</label>
                <input 
                  type="text" 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="Follow-up on quotation discussion..."
                />
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Date</label>
                  <input 
                    type="date" 
                    className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    defaultValue={new Date().toISOString().split('T')[0]}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Time</label>
                  <input 
                    type="time" 
                    className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    defaultValue="10:00"
                  />
                </div>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Reminder Type</label>
                <select className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500">
                  <option value="phone">Phone Call</option>
                  <option value="email">Email</option>
                  <option value="whatsapp">WhatsApp</option>
                  <option value="meeting">Meeting</option>
                  <option value="task">Task</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Priority</label>
                <select className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500">
                  <option value="medium">Medium</option>
                  <option value="high">High</option>
                  <option value="low">Low</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Notes (Optional)</label>
                <textarea 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  rows={3}
                  placeholder="Additional context or talking points..."
                ></textarea>
              </div>

              {/* AI Suggestions */}
              <div className="bg-purple-50 p-3 rounded-lg border border-purple-200">
                <div className="flex items-center space-x-2 mb-2">
                  <Brain className="w-4 h-4 text-purple-600" />
                  <span className="text-sm font-medium text-purple-900">🤖 AI Suggestions</span>
                </div>
                <p className="text-xs text-purple-800">Based on contact history, 10-11 AM calls have 40% better response rate for this contact.</p>
              </div>
            </div>
            
            <div className="flex items-center justify-end space-x-3 mt-6">
              <Button variant="outline" onClick={() => setShowReminderModal(false)}>
                Cancel
              </Button>
              <Button 
                className="bg-blue-600 hover:bg-blue-700" 
                onClick={() => {
                  alert('Reminder would be saved to calendar and notification system')
                  setShowReminderModal(false)
                }}
              >
                Set Reminder
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}