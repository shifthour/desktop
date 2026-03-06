"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Calendar, Clock, Users, Phone, Target, CheckCircle2, AlertCircle, Star, Brain, Sparkles, ArrowRight, Filter, Plus, X } from "lucide-react"

export function DailyHubContent() {
  const [selectedView, setSelectedView] = useState("all")
  const [showAddActivityModal, setShowAddActivityModal] = useState(false)
  const [showFilterModal, setShowFilterModal] = useState(false)
  const [filters, setFilters] = useState({
    priority: "all",
    status: "all",
    type: "all",
    dateRange: "today"
  })

  const todayActivities = [
    {
      id: 1,
      type: "meeting",
      title: "Product Demo with TSAR Labcare",
      time: "10:00 AM",
      duration: "45 min",
      priority: "high",
      aiScore: 92,
      participants: ["Mr. Rajesh Kumar", "Dr. Priya Sharma"],
      status: "upcoming",
      aiInsight: "High conversion probability. Prepare fibrinometer pricing options."
    },
    {
      id: 2,
      type: "call",
      title: "Follow-up: Kerala Agricultural University",
      time: "11:30 AM",
      duration: "20 min",
      priority: "medium",
      aiScore: 78,
      status: "upcoming",
      aiInsight: "Installation feedback positive. Discuss maintenance contract."
    },
    {
      id: 3,
      type: "task",
      title: "Submit quotation for Eurofins",
      time: "2:00 PM",
      priority: "high",
      aiScore: 88,
      status: "in-progress",
      aiInsight: "Deadline today. Competitor pricing intel available."
    },
    {
      id: 4,
      type: "meeting",
      title: "Team sync on Q3 targets",
      time: "3:30 PM",
      duration: "30 min",
      priority: "medium",
      aiScore: 65,
      participants: ["Sales Team"],
      status: "upcoming",
      aiInsight: "78% of Q3 target achieved. Focus on pending deals."
    },
    {
      id: 5,
      type: "task",
      title: "Review and approve 3 pending orders",
      time: "4:30 PM",
      priority: "low",
      aiScore: 45,
      status: "pending",
      aiInsight: "Standard orders. Can be delegated or batch processed."
    }
  ]

  const aiPriorities = [
    {
      title: "Close TSAR Labcare Deal",
      reason: "92% win probability, ₹8.5L value",
      action: "Prepare custom pricing",
      urgent: true
    },
    {
      title: "Contact 5 warm leads",
      reason: "AI detected high engagement signals",
      action: "Send personalized emails",
      urgent: false
    },
    {
      title: "Update CRM for yesterday's meetings",
      reason: "3 meetings pending documentation",
      action: "Quick update required",
      urgent: false
    }
  ]

  const getTypeIcon = (type: string) => {
    switch(type) {
      case 'meeting': return <Users className="w-4 h-4" />
      case 'call': return <Phone className="w-4 h-4" />
      case 'task': return <Target className="w-4 h-4" />
      default: return <CheckCircle2 className="w-4 h-4" />
    }
  }

  const getStatusBadge = (status: string) => {
    switch(status) {
      case 'upcoming': return <Badge className="bg-blue-100 text-blue-800">Upcoming</Badge>
      case 'in-progress': return <Badge className="bg-yellow-100 text-yellow-800">In Progress</Badge>
      case 'pending': return <Badge className="bg-gray-100 text-gray-800">Pending</Badge>
      case 'completed': return <Badge className="bg-green-100 text-green-800">Completed</Badge>
      default: return null
    }
  }

  const applyFilters = (activities: any[]) => {
    return activities.filter(activity => {
      if (filters.priority !== "all" && activity.priority !== filters.priority) return false
      if (filters.status !== "all" && activity.status !== filters.status) return false
      if (filters.type !== "all" && activity.type !== filters.type) return false
      // Date range filtering would be implemented here for real data
      return true
    })
  }

  const filteredActivities = applyFilters(todayActivities)

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Daily Hub</h1>
          <p className="text-gray-500 mt-1">Your unified activity command center for {new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}</p>
        </div>
        <div className="flex items-center space-x-3">
          <Button variant="outline" size="sm" onClick={() => setShowFilterModal(true)}>
            <Filter className="w-4 h-4 mr-2" />
            Filter
            {(filters.priority !== "all" || filters.status !== "all" || filters.type !== "all" || filters.dateRange !== "today") && (
              <Badge className="ml-2 bg-blue-500 text-white text-xs w-2 h-2 p-0 flex items-center justify-center">•</Badge>
            )}
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700" size="sm" onClick={() => setShowAddActivityModal(true)}>
            <Plus className="w-4 h-4 mr-2" />
            Add Activity
          </Button>
        </div>
      </div>

      {/* AI Priority Box */}
      <Card className="border-purple-200 bg-gradient-to-r from-purple-50 to-indigo-50">
        <CardHeader>
          <CardTitle className="flex items-center">
            <Brain className="w-5 h-5 mr-2 text-purple-600" />
            AI Priority Recommendations
          </CardTitle>
          <CardDescription>Your AI assistant suggests focusing on these activities today</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {aiPriorities.map((priority, index) => (
              <div key={index} className="flex items-center justify-between p-3 bg-white rounded-lg border border-purple-100">
                <div className="flex items-start space-x-3">
                  <Sparkles className={`w-5 h-5 ${priority.urgent ? 'text-red-500' : 'text-purple-500'} mt-0.5`} />
                  <div>
                    <p className="font-medium text-gray-900">{priority.title}</p>
                    <p className="text-sm text-gray-600">{priority.reason}</p>
                  </div>
                </div>
                <Button size="sm" variant={priority.urgent ? "default" : "outline"}>
                  {priority.action}
                </Button>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Stats Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Total Activities</p>
                <p className="text-2xl font-bold">12</p>
              </div>
              <Calendar className="w-8 h-8 text-blue-500" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Completed</p>
                <p className="text-2xl font-bold">5</p>
              </div>
              <CheckCircle2 className="w-8 h-8 text-green-500" />
            </div>
            <Progress value={42} className="mt-3" />
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">High Priority</p>
                <p className="text-2xl font-bold">3</p>
              </div>
              <AlertCircle className="w-8 h-8 text-red-500" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">AI Score</p>
                <p className="text-2xl font-bold">86%</p>
              </div>
              <Brain className="w-8 h-8 text-purple-500" />
            </div>
            <p className="text-xs text-gray-500 mt-2">Productivity index</p>
          </CardContent>
        </Card>
      </div>

      {/* Main Activity Timeline */}
      <Card>
        <CardHeader>
          <CardTitle>Today's Timeline</CardTitle>
          <CardDescription>All your activities in one unified view</CardDescription>
        </CardHeader>
        <CardContent>
          <Tabs defaultValue="all" className="w-full">
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="all">All Activities</TabsTrigger>
              <TabsTrigger value="meetings">Meetings</TabsTrigger>
              <TabsTrigger value="calls">Calls</TabsTrigger>
              <TabsTrigger value="tasks">Tasks</TabsTrigger>
            </TabsList>
            <TabsContent value="all" className="space-y-4 mt-4">
              {filteredActivities.length === 0 ? (
                <div className="text-center py-8 text-gray-500">
                  No activities match the current filters. Try adjusting your filter settings.
                </div>
              ) : (
                filteredActivities.map((activity) => (
                <div key={activity.id} className="flex items-start space-x-4 p-4 border rounded-lg hover:bg-gray-50 transition-colors">
                  <div className="flex-shrink-0 mt-1">
                    <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                      activity.type === 'meeting' ? 'bg-blue-100 text-blue-600' :
                      activity.type === 'call' ? 'bg-green-100 text-green-600' :
                      'bg-purple-100 text-purple-600'
                    }`}>
                      {getTypeIcon(activity.type)}
                    </div>
                  </div>
                  <div className="flex-1">
                    <div className="flex items-start justify-between">
                      <div>
                        <div className="flex items-center space-x-2">
                          <h3 className="font-medium text-gray-900">{activity.title}</h3>
                          {activity.priority === 'high' && <Badge variant="destructive" className="text-xs">High Priority</Badge>}
                        </div>
                        <div className="flex items-center space-x-4 mt-1 text-sm text-gray-500">
                          <span className="flex items-center">
                            <Clock className="w-3 h-3 mr-1" />
                            {activity.time}
                          </span>
                          {activity.duration && <span>{activity.duration}</span>}
                          {activity.participants && (
                            <span className="flex items-center">
                              <Users className="w-3 h-3 mr-1" />
                              {activity.participants.length} participants
                            </span>
                          )}
                        </div>
                        {activity.aiInsight && (
                          <div className="flex items-start space-x-2 mt-2 p-2 bg-purple-50 rounded-md">
                            <Brain className="w-4 h-4 text-purple-600 mt-0.5" />
                            <p className="text-sm text-purple-900">{activity.aiInsight}</p>
                          </div>
                        )}
                      </div>
                      <div className="flex flex-col items-end space-y-2">
                        {getStatusBadge(activity.status)}
                        <div className="flex items-center space-x-1">
                          <Star className="w-4 h-4 text-yellow-500 fill-current" />
                          <span className="text-sm font-medium">{activity.aiScore}%</span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
                ))
              )}
            </TabsContent>
            <TabsContent value="meetings">
              <div className="text-center py-8 text-gray-500">
                Meetings view - filtered content
              </div>
            </TabsContent>
            <TabsContent value="calls">
              <div className="text-center py-8 text-gray-500">
                Calls view - filtered content
              </div>
            </TabsContent>
            <TabsContent value="tasks">
              <div className="text-center py-8 text-gray-500">
                Tasks view - filtered content
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {/* Add Activity Modal */}
      {showAddActivityModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-md mx-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold">Add New Activity</h3>
              <Button variant="ghost" size="sm" onClick={() => setShowAddActivityModal(false)}>
                <X className="w-4 h-4" />
              </Button>
            </div>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Activity Type</label>
                <select className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500">
                  <option value="meeting">Meeting</option>
                  <option value="call">Call</option>
                  <option value="task">Task</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Title</label>
                <input 
                  type="text" 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="Enter activity title..."
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
                  />
                </div>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Priority</label>
                <select className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500">
                  <option value="low">Low</option>
                  <option value="medium">Medium</option>
                  <option value="high">High</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Description (Optional)</label>
                <textarea 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  rows={3}
                  placeholder="Additional details..."
                ></textarea>
              </div>
            </div>
            
            <div className="flex items-center justify-end space-x-3 mt-6">
              <Button variant="outline" onClick={() => setShowAddActivityModal(false)}>
                Cancel
              </Button>
              <Button 
                className="bg-blue-600 hover:bg-blue-700" 
                onClick={() => {
                  // Here you would typically save the activity
                  alert('Activity would be saved to database')
                  setShowAddActivityModal(false)
                }}
              >
                Add Activity
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Filter Modal */}
      {showFilterModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 w-full max-w-md mx-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold">Filter Activities</h3>
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Status</label>
                <select 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={filters.status}
                  onChange={(e) => setFilters({...filters, status: e.target.value})}
                >
                  <option value="all">All Status</option>
                  <option value="upcoming">Upcoming</option>
                  <option value="in-progress">In Progress</option>
                  <option value="pending">Pending</option>
                  <option value="completed">Completed</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Activity Type</label>
                <select 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={filters.type}
                  onChange={(e) => setFilters({...filters, type: e.target.value})}
                >
                  <option value="all">All Types</option>
                  <option value="meeting">Meetings</option>
                  <option value="call">Calls</option>
                  <option value="task">Tasks</option>
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Date Range</label>
                <select 
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={filters.dateRange}
                  onChange={(e) => setFilters({...filters, dateRange: e.target.value})}
                >
                  <option value="today">Today</option>
                  <option value="tomorrow">Tomorrow</option>
                  <option value="thisWeek">This Week</option>
                  <option value="nextWeek">Next Week</option>
                  <option value="thisMonth">This Month</option>
                </select>
              </div>
              
              {/* Active Filters Summary */}
              <div className="bg-gray-50 p-3 rounded-lg">
                <p className="text-sm font-medium text-gray-700 mb-1">Active Filters:</p>
                <div className="flex flex-wrap gap-1">
                  {filters.priority !== "all" && <Badge variant="outline" className="text-xs">Priority: {filters.priority}</Badge>}
                  {filters.status !== "all" && <Badge variant="outline" className="text-xs">Status: {filters.status}</Badge>}
                  {filters.type !== "all" && <Badge variant="outline" className="text-xs">Type: {filters.type}</Badge>}
                  {filters.dateRange !== "today" && <Badge variant="outline" className="text-xs">Date: {filters.dateRange}</Badge>}
                  {filters.priority === "all" && filters.status === "all" && filters.type === "all" && filters.dateRange === "today" && (
                    <span className="text-xs text-gray-500">No filters applied</span>
                  )}
                </div>
              </div>
            </div>
            
            <div className="flex items-center justify-between mt-6">
              <Button 
                variant="outline" 
                onClick={() => {
                  setFilters({ priority: "all", status: "all", type: "all", dateRange: "today" })
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
    </div>
  )
}