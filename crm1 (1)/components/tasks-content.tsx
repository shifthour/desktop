"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Checkbox } from "@/components/ui/checkbox"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Separator } from "@/components/ui/separator"
import {
  Search,
  Filter,
  Plus,
  ChevronDown,
  Calendar,
  User,
  Clock,
  Priority,
  Zap,
  Brain,
  MessageSquare,
  TrendingUp,
} from "lucide-react"
import { AIActivityAssistant } from "./ai-activity-assistant"

// Mock task data
const tasks = [
  {
    id: 1,
    title: "Register for upcoming CRM Webinar",
    description: "Complete registration for the CRM training webinar scheduled for next week",
    status: "Not Started",
    priority: "Low",
    assignee: "vemularanjithkumar.cse",
    contact: "Kris Marrier (Sample)",
    company: "King (Sample)",
    dueDate: "18/08/2025",
    tags: ["Training", "CRM"],
    aiSuggestion: "Consider scheduling this during low-activity hours for maximum attendance"
  },
  {
    id: 2,
    title: "Competitor Comparison Document",
    description: "Prepare detailed analysis of competitor products and pricing strategies",
    status: "Not Started",
    priority: "Highest",
    assignee: "vemularanjithkumar.cse",
    contact: "Capla Paprocki (Sample)",
    company: "Feltz Printing Service",
    dueDate: "16/08/2025",
    tags: ["Analysis", "Competition"],
    aiSuggestion: "High priority - competitor analysis shows market opportunity"
  },
  {
    id: 3,
    title: "Get Approval from Manager",
    description: "Obtain manager approval for the new marketing campaign budget",
    status: "Not Started",
    priority: "Low", 
    assignee: "vemularanjithkumar.cse",
    contact: "Simon Morasca (Sample)",
    company: "Chapman",
    dueDate: "17/08/2025",
    tags: ["Approval", "Budget"],
    aiSuggestion: "Schedule a brief meeting to expedite approval process"
  },
  {
    id: 4,
    title: "Refer CRM Videos",
    description: "Review CRM training videos and create summary notes for team",
    status: "In Progress",
    priority: "Normal",
    assignee: "vemularanjithkumar.cse",
    contact: "Mitsue Tollner (Sample)",
    company: "Morlong Associates",
    dueDate: "20/08/2025",
    tags: ["Training", "Documentation"],
    aiSuggestion: "Break into smaller chunks for better retention"
  },
  {
    id: 5,
    title: "Get Approval from Manager",
    description: "Budget approval for Q4 marketing initiatives",
    status: "In Progress",
    priority: "High",
    assignee: "vemularanjithkumar.cse",
    contact: "Kris Marrier (Sample)",
    company: "King (Sample)",
    dueDate: "19/08/2025",
    tags: ["Approval", "Q4"],
    aiSuggestion: "Follow up required - high priority item pending"
  },
  {
    id: 6,
    title: "Register for upcoming CRM Webinar",
    description: "Additional webinar registration for advanced features",
    status: "In Progress",
    priority: "Normal",
    assignee: "vemularanjithkumar.cse",
    contact: "Michael Ruta (Sample)",
    dueDate: "21/08/2025",
    tags: ["Training", "Advanced"],
    aiSuggestion: "Coordinate with team calendar for optimal scheduling"
  }
]

const statusColumns = [
  { name: "Not Started", color: "bg-blue-100 text-blue-800", count: 3 },
  { name: "Deferred", color: "bg-yellow-100 text-yellow-800", count: 0 },
  { name: "In Progress", color: "bg-green-100 text-green-800", count: 3 }
]

const filters = [
  { name: "Touched Records", checked: false },
  { name: "Untouched Records", checked: false },
  { name: "Record Action", checked: false },
  { name: "Related Records Action", checked: false },
  { name: "Locked", checked: false }
]

const fieldFilters = [
  { name: "Closed Time", checked: false },
  { name: "Contact Name", checked: false },
  { name: "Created By", checked: false },
  { name: "Created Time", checked: false },
  { name: "Due Date", checked: false },
  { name: "Last Activity Time", checked: false },
  { name: "Modified By", checked: false },
  { name: "Modified Time", checked: false },
  { name: "Priority", checked: false },
  { name: "Related To", checked: false },
  { name: "Status", checked: false },
  { name: "Subject", checked: false }
]

export function TasksContent() {
  const [searchQuery, setSearchQuery] = useState("")
  const [selectedView, setSelectedView] = useState("Tasks by Status")
  const [showAIInsights, setShowAIInsights] = useState(true)

  return (
    <>
    <div className="p-6 space-y-6">
      {/* Header Section */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <h1 className="text-2xl font-bold text-gray-900">Tasks</h1>
          <Select defaultValue="All Tasks">
            <SelectTrigger className="w-40">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="All Tasks">All Tasks</SelectItem>
              <SelectItem value="My Tasks">My Tasks</SelectItem>
              <SelectItem value="Team Tasks">Team Tasks</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="flex items-center space-x-3">
          <Button variant="outline" size="sm">
            <Filter className="w-4 h-4 mr-2" />
            Filter
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Create Task
          </Button>
          <Select defaultValue="Actions">
            <SelectTrigger className="w-32">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="Actions">Actions</SelectItem>
              <SelectItem value="Export">Export</SelectItem>
              <SelectItem value="Import">Import</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* AI Insights Panel */}
      {showAIInsights && (
        <Card className="border-purple-200 bg-gradient-to-r from-purple-50 to-blue-50">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <Brain className="w-5 h-5 text-purple-600" />
                <CardTitle className="text-lg text-purple-900">AI Task Insights</CardTitle>
              </div>
              <Button variant="ghost" size="sm" onClick={() => setShowAIInsights(false)}>
                ×
              </Button>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <TrendingUp className="w-5 h-5 text-green-600" />
                <div>
                  <p className="font-medium text-sm">Productivity Trend</p>
                  <p className="text-xs text-gray-600">23% increase this week</p>
                </div>
              </div>
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <Clock className="w-5 h-5 text-orange-600" />
                <div>
                  <p className="font-medium text-sm">Overdue Tasks</p>
                  <p className="text-xs text-gray-600">2 tasks need attention</p>
                </div>
              </div>
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <Zap className="w-5 h-5 text-blue-600" />
                <div>
                  <p className="font-medium text-sm">AI Suggestions</p>
                  <p className="text-xs text-gray-600">4 optimization tips available</p>
                </div>
              </div>
            </div>
            <div className="flex items-start space-x-3 p-3 bg-yellow-50 rounded-lg border border-yellow-200">
              <MessageSquare className="w-4 h-4 text-yellow-600 mt-0.5" />
              <div>
                <p className="font-medium text-sm text-yellow-800">Smart Recommendation</p>
                <p className="text-xs text-yellow-700">Consider batching similar tasks like "Get Approval from Manager" for more efficient workflow.</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      <div className="flex gap-6">
        {/* Filters Sidebar */}
        <div className="w-72 space-y-6">
          {/* View Selector */}
          <div className="space-y-3">
            <Select value={selectedView} onValueChange={setSelectedView}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="Tasks by Status">Tasks by Status</SelectItem>
                <SelectItem value="Tasks by Priority">Tasks by Priority</SelectItem>
                <SelectItem value="Tasks by Assignee">Tasks by Assignee</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
            <Input 
              placeholder="Search tasks..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>

          {/* System Defined Filters */}
          <div>
            <div className="flex items-center space-x-2 mb-3">
              <ChevronDown className="w-4 h-4" />
              <h3 className="font-medium text-sm">System Defined Filters</h3>
            </div>
            <div className="space-y-2">
              {filters.map((filter, index) => (
                <div key={index} className="flex items-center space-x-2">
                  <Checkbox id={`filter-${index}`} />
                  <label htmlFor={`filter-${index}`} className="text-sm text-gray-700 cursor-pointer">
                    {filter.name}
                  </label>
                </div>
              ))}
            </div>
          </div>

          {/* Filter By Fields */}
          <div>
            <div className="flex items-center space-x-2 mb-3">
              <ChevronDown className="w-4 h-4" />
              <h3 className="font-medium text-sm">Filter By Fields</h3>
            </div>
            <ScrollArea className="h-48">
              <div className="space-y-2">
                {fieldFilters.map((filter, index) => (
                  <div key={index} className="flex items-center space-x-2">
                    <Checkbox id={`field-${index}`} />
                    <label htmlFor={`field-${index}`} className="text-sm text-gray-700 cursor-pointer">
                      {filter.name}
                    </label>
                  </div>
                ))}
              </div>
            </ScrollArea>
          </div>
        </div>

        {/* Tasks Kanban Board */}
        <div className="flex-1">
          <div className="grid grid-cols-3 gap-6">
            {statusColumns.map((column) => (
              <div key={column.name} className="space-y-4">
                {/* Column Header */}
                <div className="flex items-center justify-between p-4 bg-white rounded-lg border">
                  <div className="flex items-center space-x-2">
                    <h3 className="font-medium text-gray-900">{column.name}</h3>
                    <Badge variant="secondary" className={column.color}>
                      {column.count}
                    </Badge>
                  </div>
                </div>

                {/* Tasks in Column */}
                <div className="space-y-3">
                  {column.count === 0 ? (
                    <div className="p-8 text-center text-gray-500 bg-gray-50 rounded-lg border-2 border-dashed">
                      <p className="text-sm">No Tasks found.</p>
                    </div>
                  ) : (
                    tasks
                      .filter(task => task.status === column.name)
                      .map((task) => (
                        <Card key={task.id} className="hover:shadow-md transition-shadow cursor-pointer">
                          <CardContent className="p-4">
                            <div className="space-y-3">
                              <div className="flex items-start justify-between">
                                <h4 className="font-medium text-sm text-gray-900 leading-5">
                                  {task.title}
                                </h4>
                                <Badge 
                                  variant={task.priority === "Highest" ? "destructive" : 
                                          task.priority === "High" ? "destructive" :
                                          task.priority === "Normal" ? "default" : "secondary"}
                                  className="text-xs"
                                >
                                  {task.priority}
                                </Badge>
                              </div>
                              
                              <p className="text-xs text-gray-600 line-clamp-2">
                                {task.description}
                              </p>

                              <div className="flex items-center space-x-4 text-xs text-gray-500">
                                <div className="flex items-center space-x-1">
                                  <Calendar className="w-3 h-3" />
                                  <span>{task.dueDate}</span>
                                </div>
                                <div className="flex items-center space-x-1">
                                  <User className="w-3 h-3" />
                                  <span className="truncate max-w-20">{task.assignee}</span>
                                </div>
                              </div>

                              <div className="space-y-2">
                                <p className="text-xs text-gray-700">
                                  <span className="font-medium">{task.contact}</span>
                                </p>
                                <p className="text-xs text-gray-600">{task.company}</p>
                              </div>

                              {/* AI Suggestion */}
                              {task.aiSuggestion && (
                                <div className="p-2 bg-purple-50 rounded border border-purple-100">
                                  <div className="flex items-start space-x-2">
                                    <Brain className="w-3 h-3 text-purple-600 mt-0.5" />
                                    <p className="text-xs text-purple-700">
                                      {task.aiSuggestion}
                                    </p>
                                  </div>
                                </div>
                              )}

                              <div className="flex flex-wrap gap-1">
                                {task.tags.map((tag, tagIndex) => (
                                  <Badge key={tagIndex} variant="outline" className="text-xs px-2 py-1">
                                    {tag}
                                  </Badge>
                                ))}
                              </div>
                            </div>
                          </CardContent>
                        </Card>
                      ))
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
    <AIActivityAssistant />
    </>
  )
}