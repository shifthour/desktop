"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Checkbox } from "@/components/ui/checkbox"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Search,
  Filter,
  Plus,
  ChevronDown,
  Calendar,
  User,
  Clock,
  MapPin,
  Brain,
  Zap,
  TrendingUp,
  Users,
  MessageSquare,
  Building2,
} from "lucide-react"
import { AIActivityAssistant } from "./ai-activity-assistant"

// Mock meetings data
const meetings = [
  {
    id: 1,
    title: "Demo",
    type: "Product Demo",
    from: "18/08/2025 07:15 PM",
    to: "18/08/2025 08:15 PM",
    relatedTo: "Printing Dimensions",
    contact: "Donette Foller",
    location: "Conference Room A",
    status: "Scheduled",
    aiInsight: "Optimal time slot based on participant availability"
  },
  {
    id: 2,
    title: "Webinar",
    type: "Training",
    from: "18/08/2025 09:15 PM",
    to: "18/08/2025 10:15 PM",
    relatedTo: "Commercial Press (Sample)",
    contact: "Leota Dilliard",
    location: "Virtual - Zoom",
    status: "Scheduled",
    aiInsight: "High engagement expected - similar webinars had 89% attendance"
  },
  {
    id: 3,
    title: "TradeShow",
    type: "Event",
    from: "18/08/2025",
    to: "19/08/2025",
    relatedTo: "Chemel",
    contact: "James Venere",
    location: "Convention Center",
    status: "Confirmed",
    aiInsight: "Prime networking opportunity - 15 key prospects confirmed"
  },
  {
    id: 4,
    title: "Webinar",
    type: "Product Training",
    from: "18/08/2025 08:15 PM",
    to: "18/08/2025 11:15 PM",
    relatedTo: "Chanay (Sample)",
    contact: "Josephine Darakjy",
    location: "Virtual - Teams",
    status: "Scheduled",
    aiInsight: "Consider shorter duration - 3hr sessions show 40% drop-off"
  },
  {
    id: 5,
    title: "Seminar",
    type: "Educational",
    from: "18/08/2025 07:15 PM",
    to: "18/08/2025 09:15 PM",
    relatedTo: "Carissa Kidman (Sample)",
    contact: "Josephine Darakjy",
    location: "Main Auditorium",
    status: "Scheduled",
    aiInsight: "Popular topic - expect high attendance based on registration data"
  },
  {
    id: 6,
    title: "Attend Customer conference",
    type: "Conference",
    from: "18/08/2025",
    to: "18/08/2025",
    relatedTo: "Feltz Printing Service",
    contact: "Capla Paprocki",
    location: "Customer Office",
    status: "Pending",
    aiInsight: "Key customer relationship - strategic importance high"
  },
  {
    id: 7,
    title: "CRM Webinar",
    type: "Training",
    from: "18/08/2025 06:15 PM",
    to: "18/08/2025 08:15 PM",
    relatedTo: "Morlong Associates",
    contact: "Mitsue Tollner",
    location: "Virtual - Webex",
    status: "Scheduled",
    aiInsight: "Follow-up recommended within 24 hours for max conversion"
  },
  {
    id: 8,
    title: "CRM Webinar",
    type: "Training",
    from: "18/08/2025 05:15 PM",
    to: "18/08/2025 06:15 PM",
    relatedTo: "Felix Hirpara (Sample)",
    contact: "Felix Hirpara",
    location: "Virtual - Zoom",
    status: "Scheduled",
    aiInsight: "Reschedule recommended - conflicts with similar session"
  },
  {
    id: 9,
    title: "CRM Webinar",
    type: "Training",
    from: "18/08/2025 05:15 PM",
    to: "18/08/2025 06:15 PM",
    relatedTo: "Benton",
    contact: "John Butt",
    location: "Virtual - Teams",
    status: "Scheduled",
    aiInsight: "High-value prospect - personalize presentation approach"
  }
]

const filters = [
  { name: "Touched Records", checked: false },
  { name: "Untouched Records", checked: false },
  { name: "Record Action", checked: false },
  { name: "Related Records Action", checked: false }
]

const fieldFilters = [
  { name: "All day", checked: false },
  { name: "Check-In Address", checked: false },
  { name: "Check-In By", checked: false },
  { name: "Check-In City", checked: false },
  { name: "Check-In Country", checked: false },
  { name: "Check-In State", checked: false },
  { name: "Check-In Sub-Locality", checked: false },
  { name: "Check-In Time", checked: false },
  { name: "Checked In Status", checked: false },
  { name: "Contact Name", checked: false },
  { name: "Created By", checked: false },
  { name: "Created Time", checked: false },
  { name: "From", checked: false }
]

export function MeetingsContent() {
  const [searchQuery, setSearchQuery] = useState("")
  const [selectedView, setSelectedView] = useState("All Meetings")
  const [showAIInsights, setShowAIInsights] = useState(true)

  return (
    <>
    <div className="p-6 space-y-6">
      {/* Header Section */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <h1 className="text-2xl font-bold text-gray-900">Meetings</h1>
          <Select value={selectedView} onValueChange={setSelectedView}>
            <SelectTrigger className="w-40">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="All Meetings">All Meetings</SelectItem>
              <SelectItem value="My Meetings">My Meetings</SelectItem>
              <SelectItem value="Today's Meetings">Today's Meetings</SelectItem>
              <SelectItem value="Upcoming Meetings">Upcoming Meetings</SelectItem>
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
            Create Meeting
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

      {/* AI Meeting Insights Panel */}
      {showAIInsights && (
        <Card className="border-blue-200 bg-gradient-to-r from-blue-50 to-indigo-50">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <Brain className="w-5 h-5 text-blue-600" />
                <CardTitle className="text-lg text-blue-900">AI Meeting Intelligence</CardTitle>
              </div>
              <Button variant="ghost" size="sm" onClick={() => setShowAIInsights(false)}>
                ×
              </Button>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <Calendar className="w-5 h-5 text-blue-600" />
                <div>
                  <p className="font-medium text-sm">Today's Meetings</p>
                  <p className="text-xs text-gray-600">9 meetings scheduled</p>
                </div>
              </div>
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <TrendingUp className="w-5 h-5 text-green-600" />
                <div>
                  <p className="font-medium text-sm">Attendance Rate</p>
                  <p className="text-xs text-gray-600">94% this month</p>
                </div>
              </div>
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <Zap className="w-5 h-5 text-purple-600" />
                <div>
                  <p className="font-medium text-sm">AI Optimizations</p>
                  <p className="text-xs text-gray-600">3 scheduling improvements</p>
                </div>
              </div>
            </div>
            <div className="flex items-start space-x-3 p-3 bg-green-50 rounded-lg border border-green-200">
              <MessageSquare className="w-4 h-4 text-green-600 mt-0.5" />
              <div>
                <p className="font-medium text-sm text-green-800">Smart Scheduling</p>
                <p className="text-xs text-green-700">AI detected 3 potential scheduling conflicts and suggests optimal time slots for better attendance.</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      <div className="flex gap-6">
        {/* Filters Sidebar */}
        <div className="w-72 space-y-6">
          {/* Search */}
          <div className="space-y-3">
            <p className="text-sm font-medium">Filter Meetings by</p>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input 
                placeholder="Search meetings..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10"
              />
            </div>
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

        {/* Meetings Table */}
        <div className="flex-1">
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <p className="text-sm text-gray-600">Total Records: <span className="font-medium">9</span></p>
                </div>
                <div className="flex items-center space-x-2">
                  <span className="text-sm text-gray-600">10 Records Per Page</span>
                  <span className="text-sm text-gray-600">1 - 9</span>
                  <div className="flex space-x-1">
                    <Button variant="outline" size="sm">‹</Button>
                    <Button variant="outline" size="sm">›</Button>
                  </div>
                </div>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-12">
                        <Checkbox />
                      </TableHead>
                      <TableHead>Title</TableHead>
                      <TableHead>From</TableHead>
                      <TableHead>To</TableHead>
                      <TableHead>Related To</TableHead>
                      <TableHead>Contact</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>AI Insights</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {meetings.map((meeting) => (
                      <TableRow key={meeting.id} className="hover:bg-gray-50">
                        <TableCell>
                          <Checkbox />
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <p className="font-medium text-sm">{meeting.title}</p>
                            <Badge variant="outline" className="text-xs">
                              {meeting.type}
                            </Badge>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <Calendar className="w-4 h-4 text-gray-400" />
                            <span className="text-sm">{meeting.from}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <Clock className="w-4 h-4 text-gray-400" />
                            <span className="text-sm">{meeting.to}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <Building2 className="w-4 h-4 text-gray-400" />
                            <span className="text-sm">{meeting.relatedTo}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <User className="w-4 h-4 text-gray-400" />
                            <span className="text-sm">{meeting.contact}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge 
                            variant={meeting.status === "Confirmed" ? "default" : 
                                   meeting.status === "Pending" ? "secondary" : "outline"}
                            className="text-xs"
                          >
                            {meeting.status}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-start space-x-2 max-w-xs">
                            <Brain className="w-4 h-4 text-purple-600 mt-0.5 flex-shrink-0" />
                            <p className="text-xs text-purple-700">{meeting.aiInsight}</p>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
    <AIActivityAssistant />
    </>
  )
}