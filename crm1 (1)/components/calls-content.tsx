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
  Phone,
  PhoneCall,
  Clock,
  User,
  Brain,
  Zap,
  TrendingUp,
  MessageSquare,
  BarChart3,
  PhoneIncoming,
  PhoneOutgoing,
  PhoneMissed,
} from "lucide-react"
import { AIActivityAssistant } from "./ai-activity-assistant"

// Mock calls data
const calls = [
  {
    id: 1,
    subject: "Follow up with Lead",
    callType: "Outbound",
    startTime: "18/08/2025 05:15 PM",
    duration: "15 min",
    contact: "John Smith",
    company: "Tech Solutions Inc",
    status: "Completed",
    outcome: "Interested",
    aiInsight: "High conversion probability - showed strong interest in product demo",
    sentiment: "Positive"
  },
  {
    id: 2,
    subject: "Product Demonstration Call",
    callType: "Outbound",
    startTime: "18/08/2025 03:30 PM", 
    duration: "32 min",
    contact: "Sarah Johnson",
    company: "Marketing Pro Ltd",
    status: "Completed",
    outcome: "Scheduled Meeting",
    aiInsight: "Decision maker identified - recommend follow-up within 48 hours",
    sentiment: "Positive"
  },
  {
    id: 3,
    subject: "Customer Support Query",
    callType: "Inbound",
    startTime: "18/08/2025 02:15 PM",
    duration: "8 min",
    contact: "Mike Wilson",
    company: "Global Enterprises",
    status: "Completed",
    outcome: "Issue Resolved",
    aiInsight: "Customer satisfaction high - potential upsell opportunity detected",
    sentiment: "Neutral"
  },
  {
    id: 4,
    subject: "Quarterly Business Review",
    callType: "Scheduled",
    startTime: "18/08/2025 04:00 PM",
    duration: "45 min",
    contact: "Lisa Chen",
    company: "Innovation Hub",
    status: "Completed",
    outcome: "Contract Renewal",
    aiInsight: "Strong relationship confirmed - renewal likelihood 95%",
    sentiment: "Positive"
  },
  {
    id: 5,
    subject: "Cold Outreach Call",
    callType: "Outbound", 
    startTime: "18/08/2025 01:45 PM",
    duration: "12 min",
    contact: "Robert Brown",
    company: "StartupTech",
    status: "Completed",
    outcome: "No Interest",
    aiInsight: "Budget constraints identified - revisit in Q4",
    sentiment: "Negative"
  },
  {
    id: 6,
    subject: "Partnership Discussion",
    callType: "Inbound",
    startTime: "18/08/2025 11:30 AM",
    duration: "28 min",
    contact: "Emily Davis",
    company: "Strategic Partners LLC",
    status: "Completed",
    outcome: "Partnership Proposal",
    aiInsight: "High-value opportunity - joint venture potential identified",
    sentiment: "Positive"
  }
]

const filters = [
  { name: "Touched Records", checked: false },
  { name: "Untouched Records", checked: false },
  { name: "Record Action", checked: false },
  { name: "Related Records Action", checked: false }
]

const fieldFilters = [
  { name: "Call Duration", checked: false },
  { name: "Call Duration (in seconds)", checked: false },
  { name: "Call Owner", checked: false },
  { name: "Call Start Time", checked: false },
  { name: "Call Type", checked: false },
  { name: "Caller ID", checked: false },
  { name: "Contact Name", checked: false },
  { name: "Created By", checked: false },
  { name: "Created Time", checked: false },
  { name: "Dialled Number", checked: false },
  { name: "Last Activity Time", checked: false },
  { name: "Modified By", checked: false },
  { name: "Modified Time", checked: false }
]

export function CallsContent() {
  const [searchQuery, setSearchQuery] = useState("")
  const [selectedView, setSelectedView] = useState("All Calls")
  const [showAIInsights, setShowAIInsights] = useState(true)

  const getCallTypeIcon = (type: string) => {
    switch (type) {
      case "Inbound":
        return <PhoneIncoming className="w-4 h-4 text-green-600" />
      case "Outbound":
        return <PhoneOutgoing className="w-4 h-4 text-blue-600" />
      case "Scheduled":
        return <Phone className="w-4 h-4 text-purple-600" />
      default:
        return <PhoneMissed className="w-4 h-4 text-red-600" />
    }
  }

  const getSentimentColor = (sentiment: string) => {
    switch (sentiment) {
      case "Positive":
        return "text-green-600 bg-green-50"
      case "Negative":
        return "text-red-600 bg-red-50"
      default:
        return "text-gray-600 bg-gray-50"
    }
  }

  return (
    <>
    <div className="p-6 space-y-6">
      {/* Header Section */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <h1 className="text-2xl font-bold text-gray-900">Calls</h1>
          <Select value={selectedView} onValueChange={setSelectedView}>
            <SelectTrigger className="w-40">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="All Calls">All Calls</SelectItem>
              <SelectItem value="My Calls">My Calls</SelectItem>
              <SelectItem value="Today's Calls">Today's Calls</SelectItem>
              <SelectItem value="Missed Calls">Missed Calls</SelectItem>
              <SelectItem value="Completed Calls">Completed Calls</SelectItem>
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
            Create Call
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

      {/* AI Call Analytics Panel */}
      {showAIInsights && (
        <Card className="border-emerald-200 bg-gradient-to-r from-emerald-50 to-teal-50">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <Brain className="w-5 h-5 text-emerald-600" />
                <CardTitle className="text-lg text-emerald-900">AI Call Intelligence</CardTitle>
              </div>
              <Button variant="ghost" size="sm" onClick={() => setShowAIInsights(false)}>
                ×
              </Button>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <PhoneCall className="w-5 h-5 text-blue-600" />
                <div>
                  <p className="font-medium text-sm">Total Calls</p>
                  <p className="text-xs text-gray-600">6 calls today</p>
                </div>
              </div>
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <TrendingUp className="w-5 h-5 text-green-600" />
                <div>
                  <p className="font-medium text-sm">Success Rate</p>
                  <p className="text-xs text-gray-600">83% positive outcomes</p>
                </div>
              </div>
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <Clock className="w-5 h-5 text-orange-600" />
                <div>
                  <p className="font-medium text-sm">Avg Duration</p>
                  <p className="text-xs text-gray-600">23 minutes</p>
                </div>
              </div>
              <div className="flex items-center space-x-3 p-3 bg-white rounded-lg border">
                <BarChart3 className="w-5 h-5 text-purple-600" />
                <div>
                  <p className="font-medium text-sm">Conversion</p>
                  <p className="text-xs text-gray-600">67% lead to meeting</p>
                </div>
              </div>
            </div>
            <div className="flex items-start space-x-3 p-3 bg-blue-50 rounded-lg border border-blue-200">
              <MessageSquare className="w-4 h-4 text-blue-600 mt-0.5" />
              <div>
                <p className="font-medium text-sm text-blue-800">Smart Insights</p>
                <p className="text-xs text-blue-700">AI analysis shows optimal call times between 2-4 PM result in 40% higher engagement rates. Consider scheduling important calls during this window.</p>
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
            <p className="text-sm font-medium">Filter Calls by</p>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input 
                placeholder="Search calls..."
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

        {/* Calls Table */}
        <div className="flex-1">
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <p className="text-sm text-gray-600">Total Records: <span className="font-medium">6</span></p>
                </div>
                <div className="flex items-center space-x-2">
                  <span className="text-sm text-gray-600">10 Records Per Page</span>
                  <span className="text-sm text-gray-600">1 - 6</span>
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
                      <TableHead>Subject</TableHead>
                      <TableHead>Call Type</TableHead>
                      <TableHead>Call Start Time</TableHead>
                      <TableHead>Duration</TableHead>
                      <TableHead>Contact</TableHead>
                      <TableHead>Outcome</TableHead>
                      <TableHead>AI Analysis</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {calls.map((call) => (
                      <TableRow key={call.id} className="hover:bg-gray-50">
                        <TableCell>
                          <Checkbox />
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <p className="font-medium text-sm">{call.subject}</p>
                            <Badge className={`text-xs px-2 py-1 ${getSentimentColor(call.sentiment)}`}>
                              {call.sentiment}
                            </Badge>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            {getCallTypeIcon(call.callType)}
                            <span className="text-sm">{call.callType}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <Clock className="w-4 h-4 text-gray-400" />
                            <span className="text-sm">{call.startTime}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline" className="text-xs">
                            {call.duration}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="flex items-center space-x-2">
                              <User className="w-4 h-4 text-gray-400" />
                              <span className="text-sm font-medium">{call.contact}</span>
                            </div>
                            <p className="text-xs text-gray-600">{call.company}</p>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge 
                            variant={call.outcome === "Contract Renewal" || call.outcome === "Interested" || call.outcome === "Partnership Proposal" ? "default" : 
                                   call.outcome === "Scheduled Meeting" || call.outcome === "Issue Resolved" ? "secondary" : "destructive"}
                            className="text-xs"
                          >
                            {call.outcome}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-start space-x-2 max-w-xs">
                            <Brain className="w-4 h-4 text-emerald-600 mt-0.5 flex-shrink-0" />
                            <p className="text-xs text-emerald-700">{call.aiInsight}</p>
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