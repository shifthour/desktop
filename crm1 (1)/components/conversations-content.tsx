"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { StatusIndicator } from "@/components/ui/status-indicator"
import {
  MessageSquare,
  Phone,
  Mail,
  Video,
  Search,
  Filter,
  Plus,
  Clock,
  Users,
  TrendingUp,
  AlertCircle,
  Send,
  Paperclip,
  Smile,
} from "lucide-react"

const conversations = [
  {
    id: "CONV-001",
    customer: "TSAR Labcare",
    contact: "Mr. Mahesh",
    lastMessage: "When can we schedule the installation?",
    timestamp: "2 minutes ago",
    unreadCount: 2,
    channel: "email",
    status: "active",
    priority: "high",
    assignedTo: "Pauline",
    tags: ["Installation", "Urgent"],
  },
  {
    id: "CONV-002",
    customer: "Eurofins Advinus",
    contact: "Dr. Research Head",
    lastMessage: "Please send the technical specifications",
    timestamp: "1 hour ago",
    unreadCount: 0,
    channel: "chat",
    status: "waiting",
    priority: "medium",
    assignedTo: "Hari Kumar K",
    tags: ["Technical", "Specifications"],
  },
  {
    id: "CONV-003",
    customer: "Kerala Agricultural University",
    contact: "Dr. Department Head",
    lastMessage: "Thank you for the quick resolution",
    timestamp: "3 hours ago",
    unreadCount: 0,
    channel: "phone",
    status: "resolved",
    priority: "low",
    assignedTo: "Arvind K",
    tags: ["Support", "Resolved"],
  },
]

const stats = [
  {
    title: "Active Conversations",
    value: "47",
    change: { value: "+8", type: "positive" as const, period: "today" },
    icon: MessageSquare,
  },
  {
    title: "Response Time",
    value: "2.4h",
    change: { value: "-0.5h", type: "positive" as const, period: "this week" },
    icon: Clock,
  },
  {
    title: "Resolution Rate",
    value: "94%",
    change: { value: "+3%", type: "positive" as const, period: "this month" },
    icon: TrendingUp,
  },
  {
    title: "Customer Satisfaction",
    value: "4.8",
    change: { value: "+0.2", type: "positive" as const, period: "this month" },
    icon: Users,
  },
]

const aiInsights = [
  {
    type: "opportunity",
    message: "TSAR Labcare mentioned budget approval - potential upsell opportunity",
    confidence: 85,
    action: "Schedule follow-up call",
  },
  {
    type: "risk",
    message: "Eurofins Advinus hasn't responded to quote in 5 days - risk of losing deal",
    confidence: 72,
    action: "Send follow-up email",
  },
  {
    type: "satisfaction",
    message: "Kerala Agricultural University expressed high satisfaction - request testimonial",
    confidence: 91,
    action: "Request review",
  },
]

export function ConversationsContent() {
  const [selectedConversation, setSelectedConversation] = useState(conversations[0])
  const [messageText, setMessageText] = useState("")

  const getChannelIcon = (channel: string) => {
    switch (channel) {
      case "email":
        return Mail
      case "chat":
        return MessageSquare
      case "phone":
        return Phone
      case "video":
        return Video
      default:
        return MessageSquare
    }
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case "active":
        return "success"
      case "waiting":
        return "warning"
      case "resolved":
        return "default"
      default:
        return "default"
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case "high":
        return "error"
      case "medium":
        return "warning"
      case "low":
        return "success"
      default:
        return "default"
    }
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Customer Conversations</h1>
          <p className="text-gray-600">Unified communication hub for all customer interactions</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Filter className="w-4 h-4 mr-2" />
            Filter
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            New Conversation
          </Button>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stats.map((stat) => (
          <EnhancedCard key={stat.title} title={stat.title} value={stat.value} change={stat.change} icon={stat.icon} />
        ))}
      </div>

      {/* AI Insights */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <AlertCircle className="w-5 h-5 mr-2" />
            AI-Powered Insights
          </CardTitle>
          <CardDescription>Smart recommendations based on conversation analysis</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {aiInsights.map((insight, index) => (
              <div
                key={index}
                className="flex items-center justify-between p-3 border rounded-lg bg-gradient-to-r from-blue-50 to-indigo-50"
              >
                <div className="flex-1">
                  <p className="text-sm font-medium">{insight.message}</p>
                  <div className="flex items-center space-x-2 mt-1">
                    <Badge variant="outline" className="text-xs">
                      {insight.confidence}% confidence
                    </Badge>
                    <Badge variant="outline" className="text-xs capitalize">
                      {insight.type}
                    </Badge>
                  </div>
                </div>
                <Button size="sm" variant="outline">
                  {insight.action}
                </Button>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Conversation Interface */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Conversation List */}
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle>Conversations</CardTitle>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
              <Input placeholder="Search conversations..." className="pl-10" />
            </div>
          </CardHeader>
          <CardContent className="p-0">
            <div className="space-y-1">
              {conversations.map((conversation) => {
                const ChannelIcon = getChannelIcon(conversation.channel)
                return (
                  <div
                    key={conversation.id}
                    className={`p-4 border-b cursor-pointer hover:bg-gray-50 ${
                      selectedConversation.id === conversation.id ? "bg-blue-50 border-l-4 border-l-blue-500" : ""
                    }`}
                    onClick={() => setSelectedConversation(conversation)}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center space-x-2">
                        <ChannelIcon className="w-4 h-4 text-gray-400" />
                        <span className="font-medium text-sm">{conversation.customer}</span>
                        {conversation.unreadCount > 0 && (
                          <Badge className="bg-red-500 text-white text-xs">{conversation.unreadCount}</Badge>
                        )}
                      </div>
                      <span className="text-xs text-gray-500">{conversation.timestamp}</span>
                    </div>
                    <p className="text-sm text-gray-600 truncate">{conversation.lastMessage}</p>
                    <div className="flex items-center justify-between mt-2">
                      <div className="flex items-center space-x-2">
                        <StatusIndicator
                          status={conversation.priority}
                          variant={getPriorityColor(conversation.priority)}
                          size="sm"
                        />
                        <StatusIndicator
                          status={conversation.status}
                          variant={getStatusColor(conversation.status)}
                          size="sm"
                        />
                      </div>
                      <span className="text-xs text-gray-500">{conversation.assignedTo}</span>
                    </div>
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>

        {/* Conversation Detail */}
        <Card className="lg:col-span-2">
          <CardHeader className="border-b">
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>{selectedConversation.customer}</CardTitle>
                <CardDescription>
                  {selectedConversation.contact} • {selectedConversation.channel}
                </CardDescription>
              </div>
              <div className="flex items-center space-x-2">
                <Button variant="outline" size="sm">
                  <Phone className="w-4 h-4" />
                </Button>
                <Button variant="outline" size="sm">
                  <Video className="w-4 h-4" />
                </Button>
                <Button variant="outline" size="sm">
                  <Mail className="w-4 h-4" />
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent className="p-0">
            {/* Message History */}
            <div className="h-96 overflow-y-auto p-4 space-y-4">
              <div className="flex items-start space-x-3">
                <Avatar className="w-8 h-8">
                  <AvatarFallback>MH</AvatarFallback>
                </Avatar>
                <div className="flex-1">
                  <div className="bg-gray-100 rounded-lg p-3">
                    <p className="text-sm">
                      Hi, we're interested in the TRICOLOR MULTICHANNEL FIBRINOMETER. Can you provide more details about
                      installation requirements?
                    </p>
                  </div>
                  <span className="text-xs text-gray-500 mt-1">2 hours ago</span>
                </div>
              </div>

              <div className="flex items-start space-x-3 justify-end">
                <div className="flex-1 text-right">
                  <div className="bg-blue-500 text-white rounded-lg p-3 inline-block">
                    <p className="text-sm">
                      Thank you for your interest! I'll send you the detailed installation guide and technical
                      specifications. Our team can also schedule a site visit to assess your requirements.
                    </p>
                  </div>
                  <span className="text-xs text-gray-500 mt-1 block">1 hour ago</span>
                </div>
                <Avatar className="w-8 h-8">
                  <AvatarFallback>PS</AvatarFallback>
                </Avatar>
              </div>

              <div className="flex items-start space-x-3">
                <Avatar className="w-8 h-8">
                  <AvatarFallback>MH</AvatarFallback>
                </Avatar>
                <div className="flex-1">
                  <div className="bg-gray-100 rounded-lg p-3">
                    <p className="text-sm">That would be great! When can we schedule the installation?</p>
                  </div>
                  <span className="text-xs text-gray-500 mt-1">2 minutes ago</span>
                </div>
              </div>
            </div>

            {/* Message Input */}
            <div className="border-t p-4">
              <div className="flex items-center space-x-2">
                <Button variant="ghost" size="sm">
                  <Paperclip className="w-4 h-4" />
                </Button>
                <Button variant="ghost" size="sm">
                  <Smile className="w-4 h-4" />
                </Button>
                <Input
                  placeholder="Type your message..."
                  value={messageText}
                  onChange={(e) => setMessageText(e.target.value)}
                  className="flex-1"
                />
                <Button className="bg-blue-600 hover:bg-blue-700">
                  <Send className="w-4 h-4" />
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
