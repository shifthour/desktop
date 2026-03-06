"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Brain,
  Zap,
  Calendar,
  Phone,
  CheckSquare,
  TrendingUp,
  MessageSquare,
  Clock,
  Users,
  Target,
  Lightbulb,
  Workflow,
  BarChart3,
  AlertTriangle,
  Sparkles,
  Bot,
  Mic,
  FileText,
  Send,
  Settings,
} from "lucide-react"

interface AIRecommendation {
  id: string
  type: 'task' | 'meeting' | 'call' | 'general'
  priority: 'high' | 'medium' | 'low'
  title: string
  description: string
  action: string
  confidence: number
  impact: 'high' | 'medium' | 'low'
  category: string
}

interface AIInsight {
  id: string
  type: string
  title: string
  value: string
  trend: 'up' | 'down' | 'stable'
  description: string
}

const aiRecommendations: AIRecommendation[] = [
  {
    id: '1',
    type: 'task',
    priority: 'high',
    title: 'Optimize Task Assignment',
    description: 'AI detected that vemularanjithkumar.cse is overloaded with 15 active tasks. Redistribute 3 low-priority tasks to improve productivity.',
    action: 'Redistribute tasks',
    confidence: 92,
    impact: 'high',
    category: 'Workload Balance'
  },
  {
    id: '2',
    type: 'meeting',
    priority: 'medium', 
    title: 'Schedule Follow-up Meetings',
    description: '3 successful demos this week have no follow-up scheduled. Auto-schedule follow-up meetings within 48 hours for optimal conversion.',
    action: 'Auto-schedule meetings',
    confidence: 87,
    impact: 'high',
    category: 'Lead Nurturing'
  },
  {
    id: '3',
    type: 'call',
    priority: 'high',
    title: 'Improve Call Success Rate',
    description: 'Analysis shows calls between 2-4 PM have 40% higher success rate. Reschedule 4 pending calls to optimal time slots.',
    action: 'Optimize call timing',
    confidence: 94,
    impact: 'medium',
    category: 'Call Optimization'
  },
  {
    id: '4',
    type: 'general',
    priority: 'medium',
    title: 'Activity Pattern Analysis',
    description: 'Your team completes 23% more tasks on Tuesdays and Wednesdays. Consider scheduling important activities on these days.',
    action: 'Adjust scheduling',
    confidence: 78,
    impact: 'medium',
    category: 'Productivity'
  }
]

const aiInsights: AIInsight[] = [
  {
    id: '1',
    type: 'productivity',
    title: 'Task Completion Rate',
    value: '94%',
    trend: 'up',
    description: '8% increase from last week'
  },
  {
    id: '2',
    type: 'meetings',
    title: 'Meeting Success Rate',
    value: '87%',
    trend: 'up',
    description: 'Meetings leading to next actions'
  },
  {
    id: '3',
    type: 'calls',
    title: 'Call Conversion',
    value: '67%',
    trend: 'stable',
    description: 'Calls resulting in meetings/deals'
  },
  {
    id: '4',
    type: 'efficiency',
    title: 'Response Time',
    value: '2.3h',
    trend: 'down',
    description: 'Average response to inquiries'
  }
]

export function AIActivityAssistant() {
  const [isOpen, setIsOpen] = useState(false)
  const [activeTab, setActiveTab] = useState('recommendations')
  const [chatMessage, setChatMessage] = useState('')
  const [chatHistory, setChatHistory] = useState([
    {
      type: 'ai',
      message: "Hello! I'm your AI Activity Assistant. I can help you optimize your tasks, meetings, and calls. What would you like to know?",
      timestamp: new Date()
    }
  ])

  const handleSendMessage = () => {
    if (!chatMessage.trim()) return

    const newMessage = {
      type: 'user' as const,
      message: chatMessage,
      timestamp: new Date()
    }

    setChatHistory(prev => [...prev, newMessage])

    // Simulate AI response
    setTimeout(() => {
      const aiResponse = {
        type: 'ai' as const,
        message: generateAIResponse(chatMessage),
        timestamp: new Date()
      }
      setChatHistory(prev => [...prev, aiResponse])
    }, 1000)

    setChatMessage('')
  }

  const generateAIResponse = (message: string): string => {
    const lowerMessage = message.toLowerCase()
    
    if (lowerMessage.includes('task')) {
      return "I analyzed your tasks and found that you have 15 active tasks with varying priorities. I recommend focusing on the 3 highest priority items first. Would you like me to create an optimized schedule?"
    } else if (lowerMessage.includes('meeting')) {
      return "Your meeting success rate is 87% this month! I notice you have 3 demo meetings without follow-ups scheduled. Shall I auto-schedule follow-up meetings for optimal timing?"
    } else if (lowerMessage.includes('call')) {
      return "Call analysis shows your best performance between 2-4 PM with 40% higher success rates. I can reschedule your pending calls to these optimal time slots if you'd like."
    } else if (lowerMessage.includes('schedule')) {
      return "Based on your activity patterns, I recommend scheduling important tasks on Tuesdays and Wednesdays when your team completes 23% more work. Would you like me to suggest a schedule?"
    } else {
      return "I can help you optimize your activities in several ways: task prioritization, meeting scheduling, call timing optimization, and workload balancing. What specific area would you like to improve?"
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'high':
        return 'bg-red-50 text-red-700 border-red-200'
      case 'medium':
        return 'bg-yellow-50 text-yellow-700 border-yellow-200'
      case 'low':
        return 'bg-green-50 text-green-700 border-green-200'
      default:
        return 'bg-gray-50 text-gray-700 border-gray-200'
    }
  }

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'task':
        return <CheckSquare className="w-4 h-4" />
      case 'meeting':
        return <Calendar className="w-4 h-4" />
      case 'call':
        return <Phone className="w-4 h-4" />
      default:
        return <Lightbulb className="w-4 h-4" />
    }
  }

  const getTrendIcon = (trend: string) => {
    switch (trend) {
      case 'up':
        return <TrendingUp className="w-4 h-4 text-green-600" />
      case 'down':
        return <TrendingUp className="w-4 h-4 text-red-600 rotate-180" />
      default:
        return <BarChart3 className="w-4 h-4 text-gray-600" />
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={setIsOpen}>
      <DialogTrigger asChild>
        <Button className="fixed bottom-6 right-6 h-14 w-14 rounded-full bg-gradient-to-r from-purple-600 to-blue-600 hover:from-purple-700 hover:to-blue-700 shadow-lg z-50">
          <Brain className="w-6 h-6 text-white" />
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-4xl max-h-[80vh] overflow-hidden">
        <DialogHeader>
          <DialogTitle className="flex items-center space-x-2">
            <Bot className="w-6 h-6 text-purple-600" />
            <span>AI Activity Assistant</span>
            <Badge className="bg-purple-100 text-purple-800">Beta</Badge>
          </DialogTitle>
          <DialogDescription>
            Your intelligent companion for optimizing tasks, meetings, and calls
          </DialogDescription>
        </DialogHeader>

        <div className="flex space-x-1 mb-4">
          <Button
            variant={activeTab === 'recommendations' ? 'default' : 'ghost'}
            size="sm"
            onClick={() => setActiveTab('recommendations')}
            className="flex items-center space-x-2"
          >
            <Sparkles className="w-4 h-4" />
            <span>Recommendations</span>
          </Button>
          <Button
            variant={activeTab === 'insights' ? 'default' : 'ghost'}
            size="sm"
            onClick={() => setActiveTab('insights')}
            className="flex items-center space-x-2"
          >
            <BarChart3 className="w-4 h-4" />
            <span>Insights</span>
          </Button>
          <Button
            variant={activeTab === 'chat' ? 'default' : 'ghost'}
            size="sm"
            onClick={() => setActiveTab('chat')}
            className="flex items-center space-x-2"
          >
            <MessageSquare className="w-4 h-4" />
            <span>AI Chat</span>
          </Button>
          <Button
            variant={activeTab === 'automation' ? 'default' : 'ghost'}
            size="sm"
            onClick={() => setActiveTab('automation')}
            className="flex items-center space-x-2"
          >
            <Workflow className="w-4 h-4" />
            <span>Automation</span>
          </Button>
        </div>

        <div className="overflow-y-auto max-h-[60vh]">
          {activeTab === 'recommendations' && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4 mb-4">
                <div className="flex items-center space-x-3 p-3 bg-blue-50 rounded-lg">
                  <Brain className="w-5 h-5 text-blue-600" />
                  <div>
                    <p className="font-medium text-sm">Active Recommendations</p>
                    <p className="text-xs text-gray-600">{aiRecommendations.length} insights available</p>
                  </div>
                </div>
                <div className="flex items-center space-x-3 p-3 bg-green-50 rounded-lg">
                  <Target className="w-5 h-5 text-green-600" />
                  <div>
                    <p className="font-medium text-sm">Potential Impact</p>
                    <p className="text-xs text-gray-600">15% productivity increase</p>
                  </div>
                </div>
              </div>

              {aiRecommendations.map((rec) => (
                <Card key={rec.id} className="border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <div className="space-y-3">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center space-x-2">
                          {getTypeIcon(rec.type)}
                          <h4 className="font-medium text-sm">{rec.title}</h4>
                          <Badge className={getPriorityColor(rec.priority)}>
                            {rec.priority}
                          </Badge>
                        </div>
                        <div className="flex items-center space-x-2">
                          <span className="text-xs text-gray-500">
                            {rec.confidence}% confidence
                          </span>
                          <Badge variant="outline">
                            {rec.impact} impact
                          </Badge>
                        </div>
                      </div>
                      
                      <p className="text-sm text-gray-600">{rec.description}</p>
                      
                      <div className="flex items-center justify-between">
                        <Badge variant="secondary" className="text-xs">
                          {rec.category}
                        </Badge>
                        <Button size="sm" className="text-xs">
                          {rec.action}
                        </Button>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}

          {activeTab === 'insights' && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                {aiInsights.map((insight) => (
                  <Card key={insight.id}>
                    <CardContent className="p-4">
                      <div className="space-y-3">
                        <div className="flex items-center justify-between">
                          <h4 className="font-medium text-sm">{insight.title}</h4>
                          {getTrendIcon(insight.trend)}
                        </div>
                        <div>
                          <p className="text-2xl font-bold text-blue-600">{insight.value}</p>
                          <p className="text-xs text-gray-500">{insight.description}</p>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>

              <Card className="mt-6">
                <CardHeader>
                  <CardTitle className="text-lg">Performance Trends</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-3">
                    <div className="flex items-center justify-between p-3 bg-green-50 rounded-lg">
                      <div className="flex items-center space-x-3">
                        <CheckSquare className="w-5 h-5 text-green-600" />
                        <span className="font-medium">Task Completion</span>
                      </div>
                      <div className="text-right">
                        <p className="font-bold text-green-600">+12%</p>
                        <p className="text-xs text-gray-500">vs last week</p>
                      </div>
                    </div>
                    <div className="flex items-center justify-between p-3 bg-blue-50 rounded-lg">
                      <div className="flex items-center space-x-3">
                        <Calendar className="w-5 h-5 text-blue-600" />
                        <span className="font-medium">Meeting Efficiency</span>
                      </div>
                      <div className="text-right">
                        <p className="font-bold text-blue-600">+8%</p>
                        <p className="text-xs text-gray-500">vs last month</p>
                      </div>
                    </div>
                    <div className="flex items-center justify-between p-3 bg-purple-50 rounded-lg">
                      <div className="flex items-center space-x-3">
                        <Phone className="w-5 h-5 text-purple-600" />
                        <span className="font-medium">Call Success Rate</span>
                      </div>
                      <div className="text-right">
                        <p className="font-bold text-purple-600">+15%</p>
                        <p className="text-xs text-gray-500">vs last quarter</p>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          )}

          {activeTab === 'chat' && (
            <div className="space-y-4">
              <div className="h-96 border rounded-lg p-4 overflow-y-auto bg-gray-50">
                <div className="space-y-4">
                  {chatHistory.map((chat, index) => (
                    <div key={index} className={`flex ${chat.type === 'user' ? 'justify-end' : 'justify-start'}`}>
                      <div className={`max-w-xs lg:max-w-md px-4 py-2 rounded-lg ${
                        chat.type === 'user' 
                          ? 'bg-blue-600 text-white' 
                          : 'bg-white border border-gray-200'
                      }`}>
                        <p className="text-sm">{chat.message}</p>
                        <p className="text-xs opacity-70 mt-1">
                          {chat.timestamp.toLocaleTimeString()}
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              <div className="flex space-x-2">
                <Input
                  placeholder="Ask me anything about your activities..."
                  value={chatMessage}
                  onChange={(e) => setChatMessage(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && handleSendMessage()}
                />
                <Button onClick={handleSendMessage}>
                  <Send className="w-4 h-4" />
                </Button>
              </div>
            </div>
          )}

          {activeTab === 'automation' && (
            <div className="space-y-4">
              <div className="grid grid-cols-1 gap-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center space-x-2">
                      <Workflow className="w-5 h-5" />
                      <span>Smart Automation Rules</span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="space-y-3">
                      <div className="flex items-center justify-between p-3 border rounded-lg">
                        <div className="space-y-1">
                          <p className="font-medium text-sm">Auto-assign tasks by expertise</p>
                          <p className="text-xs text-gray-500">Automatically assign tasks based on team member skills</p>
                        </div>
                        <Button size="sm" variant="outline">Enable</Button>
                      </div>
                      <div className="flex items-center justify-between p-3 border rounded-lg">
                        <div className="space-y-1">
                          <p className="font-medium text-sm">Smart meeting scheduling</p>
                          <p className="text-xs text-gray-500">Schedule meetings at optimal times for all participants</p>
                        </div>
                        <Button size="sm" variant="outline">Enable</Button>
                      </div>
                      <div className="flex items-center justify-between p-3 border rounded-lg">
                        <div className="space-y-1">
                          <p className="font-medium text-sm">Follow-up reminders</p>
                          <p className="text-xs text-gray-500">Automatically create follow-up tasks after calls/meetings</p>
                        </div>
                        <Button size="sm" variant="outline">Enable</Button>
                      </div>
                      <div className="flex items-center justify-between p-3 border rounded-lg bg-green-50">
                        <div className="space-y-1">
                          <p className="font-medium text-sm">Priority escalation</p>
                          <p className="text-xs text-gray-500">Auto-escalate overdue high-priority tasks</p>
                        </div>
                        <Button size="sm" className="bg-green-600">Enabled</Button>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center space-x-2">
                      <Settings className="w-5 h-5" />
                      <span>AI Configuration</span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="space-y-3">
                      <div>
                        <label className="text-sm font-medium">AI Aggressiveness Level</label>
                        <Select defaultValue="moderate">
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="conservative">Conservative - Only high-confidence suggestions</SelectItem>
                            <SelectItem value="moderate">Moderate - Balanced approach</SelectItem>
                            <SelectItem value="aggressive">Aggressive - Proactive optimization</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div>
                        <label className="text-sm font-medium">Notification Frequency</label>
                        <Select defaultValue="daily">
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="realtime">Real-time</SelectItem>
                            <SelectItem value="hourly">Hourly</SelectItem>
                            <SelectItem value="daily">Daily</SelectItem>
                            <SelectItem value="weekly">Weekly</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}