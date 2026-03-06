"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Brain, Sparkles, Send, Mic, Copy, ThumbsUp, ThumbsDown, Zap, TrendingUp, Users, Target, Calendar, MessageSquare } from "lucide-react"

export function AIAssistantContent() {
  const [message, setMessage] = useState("")
  const [chatHistory, setChatHistory] = useState([
    {
      role: "assistant",
      content: "Hello! I'm your AI CRM Assistant. I can help you with lead prioritization, meeting preparation, email drafts, and activity recommendations. How can I assist you today?",
      timestamp: "10:00 AM"
    }
  ])

  const automations = [
    {
      id: 1,
      name: "Auto-schedule follow-ups",
      description: "Automatically schedule follow-up tasks after meetings",
      status: "active",
      savings: "2 hrs/week"
    },
    {
      id: 2,
      name: "Lead scoring",
      description: "AI-powered lead scoring based on engagement",
      status: "active",
      savings: "5 hrs/week"
    },
    {
      id: 3,
      name: "Email templates",
      description: "Smart email suggestions based on context",
      status: "active",
      savings: "3 hrs/week"
    },
    {
      id: 4,
      name: "Meeting summaries",
      description: "Auto-generate meeting notes and action items",
      status: "inactive",
      savings: "4 hrs/week"
    }
  ]

  const [insights, setInsights] = useState<any[]>([])
  const [statsLoaded, setStatsLoaded] = useState(false)

  useEffect(() => {
    loadRealInsights()
  }, [])

  const loadRealInsights = async () => {
    try {
      const [leadsRes, dealsRes, accountsRes] = await Promise.all([
        fetch('/api/leads'),
        fetch('/api/deals'),
        fetch('/api/accounts')
      ])
      
      const leadsData = await leadsRes.json()
      const dealsData = await dealsRes.json()
      const accountsData = await accountsRes.json()
      
      const leads = leadsData.leads || []
      const deals = dealsData.deals || []
      const accounts = accountsData.accounts || []
      
      const realInsights = []
      
      // High probability deals
      const highProbDeals = deals.filter((deal: any) => deal.probability >= 80)
      if (highProbDeals.length > 0) {
        realInsights.push({
          type: "opportunity",
          title: "High-value opportunity detected",
          description: `${highProbDeals[0].account_name} showing strong buying signals - ${highProbDeals[0].probability}% close probability`,
          action: "Prepare Proposal",
          icon: TrendingUp,
          color: "text-green-600"
        })
      }
      
      // Stale leads (no recent activity)
      const staleLeads = leads.filter((lead: any) => {
        const leadDate = new Date(lead.created_at)
        const daysSince = Math.floor((Date.now() - leadDate.getTime()) / (1000 * 60 * 60 * 24))
        return daysSince > 7 && lead.lead_status === 'New'
      })
      if (staleLeads.length > 0) {
        realInsights.push({
          type: "risk",
          title: "Stale leads detected",
          description: `${staleLeads.length} leads have no follow-up activity for over 7 days`,
          action: "Schedule Follow-up",
          icon: Users,
          color: "text-red-600"
        })
      }
      
      // Total pipeline value insight
      const totalValue = deals.reduce((sum: number, deal: any) => sum + (deal.value || 0), 0)
      if (totalValue > 0) {
        realInsights.push({
          type: "optimization",
          title: "Pipeline value analysis",
          description: `Current pipeline worth ₹${(totalValue / 100000).toFixed(1)}L across ${deals.length} deals`,
          action: "View Analysis",
          icon: Calendar,
          color: "text-blue-600"
        })
      }
      
      // Recent qualified leads
      const qualifiedLeads = leads.filter((lead: any) => lead.lead_status === 'Qualified')
      if (qualifiedLeads.length > 0) {
        realInsights.push({
          type: "lead",
          title: "Qualified leads ready",
          description: `${qualifiedLeads.length} qualified leads ready for deal conversion`,
          action: "Convert to Deals",
          icon: Target,
          color: "text-purple-600"
        })
      }
      
      setInsights(realInsights)
      setStatsLoaded(true)
      
    } catch (error) {
      console.error('Error loading insights:', error)
      setStatsLoaded(true)
    }
  }

  const handleSendMessage = () => {
    if (message.trim()) {
      setChatHistory([...chatHistory, {
        role: "user",
        content: message,
        timestamp: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
      }])
      setMessage("")
      
      // Simulate AI response
      setTimeout(() => {
        setChatHistory(prev => [...prev, {
          role: "assistant",
          content: "I understand you want to know about lead prioritization. Based on your current pipeline data, I can see your qualified leads and active deals. Would you like me to analyze your highest probability deals or suggest follow-up actions for recent leads?",
          timestamp: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
        }])
      }, 1000)
    }
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">AI Assistant</h1>
          <p className="text-gray-500 mt-1">Your intelligent CRM companion powered by advanced AI</p>
        </div>
        <Badge className="bg-green-100 text-green-800">
          <div className="w-2 h-2 bg-green-500 rounded-full mr-2 animate-pulse" />
          AI Active
        </Badge>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Chat Interface */}
        <div className="lg:col-span-2">
          <Card className="h-[600px] flex flex-col">
            <CardHeader>
              <CardTitle className="flex items-center">
                <Brain className="w-5 h-5 mr-2 text-purple-600" />
                AI Chat Assistant
              </CardTitle>
            </CardHeader>
            <CardContent className="flex-1 flex flex-col">
              {/* Chat Messages */}
              <div className="flex-1 overflow-y-auto space-y-4 mb-4">
                {chatHistory.map((msg, index) => (
                  <div key={index} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                    <div className={`max-w-[80%] ${msg.role === 'user' ? 'order-2' : ''}`}>
                      <div className={`rounded-lg p-3 ${
                        msg.role === 'user' 
                          ? 'bg-blue-600 text-white' 
                          : 'bg-gray-100 text-gray-900'
                      }`}>
                        <p className="text-sm">{msg.content}</p>
                      </div>
                      <p className="text-xs text-gray-500 mt-1">{msg.timestamp}</p>
                      {msg.role === 'assistant' && (
                        <div className="flex items-center space-x-2 mt-2">
                          <Button variant="ghost" size="sm">
                            <Copy className="w-3 h-3 mr-1" />
                            Copy
                          </Button>
                          <Button variant="ghost" size="sm">
                            <ThumbsUp className="w-3 h-3" />
                          </Button>
                          <Button variant="ghost" size="sm">
                            <ThumbsDown className="w-3 h-3" />
                          </Button>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
              
              {/* Input Area */}
              <div className="border-t pt-4">
                <div className="flex items-center space-x-2">
                  <Input
                    placeholder="Ask me anything about your CRM data..."
                    value={message}
                    onChange={(e) => setMessage(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && handleSendMessage()}
                    className="flex-1"
                  />
                  <Button variant="ghost" size="icon">
                    <Mic className="w-4 h-4" />
                  </Button>
                  <Button onClick={handleSendMessage} className="bg-purple-600 hover:bg-purple-700">
                    <Send className="w-4 h-4" />
                  </Button>
                </div>
                <div className="flex flex-wrap gap-2 mt-2">
                  <Badge variant="outline" className="cursor-pointer hover:bg-gray-100">
                    Show my qualified leads
                  </Badge>
                  <Badge variant="outline" className="cursor-pointer hover:bg-gray-100">
                    Analyze deal pipeline
                  </Badge>
                  <Badge variant="outline" className="cursor-pointer hover:bg-gray-100">
                    Suggest follow-up actions
                  </Badge>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Insights Panel */}
        <div className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <Sparkles className="w-5 h-5 mr-2 text-yellow-500" />
                AI Insights
              </CardTitle>
              <CardDescription>Proactive recommendations</CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              {insights.map((insight) => (
                <div key={insight.title} className="p-3 border rounded-lg hover:bg-gray-50">
                  <div className="flex items-start space-x-3">
                    <insight.icon className={`w-5 h-5 ${insight.color} mt-0.5`} />
                    <div className="flex-1">
                      <p className="font-medium text-sm">{insight.title}</p>
                      <p className="text-xs text-gray-600 mt-1">{insight.description}</p>
                      <Button size="sm" variant="outline" className="mt-2">
                        {insight.action}
                      </Button>
                    </div>
                  </div>
                </div>
              ))}
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Automations */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Zap className="w-5 h-5 mr-2 text-yellow-500" />
            AI Automations
          </CardTitle>
          <CardDescription>Tasks being handled automatically by AI</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {automations.map((automation) => (
              <div key={automation.id} className="p-4 border rounded-lg">
                <div className="flex items-start justify-between mb-2">
                  <h4 className="font-medium text-sm">{automation.name}</h4>
                  <Badge className={automation.status === 'active' ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}>
                    {automation.status}
                  </Badge>
                </div>
                <p className="text-xs text-gray-600 mb-2">{automation.description}</p>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-purple-600 font-medium">Saves {automation.savings}</span>
                  <Button size="sm" variant="ghost">
                    Configure
                  </Button>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}