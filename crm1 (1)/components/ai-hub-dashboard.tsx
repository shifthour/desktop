"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
  Bot, 
  Brain,
  TrendingUp,
  AlertTriangle,
  Clock,
  Phone,
  Mail,
  MessageSquare,
  FileText,
  Target,
  Zap,
  CheckCircle,
  Star,
  DollarSign,
  Calendar,
  ArrowRight,
  Bell
} from "lucide-react"
import AgenticAIService from "@/lib/agentic-ai-service"

interface AIAction {
  id: string
  type: 'call' | 'email' | 'whatsapp' | 'demo' | 'proposal' | 'follow-up'
  priority: 'high' | 'medium' | 'low'
  title: string
  description: string
  entityId: string
  entityType: 'lead' | 'deal' | 'account'
  confidence: number
  estimatedImpact: string
  suggestedTime?: string
}

interface AgentInsight {
  agent: 'sales-intelligence' | 'follow-up-automation' | 'revenue-optimizer'
  type: 'opportunity' | 'risk' | 'optimization' | 'alert'
  title: string
  description: string
  actions: AIAction[]
  confidence: number
  impact: 'high' | 'medium' | 'low'
  urgency: 'immediate' | 'today' | 'this-week' | 'next-week'
}

export function AIHubDashboard() {
  const [insights, setInsights] = useState<AgentInsight[]>([])
  const [loading, setLoading] = useState(true)
  const [selectedAgent, setSelectedAgent] = useState<string>('all')
  const [expandedInsight, setExpandedInsight] = useState<string>('')

  useEffect(() => {
    loadAIInsights()
  }, [])

  const loadAIInsights = async () => {
    try {
      setLoading(true)
      
      // Fetch real data from all sources
      const [leadsRes, dealsRes, accountsRes] = await Promise.all([
        fetch('/api/leads'),
        fetch('/api/deals'),  
        fetch('/api/accounts')
      ])
      
      const leadsData = await leadsRes.json()
      const dealsData = await dealsRes.json()
      const accountsData = await accountsRes.json()
      
      // Extract data from API responses correctly
      const leads = Array.isArray(leadsData) ? leadsData : (leadsData.leads || [])
      const deals = Array.isArray(dealsData) ? dealsData : (dealsData.deals || [])
      const accounts = Array.isArray(accountsData) ? accountsData : (accountsData.accounts || [])
      
      // Debug: Log the data being passed to AI service
      console.log('AI Hub Data Debug:', {
        leadsCount: leads.length,
        dealsCount: deals.length,
        accountsCount: accounts.length,
        sampleLead: leads[0],
        sampleDeal: deals[0]
      })
      
      // Generate insights using the agentic AI service
      const aiInsights = await AgenticAIService.generateAllAgentInsights(leads, deals, accounts)
      console.log('Generated AI Insights:', aiInsights)
      setInsights(aiInsights)
      
    } catch (error) {
      console.error('Error loading AI insights:', error)
    } finally {
      setLoading(false)
    }
  }

  const getAgentIcon = (agent: string) => {
    switch (agent) {
      case 'sales-intelligence': return Brain
      case 'follow-up-automation': return Clock
      case 'revenue-optimizer': return TrendingUp
      default: return Bot
    }
  }

  const getAgentColor = (agent: string) => {
    switch (agent) {
      case 'sales-intelligence': return 'from-blue-500 to-blue-600'
      case 'follow-up-automation': return 'from-green-500 to-green-600'
      case 'revenue-optimizer': return 'from-purple-500 to-purple-600'
      default: return 'from-gray-500 to-gray-600'
    }
  }

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'opportunity': return Target
      case 'risk': return AlertTriangle
      case 'optimization': return Zap
      case 'alert': return Bell
      default: return Bot
    }
  }

  const getActionIcon = (actionType: string) => {
    switch (actionType) {
      case 'call': return Phone
      case 'email': return Mail
      case 'whatsapp': return MessageSquare
      case 'demo': return FileText
      case 'proposal': return FileText
      case 'follow-up': return Calendar
      default: return CheckCircle
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'high': return 'bg-red-100 text-red-800 border-red-200'
      case 'medium': return 'bg-yellow-100 text-yellow-800 border-yellow-200'
      case 'low': return 'bg-green-100 text-green-800 border-green-200'
      default: return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const getUrgencyColor = (urgency: string) => {
    switch (urgency) {
      case 'immediate': return 'bg-red-500'
      case 'today': return 'bg-orange-500'
      case 'this-week': return 'bg-yellow-500'
      case 'next-week': return 'bg-green-500'
      default: return 'bg-gray-500'
    }
  }

  const filteredInsights = selectedAgent === 'all' 
    ? insights 
    : insights.filter(insight => insight.agent === selectedAgent)

  const agentStats = {
    'sales-intelligence': insights.filter(i => i.agent === 'sales-intelligence').length,
    'follow-up-automation': insights.filter(i => i.agent === 'follow-up-automation').length,
    'revenue-optimizer': insights.filter(i => i.agent === 'revenue-optimizer').length,
    total: insights.length
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">AI Command Center</h1>
          <p className="text-gray-600">Autonomous AI agents working on your sales pipeline</p>
        </div>
        <Button onClick={loadAIInsights} disabled={loading}>
          <Bot className="w-4 h-4 mr-2" />
          {loading ? 'Analyzing...' : 'Refresh Insights'}
        </Button>
      </div>

      {/* Agent Performance Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Total Insights</p>
                <p className="text-2xl font-bold">{loading ? '...' : agentStats.total}</p>
              </div>
              <Bot className="w-8 h-8 text-indigo-600" />
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Sales Intelligence</p>
                <p className="text-2xl font-bold text-blue-600">{loading ? '...' : agentStats['sales-intelligence']}</p>
              </div>
              <Brain className="w-8 h-8 text-blue-600" />
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Follow-up Auto</p>
                <p className="text-2xl font-bold text-green-600">{loading ? '...' : agentStats['follow-up-automation']}</p>
              </div>
              <Clock className="w-8 h-8 text-green-600" />
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Revenue Optimizer</p>
                <p className="text-2xl font-bold text-purple-600">{loading ? '...' : agentStats['revenue-optimizer']}</p>
              </div>
              <TrendingUp className="w-8 h-8 text-purple-600" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Agent Filter Tabs */}
      <Tabs value={selectedAgent} onValueChange={setSelectedAgent} className="w-full">
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="all">All Agents</TabsTrigger>
          <TabsTrigger value="sales-intelligence">Sales Intelligence</TabsTrigger>
          <TabsTrigger value="follow-up-automation">Follow-up Automation</TabsTrigger>
          <TabsTrigger value="revenue-optimizer">Revenue Optimizer</TabsTrigger>
        </TabsList>

        <TabsContent value={selectedAgent} className="space-y-4">
          {loading ? (
            <Card>
              <CardContent className="p-8 text-center">
                <Bot className="w-12 h-12 mx-auto mb-4 text-gray-400 animate-pulse" />
                <p className="text-gray-600">AI agents analyzing your pipeline data...</p>
              </CardContent>
            </Card>
          ) : filteredInsights.length === 0 ? (
            <Card>
              <CardContent className="p-8 text-center">
                <CheckCircle className="w-12 h-12 mx-auto mb-4 text-green-500" />
                <h3 className="text-lg font-semibold mb-2">All Good!</h3>
                <p className="text-gray-600">No urgent insights at the moment. Your pipeline is running smoothly.</p>
              </CardContent>
            </Card>
          ) : (
            filteredInsights.map((insight) => {
              const AgentIcon = getAgentIcon(insight.agent)
              const TypeIcon = getTypeIcon(insight.type)
              const isExpanded = expandedInsight === insight.title
              
              return (
                <Card key={insight.title} className="overflow-hidden border-l-4" 
                      style={{ borderLeftColor: insight.urgency === 'immediate' ? '#ef4444' : 
                                               insight.urgency === 'today' ? '#f97316' :
                                               insight.urgency === 'this-week' ? '#eab308' : '#22c55e' }}>
                  <CardContent className="p-6">
                    <div className="flex items-start justify-between mb-4">
                      <div className="flex items-start space-x-4">
                        <div className={`p-3 rounded-lg bg-gradient-to-r ${getAgentColor(insight.agent)}`}>
                          <AgentIcon className="w-6 h-6 text-white" />
                        </div>
                        <div className="flex-1">
                          <div className="flex items-center space-x-2 mb-2">
                            <TypeIcon className="w-4 h-4 text-gray-500" />
                            <h3 className="text-lg font-semibold">{insight.title}</h3>
                            <Badge variant="outline" className={getPriorityColor(insight.impact)}>
                              {insight.impact} impact
                            </Badge>
                            <div className={`w-3 h-3 rounded-full ${getUrgencyColor(insight.urgency)}`} 
                                 title={`Urgency: ${insight.urgency}`} />
                          </div>
                          <p className="text-gray-600 mb-3">{insight.description}</p>
                          <div className="flex items-center space-x-4 text-sm text-gray-500">
                            <span>Agent: {insight.agent.replace('-', ' ')}</span>
                            <span>Confidence: {insight.confidence}%</span>
                            <span>{insight.actions.length} actions</span>
                          </div>
                        </div>
                      </div>
                    </div>
                    
                    {/* Actions Grid */}
                    <div className="space-y-3">
                      <div className="flex items-center justify-between">
                        <h4 className="text-sm font-medium text-gray-700">Recommended Actions</h4>
                        <Button 
                          variant="ghost" 
                          size="sm"
                          onClick={() => setExpandedInsight(isExpanded ? '' : insight.title)}
                        >
                          {isExpanded ? 'Collapse' : 'Expand'}
                        </Button>
                      </div>
                      
                      <div className={`grid gap-3 ${isExpanded ? 'grid-cols-1' : 'grid-cols-1 md:grid-cols-2'}`}>
                        {insight.actions.slice(0, isExpanded ? insight.actions.length : 2).map((action) => {
                          const ActionIcon = getActionIcon(action.type)
                          
                          return (
                            <div key={action.id} className="p-4 bg-gray-50 rounded-lg border">
                              <div className="flex items-start justify-between">
                                <div className="flex items-start space-x-3 flex-1">
                                  <div className={`p-2 rounded ${getPriorityColor(action.priority)}`}>
                                    <ActionIcon className="w-4 h-4" />
                                  </div>
                                  <div className="flex-1">
                                    <h5 className="font-medium text-sm mb-1">{action.title}</h5>
                                    <p className="text-xs text-gray-600 mb-2">{action.description}</p>
                                    <div className="flex items-center space-x-3 text-xs text-gray-500">
                                      <span>Type: {action.entityType}</span>
                                      <span>Impact: {action.estimatedImpact}</span>
                                      {action.suggestedTime && (
                                        <span>Best time: {action.suggestedTime}</span>
                                      )}
                                    </div>
                                  </div>
                                </div>
                                <div className="flex flex-col items-end space-y-1">
                                  <Badge variant="outline" className="text-xs">
                                    {action.confidence}% confident
                                  </Badge>
                                  <Button size="sm" variant="outline">
                                    <ArrowRight className="w-3 h-3" />
                                  </Button>
                                </div>
                              </div>
                            </div>
                          )
                        })}
                      </div>
                      
                      {!isExpanded && insight.actions.length > 2 && (
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          className="w-full"
                          onClick={() => setExpandedInsight(insight.title)}
                        >
                          View {insight.actions.length - 2} more actions
                        </Button>
                      )}
                    </div>
                  </CardContent>
                </Card>
              )
            })
          )}
        </TabsContent>
      </Tabs>

      {/* Quick Actions Summary */}
      {!loading && insights.length > 0 && (
        <Card className="border-l-4 border-l-indigo-500">
          <CardHeader>
            <CardTitle className="flex items-center">
              <Zap className="w-5 h-5 mr-2 text-indigo-600" />
              Priority Actions Summary
            </CardTitle>
            <CardDescription>
              Most urgent actions across all AI agents
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {insights
                .filter(insight => insight.urgency === 'immediate')
                .flatMap(insight => insight.actions)
                .slice(0, 6)
                .map((action) => {
                  const ActionIcon = getActionIcon(action.type)
                  return (
                    <div key={action.id} className="p-4 bg-gradient-to-r from-red-50 to-orange-50 rounded-lg border border-red-200">
                      <div className="flex items-center space-x-2 mb-2">
                        <ActionIcon className="w-4 h-4 text-red-600" />
                        <Badge className="bg-red-500 text-white text-xs">URGENT</Badge>
                      </div>
                      <h4 className="font-medium text-sm text-gray-900 mb-1">{action.title}</h4>
                      <p className="text-xs text-gray-600 mb-2">{action.description}</p>
                      <div className="flex justify-between items-center">
                        <span className="text-xs text-gray-500">{action.estimatedImpact}</span>
                        <Button size="sm" className="bg-red-600 hover:bg-red-700">
                          Take Action
                        </Button>
                      </div>
                    </div>
                  )
                })}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}