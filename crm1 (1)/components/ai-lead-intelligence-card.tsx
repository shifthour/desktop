"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Brain, Phone, Mail, MessageSquare, Zap, Target, AlertTriangle, CheckCircle } from "lucide-react"
import AgenticAIService from "@/lib/agentic-ai-service"

interface AILeadIntelligenceProps {
  leads: any[]
}

export function AILeadIntelligenceCard({ leads }: AILeadIntelligenceProps) {
  const [insights, setInsights] = useState<any[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (leads.length > 0) {
      generateInsights()
    }
  }, [leads])

  const generateInsights = async () => {
    setLoading(true)
    try {
      // Convert leads to format expected by AgenticAIService
      const formattedLeads = leads.map(lead => ({
        id: lead.id,
        account_name: lead.accountName || lead.leadName,
        contact_name: lead.contactName,
        phone: lead.phone || lead.contactNo,
        email: lead.email,
        product_name: lead.product,
        budget: lead.budget,
        lead_source: lead.leadSource,
        lead_status: lead.leadStatus || lead.salesStage,
        priority: lead.priority,
        city: lead.city,
        state: lead.state,
        created_at: new Date(lead.date.split('/').reverse().join('-')).toISOString(),
        next_followup_date: lead.nextFollowupDate,
        assigned_to: lead.assignedTo,
        department: lead.department
      }))

      // Generate insights using Sales Intelligence Agent only (for leads page)
      const salesInsights = AgenticAIService.generateSalesIntelligence(formattedLeads, [], [])
      setInsights(salesInsights)
    } catch (error) {
      console.error('Error generating lead insights:', error)
    } finally {
      setLoading(false)
    }
  }

  const getActionIcon = (actionType: string) => {
    switch (actionType) {
      case 'call': return Phone
      case 'email': return Mail
      case 'whatsapp': return MessageSquare
      default: return CheckCircle
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'high': return 'bg-red-100 text-red-800'
      case 'medium': return 'bg-yellow-100 text-yellow-800'
      case 'low': return 'bg-green-100 text-green-800'
      default: return 'bg-gray-100 text-gray-800'
    }
  }

  if (leads.length === 0) {
    return (
      <Card className="border-l-4 border-l-blue-500">
        <CardContent className="p-6 text-center">
          <Brain className="w-12 h-12 mx-auto mb-4 text-gray-400" />
          <h3 className="text-lg font-semibold mb-2">AI Lead Intelligence</h3>
          <p className="text-gray-600">AI insights will appear when leads are available</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className="border-l-4 border-l-blue-500">
      <CardHeader>
        <CardTitle className="flex items-center">
          <div className="p-2 rounded-lg bg-gradient-to-r from-blue-500 to-blue-600 mr-3">
            <Brain className="w-5 h-5 text-white" />
          </div>
          AI Lead Intelligence
        </CardTitle>
        <CardDescription>
          Real-time insights from Sales Intelligence Agent analyzing your lead data
        </CardDescription>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="flex items-center justify-center p-6">
            <Brain className="w-6 h-6 animate-pulse text-blue-500 mr-2" />
            <span className="text-gray-600">Analyzing leads...</span>
          </div>
        ) : insights.length === 0 ? (
          <div className="p-6 text-center">
            <CheckCircle className="w-10 h-10 mx-auto mb-3 text-green-500" />
            <h4 className="font-medium mb-1">All leads reviewed</h4>
            <p className="text-sm text-gray-600">No immediate actions required</p>
          </div>
        ) : (
          <div className="space-y-4">
            {insights.map((insight, index) => (
              <div key={index} className="border rounded-lg p-4">
                <div className="flex items-start justify-between mb-3">
                  <div className="flex items-center space-x-2">
                    {insight.type === 'opportunity' ? (
                      <Target className="w-5 h-5 text-green-600" />
                    ) : (
                      <AlertTriangle className="w-5 h-5 text-orange-600" />
                    )}
                    <h4 className="font-semibold text-gray-900">{insight.title}</h4>
                    <Badge variant="outline" className="text-xs">
                      {insight.confidence}% confident
                    </Badge>
                  </div>
                  <Badge className={`text-xs ${insight.urgency === 'immediate' ? 'bg-red-500' : 'bg-blue-500'}`}>
                    {insight.urgency}
                  </Badge>
                </div>
                
                <p className="text-sm text-gray-600 mb-3">{insight.description}</p>
                
                <div className="space-y-2">
                  {insight.actions.slice(0, 3).map((action: any) => {
                    const ActionIcon = getActionIcon(action.type)
                    return (
                      <div key={action.id} className="flex items-center justify-between p-3 bg-gray-50 rounded">
                        <div className="flex items-center space-x-3">
                          <div className={`p-1 rounded ${getPriorityColor(action.priority)}`}>
                            <ActionIcon className="w-4 h-4" />
                          </div>
                          <div>
                            <h5 className="text-sm font-medium">{action.title}</h5>
                            <p className="text-xs text-gray-600">{action.description}</p>
                            <p className="text-xs text-blue-600">{action.estimatedImpact}</p>
                          </div>
                        </div>
                        <Button size="sm" variant="outline">
                          Act Now
                        </Button>
                      </div>
                    )
                  })}
                </div>
                
                {insight.actions.length > 3 && (
                  <Button variant="ghost" size="sm" className="w-full mt-2">
                    View {insight.actions.length - 3} more actions
                  </Button>
                )}
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}