"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { TrendingUp, Phone, Mail, MessageSquare, AlertTriangle, Target, CheckCircle, DollarSign } from "lucide-react"
import AgenticAIService from "@/lib/agentic-ai-service"

interface AIDealIntelligenceProps {
  deals: any[]
  accounts: any[]
}

export function AIDealIntelligenceCard({ deals, accounts }: AIDealIntelligenceProps) {
  const [insights, setInsights] = useState<any[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (deals.length > 0) {
      generateInsights()
    }
  }, [deals, accounts])

  const generateInsights = async () => {
    setLoading(true)
    try {
      // Convert deals to format expected by AgenticAIService
      const formattedDeals = deals.map(deal => ({
        id: deal.id,
        account_name: deal.accountName,
        contact_person: deal.contactPerson,
        product: deal.product,
        value: deal.value,
        stage: deal.stage,
        probability: deal.probability,
        expected_close_date: deal.expectedCloseDate,
        assigned_to: deal.assignedTo,
        priority: deal.priority,
        status: deal.status,
        created_at: deal.createdAt,
        next_followup_date: deal.nextFollowupDate,
        phone: deal.phone,
        email: deal.email
      }))

      const formattedAccounts = accounts.map(account => ({
        id: account.id,
        account_name: account.account_name,
        industry: account.industry,
        annual_revenue: account.annual_revenue,
        employee_count: account.employee_count,
        city: account.city,
        state: account.state
      }))

      // Generate insights using Revenue Optimizer Agent (for deals page)
      const revenueInsights = AgenticAIService.generateRevenueOptimization(formattedDeals, formattedAccounts)
      setInsights(revenueInsights)
    } catch (error) {
      console.error('Error generating deal insights:', error)
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

  if (deals.length === 0) {
    return (
      <Card className="border-l-4 border-l-purple-500">
        <CardContent className="p-6 text-center">
          <TrendingUp className="w-12 h-12 mx-auto mb-4 text-gray-400" />
          <h3 className="text-lg font-semibold mb-2">AI Revenue Intelligence</h3>
          <p className="text-gray-600">AI insights will appear when deals are available</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className="border-l-4 border-l-purple-500">
      <CardHeader>
        <CardTitle className="flex items-center">
          <div className="p-2 rounded-lg bg-gradient-to-r from-purple-500 to-purple-600 mr-3">
            <TrendingUp className="w-5 h-5 text-white" />
          </div>
          AI Revenue Intelligence
        </CardTitle>
        <CardDescription>
          Revenue Optimizer Agent analyzing your pipeline for risks and opportunities
        </CardDescription>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="flex items-center justify-center p-6">
            <TrendingUp className="w-6 h-6 animate-pulse text-purple-500 mr-2" />
            <span className="text-gray-600">Analyzing pipeline...</span>
          </div>
        ) : insights.length === 0 ? (
          <div className="p-6 text-center">
            <CheckCircle className="w-10 h-10 mx-auto mb-3 text-green-500" />
            <h4 className="font-medium mb-1">Pipeline optimized</h4>
            <p className="text-sm text-gray-600">No immediate risks or optimization opportunities detected</p>
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
                      <AlertTriangle className="w-5 h-5 text-red-600" />
                    )}
                    <h4 className="font-semibold text-gray-900">{insight.title}</h4>
                    <Badge variant="outline" className="text-xs">
                      {insight.confidence}% confident
                    </Badge>
                  </div>
                  <Badge className={`text-xs ${insight.urgency === 'immediate' ? 'bg-red-500' : 'bg-purple-500'}`}>
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
                            <p className="text-xs text-purple-600 font-medium">{action.estimatedImpact}</p>
                          </div>
                        </div>
                        <Button size="sm" variant="outline">
                          Execute
                        </Button>
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}