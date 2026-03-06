"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
  Brain, 
  TrendingUp, 
  Lightbulb, 
  Target, 
  AlertCircle, 
  CheckCircle2, 
  Clock,
  Sparkles,
  ArrowRight,
  Mail,
  Phone
} from "lucide-react"
import { AIInsight } from "@/lib/ai-services"

interface AIInsightsPanelProps {
  insights: AIInsight[]
  title?: string
}

export function AIInsightsPanel({ insights, title = "AI Insights" }: AIInsightsPanelProps) {
  const [expandedInsight, setExpandedInsight] = useState<number | null>(null)
  
  const getInsightIcon = (type: AIInsight['type']) => {
    switch (type) {
      case 'lead-score':
        return <Target className="w-4 h-4" />
      case 'next-action':
        return <ArrowRight className="w-4 h-4" />
      case 'prediction':
        return <TrendingUp className="w-4 h-4" />
      case 'recommendation':
        return <Lightbulb className="w-4 h-4" />
      default:
        return <Brain className="w-4 h-4" />
    }
  }
  
  const getPriorityColor = (priority: AIInsight['priority']) => {
    switch (priority) {
      case 'critical':
        return 'bg-red-100 text-red-800 border-red-200'
      case 'high':
        return 'bg-orange-100 text-orange-800 border-orange-200'
      case 'medium':
        return 'bg-blue-100 text-blue-800 border-blue-200'
      case 'low':
        return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }
  
  const getActionButton = (insight: AIInsight) => {
    if (insight.type === 'next-action') {
      return (
        <div className="flex space-x-2">
          <Button size="sm" variant="outline">
            <Mail className="w-3 h-3 mr-1" />
            Email
          </Button>
          <Button size="sm" variant="outline">
            <Phone className="w-3 h-3 mr-1" />
            Call
          </Button>
        </div>
      )
    }
    
    if (insight.actionable) {
      return (
        <Button size="sm">
          Take Action
          <ArrowRight className="w-3 h-3 ml-1" />
        </Button>
      )
    }
    
    return null
  }
  
  const groupedInsights = insights.reduce((acc, insight) => {
    const key = insight.type
    if (!acc[key]) acc[key] = []
    acc[key].push(insight)
    return acc
  }, {} as Record<string, AIInsight[]>)
  
  return (
    <Card className="border-l-4 border-l-purple-500">
      <CardHeader>
        <CardTitle className="flex items-center">
          <div className="p-2 rounded-lg bg-gradient-to-r from-purple-500 to-purple-600 mr-3">
            <Sparkles className="w-5 h-5 text-white" />
          </div>
          {title}
        </CardTitle>
        <CardDescription>
          AI-powered insights and recommendations based on your data
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="all" className="w-full">
          <TabsList className="grid w-full grid-cols-5">
            <TabsTrigger value="all">All</TabsTrigger>
            <TabsTrigger value="lead-score">Scoring</TabsTrigger>
            <TabsTrigger value="next-action">Actions</TabsTrigger>
            <TabsTrigger value="prediction">Predictions</TabsTrigger>
            <TabsTrigger value="recommendation">Tips</TabsTrigger>
          </TabsList>
          
          <TabsContent value="all" className="space-y-3 mt-4">
            {insights.map((insight, index) => (
              <div
                key={index}
                className="p-3 border rounded-lg hover:bg-gray-50 transition-colors cursor-pointer"
                onClick={() => setExpandedInsight(expandedInsight === index ? null : index)}
              >
                <div className="flex items-start justify-between">
                  <div className="flex items-start space-x-3 flex-1">
                    <div className="p-1.5 rounded-lg bg-purple-100 text-purple-600">
                      {getInsightIcon(insight.type)}
                    </div>
                    <div className="flex-1">
                      <div className="flex items-center space-x-2 mb-1">
                        <h4 className="font-medium text-sm">{insight.title}</h4>
                        <Badge 
                          variant="outline" 
                          className={`text-xs ${getPriorityColor(insight.priority)}`}
                        >
                          {insight.priority.toUpperCase()}
                        </Badge>
                      </div>
                      <p className="text-xs text-gray-600 mb-2">{insight.description}</p>
                      
                      {expandedInsight === index && (
                        <div className="flex items-center justify-between mt-3 pt-2 border-t">
                          <div className="flex items-center space-x-3 text-xs text-gray-500">
                            <div className="flex items-center space-x-1">
                              <CheckCircle2 className="w-3 h-3" />
                              <span>Confidence: {Math.round(insight.confidence * 100)}%</span>
                            </div>
                            <div className="flex items-center space-x-1">
                              <Clock className="w-3 h-3" />
                              <span>Real-time</span>
                            </div>
                          </div>
                          {getActionButton(insight)}
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            ))}
            
            {insights.length === 0 && (
              <div className="text-center py-8 text-gray-500">
                <Brain className="w-12 h-12 mx-auto mb-3 text-gray-300" />
                <p>No AI insights available yet.</p>
                <p className="text-sm">Insights will appear as data is analyzed.</p>
              </div>
            )}
          </TabsContent>
          
          {Object.entries(groupedInsights).map(([type, typeInsights]) => (
            <TabsContent key={type} value={type} className="space-y-3 mt-4">
              {typeInsights.map((insight, index) => (
                <div
                  key={index}
                  className="p-3 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-start justify-between">
                    <div className="flex items-start space-x-3 flex-1">
                      <div className="p-1.5 rounded-lg bg-purple-100 text-purple-600">
                        {getInsightIcon(insight.type)}
                      </div>
                      <div className="flex-1">
                        <div className="flex items-center space-x-2 mb-1">
                          <h4 className="font-medium text-sm">{insight.title}</h4>
                          <Badge 
                            variant="outline" 
                            className={`text-xs ${getPriorityColor(insight.priority)}`}
                          >
                            {insight.priority.toUpperCase()}
                          </Badge>
                        </div>
                        <p className="text-xs text-gray-600">{insight.description}</p>
                      </div>
                    </div>
                    {getActionButton(insight)}
                  </div>
                </div>
              ))}
            </TabsContent>
          ))}
        </Tabs>
      </CardContent>
    </Card>
  )
}
