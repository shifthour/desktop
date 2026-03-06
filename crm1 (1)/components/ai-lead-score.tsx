"use client"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { Brain, TrendingUp, Target, Zap } from "lucide-react"
import { AILeadScoringService, LeadData, AIInsight } from "@/lib/ai-services"

interface AILeadScoreProps {
  lead: LeadData
  showDetails?: boolean
}

export function AILeadScore({ lead, showDetails = false }: AILeadScoreProps) {
  const score = AILeadScoringService.calculateLeadScore(lead)
  const insight = AILeadScoringService.getLeadScoreInsight(score)
  
  const getScoreColor = (score: number) => {
    if (score >= 80) return "text-green-600"
    if (score >= 60) return "text-blue-600"
    if (score >= 40) return "text-yellow-600"
    return "text-red-600"
  }
  
  const getScoreGradient = (score: number) => {
    if (score >= 80) return "from-green-500 to-green-600"
    if (score >= 60) return "from-blue-500 to-blue-600"
    if (score >= 40) return "from-yellow-500 to-yellow-600"
    return "from-red-500 to-red-600"
  }
  
  if (!showDetails) {
    return (
      <div className="flex items-center space-x-2">
        <Brain className="w-4 h-4 text-purple-600" />
        <span className={`font-semibold ${getScoreColor(score)}`}>
          {score}
        </span>
        <Badge 
          variant="outline" 
          className={`text-xs ${
            insight.priority === 'critical' ? 'border-red-500 text-red-700' :
            insight.priority === 'high' ? 'border-orange-500 text-orange-700' :
            insight.priority === 'medium' ? 'border-blue-500 text-blue-700' :
            'border-gray-500 text-gray-700'
          }`}
        >
          {insight.priority.toUpperCase()}
        </Badge>
      </div>
    )
  }
  
  return (
    <Card className="border-l-4 border-l-purple-500">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center text-lg">
          <div className={`p-2 rounded-lg bg-gradient-to-r ${getScoreGradient(score)} mr-3`}>
            <Brain className="w-5 h-5 text-white" />
          </div>
          AI Lead Score
        </CardTitle>
        <CardDescription>
          Intelligent lead scoring based on multiple factors
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <span className="text-2xl font-bold text-gray-900">{score}</span>
            <span className="text-gray-500">/100</span>
          </div>
          <Badge 
            className={`${
              insight.priority === 'critical' ? 'bg-red-500' :
              insight.priority === 'high' ? 'bg-orange-500' :
              insight.priority === 'medium' ? 'bg-blue-500' :
              'bg-gray-500'
            } text-white`}
          >
            {insight.priority.toUpperCase()} PRIORITY
          </Badge>
        </div>
        
        <Progress value={score} className="h-2" />
        
        <div className="flex items-start space-x-2">
          <Target className="w-4 h-4 text-purple-600 mt-0.5" />
          <p className="text-sm text-gray-700 flex-1">{insight.description}</p>
        </div>
        
        <div className="flex items-center justify-between text-xs text-gray-500">
          <div className="flex items-center space-x-1">
            <TrendingUp className="w-3 h-3" />
            <span>Confidence: {Math.round(insight.confidence * 100)}%</span>
          </div>
          <div className="flex items-center space-x-1">
            <Zap className="w-3 h-3" />
            <span>Auto-updated</span>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
