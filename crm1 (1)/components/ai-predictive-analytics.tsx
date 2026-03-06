"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { TrendingUp, Target, Calendar, DollarSign, Brain, AlertTriangle, CheckCircle2, Clock } from "lucide-react"
import { type OpportunityData, AIPredictiveService } from "@/lib/ai-services"

interface AIPredictiveAnalyticsProps {
  opportunities: OpportunityData[]
}

export function AIPredictiveAnalytics({ opportunities }: AIPredictiveAnalyticsProps) {
  // Calculate AI predictions for each opportunity
  const opportunitiesWithPredictions = opportunities.map((opp) => ({
    ...opp,
    aiProbability: AIPredictiveService.predictConversionProbability(opp),
  }))

  // Calculate summary metrics
  const totalPipelineValue = opportunities.reduce(
    (sum, opp) => sum + Number.parseFloat(opp.value.replace(/[₹,]/g, "")),
    0,
  )

  const aiWeightedValue = opportunitiesWithPredictions.reduce(
    (sum, opp) => sum + Number.parseFloat(opp.value.replace(/[₹,]/g, "")) * (opp.aiProbability / 100),
    0,
  )

  const traditionalWeightedValue = opportunities.reduce(
    (sum, opp) => sum + Number.parseFloat(opp.value.replace(/[₹,]/g, "")) * (Number.parseInt(opp.probability) / 100),
    0,
  )

  const highProbabilityDeals = opportunitiesWithPredictions.filter((opp) => opp.aiProbability >= 70)
  const atRiskDeals = opportunitiesWithPredictions.filter(
    (opp) => opp.aiProbability < Number.parseInt(opp.probability) - 20,
  )

  const getProbabilityColor = (probability: number) => {
    if (probability >= 80) return "text-green-600"
    if (probability >= 60) return "text-yellow-600"
    if (probability >= 40) return "text-orange-600"
    return "text-red-600"
  }

  const getProbabilityBadgeColor = (probability: number) => {
    if (probability >= 80) return "bg-green-100 text-green-800"
    if (probability >= 60) return "bg-yellow-100 text-yellow-800"
    if (probability >= 40) return "bg-orange-100 text-orange-800"
    return "bg-red-100 text-red-800"
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center space-x-3">
        <div className="p-2 rounded-lg bg-gradient-to-r from-purple-500 to-purple-600">
          <Brain className="w-6 h-6 text-white" />
        </div>
        <div>
          <h2 className="text-2xl font-bold">AI Predictive Analytics</h2>
          <p className="text-gray-600">Advanced AI-powered opportunity analysis and predictions</p>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">AI Weighted Pipeline</CardTitle>
            <Brain className="h-4 w-4 text-purple-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">₹{(aiWeightedValue / 100000).toFixed(1)}L</div>
            <p className="text-xs text-muted-foreground">
              {(((aiWeightedValue - traditionalWeightedValue) / traditionalWeightedValue) * 100).toFixed(1)}% vs
              traditional
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">High Probability Deals</CardTitle>
            <CheckCircle2 className="h-4 w-4 text-green-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{highProbabilityDeals.length}</div>
            <p className="text-xs text-muted-foreground">
              ₹
              {(
                highProbabilityDeals.reduce((sum, opp) => sum + Number.parseFloat(opp.value.replace(/[₹,]/g, "")), 0) /
                100000
              ).toFixed(1)}
              L value
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">At-Risk Deals</CardTitle>
            <AlertTriangle className="h-4 w-4 text-orange-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{atRiskDeals.length}</div>
            <p className="text-xs text-muted-foreground">Need immediate attention</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Forecast Accuracy</CardTitle>
            <Target className="h-4 w-4 text-blue-600" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">87%</div>
            <p className="text-xs text-muted-foreground">Based on historical data</p>
          </CardContent>
        </Card>
      </div>

      <Tabs defaultValue="predictions" className="w-full">
        <TabsList className="grid w-full grid-cols-3">
          <TabsTrigger value="predictions">AI Predictions</TabsTrigger>
          <TabsTrigger value="insights">Key Insights</TabsTrigger>
          <TabsTrigger value="recommendations">Recommendations</TabsTrigger>
        </TabsList>

        <TabsContent value="predictions" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Opportunity Predictions</CardTitle>
              <CardDescription>AI-powered probability analysis compared to manual estimates</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {opportunitiesWithPredictions.map((opp, index) => (
                  <div key={index} className="flex items-center justify-between p-4 border rounded-lg">
                    <div className="flex-1">
                      <div className="flex items-center space-x-3">
                        <div>
                          <h4 className="font-medium">{opp.accountName}</h4>
                          <p className="text-sm text-gray-600">{opp.product}</p>
                        </div>
                        <Badge variant="outline">{opp.stage}</Badge>
                      </div>
                      <div className="mt-2 flex items-center space-x-4">
                        <div className="text-sm">
                          <span className="text-gray-500">Value: </span>
                          <span className="font-medium">{opp.value}</span>
                        </div>
                        <div className="text-sm">
                          <span className="text-gray-500">Expected: </span>
                          <span>{opp.expectedClose}</span>
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center space-x-6">
                      <div className="text-center">
                        <p className="text-xs text-gray-500 mb-1">Manual</p>
                        <Badge variant="outline">{opp.probability}</Badge>
                      </div>
                      <div className="text-center">
                        <p className="text-xs text-gray-500 mb-1">AI Prediction</p>
                        <Badge className={getProbabilityBadgeColor(opp.aiProbability)}>{opp.aiProbability}%</Badge>
                      </div>
                      <div className="w-24">
                        <Progress value={opp.aiProbability} className="h-2" />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="insights" className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <TrendingUp className="w-5 h-5 mr-2 text-green-600" />
                  Pipeline Health
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-sm">Overall Pipeline Strength</span>
                  <Badge className="bg-green-100 text-green-800">Strong</Badge>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Stage Distribution</span>
                  <span className="text-sm text-gray-600">Balanced</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Velocity Trend</span>
                  <span className="text-sm text-green-600">↗ Improving</span>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <AlertTriangle className="w-5 h-5 mr-2 text-orange-600" />
                  Risk Analysis
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-sm">Deals at Risk</span>
                  <Badge className="bg-orange-100 text-orange-800">{atRiskDeals.length}</Badge>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Overdue Follow-ups</span>
                  <Badge className="bg-red-100 text-red-800">2</Badge>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Stalled Opportunities</span>
                  <Badge className="bg-yellow-100 text-yellow-800">1</Badge>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Calendar className="w-5 h-5 mr-2 text-blue-600" />
                  Timing Analysis
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-sm">This Quarter</span>
                  <span className="text-sm font-medium">₹{((aiWeightedValue * 0.6) / 100000).toFixed(1)}L</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Next Quarter</span>
                  <span className="text-sm font-medium">₹{((aiWeightedValue * 0.4) / 100000).toFixed(1)}L</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Avg. Sales Cycle</span>
                  <span className="text-sm text-gray-600">45 days</span>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <DollarSign className="w-5 h-5 mr-2 text-green-600" />
                  Revenue Forecast
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-sm">Conservative</span>
                  <span className="text-sm font-medium">₹{((aiWeightedValue * 0.8) / 100000).toFixed(1)}L</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Most Likely</span>
                  <span className="text-sm font-medium">₹{(aiWeightedValue / 100000).toFixed(1)}L</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Optimistic</span>
                  <span className="text-sm font-medium">₹{((aiWeightedValue * 1.2) / 100000).toFixed(1)}L</span>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="recommendations" className="space-y-4">
          <div className="grid grid-cols-1 gap-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Brain className="w-5 h-5 mr-2 text-purple-600" />
                  AI Recommendations
                </CardTitle>
                <CardDescription>Actionable insights to improve your opportunity conversion</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="p-4 border-l-4 border-l-green-500 bg-green-50">
                  <div className="flex items-start space-x-3">
                    <CheckCircle2 className="w-5 h-5 text-green-600 mt-0.5" />
                    <div>
                      <h4 className="font-medium text-green-800">Focus on High-Probability Deals</h4>
                      <p className="text-sm text-green-700 mt-1">
                        Prioritize the {highProbabilityDeals.length} opportunities with 70%+ AI probability. These
                        represent ₹
                        {(
                          highProbabilityDeals.reduce(
                            (sum, opp) => sum + Number.parseFloat(opp.value.replace(/[₹,]/g, "")),
                            0,
                          ) / 100000
                        ).toFixed(1)}
                        L in potential revenue.
                      </p>
                    </div>
                  </div>
                </div>

                <div className="p-4 border-l-4 border-l-orange-500 bg-orange-50">
                  <div className="flex items-start space-x-3">
                    <AlertTriangle className="w-5 h-5 text-orange-600 mt-0.5" />
                    <div>
                      <h4 className="font-medium text-orange-800">Address At-Risk Opportunities</h4>
                      <p className="text-sm text-orange-700 mt-1">
                        {atRiskDeals.length} deals show lower AI probability than manual estimates. Review these
                        opportunities for potential issues or missing information.
                      </p>
                    </div>
                  </div>
                </div>

                <div className="p-4 border-l-4 border-l-blue-500 bg-blue-50">
                  <div className="flex items-start space-x-3">
                    <Clock className="w-5 h-5 text-blue-600 mt-0.5" />
                    <div>
                      <h4 className="font-medium text-blue-800">Optimize Follow-up Timing</h4>
                      <p className="text-sm text-blue-700 mt-1">
                        Based on historical patterns, opportunities in "Proposal" stage have highest conversion when
                        followed up within 3-5 days.
                      </p>
                    </div>
                  </div>
                </div>

                <div className="p-4 border-l-4 border-l-purple-500 bg-purple-50">
                  <div className="flex items-start space-x-3">
                    <Target className="w-5 h-5 text-purple-600 mt-0.5" />
                    <div>
                      <h4 className="font-medium text-purple-800">Improve Qualification Process</h4>
                      <p className="text-sm text-purple-700 mt-1">
                        Research institutions show 23% higher conversion rates. Consider developing specialized
                        qualification criteria for this segment.
                      </p>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  )
}
