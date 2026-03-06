"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { TrendingUp, TrendingDown, Target, Calendar, AlertTriangle, Award, BarChart3 } from "lucide-react"


const performanceMetrics = [
  {
    metric: "Revenue Growth",
    current: 15.3,
    target: 18.0,
    unit: "%",
    status: "good",
  },
  {
    metric: "Customer Acquisition",
    current: 47,
    target: 50,
    unit: "customers",
    status: "warning",
  },
  {
    metric: "Average Deal Size",
    current: 7.9,
    target: 8.5,
    unit: "₹L",
    status: "good",
  },
  {
    metric: "Sales Cycle",
    current: 45,
    target: 40,
    unit: "days",
    status: "warning",
  },
]

export function AdvancedAnalytics() {
  return (
    <div className="space-y-6">
      {/* Performance Dashboard */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Target className="w-5 h-5 mr-2" />
            Performance vs Targets
          </CardTitle>
          <CardDescription>Track key metrics against business objectives</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {performanceMetrics.map((metric, index) => (
              <div key={index} className="space-y-3">
                <div className="flex items-center justify-between">
                  <h3 className="font-medium">{metric.metric}</h3>
                  <Badge
                    variant="outline"
                    className={
                      metric.status === "good"
                        ? "border-green-200 bg-green-50 text-green-700"
                        : "border-yellow-200 bg-yellow-50 text-yellow-700"
                    }
                  >
                    {metric.status === "good" ? "On Track" : "Needs Attention"}
                  </Badge>
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-sm">
                    <span>
                      Current: {metric.current}
                      {metric.unit}
                    </span>
                    <span>
                      Target: {metric.target}
                      {metric.unit}
                    </span>
                  </div>
                  <Progress value={(metric.current / metric.target) * 100} className="h-2" />
                  <div className="text-xs text-gray-500">
                    {((metric.current / metric.target) * 100).toFixed(1)}% of target achieved
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

    </div>
  )
}
