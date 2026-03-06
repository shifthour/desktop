"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { AlertTriangle } from "lucide-react"

export function ComplaintsChart() {
  const complaintsData = [
    { category: "Same Day", count: 92, color: "bg-green-500" },
    { category: "1-7 days", count: 33, color: "bg-yellow-500" },
    { category: "8-30 days", count: 23, color: "bg-orange-500" },
    { category: ">30 days", count: 75, color: "bg-red-500" },
  ]

  const total = complaintsData.reduce((sum, item) => sum + item.count, 0)

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center">
          <AlertTriangle className="w-5 h-5 mr-2" />
          Complaints TAT (Turn Around Time)
        </CardTitle>
        <CardDescription>Resolution time distribution</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {complaintsData.map((item) => (
            <div key={item.category} className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className={`w-3 h-3 rounded-full ${item.color}`} />
                <span className="text-sm font-medium">{item.category}</span>
              </div>
              <div className="flex items-center space-x-2">
                <div className="w-24 bg-gray-200 rounded-full h-2">
                  <div
                    className={`h-2 rounded-full ${item.color}`}
                    style={{ width: `${(item.count / total) * 100}%` }}
                  />
                </div>
                <span className="text-sm text-gray-600 w-8">{item.count}</span>
              </div>
            </div>
          ))}
        </div>
        <div className="mt-4 pt-4 border-t">
          <div className="flex justify-between text-sm">
            <span className="text-gray-600">Total Complaints</span>
            <span className="font-medium">{total}</span>
          </div>
          <div className="flex justify-between text-sm mt-1">
            <span className="text-gray-600">Avg. Resolution Time</span>
            <span className="font-medium">4.2 days</span>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
