"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { TrendingUp } from "lucide-react"

export function SalesChart() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center">
          <TrendingUp className="w-5 h-5 mr-2" />
          Monthly Sales Performance
        </CardTitle>
        <CardDescription>Revenue trends over the past 6 months</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-64 flex items-end justify-between space-x-2">
          {[
            { month: "Feb", value: 65, amount: "₹32.5L" },
            { month: "Mar", value: 78, amount: "₹39.0L" },
            { month: "Apr", value: 52, amount: "₹26.0L" },
            { month: "May", value: 85, amount: "₹42.5L" },
            { month: "Jun", value: 92, amount: "₹46.0L" },
            { month: "Jul", value: 88, amount: "₹44.0L" },
          ].map((data, index) => (
            <div key={data.month} className="flex flex-col items-center space-y-2 flex-1">
              <div className="text-xs font-medium text-gray-600">{data.amount}</div>
              <div
                className="w-full bg-blue-500 rounded-t-sm transition-all duration-300 hover:bg-blue-600"
                style={{ height: `${data.value * 2}px` }}
              />
              <div className="text-xs text-gray-500">{data.month}</div>
            </div>
          ))}
        </div>
        <div className="mt-4 text-sm text-gray-600">
          <span className="text-green-600 font-medium">↗ 12.5%</span> increase from last quarter
        </div>
      </CardContent>
    </Card>
  )
}
