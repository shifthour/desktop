"use client"

import React, { useState } from "react"
import { 
  Dialog, 
  DialogContent, 
  DialogDescription, 
  DialogHeader, 
  DialogTitle 
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
  Download, 
  Share2, 
  Printer, 
  RefreshCw, 
  BarChart3, 
  PieChart, 
  LineChart,
  TrendingUp,
  TrendingDown,
  Minus,
  DollarSign,
  Users,
  Target,
  Clock,
  CheckCircle,
  AlertTriangle
} from "lucide-react"
import { 
  DropdownMenu, 
  DropdownMenuContent, 
  DropdownMenuItem, 
  DropdownMenuTrigger 
} from "@/components/ui/dropdown-menu"

interface ReportViewerModalProps {
  isOpen: boolean
  onClose: () => void
  reportData: any
  reportName: string
  reportType: string
  onExport: (format: string) => void
}

export function ReportViewerModal({ 
  isOpen, 
  onClose, 
  reportData, 
  reportName, 
  reportType,
  onExport 
}: ReportViewerModalProps) {
  const [activeTab, setActiveTab] = useState("overview")

  if (!reportData || !reportData.data) {
    return null
  }

  const data = reportData.data

  // Helper function to render KPI cards
  const renderKPICard = (title: string, value: any, trend?: any, icon?: any) => {
    const IconComponent = icon || Target
    const trendIcon = trend?.type === 'positive' ? TrendingUp : 
                     trend?.type === 'negative' ? TrendingDown : Minus
    const trendColor = trend?.type === 'positive' ? 'text-green-600' : 
                      trend?.type === 'negative' ? 'text-red-600' : 'text-gray-600'

    return (
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">{title}</p>
              <p className="text-2xl font-bold">{formatValue(value)}</p>
              {trend && (
                <div className={`flex items-center mt-1 ${trendColor}`}>
                  {React.createElement(trendIcon, { className: "w-4 h-4 mr-1" })}
                  <span className="text-sm">{trend.value} {trend.period}</span>
                </div>
              )}
            </div>
            <div className="h-12 w-12 bg-blue-100 rounded-full flex items-center justify-center">
              {React.createElement(IconComponent, { className: "w-6 h-6 text-blue-600" })}
            </div>
          </div>
        </CardContent>
      </Card>
    )
  }

  // Helper function to format values
  const formatValue = (value: any) => {
    if (typeof value === 'number') {
      if (value > 1000000) {
        return `₹${(value / 1000000).toFixed(1)}M`
      } else if (value > 1000) {
        return `₹${(value / 1000).toFixed(0)}K`
      }
      return value.toLocaleString()
    }
    return value
  }

  // Helper function to render data tables
  const renderDataTable = (title: string, data: any[], columns: string[]) => (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr className="border-b">
                {columns.map(col => (
                  <th key={col} className="text-left p-2 font-medium text-gray-600">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.slice(0, 10).map((row, idx) => (
                <tr key={idx} className="border-b border-gray-100">
                  {columns.map(col => (
                    <td key={col} className="p-2">
                      {formatValue(row[col.toLowerCase().replace(' ', '_')] || row[col] || '-')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )

  // Helper function to render chart placeholders
  const renderChartPlaceholder = (title: string, type: string) => (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-64 bg-gray-50 rounded-lg flex items-center justify-center">
          <div className="text-center">
            {type === 'bar' && <BarChart3 className="w-12 h-12 text-gray-400 mx-auto mb-2" />}
            {type === 'pie' && <PieChart className="w-12 h-12 text-gray-400 mx-auto mb-2" />}
            {type === 'line' && <LineChart className="w-12 h-12 text-gray-400 mx-auto mb-2" />}
            <p className="text-gray-500">Chart visualization would be rendered here</p>
            <p className="text-sm text-gray-400">Integration with Chart.js or similar library</p>
          </div>
        </div>
      </CardContent>
    </Card>
  )

  // Render content based on report type
  const renderReportContent = () => {
    switch (reportType) {
      case 'executive-overview':
        return (
          <div className="space-y-6">
            {/* KPIs Grid */}
            {data.kpis && (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {renderKPICard("Total Leads", data.kpis.totalLeads, data.trends?.leadsGrowth, Users)}
                {renderKPICard("Total Deals", data.kpis.totalDeals, data.trends?.dealsGrowth, Target)}
                {renderKPICard("Pipeline Value", data.kpis.totalPipelineValue, data.trends?.pipelineGrowth, DollarSign)}
                {renderKPICard("Conversion Rate", `${data.kpis.conversionRate}%`, null, CheckCircle)}
              </div>
            )}
            
            {/* Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {renderChartPlaceholder("Pipeline Trends", "line")}
              {renderChartPlaceholder("Lead Sources", "pie")}
            </div>
          </div>
        )

      case 'sales-pipeline':
        return (
          <div className="space-y-6">
            {/* Pipeline Summary */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {renderKPICard("Total Pipeline Value", data.totalValue, null, DollarSign)}
              {renderKPICard("Total Deals", data.totalDeals, null, Target)}
              {renderKPICard("Average Deal Size", data.averageDealSize, null, TrendingUp)}
            </div>

            {/* Pipeline by Stage */}
            {data.pipelineByStage && (
              <Card>
                <CardHeader>
                  <CardTitle>Pipeline by Stage</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {Object.entries(data.pipelineByStage).map(([stage, values]: [string, any]) => (
                      <div key={stage} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                        <span className="font-medium">{stage}</span>
                        <div className="text-right">
                          <div className="font-semibold">{formatValue(values.value)}</div>
                          <div className="text-sm text-gray-600">{values.count} deals</div>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {renderChartPlaceholder("Pipeline Funnel", "bar")}
          </div>
        )

      case 'lead-conversion':
        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {renderKPICard("Total Leads", data.totalLeads, null, Users)}
              {renderKPICard("Conversion Rate", `${data.conversionRate}%`, null, Target)}
            </div>

            {/* Conversion by Source */}
            {data.conversionBySource && (
              <Card>
                <CardHeader>
                  <CardTitle>Conversion by Lead Source</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {Object.entries(data.conversionBySource).map(([source, values]: [string, any]) => (
                      <div key={source} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                        <span className="font-medium">{source}</span>
                        <div className="flex space-x-4 text-sm">
                          <span>Total: {values.total}</span>
                          <span>Qualified: {values.qualified}</span>
                          <span>Converted: {values.converted}</span>
                          <span className="font-semibold">
                            Rate: {values.total > 0 ? ((values.converted / values.total) * 100).toFixed(1) : 0}%
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {renderChartPlaceholder("Conversion Funnel", "bar")}
          </div>
        )

      default:
        // Generic report renderer
        return (
          <div className="space-y-6">
            {/* Try to render common data structures */}
            {data.kpis && (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {Object.entries(data.kpis).map(([key, value]) => 
                  renderKPICard(key.replace(/([A-Z])/g, ' $1').replace(/^./, str => str.toUpperCase()), value)
                )}
              </div>
            )}

            {/* Raw data display */}
            <Card>
              <CardHeader>
                <CardTitle>Report Data</CardTitle>
              </CardHeader>
              <CardContent>
                <pre className="text-sm bg-gray-50 p-4 rounded-lg overflow-x-auto">
                  {JSON.stringify(data, null, 2)}
                </pre>
              </CardContent>
            </Card>
          </div>
        )
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-6xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <div className="flex items-center justify-between">
            <div>
              <DialogTitle className="text-xl">{reportName}</DialogTitle>
              <DialogDescription>
                Generated on {new Date(reportData.generatedAt).toLocaleString()} • 
                Period: {reportData.period?.replace('-', ' ')} • 
                Type: {reportType}
              </DialogDescription>
            </div>
            <div className="flex space-x-2">
              <Button variant="outline" size="sm">
                <RefreshCw className="w-4 h-4 mr-2" />
                Refresh
              </Button>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline" size="sm">
                    <Download className="w-4 h-4 mr-2" />
                    Export
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onClick={() => onExport('pdf')}>
                    Export as PDF
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => onExport('excel')}>
                    Export as Excel
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => onExport('csv')}>
                    Export as CSV
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
              <Button variant="outline" size="sm">
                <Share2 className="w-4 h-4 mr-2" />
                Share
              </Button>
              <Button variant="outline" size="sm">
                <Printer className="w-4 h-4 mr-2" />
                Print
              </Button>
            </div>
          </div>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="details">Detailed Data</TabsTrigger>
            <TabsTrigger value="charts">Charts</TabsTrigger>
            <TabsTrigger value="insights">Insights</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="mt-4">
            {renderReportContent()}
          </TabsContent>

          <TabsContent value="details" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Raw Report Data</CardTitle>
                <CardDescription>Complete dataset for this report</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="max-h-96 overflow-y-auto">
                  <pre className="text-sm bg-gray-50 p-4 rounded-lg">
                    {JSON.stringify(data, null, 2)}
                  </pre>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="charts" className="mt-4">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {renderChartPlaceholder("Primary Chart", "bar")}
              {renderChartPlaceholder("Secondary Chart", "line")}
              {renderChartPlaceholder("Distribution Chart", "pie")}
              {renderChartPlaceholder("Trend Analysis", "line")}
            </div>
          </TabsContent>

          <TabsContent value="insights" className="mt-4">
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle>Key Insights</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex items-start space-x-3">
                      <TrendingUp className="w-5 h-5 text-green-600 mt-0.5" />
                      <div>
                        <p className="font-medium">Positive Trend Identified</p>
                        <p className="text-sm text-gray-600">Performance metrics show improvement over the selected period</p>
                      </div>
                    </div>
                    <div className="flex items-start space-x-3">
                      <Target className="w-5 h-5 text-blue-600 mt-0.5" />
                      <div>
                        <p className="font-medium">Goals Alignment</p>
                        <p className="text-sm text-gray-600">Current performance is aligned with quarterly targets</p>
                      </div>
                    </div>
                    <div className="flex items-start space-x-3">
                      <AlertTriangle className="w-5 h-5 text-orange-600 mt-0.5" />
                      <div>
                        <p className="font-medium">Areas for Improvement</p>
                        <p className="text-sm text-gray-600">Some metrics require attention to maintain growth trajectory</p>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Recommendations</CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-2">
                    <li className="flex items-start space-x-2">
                      <CheckCircle className="w-4 h-4 text-green-600 mt-0.5" />
                      <span className="text-sm">Continue current strategies for high-performing areas</span>
                    </li>
                    <li className="flex items-start space-x-2">
                      <CheckCircle className="w-4 h-4 text-green-600 mt-0.5" />
                      <span className="text-sm">Focus resources on underperforming segments</span>
                    </li>
                    <li className="flex items-start space-x-2">
                      <CheckCircle className="w-4 h-4 text-green-600 mt-0.5" />
                      <span className="text-sm">Schedule regular review cycles to track improvements</span>
                    </li>
                  </ul>
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  )
}