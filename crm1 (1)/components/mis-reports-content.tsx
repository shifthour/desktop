"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useToast } from "@/hooks/use-toast"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { ReportViewerModal } from "@/components/report-viewer-modal"
import { 
  BarChart3, TrendingUp, Users, Package, AlertTriangle, Calendar, Download, Eye, 
  DollarSign, Target, Clock, CheckCircle, UserCheck, Settings, FileText,
  PieChart, LineChart, Activity, Zap, Briefcase, HeadphonesIcon, Filter,
  RefreshCw, Share2, Mail, Printer, FileSpreadsheet, Search, Building, ChevronDown
} from "lucide-react"
import { 
  DropdownMenu, 
  DropdownMenuContent, 
  DropdownMenuItem, 
  DropdownMenuTrigger 
} from "@/components/ui/dropdown-menu"

// Define comprehensive report categories based on actual CRM data
const reportCategories = {
  executive: {
    title: "Executive Dashboard",
    description: "High-level KPIs and business metrics",
    icon: BarChart3,
    color: "bg-blue-600",
    reports: [
      {
        id: "executive-overview",
        name: "Executive Overview",
        description: "Complete business performance summary",
        type: "dashboard",
        frequency: "real-time",
        lastGenerated: "Live",
        estimatedTime: "< 1 min",
        kpis: ["Revenue", "Pipeline", "Customer Satisfaction", "Team Performance"]
      },
      {
        id: "monthly-business-review",
        name: "Monthly Business Review",
        description: "Comprehensive monthly performance analysis",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "3 days ago",
        estimatedTime: "5-10 min"
      },
      {
        id: "quarterly-trends",
        name: "Quarterly Trends Analysis",
        description: "Business trends and forecasting",
        type: "analytical",
        frequency: "quarterly",
        lastGenerated: "2 weeks ago",
        estimatedTime: "10-15 min"
      }
    ]
  },
  sales: {
    title: "Sales Performance",
    description: "Revenue, pipeline, and sales team analytics",
    icon: DollarSign,
    color: "bg-green-600",
    reports: [
      {
        id: "sales-pipeline",
        name: "Sales Pipeline Analysis",
        description: "Deal stages, velocity, and conversion rates",
        type: "analytical",
        frequency: "daily",
        lastGenerated: "2 hours ago",
        estimatedTime: "3-5 min"
      },
      {
        id: "lead-conversion",
        name: "Lead Conversion Report",
        description: "Lead sources, qualification, and conversion tracking",
        type: "analytical",
        frequency: "weekly",
        lastGenerated: "Yesterday",
        estimatedTime: "5-8 min"
      },
      {
        id: "quotation-analysis",
        name: "Quotation Analysis",
        description: "Quote-to-order conversion and pricing analysis",
        type: "analytical",
        frequency: "weekly",
        lastGenerated: "3 days ago",
        estimatedTime: "3-5 min"
      },
      {
        id: "sales-rep-performance",
        name: "Sales Rep Performance",
        description: "Individual and team performance metrics",
        type: "operational",
        frequency: "weekly",
        lastGenerated: "Yesterday",
        estimatedTime: "5-8 min"
      },
      {
        id: "revenue-forecast",
        name: "Revenue Forecasting",
        description: "Predictive revenue analysis based on pipeline",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "1 week ago",
        estimatedTime: "8-12 min"
      }
    ]
  },
  customer: {
    title: "Customer Analytics",
    description: "Account management and customer insights",
    icon: Users,
    color: "bg-purple-600",
    reports: [
      {
        id: "customer-acquisition",
        name: "Customer Acquisition Analysis",
        description: "New customer trends and acquisition costs",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "5 days ago",
        estimatedTime: "5-8 min"
      },
      {
        id: "account-health",
        name: "Account Health Dashboard",
        description: "Customer engagement and satisfaction metrics",
        type: "dashboard",
        frequency: "weekly",
        lastGenerated: "2 days ago",
        estimatedTime: "3-5 min"
      },
      {
        id: "customer-lifecycle",
        name: "Customer Lifecycle Analysis",
        description: "Customer journey and lifetime value analysis",
        type: "analytical",
        frequency: "quarterly",
        lastGenerated: "3 weeks ago",
        estimatedTime: "10-15 min"
      },
      {
        id: "top-customers",
        name: "Top Customers Report",
        description: "Revenue contribution and engagement analysis",
        type: "operational",
        frequency: "monthly",
        lastGenerated: "1 week ago",
        estimatedTime: "3-5 min"
      }
    ]
  },
  product: {
    title: "Product Performance",
    description: "Product analytics and inventory insights",
    icon: Package,
    color: "bg-orange-600",
    reports: [
      {
        id: "product-sales-analysis",
        name: "Product Sales Analysis",
        description: "Product-wise revenue and volume analysis",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "1 week ago",
        estimatedTime: "5-8 min"
      },
      {
        id: "category-performance",
        name: "Category Performance",
        description: "Product category trends and comparisons",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "1 week ago",
        estimatedTime: "5-8 min"
      },
      {
        id: "pricing-analysis",
        name: "Pricing & Margin Analysis",
        description: "Pricing optimization and profitability analysis",
        type: "analytical",
        frequency: "quarterly",
        lastGenerated: "2 weeks ago",
        estimatedTime: "8-12 min"
      },
      {
        id: "product-demand",
        name: "Product Demand Forecasting",
        description: "Demand patterns and inventory planning",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "1 week ago",
        estimatedTime: "8-10 min"
      }
    ]
  },
  service: {
    title: "Service Operations",
    description: "AMC, installations, and service analytics",
    icon: Settings,
    color: "bg-teal-600",
    reports: [
      {
        id: "amc-performance",
        name: "AMC Performance Dashboard",
        description: "Contract status, renewals, and service metrics",
        type: "dashboard",
        frequency: "weekly",
        lastGenerated: "Yesterday",
        estimatedTime: "3-5 min"
      },
      {
        id: "installation-tracking",
        name: "Installation Tracking Report",
        description: "Installation schedules, completion rates, and delays",
        type: "operational",
        frequency: "daily",
        lastGenerated: "4 hours ago",
        estimatedTime: "3-5 min"
      },
      {
        id: "service-efficiency",
        name: "Service Efficiency Analysis",
        description: "Response times, resolution rates, and SLA compliance",
        type: "analytical",
        frequency: "weekly",
        lastGenerated: "2 days ago",
        estimatedTime: "5-8 min"
      },
      {
        id: "contract-renewals",
        name: "Contract Renewal Forecast",
        description: "Upcoming renewals and revenue predictions",
        type: "operational",
        frequency: "monthly",
        lastGenerated: "1 week ago",
        estimatedTime: "5-8 min"
      }
    ]
  },
  support: {
    title: "Support Analytics",
    description: "Customer support and issue resolution metrics",
    icon: HeadphonesIcon,
    color: "bg-red-600",
    reports: [
      {
        id: "support-performance",
        name: "Support Performance Dashboard",
        description: "Ticket resolution, response times, and satisfaction",
        type: "dashboard",
        frequency: "daily",
        lastGenerated: "6 hours ago",
        estimatedTime: "2-3 min"
      },
      {
        id: "case-analysis",
        name: "Case & Complaint Analysis",
        description: "Issue patterns, root causes, and prevention",
        type: "analytical",
        frequency: "weekly",
        lastGenerated: "3 days ago",
        estimatedTime: "5-8 min"
      },
      {
        id: "knowledge-base",
        name: "Knowledge Base Analytics",
        description: "Solution effectiveness and knowledge utilization",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "2 weeks ago",
        estimatedTime: "5-8 min"
      },
      {
        id: "customer-satisfaction",
        name: "Customer Satisfaction Report",
        description: "Satisfaction trends and improvement areas",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "1 week ago",
        estimatedTime: "5-8 min"
      }
    ]
  },
  operational: {
    title: "Operational Reports",
    description: "Team productivity and system utilization",
    icon: Activity,
    color: "bg-indigo-600",
    reports: [
      {
        id: "activity-summary",
        name: "Activity Summary Report",
        description: "Team activities, follow-ups, and productivity metrics",
        type: "operational",
        frequency: "weekly",
        lastGenerated: "Yesterday",
        estimatedTime: "3-5 min"
      },
      {
        id: "user-adoption",
        name: "System Adoption Analysis",
        description: "User engagement and feature utilization",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "2 weeks ago",
        estimatedTime: "5-8 min"
      },
      {
        id: "document-usage",
        name: "Document Library Analytics",
        description: "Document access patterns and popular resources",
        type: "analytical",
        frequency: "monthly",
        lastGenerated: "1 week ago",
        estimatedTime: "3-5 min"
      },
      {
        id: "follow-up-efficiency",
        name: "Follow-up Efficiency Report",
        description: "Follow-up completion rates and effectiveness",
        type: "operational",
        frequency: "weekly",
        lastGenerated: "2 days ago",
        estimatedTime: "3-5 min"
      }
    ]
  }
}

// Executive dashboard KPIs
const executiveKPIs = [
  {
    title: "Total Pipeline Value",
    value: "₹2.4M",
    change: { value: "+18%", type: "positive" as const, period: "vs last month" },
    icon: Target,
    iconColor: "text-green-600",
    iconBg: "bg-green-100"
  },
  {
    title: "Active Customers",
    value: "247",
    change: { value: "+12", type: "positive" as const, period: "this month" },
    icon: Building,
    iconColor: "text-blue-600",
    iconBg: "bg-blue-100"
  },
  {
    title: "Open Support Cases",
    value: "23",
    change: { value: "-5", type: "positive" as const, period: "vs yesterday" },
    icon: AlertTriangle,
    iconColor: "text-orange-600",
    iconBg: "bg-orange-100"
  },
  {
    title: "AMC Renewals Due",
    value: "18",
    change: { value: "Next 30 days", type: "neutral" as const, period: "" },
    icon: Clock,
    iconColor: "text-purple-600",
    iconBg: "bg-purple-100"
  }
]

export function MISReportsContent() {
  const { toast } = useToast()
  const [selectedCategory, setSelectedCategory] = useState("executive")
  const [selectedPeriod, setSelectedPeriod] = useState("current-month")
  const [isGenerating, setIsGenerating] = useState(false)
  const [searchQuery, setSearchQuery] = useState("")
  const [showReportModal, setShowReportModal] = useState(false)
  const [currentReportData, setCurrentReportData] = useState(null)
  const [currentReportName, setCurrentReportName] = useState("")
  const [currentReportType, setCurrentReportType] = useState("")

  // Filter reports based on search
  const getFilteredReports = (reports: any[]) => {
    if (!searchQuery) return reports
    return reports.filter(report => 
      report.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      report.description.toLowerCase().includes(searchQuery.toLowerCase())
    )
  }

  // Generate report function
  const generateReport = async (reportId: string, reportName: string) => {
    setIsGenerating(true)
    try {
      const response = await fetch(`/api/reports?type=${reportId}&period=${selectedPeriod}&company_id=de19ccb7-e90d-4507-861d-a3aecf5e3f29`)
      
      if (!response.ok) {
        throw new Error('Failed to generate report')
      }

      const reportData = await response.json()
      
      toast({
        title: "Report Generated",
        description: `${reportName} has been generated successfully`,
      })

      // Open the report in the viewer modal
      setCurrentReportData(reportData)
      setCurrentReportName(reportName)
      setCurrentReportType(reportId)
      setShowReportModal(true)
      
    } catch (error) {
      console.error('Report generation error:', error)
      toast({
        title: "Error",
        description: "Failed to generate report. Please try again.",
        variant: "destructive"
      })
    } finally {
      setIsGenerating(false)
    }
  }

  // Export report function
  const exportReport = async (reportId: string, format: string) => {
    try {
      // First generate the report data
      const reportResponse = await fetch(`/api/reports?type=${reportId}&period=${selectedPeriod}&company_id=de19ccb7-e90d-4507-861d-a3aecf5e3f29`)
      
      if (!reportResponse.ok) {
        throw new Error('Failed to fetch report data')
      }

      const reportData = await reportResponse.json()

      // Then export it
      const exportResponse = await fetch('/api/reports/export', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          reportType: reportId,
          format: format,
          data: reportData.data,
          period: selectedPeriod
        })
      })

      if (!exportResponse.ok) {
        throw new Error('Failed to export report')
      }

      const exportResult = await exportResponse.json()

      toast({
        title: "Export Ready",
        description: `Report exported to ${format.toUpperCase()}. File: ${exportResult.fileName}`,
      })

      // Trigger download
      if (exportResult.downloadUrl) {
        const link = document.createElement('a')
        link.href = exportResult.downloadUrl
        link.download = exportResult.fileName
        document.body.appendChild(link)
        link.click()
        document.body.removeChild(link)
      }

      console.log('Export result:', exportResult)
      
    } catch (error) {
      console.error('Export error:', error)
      toast({
        title: "Export Failed",
        description: "Failed to export report. Please try again.",
        variant: "destructive"
      })
    }
  }

  const getStatusColor = (frequency: string) => {
    switch (frequency) {
      case "real-time":
      case "daily":
        return "bg-green-100 text-green-800"
      case "weekly":
        return "bg-blue-100 text-blue-800"
      case "monthly":
        return "bg-purple-100 text-purple-800"
      case "quarterly":
        return "bg-orange-100 text-orange-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getTypeIcon = (type: string) => {
    switch (type) {
      case "dashboard":
        return PieChart
      case "analytical":
        return LineChart
      case "operational":
        return BarChart3
      default:
        return FileText
    }
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">MIS Reports</h1>
          <p className="text-gray-600">Comprehensive business intelligence and analytics</p>
        </div>
        <div className="flex space-x-2">
          <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
            <SelectTrigger className="w-48">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="current-month">Current Month</SelectItem>
              <SelectItem value="last-month">Last Month</SelectItem>
              <SelectItem value="current-quarter">Current Quarter</SelectItem>
              <SelectItem value="last-quarter">Last Quarter</SelectItem>
              <SelectItem value="current-year">Current Year</SelectItem>
              <SelectItem value="custom">Custom Range</SelectItem>
            </SelectContent>
          </Select>
          <Button variant="outline">
            <Calendar className="w-4 h-4 mr-2" />
            Schedule Reports
          </Button>
          <Button>
            <Settings className="w-4 h-4 mr-2" />
            Report Builder
          </Button>
        </div>
      </div>

      {/* Executive KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {executiveKPIs.map((kpi) => (
          <EnhancedCard 
            key={kpi.title}
            title={kpi.title}
            value={kpi.value}
            change={kpi.change}
            icon={kpi.icon}
            iconColor={kpi.iconColor}
            iconBg={kpi.iconBg}
          />
        ))}
      </div>

      {/* Search and Filter */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-col md:flex-row gap-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <Input
                  placeholder="Search reports by name or description..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
            <Button variant="outline">
              <Filter className="w-4 h-4 mr-2" />
              Advanced Filters
            </Button>
            <Button variant="outline">
              <RefreshCw className="w-4 h-4 mr-2" />
              Refresh Data
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Report Categories Tabs */}
      <Tabs value={selectedCategory} onValueChange={setSelectedCategory}>
        <TabsList className="grid w-full grid-cols-7">
          {Object.entries(reportCategories).map(([key, category]) => (
            <TabsTrigger key={key} value={key} className="flex items-center">
              <category.icon className="w-4 h-4 mr-1" />
              <span className="hidden sm:inline">{category.title}</span>
            </TabsTrigger>
          ))}
        </TabsList>

        {Object.entries(reportCategories).map(([key, category]) => (
          <TabsContent key={key} value={key}>
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <div className={`p-3 rounded-lg ${category.color.replace('600', '100')} mr-4`}>
                      <category.icon className={`w-6 h-6 ${category.color.replace('bg-', 'text-')}`} />
                    </div>
                    <div>
                      <CardTitle>{category.title}</CardTitle>
                      <CardDescription>{category.description}</CardDescription>
                    </div>
                  </div>
                  <Badge variant="outline">{getFilteredReports(category.reports).length} reports</Badge>
                </div>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                  {getFilteredReports(category.reports).map((report) => {
                    const TypeIcon = getTypeIcon(report.type)
                    return (
                      <Card key={report.id} className="hover:shadow-md transition-shadow">
                        <CardContent className="p-4">
                          <div className="flex items-start justify-between mb-3">
                            <div className="flex items-center">
                              <TypeIcon className="w-5 h-5 text-gray-500 mr-3" />
                              <div>
                                <h4 className="font-semibold text-sm">{report.name}</h4>
                                <p className="text-xs text-gray-500 mt-1">{report.description}</p>
                              </div>
                            </div>
                            <Badge className={getStatusColor(report.frequency)}>
                              {report.frequency}
                            </Badge>
                          </div>
                          
                          <div className="flex items-center justify-between text-xs text-gray-500 mb-3">
                            <span>Last generated: {report.lastGenerated}</span>
                            <span>Est. time: {report.estimatedTime}</span>
                          </div>

                          {report.kpis && (
                            <div className="mb-3">
                              <p className="text-xs text-gray-500 mb-1">Key Metrics:</p>
                              <div className="flex flex-wrap gap-1">
                                {report.kpis.map((kpi) => (
                                  <Badge key={kpi} variant="outline" className="text-xs">
                                    {kpi}
                                  </Badge>
                                ))}
                              </div>
                            </div>
                          )}

                          <div className="flex items-center justify-between">
                            <div className="flex space-x-1">
                              <Button 
                                variant="ghost" 
                                size="sm" 
                                onClick={() => generateReport(report.id, report.name)}
                                disabled={isGenerating}
                                title="View Report"
                              >
                                <Eye className="w-4 h-4" />
                              </Button>
                              <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                  <Button variant="ghost" size="sm" title="Export Report">
                                    <Download className="w-4 h-4" />
                                    <ChevronDown className="w-3 h-3 ml-1" />
                                  </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end">
                                  <DropdownMenuItem onClick={() => exportReport(report.id, 'pdf')}>
                                    <FileText className="w-4 h-4 mr-2" />
                                    Export as PDF
                                  </DropdownMenuItem>
                                  <DropdownMenuItem onClick={() => exportReport(report.id, 'excel')}>
                                    <FileSpreadsheet className="w-4 h-4 mr-2" />
                                    Export as Excel
                                  </DropdownMenuItem>
                                  <DropdownMenuItem onClick={() => exportReport(report.id, 'csv')}>
                                    <FileText className="w-4 h-4 mr-2" />
                                    Export as CSV
                                  </DropdownMenuItem>
                                  <DropdownMenuItem onClick={() => exportReport(report.id, 'json')}>
                                    <FileText className="w-4 h-4 mr-2" />
                                    Export as JSON
                                  </DropdownMenuItem>
                                </DropdownMenuContent>
                              </DropdownMenu>
                              <Button variant="ghost" size="sm" title="Share Report">
                                <Share2 className="w-4 h-4" />
                              </Button>
                            </div>
                            <Button 
                              size="sm"
                              onClick={() => generateReport(report.id, report.name)}
                              disabled={isGenerating}
                            >
                              {isGenerating ? "Generating..." : "Generate"}
                            </Button>
                          </div>
                        </CardContent>
                      </Card>
                    )
                  })}
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        ))}
      </Tabs>

      {/* Quick Actions */}
      <Card>
        <CardHeader>
          <CardTitle>Quick Actions</CardTitle>
          <CardDescription>Common reporting tasks and utilities</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Button variant="outline" className="h-16 flex-col">
              <FileSpreadsheet className="w-6 h-6 mb-2" />
              <span className="text-sm">Export All Data</span>
            </Button>
            <Button variant="outline" className="h-16 flex-col">
              <Mail className="w-6 h-6 mb-2" />
              <span className="text-sm">Email Reports</span>
            </Button>
            <Button variant="outline" className="h-16 flex-col">
              <Calendar className="w-6 h-6 mb-2" />
              <span className="text-sm">Schedule Setup</span>
            </Button>
            <Button variant="outline" className="h-16 flex-col">
              <Settings className="w-6 h-6 mb-2" />
              <span className="text-sm">Report Templates</span>
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Report Viewer Modal */}
      <ReportViewerModal
        isOpen={showReportModal}
        onClose={() => setShowReportModal(false)}
        reportData={currentReportData}
        reportName={currentReportName}
        reportType={currentReportType}
        onExport={(format) => exportReport(currentReportType, format)}
      />
    </div>
  )
}