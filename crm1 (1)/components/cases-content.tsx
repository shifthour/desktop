"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Plus, Search, Download, Eye, Edit, AlertCircle, Clock, CheckCircle,
  Building, User, Package, Phone, Mail, Settings, Brain, TrendingUp, AlertTriangle
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"

const caseCategories = ["All", "Technical Support", "Delivery Issue", "Software Issue", "Hardware Issue", "Training", "Warranty", "Installation"]
const issueSeverities = ["All", "Low", "Medium", "High", "Critical"]
const caseStatuses = ["All", "Open", "In Progress", "Under Investigation", "Pending Customer", "Resolved", "Closed", "Escalated"]

export function CasesContent() {
  const { toast } = useToast()
  const [casesList, setCasesList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedCategory, setSelectedCategory] = useState("All")
  const [selectedSeverity, setSelectedSeverity] = useState("All")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedAssignedTo, setSelectedAssignedTo] = useState("All")
  const [casesStatsState, setCasesStatsState] = useState({
    total: 0,
    open: 0,
    resolved: 0,
    avgResolutionTime: 0,
    highPriority: 0
  })

  // Load cases from API on component mount
  useEffect(() => {
    loadCases()
  }, [])

  const loadCases = async () => {
    try {
      const response = await fetch('/api/cases')
      if (response.ok) {
        const data = await response.json()
        const cases = data.cases || []
        console.log("loadCases - cases from API:", cases)
        setCasesList(cases)
        
        // Calculate stats
        const open = cases.filter(c => c.status === 'Open' || c.status === 'In Progress' || c.status === 'Under Investigation').length
        const resolved = cases.filter(c => c.status === 'Resolved' || c.status === 'Closed').length
        const highPriority = cases.filter(c => c.priority === 'High' || c.priority === 'Critical').length
        
        setCasesStatsState({
          total: cases.length,
          open: open,
          resolved: resolved,
          avgResolutionTime: 2.4, // Mock average
          highPriority: highPriority
        })
      } else {
        console.error('Failed to fetch cases')
        // Fallback to localStorage
        const storedCases = storageService.getAll<any>('cases')
        setCasesList(storedCases)
      }
    } catch (error) {
      console.error('Error loading cases:', error)
      // Fallback to localStorage
      const storedCases = storageService.getAll<any>('cases')
      setCasesList(storedCases)
    }
  }

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "open":
        return "bg-red-100 text-red-800"
      case "in progress":
        return "bg-yellow-100 text-yellow-800"
      case "under investigation":
        return "bg-blue-100 text-blue-800"
      case "pending customer":
        return "bg-purple-100 text-purple-800"
      case "resolved":
        return "bg-green-100 text-green-800"
      case "closed":
        return "bg-gray-100 text-gray-800"
      case "escalated":
        return "bg-orange-100 text-orange-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getSeverityColor = (severity: string) => {
    switch (severity?.toLowerCase()) {
      case "low":
        return "bg-green-100 text-green-800"
      case "medium":
        return "bg-yellow-100 text-yellow-800"
      case "high":
        return "bg-orange-100 text-orange-800"
      case "critical":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const formatDate = (dateString: string) => {
    if (!dateString) return 'N/A'
    return new Date(dateString).toLocaleDateString('en-IN', {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    })
  }

  // Filter cases based on search and filters
  const filteredCases = casesList.filter(caseItem => {
    const matchesSearch = searchTerm === "" || 
      caseItem.case_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      caseItem.title?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      caseItem.customer_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      caseItem.contact_person?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesCategory = selectedCategory === "All" || caseItem.case_category === selectedCategory
    const matchesSeverity = selectedSeverity === "All" || caseItem.severity === selectedSeverity
    const matchesStatus = selectedStatus === "All" || caseItem.status === selectedStatus
    const matchesAssigned = selectedAssignedTo === "All" || caseItem.assigned_to === selectedAssignedTo

    return matchesSearch && matchesCategory && matchesSeverity && matchesStatus && matchesAssigned
  })

  // Get unique assigned users for filter
  const uniqueAssignedUsers = Array.from(
    new Set(casesList.map(caseItem => caseItem.assigned_to).filter(Boolean))
  )

  // Enhanced stats for the cards
  const stats = [
    {
      title: "Total Cases",
      value: casesStatsState.total.toString(),
      change: "All reported cases",
      icon: AlertTriangle,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Open Cases",
      value: casesStatsState.open.toString(),
      change: "Need attention",
      icon: AlertCircle,
      iconColor: "text-red-600",
      iconBg: "bg-red-100"
    },
    {
      title: "Resolved Cases",
      value: casesStatsState.resolved.toString(),
      change: "Successfully resolved",
      icon: CheckCircle,
      iconColor: "text-green-600",
      iconBg: "bg-green-100"
    },
    {
      title: "High Priority",
      value: casesStatsState.highPriority.toString(),
      change: "Critical & High priority",
      icon: TrendingUp,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Cases Management</h1>
          <p className="text-gray-600">Track and manage customer support cases and issues</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button>
            <Plus className="w-4 h-4 mr-2" />
            Log Case
          </Button>
        </div>
      </div>

      {/* Enhanced Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stats.map((stat) => (
          <EnhancedCard 
            key={stat.title} 
            title={stat.title} 
            value={stat.value} 
            change={stat.change} 
            icon={stat.icon}
            iconColor={stat.iconColor}
            iconBg={stat.iconBg}
          />
        ))}
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Case Search & Filters</CardTitle>
          <CardDescription>Filter and search through support cases</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search cases by number, title, customer, or contact..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10"
              />
            </div>
            <Select value={selectedCategory} onValueChange={setSelectedCategory}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Categories" />
              </SelectTrigger>
              <SelectContent>
                {caseCategories.map((category) => (
                  <SelectItem key={category} value={category}>
                    {category}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedSeverity} onValueChange={setSelectedSeverity}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Severity" />
              </SelectTrigger>
              <SelectContent>
                {issueSeverities.map((severity) => (
                  <SelectItem key={severity} value={severity}>
                    {severity}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedStatus} onValueChange={setSelectedStatus}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Status" />
              </SelectTrigger>
              <SelectContent>
                {caseStatuses.map((status) => (
                  <SelectItem key={status} value={status}>
                    {status}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedAssignedTo} onValueChange={setSelectedAssignedTo}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Assigned" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="All">All Assigned</SelectItem>
                {uniqueAssignedUsers.map(user => (
                  <SelectItem key={user} value={user}>{user}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Cases List - Card View */}
      <Card>
        <CardHeader>
          <CardTitle>Cases List</CardTitle>
          <CardDescription>Showing {filteredCases.length} of {casesList.length} cases</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredCases.length === 0 ? (
              <div className="text-center py-12">
                <AlertTriangle className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No cases found</p>
                <p className="text-sm text-gray-400 mt-2">
                  {searchTerm || selectedCategory !== "All" || selectedSeverity !== "All" || selectedStatus !== "All" || selectedAssignedTo !== "All"
                    ? "Try adjusting your filters" 
                    : "Click 'Log Case' to create your first case"}
                </p>
              </div>
            ) : (
              filteredCases.map((caseItem) => (
                <div
                  key={caseItem.id}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center space-x-6">
                    <div className="w-12 h-12 bg-red-100 rounded-lg flex items-center justify-center">
                      <AlertTriangle className="w-6 h-6 text-red-600" />
                    </div>
                    
                    {/* Case Details */}
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="font-semibold text-gray-900">
                          {caseItem.case_number}
                        </h3>
                        <Badge className={getStatusColor(caseItem.status)}>
                          {caseItem.status}
                        </Badge>
                        <Badge className={getSeverityColor(caseItem.severity)}>
                          {caseItem.severity}
                        </Badge>
                        <Badge variant="outline" className="text-blue-600 border-blue-200">
                          {caseItem.case_category}
                        </Badge>
                      </div>
                      <p className="text-sm font-medium text-gray-900 mt-1">{caseItem.title}</p>
                      <p className="text-sm text-gray-600 mt-1">
                        Date: {formatDate(caseItem.case_date)}
                      </p>
                    </div>
                    
                    {/* Customer & Contact Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Building className="w-4 h-4 text-gray-400" />
                        <span className="font-medium">{caseItem.customer_name}</span>
                      </div>
                      {caseItem.contact_person && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <User className="w-4 h-4 text-gray-400" />
                          <span>{caseItem.contact_person}</span>
                        </div>
                      )}
                      {caseItem.contact_phone && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Phone className="w-3 h-3 text-gray-400" />
                          <span>{caseItem.contact_phone}</span>
                        </div>
                      )}
                    </div>
                    
                    {/* Product & Assignment Details */}
                    <div className="border-l pl-6">
                      {caseItem.product_name && (
                        <div className="flex items-center gap-1 text-sm">
                          <Package className="w-4 h-4 text-gray-400" />
                          <span className="max-w-xs truncate" title={caseItem.product_name}>
                            {caseItem.product_name}
                          </span>
                        </div>
                      )}
                      {caseItem.assigned_to && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Settings className="w-4 h-4 text-gray-400" />
                          <span>Assigned: {caseItem.assigned_to}</span>
                        </div>
                      )}
                      {caseItem.issue_type && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <span className="max-w-xs truncate" title={caseItem.issue_type}>
                            Type: {caseItem.issue_type}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                  
                  {/* Action Buttons */}
                  <div className="flex items-center space-x-1">
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="View Case Details"
                    >
                      <Eye className="w-4 h-4" />
                    </Button>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="Edit Case"
                    >
                      <Edit className="w-4 h-4" />
                    </Button>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="AI Analysis"
                      className="text-purple-600 hover:text-purple-800 hover:bg-purple-50"
                    >
                      <Brain className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

