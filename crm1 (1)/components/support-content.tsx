"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Plus, Search, Download, Eye, Edit, AlertCircle, Clock, CheckCircle,
  Building, User, Package, Phone, Mail, Settings, Brain, TrendingUp, 
  AlertTriangle, Lightbulb, FileText, LinkIcon, ArrowRight, Target,
  BarChart3, Users, Timer, Award
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import { AddCaseModal } from "@/components/add-case-modal"
import { AddSolutionModal } from "@/components/add-solution-modal"
import { CaseDetailsModal } from "@/components/case-details-modal"

const caseStatuses = ["All", "Open", "In Progress", "Under Investigation", "Pending Customer", "Resolved", "Closed", "Escalated"]
const casePriorities = ["All", "Low", "Medium", "High", "Critical"]
const caseCategories = ["All", "Technical Support", "Delivery Issue", "Software Issue", "Hardware Issue", "Training", "Warranty", "Installation"]
const solutionStatuses = ["All", "Draft", "Under Review", "Approved", "Implemented", "Tested", "Verified", "Published"]

export function SupportContent() {
  const { toast } = useToast()
  
  // Cases State
  const [casesList, setCasesList] = useState<any[]>([])
  const [filteredCases, setFilteredCases] = useState<any[]>([])
  
  // Solutions State  
  const [solutionsList, setSolutionsList] = useState<any[]>([])
  const [filteredSolutions, setFilteredSolutions] = useState<any[]>([])
  
  // Related Data State
  const [complaintsData, setComplaintsData] = useState<any[]>([])
  const [installationsData, setInstallationsData] = useState<any[]>([])
  const [amcData, setAmcData] = useState<any[]>([])
  
  // UI State
  const [activeTab, setActiveTab] = useState("overview")
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedPriority, setSelectedPriority] = useState("All")
  const [selectedCategory, setSelectedCategory] = useState("All")
  const [selectedCaseForSolution, setSelectedCaseForSolution] = useState<any>(null)
  const [selectedCaseDetails, setSelectedCaseDetails] = useState<any>(null)
  
  // Modal States
  const [isAddCaseModalOpen, setIsAddCaseModalOpen] = useState(false)
  const [isAddSolutionModalOpen, setIsAddSolutionModalOpen] = useState(false)
  const [isCaseDetailsModalOpen, setIsCaseDetailsModalOpen] = useState(false)
  
  // Statistics State
  const [supportStats, setSupportStats] = useState({
    totalCases: 0,
    openCases: 0,
    resolvedCases: 0,
    avgResolutionTime: 0,
    totalSolutions: 0,
    reusableSolutions: 0,
    customerSatisfaction: 0,
    criticalCases: 0
  })

  // Load all data on component mount
  useEffect(() => {
    loadAllSupportData()
  }, [])

  // Filter data when search/filter changes
  useEffect(() => {
    filterCases()
    filterSolutions()
  }, [casesList, solutionsList, searchTerm, selectedStatus, selectedPriority, selectedCategory])

  const loadAllSupportData = async () => {
    try {
      // Load all support-related data in parallel
      const [casesRes, solutionsRes, complaintsRes, installationsRes, amcRes] = await Promise.all([
        fetch('/api/cases'),
        fetch('/api/solutions'), 
        fetch('/api/complaints'),
        fetch('/api/installations'),
        fetch('/api/amc')
      ])

      if (casesRes.ok) {
        const casesData = await casesRes.json()
        setCasesList(casesData.cases || [])
      }

      if (solutionsRes.ok) {
        const solutionsData = await solutionsRes.json()
        setSolutionsList(solutionsData.solutions || [])
      }

      if (complaintsRes.ok) {
        const complaintsData = await complaintsRes.json()
        setComplaintsData(complaintsData.complaints || [])
      }

      if (installationsRes.ok) {
        const installationsData = await installationsRes.json()
        setInstallationsData(installationsData.installations || [])
      }

      if (amcRes.ok) {
        const amcResData = await amcRes.json()
        setAmcData(amcResData.amcContracts || [])
      }

    } catch (error) {
      console.error('Error loading support data:', error)
      toast({
        title: "Error",
        description: "Failed to load support data",
        variant: "destructive"
      })
    }
  }

  // Calculate statistics from real data
  useEffect(() => {
    if (casesList.length > 0 || solutionsList.length > 0) {
      const openCases = casesList.filter(c => c.status === 'Open' || c.status === 'In Progress').length
      const resolvedCases = casesList.filter(c => c.status === 'Resolved' || c.status === 'Closed').length
      const criticalCases = casesList.filter(c => c.priority === 'Critical' || c.severity === 'Critical').length
      const reusableSolutions = solutionsList.filter(s => s.reusable === true).length

      setSupportStats({
        totalCases: casesList.length,
        openCases: openCases,
        resolvedCases: resolvedCases,
        avgResolutionTime: 2.5, // This could be calculated from actual data
        totalSolutions: solutionsList.length,
        reusableSolutions: reusableSolutions,
        customerSatisfaction: 4.2, // This could come from actual feedback
        criticalCases: criticalCases
      })
    }
  }, [casesList, solutionsList])

  const filterCases = () => {
    let filtered = casesList.filter(case_ => {
      const matchesSearch = searchTerm === "" || 
        case_.case_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        case_.title?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        case_.customer_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        case_.description?.toLowerCase().includes(searchTerm.toLowerCase())

      const matchesStatus = selectedStatus === "All" || case_.status === selectedStatus
      const matchesPriority = selectedPriority === "All" || case_.priority === selectedPriority
      const matchesCategory = selectedCategory === "All" || case_.case_category === selectedCategory

      return matchesSearch && matchesStatus && matchesPriority && matchesCategory
    })
    
    setFilteredCases(filtered)
  }

  const filterSolutions = () => {
    let filtered = solutionsList.filter(solution => {
      const matchesSearch = searchTerm === "" || 
        solution.solution_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        solution.title?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        solution.description?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        solution.case_number?.toLowerCase().includes(searchTerm.toLowerCase())

      return matchesSearch
    })
    
    setFilteredSolutions(filtered)
  }

  const handleSaveCase = async (caseData: any) => {
    try {
      const response = await fetch('/api/cases', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(caseData)
      })

      if (response.ok) {
        const result = await response.json()
        toast({
          title: "Success",
          description: `Case ${result.case.case_number} created successfully`
        })
        setIsAddCaseModalOpen(false)
        loadAllSupportData() // Reload to get updated data
      } else {
        throw new Error('Failed to create case')
      }
    } catch (error) {
      toast({
        title: "Error", 
        description: "Failed to create case",
        variant: "destructive"
      })
    }
  }

  const handleSaveSolution = async (solutionData: any) => {
    try {
      const response = await fetch('/api/solutions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(solutionData)
      })

      if (response.ok) {
        const result = await response.json()
        toast({
          title: "Success",
          description: `Solution ${result.solution.solution_number} created successfully`
        })
        setIsAddSolutionModalOpen(false)
        setSelectedCaseForSolution(null)
        loadAllSupportData() // Reload to get updated data
      } else {
        throw new Error('Failed to create solution')
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to create solution", 
        variant: "destructive"
      })
    }
  }

  const handleCreateSolutionFromCase = (case_: any) => {
    setSelectedCaseForSolution(case_)
    setIsAddSolutionModalOpen(true)
  }

  const handleViewCaseDetails = (case_: any) => {
    // Get related solutions for this case
    const relatedSolutions = solutionsList.filter(sol => 
      sol.case_number === case_.case_number || sol.case_id === case_.id
    )
    
    // Get related complaints if any
    const relatedComplaints = complaintsData.filter(comp =>
      comp.account_name === case_.customer_name || 
      comp.product_name === case_.product_name
    )

    setSelectedCaseDetails({
      ...case_,
      relatedSolutions,
      relatedComplaints
    })
    setIsCaseDetailsModalOpen(true)
  }

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "open": return "bg-blue-100 text-blue-800"
      case "in progress": return "bg-yellow-100 text-yellow-800"
      case "resolved": return "bg-green-100 text-green-800"
      case "closed": return "bg-gray-100 text-gray-800"
      case "escalated": return "bg-red-100 text-red-800"
      default: return "bg-gray-100 text-gray-800"
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority?.toLowerCase()) {
      case "low": return "bg-green-100 text-green-800"
      case "medium": return "bg-yellow-100 text-yellow-800"
      case "high": return "bg-orange-100 text-orange-800"
      case "critical": return "bg-red-100 text-red-800"
      default: return "bg-gray-100 text-gray-800"
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

  // Stats for enhanced cards
  const statsCards = [
    {
      title: "Total Cases",
      value: supportStats.totalCases.toString(),
      change: `${supportStats.openCases} open`,
      icon: AlertCircle,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Resolved Cases", 
      value: supportStats.resolvedCases.toString(),
      change: `${Math.round((supportStats.resolvedCases / Math.max(supportStats.totalCases, 1)) * 100)}% success rate`,
      icon: CheckCircle,
      iconColor: "text-green-600", 
      iconBg: "bg-green-100"
    },
    {
      title: "Solutions Available",
      value: supportStats.totalSolutions.toString(),
      change: `${supportStats.reusableSolutions} reusable`,
      icon: Lightbulb,
      iconColor: "text-purple-600",
      iconBg: "bg-purple-100"
    },
    {
      title: "Avg. Resolution Time",
      value: `${supportStats.avgResolutionTime}d`,
      change: "Days per case",
      icon: Timer,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Support Center</h1>
          <p className="text-gray-600">Unified case management and solution knowledge base</p>
        </div>
        <div className="flex space-x-2">
          <Button onClick={() => setIsAddCaseModalOpen(true)}>
            <Plus className="w-4 h-4 mr-2" />
            New Case & Solution
          </Button>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {statsCards.map((stat) => (
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

      {/* Main Content */}
      <div>

        {/* Filters */}
        <Card className="mt-6">
          <CardHeader>
            <CardTitle>Search & Filters</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center space-x-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
                <Input
                  placeholder="Search cases, solutions, customers..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
              <Select value={selectedStatus} onValueChange={setSelectedStatus}>
                <SelectTrigger className="w-48">
                  <SelectValue placeholder="All Statuses" />
                </SelectTrigger>
                <SelectContent>
                  {caseStatuses.map((status) => (
                    <SelectItem key={status} value={status}>{status}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={selectedPriority} onValueChange={setSelectedPriority}>
                <SelectTrigger className="w-48">
                  <SelectValue placeholder="All Priorities" />
                </SelectTrigger>
                <SelectContent>
                  {casePriorities.map((priority) => (
                    <SelectItem key={priority} value={priority}>{priority}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={selectedCategory} onValueChange={setSelectedCategory}>
                <SelectTrigger className="w-48">
                  <SelectValue placeholder="All Categories" />
                </SelectTrigger>
                <SelectContent>
                  {caseCategories.map((category) => (
                    <SelectItem key={category} value={category}>{category}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </CardContent>
        </Card>

        {/* Unified Cases & Solutions List */}
        <Card className="space-y-6">
          <CardHeader>
            <CardTitle>Cases and Solutions</CardTitle>
            <CardDescription>All support cases and solutions in one place</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {filteredCases.length === 0 ? (
                <div className="text-center py-12">
                  <AlertCircle className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                  <p className="text-gray-500">No cases and solutions found</p>
                </div>
              ) : (
                filteredCases.map((case_) => (
                  <div key={case_.id} className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50">
                    <div className="flex items-center space-x-4">
                      <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                        <AlertCircle className="w-5 h-5 text-blue-600" />
                      </div>
                      
                      <div>
                        <div className="flex items-center gap-2">
                          <h3 className="font-semibold">{case_.case_number}</h3>
                          <Badge className={getStatusColor(case_.status)}>{case_.status}</Badge>
                          <Badge className={getPriorityColor(case_.priority)}>{case_.priority}</Badge>
                        </div>
                        <p className="text-sm text-gray-600 mt-1">{case_.title}</p>
                        <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                          <span className="flex items-center gap-1">
                            <Building className="w-3 h-3" />
                            {case_.customer_name}
                          </span>
                          <span className="flex items-center gap-1">
                            <User className="w-3 h-3" />
                            {case_.assigned_to || 'Unassigned'}
                          </span>
                          <span className="flex items-center gap-1">
                            <Clock className="w-3 h-3" />
                            {formatDate(case_.case_date)}
                          </span>
                          {case_.product_name && (
                            <span className="flex items-center gap-1">
                              <Package className="w-3 h-3" />
                              {case_.product_name}
                            </span>
                          )}
                          {case_.solution && (
                            <span className="flex items-center gap-1">
                              <CheckCircle className="w-3 h-3 text-green-500" />
                              <span className="text-green-600">Solution Provided</span>
                            </span>
                          )}
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center space-x-2">
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        onClick={() => handleViewCaseDetails(case_)}
                        title="View Details"
                      >
                        <Eye className="w-4 h-4" />
                      </Button>
                    </div>
                  </div>
                ))
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Modals */}
      <AddCaseModal
        isOpen={isAddCaseModalOpen}
        onClose={() => setIsAddCaseModalOpen(false)}
        onSave={handleSaveCase}
        complaintsData={complaintsData}
        installationsData={installationsData}
        amcData={amcData}
      />

      <AddSolutionModal
        isOpen={isAddSolutionModalOpen}
        onClose={() => {
          setIsAddSolutionModalOpen(false)
          setSelectedCaseForSolution(null)
        }}
        onSave={handleSaveSolution}
        linkedCase={selectedCaseForSolution}
      />

      <CaseDetailsModal
        isOpen={isCaseDetailsModalOpen}
        onClose={() => {
          setIsCaseDetailsModalOpen(false)
          setSelectedCaseDetails(null)
        }}
        caseData={selectedCaseDetails}
        onCreateSolution={() => {
          setIsCaseDetailsModalOpen(false)
          handleCreateSolutionFromCase(selectedCaseDetails)
        }}
      />
    </div>
  )
}