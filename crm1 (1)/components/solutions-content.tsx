"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Plus, Search, Download, Eye, Edit, Lightbulb, CheckCircle, Clock,
  Building, User, Package, Settings, TrendingUp, Brain
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"

const solutionCategories = ["All", "Bug Fix", "Configuration Change", "Hardware Replacement", "Software Update", "Process Improvement", "Training", "Documentation Update", "Preventive Measure", "Workaround", "Permanent Fix"]
const solutionStatuses = ["All", "Draft", "Under Review", "Approved", "Implemented", "Tested", "Verified", "Published", "Superseded", "Archived"]
const difficultyLevels = ["All", "Easy", "Medium", "Hard", "Expert"]
const effectivenessLevels = ["All", "Not Effective", "Partially Effective", "Effective", "Highly Effective"]

export function SolutionsContent() {
  const { toast } = useToast()
  const [solutionsList, setSolutionsList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedCategory, setSelectedCategory] = useState("All")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedDifficulty, setSelectedDifficulty] = useState("All")
  const [selectedEffectiveness, setSelectedEffectiveness] = useState("All")
  const [selectedImplementedBy, setSelectedImplementedBy] = useState("All")
  const [solutionsStatsState, setSolutionsStatsState] = useState({
    total: 0,
    implemented: 0,
    knowledgeBase: 0,
    reusable: 0,
    highEffectiveness: 0
  })

  // Load solutions from API on component mount
  useEffect(() => {
    loadSolutions()
  }, [])

  const loadSolutions = async () => {
    try {
      const response = await fetch('/api/solutions')
      if (response.ok) {
        const data = await response.json()
        const solutions = data.solutions || []
        console.log("loadSolutions - solutions from API:", solutions)
        setSolutionsList(solutions)
        
        // Calculate stats
        const implemented = solutions.filter(s => s.status === 'Implemented' || s.status === 'Verified' || s.status === 'Published').length
        const knowledgeBase = solutions.filter(s => s.knowledge_base_entry === true).length
        const reusable = solutions.filter(s => s.reusable === true).length
        const highEffectiveness = solutions.filter(s => s.effectiveness === 'Highly Effective' || s.effectiveness === 'Effective').length
        
        setSolutionsStatsState({
          total: solutions.length,
          implemented: implemented,
          knowledgeBase: knowledgeBase,
          reusable: reusable,
          highEffectiveness: highEffectiveness
        })
      } else {
        console.error('Failed to fetch solutions')
        // Fallback to localStorage
        const storedSolutions = storageService.getAll<any>('solutions')
        setSolutionsList(storedSolutions)
      }
    } catch (error) {
      console.error('Error loading solutions:', error)
      // Fallback to localStorage
      const storedSolutions = storageService.getAll<any>('solutions')
      setSolutionsList(storedSolutions)
    }
  }

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "draft":
        return "bg-gray-100 text-gray-800"
      case "under review":
        return "bg-blue-100 text-blue-800"
      case "approved":
        return "bg-purple-100 text-purple-800"
      case "implemented":
        return "bg-green-100 text-green-800"
      case "tested":
        return "bg-cyan-100 text-cyan-800"
      case "verified":
        return "bg-emerald-100 text-emerald-800"
      case "published":
        return "bg-green-100 text-green-800"
      case "superseded":
        return "bg-orange-100 text-orange-800"
      case "archived":
        return "bg-gray-100 text-gray-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getDifficultyColor = (difficulty: string) => {
    switch (difficulty?.toLowerCase()) {
      case "easy":
        return "bg-green-100 text-green-800"
      case "medium":
        return "bg-yellow-100 text-yellow-800"
      case "hard":
        return "bg-orange-100 text-orange-800"
      case "expert":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getEffectivenessColor = (effectiveness: string) => {
    switch (effectiveness?.toLowerCase()) {
      case "not effective":
        return "bg-red-100 text-red-800"
      case "partially effective":
        return "bg-yellow-100 text-yellow-800"
      case "effective":
        return "bg-green-100 text-green-800"
      case "highly effective":
        return "bg-emerald-100 text-emerald-800"
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

  // Filter solutions based on search and filters
  const filteredSolutions = solutionsList.filter(solution => {
    const matchesSearch = searchTerm === "" || 
      solution.solution_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      solution.title?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      solution.customer_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      solution.case_number?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesCategory = selectedCategory === "All" || solution.solution_type === selectedCategory
    const matchesStatus = selectedStatus === "All" || solution.status === selectedStatus
    const matchesDifficulty = selectedDifficulty === "All" || solution.difficulty_level === selectedDifficulty
    const matchesEffectiveness = selectedEffectiveness === "All" || solution.effectiveness === selectedEffectiveness
    const matchesImplementedBy = selectedImplementedBy === "All" || solution.implemented_by === selectedImplementedBy

    return matchesSearch && matchesCategory && matchesStatus && matchesDifficulty && matchesEffectiveness && matchesImplementedBy
  })

  // Get unique implemented by users for filter
  const uniqueImplementedByUsers = Array.from(
    new Set(solutionsList.map(solution => solution.implemented_by).filter(Boolean))
  )

  // Enhanced stats for the cards
  const stats = [
    {
      title: "Total Solutions",
      value: solutionsStatsState.total.toString(),
      change: "Knowledge base entries",
      icon: Lightbulb,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Implemented",
      value: solutionsStatsState.implemented.toString(),
      change: "Successfully deployed",
      icon: CheckCircle,
      iconColor: "text-green-600",
      iconBg: "bg-green-100"
    },
    {
      title: "Knowledge Base",
      value: solutionsStatsState.knowledgeBase.toString(),
      change: "Available for reference",
      icon: Brain,
      iconColor: "text-purple-600",
      iconBg: "bg-purple-100"
    },
    {
      title: "Reusable Solutions",
      value: solutionsStatsState.reusable.toString(),
      change: "Can be applied again",
      icon: TrendingUp,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Solutions Knowledge Base</h1>
          <p className="text-gray-600">Repository of solutions and resolutions for customer cases</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button>
            <Plus className="w-4 h-4 mr-2" />
            Add Solution
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
          <CardTitle>Solution Search & Filters</CardTitle>
          <CardDescription>Filter and search through solution knowledge base</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search solutions by number, title, customer, or case..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10"
              />
            </div>
            <Select value={selectedCategory} onValueChange={setSelectedCategory}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Types" />
              </SelectTrigger>
              <SelectContent>
                {solutionCategories.map((category) => (
                  <SelectItem key={category} value={category}>
                    {category}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedStatus} onValueChange={setSelectedStatus}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Status" />
              </SelectTrigger>
              <SelectContent>
                {solutionStatuses.map((status) => (
                  <SelectItem key={status} value={status}>
                    {status}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedDifficulty} onValueChange={setSelectedDifficulty}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Difficulty" />
              </SelectTrigger>
              <SelectContent>
                {difficultyLevels.map((difficulty) => (
                  <SelectItem key={difficulty} value={difficulty}>
                    {difficulty}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedImplementedBy} onValueChange={setSelectedImplementedBy}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Implemented By" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="All">All Implemented By</SelectItem>
                {uniqueImplementedByUsers.map(user => (
                  <SelectItem key={user} value={user}>{user}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Solutions List - Card View */}
      <Card>
        <CardHeader>
          <CardTitle>Solutions List</CardTitle>
          <CardDescription>Showing {filteredSolutions.length} of {solutionsList.length} solutions</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredSolutions.length === 0 ? (
              <div className="text-center py-12">
                <Lightbulb className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No solutions found</p>
                <p className="text-sm text-gray-400 mt-2">
                  {searchTerm || selectedCategory !== "All" || selectedStatus !== "All" || selectedDifficulty !== "All" || selectedImplementedBy !== "All"
                    ? "Try adjusting your filters" 
                    : "Click 'Add Solution' to create your first solution"}
                </p>
              </div>
            ) : (
              filteredSolutions.map((solution) => (
                <div
                  key={solution.id}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center space-x-6">
                    <div className="w-12 h-12 bg-green-100 rounded-lg flex items-center justify-center">
                      <Lightbulb className="w-6 h-6 text-green-600" />
                    </div>
                    
                    {/* Solution Details */}
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="font-semibold text-gray-900">
                          {solution.solution_number}
                        </h3>
                        <Badge className={getStatusColor(solution.status)}>
                          {solution.status}
                        </Badge>
                        <Badge className={getDifficultyColor(solution.difficulty_level)}>
                          {solution.difficulty_level}
                        </Badge>
                        <Badge className={getEffectivenessColor(solution.effectiveness)}>
                          {solution.effectiveness}
                        </Badge>
                        {solution.knowledge_base_entry && (
                          <Badge variant="outline" className="text-purple-600 border-purple-200">
                            KB Entry
                          </Badge>
                        )}
                        {solution.reusable && (
                          <Badge variant="outline" className="text-blue-600 border-blue-200">
                            Reusable
                          </Badge>
                        )}
                      </div>
                      <p className="text-sm font-medium text-gray-900 mt-1">{solution.title}</p>
                      <p className="text-sm text-gray-600 mt-1">
                        Date: {formatDate(solution.solution_date)}
                        {solution.case_number && ` | Case: ${solution.case_number}`}
                      </p>
                    </div>
                    
                    {/* Customer & Solution Type Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Building className="w-4 h-4 text-gray-400" />
                        <span className="font-medium">{solution.customer_name}</span>
                      </div>
                      <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                        <Settings className="w-4 h-4 text-gray-400" />
                        <span>{solution.solution_type}</span>
                      </div>
                      {solution.product_name && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Package className="w-4 h-4 text-gray-400" />
                          <span className="max-w-xs truncate" title={solution.product_name}>
                            {solution.product_name}
                          </span>
                        </div>
                      )}
                    </div>
                    
                    {/* Implementation Details */}
                    <div className="border-l pl-6">
                      {solution.implemented_by && (
                        <div className="flex items-center gap-1 text-sm">
                          <User className="w-4 h-4 text-gray-400" />
                          <span>By: {solution.implemented_by}</span>
                        </div>
                      )}
                      {solution.implementation_time_hours && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Clock className="w-4 h-4 text-gray-400" />
                          <span>Time: {solution.implementation_time_hours}h</span>
                        </div>
                      )}
                      {solution.customer_satisfaction && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <span>Rating: {solution.customer_satisfaction}/5 ⭐</span>
                        </div>
                      )}
                    </div>
                  </div>
                  
                  {/* Action Buttons */}
                  <div className="flex items-center space-x-1">
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="View Solution Details"
                    >
                      <Eye className="w-4 h-4" />
                    </Button>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="Edit Solution"
                    >
                      <Edit className="w-4 h-4" />
                    </Button>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="Solution Analysis"
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
