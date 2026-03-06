"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Plus, Search, Download, Eye, Edit, AlertTriangle, Clock, Brain, 
  Building, User, Package, Phone, Mail, Settings, CheckCircle, TrendingUp
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"
import { AddComplaintModal } from "@/components/add-complaint-modal"


const complaintTypes = ["All", "No Warranty/AMC", "Under Warranty", "Under AMC"]
const severities = ["All", "Low", "Medium", "High", "Critical"]
const statuses = ["All", "New", "In Progress", "Resolved", "Closed", "Escalated"]

export function ComplaintsContent() {
  const { toast } = useToast()
  const [complaintsList, setComplaintsList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedType, setSelectedType] = useState("All")
  const [selectedSeverity, setSelectedSeverity] = useState("All")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedAssignedTo, setSelectedAssignedTo] = useState("All")
  const [complaintsStatsState, setComplaintsStatsState] = useState({
    total: 0,
    open: 0,
    avgResolutionTime: 0,
    resolutionRate: 0,
    newComplaints: 0
  })
  const [isAddModalOpen, setIsAddModalOpen] = useState(false)
  const [editingComplaint, setEditingComplaint] = useState(null)

  // Load complaints from API on component mount
  useEffect(() => {
    loadComplaints()
  }, [])

  const loadComplaints = async () => {
    try {
      const response = await fetch('/api/complaints')
      if (response.ok) {
        const data = await response.json()
        const complaints = data.complaints || []
        console.log("loadComplaints - complaints from API:", complaints)
        setComplaintsList(complaints)
        
        // Calculate stats
        const open = complaints.filter(comp => comp.status === 'New' || comp.status === 'In Progress' || comp.status === 'Acknowledged').length
        const resolved = complaints.filter(comp => comp.status === 'Resolved' || comp.status === 'Closed').length
        const newComplaints = complaints.filter(comp => comp.status === 'New').length
        
        setComplaintsStatsState({
          total: complaints.length,
          open: open,
          avgResolutionTime: 4.2, // Mock average
          resolutionRate: Math.round((resolved / Math.max(complaints.length, 1)) * 100),
          newComplaints: newComplaints
        })
      } else {
        console.error('Failed to fetch complaints')
        // Fallback to localStorage
        const storedComplaints = storageService.getAll<any>('complaints')
        setComplaintsList(storedComplaints)
      }
    } catch (error) {
      console.error('Error loading complaints:', error)
      // Fallback to localStorage
      const storedComplaints = storageService.getAll<any>('complaints')
      setComplaintsList(storedComplaints)
    }
  }

  const getSeverityColor = (severity: string) => {
    switch (severity.toLowerCase()) {
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

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "new":
        return "bg-blue-100 text-blue-800"
      case "acknowledged":
        return "bg-purple-100 text-purple-800"
      case "in progress":
        return "bg-yellow-100 text-yellow-800"
      case "resolved":
        return "bg-green-100 text-green-800"
      case "closed":
        return "bg-gray-100 text-gray-800"
      case "escalated":
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

  const handleCreateComplaint = async (complaintData: any) => {
    try {
      const response = await fetch('/api/complaints', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(complaintData),
      })

      if (response.ok) {
        const result = await response.json()
        console.log('Complaint created successfully:', result.complaint)
        
        toast({
          title: "Success",
          description: `Complaint ${result.complaint.complaint_number} created successfully`,
        })
        
        // Reload the complaints list
        await loadComplaints()
      } else {
        const error = await response.json()
        console.error('Failed to create complaint:', error)
        toast({
          title: "Error",
          description: error.error || "Failed to create complaint",
          variant: "destructive",
        })
      }
    } catch (error) {
      console.error('Error creating complaint:', error)
      toast({
        title: "Error",
        description: "Failed to create complaint. Please try again.",
        variant: "destructive",
      })
    }
  }

  const handleUpdateComplaint = async (complaintData: any) => {
    try {
      const response = await fetch('/api/complaints', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(complaintData),
      })

      if (response.ok) {
        const result = await response.json()
        console.log('Complaint updated successfully:', result.complaint)
        
        toast({
          title: "Success",
          description: `Complaint ${result.complaint.complaint_number} updated successfully`,
        })
        
        // Reload the complaints list
        await loadComplaints()
      } else {
        const error = await response.json()
        console.error('Failed to update complaint:', error)
        toast({
          title: "Error",
          description: error.error || "Failed to update complaint",
          variant: "destructive",
        })
      }
    } catch (error) {
      console.error('Error updating complaint:', error)
      toast({
        title: "Error",
        description: "Failed to update complaint. Please try again.",
        variant: "destructive",
      })
    }
  }

  const handleSaveComplaint = async (complaintData: any) => {
    if (editingComplaint) {
      await handleUpdateComplaint({ ...complaintData, id: editingComplaint.id })
    } else {
      await handleCreateComplaint(complaintData)
    }
    setIsAddModalOpen(false)
    setEditingComplaint(null)
  }

  const handleEditComplaint = (complaint: any) => {
    setEditingComplaint(complaint)
    setIsAddModalOpen(true)
  }

  const handleNewComplaint = () => {
    setEditingComplaint(null)
    setIsAddModalOpen(true)
  }

  // Filter complaints based on search and filters
  const filteredComplaints = complaintsList.filter(complaint => {
    const matchesSearch = searchTerm === "" || 
      complaint.complaint_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      complaint.account_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      complaint.product_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      complaint.contact_person?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesType = selectedType === "All" || complaint.complaint_type === selectedType
    const matchesSeverity = selectedSeverity === "All" || complaint.severity === selectedSeverity
    const matchesStatus = selectedStatus === "All" || complaint.status === selectedStatus
    const matchesAssigned = selectedAssignedTo === "All" || complaint.assigned_to === selectedAssignedTo

    return matchesSearch && matchesType && matchesSeverity && matchesStatus && matchesAssigned
  })

  // Get unique assigned users for filter
  const uniqueAssignedUsers = Array.from(
    new Set(complaintsList.map(complaint => complaint.assigned_to).filter(Boolean))
  )

  // Enhanced stats for the cards
  const stats = [
    {
      title: "Total Complaints",
      value: complaintsStatsState.total.toString(),
      change: `${complaintsStatsState.newComplaints} new`,
      icon: AlertTriangle,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Open Complaints",
      value: complaintsStatsState.open.toString(),
      change: "Need attention",
      icon: Clock,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    },
    {
      title: "Avg. Resolution Time",
      value: `${complaintsStatsState.avgResolutionTime}`,
      change: "Days",
      icon: TrendingUp,
      iconColor: "text-green-600",
      iconBg: "bg-green-100"
    },
    {
      title: "Resolution Rate",
      value: `${complaintsStatsState.resolutionRate}%`,
      change: "Success rate",
      icon: CheckCircle,
      iconColor: "text-purple-600",
      iconBg: "bg-purple-100"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Complaints Management</h1>
          <p className="text-gray-600">Track and resolve customer complaints efficiently</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button onClick={handleNewComplaint}>
            <Plus className="w-4 h-4 mr-2" />
            Log Complaint
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
          <CardTitle>Complaint Search & Filters</CardTitle>
          <CardDescription>Filter and search through complaints</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search complaints by number, account, product, or contact..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10"
              />
            </div>
            <Select value={selectedType} onValueChange={setSelectedType}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Types" />
              </SelectTrigger>
              <SelectContent>
                {complaintTypes.map((type) => (
                  <SelectItem key={type} value={type}>
                    {type}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedSeverity} onValueChange={setSelectedSeverity}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Severity" />
              </SelectTrigger>
              <SelectContent>
                {severities.map((severity) => (
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
                {statuses.map((status) => (
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

      {/* Complaints List - Card View */}
      <Card>
        <CardHeader>
          <CardTitle>Complaints List</CardTitle>
          <CardDescription>Showing {filteredComplaints.length} of {complaintsList.length} complaints</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredComplaints.length === 0 ? (
              <div className="text-center py-12">
                <AlertTriangle className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No complaints found</p>
                <p className="text-sm text-gray-400 mt-2">
                  {searchTerm || selectedType !== "All" || selectedSeverity !== "All" || selectedStatus !== "All" || selectedAssignedTo !== "All"
                    ? "Try adjusting your filters" 
                    : "Click 'Log Complaint' to create your first complaint"}
                </p>
              </div>
            ) : (
              filteredComplaints.map((complaint) => (
                <div
                  key={complaint.id}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center space-x-6">
                    <div className="w-12 h-12 bg-red-100 rounded-lg flex items-center justify-center">
                      <AlertTriangle className="w-6 h-6 text-red-600" />
                    </div>
                    
                    {/* Complaint Details */}
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="font-semibold text-gray-900">
                          {complaint.complaint_number}
                        </h3>
                        <Badge className={getStatusColor(complaint.status)}>
                          {complaint.status}
                        </Badge>
                        <Badge className={getSeverityColor(complaint.severity)}>
                          {complaint.severity}
                        </Badge>
                        <Badge variant="outline" className="text-blue-600 border-blue-200">
                          {complaint.complaint_type}
                        </Badge>
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Date: {formatDate(complaint.complaint_date)}
                        {complaint.resolution_date && ` | Resolved: ${formatDate(complaint.resolution_date)}`}
                      </p>
                    </div>
                    
                    {/* Account & Contact Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Building className="w-4 h-4 text-gray-400" />
                        <span className="font-medium">{complaint.account_name}</span>
                      </div>
                      {complaint.contact_person && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <User className="w-4 h-4 text-gray-400" />
                          <span>{complaint.contact_person}</span>
                        </div>
                      )}
                      {complaint.contact_phone && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Phone className="w-3 h-3 text-gray-400" />
                          <span>{complaint.contact_phone}</span>
                        </div>
                      )}
                    </div>
                    
                    {/* Product & Assignment Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Package className="w-4 h-4 text-gray-400" />
                        <span className="max-w-xs truncate" title={complaint.product_name}>
                          {complaint.product_name}
                        </span>
                      </div>
                      {complaint.assigned_to && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Settings className="w-4 h-4 text-gray-400" />
                          <span>Assigned: {complaint.assigned_to}</span>
                        </div>
                      )}
                      {complaint.subject && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <span className="max-w-xs truncate" title={complaint.subject}>
                            {complaint.subject}
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
                      title="View Complaint Details"
                    >
                      <Eye className="w-4 h-4" />
                    </Button>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="Edit Complaint"
                      onClick={() => handleEditComplaint(complaint)}
                    >
                      <Edit className="w-4 h-4" />
                    </Button>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="AI Resolution"
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

      {/* Add Complaint Modal */}
      <AddComplaintModal
        isOpen={isAddModalOpen}
        onClose={() => {
          setIsAddModalOpen(false)
          setEditingComplaint(null)
        }}
        onSave={handleSaveComplaint}
        editingComplaint={editingComplaint}
      />
    </div>
  )
}
