"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Plus, Search, Download, Eye, Edit, Calendar, Shield, Building, User, 
  Package, IndianRupee, Clock, AlertTriangle, CheckCircle, TrendingUp, FileText
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"
import { AddAMCModal } from "@/components/add-amc-modal"
import { AddComplaintModal } from "@/components/add-complaint-modal"


const statuses = ["All", "Active", "Expired", "Pending Renewal", "Cancelled", "Draft"]

export function AMCContent() {
  const { toast } = useToast()
  const [amcContractsList, setAmcContractsList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedAssignedTo, setSelectedAssignedTo] = useState("All")
  const [amcStatsState, setAmcStatsState] = useState({
    total: 0,
    active: 0,
    renewalDue: 0,
    revenue: 0,
    expired: 0
  })
  const [isAddModalOpen, setIsAddModalOpen] = useState(false)
  const [editingAMC, setEditingAMC] = useState(null)
  const [isAddComplaintModalOpen, setIsAddComplaintModalOpen] = useState(false)
  const [sourceAMC, setSourceAMC] = useState<any>(null)

  // Load AMC contracts from API on component mount
  useEffect(() => {
    loadAmcContracts()
  }, [])

  const loadAmcContracts = async () => {
    try {
      const response = await fetch('/api/amc')
      if (response.ok) {
        const data = await response.json()
        const contracts = data.amcContracts || []
        console.log("loadAmcContracts - contracts from API:", contracts)
        setAmcContractsList(contracts)
        
        // Calculate stats
        const active = contracts.filter(amc => amc.status === 'Active').length
        const expired = contracts.filter(amc => amc.status === 'Expired').length
        const renewalDue = contracts.filter(amc => isRenewalDue(amc.contract_end_date)).length
        const totalRevenue = contracts.reduce((sum, amc) => sum + (amc.contract_value || 0), 0)
        
        setAmcStatsState({
          total: contracts.length,
          active: active,
          renewalDue: renewalDue,
          revenue: totalRevenue,
          expired: expired
        })
      } else {
        console.error('Failed to fetch AMC contracts')
        // Fallback to localStorage
        const storedContracts = storageService.getAll<any>('amc_contracts')
        setAmcContractsList(storedContracts)
      }
    } catch (error) {
      console.error('Error loading AMC contracts:', error)
      // Fallback to localStorage
      const storedContracts = storageService.getAll<any>('amc_contracts')
      setAmcContractsList(storedContracts)
    }
  }

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "active":
        return "bg-green-100 text-green-800"
      case "expired":
        return "bg-red-100 text-red-800"
      case "pending renewal":
        return "bg-yellow-100 text-yellow-800"
      case "cancelled":
        return "bg-gray-100 text-gray-800"
      case "draft":
        return "bg-blue-100 text-blue-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const isRenewalDue = (renewalDate: string) => {
    if (!renewalDate) return false
    const renewal = new Date(renewalDate)
    const today = new Date()
    const daysUntilRenewal = Math.ceil((renewal.getTime() - today.getTime()) / (1000 * 3600 * 24))
    return daysUntilRenewal <= 30 && daysUntilRenewal > 0
  }

  const formatDate = (dateString: string) => {
    if (!dateString) return 'N/A'
    return new Date(dateString).toLocaleDateString('en-IN', {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    })
  }

  const formatCurrency = (amount: number) => {
    if (!amount) return '₹0'
    if (amount >= 100000) {
      return `₹${(amount / 100000).toFixed(1)}L`
    }
    return `₹${amount.toLocaleString()}`
  }

  const handleCreateAMC = async (amcData: any) => {
    try {
      const response = await fetch('/api/amc', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(amcData),
      })

      if (response.ok) {
        const result = await response.json()
        console.log('AMC created successfully:', result.amcContract)
        
        toast({
          title: "Success",
          description: `AMC contract ${result.amcContract.amc_number} created successfully`,
        })
        
        // Reload the contracts list
        await loadAmcContracts()
      } else {
        // Try to parse error response and extract detailed information
        let errorMessage = "Failed to create AMC contract"
        let errorDetails = ""

        try {
          const errorData = await response.json()
          console.error('API Error Response:', errorData)

          // Extract error message and details
          if (errorData.error) {
            errorMessage = errorData.error
          }
          if (errorData.details) {
            errorDetails = errorData.details
          }
          if (errorData.hint) {
            errorDetails += (errorDetails ? ' | ' : '') + errorData.hint
          }
        } catch (parseError) {
          // If JSON parsing fails, try to get text
          const errorText = await response.text()
          console.error('API Error Text:', errorText)
          errorMessage = errorText || errorMessage
        }

        toast({
          title: "Failed to create AMC",
          description: errorDetails ? `${errorMessage}: ${errorDetails}` : errorMessage,
          variant: "destructive",
        })
      }
    } catch (error) {
      console.error('Error creating AMC:', error)
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred'
      toast({
        title: "Error",
        description: `Failed to create AMC contract: ${errorMessage}`,
        variant: "destructive",
      })
    }
  }

  const handleUpdateAMC = async (amcData: any) => {
    try {
      const response = await fetch('/api/amc', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(amcData),
      })

      if (response.ok) {
        const result = await response.json()
        console.log('AMC updated successfully:', result.amcContract)
        
        toast({
          title: "Success",
          description: `AMC contract ${result.amcContract.amc_number} updated successfully`,
        })
        
        // Reload the contracts list
        await loadAmcContracts()
      } else {
        // Try to parse error response and extract detailed information
        let errorMessage = "Failed to update AMC contract"
        let errorDetails = ""

        try {
          const errorData = await response.json()
          console.error('API Error Response:', errorData)

          // Extract error message and details
          if (errorData.error) {
            errorMessage = errorData.error
          }
          if (errorData.details) {
            errorDetails = errorData.details
          }
          if (errorData.hint) {
            errorDetails += (errorDetails ? ' | ' : '') + errorData.hint
          }
        } catch (parseError) {
          // If JSON parsing fails, try to get text
          const errorText = await response.text()
          console.error('API Error Text:', errorText)
          errorMessage = errorText || errorMessage
        }

        toast({
          title: "Failed to update AMC",
          description: errorDetails ? `${errorMessage}: ${errorDetails}` : errorMessage,
          variant: "destructive",
        })
      }
    } catch (error) {
      console.error('Error updating AMC:', error)
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred'
      toast({
        title: "Error",
        description: `Failed to update AMC contract: ${errorMessage}`,
        variant: "destructive",
      })
    }
  }

  const handleSaveAMC = async (amcData: any) => {
    if (editingAMC) {
      await handleUpdateAMC({ ...amcData, id: editingAMC.id })
    } else {
      await handleCreateAMC(amcData)
    }
    setIsAddModalOpen(false)
    setEditingAMC(null)
  }

  const handleEditAMC = (amc: any) => {
    setEditingAMC(amc)
    setIsAddModalOpen(true)
  }

  const handleNewAMC = () => {
    setEditingAMC(null)
    setIsAddModalOpen(true)
  }

  const handleCreateComplaintFromAMC = (amc: any) => {
    console.log('Creating complaint from AMC:', amc)
    setSourceAMC(amc)
    setIsAddComplaintModalOpen(true)
  }

  const handleSaveComplaint = async (complaintData: any) => {
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
        
        setIsAddComplaintModalOpen(false)
        setSourceAMC(null)
      } else {
        // Try to parse error response and extract detailed information
        let errorMessage = "Failed to create complaint"
        let errorDetails = ""

        try {
          const errorData = await response.json()
          console.error('API Error Response:', errorData)

          // Extract error message and details
          if (errorData.error) {
            errorMessage = errorData.error
          }
          if (errorData.details) {
            errorDetails = errorData.details
          }
          if (errorData.hint) {
            errorDetails += (errorDetails ? ' | ' : '') + errorData.hint
          }
        } catch (parseError) {
          // If JSON parsing fails, try to get text
          const errorText = await response.text()
          console.error('API Error Text:', errorText)
          errorMessage = errorText || errorMessage
        }

        toast({
          title: "Failed to create complaint",
          description: errorDetails ? `${errorMessage}: ${errorDetails}` : errorMessage,
          variant: "destructive",
        })
      }
    } catch (error) {
      console.error('Error creating complaint:', error)
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred'
      toast({
        title: "Error",
        description: `Failed to create complaint: ${errorMessage}`,
        variant: "destructive",
      })
    }
  }

  const handleViewAMCPDF = async (contract: any) => {
    try {
      console.log('=== STARTING AMC PDF GENERATION ===')
      console.log('AMC Contract:', contract)
      console.log('Contract keys:', Object.keys(contract))
      console.log('customer_email:', contract.customer_email)
      console.log('customer_phone:', contract.customer_phone)
      console.log('product_name:', contract.product_name)
      console.log('equipment_details:', contract.equipment_details)
      
      // Generate PDF for AMC contract
      const printWindow = window.open('', '_blank')
      if (printWindow) {
        // Extract equipment/product information from contract
        let itemsData = []
        
        // Try to get installation details if this AMC was created from an installation
        let sourceInstallation = null
        if (contract.installation_id || contract.installation_number) {
          try {
            const installationsResponse = await fetch('/api/installations')
            if (installationsResponse.ok) {
              const installationsData = await installationsResponse.json()
              const installations = installationsData.installations || []
              sourceInstallation = installations.find((inst: any) => 
                inst.id === contract.installation_id || 
                inst.installation_number === contract.installation_number
              )
              console.log('Found source installation:', sourceInstallation)
            }
          } catch (error) {
            console.log('Could not fetch installation data:', error)
          }
        }

        // Check if there are equipment_details (JSON array)
        if (contract.equipment_details && Array.isArray(contract.equipment_details)) {
          itemsData = contract.equipment_details.map((item: any, index: number) => ({
            product: item.name || item.product_name || contract.product_name || 'AMC Service',
            description: item.description || `Annual maintenance contract for ${item.name || 'equipment'}`,
            quantity: item.quantity || 1,
            unitPrice: contract.contract_value || 0,
            discount: 0,
            amount: contract.contract_value || 0
          }))
        } else {
          // Try to get product info from source installation
          const productName = contract.product_name || 
                              sourceInstallation?.product_name || 
                              'Laboratory Equipment'
          
          itemsData = [{
            product: productName,
            description: `Annual maintenance contract for ${productName}`,
            quantity: 1,
            unitPrice: contract.contract_value || 0,
            discount: 0,
            amount: contract.contract_value || 0
          }]
        }

        console.log('AMC PDF itemsData:', itemsData)
        
        const html = `
          <!DOCTYPE html>
          <html>
          <head>
            <title>AMC Contract: ${contract.amc_number}</title>
            <style>
              @page { size: A4; margin: 20mm; }
              body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
              .header { border-bottom: 2px solid #16a34a; padding-bottom: 20px; margin-bottom: 30px; }
              .company-name { font-size: 24px; font-weight: bold; color: #15803d; }
              .contract-title { text-align: center; font-size: 28px; color: #15803d; margin: 30px 0; font-weight: bold; }
              .info-section { margin-bottom: 20px; }
              .field { margin: 8px 0; }
              .field label { font-weight: bold; display: inline-block; width: 150px; }
              .customer-section { background: #f0fdf4; padding: 15px; border-radius: 8px; margin-bottom: 30px; }
              .contract-section { background: #f8fafc; padding: 15px; border-radius: 8px; margin-bottom: 30px; }
              .items-table { width: 100%; border-collapse: collapse; margin-bottom: 30px; }
              .items-table th { background: #16a34a; color: white; padding: 10px; text-align: left; }
              .items-table td { padding: 10px; border-bottom: 1px solid #e5e7eb; }
              .totals-section { text-align: right; margin-top: 20px; }
              .total-row { display: flex; justify-content: flex-end; margin: 5px 0; }
              .total-label { width: 150px; text-align: right; margin-right: 20px; }
              .total-value { width: 150px; text-align: right; }
              .grand-total { font-size: 18px; font-weight: bold; color: #15803d; border-top: 2px solid #16a34a; padding-top: 10px; }
              .footer { margin-top: 30px; text-align: center; color: #666; font-size: 12px; }
              .service-details { margin-top: 20px; }
            </style>
          </head>
          <body>
            <div class="header">
              <div class="company-name">LabGigs CRM System</div>
            </div>
            
            <div class="contract-title">Annual Maintenance Contract</div>
            
            <div class="info-section">
              <div class="field">
                <label>AMC Number:</label> ${contract.amc_number || 'N/A'}
              </div>
              <div class="field">
                <label>Contract Date:</label> ${formatDate(contract.contract_start_date)}
              </div>
            </div>
            
            <div class="customer-section">
              <h3>Customer Information</h3>
              <div class="field">
                <label>Customer Name:</label> ${contract.customer_name || contract.account_name || sourceInstallation?.account_name || 'N/A'}
              </div>
              <div class="field">
                <label>Contact Person:</label> ${contract.contact_person || sourceInstallation?.contact_person || 'N/A'}
              </div>
              <div class="field">
                <label>Email:</label> ${contract.customer_email || sourceInstallation?.contact_email || 'N/A'}
              </div>
              <div class="field">
                <label>Phone:</label> ${contract.customer_phone || sourceInstallation?.contact_phone || 'N/A'}
              </div>
              <div class="field">
                <label>Service Address:</label> ${contract.service_address || sourceInstallation?.installation_address || 'N/A'}
              </div>
            </div>
            
            <div class="contract-section">
              <h3>Contract Details</h3>
              <div class="field">
                <label>Contract Period:</label> ${formatDate(contract.contract_start_date)} to ${formatDate(contract.contract_end_date)}
              </div>
              <div class="field">
                <label>Service Frequency:</label> ${contract.service_frequency || 'Annual'}
              </div>
              <div class="field">
                <label>Contract Type:</label> ${contract.contract_type || 'Comprehensive'}
              </div>
              <div class="field">
                <label>Status:</label> ${contract.status || 'Active'}
              </div>
            </div>
            
            <table class="items-table">
              <thead>
                <tr>
                  <th style="width: 5%;">S.No</th>
                  <th style="width: 35%;">Equipment/Service</th>
                  <th style="width: 35%;">Description</th>
                  <th style="width: 8%;">Qty</th>
                  <th style="width: 17%;">Annual Amount</th>
                </tr>
              </thead>
              <tbody>
                ${itemsData.map((item, index) => `
                  <tr>
                    <td>${index + 1}</td>
                    <td>${item.product}</td>
                    <td>${item.description}</td>
                    <td style="text-align: center;">${item.quantity}</td>
                    <td style="text-align: right;">₹${(item.amount || 0).toLocaleString()}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
            
            <div class="totals-section">
              <div class="total-row grand-total">
                <span class="total-label">Total Contract Value:</span>
                <span class="total-value">₹${(contract.contract_value || 0).toLocaleString()}</span>
              </div>
            </div>
            
            <div class="service-details">
              <h3>Service Terms</h3>
              <p><strong>Service Frequency:</strong> ${contract.service_frequency || 'Annual'} maintenance visits</p>
              <p><strong>Response Time:</strong> 24-48 hours for service calls</p>
              <p><strong>Coverage:</strong> ${contract.contract_type || 'Comprehensive'} maintenance including preventive and corrective services</p>
              <p><strong>Renewal:</strong> Contract renewal required before ${formatDate(contract.contract_end_date)}</p>
            </div>
            
            <div class="footer">
              <p><small>Generated on: ${new Date().toLocaleString()}</small></p>
              <p><small>This is a system generated AMC contract document</small></p>
            </div>
          </body>
          </html>
        `
        
        printWindow.document.write(html)
        printWindow.document.close()
        printWindow.focus()
        setTimeout(() => {
          printWindow.print()
        }, 250)
      }
    } catch (error) {
      console.error('Error generating AMC PDF:', error)
      toast({
        title: "Error",
        description: "Failed to generate AMC PDF. Please try again.",
        variant: "destructive"
      })
    }
  }

  // Filter AMC contracts based on search and filters
  const filteredAmcContracts = amcContractsList.filter(contract => {
    const matchesSearch = searchTerm === "" || 
      contract.amc_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (contract.customer_name || contract.account_name)?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      contract.product_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      contract.serial_number?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesStatus = selectedStatus === "All" || contract.status === selectedStatus
    const matchesAssigned = selectedAssignedTo === "All" || contract.assigned_to === selectedAssignedTo

    return matchesSearch && matchesStatus && matchesAssigned
  })

  // Get unique assigned users for filter
  const uniqueAssignedUsers = Array.from(
    new Set(amcContractsList.map(contract => contract.assigned_to).filter(Boolean))
  )

  // Enhanced stats for the cards
  const stats = [
    {
      title: "Total AMCs",
      value: amcStatsState.total.toString(),
      change: "All contracts",
      icon: Shield,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Active AMCs",
      value: amcStatsState.active.toString(),
      change: "Currently active",
      icon: CheckCircle,
      iconColor: "text-green-600",
      iconBg: "bg-green-100"
    },
    {
      title: "Renewal Due",
      value: amcStatsState.renewalDue.toString(),
      change: "Next 30 days",
      icon: AlertTriangle,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    },
    {
      title: "AMC Revenue",
      value: formatCurrency(amcStatsState.revenue),
      change: "Total value",
      icon: IndianRupee,
      iconColor: "text-purple-600",
      iconBg: "bg-purple-100"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">AMC Management</h1>
          <p className="text-gray-600">Annual Maintenance Contract tracking and management</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button onClick={handleNewAMC}>
            <Plus className="w-4 h-4 mr-2" />
            New AMC
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
          <CardTitle>AMC Search & Filters</CardTitle>
          <CardDescription>Filter and search through AMC contracts</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search AMCs by number, account, product, or serial..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10"
              />
            </div>
            <Select value={selectedStatus} onValueChange={setSelectedStatus}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Statuses" />
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

      {/* AMC Contracts List - Card View */}
      <Card>
        <CardHeader>
          <CardTitle>AMC Contracts List</CardTitle>
          <CardDescription>Showing {filteredAmcContracts.length} of {amcContractsList.length} AMC contracts</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredAmcContracts.length === 0 ? (
              <div className="text-center py-12">
                <Shield className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No AMC contracts found</p>
                <p className="text-sm text-gray-400 mt-2">
                  {searchTerm || selectedStatus !== "All" || selectedAssignedTo !== "All"
                    ? "Try adjusting your filters" 
                    : "Click 'New AMC' to create your first contract"}
                </p>
              </div>
            ) : (
              filteredAmcContracts.map((contract) => (
                <div
                  key={contract.id}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center space-x-6">
                    <div className="w-12 h-12 bg-green-100 rounded-lg flex items-center justify-center">
                      <Shield className="w-6 h-6 text-green-600" />
                    </div>
                    
                    {/* AMC Contract Details */}
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="font-semibold text-gray-900">
                          {contract.amc_number}
                        </h3>
                        <Badge className={getStatusColor(contract.status)}>
                          {contract.status}
                        </Badge>
                        {isRenewalDue(contract.contract_end_date) && (
                          <Badge variant="outline" className="text-xs bg-yellow-50 text-yellow-700 border-yellow-200">
                            Due Soon
                          </Badge>
                        )}
                        {contract.sales_order_number && (
                          <Badge variant="outline" className="text-purple-600 border-purple-200">
                            SO: {contract.sales_order_number}
                          </Badge>
                        )}
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Contract: {formatDate(contract.contract_start_date)} - {formatDate(contract.contract_end_date)}
                      </p>
                    </div>
                    
                    {/* Account & Product Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Building className="w-4 h-4 text-gray-400" />
                        <span className="font-medium">{contract.customer_name || contract.account_name || 'Unknown Account'}</span>
                      </div>
                      
                      {/* PDF Link */}
                      <div className="flex items-center gap-1 text-sm mt-1">
                        <button
                          onClick={() => handleViewAMCPDF(contract)}
                          className="flex items-center gap-1 text-blue-600 hover:text-blue-800 hover:underline transition-colors"
                          title="View AMC PDF"
                        >
                          <FileText className="w-3 h-3" />
                          <span>View Details PDF</span>
                        </button>
                      </div>
                      {contract.serial_number && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <span className="text-xs">S/N:</span>
                          <span>{contract.serial_number}</span>
                        </div>
                      )}
                    </div>
                    
                    {/* Contract Value & Assignment Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <IndianRupee className="w-4 h-4 text-green-600" />
                        <span className="font-semibold text-green-600">
                          {formatCurrency(contract.contract_value)}
                        </span>
                      </div>
                      {contract.assigned_to && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <User className="w-4 h-4 text-gray-400" />
                          <span>Assigned: {contract.assigned_to}</span>
                        </div>
                      )}
                      {contract.service_frequency && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Clock className="w-3 h-3 text-gray-400" />
                          <span>{contract.service_frequency} service</span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>

      {/* Add AMC Modal */}
      <AddAMCModal
        isOpen={isAddModalOpen}
        onClose={() => {
          setIsAddModalOpen(false)
          setEditingAMC(null)
        }}
        onSave={handleSaveAMC}
        editingAMC={editingAMC}
      />

      {/* Add Complaint Modal */}
      <AddComplaintModal
        isOpen={isAddComplaintModalOpen}
        onClose={() => {
          setIsAddComplaintModalOpen(false)
          setSourceAMC(null)
        }}
        onSave={handleSaveComplaint}
        sourceData={sourceAMC}
        sourceType="amc"
      />
    </div>
  )
}
