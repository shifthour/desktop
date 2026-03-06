"use client"

import { useState, useMemo, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import {
  Plus,
  Search,
  Edit,
  TrendingUp,
  Calendar,
  IndianRupee,
  Target,
  Handshake,
  Brain,
  Zap,
  Download,
  Upload,
  Phone,
  Mail,
  Users,
  FileText,
  Trash2,
} from "lucide-react"
import { AIPredictiveAnalytics } from "@/components/ai-predictive-analytics"
import { AIInsightsPanel } from "@/components/ai-insights-panel"
import { AIEmailGenerator } from "@/components/ai-email-generator"
import { AIProductRecommendations } from "@/components/ai-product-recommendations"
import { DynamicImportModal } from "@/components/dynamic-import-modal"
import { MultipleProductSelector } from "@/components/multiple-product-selector"
import { AddQuotationModal } from "@/components/add-quotation-modal"
import { AIInsightsService, type OpportunityData, type AIInsight } from "@/lib/ai-services"
import { AIDealIntelligenceCard } from "@/components/ai-deals-intelligence-card"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"
import { createClient } from '@supabase/supabase-js'

// Smart Product Display Component for Deals
const ProductsDisplay = ({ dealProducts, totalValue }: { dealProducts: any[], totalValue: number }) => {
  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(amount)
  }

  if (!dealProducts || dealProducts.length === 0) {
    return <span className="text-gray-400 text-sm">No products</span>
  }

  if (dealProducts.length === 1) {
    // Single product - show name clearly
    const product = dealProducts[0]
    return (
      <div>
        <p className="text-sm font-medium">{product.product_name}</p>
        <div className="text-xs text-gray-500 space-y-1 mt-1">
          <p>Qty: {product.quantity}</p>
          <p>Total: {formatCurrency(product.total_amount || 0)}</p>
        </div>
      </div>
    )
  }

  // Multiple products - show smart summary
  const firstProduct = dealProducts[0]
  const remainingCount = dealProducts.length - 1
  
  return (
    <div>
      <div className="flex items-center space-x-2">
        <p className="text-sm font-medium">{firstProduct.product_name}</p>
        <Popover>
          <PopoverTrigger asChild>
            <Badge 
              variant="outline" 
              className="text-xs cursor-pointer hover:bg-blue-50 hover:border-blue-300"
            >
              +{remainingCount} more
            </Badge>
          </PopoverTrigger>
          <PopoverContent className="w-80" align="start">
            <div className="space-y-3">
              <h4 className="font-semibold text-sm">All Products ({dealProducts.length})</h4>
              <div className="space-y-2 max-h-48 overflow-y-auto">
                {dealProducts.map((product, index) => (
                  <div key={index} className="flex justify-between items-center text-sm border-b pb-2">
                    <div>
                      <div className="font-medium">{product.product_name}</div>
                      <div className="text-xs text-gray-500">Qty: {product.quantity}</div>
                    </div>
                    <div className="text-right">
                      <div className="font-medium">{formatCurrency(product.total_amount || 0)}</div>
                      <div className="text-xs text-gray-500">
                        @ {formatCurrency(product.price_per_unit || 0)}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
              <div className="border-t pt-2">
                <div className="flex justify-between font-semibold">
                  <span>Total Value:</span>
                  <span className="text-blue-600">{formatCurrency(totalValue)}</span>
                </div>
              </div>
            </div>
          </PopoverContent>
        </Popover>
      </div>
      <div className="text-xs text-gray-500 space-y-1 mt-1">
        <p>Products: {dealProducts.length}</p>
        <p>Total: {formatCurrency(totalValue)}</p>
      </div>
    </div>
  )
}

interface Deal {
  id: string
  deal_name: string
  account_name: string
  department?: string
  city?: string
  state?: string
  contact_person: string
  phone?: string
  email?: string
  // Multiple products support
  dealProducts?: any[]
  totalProducts?: number
  calculatedValue?: number
  whatsapp?: string
  product: string
  quantity?: number
  price_per_unit?: number
  budget?: number
  value: number
  stage: string
  probability: number
  expected_close_date: string
  last_activity: string
  source: string
  source_lead_id?: string
  assigned_to: string
  priority: "High" | "Medium" | "Low"
  status: "Active" | "Won" | "Lost" | "On Hold"
  notes?: string
  created_at: string
  updated_at: string
}


// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
  </svg>
)

export function OpportunitiesContent() {
  const { toast } = useToast()
  const [deals, setDeals] = useState<Deal[]>([])
  const [accounts, setAccounts] = useState<any[]>([])
  const [statsLoaded, setStatsLoaded] = useState(false)
  const [dealsStats, setDealsStats] = useState({
    total: 0,
    thisMonth: 0,
    totalValue: 0,
    weightedValue: 0
  })
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedStage, setSelectedStage] = useState("all")
  const [selectedAssigned, setSelectedAssigned] = useState("all")
  const [isAddDialogOpen, setIsAddDialogOpen] = useState(false)
  const [isEditDialogOpen, setIsEditDialogOpen] = useState(false)
  const [selectedDeal, setSelectedDeal] = useState<Deal | null>(null)
  const [isImportModalOpen, setIsImportModalOpen] = useState(false)
  const [isImporting, setIsImporting] = useState(false)
  const [importProgress, setImportProgress] = useState({ current: 0, total: 0 })
  const [isQuotationModalOpen, setIsQuotationModalOpen] = useState(false)
  const [dealForQuotation, setDealForQuotation] = useState<Deal | null>(null)
  const [isEmailModalOpen, setIsEmailModalOpen] = useState(false)
  const [emailDeal, setEmailDeal] = useState<Deal | null>(null)
  
  // Add Deal form states
  const [selectedProducts, setSelectedProducts] = useState<any[]>([])
  const [totalBudget, setTotalBudget] = useState(0)

  // Load deals from database on component mount
  useEffect(() => {
    loadDeals()
  }, [])


  const loadDeals = async () => {
    try {
      // Load ALL deals and accounts without any filtering (optimized - products in single query)
      const [dealsResponse, accountsResponse] = await Promise.all([
        fetch(`/api/deals`),
        fetch(`/api/accounts`)
      ])

      if (dealsResponse.ok) {
        const data = await dealsResponse.json()
        const dealsArray = data.deals || []

        // Transform deals with products already included (no additional API calls)
        const dealsWithProducts = dealsArray.map((deal: any) => {
          // Handle products - use included data or fallback to legacy
          let dealProducts = deal.deal_products || []
          if (dealProducts.length === 0 && deal.product) {
            dealProducts = [{
              product_name: deal.product,
              quantity: deal.quantity || 1,
              price_per_unit: deal.price_per_unit || (deal.value || 0),
              total_amount: deal.value || 0
            }]
          }

          // Use deal's own contact fields
          const contactPhone = deal.phone_number || deal.phone || ''
          const contactEmail = deal.email_address || deal.email || ''

          return {
            ...deal,
            phone: contactPhone,
            email: contactEmail,
            dealProducts,
            totalProducts: dealProducts.length,
            calculatedValue: dealProducts.reduce((sum: number, p: any) => sum + (p.total_amount || 0), 0) || deal.value || 0
          }
        })

        console.log("loadDeals - API deals with products:", dealsWithProducts)
        console.log("First deal phone/email check:", dealsWithProducts[0]?.phone, dealsWithProducts[0]?.email)
        setDeals(dealsWithProducts)
        
        if (accountsResponse.ok) {
          const accountsData = await accountsResponse.json()
          setAccounts(accountsData.accounts || [])
        }
        
        // Calculate stats using calculated values from multiple products
        const thisMonth = dealsWithProducts.filter((deal: Deal) => 
          new Date(deal.expected_close_date).getMonth() === new Date().getMonth()
        ).length
        
        const totalValue = dealsWithProducts.reduce((sum: number, deal: Deal) => 
          sum + (deal.calculatedValue || deal.value || 0), 0
        )
        
        const weightedValue = dealsWithProducts.reduce((sum: number, deal: Deal) =>
          sum + ((deal.calculatedValue || deal.value || 0) * (deal.probability / 100)), 0
        )
        
        setDealsStats({
          total: dealsWithProducts.length,
          thisMonth: thisMonth,
          totalValue: totalValue,
          weightedValue: weightedValue
        })
        setStatsLoaded(true)
      } else {
        console.error('Failed to fetch deals:', response.statusText)
        toast({
          title: "Error",
          description: "Failed to load deals from database",
          variant: "destructive"
        })
        setStatsLoaded(true)
      }
    } catch (error) {
      console.error('Error loading deals:', error)
      toast({
        title: "Error", 
        description: "Failed to load deals from database",
        variant: "destructive"
      })
      setStatsLoaded(true)
    }
  }

  const handleImportData = async (importedDeals: any[]) => {
    setIsImporting(true)
    setImportProgress({ current: 0, total: importedDeals.length })

    try {
      let successCount = 0
      let failCount = 0

      for (let i = 0; i < importedDeals.length; i++) {
        const dealData = importedDeals[i]

        try {
          // Add default values for required fields
          const dealToCreate = {
            ...dealData,
            deal_name: dealData.deal_name || `${dealData.account_name} - ${dealData.product}`,
            status: dealData.status || 'Active',
            stage: dealData.stage || 'Qualification',
            probability: dealData.probability || 25,
            value: dealData.value || 0,
            expected_close_date: dealData.expected_close_date || new Date().toISOString().split('T')[0],
            last_activity: 'Imported',
            source: dealData.source || 'Import'
          }

          const response = await fetch('/api/deals', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(dealToCreate)
          })

          if (response.ok) {
            successCount++
          } else {
            failCount++
            console.error(`Failed to import deal ${i + 1}:`, await response.text())
          }
        } catch (err) {
          failCount++
          console.error(`Error importing deal ${i + 1}:`, err)
        }

        // Update progress
        setImportProgress({ current: i + 1, total: importedDeals.length })
      }

      // Reload deals from database
      await loadDeals()

      if (successCount > 0) {
        toast({
          title: "Import completed",
          description: `Successfully imported ${successCount} of ${importedDeals.length} deals.${failCount > 0 ? ` ${failCount} failed.` : ''}`
        })
      } else {
        toast({
          title: "Import failed",
          description: "No deals were imported successfully.",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error importing deals:', error)
      toast({
        title: "Import failed",
        description: "Failed to import deals to database",
        variant: "destructive"
      })
    } finally {
      setIsImporting(false)
      setImportProgress({ current: 0, total: 0 })
    }
  }

  // Convert deals to the format expected by AI services
  const opportunityData: OpportunityData[] = useMemo(
    () =>
      deals.map((deal) => ({
        id: deal.id,
        accountName: deal.account_name,
        contactName: deal.contact_person,
        product: deal.product,
        value: `₹${deal.value.toLocaleString()}`,
        stage: deal.stage,
        probability: `${deal.probability}%`,
        expectedClose: deal.expected_close_date,
        source: deal.source,
      })),
    [deals],
  )

  // Generate AI insights
  const aiInsights: AIInsight[] = useMemo(() => {
    return AIInsightsService.generateDashboardInsights([], opportunityData, [])
  }, [opportunityData])

  const filteredDeals = deals.filter((deal) => {
    const matchesSearch =
      deal.account_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      deal.contact_person.toLowerCase().includes(searchTerm.toLowerCase()) ||
      deal.product.toLowerCase().includes(searchTerm.toLowerCase())
    const matchesStage = selectedStage === "all" || deal.stage === selectedStage
    const matchesAssigned = selectedAssigned === "all" || deal.assigned_to === selectedAssigned

    return matchesSearch && matchesStage && matchesAssigned
  })

  const getStageColor = (stage: string) => {
    switch (stage) {
      case "Qualification":
        return "bg-blue-100 text-blue-800"
      case "Demo":
        return "bg-purple-100 text-purple-800"
      case "Proposal":
        return "bg-orange-100 text-orange-800"
      case "Negotiation":
        return "bg-yellow-100 text-yellow-800"
      case "Closed Won":
        return "bg-green-100 text-green-800"
      case "Closed Lost":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case "High":
        return "bg-red-100 text-red-800"
      case "Medium":
        return "bg-yellow-100 text-yellow-800"
      case "Low":
        return "bg-green-100 text-green-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const handleSaveDeal = async (dealData: any) => {
    try {
      const response = await fetch('/api/deals', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(dealData)
      })
      
      if (!response.ok) {
        throw new Error('Failed to create deal')
      }
      
      const newDeal = await response.json()
      
      // Reload deals from database
      loadDeals()
      
      toast({
        title: "Deal created",
        description: `Deal ${newDeal.deal.account_name} has been successfully created.`
      })
      setIsAddDialogOpen(false)
      // Clear form
      setSelectedProducts([])
      setTotalBudget(0)
    } catch (error) {
      console.error('Error creating deal:', error)
      toast({
        title: "Error",
        description: "Failed to create deal",
        variant: "destructive"
      })
    }
  }
  
  const handleUpdateDeal = async (updatedDealData: any) => {
    try {
      const response = await fetch('/api/deals', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          id: selectedDeal?.id,
          ...updatedDealData
        })
      })
      
      if (!response.ok) {
        throw new Error('Failed to update deal')
      }
      
      const updated = await response.json()
      
      // Reload deals from database
      loadDeals()
      
      toast({
        title: "Deal updated",
        description: `Deal ${updated.deal.account_name} has been successfully updated.`
      })
    } catch (error) {
      console.error('Error updating deal:', error)
      toast({
        title: "Error",
        description: "Failed to update deal",
        variant: "destructive"
      })
    }
    
    setIsEditDialogOpen(false)
    setSelectedDeal(null)
  }

  const handleCreateQuotation = (deal: Deal) => {
    console.log("Creating quotation from deal:", deal)
    setDealForQuotation(deal)
    setIsQuotationModalOpen(true)
  }

  const handleSaveQuotation = async (quotationData: any) => {
    try {
      const response = await fetch('/api/quotations', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(quotationData)
      })

      if (response.ok) {
        const data = await response.json()
        const newQuotation = data.quotation

        toast({
          title: "Quotation created",
          description: `Quotation ${newQuotation.quote_number} has been successfully created from deal: ${dealForQuotation?.deal_name}`
        })
        setIsQuotationModalOpen(false)
        setDealForQuotation(null)

        // Navigate to quotations page
        window.location.href = '/quotations'
      } else {
        throw new Error('Failed to create quotation')
      }
    } catch (error) {
      console.error("Failed to create quotation:", error)
      toast({
        title: "Error",
        description: "Failed to create quotation from deal. Please try again.",
        variant: "destructive"
      })
    }
  }

  const handleDeleteDeal = async (dealId: string, dealName: string) => {
    if (!confirm(`Are you sure you want to delete deal "${dealName}"?`)) {
      return
    }

    try {
      const user = localStorage.getItem('user')
      if (!user) return

      const parsedUser = JSON.parse(user)
      const response = await fetch(`/api/deals?id=${dealId}&companyId=${parsedUser.company_id}`, {
        method: 'DELETE',
      })

      if (response.ok) {
        toast({
          title: "Deal deleted",
          description: `Deal "${dealName}" has been successfully deleted`
        })
        loadDeals()
      } else {
        throw new Error('Failed to delete deal')
      }
    } catch (error) {
      console.error("Failed to delete deal:", error)
      toast({
        title: "Error",
        description: "Failed to delete deal. Please try again.",
        variant: "destructive"
      })
    }
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Deals</h1>
          <p className="text-gray-600 mt-1">Manage and track your sales deals</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button variant="outline" onClick={() => setIsImportModalOpen(true)}>
            <Upload className="w-4 h-4 mr-2" />
            Import Data
          </Button>
          <Button onClick={() => setIsAddDialogOpen(true)} className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Add Deal
          </Button>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-blue-100 mr-4">
                <Handshake className="w-6 h-6 text-blue-600" />
              </div>
              <div>
                <p className="text-sm font-medium text-gray-600">Total Deals</p>
                <p className="text-2xl font-bold">{!statsLoaded ? "..." : dealsStats.total}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-green-100 mr-4">
                <IndianRupee className="w-6 h-6 text-green-600" />
              </div>
              <div>
                <p className="text-sm font-medium text-gray-600">Pipeline Value</p>
                <p className="text-2xl font-bold">{!statsLoaded ? "..." : `₹${(dealsStats.totalValue / 100000).toFixed(1)}L`}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-purple-100 mr-4">
                <TrendingUp className="w-6 h-6 text-purple-600" />
              </div>
              <div>
                <p className="text-sm font-medium text-gray-600">Weighted Pipeline</p>
                <p className="text-2xl font-bold">{!statsLoaded ? "..." : `₹${(dealsStats.weightedValue / 100000).toFixed(1)}L`}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-orange-100 mr-4">
                <Calendar className="w-6 h-6 text-orange-600" />
              </div>
              <div>
                <p className="text-sm font-medium text-gray-600">Closing This Month</p>
                <p className="text-2xl font-bold">{!statsLoaded ? "..." : dealsStats.thisMonth}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Deal Search & Filters */}
      <Card>
        <CardHeader>
          <CardTitle>Deal Search & Filters</CardTitle>
          <CardDescription>Filter and search through your deals</CardDescription>
        </CardHeader>
            <CardContent className="p-4">
              <div className="flex flex-col md:flex-row gap-4">
                <div className="flex-1">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                    <Input
                      placeholder="Search deals..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="pl-10"
                    />
                  </div>
                </div>
                <Select value={selectedStage} onValueChange={setSelectedStage}>
                  <SelectTrigger className="w-full md:w-48">
                    <SelectValue placeholder="Filter by stage" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Stages</SelectItem>
                    <SelectItem value="Demo">Demo</SelectItem>
                    <SelectItem value="Proposal">Proposal</SelectItem>
                    <SelectItem value="Negotiation">Negotiation</SelectItem>
                    <SelectItem value="WonDeal">WonDeal</SelectItem>
                    <SelectItem value="LostDeal">LostDeal</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={selectedAssigned} onValueChange={setSelectedAssigned}>
                  <SelectTrigger className="w-full md:w-48">
                    <SelectValue placeholder="Filter by assigned" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Assigned</SelectItem>
                    {Array.from(new Set(deals.map(deal => deal.assigned_to).filter(Boolean))).map(assigned => (
                      <SelectItem key={assigned} value={assigned}>{assigned}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          {/* Opportunities Table */}
          <Card>
            <CardHeader>
              <CardTitle>Deals List</CardTitle>
              <CardDescription>Track and manage your sales deals</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {filteredDeals.length === 0 ? (
                  <p className="text-center text-gray-500 py-8">No deals found</p>
                ) : (
                  filteredDeals.map((deal) => (
                    <div
                      key={deal.id}
                      className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                    >
                      <div className="flex items-center space-x-14">
                        <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                          <Handshake className="w-6 h-6 text-blue-600" />
                        </div>
                        
                        {/* Account Details */}
                        <div>
                          <div className="flex items-center space-x-2">
                            <span
                              className={`px-2 py-1 text-xs rounded-full ${
                                deal.status === "Active" ? "bg-green-100 text-green-800" :
                                deal.status === "Won" ? "bg-blue-100 text-blue-800" :
                                deal.status === "Lost" ? "bg-red-100 text-red-800" :
                                "bg-gray-100 text-gray-800"
                              }`}
                            >
                              {deal.status}
                            </span>
                          </div>
                          {deal.deal_name && (
                            <p className="text-sm font-semibold text-gray-900 mt-1">
                              {deal.deal_name}
                            </p>
                          )}
                          <p className="text-sm text-gray-700 mt-1">
                            <span className="font-medium">Account:</span> {deal.account_name}
                          </p>
                          {deal.department && deal.department !== "None" && (
                            <p className="text-sm text-gray-600">Department: {deal.department}</p>
                          )}
                        </div>
                        
                        {/* Contact Details */}
                        <div className="ml-14">
                          <div className="text-xs text-gray-500 space-y-1">
                            <p className="font-medium">Contact: {deal.contact_person}</p>
                            {deal.phone && (
                              <div className="flex items-center">
                                <Phone className="w-3 h-3 mr-1" />
                                <span>{deal.phone}</span>
                              </div>
                            )}
                            {deal.email && (
                              <div className="flex items-center">
                                <Mail className="w-3 h-3 mr-1" />
                                <span>{deal.email}</span>
                              </div>
                            )}
                          </div>
                        </div>
                        
                        {/* Product Details - beside contact */}
                        <div className="ml-14">
                          <div className="flex items-start gap-x-14">
                            {/* Smart Products Display */}
                            <ProductsDisplay
                              dealProducts={deal.dealProducts}
                              totalValue={deal.calculatedValue || deal.value}
                            />
                            <div className="text-xs text-gray-500 space-y-1">
                              <p>Stage: {deal.stage}</p>
                              {deal.assigned_to && (
                                <p className="font-medium">Assigned: {deal.assigned_to}</p>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                      
                      <div className="flex flex-col items-end space-y-2">
                        {/* First Row - Communication Buttons */}
                        <div className="flex space-x-1">
                          {deal.phone && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => window.open(`tel:${deal.phone}`, '_blank')}
                              className="text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                              title="Call Contact"
                            >
                              <Phone className="w-4 h-4" />
                            </Button>
                          )}
                          {deal.whatsapp && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => {
                                const message = encodeURIComponent(`Hi ${deal.contact_person},\n\nHope you are doing well!\n\nPlease find attached quotation for ${deal.product} for your reference.\n\nLet me know if you have any questions or need any clarifications.\n\nThanks & Regards`)
                                window.open(`https://wa.me/${deal.whatsapp.replace(/[^0-9]/g, '')}?text=${message}`, '_blank')
                              }}
                              className="text-green-600 hover:text-green-800 hover:bg-green-50"
                              title="WhatsApp Contact"
                            >
                              <WhatsAppIcon className="w-4 h-4" />
                            </Button>
                          )}
                          {deal.email && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => {
                                setEmailDeal(deal)
                                setIsEmailModalOpen(true)
                              }}
                              className="text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                              title="Send Email"
                            >
                              <Mail className="w-4 h-4" />
                            </Button>
                          )}
                        </div>

                        {/* Second Row - Action Buttons */}
                        <div className="flex space-x-1">
                          {/* Create Quotation Button */}
                          {deal.status === "Active" && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleCreateQuotation(deal)}
                              title="Create Quotation"
                              className="text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                            >
                              <FileText className="w-4 h-4" />
                            </Button>
                          )}

                          {/* Edit Button */}
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => {
                              setSelectedDeal(deal)
                              setIsEditDialogOpen(true)
                            }}
                            title="Edit Deal"
                          >
                            <Edit className="w-4 h-4" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </CardContent>
          </Card>

      {/* Add Deal Dialog */}
      <Dialog open={isAddDialogOpen} onOpenChange={setIsAddDialogOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Add New Deal</DialogTitle>
            <DialogDescription>Create a new sales deal to track in your pipeline.</DialogDescription>
          </DialogHeader>
          <div className="grid grid-cols-2 gap-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="accountName">Account Name</Label>
              <Input id="accountName" placeholder="Enter account name" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="contactPerson">Contact Person</Label>
              <Input id="contactPerson" placeholder="Enter contact person" />
            </div>
            <div className="col-span-2 space-y-2">
              <MultipleProductSelector 
                selectedProducts={selectedProducts}
                onProductsChange={setSelectedProducts}
                onTotalBudgetChange={setTotalBudget}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="stage">Stage</Label>
              <Select>
                <SelectTrigger>
                  <SelectValue placeholder="Select stage" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="Qualification">Qualification</SelectItem>
                  <SelectItem value="Demo">Demo</SelectItem>
                  <SelectItem value="Proposal">Proposal</SelectItem>
                  <SelectItem value="Negotiation">Negotiation</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="probability">Probability</Label>
              <Input id="probability" placeholder="Enter probability %" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="closeDate">Expected Close Date</Label>
              <Input id="closeDate" type="date" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="priority">Priority</Label>
              <Select>
                <SelectTrigger>
                  <SelectValue placeholder="Select priority" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="High">High</SelectItem>
                  <SelectItem value="Medium">Medium</SelectItem>
                  <SelectItem value="Low">Low</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="col-span-2 space-y-2">
              <Label htmlFor="notes">Notes</Label>
              <Textarea id="notes" placeholder="Enter any additional notes" />
            </div>
          </div>
          <div className="flex justify-end space-x-2">
            <Button variant="outline" onClick={() => {
              setIsAddDialogOpen(false)
              setSelectedProducts([])
              setTotalBudget(0)
            }}>
              Cancel
            </Button>
            <Button onClick={() => {
              // Validate required fields
              const accountName = (document.getElementById('accountName') as HTMLInputElement)?.value || ''
              const contactPerson = (document.getElementById('contactPerson') as HTMLInputElement)?.value || ''
              
              if (!accountName) {
                toast({
                  title: "Validation Error",
                  description: "Please enter account name",
                  variant: "destructive"
                })
                return
              }
              
              if (selectedProducts.length === 0) {
                toast({
                  title: "Validation Error", 
                  description: "Please select at least one product",
                  variant: "destructive"
                })
                return
              }
              
              // Collect form data and create deal with multiple products
              const formData = {
                deal_name: `${accountName} - ${selectedProducts[0]?.productName || 'Deal'}`,
                account_name: accountName,
                contact_person: contactPerson,
                // Include selected products for the new API
                selected_products: selectedProducts.map(product => ({
                  product_id: product.id,
                  product_name: product.productName,
                  quantity: product.quantity,
                  price_per_unit: product.price,
                  total_amount: product.totalAmount
                })),
                // For backward compatibility, include primary product info
                product: selectedProducts[0]?.productName || '',
                value: totalBudget,
                stage: 'Qualification',
                probability: parseInt((document.getElementById('probability') as HTMLInputElement)?.value || '25'),
                expected_close_date: (document.getElementById('closeDate') as HTMLInputElement)?.value || '',
                last_activity: 'Created',
                source: 'Manual Entry',
                assigned_to: 'Admin',
                priority: 'Medium',
                status: 'Active'
              }
              handleSaveDeal(formData)
            }}>Create Deal</Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Edit Deal Dialog */}
      <Dialog open={isEditDialogOpen} onOpenChange={setIsEditDialogOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Edit Deal</DialogTitle>
            <DialogDescription>Update the sales deal information.</DialogDescription>
          </DialogHeader>
          {selectedDeal && (
            <div className="grid grid-cols-2 gap-4 py-4">
              <div className="space-y-2">
                <Label htmlFor="edit-accountName">Account Name</Label>
                <Input id="edit-accountName" defaultValue={selectedDeal.account_name} placeholder="Enter account name" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="edit-contactPerson">Contact Person</Label>
                <Input id="edit-contactPerson" defaultValue={selectedDeal.contact_person} placeholder="Enter contact person" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="edit-product">Product</Label>
                <Input id="edit-product" defaultValue={selectedDeal.product} placeholder="Enter product" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="edit-value">Value</Label>
                <Input id="edit-value" type="number" defaultValue={selectedDeal.value.toString()} placeholder="Enter value" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="edit-stage">Stage</Label>
                <Select defaultValue={selectedDeal.stage}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select stage" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Qualification">Qualification</SelectItem>
                    <SelectItem value="Demo">Demo</SelectItem>
                    <SelectItem value="Proposal">Proposal</SelectItem>
                    <SelectItem value="Negotiation">Negotiation</SelectItem>
                    <SelectItem value="Closed Won">Closed Won</SelectItem>
                    <SelectItem value="Closed Lost">Closed Lost</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="edit-probability">Probability (%)</Label>
                <Input id="edit-probability" type="number" min="0" max="100" defaultValue={selectedDeal.probability.toString()} placeholder="Enter probability %" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="edit-closeDate">Expected Close Date</Label>
                <Input id="edit-closeDate" type="date" defaultValue={selectedDeal.expected_close_date} />
              </div>
              <div className="space-y-2">
                <Label htmlFor="edit-priority">Priority</Label>
                <Select defaultValue={selectedDeal.priority}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select priority" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="High">High</SelectItem>
                    <SelectItem value="Medium">Medium</SelectItem>
                    <SelectItem value="Low">Low</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="edit-assignedTo">Assigned To</Label>
                <Select defaultValue={selectedDeal.assigned_to || ''}>
                  <SelectTrigger id="edit-assignedTo">
                    <SelectValue placeholder="Select account" />
                  </SelectTrigger>
                  <SelectContent>
                    {accounts.map((account: any) => (
                      <SelectItem key={account.id} value={account.accountName}>
                        {account.accountName}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="col-span-2 space-y-2">
                <Label htmlFor="edit-notes">Notes</Label>
                <Textarea id="edit-notes" defaultValue={selectedDeal.notes} placeholder="Enter any additional notes" />
              </div>
            </div>
          )}
          <div className="flex justify-end space-x-2">
            <Button variant="outline" onClick={() => setIsEditDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={() => {
              if (selectedDeal) {
                const stageSelect = document.getElementById('edit-stage') as HTMLSelectElement
                const prioritySelect = document.getElementById('edit-priority') as HTMLSelectElement
                const assignedToSelect = document.getElementById('edit-assignedTo') as HTMLSelectElement

                const formData = {
                  deal_name: `${(document.getElementById('edit-accountName') as HTMLInputElement)?.value || selectedDeal.account_name} - ${(document.getElementById('edit-product') as HTMLInputElement)?.value || selectedDeal.product}`,
                  account_name: (document.getElementById('edit-accountName') as HTMLInputElement)?.value || selectedDeal.account_name,
                  contact_person: (document.getElementById('edit-contactPerson') as HTMLInputElement)?.value || selectedDeal.contact_person,
                  product: (document.getElementById('edit-product') as HTMLInputElement)?.value || selectedDeal.product,
                  value: parseFloat((document.getElementById('edit-value') as HTMLInputElement)?.value || selectedDeal.value.toString()),
                  stage: stageSelect?.value || selectedDeal.stage,
                  probability: parseInt((document.getElementById('edit-probability') as HTMLInputElement)?.value || selectedDeal.probability.toString()),
                  expected_close_date: (document.getElementById('edit-closeDate') as HTMLInputElement)?.value || selectedDeal.expected_close_date,
                  priority: prioritySelect?.value || selectedDeal.priority,
                  assigned_to: assignedToSelect?.value || selectedDeal.assigned_to,
                  notes: (document.getElementById('edit-notes') as HTMLTextAreaElement)?.value || selectedDeal.notes,
                  last_activity: 'Updated'
                }
                handleUpdateDeal(formData)
              }
            }}>Update Deal</Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* AI Intelligence Section - Real-time revenue analysis */}
      <AIDealIntelligenceCard deals={deals} accounts={accounts} />

      {/* Dynamic Import Modal */}
      <DynamicImportModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        moduleType="deals"
        onImport={handleImportData}
        isImporting={isImporting}
        importProgress={importProgress}
      />

      {/* Add Quotation Modal */}
      {dealForQuotation && (
        <AddQuotationModal
          isOpen={isQuotationModalOpen}
          onClose={() => {
            setIsQuotationModalOpen(false)
            setDealForQuotation(null)
          }}
          onSave={handleSaveQuotation}
          isCreateMode={true}  // This is a new quotation, not editing
          editingQuotation={{
            // Pre-populate with deal data
            customer_name: dealForQuotation.account_name,
            contact_person: dealForQuotation.contact_person,
            customer_email: dealForQuotation.email,
            customer_phone: dealForQuotation.phone,
            billing_address: `${dealForQuotation.city || ''}, ${dealForQuotation.state || ''}`.trim().replace(/^,|,$/, ''),
            subject: `Quotation for ${dealForQuotation.deal_name}`,
            notes: `Generated from deal: ${dealForQuotation.deal_name}`,
            priority: dealForQuotation.priority || 'Medium',
            // Properly structure items from deal products
            items: dealForQuotation.dealProducts && dealForQuotation.dealProducts.length > 0 ? 
              dealForQuotation.dealProducts.map((product: any, index: number) => ({
                id: (index + 1).toString(),
                product: product.product_name || "Product/Service",
                description: `Product from deal: ${dealForQuotation.deal_name}`,
                quantity: product.quantity || 1,
                unitPrice: product.price_per_unit || (product.total_amount / (product.quantity || 1)) || 0,
                discount: 0,
                taxRate: 18,
                amount: product.total_amount || ((product.quantity || 1) * (product.price_per_unit || 0))
              })) : [{
                id: "1",
                product: dealForQuotation.product || "Product/Service",
                description: `Product from deal: ${dealForQuotation.deal_name}`,
                quantity: dealForQuotation.quantity || 1,
                unitPrice: dealForQuotation.price_per_unit || dealForQuotation.value || 0,
                discount: 0,
                taxRate: 18,
                amount: dealForQuotation.value || 0
              }]
          }}
        />
      )}

      {/* Email Compose Modal */}
      <Dialog open={isEmailModalOpen} onOpenChange={setIsEmailModalOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Compose Email</DialogTitle>
            <DialogDescription>
              Email draft ready. Click "Open Email Client" to send through your default email application.
            </DialogDescription>
          </DialogHeader>
          {emailDeal && (
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <Label className="text-sm font-medium">From:</Label>
                <div className="p-2 bg-gray-50 rounded-md">
                  {(() => {
                    const storedUser = localStorage.getItem('user')
                    try {
                      const user = storedUser ? JSON.parse(storedUser) : null
                      return user?.email || 'Your email address'
                    } catch {
                      return 'Your email address'
                    }
                  })()}
                </div>
              </div>
              
              <div className="space-y-2">
                <Label className="text-sm font-medium">To:</Label>
                <div className="p-2 bg-gray-50 rounded-md">{emailDeal.email}</div>
              </div>
              
              <div className="space-y-2">
                <Label className="text-sm font-medium">Subject:</Label>
                <Input 
                  id="email-subject" 
                  defaultValue={`Regarding ${emailDeal.deal_name} - ${emailDeal.product}`}
                  className="font-medium"
                />
              </div>
              
              <div className="space-y-2">
                <Label className="text-sm font-medium">Message:</Label>
                <Textarea 
                  id="email-body"
                  defaultValue={`Dear ${emailDeal.contact_person},

I hope this email finds you well. I wanted to follow up regarding our discussion about ${emailDeal.product} for ${emailDeal.account_name}.

Please let me know if you have any questions or if you'd like to schedule a meeting to discuss this further.

Best regards,
${(() => {
  const storedUser = localStorage.getItem('user')
  try {
    const user = storedUser ? JSON.parse(storedUser) : null
    return user?.full_name || user?.name || 'Your Name'
  } catch {
    return 'Your Name'
  }
})()}`}
                  rows={10}
                  className="font-mono text-sm"
                />
              </div>
              
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
                <p className="text-sm text-blue-800">
                  <strong>Note:</strong> This will open your default email client with the pre-filled content. 
                  Make sure you have an email client configured on your system.
                </p>
              </div>
            </div>
          )}
          <div className="flex justify-end space-x-2">
            <Button variant="outline" onClick={() => {
              setIsEmailModalOpen(false)
              setEmailDeal(null)
            }}>
              Cancel
            </Button>
            <Button 
              onClick={() => {
                if (emailDeal) {
                  const subject = encodeURIComponent((document.getElementById('email-subject') as HTMLInputElement)?.value || '')
                  const body = encodeURIComponent((document.getElementById('email-body') as HTMLTextAreaElement)?.value || '')
                  window.open(`mailto:${emailDeal.email}?subject=${subject}&body=${body}`, '_blank')
                  
                  toast({
                    title: "Email client opened",
                    description: "Your default email client should now be open with the pre-filled content."
                  })
                  
                  setIsEmailModalOpen(false)
                  setEmailDeal(null)
                }
              }}
              className="bg-blue-600 hover:bg-blue-700"
            >
              <Mail className="w-4 h-4 mr-2" />
              Open Email Client
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
