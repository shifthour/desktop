"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Plus, Search, Download, Eye, Edit, Wrench, CheckCircle, Calendar, Building, 
  User, Phone, Mail, Package, MapPin, Clock, Settings, AlertCircle, TrendingUp, FileText
} from "lucide-react"
import { AddInstallationModal } from "@/components/add-installation-modal"
import { AddAMCModal } from "@/components/add-amc-modal"
import { AddComplaintModal } from "@/components/add-complaint-modal"
import { useToast } from "@/hooks/use-toast"

const statuses = ["All", "Pending", "Scheduled", "In Progress", "Completed", "On Hold", "Cancelled"]
const warrantyStatuses = ["All", "Under Warranty", "Warranty Expired", "Extended Warranty"]

export function InstallationsContent() {
  const { toast } = useToast()
  const [installationsList, setInstallationsList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedWarrantyStatus, setSelectedWarrantyStatus] = useState("All")
  const [selectedAssignedTo, setSelectedAssignedTo] = useState("All")
  const [installationsStatsState, setInstallationsStatsState] = useState({
    total: 0,
    completed: 0,
    inProgress: 0,
    avgTime: 0,
    pendingCount: 0
  })
  const [isAddInstallationModalOpen, setIsAddInstallationModalOpen] = useState(false)
  const [editingInstallation, setEditingInstallation] = useState<any>(null)
  const [isAddAMCModalOpen, setIsAddAMCModalOpen] = useState(false)
  const [sourceInstallation, setSourceInstallation] = useState<any>(null)
  const [isAddComplaintModalOpen, setIsAddComplaintModalOpen] = useState(false)
  const [sourceInstallationForComplaint, setSourceInstallationForComplaint] = useState<any>(null)

  // Load installations from API on component mount
  useEffect(() => {
    loadInstallations()
  }, [])

  const handleSaveInstallation = async (installationData: any) => {
    try {
      const response = await fetch('/api/installations', {
        method: editingInstallation ? 'PUT' : 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(editingInstallation ? { id: editingInstallation.id, ...installationData } : installationData)
      })

      if (response.ok) {
        const data = await response.json()
        const installation = data.installation

        // If this is a new installation (not editing) and has annual maintenance charges, create AMC record
        console.log('=== AMC AUTO-CREATION CHECK ===')
        console.log('editingInstallation:', editingInstallation)
        console.log('annual_maintenance_charges:', installationData.annual_maintenance_charges)
        console.log('parsed amount:', parseFloat(installationData.annual_maintenance_charges))
        
        if (!editingInstallation && installationData.annual_maintenance_charges && parseFloat(installationData.annual_maintenance_charges) > 0) {
          console.log('=== CREATING AMC RECORD ===');
          try {
            // Calculate AMC date (installation date + 1 year)
            const installationDate = new Date(installationData.scheduled_date || installationData.installation_date)
            const amcDate = new Date(installationDate)
            amcDate.setFullYear(amcDate.getFullYear() + 1)

            const amcData = {
              // Copy installation details
              customer_name: installationData.customer_name || installationData.account_name,
              contact_person: installationData.contact_person,
              customer_phone: installationData.customer_phone,
              customer_email: installationData.customer_email,
              installation_address: installationData.installation_address,
              city: installationData.city,
              state: installationData.state,
              pincode: installationData.pincode,
              
              // Product details
              product_name: installationData.product_name,
              product_model: installationData.product_model,
              serial_number: installationData.serial_number,
              
              // AMC specific details
              amc_start_date: amcDate.toISOString().split('T')[0],
              amc_end_date: new Date(amcDate.getFullYear() + 1, amcDate.getMonth(), amcDate.getDate()).toISOString().split('T')[0],
              amc_amount: parseFloat(installationData.annual_maintenance_charges),
              frequency: 'annual',
              status: 'active',
              
              // Reference to installation
              installation_id: installation.id,
              installation_number: installation.installation_number,
              
              // Source information
              source_type: 'installation',
              source_reference: installation.installation_number
            }

            const amcResponse = await fetch('/api/amc', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
              },
              body: JSON.stringify(amcData),
            })

            if (amcResponse.ok) {
              const amcResult = await amcResponse.json()
              console.log('AMC record created:', amcResult.amcContract.amc_number)
            }
          } catch (amcError) {
            console.error('Error creating AMC record:', amcError)
            // Don't fail the installation creation if AMC creation fails
          }
        }

        toast({
          title: editingInstallation ? "Installation Updated" : "Installation Scheduled",
          description: `Installation ${installation.installation_number} has been ${editingInstallation ? 'updated' : 'scheduled'} for ${installationData.account_name || installationData.customer_name}`
        })

        setIsAddInstallationModalOpen(false)
        setEditingInstallation(null)
        loadInstallations() // Reload the list
      } else {
        throw new Error(`Failed to ${editingInstallation ? 'update' : 'create'} installation`)
      }
    } catch (error) {
      console.error('Error saving installation:', error)
      toast({
        title: "Error",
        description: `Failed to ${editingInstallation ? 'update' : 'schedule'} installation`,
        variant: "destructive"
      })
    }
  }

  const loadInstallations = async () => {
    try {
      const response = await fetch('/api/installations')
      if (response.ok) {
        const data = await response.json()
        const installations = data.installations || []
        setInstallationsList(installations)
        
        // Calculate stats
        const completed = installations.filter(inst => inst.status === 'Completed').length
        const inProgress = installations.filter(inst => inst.status === 'In Progress').length
        const pending = installations.filter(inst => inst.status === 'Pending' || inst.status === 'Scheduled').length
        
        setInstallationsStatsState({
          total: installations.length,
          completed: completed,
          inProgress: inProgress,
          avgTime: 2.3, // Mock average time
          pendingCount: pending
        })
      } else {
        console.error('Failed to fetch installations')
        setInstallationsList([])
      }
    } catch (error) {
      console.error('Error loading installations:', error)
      setInstallationsList([])
    }
  }

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "pending":
        return "bg-gray-100 text-gray-800"
      case "scheduled":
        return "bg-blue-100 text-blue-800"
      case "in progress":
        return "bg-yellow-100 text-yellow-800"
      case "completed":
        return "bg-green-100 text-green-800"
      case "on hold":
        return "bg-orange-100 text-orange-800"
      case "cancelled":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getWarrantyColor = (warranty: string) => {
    switch (warranty?.toLowerCase()) {
      case "under warranty":
        return "bg-green-100 text-green-800"
      case "warranty expired":
        return "bg-red-100 text-red-800"
      case "extended warranty":
        return "bg-blue-100 text-blue-800"
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

  const handleCreateAMCFromInstallation = (installation: any) => {
    setSourceInstallation(installation)
    setIsAddAMCModalOpen(true)
  }

  const handleSaveAMC = async (amcData: any) => {
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
        
        toast({
          title: "Success",
          description: `AMC contract ${result.amcContract.amc_number} created successfully`,
        })
        
        setIsAddAMCModalOpen(false)
        setSourceInstallation(null)
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error || "Failed to create AMC contract",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to create AMC contract. Please try again.",
        variant: "destructive",
      })
    }
  }

  const handleCreateComplaintFromInstallation = (installation: any) => {
    setSourceInstallationForComplaint(installation)
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
        
        toast({
          title: "Success",
          description: `Complaint ${result.complaint.complaint_number} created successfully`,
        })
        
        setIsAddComplaintModalOpen(false)
        setSourceInstallationForComplaint(null)
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error || "Failed to create complaint",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to create complaint. Please try again.",
        variant: "destructive",
      })
    }
  }

  const handleViewInstallationPDF = async (installation: any) => {
    try {
      console.log('=== STARTING PDF GENERATION ===')
      console.log('Installation:', installation)
      
      // First, try to fetch the original source data (same logic as installation creation modal)
      let sourceData = installation
      
      if (installation.source_type === 'sales_order' && installation.source_reference) {
        try {
          const response = await fetch('/api/sales-orders')
          if (response.ok) {
            const data = await response.json()
            const salesOrders = data.salesOrders || []
            
            const originalSalesOrder = salesOrders.find((so: any) => 
              so.order_id === installation.source_reference || 
              so.sales_order_number === installation.source_reference ||
              so.id === installation.source_reference
            )
            
            if (originalSalesOrder) {
              console.log("Found original sales order for PDF:", originalSalesOrder)
              sourceData = originalSalesOrder
            }
          }
        } catch (error) {
          console.log('Error fetching sales order for PDF:', error)
        }
      } else if (installation.source_type === 'invoice' && installation.source_reference) {
        try {
          console.log('Fetching original invoice for PDF:', installation.source_reference)
          const response = await fetch('/api/invoices')
          if (response.ok) {
            const data = await response.json()
            const invoices = data.invoices || []
            
            const originalInvoice = invoices.find((inv: any) => 
              inv.invoice_number === installation.source_reference ||
              inv.id === installation.source_reference
            )
            
            if (originalInvoice) {
              console.log("Found original invoice for PDF:", originalInvoice)
              sourceData = originalInvoice
            }
          }
        } catch (error) {
          console.log('Error fetching invoice for PDF:', error)
        }
      }

      // Generate PDF using EXACT same logic as installation creation modal
      const printWindow = window.open('', '_blank')
      if (printWindow) {
        const title = installation.source_type === 'sales_order' ? 'Sales Order' : 
                      installation.source_type === 'invoice' ? 'Invoice' : 'Installation'
        const refNumber = installation.source_reference || installation.installation_number
        
        // Extract products information - PRIORITIZE quotation line items first
        let itemsData = []

        // For invoices, check if they have their own line items first
        if (installation.source_type === 'invoice' && sourceData.line_items && Array.isArray(sourceData.line_items)) {
          console.log("Using line_items from invoice:", sourceData.line_items)
          itemsData = sourceData.line_items.map((item: any) => ({
            product: item.product || item.name || 'Product/Service',
            description: item.description || 'Professional equipment',
            quantity: item.quantity || 1,
            unitPrice: item.unitPrice || item.unit_price || 0,
            discount: item.discount || 0,
            amount: item.amount || 0
          }))
        }
        // If no invoice line items, check if there's a quotation reference in the invoice
        else if (installation.source_type === 'invoice' && sourceData.notes && sourceData.notes.includes('quotation')) {
          // Extract quotation reference from invoice notes
          const quotationMatch = sourceData.notes.match(/(?:quotation|QTN)[:\s]*([A-Z0-9-]+)/i)
          if (quotationMatch) {
            const quotationRef = quotationMatch[1]
            console.log('Found quotation reference in invoice notes:', quotationRef)
            try {
              const response = await fetch(`/api/quotations?companyId=de19ccb7-e90d-4507-861d-a3aecf5e3f29`)
              if (response.ok) {
                const data = await response.json()
                const quotations = data.quotations || []
                
                const originalQuotation = quotations.find((q: any) => 
                  q.quote_number === quotationRef || 
                  q.quotationId === quotationRef ||
                  q.id === quotationRef
                )
                
                if (originalQuotation && originalQuotation.line_items && originalQuotation.line_items.length > 0) {
                  console.log("Using quotation line items from invoice reference:", originalQuotation.line_items)
                  itemsData = originalQuotation.line_items.map((item: any) => ({
                    product: item.product || item.name || 'Product/Service',
                    description: item.description || 'Professional equipment',
                    quantity: item.quantity || 1,
                    unitPrice: item.unitPrice || item.unit_price || 0,
                    discount: item.discount || 0,
                    amount: item.amount || 0
                  }))
                }
              }
            } catch (error) {
              console.log('Error fetching quotation from invoice notes:', error)
            }
          }
        }

        // If there's a quotation reference, fetch the detailed line items
        if (itemsData.length === 0 && sourceData.quotation_ref) {
          try {
            console.log('Fetching quotation for PDF:', sourceData.quotation_ref)
            const response = await fetch(`/api/quotations?companyId=de19ccb7-e90d-4507-861d-a3aecf5e3f29`)
            if (response.ok) {
              const data = await response.json()
              const quotations = data.quotations || []
              
              const originalQuotation = quotations.find((q: any) => 
                q.quote_number === sourceData.quotation_ref || 
                q.quotationId === sourceData.quotation_ref ||
                q.id === sourceData.quotation_ref
              )
              
              console.log('Found quotation:', originalQuotation)
              
              if (originalQuotation && originalQuotation.line_items && originalQuotation.line_items.length > 0) {
                console.log("Using real quotation line items for PDF:", originalQuotation.line_items)
                itemsData = originalQuotation.line_items.map((item: any) => ({
                  product: item.product || item.name || 'Product/Service',
                  description: item.description || 'Professional equipment',
                  quantity: item.quantity || 1,
                  unitPrice: item.unitPrice || item.unit_price || 0,
                  discount: item.discount || 0,
                  amount: item.amount || 0
                }))
              }
            }
          } catch (error) {
            console.log('Could not fetch quotation details for PDF:', error)
          }
        }

        // Fallback only if no quotation line items found
        if (itemsData.length === 0) {
          // Handle invoice-specific fields
          let productName = 'Product/Service'
          let description = ''
          let unitPrice = 0
          let totalAmount = 0
          
          if (installation.source_type === 'invoice') {
            productName = sourceData.items || sourceData.item_description || sourceData.product || 'Products/Services'
            // Clean up description - remove system-generated text
            let cleanDescription = sourceData.description || sourceData.item_description || ''
            if (cleanDescription.includes('Converted from quotation') || 
                cleanDescription.includes('Generated from Sales Order')) {
              // Extract only the meaningful description part
              const lines = cleanDescription.split('\n')
              cleanDescription = lines.find(line => 
                !line.includes('Converted from') && 
                !line.includes('Generated from') && 
                line.trim().length > 0
              ) || 'Professional equipment and services'
            }
            description = cleanDescription || 'Professional equipment and services'
            unitPrice = sourceData.unit_price || sourceData.total_amount || sourceData.amount || 0
            totalAmount = sourceData.total_amount || sourceData.amount || 0
            
            // Check if product name contains multiple products (separated by commas or keywords)
            if (productName && (productName.includes(',') || productName.includes(' and ') || productName.includes(' & '))) {
              // Try to split into multiple products
              let products = []
              if (productName.includes(',')) {
                products = productName.split(',').map(p => p.trim()).filter(p => p.length > 0)
              } else if (productName.includes(' and ')) {
                products = productName.split(' and ').map(p => p.trim()).filter(p => p.length > 0)
              } else if (productName.includes(' & ')) {
                products = productName.split(' & ').map(p => p.trim()).filter(p => p.length > 0)
              }
              
              if (products.length > 1) {
                console.log('Split products:', products)
                itemsData = products.map((product, index) => ({
                  product: product,
                  description: index === 0 ? description : 'Professional equipment',
                  quantity: 1,
                  unitPrice: index === 0 ? unitPrice : 0,
                  discount: 0,
                  amount: index === 0 ? totalAmount : 0
                }))
              } else {
                itemsData = [{
                  product: productName,
                  description: description,
                  quantity: sourceData.quantity || 1,
                  unitPrice: unitPrice,
                  discount: sourceData.discount || 0,
                  amount: totalAmount
                }]
              }
            } else {
              itemsData = [{
                product: productName,
                description: description,
                quantity: sourceData.quantity || 1,
                unitPrice: unitPrice,
                discount: sourceData.discount || 0,
                amount: totalAmount
              }]
            }
          } else {
            productName = sourceData.product || sourceData.products || sourceData.products_quoted || 'Product/Service'
            description = sourceData.product_description || sourceData.description || ''
            unitPrice = sourceData.unit_price || sourceData.total_amount || sourceData.amount || 0
            totalAmount = sourceData.total_amount || sourceData.amount || 0
            
            itemsData = [{
              product: productName,
              description: description,
              quantity: sourceData.quantity || 1,
              unitPrice: unitPrice,
              discount: sourceData.discount || 0,
              amount: totalAmount
            }]
          }
        }

        console.log('Final itemsData for PDF:', itemsData)
        
        // Use EXACT same HTML as installation creation modal
        const html = `
          <!DOCTYPE html>
          <html>
          <head>
            <title>${title}: ${refNumber}</title>
            <style>
              @page { size: A4; margin: 20mm; }
              body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
              .header { border-bottom: 2px solid #2563eb; padding-bottom: 20px; margin-bottom: 30px; }
              .company-name { font-size: 24px; font-weight: bold; color: #1e40af; }
              .order-title { text-align: center; font-size: 28px; color: #1e40af; margin: 30px 0; font-weight: bold; }
              .info-section { margin-bottom: 20px; }
              .field { margin: 8px 0; }
              .field label { font-weight: bold; display: inline-block; width: 150px; }
              .customer-section { background: #f3f4f6; padding: 15px; border-radius: 8px; margin-bottom: 30px; }
              .items-table { width: 100%; border-collapse: collapse; margin-bottom: 30px; }
              .items-table th { background: #2563eb; color: white; padding: 10px; text-align: left; }
              .items-table td { padding: 10px; border-bottom: 1px solid #e5e7eb; }
              .totals-section { text-align: right; margin-top: 20px; }
              .total-row { display: flex; justify-content: flex-end; margin: 5px 0; }
              .total-label { width: 150px; text-align: right; margin-right: 20px; }
              .total-value { width: 150px; text-align: right; }
              .grand-total { font-size: 18px; font-weight: bold; color: #1e40af; border-top: 2px solid #2563eb; padding-top: 10px; }
              .footer { margin-top: 30px; text-align: center; color: #666; font-size: 12px; }
            </style>
          </head>
          <body>
            <div class="header">
              <div class="company-name">LabGigs CRM System</div>
            </div>
            
            <div class="order-title">${title}</div>
            
            <div class="info-section">
              <div class="field">
                <label>${title} Number:</label> ${refNumber}
              </div>
              <div class="field">
                <label>Date:</label> ${sourceData.order_date || sourceData.invoice_date || sourceData.date || new Date().toLocaleDateString()}
              </div>
            </div>
            
            <div class="customer-section">
              <h3>Customer Information</h3>
              <div class="field">
                <label>Account Name:</label> ${sourceData.customer_name || sourceData.accountName || installation.account_name || 'N/A'}
              </div>
              <div class="field">
                <label>Contact Person:</label> ${sourceData.contact_person || sourceData.contactPerson || installation.contact_person || 'N/A'}
              </div>
              <div class="field">
                <label>Email:</label> ${sourceData.customer_email || sourceData.customerEmail || sourceData.email || installation.contact_email || 'N/A'}
              </div>
              <div class="field">
                <label>Phone:</label> ${sourceData.customer_phone || sourceData.customerPhone || sourceData.phone || installation.contact_phone || 'N/A'}
              </div>
              <div class="field">
                <label>Address:</label> ${sourceData.billing_address || sourceData.shipping_address || sourceData.address || installation.installation_address || 'N/A'}
              </div>
            </div>
            
            <table class="items-table">
              <thead>
                <tr>
                  <th style="width: 5%;">S.No</th>
                  <th style="width: 35%;">Product/Service</th>
                  <th style="width: 25%;">Description</th>
                  <th style="width: 8%;">Qty</th>
                  <th style="width: 12%;">Unit Price</th>
                  <th style="width: 8%;">Discount</th>
                  <th style="width: 12%;">Amount</th>
                </tr>
              </thead>
              <tbody>
                ${itemsData.map((item, index) => `
                  <tr>
                    <td>${index + 1}</td>
                    <td>${item.product}</td>
                    <td>${item.description}</td>
                    <td style="text-align: center;">${item.quantity}</td>
                    <td style="text-align: right;">₹${(item.unitPrice || 0).toLocaleString()}</td>
                    <td style="text-align: center;">${item.discount}%</td>
                    <td style="text-align: right;">₹${(item.amount || 0).toLocaleString()}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
            
            <div class="totals-section">
              <div class="total-row grand-total">
                <span class="total-label">Total Amount:</span>
                <span class="total-value">₹${(sourceData.total_amount || sourceData.amount || 0).toLocaleString()}</span>
              </div>
            </div>
            
            <div class="footer">
              <p><small>Generated on: ${new Date().toLocaleString()}</small></p>
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
      console.error('Error generating PDF:', error)
      toast({
        title: "Error",
        description: "Failed to generate PDF. Please try again.",
        variant: "destructive"
      })
    }
  }

  // Filter installations based on search and filters
  const filteredInstallations = installationsList.filter(installation => {
    const matchesSearch = searchTerm === "" || 
      installation.installation_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      installation.account_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      installation.product_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      installation.serial_number?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesStatus = selectedStatus === "All" || installation.status === selectedStatus
    const matchesWarrantyStatus = selectedWarrantyStatus === "All" || installation.warranty_status === selectedWarrantyStatus
    const matchesAssigned = selectedAssignedTo === "All" || installation.assigned_to === selectedAssignedTo

    return matchesSearch && matchesStatus && matchesWarrantyStatus && matchesAssigned
  })

  // Get unique assigned users for filter
  const uniqueAssignedUsers = Array.from(
    new Set(installationsList.map(installation => installation.assigned_to).filter(Boolean))
  )

  // Enhanced stats for the cards
  const stats = [
    {
      title: "Total Installations",
      value: installationsStatsState.total.toString(),
      change: `${installationsStatsState.pendingCount} pending`,
      icon: Wrench,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Completed",
      value: installationsStatsState.completed.toString(),
      change: `${Math.round((installationsStatsState.completed / Math.max(installationsStatsState.total, 1)) * 100)}% success rate`,
      icon: CheckCircle,
      iconColor: "text-green-600",
      iconBg: "bg-green-100"
    },
    {
      title: "In Progress",
      value: installationsStatsState.inProgress.toString(),
      change: "Active installations",
      icon: Settings,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    },
    {
      title: "Avg. Time",
      value: `${installationsStatsState.avgTime}`,
      change: "Days per installation",
      icon: Clock,
      iconColor: "text-purple-600",
      iconBg: "bg-purple-100"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Installations Management</h1>
          <p className="text-gray-600">Track and manage equipment installations</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button onClick={() => setIsAddInstallationModalOpen(true)}>
            <Plus className="w-4 h-4 mr-2" />
            Schedule Installation
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
          <CardTitle>Installation Search & Filters</CardTitle>
          <CardDescription>Filter and search through installations</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search installations by number, account, product, or serial..."
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
            <Select value={selectedWarrantyStatus} onValueChange={setSelectedWarrantyStatus}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Warranty" />
              </SelectTrigger>
              <SelectContent>
                {warrantyStatuses.map((warranty) => (
                  <SelectItem key={warranty} value={warranty}>
                    {warranty}
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

      {/* Installations List - Card View */}
      <Card>
        <CardHeader>
          <CardTitle>Installations List</CardTitle>
          <CardDescription>Showing {filteredInstallations.length} of {installationsList.length} installations</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredInstallations.length === 0 ? (
              <div className="text-center py-12">
                <Wrench className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No installations found</p>
                <p className="text-sm text-gray-400 mt-2">
                  {searchTerm || selectedStatus !== "All" || selectedWarrantyStatus !== "All" || selectedAssignedTo !== "All"
                    ? "Try adjusting your filters" 
                    : "Click 'Schedule Installation' to create your first installation"}
                </p>
              </div>
            ) : (
              filteredInstallations.map((installation) => (
                <div
                  key={installation.id}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center space-x-6">
                    <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                      <Wrench className="w-6 h-6 text-blue-600" />
                    </div>
                    
                    {/* Installation Details */}
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="font-semibold text-gray-900">
                          {installation.installation_number || installation.id?.substring(0, 8) || 'N/A'}
                        </h3>
                        <Badge className={getStatusColor(installation.status || 'Pending')}>
                          {installation.status || 'Pending'}
                        </Badge>
                        {installation.warranty_status && (
                          <Badge className={getWarrantyColor(installation.warranty_status)}>
                            {installation.warranty_status}
                          </Badge>
                        )}
                        {installation.sales_order_number && (
                          <Badge variant="outline" className="text-purple-600 border-purple-200">
                            SO: {installation.sales_order_number}
                          </Badge>
                        )}
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Installation: {formatDate(installation.installation_date)} 
                        {installation.installation_completion_date && ` | Completed: ${formatDate(installation.installation_completion_date)}`}
                      </p>
                    </div>
                    
                    {/* Account & Product Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Building className="w-4 h-4 text-gray-400" />
                        <span className="font-medium">{installation.account_name || 'Unknown Account'}</span>
                      </div>
                      
                      {/* PDF Link */}
                      <div className="flex items-center gap-1 text-sm mt-1">
                        <button
                          onClick={() => handleViewInstallationPDF(installation)}
                          className="flex items-center gap-1 text-blue-600 hover:text-blue-800 hover:underline transition-colors"
                          title="View Installation PDF"
                        >
                          <FileText className="w-3 h-3" />
                          <span>View Details PDF</span>
                        </button>
                      </div>
                      
                    </div>
                    
                    {/* Site & Assignment Details */}
                    <div className="border-l pl-6">
                      {installation.site_details && (
                        <div className="flex items-center gap-1 text-sm">
                          <MapPin className="w-4 h-4 text-gray-400" />
                          <span className="max-w-xs truncate" title={installation.site_details}>
                            {installation.site_details}
                          </span>
                        </div>
                      )}
                      {installation.assigned_to && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <User className="w-4 h-4 text-gray-400" />
                          <span>Assigned: {installation.assigned_to}</span>
                        </div>
                      )}
                      {installation.technician_name && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Settings className="w-3 h-3 text-gray-400" />
                          <span>Tech: {installation.technician_name}</span>
                        </div>
                      )}
                    </div>
                  </div>
                  
                  {/* Action Buttons */}
                  <div className="flex items-center space-x-1">
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="Edit Installation"
                      onClick={() => {
                        // Map database fields to modal form fields
                        const mappedInstallation = {
                          ...installation,
                          // Map database field names to modal form field names
                          customer_name: installation.account_name || '',
                          customer_phone: installation.contact_phone || '',
                          customer_email: installation.contact_email || '',
                          assigned_technician: installation.technician_name || '',
                          scheduled_date: installation.installation_date || '',
                          pre_installation_notes: installation.installation_notes || '',
                          product_model: installation.model_number || ''
                        }
                        setEditingInstallation(mappedInstallation)
                        setIsAddInstallationModalOpen(true)
                      }}
                    >
                      <Edit className="w-4 h-4" />
                    </Button>
                    {installation.status === 'Completed' && (
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Create AMC Contract"
                        className="text-purple-600 hover:text-purple-800 hover:bg-purple-50"
                        onClick={() => handleCreateAMCFromInstallation(installation)}
                      >
                        <Settings className="w-4 h-4" />
                      </Button>
                    )}
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="Create Complaint"
                      className="text-red-600 hover:text-red-800 hover:bg-red-50"
                      onClick={() => handleCreateComplaintFromInstallation(installation)}
                    >
                      <AlertCircle className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>

      {/* Add Installation Modal */}
      <AddInstallationModal
        isOpen={isAddInstallationModalOpen}
        onClose={() => {
          setIsAddInstallationModalOpen(false)
          setEditingInstallation(null)
        }}
        onSave={handleSaveInstallation}
        editingInstallation={editingInstallation}
        sourceType="direct"
      />

      {/* Add AMC Modal */}
      <AddAMCModal
        isOpen={isAddAMCModalOpen}
        onClose={() => {
          setIsAddAMCModalOpen(false)
          setSourceInstallation(null)
        }}
        onSave={handleSaveAMC}
        sourceData={sourceInstallation}
        sourceType="installation"
      />

      {/* Add Complaint Modal */}
      <AddComplaintModal
        isOpen={isAddComplaintModalOpen}
        onClose={() => {
          setIsAddComplaintModalOpen(false)
          setSourceInstallationForComplaint(null)
        }}
        onSave={handleSaveComplaint}
        sourceData={sourceInstallationForComplaint}
        sourceType="installation"
      />
    </div>
  )
}