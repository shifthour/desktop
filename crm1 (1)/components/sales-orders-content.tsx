"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Plus, Search, Download, Eye, Edit, ShoppingCart, Truck, Upload,
  TrendingUp, Calendar, IndianRupee, CheckCircle,
  Building, User, Phone, Mail, Package, DollarSign, Printer,
  FileText, Clock, Send
} from "lucide-react"
import { AddSalesOrderModal } from "@/components/add-sales-order-modal"
import { AddInvoiceModal } from "@/components/add-invoice-modal"
import { AddInstallationModal } from "@/components/add-installation-modal"
import { DataImportModal } from "@/components/data-import-modal"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"


const statuses = ["All", "Draft", "Confirmed", "Processing", "Shipped", "Delivered", "Cancelled"]
const paymentStatuses = ["All", "Pending", "Advance Received", "Paid", "Overdue"]

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg
    viewBox="0 0 24 24"
    className={className}
    fill="currentColor"
  >
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893A11.821 11.821 0 0020.891 3.426"/>
  </svg>
)

export function SalesOrdersContent() {
  const { toast } = useToast()
  const router = useRouter()
  const [salesOrdersList, setSalesOrdersList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [salesOrdersStats, setSalesOrdersStats] = useState({
    total: 0,
    totalValue: 0,
    pendingDelivery: 0,
    avgValue: 0,
    thisMonthCount: 0,
    deliveredCount: 0
  })
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedPaymentStatus, setSelectedPaymentStatus] = useState("All")
  const [selectedAssignedTo, setSelectedAssignedTo] = useState("All")
  const [isAddSalesOrderModalOpen, setIsAddSalesOrderModalOpen] = useState(false)
  const [isDataImportModalOpen, setIsDataImportModalOpen] = useState(false)
  const [editingOrderData, setEditingOrderData] = useState<any>(null)
  const [isInvoiceModalOpen, setIsInvoiceModalOpen] = useState(false)
  const [convertingSalesOrder, setConvertingSalesOrder] = useState<any>(null)
  const [isInstallationModalOpen, setIsInstallationModalOpen] = useState(false)
  const [installationSalesOrder, setInstallationSalesOrder] = useState<any>(null)
  const [companyLogo, setCompanyLogo] = useState<string>("")
  const [companyDetails, setCompanyDetails] = useState<any>(null)

  // Load sales orders from localStorage on component mount
  useEffect(() => {
    loadSalesOrders()
    loadCompanyDetails()
  }, [])

  const loadCompanyDetails = async () => {
    if (typeof window !== 'undefined') {
      const storedUser = localStorage.getItem('user')
      if (!storedUser) return
      
      try {
        const user = JSON.parse(storedUser)
        if (user.company_id) {
          const response = await fetch(`/api/admin/companies?companyId=${user.company_id}`)
          if (response.ok) {
            const company = await response.json()
            setCompanyDetails(company)
            if (company.logo_url) {
              setCompanyLogo(company.logo_url)
            }
          }
        }
      } catch (error) {
        console.error('Error loading company details:', error)
      }
    }
  }

  const loadSalesOrders = async () => {
    try {
      const response = await fetch('/api/sales-orders')
      if (response.ok) {
        const data = await response.json()
        const salesOrders = data.salesOrders || []
        console.log("loadSalesOrders - salesOrders from API:", salesOrders)
        setSalesOrdersList(salesOrders)
        
        // Calculate stats
        const pendingDelivery = salesOrders.filter(order => 
          order.status === 'confirmed' || order.status === 'processing' || order.status === 'shipped'
        ).length
        
        const deliveredCount = salesOrders.filter(order => order.status === 'delivered').length
        
        const totalValue = salesOrders.reduce((sum, order) => {
          return sum + (order.total_amount || 0)
        }, 0)
        
        const avgValue = salesOrders.length > 0 ? totalValue / salesOrders.length : 0
        
        // Calculate this month's orders
        const currentMonth = new Date().getMonth()
        const currentYear = new Date().getFullYear()
        const thisMonthCount = salesOrders.filter(order => {
          const orderDate = new Date(order.order_date || order.created_at)
          return orderDate.getMonth() === currentMonth && orderDate.getFullYear() === currentYear
        }).length
        
        setSalesOrdersStats({
          total: salesOrders.length,
          totalValue: totalValue,
          pendingDelivery: pendingDelivery,
          avgValue: avgValue,
          thisMonthCount: thisMonthCount,
          deliveredCount: deliveredCount
        })
      } else {
        console.error('Failed to fetch sales orders')
        // Fallback to localStorage
        const storedOrders = storageService.getAll<any>('salesOrders')
        setSalesOrdersList(storedOrders)
      }
    } catch (error) {
      console.error('Error loading sales orders:', error)
      // Fallback to localStorage
      const storedOrders = storageService.getAll<any>('salesOrders')
      setSalesOrdersList(storedOrders)
    }
  }
  
  const handleSaveSalesOrder = async (salesOrderData: any) => {
    console.log("handleSaveSalesOrder called with:", salesOrderData)
    
    // Check if this is an edit operation
    const isEditing = editingOrderData && editingOrderData.originalOrderId
    
    try {
      const response = await fetch('/api/sales-orders', {
        method: isEditing ? 'PUT' : 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(isEditing ? { ...salesOrderData, id: editingOrderData.originalOrderId } : salesOrderData)
      })
      
      if (response.ok) {
        const data = await response.json()
        const salesOrder = data.salesOrder
        console.log(isEditing ? "Sales order updated:" : "New sales order created:", salesOrder)
        
        // Refresh sales orders list
        await loadSalesOrders()
        
        toast({
          title: isEditing ? "Sales order updated" : "Sales order created",
          description: `Sales order ${salesOrder.order_id} has been successfully ${isEditing ? 'updated' : 'created'}.`
        })
        setIsAddSalesOrderModalOpen(false)
        setEditingOrderData(null)
      } else {
        throw new Error('Failed to create sales order')
      }
    } catch (error) {
      console.error("Failed to create sales order:", error)
      // Fallback to localStorage
      const newSalesOrder = storageService.create('salesOrders', salesOrderData)
      if (newSalesOrder) {
        loadSalesOrders()
        toast({
          title: "Sales order created (local)",
          description: `Sales order ${newSalesOrder.id || newSalesOrder.orderId} has been created locally.`
        })
        setIsAddSalesOrderModalOpen(false)
      } else {
        toast({
          title: "Error",
          description: "Failed to create sales order",
          variant: "destructive"
        })
      }
    }
  }
  
  const handleImportData = (importedOrders: any[]) => {
    console.log('Imported sales orders:', importedOrders)
    const createdOrders = storageService.createMany('salesOrders', importedOrders)
    
    // Immediately add imported orders to state
    setSalesOrdersList(prevOrders => [...prevOrders, ...createdOrders])
    
    // Update stats
    loadSalesOrders()
    
    toast({
      title: "Data imported",
      description: `Successfully imported ${createdOrders.length} sales orders.`
    })
  }
  
  // Get unique assigned users for filter
  const uniqueAssignedUsers = Array.from(
    new Set(salesOrdersList.map(order => order.assigned_to).filter(Boolean))
  )
  
  // Filter orders based on search and filters
  const filteredOrders = salesOrdersList.filter(order => {
    const matchesSearch = searchTerm === "" || 
      order.id?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      order.order_id?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      order.customer_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      order.contact_person?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesStatus = selectedStatus === "All" || order.status === selectedStatus.toLowerCase()
    const matchesPaymentStatus = selectedPaymentStatus === "All" || order.payment_status === selectedPaymentStatus.toLowerCase().replace(' ', '_')
    const matchesAssignedTo = selectedAssignedTo === "All" || order.assigned_to === selectedAssignedTo

    return matchesSearch && matchesStatus && matchesPaymentStatus && matchesAssignedTo
  })

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "draft":
        return "bg-gray-100 text-gray-800"
      case "confirmed":
        return "bg-blue-100 text-blue-800"
      case "processing":
        return "bg-yellow-100 text-yellow-800"
      case "shipped":
        return "bg-purple-100 text-purple-800"
      case "delivered":
        return "bg-green-100 text-green-800"
      case "cancelled":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getPaymentStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "paid":
        return "bg-green-100 text-green-800"
      case "advance received":
        return "bg-blue-100 text-blue-800"
      case "pending":
        return "bg-yellow-100 text-yellow-800"
      case "overdue":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const handleGenerateInvoice = async (salesOrder: any) => {
    console.log("Opening invoice modal for sales order:", salesOrder)
    
    // Enhanced sales order with detailed items data (same logic as print view)
    let enhancedSalesOrder = { ...salesOrder }
    
    // If there's a quotation reference, fetch the original quotation items
    if (salesOrder.quotation_ref) {
      try {
        console.log("Fetching quotation data for sales order:", salesOrder.quotation_ref)
        const response = await fetch(`/api/quotations?companyId=${companyDetails?.id || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'}`)
        if (response.ok) {
          const data = await response.json()
          const quotations = data.quotations || []
          
          const originalQuotation = quotations.find((q: any) => 
            q.quote_number === salesOrder.quotation_ref || 
            q.quotationId === salesOrder.quotation_ref ||
            q.id === salesOrder.quotation_ref
          )
          
          if (originalQuotation && originalQuotation.line_items) {
            console.log("Found original quotation items for invoice:", originalQuotation.line_items)
            
            // Process the items the same way as print view
            const processedItems = originalQuotation.line_items.map((item: any, index: number) => {
              // Clean up description by removing "Product from deal:" prefix
              let cleanDescription = item.description || ""
              if (cleanDescription.includes("Product from deal:")) {
                const parts = cleanDescription.split(" - ")
                if (parts.length > 1) {
                  cleanDescription = parts[parts.length - 1]
                } else {
                  cleanDescription = item.product || "Product description"
                }
              }
              
              return {
                id: (index + 1).toString(),
                product: item.product || "Product",
                description: cleanDescription || item.product || "",
                quantity: item.quantity || 1,
                unitPrice: item.unitPrice || item.unit_price || 0,
                discount: item.discount || 0,
                taxRate: item.taxRate || item.tax_rate || 18,
                amount: item.amount || item.total || (item.quantity * item.unitPrice) || 0
              }
            })
            
            // Add the processed items to the enhanced sales order
            enhancedSalesOrder.items = processedItems
            console.log("Enhanced sales order with items:", enhancedSalesOrder.items)
          }
        }
      } catch (error) {
        console.error("Error fetching quotation data for invoice:", error)
      }
    }
    
    setConvertingSalesOrder(enhancedSalesOrder)
    setIsInvoiceModalOpen(true)
  }

  const handleSaveInvoiceFromSalesOrder = async (invoiceData: any) => {
    console.log("handleSaveInvoiceFromSalesOrder called with:", invoiceData)
    
    try {
      // Call the invoice API
      const response = await fetch('/api/invoices', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(invoiceData)
      })

      if (response.ok) {
        const data = await response.json()
        const newInvoice = data.invoice
        console.log("New invoice created:", newInvoice)
        
        toast({
          title: "Invoice Created",
          description: `Invoice ${newInvoice?.invoice_number || invoiceData.invoice_number} has been successfully created from sales order ${convertingSalesOrder?.order_id || convertingSalesOrder?.id}.`
        })
        
        // Close modal and clear state
        setIsInvoiceModalOpen(false)
        setConvertingSalesOrder(null)
        
        // Navigate to invoices page after a delay to show the success message
        setTimeout(() => {
          window.location.href = '/invoices'
        }, 2000) // 2 second delay
      } else {
        const error = await response.text()
        console.error('API Error:', error)
        throw new Error('Failed to create invoice via API')
      }
    } catch (error) {
      console.error('Error creating invoice:', error)
      toast({
        title: "Error",
        description: "Failed to create invoice from sales order",
        variant: "destructive"
      })
    }
  }

  const handleScheduleInstallation = (salesOrder: any) => {
    setInstallationSalesOrder(salesOrder)
    setIsInstallationModalOpen(true)
  }

  const handleSaveInstallationFromSalesOrder = async (installationData: any) => {
    console.log('🔧 SALES ORDER INSTALLATION FUNCTION CALLED 🔧', installationData.annual_maintenance_charges)
    try {
      console.log('=== INSTALLATION CREATION DEBUG ===')
      console.log('Installation data being sent:', JSON.stringify(installationData, null, 2))
      
      const response = await fetch('/api/installations', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(installationData)
      })

      if (response.ok) {
        const data = await response.json()
        const newInstallation = data.installation

        // Auto-create AMC record if annual maintenance charges > 0
        console.log('=== AMC AUTO-CREATION CHECK (SALES ORDER) ===')
        console.log('annual_maintenance_charges:', installationData.annual_maintenance_charges)
        console.log('parsed amount:', parseFloat(installationData.annual_maintenance_charges))
        
        if (installationData.annual_maintenance_charges && parseFloat(installationData.annual_maintenance_charges) > 0) {
          console.log('=== CREATING AMC RECORD FROM SALES ORDER ===');
          try {
            // Calculate AMC date (installation date + 1 year)
            const installationDate = new Date(installationData.scheduled_date || installationData.installation_date)
            const amcDate = new Date(installationDate)
            amcDate.setFullYear(amcDate.getFullYear() + 1)

            const amcData = {
              // Customer Information (matching existing table structure)
              customer_name: installationData.customer_name || installationData.account_name,
              contact_person: installationData.contact_person,
              customer_phone: installationData.customer_phone || '',
              customer_email: installationData.customer_email || '',
              service_address: installationData.installation_address,
              city: installationData.city,
              state: installationData.state,
              pincode: installationData.pincode || '',
              
              // Required product fields (based on error message)
              product_name: installationData.product_name || 'Laboratory Equipment',
              
              // Equipment details as JSON array
              equipment_details: [{
                equipment: installationData.product_name || 'Laboratory Equipment',
                model: installationData.product_model || '',
                serial: installationData.serial_number || '',
                installation_date: installationData.installation_date
              }],
              
              // Contract Details
              contract_type: 'comprehensive',
              contract_value: parseFloat(installationData.annual_maintenance_charges),
              
              // Contract Period (using correct field names from error message)
              contract_start_date: amcDate.toISOString().split('T')[0],
              contract_end_date: new Date(amcDate.getFullYear() + 1, amcDate.getMonth(), amcDate.getDate()).toISOString().split('T')[0],
              duration_months: 12,
              
              // Service Details
              service_frequency: 'Annual',
              number_of_services: 1,
              services_completed: 0,
              
              // Coverage Details
              labour_charges_included: true,
              spare_parts_included: false,
              emergency_support: false,
              response_time_hours: 24,
              
              // Status
              status: 'Active',
              
              // Reference to installation
              installation_id: newInstallation.id,
              installation_number: newInstallation.installation_number,
              
              // Source information
              source_type: 'installation',
              source_reference: newInstallation.installation_number,
              
              // Special instructions
              special_instructions: `Auto-created from installation: ${newInstallation.installation_number}`
            }

            console.log('AMC Data being sent:', JSON.stringify(amcData, null, 2))
            console.log('Service frequency value:', amcData.service_frequency)
            
            const amcResponse = await fetch('/api/amc', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
              },
              body: JSON.stringify(amcData),
            })

            if (amcResponse.ok) {
              const amcResult = await amcResponse.json()
              console.log('AMC record created from sales order:', amcResult.amcContract.amc_number)
              
              toast({
                title: "Installation & AMC Scheduled",
                description: `Installation ${newInstallation.installation_number} and AMC ${amcResult.amcContract.amc_number} have been created successfully`
              })
            } else {
              console.error('Failed to create AMC record')
              toast({
                title: "Installation Scheduled",
                description: `Installation ${newInstallation.installation_number} has been scheduled for ${installationData.customer_name}`
              })
            }
          } catch (amcError) {
            console.error('Error creating AMC record:', amcError)
            // Don't fail the installation creation if AMC creation fails
            toast({
              title: "Installation Scheduled",
              description: `Installation ${newInstallation.installation_number} has been scheduled for ${installationData.customer_name}`
            })
          }
        } else {
          toast({
            title: "Installation Scheduled",
            description: `Installation ${newInstallation.installation_number} has been scheduled for ${installationData.customer_name}`
          })
        }

        setIsInstallationModalOpen(false)
        setInstallationSalesOrder(null)
        
        // Navigate to installations page
        router.push('/installations')
      } else {
        // Get the actual error from the API response
        const errorData = await response.json().catch(() => ({ error: 'Unknown error' }))
        console.error('API Error Response:', errorData)
        throw new Error(errorData.error || `HTTP ${response.status}: Failed to create installation`)
      }
    } catch (error) {
      console.error('Error scheduling installation:', error)
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to schedule installation from sales order",
        variant: "destructive"
      })
    }
  }

  // Enhanced stats for the cards
  const stats = [
    {
      title: "Total Orders",
      value: salesOrdersStats.total.toString(),
      change: `+${salesOrdersStats.thisMonthCount} this month`,
      icon: ShoppingCart,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Order Value",
      value: `₹${(salesOrdersStats.totalValue / 10000000).toFixed(1)}Cr`,
      change: "This quarter",
      icon: IndianRupee,
      iconColor: "text-green-600",
      iconBg: "bg-green-100"
    },
    {
      title: "Pending Delivery",
      value: salesOrdersStats.pendingDelivery.toString(),
      change: salesOrdersStats.pendingDelivery > 0 ? "Need attention" : "All clear",
      icon: Truck,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    },
    {
      title: "Delivered",
      value: salesOrdersStats.deliveredCount.toString(),
      change: "Successfully completed",
      icon: CheckCircle,
      iconColor: "text-purple-600",
      iconBg: "bg-purple-100"
    }
  ]

  // Handler functions for action buttons
  const handleViewOrder = (order: any) => {
    console.log("Viewing sales order:", order)
    toast({
      title: "Order Details",
      description: `Sales Order ${order.order_id || order.id} - ${order.customer}`
    })
    // TODO: Implement view modal or navigate to details page
  }

  const handleSendWhatsApp = (order: any) => {
    const companyName = companyDetails?.company_name || companyDetails?.name || 'Our Company'
    const orderNumber = order.order_id || order.id
    const customerName = order.customer_name || order.customer || 'Valued Customer'
    const orderDate = order.order_date || order.created_at?.split('T')[0] || new Date().toISOString().split('T')[0]
    const deliveryDate = order.delivery_date || order.expected_delivery
    const amount = order.total_amount || order.amount
    const product = order.product || order.products || 'Product/Service'
    const assignedTo = order.assigned_to || 'Sales Team'
    
    const message = `Hi ${customerName},\n\nHope you are doing well!\n\nPlease find attached sales order ${orderNumber} for ${product} for your reference.\n\n📋 *Sales Order Details:*\n📅 Order Date: ${orderDate}\n${deliveryDate ? `🚚 Expected Delivery: ${deliveryDate}\n` : ''}💰 Amount: ₹${amount?.toLocaleString() || 'TBD'}\n\nYour order is confirmed and we will keep you updated on the progress.\n\nLet me know if you have any questions or need any clarifications.\n\nThanks & Regards,\n${assignedTo}`
    
    const phoneNumber = (order.customer_phone || order.phone)?.replace(/[^0-9]/g, '')
    if (phoneNumber && phoneNumber.length >= 10) {
      const whatsappLink = `https://wa.me/${phoneNumber.startsWith('91') ? phoneNumber : '91' + phoneNumber}?text=${encodeURIComponent(message)}`
      window.open(whatsappLink, '_blank')
      
      toast({
        title: "WhatsApp opened",
        description: `Sales Order ${orderNumber} message prepared for ${order.customer_name}`
      })
    } else {
      toast({
        title: "No WhatsApp number",
        description: "Customer WhatsApp/phone number is not available for this sales order",
      })
    }
  }

  const handleEditOrder = async (order: any) => {
    console.log("Editing sales order:", order)
    
    let itemsData = [{
      id: "1",
      product: "Products/Services", // Generic placeholder
      description: "Multiple items as per quotation",
      quantity: 1,
      unitPrice: order.subtotal || order.total_amount || 0,
      discount: 0,
      taxRate: 18,
      amount: order.total_amount || 0
    }]

    // If there's a quotation reference, try to fetch the original quotation items
    if (order.quotation_ref) {
      try {
        const response = await fetch(`/api/quotations?companyId=${companyDetails?.id || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'}`)
        if (response.ok) {
          const data = await response.json()
          const quotations = data.quotations || []
          
          // Find the quotation that matches the reference
          const originalQuotation = quotations.find((q: any) => 
            q.quote_number === order.quotation_ref || 
            q.quotationId === order.quotation_ref ||
            q.id === order.quotation_ref
          )
          
          if (originalQuotation && originalQuotation.line_items) {
            console.log("Found original quotation with items:", originalQuotation.line_items)
            // Use the actual items from the quotation
            itemsData = originalQuotation.line_items.map((item: any, index: number) => {
              // Clean up description by removing "Product from deal:" prefix and account info
              let cleanDescription = item.description || ""
              if (cleanDescription.includes("Product from deal:")) {
                // Extract just the product part after the last " - "
                const parts = cleanDescription.split(" - ")
                if (parts.length > 1) {
                  cleanDescription = parts[parts.length - 1] // Get the last part (product name)
                } else {
                  cleanDescription = item.product || "Product description"
                }
              }
              
              return {
                id: (index + 1).toString(),
                product: item.product || "Product",
                description: cleanDescription || item.product || "",
                quantity: item.quantity || 1,
                unitPrice: item.unitPrice || item.unit_price || 0,
                discount: item.discount || 0,
                taxRate: item.taxRate || item.tax_rate || 18,
                amount: item.amount || item.total || (item.quantity * item.unitPrice) || 0
              }
            })
          }
        }
      } catch (error) {
        console.error("Error fetching quotation data:", error)
      }
    }
    
    // Convert sales order data to match the modal's expected format
    const editData = {
      accountName: order.customer_name,
      contactPerson: order.contact_person,
      customerEmail: order.customer_email,
      customerPhone: order.customer_phone,
      billingAddress: order.billing_address,
      shippingAddress: order.shipping_address,
      orderDate: order.order_date,
      quotationRef: order.quotation_ref,
      customerPO: order.customer_po,
      expectedDelivery: order.expected_delivery,
      notes: order.notes,
      assignedTo: order.assigned_to,
      priority: order.priority,
      currency: order.currency,
      paymentStatus: order.payment_status,
      paymentTerms: order.payment_terms,
      advanceAmount: order.advance_amount?.toString() || "",
      warrantyPeriod: order.warranty_period,
      installationRequired: order.installation_required || false,
      trainingRequired: order.training_required || false,
      serviceContract: order.service_contract || false,
      // Store the original order ID for updating
      originalOrderId: order.id,
      // Use the fetched items data
      items: itemsData
    }
    
    setEditingOrderData(editData)
    setIsAddSalesOrderModalOpen(true)
    
    toast({
      title: "Edit Order",
      description: `Opening edit form for Sales Order ${order.order_id || order.id}`
    })
  }

  const handleDownloadOrder = async (order: any) => {
    console.log("Downloading sales order:", order)
    
    // Get current logged-in user for "Prepared by"
    const currentUser = (() => {
      if (typeof window !== 'undefined') {
        const storedUser = localStorage.getItem('user')
        if (storedUser) {
          try {
            const user = JSON.parse(storedUser)
            return user.full_name || user.name || 'Admin'
          } catch (e) {
            return 'Admin'
          }
        }
      }
      return 'Admin'
    })()

    // Fetch original quotation items if available
    let itemsData = [{
      product: order.product || order.products || 'Product/Service',
      description: order.product_description || order.description || '',
      quantity: order.quantity || 1,
      unitPrice: order.unit_price || order.total_amount || order.amount || 0,
      discount: order.discount || 0,
      amount: order.total_amount || order.amount || 0
    }]

    if (order.quotation_ref) {
      try {
        const response = await fetch(`/api/quotations?companyId=${companyDetails?.id || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'}`)
        if (response.ok) {
          const data = await response.json()
          const quotations = data.quotations || []
          
          const originalQuotation = quotations.find((q: any) => 
            q.quote_number === order.quotation_ref || 
            q.quotationId === order.quotation_ref ||
            q.id === order.quotation_ref
          )
          
          if (originalQuotation && originalQuotation.line_items) {
            console.log("Found original quotation items for download:", originalQuotation.line_items)
            itemsData = originalQuotation.line_items.map((item: any) => {
              // Clean up description by removing "Product from deal:" prefix and account info
              let cleanDescription = item.description || ""
              if (cleanDescription.includes("Product from deal:")) {
                // Extract just the product part after the last " - "
                const parts = cleanDescription.split(" - ")
                if (parts.length > 1) {
                  cleanDescription = parts[parts.length - 1] // Get the last part (product name)
                } else {
                  cleanDescription = item.product || "Product description"
                }
              }
              
              return {
                product: item.product || "Product",
                description: cleanDescription || item.product || "",
                quantity: item.quantity || 1,
                unitPrice: item.unitPrice || item.unit_price || 0,
                discount: item.discount || 0,
                amount: item.amount || item.total || (item.quantity * item.unitPrice) || 0
              }
            })
          }
        }
      } catch (error) {
        console.error("Error fetching quotation data for download:", error)
      }
    }
    
    // Create professional sales order HTML content for PDF download
    const htmlContent = `
      <!DOCTYPE html>
      <html>
      <head>
        <title>Sales Order - ${order.order_id || order.id}</title>
        <style>
          @page { size: A4; margin: 20mm; }
          body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
          .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #2563eb; padding-bottom: 20px; margin-bottom: 30px; }
          .logo-section { flex: 1; }
          .logo { max-width: 200px; max-height: 80px; }
          .company-info { flex: 1; text-align: right; }
          .company-name { font-size: 24px; font-weight: bold; color: #1e40af; }
          .order-title { text-align: center; font-size: 28px; color: #1e40af; margin: 30px 0; font-weight: bold; }
          .order-info { display: flex; justify-content: space-between; margin-bottom: 30px; }
          .info-section { flex: 1; }
          .info-label { font-weight: bold; color: #666; }
          .customer-section { background: #f3f4f6; padding: 15px; border-radius: 8px; margin-bottom: 30px; }
          .items-table { width: 100%; border-collapse: collapse; margin-bottom: 30px; }
          .items-table th { background: #2563eb; color: white; padding: 10px; text-align: left; }
          .items-table td { padding: 10px; border-bottom: 1px solid #e5e7eb; }
          .totals-section { text-align: right; margin-top: 20px; }
          .total-row { display: flex; justify-content: flex-end; margin: 5px 0; }
          .total-label { width: 150px; text-align: right; margin-right: 20px; }
          .total-value { width: 150px; text-align: right; }
          .grand-total { font-size: 18px; font-weight: bold; color: #1e40af; border-top: 2px solid #2563eb; padding-top: 10px; }
          .terms-section { margin-top: 40px; padding: 15px; background: #f9fafb; border-radius: 8px; }
          .footer { margin-top: 50px; text-align: center; color: #666; font-size: 12px; }
          .signature-section { display: flex; justify-content: space-between; margin-top: 60px; }
          .signature-box { width: 200px; text-align: center; }
          .signature-line { border-top: 1px solid #333; margin-top: 50px; }
        </style>
      </head>
      <body>
        <div class="header">
          <div class="logo-section">
            ${companyLogo ? `<img src="${companyLogo}" alt="Company Logo" class="logo" />` : ''}
          </div>
          <div class="company-info">
            <div class="company-name">${companyDetails?.company_name || companyDetails?.name || 'Your Company Name'}</div>
            <div>${companyDetails?.address || ''}</div>
            <div>${companyDetails?.phone || ''}</div>
            <div>${companyDetails?.email || ''}</div>
            ${companyDetails?.website ? `<div>${companyDetails.website}</div>` : ''}
            ${companyDetails?.gst_number ? `<div>GST: ${companyDetails.gst_number}</div>` : ''}
          </div>
        </div>
        
        <h1 class="order-title">SALES ORDER</h1>
        
        <div class="order-info">
          <div class="info-section">
            <div><span class="info-label">Order No:</span> ${order.order_id || order.id}</div>
            <div><span class="info-label">Date:</span> ${order.order_date || order.created_at?.split('T')[0] || new Date().toLocaleDateString()}</div>
            <div><span class="info-label">Expected Delivery:</span> ${order.expected_delivery || order.delivery_date || 'TBD'}</div>
            ${order.reference_number || order.reference ? `<div><span class="info-label">Reference:</span> ${order.reference_number || order.reference}</div>` : ''}
          </div>
          <div class="info-section" style="text-align: right;">
            <div><span class="info-label">Prepared By:</span> ${currentUser}</div>
          </div>
        </div>
        
        <div class="customer-section">
          <h3 style="margin-top: 0;">Bill To:</h3>
          <div><strong>${order.customer_name || order.customer || 'N/A'}</strong></div>
          <div>Attn: ${order.contact_person || order.contact || 'N/A'}</div>
          ${order.customer_email || order.email ? `<div>Email: ${order.customer_email || order.email}</div>` : ''}
          ${order.customer_phone || order.phone ? `<div>Phone: ${order.customer_phone || order.phone}</div>` : ''}
          ${order.billing_address || order.billingAddress ? `<div>${(order.billing_address || order.billingAddress).replace(/\n/g, '<br>')}</div>` : ''}
        </div>
        
        <table class="items-table">
          <thead>
            <tr>
              <th style="width: 5%;">S.No</th>
              <th style="width: 35%;">Product/Service</th>
              <th style="width: 20%;">Description</th>
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
                <td style="text-align: right;">₹${item.unitPrice.toLocaleString()}</td>
                <td style="text-align: center;">${item.discount}%</td>
                <td style="text-align: right;">₹${item.amount.toLocaleString()}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
        
        <div class="totals-section">
          ${(() => {
            const totalAmount = order.total_amount || order.amount || 0;
            const subtotalAmount = order.subtotal_amount || (totalAmount / 1.18);
            const taxAmount = order.tax_amount || (totalAmount - subtotalAmount);
            return `
              <div class="total-row">
                <span class="total-label">Subtotal:</span>
                <span class="total-value">₹${Math.round(subtotalAmount).toLocaleString()}</span>
              </div>
              ${order.discount_amount && order.discount_amount > 0 ? `
              <div class="total-row">
                <span class="total-label">Discount:</span>
                <span class="total-value">- ₹${order.discount_amount.toLocaleString()}</span>
              </div>
              ` : ''}
              <div class="total-row">
                <span class="total-label">Tax (${order.tax_type || 'GST'} @ 18%):</span>
                <span class="total-value">₹${Math.round(taxAmount).toLocaleString()}</span>
              </div>
              <div class="total-row grand-total">
                <span class="total-label">Grand Total:</span>
                <span class="total-value">₹${totalAmount.toLocaleString()}</span>
              </div>
            `;
          })()}
        </div>
        
        <div class="footer">
          <p><strong>Thank you for your business!</strong></p>
          <p>This is a computer-generated sales order and does not require a signature.</p>
          ${companyDetails?.website ? `<p>Visit us at: ${companyDetails.website}</p>` : ''}
        </div>
      </body>
      </html>
    `
    
    // Create a new window for PDF generation and download
    const printWindow = window.open('', '_blank')
    if (printWindow) {
      printWindow.document.write(htmlContent)
      printWindow.document.close()
      
      // Wait for content to load then trigger download
      setTimeout(() => {
        printWindow.print()
        // Note: The user will need to "Save as PDF" instead of printing to a printer
      }, 500)
    }
    
    toast({
      title: "Sales Order PDF ready",
      description: `Sales Order ${order.order_id || order.id} is ready to download. Use "Save as PDF" in the print dialog.`
    })
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Sales Orders</h1>
          <p className="text-gray-600">Manage and track your sales orders</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button onClick={() => setIsDataImportModalOpen(true)} variant="outline">
            <Upload className="w-4 h-4 mr-2" />
            Import Orders
          </Button>
          <Button onClick={() => setIsAddSalesOrderModalOpen(true)} className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Create Order
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

      {/* Sales Order Search & Filters */}
      <Card>
        <CardHeader>
          <CardTitle>Sales Order Search & Filters</CardTitle>
          <CardDescription>Filter and search through your sales orders</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search orders by ID, account, contact, or product..."
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
                <SelectItem value="All">All Statuses</SelectItem>
                {statuses.filter(s => s !== "All").map((status) => (
                  <SelectItem key={status} value={status}>
                    {status}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedPaymentStatus} onValueChange={setSelectedPaymentStatus}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Payment" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="All">All Payment</SelectItem>
                {paymentStatuses.filter(s => s !== "All").map((status) => (
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

      {/* Sales Orders List - Card View */}
      <Card>
        <CardHeader>
          <CardTitle>Sales Orders List</CardTitle>
          <CardDescription>Showing {filteredOrders.length} of {salesOrdersList.length} sales orders</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredOrders.length === 0 ? (
              <div className="text-center py-12">
                <ShoppingCart className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No sales orders found</p>
                <p className="text-sm text-gray-400 mt-2">
                  {searchTerm || selectedStatus !== "All" || selectedPaymentStatus !== "All" || selectedAssignedTo !== "All"
                    ? "Try adjusting your filters" 
                    : "Click 'Create Order' to create your first sales order"}
                </p>
              </div>
            ) : (
              filteredOrders.map((order) => (
                <div
                  key={order.id || order.order_id}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center space-x-6">
                    <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                      <ShoppingCart className="w-6 h-6 text-blue-600" />
                    </div>
                    
                    {/* Order Details */}
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="font-semibold text-gray-900">
                          {order.order_id || order.id}
                        </h3>
                        <Badge className={getStatusColor(order.status)}>
                          {order.status}
                        </Badge>
                        <Badge className={getPaymentStatusColor(order.payment_status)}>
                          {order.payment_status?.replace('_', ' ')}
                        </Badge>
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Date: {order.order_date} | Delivery: {order.expected_delivery || order.delivery_date}
                      </p>
                    </div>
                    
                    {/* Customer Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Building className="w-4 h-4 text-gray-400" />
                        <span className="font-medium">{order.customer_name}</span>
                      </div>
                      <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                        <User className="w-4 h-4 text-gray-400" />
                        <span>{order.contact_person}</span>
                      </div>
                      {order.customer_phone && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Phone className="w-3 h-3 text-gray-400" />
                          <span>{order.customer_phone}</span>
                        </div>
                      )}
                    </div>
                    
                    {/* Product & Value Details */}
                    <div className="border-l pl-6">
                      {order.product && (
                        <div className="flex items-center gap-1 text-sm">
                          <Package className="w-4 h-4 text-gray-400" />
                          <span className="font-medium max-w-xs truncate" title={order.product}>{order.product}</span>
                        </div>
                      )}
                      <div className="flex items-center gap-1 text-sm mt-1">
                        <span className="font-semibold text-green-600">₹{order.total_amount?.toLocaleString()}</span>
                      </div>
                      {order.assigned_to && (
                        <p className="text-xs text-gray-500 mt-1">
                          Assigned to: {order.assigned_to}
                        </p>
                      )}
                    </div>
                  </div>
                  
                  {/* Action Buttons - Three Rows */}
                  <div className="flex flex-col items-end space-y-2">
                    {/* First Row - Main Actions */}
                    <div className="flex items-center space-x-1">
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Edit Order"
                        onClick={() => handleEditOrder(order)}
                      >
                        <Edit className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Download Order"
                        onClick={() => handleDownloadOrder(order)}
                      >
                        <Download className="w-4 h-4" />
                      </Button>
                    </div>
                    
                    {/* Second Row - Communication Actions */}
                    <div className="flex items-center space-x-1">
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Send WhatsApp"
                        onClick={() => handleSendWhatsApp(order)}
                        className="text-green-600 hover:text-green-800 hover:bg-green-50"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Send Email"
                        className="text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                      >
                        <Mail className="w-4 h-4" />
                      </Button>
                    </div>
                    
                    {/* Third Row - Business Actions */}
                    <div className="flex items-center space-x-1">
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Generate Invoice"
                        onClick={() => handleGenerateInvoice(order)}
                        className="text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                      >
                        <FileText className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Schedule Installation"
                        onClick={() => handleScheduleInstallation(order)}
                        className="text-green-600 hover:text-green-800 hover:bg-green-50"
                      >
                        <Truck className="w-4 h-4" />
                      </Button>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>

      {/* Add Sales Order Modal */}
      <AddSalesOrderModal
        isOpen={isAddSalesOrderModalOpen}
        onClose={() => {
          setIsAddSalesOrderModalOpen(false)
          setEditingOrderData(null)
        }}
        onSave={handleSaveSalesOrder}
        editingData={editingOrderData}
      />
      
      {/* Add Invoice Modal for Sales Order Conversion */}
      <AddInvoiceModal
        isOpen={isInvoiceModalOpen}
        onClose={() => {
          setIsInvoiceModalOpen(false)
          setConvertingSalesOrder(null)
        }}
        onSave={handleSaveInvoiceFromSalesOrder}
        editingData={convertingSalesOrder}
      />
      
      {/* DataImportModal component */}
      <DataImportModal 
        isOpen={isDataImportModalOpen} 
        onClose={() => setIsDataImportModalOpen(false)}
        moduleType="salesOrders"
        onImport={handleImportData}
      />

      {/* Installation Modal */}
      <AddInstallationModal
        isOpen={isInstallationModalOpen}
        onClose={() => {
          setIsInstallationModalOpen(false)
          setInstallationSalesOrder(null)
        }}
        onSave={handleSaveInstallationFromSalesOrder}
        sourceType="sales_order"
        sourceData={installationSalesOrder}
      />
    </div>
  )
}
