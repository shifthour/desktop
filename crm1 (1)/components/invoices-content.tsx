"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Receipt, IndianRupee, Clock, CheckCircle2, AlertCircle, Search, Plus, Edit, 
  Building, User, Phone, Mail, Package, Download, Truck,
  TrendingUp, TrendingDown, FileText, Upload, Printer, Banknote
} from "lucide-react"
import { AddInvoiceModal } from "@/components/add-invoice-modal"
import { AddInstallationModal } from "@/components/add-installation-modal"
import { DataImportModal } from "@/components/data-import-modal"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"

// WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
  </svg>
)

export function InvoicesContent() {
  const { toast } = useToast()
  const router = useRouter()
  const [invoicesList, setInvoicesList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [invoicesStatsState, setInvoicesStatsState] = useState({
    total: 0,
    outstanding: 0,
    paidThisMonth: 0,
    overdue: 0,
    thisMonthCount: 0,
    totalValue: 0
  })
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedPaymentStatus, setSelectedPaymentStatus] = useState("All")
  const [selectedAssignedTo, setSelectedAssignedTo] = useState("All")
  const [isAddInvoiceModalOpen, setIsAddInvoiceModalOpen] = useState(false)
  const [isDataImportModalOpen, setIsDataImportModalOpen] = useState(false)
  const [isInstallationModalOpen, setIsInstallationModalOpen] = useState(false)
  const [installationInvoice, setInstallationInvoice] = useState<any>(null)
  const [companyLogo, setCompanyLogo] = useState<string>("")
  const [companyDetails, setCompanyDetails] = useState<any>(null)

  // Load invoices and company details on component mount
  useEffect(() => {
    loadInvoices()
    loadCompanyDetails()
  }, [])

  const loadCompanyDetails = async () => {
    if (typeof window !== 'undefined') {
      const storedUser = localStorage.getItem('user')
      if (!storedUser) {
        // Fallback to default logo if no user
        setCompanyLogo('/company-logo.png')
        return
      }
      
      try {
        const user = JSON.parse(storedUser)
        if (user.company_id) {
          const response = await fetch(`/api/admin/companies?companyId=${user.company_id}`)
          if (response.ok) {
            const company = await response.json()
            setCompanyDetails(company)
            if (company.logo_url) {
              setCompanyLogo(company.logo_url)
            } else {
              // Fallback to default logo
              setCompanyLogo('/company-logo.png')
            }
          } else {
            // Fallback if API fails
            setCompanyLogo('/company-logo.png')
          }
        } else {
          // Fallback if no company_id
          setCompanyLogo('/company-logo.png')
        }
      } catch (error) {
        console.error('Error loading company details:', error)
        // Fallback on error
        setCompanyLogo('/company-logo.png')
      }
    }
  }

  const loadInvoices = async () => {
    try {
      const response = await fetch('/api/invoices')
      if (response.ok) {
        const data = await response.json()
        const invoices = data.invoices || []
        console.log("loadInvoices - invoices from API:", invoices)
        setInvoicesList(invoices)
        
        // Calculate stats
        const outstanding = invoices.filter(inv => inv.payment_status === 'unpaid' || inv.status === 'sent').reduce((sum, inv) => {
          return sum + (inv.total_amount || 0)
        }, 0)
        
        const paidThisMonth = invoices.filter(inv => {
          const paidDate = new Date(inv.payment_date || inv.invoice_date)
          return inv.payment_status === 'paid' && paidDate.getMonth() === new Date().getMonth()
        }).reduce((sum, inv) => {
          return sum + (inv.total_amount || 0)
        }, 0)
        
        const overdue = invoices.filter(inv => inv.status === 'overdue').length
        
        // Calculate this month's invoices
        const currentMonth = new Date().getMonth()
        const currentYear = new Date().getFullYear()
        const thisMonthCount = invoices.filter(inv => {
          const invoiceDate = new Date(inv.invoice_date || inv.created_at)
          return invoiceDate.getMonth() === currentMonth && invoiceDate.getFullYear() === currentYear
        }).length
        
        const totalValue = invoices.reduce((sum, inv) => {
          return sum + (inv.total_amount || 0)
        }, 0)
        
        setInvoicesStatsState({
          total: invoices.length,
          outstanding: outstanding,
          paidThisMonth: paidThisMonth,
          overdue: overdue,
          thisMonthCount: thisMonthCount,
          totalValue: totalValue
        })
      } else {
        console.error('Failed to fetch invoices')
        // Fallback to localStorage
        let storedInvoices = storageService.getAll<any>('invoices')
        if (storedInvoices.length === 0) {
          invoices.forEach(invoice => {
            storageService.create('invoices', invoice)
          })
          storedInvoices = storageService.getAll<any>('invoices')
        }
        setInvoicesList(storedInvoices)
      }
    } catch (error) {
      console.error('Error loading invoices:', error)
      // Fallback to localStorage
      let storedInvoices = storageService.getAll<any>('invoices')
      if (storedInvoices.length === 0) {
        invoices.forEach(invoice => {
          storageService.create('invoices', invoice)
        })
        storedInvoices = storageService.getAll<any>('invoices')
      }
      setInvoicesList(storedInvoices)
    }
  }
  
  const handleSaveInvoice = async (invoiceData: any) => {
    console.log("handleSaveInvoice called with:", invoiceData)
    
    try {
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
        
        // Refresh invoices list
        await loadInvoices()
        
        toast({
          title: "Invoice created",
          description: `Invoice ${newInvoice.invoice_number} has been successfully created.`
        })
        setIsAddInvoiceModalOpen(false)
      } else {
        throw new Error('Failed to create invoice')
      }
    } catch (error) {
      console.error("Failed to create invoice:", error)
      // Fallback to localStorage
      const newInvoice = storageService.create('invoices', invoiceData)
      if (newInvoice) {
        loadInvoices()
        toast({
          title: "Invoice created (local)",
          description: `Invoice ${newInvoice.id || newInvoice.invoiceId} has been created locally.`
        })
        setIsAddInvoiceModalOpen(false)
      } else {
        toast({
          title: "Error",
          description: "Failed to create invoice",
          variant: "destructive"
        })
      }
    }
  }
  
  const handleImportData = (importedInvoices: any[]) => {
    console.log('Imported invoices:', importedInvoices)
    const createdInvoices = storageService.createMany('invoices', importedInvoices)
    
    // Immediately add imported invoices to state
    setInvoicesList(prevInvoices => [...prevInvoices, ...createdInvoices])
    
    // Update stats
    loadInvoices()
    
    toast({
      title: "Data imported",
      description: `Successfully imported ${createdInvoices.length} invoices.`
    })
  }
  
  // Invoice action handlers
  const handleDownloadInvoice = async (invoice: any) => {
    console.log("Downloading invoice:", invoice)
    
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

    // Fetch original sales order items if available
    let itemsData = [{
      product: invoice.items || invoice.item_description || 'Products/Services',
      description: invoice.description || invoice.item_description || '',
      quantity: invoice.quantity || 1,
      unitPrice: invoice.unit_price || invoice.total_amount || invoice.amount || 0,
      discount: 0,
      amount: invoice.total_amount || invoice.amount || 0
    }]
    
    // Check if this invoice was created directly from a quotation
    if (invoice.notes && invoice.notes.includes('Generated from Quotation:')) {
      try {
        const quotationMatch = invoice.notes.match(/Generated from Quotation:\s*([A-Z0-9-]+)/)
        if (quotationMatch) {
          const quotationNumber = quotationMatch[1]
          console.log("Fetching original quotation data for:", quotationNumber)
          
          const quotationResponse = await fetch(`/api/quotations`)
          if (quotationResponse.ok) {
            const quotationData = await quotationResponse.json()
            const quotations = quotationData.quotations || []
            
            const originalQuotation = quotations.find((q: any) => 
              q.quote_number === quotationNumber || 
              q.quotationId === quotationNumber ||
              q.id === quotationNumber
            )
            
            if (originalQuotation && originalQuotation.line_items && originalQuotation.line_items.length > 0) {
              console.log("Found original quotation line items for direct invoice:", originalQuotation.line_items)
              itemsData = originalQuotation.line_items.map((item: any) => {
                // Clean up description
                let cleanDescription = item.description || ""
                if (cleanDescription.includes("Product from deal:")) {
                  const parts = cleanDescription.split(" - ")
                  if (parts.length > 1) {
                    cleanDescription = parts[parts.length - 1]
                  } else {
                    cleanDescription = item.product || ""
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
        }
      } catch (error) {
        console.error("Error fetching quotation data for direct invoice:", error)
      }
    }

    // Try to fetch original sales order data if sales_order_ref exists or notes mention sales order
    let salesOrderRef = invoice.sales_order_ref || invoice.salesOrderRef
    
    // If no direct reference, try to extract from notes
    if (!salesOrderRef && invoice.notes && invoice.notes.includes('Generated from Sales Order:')) {
      const match = invoice.notes.match(/Generated from Sales Order:\s*([A-Z0-9-]+)/)
      if (match) {
        salesOrderRef = match[1]
        console.log("Extracted sales order reference from notes:", salesOrderRef)
      }
    }
    
    if (salesOrderRef) {
      try {
        console.log("Looking for sales order with reference:", salesOrderRef)
        const response = await fetch(`/api/sales-orders`)
        if (response.ok) {
          const data = await response.json()
          const salesOrders = data.salesOrders || []
          console.log("Available sales orders:", salesOrders.map(so => ({ id: so.id, order_id: so.order_id })))
          
          const originalSalesOrder = salesOrders.find((so: any) => 
            so.order_id === salesOrderRef || 
            so.id === salesOrderRef
          )
          
          console.log("Found original sales order for invoice:", originalSalesOrder)
          
          // First try to get line_items directly from the sales order
          if (originalSalesOrder && originalSalesOrder.line_items && originalSalesOrder.line_items.length > 0) {
            console.log("Using line_items directly from sales order:", originalSalesOrder.line_items)
            itemsData = originalSalesOrder.line_items.map((item: any) => {
              // Clean up description
              let cleanDescription = item.description || ""
              if (cleanDescription.includes("Product from deal:")) {
                const parts = cleanDescription.split(" - ")
                if (parts.length > 1) {
                  cleanDescription = parts[parts.length - 1]
                } else {
                  cleanDescription = item.product || ""
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
          // If no line_items in sales order, try to fetch from original quotation
          else if (originalSalesOrder && originalSalesOrder.quotation_ref) {
            console.log("No line_items in sales order, fetching from quotation:", originalSalesOrder.quotation_ref)
            // Fetch quotation items
            const quotationResponse = await fetch(`/api/quotations?companyId=${companyDetails?.id || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'}`)
            if (quotationResponse.ok) {
              const quotationData = await quotationResponse.json()
              const quotations = quotationData.quotations || []
              
              const originalQuotation = quotations.find((q: any) => 
                q.quote_number === originalSalesOrder.quotation_ref || 
                q.quotationId === originalSalesOrder.quotation_ref ||
                q.id === originalSalesOrder.quotation_ref
              )
              
              if (originalQuotation && originalQuotation.line_items && originalQuotation.line_items.length > 0) {
                console.log("Found original quotation items for invoice:", originalQuotation.line_items)
                itemsData = originalQuotation.line_items.map((item: any) => {
                  // Clean up description
                  let cleanDescription = item.description || ""
                  if (cleanDescription.includes("Product from deal:")) {
                    const parts = cleanDescription.split(" - ")
                    if (parts.length > 1) {
                      cleanDescription = parts[parts.length - 1]
                    } else {
                      cleanDescription = item.product || ""
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
          }
        }
      } catch (error) {
        console.error("Error fetching sales order/quotation data for invoice:", error)
      }
    }
    
    // Create professional invoice HTML content for PDF download
    const htmlContent = `
      <!DOCTYPE html>
      <html>
      <head>
        <title>Invoice - ${invoice.invoice_number || invoice.id}</title>
        <style>
          @page { size: A4; margin: 20mm; }
          body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
          .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #2563eb; padding-bottom: 20px; margin-bottom: 30px; }
          .logo-section { flex: 1; }
          .logo { max-width: 200px; max-height: 80px; }
          .company-info { flex: 1; text-align: right; }
          .company-name { font-size: 24px; font-weight: bold; color: #1e40af; margin-bottom: 5px; }
          .invoice-title { text-align: center; font-size: 28px; color: #1e40af; margin: 30px 0; font-weight: bold; }
          .invoice-info { display: flex; justify-content: space-between; margin-bottom: 30px; }
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
          .grand-total { font-weight: bold; font-size: 18px; border-top: 2px solid #2563eb; padding-top: 10px; }
          .footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; text-align: center; color: #666; }
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
        
        <div class="invoice-title">INVOICE</div>
        
        <div class="invoice-info">
          <div class="info-section">
            <div><span class="info-label">Invoice No:</span> ${invoice.invoice_number || invoice.invoiceNumber || invoice.id}</div>
            <div><span class="info-label">Issue Date:</span> ${invoice.invoice_date || invoice.issue_date || invoice.issueDate || new Date().toLocaleDateString()}</div>
            <div><span class="info-label">Due Date:</span> ${invoice.due_date || invoice.dueDate || 'TBD'}</div>
            ${invoice.sales_order_ref || invoice.salesOrderRef ? `<div><span class="info-label">Sales Order:</span> ${invoice.sales_order_ref || invoice.salesOrderRef}</div>` : ''}
          </div>
          <div class="info-section" style="text-align: right;">
            <div><span class="info-label">Prepared By:</span> ${currentUser}</div>
          </div>
        </div>
        
        <div class="customer-section">
          <h3>Bill To:</h3>
          <div><strong>${invoice.customer_name || invoice.customer || invoice.accountName || 'N/A'}</strong></div>
          <div>Attn: ${invoice.contact_person || invoice.contact || invoice.contactPerson || 'N/A'}</div>
          ${invoice.customer_email || invoice.email ? `<div>Email: ${invoice.customer_email || invoice.email}</div>` : ''}
          ${invoice.customer_phone || invoice.phone ? `<div>Phone: ${invoice.customer_phone || invoice.phone}</div>` : ''}
          ${invoice.billing_address || invoice.address ? `<div>${(invoice.billing_address || invoice.address).replace(/\n/g, '<br>')}</div>` : ''}
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
            const totalAmount = parseFloat(invoice.total_amount || invoice.amount || invoice.totalAmount || 0);
            const subtotalAmount = parseFloat(invoice.subtotal || invoice.subtotal_amount || (totalAmount / 1.18));
            const taxAmount = parseFloat(invoice.tax_amount || invoice.taxAmount || (totalAmount - subtotalAmount));
            const discountAmount = parseFloat(invoice.discount_amount || invoice.discountAmount || 0);
            
            return `
              <div class="total-row">
                <span class="total-label">Subtotal:</span>
                <span class="total-value">₹${Math.round(subtotalAmount).toLocaleString()}</span>
              </div>
              ${discountAmount && discountAmount > 0 ? `
                <div class="total-row">
                  <span class="total-label">Discount:</span>
                  <span class="total-value">- ₹${discountAmount.toLocaleString()}</span>
                </div>
              ` : ''}
              <div class="total-row">
                <span class="total-label">Tax (${invoice.tax_type || invoice.taxType || 'GST'} @ 18%):</span>
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
          ${companyDetails?.website ? `<p>Visit us at: ${companyDetails.website}</p>` : ''}
          <p style="font-size: 12px; color: #888;">This is a computer-generated invoice and does not require a signature.</p>
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
      title: "Invoice PDF ready",
      description: `Invoice ${invoice.invoice_number || invoice.id} is ready to download. Use "Save as PDF" in the print dialog.`
    })
  }
  
  const handleSendInvoice = (invoice: any) => {
    // Prepare email details
    const subject = `Invoice ${invoice.invoice_number || invoice.id} - ${companyDetails?.company_name || 'LabGig CRM'}`
    const customerEmail = invoice.customer_email || invoice.email || ''
    const body = `Dear ${invoice.contact_person || invoice.customer_name},\n\nPlease find attached your invoice ${invoice.invoice_number || invoice.id} for ₹${invoice.total_amount?.toLocaleString()}.\n\nDue Date: ${invoice.due_date || invoice.dueDate}\n\nThank you for your business!\n\nBest regards,\nAccounts Team`
    
    // Open default email client
    window.open(`mailto:${customerEmail}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`, '_self')
    
    toast({
      title: "Email client opened",
      description: `Ready to send invoice ${invoice.invoice_number || invoice.id} to ${invoice.customer_name}.`
    })
  }
  
  const handleSendWhatsApp = (invoice: any) => {
    const customerPhone = invoice.customer_phone || invoice.phone || ''
    const invoiceNumber = invoice.invoice_number || invoice.id
    const amount = invoice.total_amount || invoice.amount || 0
    const dueDate = invoice.due_date || invoice.dueDate || 'TBD'
    
    const message = `Dear ${invoice.contact_person || invoice.customer_name},\n\nYour invoice ${invoiceNumber} for ₹${amount.toLocaleString()} has been generated.\n\nDue Date: ${dueDate}\n\nPlease let us know if you have any questions.\n\nThank you!`
    
    // Remove any non-numeric characters from phone number
    const cleanPhone = customerPhone.replace(/\D/g, '')
    
    // Open WhatsApp with pre-filled message
    window.open(`https://wa.me/${cleanPhone}?text=${encodeURIComponent(message)}`, '_blank')
    
    toast({
      title: "WhatsApp opened",
      description: `Ready to send invoice details to ${invoice.contact_person || invoice.customer_name}`
    })
  }
  
  const handleScheduleInstallation = (invoice: any) => {
    setInstallationInvoice(invoice)
    setIsInstallationModalOpen(true)
  }

  const handleSaveInstallationFromInvoice = async (installationData: any) => {
    try {
      const response = await fetch('/api/installations', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(installationData)
      })

      if (response.ok) {
        const data = await response.json()
        const installation = data.installation

        // If this installation has annual maintenance charges, create AMC record automatically
        console.log('=== AMC AUTO-CREATION CHECK FROM INVOICE ===')
        console.log('installationData:', installationData)
        console.log('annual_maintenance_charges:', installationData.annual_maintenance_charges)
        console.log('parsed amount:', parseFloat(installationData.annual_maintenance_charges))
        
        if (installationData.annual_maintenance_charges && parseFloat(installationData.annual_maintenance_charges) > 0) {
          console.log('=== CREATING AMC RECORD FROM INVOICE ===');
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
              service_address: installationData.installation_address,
              city: installationData.city,
              state: installationData.state,
              pincode: installationData.pincode || '',
              
              // Product details
              product_name: installationData.product_name || 'Laboratory Equipment',
              equipment_details: [{
                name: installationData.product_name || 'Laboratory Equipment',
                model: installationData.product_model || '',
                serial_number: installationData.serial_number || '',
                description: `Annual maintenance contract for ${installationData.product_name || 'Laboratory Equipment'}`,
                quantity: 1
              }],
              
              // AMC specific details
              contract_type: 'comprehensive',
              contract_value: parseFloat(installationData.annual_maintenance_charges),
              contract_start_date: amcDate.toISOString().split('T')[0],
              contract_end_date: new Date(amcDate.getFullYear() + 1, amcDate.getMonth(), amcDate.getDate()).toISOString().split('T')[0],
              duration_months: 12,
              service_frequency: 'Annual',
              number_of_services: 1,
              services_completed: 0,
              labour_charges_included: true,
              spare_parts_included: true,
              emergency_support: true,
              response_time_hours: 24,
              status: 'Active',
              
              // Reference to installation
              installation_id: installation.id,
              installation_number: installation.installation_number,
              
              // Source information
              source_type: 'invoice',
              source_reference: installationInvoice?.invoice_number || installationInvoice?.id,
              special_instructions: `AMC auto-created from invoice installation: ${installation.installation_number}`
            }

            console.log('AMC data being sent:', amcData)

            const amcResponse = await fetch('/api/amc', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
              },
              body: JSON.stringify(amcData),
            })

            if (amcResponse.ok) {
              const amcResult = await amcResponse.json()
              console.log('AMC record created from invoice:', amcResult.amcContract?.amc_number)
              
              toast({
                title: "Installation & AMC Scheduled",
                description: `Installation ${installation.installation_number} and AMC contract ${amcResult.amcContract?.amc_number} have been created for ${installationData.customer_name || installationData.account_name}`
              })
            } else {
              console.error('Failed to create AMC from invoice installation')
              toast({
                title: "Installation Scheduled",
                description: `Installation ${installation.installation_number} has been scheduled for ${installationData.customer_name || installationData.account_name}. AMC creation failed.`
              })
            }
          } catch (amcError) {
            console.error('Error creating AMC record from invoice:', amcError)
            toast({
              title: "Installation Scheduled",
              description: `Installation ${installation.installation_number} has been scheduled for ${installationData.customer_name || installationData.account_name}. AMC creation failed.`
            })
          }
        } else {
          toast({
            title: "Installation Scheduled",
            description: `Installation ${installation.installation_number} has been scheduled for ${installationData.customer_name || installationData.account_name}`
          })
        }

        setIsInstallationModalOpen(false)
        setInstallationInvoice(null)
        
        // Navigate to installations page
        router.push('/installations')
      } else {
        throw new Error('Failed to create installation via API')
      }
    } catch (error) {
      console.error('Error scheduling installation:', error)
      toast({
        title: "Error",
        description: "Failed to schedule installation from invoice",
        variant: "destructive"
      })
    }
  }
  
  const handleDownloadPDF = (invoice: any) => {
    // In a real application, you would generate and download a PDF
    // For now, we'll simulate the download
    const invoiceData = JSON.stringify(invoice, null, 2)
    const blob = new Blob([invoiceData], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `invoice_${invoice.id}.json`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
    
    toast({
      title: "Invoice downloaded",
      description: `Invoice ${invoice.id} has been downloaded.`
    })
  }
  
  const handleMarkAsPaid = async (invoice: any) => {
    try {
      const response = await fetch('/api/invoices', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          id: invoice.id,
          payment_status: 'paid',
          payment_method: 'Marked Manually',
          payment_date: new Date().toISOString().split('T')[0]
        })
      })
      
      if (response.ok) {
        // Refresh the invoices list
        await loadInvoices()
        
        toast({
          title: "Invoice marked as paid",
          description: `Invoice ${invoice.invoice_number} has been marked as paid.`
        })
      } else {
        throw new Error('Failed to update invoice')
      }
    } catch (error) {
      console.error("Failed to update invoice:", error)
      // Fallback to localStorage
      const updatedInvoice = storageService.update('invoices', invoice.id, {
        ...invoice,
        status: 'paid',
        paymentMethod: 'Marked Manually'
      })
      
      if (updatedInvoice) {
        loadInvoices()
        toast({
          title: "Invoice marked as paid",
          description: `Invoice ${invoice.id} has been marked as paid.`
        })
      } else {
        toast({
          title: "Error",
          description: "Failed to update invoice status",
          variant: "destructive"
        })
      }
    }
  }

  const invoicesStats = [
    {
      title: "Total Invoices",
      value: invoicesStatsState.total.toString(),
      change: "+12.5%",
      icon: FileText,
      color: "text-blue-600",
      bgColor: "bg-blue-50",
      trend: "up"
    },
    {
      title: "Outstanding Amount",
      value: `₹${(invoicesStatsState.outstanding / 100000).toFixed(1)}L`,
      change: "-8.2%", 
      icon: IndianRupee,
      color: "text-red-600",
      bgColor: "bg-red-50",
      trend: "down"
    },
    {
      title: "Paid This Month",
      value: `₹${(invoicesStatsState.paidThisMonth / 100000).toFixed(1)}L`,
      change: "+15.3%",
      icon: CheckCircle2,
      color: "text-green-600",
      bgColor: "bg-green-50",
      trend: "up"
    },
    {
      title: "Overdue",
      value: invoicesStatsState.overdue.toString(),
      change: "+5.1%",
      icon: AlertCircle,
      color: "text-orange-600",
      bgColor: "bg-orange-50",
      trend: "up"
    },
  ]

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "paid":
        return "bg-green-100 text-green-800"
      case "pending":
      case "sent":
        return "bg-yellow-100 text-yellow-800"
      case "overdue":
        return "bg-red-100 text-red-800"
      case "draft":
        return "bg-gray-100 text-gray-800"
      case "cancelled":
        return "bg-gray-100 text-gray-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  // Enhanced stats for the cards
  const stats = [
    {
      title: "Total Invoices",
      value: invoicesStatsState.total.toString(),
      change: `+${invoicesStatsState.thisMonthCount} this month`,
      icon: Receipt,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Total Value",
      value: `₹${(invoicesStatsState.totalValue / 10000000).toFixed(1)}Cr`,
      change: "All invoices",
      icon: IndianRupee,
      iconColor: "text-green-600",
      iconBg: "bg-green-100"
    },
    {
      title: "Outstanding",
      value: `₹${(invoicesStatsState.outstanding / 100000).toFixed(1)}L`,
      change: invoicesStatsState.outstanding > 0 ? "Needs collection" : "All clear",
      icon: Clock,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    },
    {
      title: "Overdue",
      value: invoicesStatsState.overdue.toString(),
      change: invoicesStatsState.overdue > 0 ? "Urgent attention" : "On time",
      icon: AlertCircle,
      iconColor: "text-red-600",
      iconBg: "bg-red-100"
    }
  ]

  const invoices = [
    {
      id: "INV-2024-001",
      customer: "Kerala Agricultural University",
      amount: "₹8,50,000",
      dueDate: "2024-08-25",
      issueDate: "2024-08-15",
      status: "paid",
      paymentMethod: "Bank Transfer",
      items: "Fibrinometer System + Accessories",
      contact: "Dr. Priya Sharma",
      overdueDays: 0
    },
    {
      id: "INV-2024-002", 
      customer: "Eurofins Advinus",
      amount: "₹12,75,000",
      dueDate: "2024-08-22",
      issueDate: "2024-08-12",
      status: "overdue",
      paymentMethod: "Pending",
      items: "Laboratory Centrifuge + Installation",
      contact: "Mr. Rajesh Kumar",
      overdueDays: 3
    },
    {
      id: "INV-2024-003",
      customer: "TSAR Labcare",
      amount: "₹6,25,000",
      dueDate: "2024-08-28",
      issueDate: "2024-08-18",
      status: "pending",
      paymentMethod: "Pending",
      items: "Microscope + Training Package",
      contact: "Ms. Pauline D'Souza",
      overdueDays: 0
    },
    {
      id: "INV-2024-004",
      customer: "JNCASR",
      amount: "₹4,50,000",
      dueDate: "2024-08-30",
      issueDate: "2024-08-20",
      status: "sent",
      paymentMethod: "Pending",
      items: "Spectrometer Accessories",
      contact: "Dr. Anu Rang",
      overdueDays: 0
    },
    {
      id: "INV-2024-005",
      customer: "Bio-Rad Laboratories",
      amount: "₹9,80,000",
      dueDate: "2024-09-05",
      issueDate: "2024-08-16",
      status: "draft",
      paymentMethod: "Pending",
      items: "Advanced PCR System",
      contact: "Mr. Sanjay Patel",
      overdueDays: 0
    },
    {
      id: "INV-2024-006",
      customer: "Thermo Fisher Scientific",
      amount: "₹3,25,000",
      dueDate: "2024-08-26",
      issueDate: "2024-08-16",
      status: "paid",
      paymentMethod: "Cheque",
      items: "Maintenance Contract Renewal",
      contact: "Ms. Anjali Menon",
      overdueDays: 0
    }
  ]

  const getStatusBadge = (status: string) => {
    const variants = {
      paid: "bg-green-100 text-green-800",
      pending: "bg-yellow-100 text-yellow-800",
      overdue: "bg-red-100 text-red-800",
      sent: "bg-blue-100 text-blue-800",
      draft: "bg-gray-100 text-gray-800",
      cancelled: "bg-gray-100 text-gray-800"
    }
    return variants[status as keyof typeof variants] || "bg-gray-100 text-gray-800"
  }


  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-IN', {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    })
  }

  // Filter invoices based on search and filters
  const filteredInvoices = invoicesList.filter(invoice => {
    const matchesSearch = searchTerm === "" || 
      invoice.id?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      invoice.invoice_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      invoice.customer_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      invoice.contact_person?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesStatus = selectedStatus === "All" || invoice.status === selectedStatus.toLowerCase()
    const matchesPaymentStatus = selectedPaymentStatus === "All" || invoice.payment_status === selectedPaymentStatus.toLowerCase()
    const matchesAssigned = selectedAssignedTo === "All" || invoice.assigned_to === selectedAssignedTo

    return matchesSearch && matchesStatus && matchesPaymentStatus && matchesAssigned
  })

  // Get unique assigned users for filter
  const uniqueAssignedUsers = Array.from(
    new Set(invoicesList.map(invoice => invoice.assigned_to).filter(Boolean))
  )

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Invoices</h1>
          <p className="text-gray-500 mt-1">Generate, track and manage customer invoices</p>
        </div>
        <div className="flex items-center space-x-3">
          <Button variant="outline" size="sm">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button onClick={() => setIsDataImportModalOpen(true)} variant="outline" size="sm">
            <Upload className="w-4 h-4 mr-2" />
            Import Invoices
          </Button>
          <Button onClick={() => setIsAddInvoiceModalOpen(true)} className="bg-blue-600 hover:bg-blue-700" size="sm">
            <Plus className="w-4 h-4 mr-2" />
            Create Invoice
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

      {/* Invoice Search & Filters */}
      <Card>
        <CardHeader>
          <CardTitle>Invoice Search & Filters</CardTitle>
          <CardDescription>Filter and search through your invoices</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search invoices by number, customer, or contact..."
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
                <SelectItem value="Draft">Draft</SelectItem>
                <SelectItem value="Sent">Sent</SelectItem>
                <SelectItem value="Paid">Paid</SelectItem>
                <SelectItem value="Overdue">Overdue</SelectItem>
              </SelectContent>
            </Select>
            <Select value={selectedPaymentStatus} onValueChange={setSelectedPaymentStatus}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Payment" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="All">All Payment</SelectItem>
                <SelectItem value="Paid">Paid</SelectItem>
                <SelectItem value="Unpaid">Unpaid</SelectItem>
                <SelectItem value="Partially Paid">Partially Paid</SelectItem>
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

      {/* Invoices List - Card View */}
      <Card>
        <CardHeader>
          <CardTitle>Invoices List</CardTitle>
          <CardDescription>Showing {filteredInvoices.length} of {invoicesList.length} invoices</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredInvoices.length === 0 ? (
              <div className="text-center py-12">
                <Receipt className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No invoices found</p>
                <p className="text-sm text-gray-400 mt-2">
                  {searchTerm || selectedStatus !== "All" || selectedPaymentStatus !== "All" || selectedAssignedTo !== "All"
                    ? "Try adjusting your filters" 
                    : "Click 'Create Invoice' to create your first invoice"}
                </p>
              </div>
            ) : (
              filteredInvoices.map((invoice) => (
                <div
                  key={invoice.id}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center space-x-6">
                    <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                      <FileText className="w-6 h-6 text-blue-600" />
                    </div>
                    
                    {/* Invoice Details */}
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="font-semibold text-gray-900">
                          {invoice.invoice_number}
                        </h3>
                        {invoice.payment_status && (
                          <Badge variant="outline">
                            {invoice.payment_status === 'unpaid' ? 'Unpaid' : 
                             invoice.payment_status === 'paid' ? 'Paid' : 
                             invoice.payment_status}
                          </Badge>
                        )}
                        {invoice.sales_order_ref && (
                          <Badge variant="outline" className="text-purple-600 border-purple-200">
                            SO: {invoice.sales_order_ref}
                          </Badge>
                        )}
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Date: {formatDate(invoice.invoice_date)} | Due: {formatDate(invoice.due_date)}
                      </p>
                    </div>
                    
                    {/* Customer Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Building className="w-4 h-4 text-gray-400" />
                        <span className="font-medium">{invoice.customer_name}</span>
                      </div>
                      <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                        <User className="w-4 h-4 text-gray-400" />
                        <span>{invoice.contact_person}</span>
                      </div>
                      {invoice.customer_phone && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Phone className="w-3 h-3 text-gray-400" />
                          <span>{invoice.customer_phone}</span>
                        </div>
                      )}
                    </div>
                    
                    {/* Amount & Assignment Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <span className="font-semibold text-green-600">₹{(invoice.total_amount || 0).toLocaleString('en-IN')}</span>
                      </div>
                      {invoice.assigned_to && (
                        <p className="text-xs text-gray-500 mt-1">
                          Assigned to: {invoice.assigned_to}
                        </p>
                      )}
                      {invoice.priority && (
                        <p className="text-xs text-gray-500 mt-1">
                          Priority: {invoice.priority}
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
                        title="Edit Invoice"
                      >
                        <Edit className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Download Invoice"
                        onClick={() => handleDownloadInvoice(invoice)}
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
                        onClick={() => handleSendWhatsApp(invoice)}
                        className="text-green-600 hover:text-green-800 hover:bg-green-50"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Send Email"
                        onClick={() => handleSendInvoice(invoice)}
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
                        title="Schedule Installation"
                        onClick={() => handleScheduleInstallation(invoice)}
                        className="text-green-600 hover:text-green-800 hover:bg-green-50"
                      >
                        <Truck className="w-4 h-4" />
                      </Button>
                      {invoice.payment_status !== 'paid' && (
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          title="Mark as Paid"
                          onClick={() => handleMarkAsPaid(invoice)}
                          className="text-green-600 hover:text-green-800 hover:bg-green-50"
                        >
                          <Banknote className="w-4 h-4" />
                        </Button>
                      )}
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>

      {/* Add Invoice Modal */}
      <AddInvoiceModal
        isOpen={isAddInvoiceModalOpen}
        onClose={() => setIsAddInvoiceModalOpen(false)}
        onSave={handleSaveInvoice}
      />
      
      {/* DataImportModal component */}
      <DataImportModal 
        isOpen={isDataImportModalOpen} 
        onClose={() => setIsDataImportModalOpen(false)}
        moduleType="invoices"
        onImport={handleImportData}
      />

      {/* Installation Modal */}
      <AddInstallationModal
        isOpen={isInstallationModalOpen}
        onClose={() => {
          setIsInstallationModalOpen(false)
          setInstallationInvoice(null)
        }}
        onSave={handleSaveInstallationFromInvoice}
        sourceType="invoice"
        sourceData={installationInvoice}
      />
    </div>
  )
}