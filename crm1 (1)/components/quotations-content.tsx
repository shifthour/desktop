"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Plus, Search, Download, Eye, Edit, FileText, Send, Printer, Upload, 
  TrendingUp, TrendingDown, Calendar, IndianRupee, CheckCircle, Clock,
  Building, User, Phone, Mail, MapPin, Package, DollarSign, MessageCircle,
  ShoppingCart
} from "lucide-react"
import { AddQuotationModal } from "@/components/add-quotation-modal"
import { ViewQuotationModal } from "@/components/view-quotation-modal"
import { AddSalesOrderModal } from "@/components/add-sales-order-modal"
import { AddInvoiceModal } from "@/components/add-invoice-modal"
import { DataImportModal } from "@/components/data-import-modal"
import { ErrorBoundary } from "@/components/error-boundary"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"

export function QuotationsContent() {
  const { toast } = useToast()
  const [quotationsList, setQuotationsList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedAssignedTo, setSelectedAssignedTo] = useState("All")
  const [isAddQuotationModalOpen, setIsAddQuotationModalOpen] = useState(false)
  const [isDataImportModalOpen, setIsDataImportModalOpen] = useState(false)
  const [editingQuotation, setEditingQuotation] = useState<any>(null)
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [viewingQuotation, setViewingQuotation] = useState<any>(null)
  const [isViewModalOpen, setIsViewModalOpen] = useState(false)
  const [isSalesOrderModalOpen, setIsSalesOrderModalOpen] = useState(false)
  const [isInvoiceModalOpen, setIsInvoiceModalOpen] = useState(false)
  const [convertingQuotation, setConvertingQuotation] = useState<any>(null)
  const [companyLogo, setCompanyLogo] = useState<string>("")
  const [companyDetails, setCompanyDetails] = useState<any>(null)
  const [quotationStats, setQuotationStats] = useState({
    total: 0,
    pendingReview: 0,
    acceptanceRate: 0,
    totalValue: 0,
    thisMonthCount: 0
  })

  // Load quotations from localStorage on component mount
  useEffect(() => {
    loadQuotations()
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

  const loadQuotations = async () => {
    try {
      const companyId = localStorage.getItem('currentCompanyId') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      const response = await fetch(`/api/quotations?companyId=${companyId}`)
      if (response.ok) {
        const data = await response.json()
        const quotations = data.quotations || []
        console.log("loadQuotations - quotations from API:", quotations)
        setQuotationsList(quotations)
        
        // Calculate stats
        const pendingReview = quotations.filter(q => q.status === 'sent' || q.status === 'viewed').length
        const accepted = quotations.filter(q => q.status === 'approved' || q.status === 'converted').length
        const acceptanceRate = quotations.length > 0 ? Math.round((accepted / quotations.length) * 100) : 0
        const totalValue = quotations.reduce((sum, q) => {
          return sum + (q.total_amount || 0)
        }, 0)
        
        // Calculate this month's quotations
        const currentMonth = new Date().getMonth()
        const currentYear = new Date().getFullYear()
        const thisMonthCount = quotations.filter(q => {
          const quotationDate = new Date(q.quote_date || q.created_at)
          return quotationDate.getMonth() === currentMonth && quotationDate.getFullYear() === currentYear
        }).length
        
        setQuotationStats({
          total: quotations.length,
          pendingReview: pendingReview,
          acceptanceRate: acceptanceRate,
          totalValue: totalValue,
          thisMonthCount: thisMonthCount
        })
      } else {
        console.error('Failed to fetch quotations')
        // Fallback to localStorage for now
        const storedQuotations = storageService.getAll<any>('quotations')
        setQuotationsList(storedQuotations)
      }
    } catch (error) {
      console.error('Error loading quotations:', error)
      // Fallback to localStorage
      const storedQuotations = storageService.getAll<any>('quotations')
      setQuotationsList(storedQuotations)
    }
  }

  const handleSaveQuotation = async (quotationData: any) => {
    console.log("handleSaveQuotation called with:", quotationData)
    
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
        console.log("New quotation created:", newQuotation)
        
        // Refresh quotations list
        await loadQuotations()
        
        toast({
          title: "Quotation created",
          description: `Quotation ${newQuotation.quote_number} has been successfully created.`
        })
        setIsAddQuotationModalOpen(false)
      } else {
        throw new Error('Failed to create quotation')
      }
    } catch (error) {
      console.error("Failed to create quotation:", error)
      // Fallback to localStorage
      const newQuotation = storageService.create('quotations', quotationData)
      if (newQuotation) {
        loadQuotations()
        toast({
          title: "Quotation created (local)",
          description: `Quotation ${newQuotation.quotationId || newQuotation.id} has been created locally.`
        })
        setIsAddQuotationModalOpen(false)
      } else {
        toast({
          title: "Error",
          description: "Failed to create quotation",
          variant: "destructive"
        })
      }
    }
  }
  
  const handleViewQuotation = (quotation: any) => {
    setViewingQuotation(quotation)
    setIsViewModalOpen(true)
  }
  
  const handleEditQuotation = (quotation: any) => {
    console.log("handleEditQuotation called with:", quotation)
    console.log("Quotation keys:", Object.keys(quotation))
    
    try {
      // Create a clean copy of the quotation data
      const cleanQuotation = {
        ...quotation,
        // Ensure consistent date format
        quotationDate: quotation.quotationDate || quotation.date,
        contactPerson: quotation.contactPerson || quotation.contactName
      }
      
      setEditingQuotation(cleanQuotation)
      setIsEditModalOpen(true)
      console.log("Edit modal state set successfully")
    } catch (error) {
      console.error("Error in handleEditQuotation:", error)
      toast({
        title: "Error",
        description: "Failed to open edit dialog. Please try again.",
        variant: "destructive"
      })
    }
  }
  
  // Custom WhatsApp Icon Component (same as leads page)
  const WhatsAppIcon = ({ className }: { className?: string }) => (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
    </svg>
  )

  const handleSendEmail = (quotation: any) => {
    // Map database fields to display fields
    const contactName = quotation.contact_person || quotation.contactPerson || quotation.contactName
    const companyName = quotation.customer_name || quotation.accountName
    const quotationNumber = quotation.quote_number || quotation.quotationId || quotation.id
    const quotationDate = quotation.quote_date || quotation.date
    const validUntil = quotation.valid_until || quotation.validUntil
    const amount = quotation.total_amount || quotation.amount
    const assignedTo = quotation.assigned_to || quotation.assignedTo || 'Sales Team'
    const customerEmail = quotation.customer_email || quotation.customerEmail
    
    const subject = `Quotation ${quotationNumber} - ${companyName}`
    const body = `Dear ${contactName},\n\nPlease find attached the quotation ${quotationNumber} for your review.\n\nQuotation Details:\n- Date: ${quotationDate}\n- Valid Until: ${validUntil}\n- Amount: ₹${amount?.toLocaleString()}\n\nThank you for your interest in our products/services.\n\nBest regards,\n${assignedTo}`
    
    const mailtoLink = `mailto:${customerEmail}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`
    
    if (customerEmail) {
      window.open(mailtoLink)
      toast({
        title: "Email client opened",
        description: `Quotation ${quotationNumber} email draft prepared for ${companyName}`
      })
    } else {
      toast({
        title: "No email address",
        description: "Customer email address is not available for this quotation",
        variant: "destructive"
      })
    }
  }

  const handleSendWhatsApp = async (quotation: any) => {
    // Map database fields to display fields
    const contactName = quotation.contact_person || quotation.contactPerson || quotation.contactName
    const companyName = quotation.customer_name || quotation.accountName
    const quotationNumber = quotation.quote_number || quotation.quotationId || quotation.id
    const quotationDate = quotation.quote_date || quotation.date
    const validUntil = quotation.valid_until || quotation.validUntil
    const amount = quotation.total_amount || quotation.amount
    const product = quotation.products_quoted || quotation.product
    const assignedTo = quotation.assigned_to || quotation.assignedTo || 'Sales Team'
    
    // Prepare WhatsApp message directly without PDF generation
    const message = `Hi ${contactName},\n\nHope you are doing well!\n\nPlease find attached quotation ${quotationNumber} for ${product} for your reference.\n\n📋 *Quotation Details:*\n📅 Date: ${quotationDate}\n⏰ Valid Until: ${validUntil}\n💰 Amount: ₹${amount?.toLocaleString()}\n\nLet me know if you have any questions or need any clarifications.\n\nThanks & Regards,\n${assignedTo}`
    
    // Try both database and localStorage field names for phone
    const phoneNumber = (quotation.customer_phone || quotation.customerPhone || quotation.whatsapp)?.replace(/[^0-9]/g, '')
    
    if (phoneNumber) {
      const whatsappLink = `https://wa.me/${phoneNumber.startsWith('91') ? phoneNumber : '91' + phoneNumber}?text=${encodeURIComponent(message)}`
      window.open(whatsappLink, '_blank')
      
      toast({
        title: "WhatsApp opened",
        description: `Message sent to ${contactName}. Use the download button if you need to attach the PDF.`
      })
    } else {
      toast({
        title: "No WhatsApp number",
        description: "Customer WhatsApp/phone number is not available for this quotation",
        variant: "destructive"
      })
    }
  }

  const handleDownloadQuotation = (quotation: any) => {
    console.log("PDF Generation - Full quotation object:", quotation)
    console.log("Valid until field options:", {
      valid_until: quotation.valid_until,
      validUntil: quotation.validUntil, 
      validity_period: quotation.validity_period,
      due_date: quotation.due_date,
      expiry_date: quotation.expiry_date
    })
    
    // Get logged-in user name for "Prepared By" field
    const loggedInUser = typeof window !== 'undefined' && localStorage.getItem('user')
    let preparedByName = quotation.assigned_to || quotation.assignedTo || 'Sales Team'
    
    if (loggedInUser) {
      try {
        const user = JSON.parse(loggedInUser)
        preparedByName = user.full_name || user.name || user.email || preparedByName
      } catch (error) {
        console.error('Error parsing user data:', error)
      }
    }
    
    // Generate HTML content for PDF (same as print but for download)
    const htmlContent = `
      <!DOCTYPE html>
      <html>
      <head>
        <title>Quotation - ${quotation.quote_number || quotation.id}</title>
        <style>
          @page { size: A4; margin: 20mm; }
          body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
          .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #2563eb; padding-bottom: 20px; margin-bottom: 30px; }
          .logo-section { flex: 1; }
          .logo { max-width: 200px; max-height: 80px; }
          .company-info { flex: 1; text-align: right; }
          .company-name { font-size: 24px; font-weight: bold; color: #1e40af; }
          .quotation-title { text-align: center; font-size: 28px; color: #1e40af; margin: 30px 0; font-weight: bold; }
          .quotation-info { display: flex; justify-content: space-between; margin-bottom: 30px; }
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
        
        <div class="quotation-title">QUOTATION</div>
        
        <div class="quotation-info">
          <div class="info-section">
            <div><span class="info-label">Quote No:</span> ${quotation.quote_number || quotation.quotationId || quotation.id}</div>
            <div><span class="info-label">Quote Date:</span> ${quotation.quote_date || quotation.date || new Date().toLocaleDateString()}</div>
            <div><span class="info-label">Valid Until:</span> ${quotation.valid_until || quotation.validUntil || quotation.validity_period || 'TBD'}</div>
          </div>
          <div class="info-section" style="text-align: right;">
            <div><span class="info-label">Prepared By:</span> ${preparedByName}</div>
          </div>
        </div>
        
        <div class="customer-section">
          <h3>Quote To:</h3>
          <div><strong>${quotation.customer_name || quotation.accountName || 'N/A'}</strong></div>
          <div>Attn: ${quotation.contact_person || quotation.contactPerson || quotation.contactName || 'N/A'}</div>
          ${quotation.customer_email || quotation.customerEmail ? `<div>Email: ${quotation.customer_email || quotation.customerEmail}</div>` : ''}
          ${quotation.customer_phone || quotation.customerPhone ? `<div>Phone: ${quotation.customer_phone || quotation.customerPhone}</div>` : ''}
          ${quotation.billing_address || quotation.billingAddress ? `<div>${(quotation.billing_address || quotation.billingAddress).replace(/\n/g, '<br>')}</div>` : ''}
        </div>
        
        <table class="items-table">
          <thead>
            <tr>
              <th style="width: 5%;">S.No</th>
              <th style="width: 45%;">Product/Service</th>
              <th style="width: 20%;">Description</th>
              <th style="width: 8%;">Qty</th>
              <th style="width: 12%;">Unit Price</th>
              <th style="width: 10%;">Amount</th>
            </tr>
          </thead>
          <tbody>
            ${(() => {
              let itemsToShow = []
              
              if (quotation.line_items && quotation.line_items.length > 0) {
                itemsToShow = quotation.line_items
              } else if (quotation.products_quoted) {
                const products = quotation.products_quoted.split(',').map((p: string) => p.trim())
                itemsToShow = products.map((product: string, index: number) => ({
                  product: product,
                  description: "As per specifications", // Simple, clean description
                  quantity: 1,
                  unitPrice: quotation.total_amount ? Math.round(quotation.total_amount / products.length) : 0,
                  amount: quotation.total_amount ? Math.round(quotation.total_amount / products.length) : 0
                }))
              } else {
                itemsToShow = [{
                  product: quotation.products_quoted || quotation.product || 'Products/Services',
                  description: quotation.product_description || quotation.description || '',
                  quantity: quotation.quantity || 1,
                  unitPrice: quotation.unit_price || quotation.total_amount || quotation.amount || 0,
                  amount: quotation.total_amount || quotation.amount || 0
                }]
              }
              
              return itemsToShow.map((item: any, index: number) => {
                // Clean up description by removing "Product from deal:" prefix and account info
                let cleanDescription = item.description || item.product || ""
                if (cleanDescription.includes("Product from deal:")) {
                  // Extract just the product part after the last " - "
                  const parts = cleanDescription.split(" - ")
                  if (parts.length > 1) {
                    cleanDescription = parts[parts.length - 1] // Get the last part (product description)
                  } else {
                    cleanDescription = item.product || "Product description"
                  }
                }
                
                // If still too long or contains unwanted details, just use the product name
                if (cleanDescription.length > 100 || cleanDescription.includes("Account:")) {
                  cleanDescription = item.product || ""
                }
                
                return `
                  <tr>
                    <td>${index + 1}</td>
                    <td>${item.product}</td>
                    <td>${cleanDescription}</td>
                    <td style="text-align: center;">${item.quantity || 1}</td>
                    <td style="text-align: right;">₹${(item.unitPrice || item.unit_price || 0).toLocaleString()}</td>
                    <td style="text-align: right;">₹${(item.amount || item.total || 0).toLocaleString()}</td>
                  </tr>
                `
              }).join('')
            })()}
          </tbody>
        </table>
        
        <div class="totals-section">
          ${(() => {
            const totalAmount = parseFloat(quotation.total_amount || quotation.amount || 0);
            const subtotalAmount = parseFloat(quotation.subtotal_amount || (totalAmount / 1.18));
            const taxAmount = parseFloat(quotation.tax_amount || (totalAmount - subtotalAmount));
            
            return `
              <div class="total-row">
                <span class="total-label">Subtotal:</span>
                <span class="total-value">₹${Math.round(subtotalAmount).toLocaleString()}</span>
              </div>
              <div class="total-row">
                <span class="total-label">Tax (GST @ 18%):</span>
                <span class="total-value">₹${Math.round(taxAmount).toLocaleString()}</span>
              </div>
              <div class="total-row grand-total">
                <span class="total-label">Grand Total:</span>
                <span class="total-value">₹${totalAmount.toLocaleString()}</span>
              </div>
            `;
          })()}
        </div>
        
        <div class="terms-section">
          <h4>Terms & Conditions:</h4>
          <ul>
            <li>Valid for ${quotation.valid_until || quotation.validUntil || '30 days'} from quote date</li>
            <li>Payment terms: ${quotation.payment_terms || quotation.paymentTerms || '50% advance, balance on delivery'}</li>
            <li>Delivery: ${quotation.delivery_timeline || quotation.deliveryTimeline || '2-3 weeks after order confirmation'}</li>
            <li>Installation & Training included as per scope</li>
            <li>Warranty as per manufacturer terms</li>
          </ul>
        </div>
        
        <div class="footer">
          <p><strong>Thank you for your business!</strong></p>
          ${companyDetails?.website ? `<p>Visit us at: ${companyDetails.website}</p>` : ''}
          <p style="font-size: 10px; color: #888;">This is a computer-generated quotation and does not require a signature.</p>
        </div>
      </body>
      </html>
    `
    
    // Create a temporary window to generate PDF and download
    const printWindow = window.open('', '_blank')
    if (printWindow) {
      printWindow.document.write(htmlContent)
      printWindow.document.close()
      
      // Wait for content to load, then trigger download
      printWindow.onload = () => {
        setTimeout(() => {
          printWindow.print()
          printWindow.close()
        }, 500)
      }
    }
    
    toast({
      title: "PDF Download Started",
      description: `Quotation ${quotation.quote_number || quotation.id} is being downloaded as PDF`
    })
  }

  const handlePrintQuotation = (quotation: any) => {
    // Get logged-in user name for "Prepared By" field
    const loggedInUser = typeof window !== 'undefined' && localStorage.getItem('user')
    let preparedByName = quotation.assigned_to || quotation.assignedTo || 'Sales Team'
    
    if (loggedInUser) {
      try {
        const user = JSON.parse(loggedInUser)
        preparedByName = user.full_name || user.name || user.email || preparedByName
      } catch (error) {
        console.error('Error parsing user data:', error)
      }
    }
    
    // Create a professional quotation print view with company logo
    const printContent = `
      <!DOCTYPE html>
      <html>
      <head>
        <title>Quotation - ${quotation.quotationId || quotation.id}</title>
        <style>
          @page { size: A4; margin: 20mm; }
          body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
          .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #2563eb; padding-bottom: 20px; margin-bottom: 30px; }
          .logo-section { flex: 1; }
          .logo { max-width: 200px; max-height: 80px; }
          .company-info { flex: 1; text-align: right; }
          .company-name { font-size: 24px; font-weight: bold; color: #1e40af; }
          .quotation-title { text-align: center; font-size: 28px; color: #1e40af; margin: 30px 0; font-weight: bold; }
          .quotation-info { display: flex; justify-content: space-between; margin-bottom: 30px; }
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
            <div class="company-name">${companyDetails?.name || 'Your Company Name'}</div>
            <div>${companyDetails?.address || ''}</div>
            <div>${companyDetails?.phone || ''}</div>
            <div>${companyDetails?.email || ''}</div>
            ${companyDetails?.website ? `<div>${companyDetails.website}</div>` : ''}
            ${companyDetails?.gst_number ? `<div>GST: ${companyDetails.gst_number}</div>` : ''}
          </div>
        </div>
        
        <h1 class="quotation-title">QUOTATION</h1>
        
        <div class="quotation-info">
          <div class="info-section">
            <div><span class="info-label">Quotation No:</span> ${quotation.quote_number || quotation.quotationId || quotation.id}</div>
            <div><span class="info-label">Date:</span> ${quotation.quote_date || quotation.date}</div>
            <div><span class="info-label">Valid Until:</span> ${quotation.valid_until || quotation.validUntil}</div>
            ${(quotation.reference_number || quotation.reference) ? `<div><span class="info-label">Reference:</span> ${quotation.reference_number || quotation.reference}</div>` : ''}
          </div>
          <div class="info-section" style="text-align: right;">
            <div><span class="info-label">Prepared By:</span> ${preparedByName}</div>
          </div>
        </div>
        
        <div class="customer-section">
          <h3 style="margin-top: 0;">Bill To:</h3>
          <div><strong>${quotation.customer_name || quotation.accountName}</strong></div>
          <div>Attn: ${quotation.contact_person || quotation.contactPerson || quotation.contactName}</div>
          ${(quotation.customer_email || quotation.customerEmail) ? `<div>Email: ${quotation.customer_email || quotation.customerEmail}</div>` : ''}
          ${(quotation.customer_phone || quotation.customerPhone) ? `<div>Phone: ${quotation.customer_phone || quotation.customerPhone}</div>` : ''}
          ${(quotation.billing_address || quotation.billingAddress) ? `<div>${(quotation.billing_address || quotation.billingAddress).replace(/\n/g, '<br>')}</div>` : ''}
        </div>
        
        ${quotation.subject ? `<div style="margin-bottom: 20px;"><strong>Subject:</strong> ${quotation.subject}</div>` : ''}
        
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
            ${(quotation.line_items || quotation.items) && (quotation.line_items || quotation.items).length > 0 ? 
              (quotation.line_items || quotation.items).map((item: any, index: number) => {
                // Clean up description by removing "Product from deal:" prefix and account info
                let cleanDescription = item.description || '';
                if (cleanDescription.includes("Product from deal:")) {
                  // Extract just the product part after the last " - "
                  const parts = cleanDescription.split(" - ");
                  if (parts.length > 1) {
                    cleanDescription = parts[parts.length - 1]; // Get the last part (product name)
                  } else {
                    cleanDescription = item.product || "";
                  }
                }
                
                return `
                  <tr>
                    <td>${index + 1}</td>
                    <td>${item.product || quotation.products_quoted || quotation.product}</td>
                    <td>${cleanDescription}</td>
                    <td style="text-align: center;">${item.quantity || 1}</td>
                    <td style="text-align: right;">₹${(item.unitPrice || 0).toLocaleString()}</td>
                    <td style="text-align: center;">${item.discount || 0}%</td>
                    <td style="text-align: right;">₹${(item.amount || 0).toLocaleString()}</td>
                  </tr>
                `;
              }).join('') : 
              `<tr>
                <td>1</td>
                <td>${quotation.products_quoted || quotation.product}</td>
                <td></td>
                <td style="text-align: center;">1</td>
                <td style="text-align: right;">₹${(quotation.total_amount || quotation.amount)?.toLocaleString()}</td>
                <td style="text-align: center;">0%</td>
                <td style="text-align: right;">₹${(quotation.total_amount || quotation.amount)?.toLocaleString()}</td>
              </tr>`
            }
          </tbody>
        </table>
        
        <div class="totals-section">
          ${quotation.totals ? `
            <div class="total-row">
              <span class="total-label">Subtotal:</span>
              <span class="total-value">₹${quotation.totals.subtotal.toLocaleString()}</span>
            </div>
            ${quotation.totals.totalDiscount > 0 ? `
              <div class="total-row">
                <span class="total-label">Discount:</span>
                <span class="total-value">- ₹${quotation.totals.totalDiscount.toLocaleString()}</span>
              </div>
            ` : ''}
            <div class="total-row">
              <span class="total-label">Tax (${quotation.tax_type || 'GST'} @ 18%):</span>
              <span class="total-value">₹${quotation.totals.totalTax.toLocaleString()}</span>
            </div>
            <div class="total-row grand-total">
              <span class="total-label">Grand Total:</span>
              <span class="total-value">₹${quotation.totals.grandTotal.toLocaleString()}</span>
            </div>
          ` : `
            ${(() => {
              const totalAmount = quotation.total_amount || quotation.amount || 0;
              const subtotalAmount = quotation.subtotal_amount || (totalAmount / 1.18);
              const taxAmount = quotation.tax_amount || (totalAmount - subtotalAmount);
              return `
                <div class="total-row">
                  <span class="total-label">Subtotal:</span>
                  <span class="total-value">₹${Math.round(subtotalAmount).toLocaleString()}</span>
                </div>
                <div class="total-row">
                  <span class="total-label">Tax (${quotation.tax_type || 'GST'} @ 18%):</span>
                  <span class="total-value">₹${Math.round(taxAmount).toLocaleString()}</span>
                </div>
                <div class="total-row grand-total">
                  <span class="total-label">Grand Total:</span>
                  <span class="total-value">₹${totalAmount.toLocaleString()}</span>
                </div>
              `;
            })()}
          `}
        </div>
        
        ${(quotation.terms_conditions || quotation.terms) ? `
          <div class="terms-section">
            <h3>Terms & Conditions:</h3>
            <pre style="white-space: pre-wrap; font-family: inherit;">${quotation.terms_conditions || quotation.terms}</pre>
          </div>
        ` : ''}
        
        <div class="footer">
          <p>Thank you for your business!</p>
          <p>This is a computer-generated quotation and does not require a physical signature.</p>
        </div>
      </body>
      </html>
    `
    
    const printWindow = window.open('', '_blank')
    if (printWindow) {
      printWindow.document.write(printContent)
      printWindow.document.close()
      setTimeout(() => {
        printWindow.print()
      }, 500)
    }
    
    toast({
      title: "Quotation ready to print",
      description: `Quotation ${quotation.quotationId || quotation.id} has been sent to printer.`
    })
  }

  const handleConvertToSalesOrder = (quotation: any) => {
    console.log("Opening sales order modal for quotation:", quotation)
    
    // Set the quotation data to be used for pre-populating the sales order form
    setConvertingQuotation(quotation)
    setIsSalesOrderModalOpen(true)
  }
  
  const handleGenerateInvoice = (quotation: any) => {
    console.log("Opening invoice modal for quotation:", quotation)
    setConvertingQuotation(quotation)
    setIsInvoiceModalOpen(true)
  }

  const handleSaveSalesOrderFromQuotation = async (salesOrderData: any) => {
    console.log("handleSaveSalesOrderFromQuotation called with:", salesOrderData)
    
    try {
      // Add quotation reference to the sales order data
      const orderData = {
        ...salesOrderData,
        quotation_ref: convertingQuotation?.quote_number || convertingQuotation?.quotationId || convertingQuotation?.id,
        notes: salesOrderData.notes ? 
          `${salesOrderData.notes}\n\nConverted from quotation ${convertingQuotation?.quote_number || convertingQuotation?.quotationId || convertingQuotation?.id}` :
          `Converted from quotation ${convertingQuotation?.quote_number || convertingQuotation?.quotationId || convertingQuotation?.id}`
      }
      
      // Save sales order to database
      const response = await fetch('/api/sales-orders', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(orderData)
      })
      
      if (response.ok) {
        const data = await response.json()
        const newSalesOrder = data.salesOrder
        console.log("New sales order created:", newSalesOrder)
        
        // Update quotation status to "Converted to Sales Order"
        if (convertingQuotation) {
          try {
            const quotationUpdateResponse = await fetch('/api/quotations', {
              method: 'PUT',
              headers: {
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({
                id: convertingQuotation.id,
                status: "Converted to Sales Order",
                converted_to_sales_order: newSalesOrder.order_id || newSalesOrder.id,
                converted_date: new Date().toISOString().split('T')[0]
              })
            })
            
            if (quotationUpdateResponse.ok) {
              // Refresh quotations list
              await loadQuotations()
            }
          } catch (updateError) {
            console.error("Error updating quotation status:", updateError)
          }
        }
        
        toast({
          title: "Sales Order Created",
          description: `Sales order ${newSalesOrder.order_id} has been successfully created from quotation ${convertingQuotation?.quote_number || convertingQuotation?.quotationId || convertingQuotation?.id}.`
        })
        
        // Close modal and clear state
        setIsSalesOrderModalOpen(false)
        setConvertingQuotation(null)
        
        // Navigate to sales orders page after a delay to show the success message
        setTimeout(() => {
          window.location.href = '/sales-orders'
        }, 2000) // 2 second delay
      } else {
        // Try to parse error as JSON first, fall back to text
        let errorMessage = 'Failed to create sales order via API'
        let errorDetails = ''

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

        // Show detailed error to user
        toast({
          title: "Failed to create sales order",
          description: errorDetails ? `${errorMessage}: ${errorDetails}` : errorMessage,
          variant: "destructive"
        })
        throw new Error(errorMessage)
      }
    } catch (error) {
      console.error("Failed to create sales order:", error)
      
      // Fallback to localStorage
      try {
        const fallbackOrderData = {
          orderId: `SO-${new Date().getFullYear()}-${String(Math.floor(Math.random() * 1000)).padStart(3, '0')}`,
          date: new Date().toLocaleDateString('en-GB'),
          quotation_ref: convertingQuotation?.quotationId || convertingQuotation?.id,
          ...salesOrderData,
          notes: salesOrderData.notes ? 
            `${salesOrderData.notes}\n\nConverted from quotation ${convertingQuotation?.quotationId || convertingQuotation?.id}` :
            `Converted from quotation ${convertingQuotation?.quotationId || convertingQuotation?.id}`
        }
        
        const newSalesOrder = storageService.create('salesOrders', fallbackOrderData)
        
        if (newSalesOrder) {
          // Update quotation status
          if (convertingQuotation) {
            storageService.update('quotations', convertingQuotation.id, {
              ...convertingQuotation,
              status: "Converted to Sales Order",
              convertedToSalesOrder: newSalesOrder.id || newSalesOrder.orderId,
              convertedDate: new Date().toLocaleDateString('en-GB')
            })
            loadQuotations()
          }
          
          toast({
            title: "Sales Order Created (local)",
            description: `Sales order ${newSalesOrder.orderId || newSalesOrder.id} has been created locally from quotation.`
          })
          
          setIsSalesOrderModalOpen(false)
          setConvertingQuotation(null)
          window.location.href = '/sales-orders'
        } else {
          throw new Error('Failed to create sales order')
        }
      } catch (fallbackError) {
        const errorMessage = fallbackError instanceof Error ? fallbackError.message : 'Unknown error occurred'
        toast({
          title: "Error",
          description: `Failed to create sales order from quotation: ${errorMessage}`,
          variant: "destructive"
        })
      }
    }
  }

  const handleSaveInvoiceFromQuotation = async (invoiceData: any) => {
    console.log("handleSaveInvoiceFromQuotation called with:", invoiceData)
    
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
          description: `Invoice ${newInvoice?.invoice_number || invoiceData.invoice_number} has been successfully created from quotation ${convertingQuotation?.quote_number || convertingQuotation?.quotationId || convertingQuotation?.id}.`
        })
        
        // Close modal and clear state
        setIsInvoiceModalOpen(false)
        setConvertingQuotation(null)
        
        // Navigate to invoices page after a delay to show the success message
        setTimeout(() => {
          window.location.href = '/invoices'
        }, 2000) // 2 second delay
      } else {
        // Try to parse error as JSON first, fall back to text
        let errorMessage = 'Failed to create invoice via API'
        let errorDetails = ''

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

        // Show detailed error to user
        toast({
          title: "Failed to create invoice",
          description: errorDetails ? `${errorMessage}: ${errorDetails}` : errorMessage,
          variant: "destructive"
        })
        return
      }
    } catch (error) {
      console.error('Error creating invoice:', error)
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred'
      toast({
        title: "Error",
        description: `Failed to create invoice from quotation: ${errorMessage}`,
        variant: "destructive"
      })
    }
  }

  const handleUpdateQuotation = async (quotationData: any) => {
    console.log("handleUpdateQuotation called with:", quotationData)
    
    try {
      const response = await fetch('/api/quotations', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          id: editingQuotation.id,
          ...quotationData
        })
      })
      
      if (response.ok) {
        const data = await response.json()
        const updatedQuotation = data.quotation
        console.log("Updated quotation:", updatedQuotation)
        
        // Refresh quotations list
        await loadQuotations()
        
        toast({
          title: "Quotation updated",
          description: `Quotation ${updatedQuotation.quote_number} has been successfully updated.`
        })
        setIsEditModalOpen(false)
        setEditingQuotation(null)
      } else {
        throw new Error('Failed to update quotation')
      }
    } catch (error) {
      console.error("Failed to update quotation:", error)
      // Fallback to localStorage
      const updatedQuotation = storageService.update('quotations', editingQuotation.id, quotationData)
      if (updatedQuotation) {
        loadQuotations()
        toast({
          title: "Quotation updated (local)",
          description: `Quotation ${updatedQuotation.quotationId || updatedQuotation.id} has been updated locally.`
        })
        setIsEditModalOpen(false)
        setEditingQuotation(null)
      } else {
        toast({
          title: "Error",
          description: "Failed to update quotation",
          variant: "destructive"
        })
      }
    }
  }

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "draft":
        return "bg-gray-100 text-gray-800"
      case "sent":
        return "bg-blue-100 text-blue-800"
      case "under review":
        return "bg-yellow-100 text-yellow-800"
      case "accepted":
        return "bg-green-100 text-green-800"
      case "rejected":
        return "bg-red-100 text-red-800"
      case "expired":
        return "bg-orange-100 text-orange-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  // Filter quotations based on search and status
  const filteredQuotations = quotationsList.filter(quotation => {
    const matchesSearch = searchTerm === "" || 
      quotation.id?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      quotation.quote_number?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      quotation.customer_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      quotation.products_quoted?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      quotation.contact_person?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesStatus = selectedStatus === "All" || quotation.status === selectedStatus

    const matchesAssigned = selectedAssignedTo === "All" || quotation.assignedTo === selectedAssignedTo

    return matchesSearch && matchesStatus && matchesAssigned
  })

  // Enhanced stats with icons and colors
  const stats = [
    {
      title: "Total Quotations",
      value: quotationStats.total.toString(),
      change: `+${quotationStats.thisMonthCount || 0} this month`,
      icon: FileText,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Pending Review",
      value: quotationStats.pendingReview.toString(),
      change: "Awaiting response",
      icon: Clock,
      iconColor: "text-yellow-600",
      iconBg: "bg-yellow-100"
    },
    {
      title: "Acceptance Rate",
      value: `${quotationStats.acceptanceRate}%`,
      change: quotationStats.acceptanceRate >= 50 ? "Good performance" : "Needs improvement",
      icon: quotationStats.acceptanceRate >= 50 ? TrendingUp : TrendingDown,
      iconColor: quotationStats.acceptanceRate >= 50 ? "text-green-600" : "text-red-600",
      iconBg: quotationStats.acceptanceRate >= 50 ? "bg-green-100" : "bg-red-100"
    },
    {
      title: "Total Value",
      value: `₹${(quotationStats.totalValue / 100000).toFixed(1)}L`,
      change: "Active quotations",
      icon: IndianRupee,
      iconColor: "text-purple-600",
      iconBg: "bg-purple-100"
    }
  ]

  // Get unique assigned users for filter
  const uniqueAssignedUsers = Array.from(new Set(quotationsList.map(q => q.assignedTo).filter(Boolean)))

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Quotations Management</h1>
          <p className="text-gray-600">Create and manage quotations for your customers</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button onClick={() => setIsDataImportModalOpen(true)} variant="outline">
            <Upload className="w-4 h-4 mr-2" />
            Import Quotations
          </Button>
          <Button onClick={() => {
            console.log('Create Quotation button clicked')
            try {
              setIsAddQuotationModalOpen(true)
              console.log('Modal state set to true')
            } catch (error) {
              console.error('Error setting modal state:', error)
            }
          }} className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Create Quotation
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

      {/* Quotation Search & Filters */}
      <Card>
        <CardHeader>
          <CardTitle>Quotation Search & Filters</CardTitle>
          <CardDescription>Filter and search through your quotations</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                placeholder="Search quotations by ID, account, contact, or product..."
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
                <SelectItem value="Under Review">Under Review</SelectItem>
                <SelectItem value="Accepted">Accepted</SelectItem>
                <SelectItem value="Rejected">Rejected</SelectItem>
                <SelectItem value="Expired">Expired</SelectItem>
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

      {/* Quotations List - Card View */}
      <Card>
        <CardHeader>
          <CardTitle>Quotations List</CardTitle>
          <CardDescription>Showing {filteredQuotations.length} of {quotationsList.length} quotations</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredQuotations.length === 0 ? (
              <div className="text-center py-12">
                <FileText className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No quotations found</p>
                <p className="text-sm text-gray-400 mt-2">
                  {searchTerm || selectedStatus !== "All" || selectedAssignedTo !== "All" 
                    ? "Try adjusting your filters" 
                    : "Click 'Create Quotation' to create your first quotation"}
                </p>
              </div>
            ) : (
              filteredQuotations.map((quotation) => (
                <div
                  key={quotation.id}
                  className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-center space-x-6">
                    <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                      <FileText className="w-6 h-6 text-blue-600" />
                    </div>
                    
                    {/* Quotation Details */}
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="font-semibold text-gray-900">
                          {quotation.quote_number || quotation.id}
                        </h3>
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Date: {quotation.quote_date} | Valid Until: {quotation.valid_until}
                      </p>
                    </div>
                    
                    {/* Customer Details */}
                    <div className="border-l pl-6">
                      <div className="flex items-center gap-1 text-sm">
                        <Building className="w-4 h-4 text-gray-400" />
                        <span className="font-medium">{quotation.customer_name}</span>
                      </div>
                      <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                        <User className="w-4 h-4 text-gray-400" />
                        <span>{quotation.contact_person}</span>
                      </div>
                      {quotation.customerPhone && (
                        <div className="flex items-center gap-1 text-sm text-gray-600 mt-1">
                          <Phone className="w-3 h-3 text-gray-400" />
                          <span>{quotation.customer_phone}</span>
                        </div>
                      )}
                    </div>
                    
                    {/* Product & Value Details */}
                    <div className="border-l pl-6">
                      {quotation.products_quoted && (
                        <div className="flex items-center gap-1 text-sm">
                          <Package className="w-4 h-4 text-gray-400" />
                          <span className="font-medium max-w-xs truncate" title={quotation.products_quoted}>{quotation.products_quoted}</span>
                        </div>
                      )}
                      <div className="flex items-center gap-1 text-sm mt-1">
                        <IndianRupee className="w-4 h-4 text-green-600" />
                        <span className="font-semibold text-green-600">₹{quotation.total_amount?.toLocaleString()}</span>
                      </div>
                      {quotation.assigned_to && (
                        <p className="text-xs text-gray-500 mt-1">
                          Assigned to: {quotation.assigned_to}
                        </p>
                      )}
                    </div>
                  </div>
                  
                  {/* Action Buttons - Two Rows */}
                  <div className="flex flex-col items-end space-y-2">
                    {/* First Row - Main Actions */}
                    <div className="flex items-center space-x-1">
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Edit Quotation"
                        onClick={() => handleEditQuotation(quotation)}
                      >
                        <Edit className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Download PDF"
                        onClick={() => handleDownloadQuotation(quotation)}
                        className="text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                      >
                        <Download className="w-4 h-4" />
                      </Button>
                    </div>
                    
                    {/* Second Row - Communication Actions */}
                    <div className="flex items-center space-x-1">
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Send Email"
                        onClick={() => handleSendEmail(quotation)}
                        className="text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                      >
                        <Mail className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        title="Send WhatsApp"
                        onClick={() => handleSendWhatsApp(quotation)}
                        className="text-green-600 hover:text-green-800 hover:bg-green-50"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                    </div>
                    
                    {/* Third Row - Business Actions */}
                    {quotation.status !== "Converted to Sales Order" && (
                      <div className="flex items-center space-x-1">
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          title="Generate Invoice"
                          onClick={() => handleGenerateInvoice(quotation)}
                          className="text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                        >
                          <FileText className="w-4 h-4" />
                        </Button>
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          title="Convert to Sales Order"
                          onClick={() => handleConvertToSalesOrder(quotation)}
                          className="text-purple-600 hover:text-purple-800 hover:bg-purple-50"
                        >
                          <ShoppingCart className="w-4 h-4" />
                        </Button>
                      </div>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>

      {/* Add Quotation Modal */}
      <ErrorBoundary
        onError={(error) => {
          console.error('AddQuotationModal error:', error)
          console.error('Error stack:', error.stack)
          console.error('Error details:', { 
            message: error.message, 
            name: error.name,
            componentStack: error.componentStack 
          })
          toast({
            title: "Modal Error",
            description: `Failed to open quotation modal: ${error.message}`,
            variant: "destructive"
          })
          setIsAddQuotationModalOpen(false)
        }}
      >
        <AddQuotationModal
          isOpen={isAddQuotationModalOpen}
          onClose={() => setIsAddQuotationModalOpen(false)}
          onSave={handleSaveQuotation}
        />
      </ErrorBoundary>
      
      {/* Edit Quotation Modal */}
      {isEditModalOpen && editingQuotation && (
        <ErrorBoundary
          onError={(error) => {
            console.error('Edit AddQuotationModal error:', error)
            toast({
              title: "Modal Error", 
              description: "Failed to open edit quotation modal. Please try again.",
              variant: "destructive"
            })
            setIsEditModalOpen(false)
            setEditingQuotation(null)
          }}
        >
          <AddQuotationModal
            key={editingQuotation.id} // Force re-render with new data
            isOpen={isEditModalOpen}
            onClose={() => {
              console.log("Closing edit modal")
              setIsEditModalOpen(false)
              setEditingQuotation(null)
            }}
            onSave={handleUpdateQuotation}
            editingQuotation={editingQuotation}
          />
        </ErrorBoundary>
      )}
      
      {/* View Quotation Modal */}
      <ViewQuotationModal
        isOpen={isViewModalOpen}
        onClose={() => {
          setIsViewModalOpen(false)
          setViewingQuotation(null)
        }}
        quotation={viewingQuotation}
        onPrint={() => handleDownloadQuotation(viewingQuotation)}
        onSendEmail={() => handleSendEmail(viewingQuotation)}
        onSendWhatsApp={() => handleSendWhatsApp(viewingQuotation)}
      />
      
      {/* Add Sales Order Modal for Quotation Conversion */}
      <AddSalesOrderModal
        isOpen={isSalesOrderModalOpen}
        onClose={() => {
          setIsSalesOrderModalOpen(false)
          setConvertingQuotation(null)
        }}
        onSave={handleSaveSalesOrderFromQuotation}
        editingData={convertingQuotation ? {
          // Map quotation data to sales order form fields
          accountName: convertingQuotation.customer_name || convertingQuotation.accountName,
          contactPerson: convertingQuotation.contact_person || convertingQuotation.contactPerson || convertingQuotation.contactName,
          customerEmail: convertingQuotation.customer_email || convertingQuotation.customerEmail,
          customerPhone: convertingQuotation.customer_phone || convertingQuotation.customerPhone,
          billingAddress: convertingQuotation.billing_address || convertingQuotation.billingAddress,
          shippingAddress: convertingQuotation.shipping_address || convertingQuotation.shippingAddress || convertingQuotation.billing_address || convertingQuotation.billingAddress,
          quotationRef: convertingQuotation.quote_number || convertingQuotation.quotationId || convertingQuotation.id,
          orderDate: new Date().toISOString().split('T')[0],
          expectedDelivery: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // 30 days from now
          shippingMethod: "Standard",
          notes: `Converted from quotation ${convertingQuotation.quote_number || convertingQuotation.quotationId || convertingQuotation.id}`,
          priority: convertingQuotation.priority || "Medium",
          paymentTerms: convertingQuotation.payment_terms || convertingQuotation.paymentTerms || "50% Advance, 50% on Delivery",
          deliveryTerms: convertingQuotation.delivery_terms || convertingQuotation.deliveryTerms || "4-6 weeks",
          currency: convertingQuotation.currency || "INR",
          assignedTo: convertingQuotation.assigned_to || convertingQuotation.assignedTo,
          items: convertingQuotation.line_items || convertingQuotation.items || 
            (convertingQuotation.products_quoted && convertingQuotation.products_quoted.includes(',') ? 
              // Handle multiple products separated by commas
              convertingQuotation.products_quoted.split(',').map((productName: string, index: number) => ({
                id: (index + 1).toString(),
                product: productName.trim(),
                description: convertingQuotation.product_description || convertingQuotation.description || "",
                quantity: 1,
                unitPrice: Math.round((convertingQuotation.total_amount || convertingQuotation.amount || 0) / convertingQuotation.products_quoted.split(',').length),
                discount: 0,
                taxRate: 18,
                amount: Math.round((convertingQuotation.total_amount || convertingQuotation.amount || 0) / convertingQuotation.products_quoted.split(',').length),
                deliveryDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]
              })) : 
              // Single product fallback
              [{
                id: "1",
                product: convertingQuotation.products_quoted || convertingQuotation.product || "Product/Service",
                description: convertingQuotation.product_description || convertingQuotation.description || "",
                quantity: 1,
                unitPrice: convertingQuotation.total_amount || convertingQuotation.amount || 0,
                discount: 0,
                taxRate: 18,
                amount: convertingQuotation.total_amount || convertingQuotation.amount || 0,
                deliveryDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]
              }]
            ),
          totals: convertingQuotation.totals || {
            subtotal: Math.round((convertingQuotation.total_amount || convertingQuotation.amount || 0) / 1.18),
            totalDiscount: 0,
            totalTax: Math.round((convertingQuotation.total_amount || convertingQuotation.amount || 0) - ((convertingQuotation.total_amount || convertingQuotation.amount || 0) / 1.18)),
            grandTotal: convertingQuotation.total_amount || convertingQuotation.amount || 0
          }
        } : undefined}
      />
      
      {/* Add Invoice Modal for Quotation Conversion */}
      <AddInvoiceModal
        isOpen={isInvoiceModalOpen}
        onClose={() => {
          setIsInvoiceModalOpen(false)
          setConvertingQuotation(null)
        }}
        onSave={handleSaveInvoiceFromQuotation}
        editingData={convertingQuotation}
      />
      
      {/* DataImportModal component */}
      <DataImportModal 
        isOpen={isDataImportModalOpen} 
        onClose={() => setIsDataImportModalOpen(false)}
        moduleType="quotations"
        onImport={(data) => console.log('Imported quotations:', data)}
      />
    </div>
  )
}