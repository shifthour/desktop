"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Checkbox } from "@/components/ui/checkbox"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Receipt, Plus, Minus, User, FileText, Settings, Save, X, Calendar, ShoppingCart } from "lucide-react"

interface AddInvoiceModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (invoiceData: any) => void
  editingData?: any
}

interface InvoiceItem {
  id: string
  product: string
  description: string
  hsnCode: string
  quantity: number
  unitPrice: number
  discount: number
  taxRate: number
  taxAmount: number
  amount: number
}

interface FormData {
  // Customer Information
  accountName: string
  contactPerson: string
  customerEmail: string
  customerPhone: string
  billingAddress: string
  shippingAddress: string
  gstNumber: string
  panNumber: string
  
  // Invoice Details
  invoiceDate: string
  dueDate: string
  salesOrderRef: string
  challanNumber: string
  placeOfSupply: string
  invoiceType: string
  notes: string
  
  // Business Settings
  assignedTo: string
  priority: string
  currency: string
  paymentStatus: string
  paymentMethod: string
  
  // Tax & Compliance
  taxType: string
  reverseCharge: boolean
  exportInvoice: boolean
  eInvoice: boolean
  
  // Bank Details
  bankName: string
  accountNumber: string
  ifscCode: string
  branchName: string
}

export function AddInvoiceModal({ isOpen, onClose, onSave, editingData }: AddInvoiceModalProps) {
  const [formData, setFormData] = useState<FormData>({
    accountName: "",
    contactPerson: "",
    customerEmail: "",
    customerPhone: "",
    billingAddress: "",
    shippingAddress: "",
    gstNumber: "",
    panNumber: "",
    invoiceDate: new Date().toISOString().split('T')[0],
    dueDate: "",
    salesOrderRef: "",
    challanNumber: "",
    placeOfSupply: "",
    invoiceType: "Tax Invoice",
    notes: "",
    assignedTo: "",
    priority: "Medium",
    currency: "INR",
    paymentStatus: "Unpaid",
    paymentMethod: "Bank Transfer",
    taxType: "GST",
    reverseCharge: false,
    exportInvoice: false,
    eInvoice: false,
    bankName: "State Bank of India",
    accountNumber: "",
    ifscCode: "",
    branchName: ""
  })

  const [items, setItems] = useState<InvoiceItem[]>([
    {
      id: "1",
      product: "",
      description: "",
      hsnCode: "",
      quantity: 1,
      unitPrice: 0,
      discount: 0,
      taxRate: 18,
      taxAmount: 0,
      amount: 0
    }
  ])

  const [activeTab, setActiveTab] = useState("customer")

  // Populate form data when editingData is provided (for quotation/sales order conversion)
  useEffect(() => {
    if (editingData) {
      console.log("AddInvoiceModal - editingData received:", editingData)
      console.log("=== INVOICE MODAL DEBUG START ===")
      console.log("editingData.products_quoted:", editingData.products_quoted)
      console.log("editingData.product:", editingData.product)
      console.log("editingData.product_description:", editingData.product_description)
      console.log("editingData.description:", editingData.description)
      console.log("=== INVOICE MODAL DEBUG END ===")
      // Determine if this is from a quotation or sales order
      const isFromQuotation = editingData.quote_number || editingData.quotationId
      const isFromSalesOrder = editingData.order_id || editingData.orderId
      
      const sourceReference = isFromQuotation 
        ? `Generated from Quotation: ${editingData.quote_number || editingData.quotationId || editingData.id}`
        : `Generated from Sales Order: ${editingData.order_id || editingData.orderId || editingData.id}`

      setFormData(prev => ({
        ...prev,
        accountName: editingData.customer_name || editingData.account || "",
        contactPerson: editingData.contact_person || editingData.contactName || "",
        customerEmail: editingData.customer_email || editingData.customerEmail || "",
        customerPhone: editingData.customer_phone || editingData.customerPhone || "",
        billingAddress: editingData.billing_address || editingData.billingAddress || "",
        shippingAddress: editingData.shipping_address || editingData.shippingAddress || "",
        salesOrderRef: isFromSalesOrder 
          ? (editingData.order_id || editingData.orderId || editingData.id) 
          : (editingData.quote_number || editingData.quotationId || editingData.id),
        notes: editingData.notes ? `${editingData.notes}\n\n${sourceReference}` : sourceReference,
        assignedTo: editingData.assigned_to || editingData.assignedTo || "",
        priority: editingData.priority || "Medium",
        paymentMethod: editingData.payment_terms || editingData.paymentMethod || "Bank Transfer"
      }))

      // Handle items from either quotations or sales orders
      const itemsArray = editingData.line_items || editingData.items
      if (itemsArray && itemsArray.length > 0) {
        console.log("AddInvoiceModal - Processing items from editingData:", itemsArray)
        const convertedItems = itemsArray.map((item: any, index: number) => {
          // Handle different field name variations
          const quantity = item.quantity || item.qty || 1
          const unitPrice = item.unitPrice || item.unit_price || item.price || 0
          const discount = item.discount || item.discount_percent || 0
          const taxRate = item.taxRate || item.tax_rate || item.gst_rate || 18
          
          console.log("Converting item:", {
            original: item,
            extracted: { quantity, unitPrice, discount, taxRate }
          })
          
          // Calculate amounts properly
          const subtotal = quantity * unitPrice
          const discountAmount = subtotal * (discount / 100)
          const taxableAmount = subtotal - discountAmount
          const taxAmount = taxableAmount * (taxRate / 100)
          const amount = taxableAmount + taxAmount
          
          return {
            id: String(index + 1),
            product: item.product || item.product_name || "",
            description: item.description || item.product_description || "",
            hsnCode: item.hsnCode || item.hsn_code || "",
            quantity,
            unitPrice,
            discount,
            taxRate,
            taxAmount,
            amount
          }
        })
        console.log("AddInvoiceModal - Setting converted items:", convertedItems)
        setItems(convertedItems)
      } else if (editingData.total_amount || editingData.amount) {
        // Handle multiple products or single product
        const totalAmount = editingData.total_amount || editingData.amount || 0
        const productString = editingData.products_quoted || editingData.product || editingData.productName || ""
        console.log("AddInvoiceModal - Processing single/multiple products:", {
          totalAmount,
          productString,
          products_quoted: editingData.products_quoted,
          product: editingData.product,
          productName: editingData.productName
        })
        
        if (productString && productString.includes(',')) {
          // Handle multiple products separated by commas
          const productNames = productString.split(',')
          const pricePerProduct = totalAmount / productNames.length
          
          const multipleItems = productNames.map((productName: string, index: number) => {
            const quantity = 1
            const discount = 0
            const taxRate = 18
            
            // Calculate for each product
            const taxableAmount = pricePerProduct / (1 + taxRate / 100)
            const unitPrice = taxableAmount / quantity
            const taxAmount = taxableAmount * (taxRate / 100)
            
            return {
              id: String(index + 1),
              product: productName.trim(),
              description: editingData.product_description || editingData.description || `${productName.trim()} - High quality laboratory equipment`,
              hsnCode: "",
              quantity,
              unitPrice,
              discount,
              taxRate,
              taxAmount,
              amount: pricePerProduct
            }
          })
          
          console.log("AddInvoiceModal - Setting multiple items:", multipleItems)
          setItems(multipleItems)
        } else {
          // Single item based on total amount - reverse calculate from total
          const quantity = 1
          const discount = 0
          const taxRate = 18
          
          // Reverse calculate: if total includes tax, find the base price
          const taxableAmount = totalAmount / (1 + taxRate / 100)
          const unitPrice = taxableAmount / quantity
          const taxAmount = taxableAmount * (taxRate / 100)
          
          setItems([{
            id: "1",
            product: productString,
            description: editingData.product_description || editingData.description || `${productString} - Professional laboratory equipment`,
            hsnCode: "",
            quantity,
            unitPrice,
            discount,
            taxRate,
            taxAmount,
            amount: totalAmount
          }])
          console.log("AddInvoiceModal - Setting single item:", {
            product: productString,
            description: editingData.product_description || editingData.description || "",
            unitPrice,
            taxAmount,
            amount: totalAmount
          })
        }
      }
    }
  }, [editingData])

  const handleInputChange = (field: keyof FormData, value: string | boolean) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  const addItem = () => {
    const newItem: InvoiceItem = {
      id: Date.now().toString(),
      product: "",
      description: "",
      hsnCode: "",
      quantity: 1,
      unitPrice: 0,
      discount: 0,
      taxRate: 18,
      taxAmount: 0,
      amount: 0
    }
    setItems(prev => [...prev, newItem])
  }

  const removeItem = (id: string) => {
    if (items.length > 1) {
      setItems(prev => prev.filter(item => item.id !== id))
    }
  }

  const updateItem = (id: string, field: keyof InvoiceItem, value: string | number) => {
    setItems(prev => prev.map(item => {
      if (item.id === id) {
        const updated = { ...item, [field]: value }
        // Calculate amounts when quantity, unit price, discount, or tax rate changes
        if (field === 'quantity' || field === 'unitPrice' || field === 'discount' || field === 'taxRate') {
          const subtotal = updated.quantity * updated.unitPrice
          const discountAmount = subtotal * (updated.discount / 100)
          const taxableAmount = subtotal - discountAmount
          updated.taxAmount = taxableAmount * (updated.taxRate / 100)
          updated.amount = taxableAmount + updated.taxAmount
        }
        return updated
      }
      return item
    }))
  }

  const calculateTotals = () => {
    const subtotal = items.reduce((sum, item) => {
      return sum + (item.quantity * item.unitPrice)
    }, 0)
    
    const totalDiscount = items.reduce((sum, item) => {
      const itemSubtotal = item.quantity * item.unitPrice
      return sum + (itemSubtotal * item.discount / 100)
    }, 0)
    
    const taxableAmount = subtotal - totalDiscount
    const totalTax = items.reduce((sum, item) => sum + item.taxAmount, 0)
    const grandTotal = taxableAmount + totalTax
    
    // Calculate CGST, SGST for intra-state or IGST for inter-state
    const isInterState = formData.placeOfSupply !== "Karnataka" // Assuming company is in Karnataka
    const cgst = isInterState ? 0 : totalTax / 2
    const sgst = isInterState ? 0 : totalTax / 2
    const igst = isInterState ? totalTax : 0
    
    return { 
      subtotal, 
      totalDiscount, 
      taxableAmount, 
      totalTax, 
      grandTotal,
      cgst,
      sgst, 
      igst,
      isInterState
    }
  }

  const handleSubmit = () => {
    const totals = calculateTotals()
    const invoiceData = {
      items,
      totals,
      invoice_number: `INV-${new Date().getFullYear()}-${Date.now().toString().slice(-6)}`,
      customer_name: formData.accountName,
      contact_person: formData.contactPerson,
      customer_email: formData.customerEmail,
      customer_phone: formData.customerPhone,
      billing_address: formData.billingAddress,
      shipping_address: formData.shippingAddress,
      invoice_date: formData.invoiceDate,
      due_date: formData.dueDate,
      total_amount: totals.grandTotal,
      status: 'draft',
      payment_status: 'unpaid',
      payment_method: formData.paymentTerms || 'Bank Transfer',
      priority: 'Medium',
      gst_number: formData.gstNumber,
      pan_number: formData.panNumber,
      notes: formData.notes,
      assigned_to: formData.assignedTo,
      bank_name: formData.bankName,
      branch_name: formData.branchName,
      account_number: formData.accountNumber,
      ifsc_code: formData.ifscCode,
      company_id: 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
    }
    
    console.log("=== INVOICE DATA BEING SENT ===")
    console.log("Invoice Data:", invoiceData)
    console.log("Fields in invoiceData:", Object.keys(invoiceData))
    console.log("accountName present?", 'accountName' in invoiceData)
    console.log("customer_name value:", invoiceData.customer_name)
    onSave(invoiceData)
  }

  // Options
  const baseAccounts = ["TSAR Labcare", "Eurofins Advinus", "Kerala Agricultural University", "JNCASR", "Guna Foods", "Bio-Rad Laboratories"]
  
  // Add customer name from editingData to accounts if it's not already there
  let accounts = [...baseAccounts]
  if (editingData?.customer_name || editingData?.account) {
    const customerName = editingData.customer_name || editingData.account
    if (!baseAccounts.includes(customerName)) {
      accounts = [customerName, ...baseAccounts]
    }
  }
  const baseProducts = ["TRICOLOR MULTICHANNEL FIBRINOMETER", "LABORATORY FREEZE DRYER/LYOPHILIZER", "ND 1000 Spectrophotometer", "Automated Media Preparator", "Bio-Safety Cabinet"]
  
  // Add products from editingData to products list if they're not already there
  let products = [...baseProducts]
  
  if (editingData?.items) {
    editingData.items.forEach((item: any) => {
      if (item.product && !products.includes(item.product)) {
        products.unshift(item.product)
      }
    })
  } else if (editingData?.products_quoted || editingData?.product) {
    const productString = editingData.products_quoted || editingData.product
    console.log("Processing productString:", productString)
    
    if (productString) {
      if (productString.includes(',')) {
        // Handle multiple products separated by commas
        const productNames = productString.split(',').map((p: string) => p.trim())
        console.log("Split product names:", productNames)
        productNames.forEach((productName: string) => {
          if (productName && !products.includes(productName)) {
            products.unshift(productName)
          }
        })
      } else {
        // Single product
        if (!products.includes(productString)) {
          products.unshift(productString)
        }
      }
    }
  }
  
  console.log("Final products array:", products)
  const assignees = ["Hari Kumar K", "Prashanth Sandilya", "Vijay Muppala", "Pauline"]
  const priorities = ["Low", "Medium", "High", "Urgent"]
  const paymentStatuses = ["Unpaid", "Partially Paid", "Paid", "Overdue", "Cancelled"]
  const paymentMethods = ["Bank Transfer", "Cheque", "Cash", "UPI", "Credit Card", "Online Payment"]
  const invoiceTypes = ["Tax Invoice", "Proforma Invoice", "Credit Note", "Debit Note", "Export Invoice"]
  const states = ["Karnataka", "Tamil Nadu", "Kerala", "Andhra Pradesh", "Telangana", "Maharashtra", "Gujarat", "Delhi", "Other"]

  const totals = calculateTotals()

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-7xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center space-x-2 text-xl">
            <FileText className="w-6 h-6 text-blue-600" />
            <span>Create New Invoice</span>
          </DialogTitle>
          <DialogDescription>
            Generate GST compliant invoice for completed sales orders
          </DialogDescription>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="customer" className="flex items-center space-x-2">
              <User className="w-4 h-4" />
              <span>Customer</span>
            </TabsTrigger>
            <TabsTrigger value="invoice" className="flex items-center space-x-2">
              <FileText className="w-4 h-4" />
              <span>Invoice Details</span>
            </TabsTrigger>
            <TabsTrigger value="items" className="flex items-center space-x-2">
              <span>📋</span>
              <span>Items & Tax</span>
            </TabsTrigger>
            <TabsTrigger value="payment" className="flex items-center space-x-2">
              <Settings className="w-4 h-4" />
              <span>Payment & Bank</span>
            </TabsTrigger>
          </TabsList>

          <TabsContent value="customer" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Customer Information</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="accountName" className="text-sm font-medium">
                      Account Name <span className="text-red-500">*</span>
                    </Label>
                    <Select value={formData.accountName} onValueChange={(value) => handleInputChange("accountName", value)}>
                      <SelectTrigger className="mt-1">
                        <SelectValue placeholder="Select account" />
                      </SelectTrigger>
                      <SelectContent>
                        {accounts.map((account) => (
                          <SelectItem key={account} value={account}>{account}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div>
                    <Label htmlFor="contactPerson" className="text-sm font-medium">
                      Contact Person <span className="text-red-500">*</span>
                    </Label>
                    <Input
                      id="contactPerson"
                      value={formData.contactPerson}
                      onChange={(e) => handleInputChange("contactPerson", e.target.value)}
                      placeholder="Primary contact person"
                      className="mt-1"
                    />
                  </div>

                  <div>
                    <Label htmlFor="gstNumber" className="text-sm font-medium">
                      GST Number <span className="text-red-500">*</span>
                    </Label>
                    <Input
                      id="gstNumber"
                      value={formData.gstNumber}
                      onChange={(e) => handleInputChange("gstNumber", e.target.value)}
                      placeholder="29AABCU9603R1ZX"
                      className="mt-1"
                    />
                  </div>

                  <div>
                    <Label htmlFor="panNumber" className="text-sm font-medium">PAN Number</Label>
                    <Input
                      id="panNumber"
                      value={formData.panNumber}
                      onChange={(e) => handleInputChange("panNumber", e.target.value)}
                      placeholder="AABCU9603R"
                      className="mt-1"
                    />
                  </div>

                  <div>
                    <Label htmlFor="customerEmail" className="text-sm font-medium">Email Address</Label>
                    <Input
                      id="customerEmail"
                      type="email"
                      value={formData.customerEmail}
                      onChange={(e) => handleInputChange("customerEmail", e.target.value)}
                      placeholder="accounts@customer.com"
                      className="mt-1"
                    />
                  </div>

                  <div>
                    <Label htmlFor="customerPhone" className="text-sm font-medium">Phone Number</Label>
                    <Input
                      id="customerPhone"
                      value={formData.customerPhone}
                      onChange={(e) => handleInputChange("customerPhone", e.target.value)}
                      placeholder="+91 98765 43210"
                      className="mt-1"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="billingAddress" className="text-sm font-medium">
                      Billing Address <span className="text-red-500">*</span>
                    </Label>
                    <Textarea
                      id="billingAddress"
                      value={formData.billingAddress}
                      onChange={(e) => handleInputChange("billingAddress", e.target.value)}
                      placeholder="Complete billing address with pincode"
                      className="mt-1 min-h-[80px]"
                    />
                  </div>

                  <div>
                    <Label htmlFor="shippingAddress" className="text-sm font-medium">Shipping Address</Label>
                    <Textarea
                      id="shippingAddress"
                      value={formData.shippingAddress}
                      onChange={(e) => handleInputChange("shippingAddress", e.target.value)}
                      placeholder="Shipping address (if different from billing)"
                      className="mt-1 min-h-[80px]"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="invoice" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Invoice Details</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div>
                    <Label htmlFor="invoiceDate" className="text-sm font-medium">
                      Invoice Date <span className="text-red-500">*</span>
                    </Label>
                    <Input
                      id="invoiceDate"
                      type="date"
                      value={formData.invoiceDate}
                      onChange={(e) => handleInputChange("invoiceDate", e.target.value)}
                      className="mt-1"
                    />
                  </div>

                  <div>
                    <Label htmlFor="dueDate" className="text-sm font-medium">
                      Due Date <span className="text-red-500">*</span>
                    </Label>
                    <Input
                      id="dueDate"
                      type="date"
                      value={formData.dueDate}
                      onChange={(e) => handleInputChange("dueDate", e.target.value)}
                      className="mt-1"
                    />
                  </div>

                  <div>
                    <Label htmlFor="invoiceType" className="text-sm font-medium">Invoice Type</Label>
                    <Select value={formData.invoiceType} onValueChange={(value) => handleInputChange("invoiceType", value)}>
                      <SelectTrigger className="mt-1">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {invoiceTypes.map((type) => (
                          <SelectItem key={type} value={type}>{type}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="salesOrderRef" className="text-sm font-medium">Sales Order Reference/Quotation Reference</Label>
                    <Input
                      id="salesOrderRef"
                      value={formData.salesOrderRef}
                      onChange={(e) => handleInputChange("salesOrderRef", e.target.value)}
                      placeholder="SO-2025-001 or QT-2025-001"
                      className="mt-1"
                    />
                  </div>

                  <div>
                    <Label htmlFor="placeOfSupply" className="text-sm font-medium">
                      Place of Supply <span className="text-red-500">*</span>
                    </Label>
                    <Select value={formData.placeOfSupply} onValueChange={(value) => handleInputChange("placeOfSupply", value)}>
                      <SelectTrigger className="mt-1">
                        <SelectValue placeholder="Select state" />
                      </SelectTrigger>
                      <SelectContent>
                        {states.map((state) => (
                          <SelectItem key={state} value={state}>{state}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                <div>
                  <Label htmlFor="notes" className="text-sm font-medium">Invoice Notes</Label>
                  <Textarea
                    id="notes"
                    value={formData.notes}
                    onChange={(e) => handleInputChange("notes", e.target.value)}
                    placeholder="Special notes, payment instructions, etc."
                    className="mt-1 min-h-[80px]"
                  />
                </div>

                {/* Compliance Options */}
                <div className="space-y-3">
                  <Label className="text-sm font-medium">GST & Compliance Options</Label>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="flex items-center space-x-3">
                      <Checkbox
                        id="reverseCharge"
                        checked={formData.reverseCharge}
                        onCheckedChange={(checked) => handleInputChange("reverseCharge", checked as boolean)}
                      />
                      <div>
                        <Label htmlFor="reverseCharge" className="text-sm font-medium">Reverse Charge</Label>
                        <p className="text-xs text-gray-500">Tax payable by recipient</p>
                      </div>
                    </div>

                    <div className="flex items-center space-x-3">
                      <Checkbox
                        id="exportInvoice"
                        checked={formData.exportInvoice}
                        onCheckedChange={(checked) => handleInputChange("exportInvoice", checked as boolean)}
                      />
                      <div>
                        <Label htmlFor="exportInvoice" className="text-sm font-medium">Export Invoice</Label>
                        <p className="text-xs text-gray-500">Zero-rated supply for exports</p>
                      </div>
                    </div>

                    <div className="flex items-center space-x-3">
                      <Checkbox
                        id="eInvoice"
                        checked={formData.eInvoice}
                        onCheckedChange={(checked) => handleInputChange("eInvoice", checked as boolean)}
                      />
                      <div>
                        <Label htmlFor="eInvoice" className="text-sm font-medium">E-Invoice</Label>
                        <p className="text-xs text-gray-500">Generate IRN for B2B transactions</p>
                      </div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="items" className="space-y-6 mt-6">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between">
                <CardTitle className="text-lg">Invoice Items</CardTitle>
                <Button onClick={addItem} size="sm" className="bg-blue-600 hover:bg-blue-700">
                  <Plus className="w-4 h-4 mr-2" />
                  Add Item
                </Button>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-48">Product/Service</TableHead>
                        <TableHead className="w-40">Description</TableHead>
                        <TableHead className="w-20">HSN Code</TableHead>
                        <TableHead className="w-14">Qty</TableHead>
                        <TableHead className="w-20">Unit Price</TableHead>
                        <TableHead className="w-14">Disc %</TableHead>
                        <TableHead className="w-14">Tax %</TableHead>
                        <TableHead className="w-20">Tax Amount</TableHead>
                        <TableHead className="w-20">Amount</TableHead>
                        <TableHead className="w-12">Action</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {items.map((item, index) => (
                        <TableRow key={item.id}>
                          <TableCell>
                            <Select
                              value={item.product}
                              onValueChange={(value) => updateItem(item.id, "product", value)}
                            >
                              <SelectTrigger className="w-48">
                                <SelectValue placeholder="Select product" />
                              </SelectTrigger>
                              <SelectContent>
                                {products.map((product) => (
                                  <SelectItem key={product} value={product}>{product}</SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </TableCell>
                          <TableCell>
                            <Input
                              value={item.description}
                              onChange={(e) => updateItem(item.id, "description", e.target.value)}
                              placeholder="Product description"
                              className="w-48"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={item.hsnCode}
                              onChange={(e) => updateItem(item.id, "hsnCode", e.target.value)}
                              placeholder="84219900"
                              className="w-20"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              min="1"
                              value={item.quantity}
                              onChange={(e) => updateItem(item.id, "quantity", parseInt(e.target.value) || 1)}
                              className="w-16"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              step="0.01"
                              value={item.unitPrice}
                              onChange={(e) => updateItem(item.id, "unitPrice", parseFloat(e.target.value) || 0)}
                              className="w-24"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              min="0"
                              max="100"
                              step="0.01"
                              value={item.discount}
                              onChange={(e) => updateItem(item.id, "discount", parseFloat(e.target.value) || 0)}
                              className="w-16"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              min="0"
                              step="0.01"
                              value={item.taxRate}
                              onChange={(e) => updateItem(item.id, "taxRate", parseFloat(e.target.value) || 0)}
                              className="w-16"
                            />
                          </TableCell>
                          <TableCell className="font-medium">
                            ₹{item.taxAmount.toLocaleString()}
                          </TableCell>
                          <TableCell className="font-medium">
                            ₹{item.amount.toLocaleString()}
                          </TableCell>
                          <TableCell>
                            {items.length > 1 && (
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => removeItem(item.id)}
                                className="text-red-600 hover:text-red-800"
                              >
                                <Minus className="w-4 h-4" />
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                {/* Tax Summary */}
                <div className="mt-6 bg-gray-50 p-4 rounded-lg">
                  <div className="flex justify-end">
                    <div className="w-96 space-y-2">
                      <div className="flex justify-between">
                        <span>Subtotal:</span>
                        <span>₹{totals.subtotal.toLocaleString()}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Total Discount:</span>
                        <span>- ₹{totals.totalDiscount.toLocaleString()}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Taxable Amount:</span>
                        <span>₹{totals.taxableAmount.toLocaleString()}</span>
                      </div>
                      
                      {totals.isInterState ? (
                        <div className="flex justify-between">
                          <span>IGST (Integrated GST):</span>
                          <span>₹{totals.igst.toLocaleString()}</span>
                        </div>
                      ) : (
                        <>
                          <div className="flex justify-between">
                            <span>CGST (Central GST):</span>
                            <span>₹{totals.cgst.toLocaleString()}</span>
                          </div>
                          <div className="flex justify-between">
                            <span>SGST (State GST):</span>
                            <span>₹{totals.sgst.toLocaleString()}</span>
                          </div>
                        </>
                      )}
                      
                      <div className="flex justify-between">
                        <span>Total Tax:</span>
                        <span>₹{totals.totalTax.toLocaleString()}</span>
                      </div>
                      <div className="flex justify-between text-lg font-bold border-t pt-2">
                        <span>Grand Total:</span>
                        <span>₹{totals.grandTotal.toLocaleString()}</span>
                      </div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="payment" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Payment Terms & Bank Details</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="assignedTo" className="text-sm font-medium">
                      Assigned To <span className="text-red-500">*</span>
                    </Label>
                    <Select value={formData.assignedTo} onValueChange={(value) => handleInputChange("assignedTo", value)}>
                      <SelectTrigger className="mt-1">
                        <SelectValue placeholder="Select assignee" />
                      </SelectTrigger>
                      <SelectContent>
                        {assignees.map((assignee) => (
                          <SelectItem key={assignee} value={assignee}>{assignee}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div>
                    <Label htmlFor="paymentStatus" className="text-sm font-medium">Payment Status</Label>
                    <Select value={formData.paymentStatus} onValueChange={(value) => handleInputChange("paymentStatus", value)}>
                      <SelectTrigger className="mt-1">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {paymentStatuses.map((status) => (
                          <SelectItem key={status} value={status}>{status}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div>
                    <Label htmlFor="paymentMethod" className="text-sm font-medium">Preferred Payment Method</Label>
                    <Select value={formData.paymentMethod} onValueChange={(value) => handleInputChange("paymentMethod", value)}>
                      <SelectTrigger className="mt-1">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {paymentMethods.map((method) => (
                          <SelectItem key={method} value={method}>{method}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div>
                    <Label htmlFor="priority" className="text-sm font-medium">Priority</Label>
                    <Select value={formData.priority} onValueChange={(value) => handleInputChange("priority", value)}>
                      <SelectTrigger className="mt-1">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {priorities.map((priority) => (
                          <SelectItem key={priority} value={priority}>{priority}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* Bank Details */}
                <div className="space-y-4">
                  <Label className="text-sm font-medium text-gray-900">Company Bank Details</Label>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="bankName" className="text-sm font-medium">Bank Name</Label>
                      <Input
                        id="bankName"
                        value={formData.bankName}
                        onChange={(e) => handleInputChange("bankName", e.target.value)}
                        className="mt-1"
                      />
                    </div>

                    <div>
                      <Label htmlFor="branchName" className="text-sm font-medium">Branch Name</Label>
                      <Input
                        id="branchName"
                        value={formData.branchName}
                        onChange={(e) => handleInputChange("branchName", e.target.value)}
                        placeholder="Branch name and location"
                        className="mt-1"
                      />
                    </div>

                    <div>
                      <Label htmlFor="accountNumber" className="text-sm font-medium">Account Number</Label>
                      <Input
                        id="accountNumber"
                        value={formData.accountNumber}
                        onChange={(e) => handleInputChange("accountNumber", e.target.value)}
                        placeholder="Bank account number"
                        className="mt-1"
                      />
                    </div>

                    <div>
                      <Label htmlFor="ifscCode" className="text-sm font-medium">IFSC Code</Label>
                      <Input
                        id="ifscCode"
                        value={formData.ifscCode}
                        onChange={(e) => handleInputChange("ifscCode", e.target.value)}
                        placeholder="SBIN0001234"
                        className="mt-1"
                      />
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        <DialogFooter className="flex justify-between pt-6 border-t">
          <Button variant="outline" onClick={onClose}>
            <X className="w-4 h-4 mr-2" />
            Cancel
          </Button>
          <div className="flex space-x-2">
            <Button onClick={handleSubmit} variant="outline">
              Save as Draft
            </Button>
            <Button onClick={handleSubmit} className="bg-blue-600 hover:bg-blue-700">
              <Save className="w-4 h-4 mr-2" />
              Generate Invoice
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}