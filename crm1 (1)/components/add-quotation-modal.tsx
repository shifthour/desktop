"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { FileText, Plus, Minus, User, Building, Calculator, Settings, Save, X, Handshake } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

interface AddQuotationModalProps {
  isOpen: boolean
  onClose: () => void
  onSave?: (quotationData: any) => void
  editingQuotation?: any
  isCreateMode?: boolean // Flag to force create mode even with editingQuotation data
}

interface QuotationItem {
  id: string
  product: string
  description: string
  quantity: number
  unitPrice: number
  discount: number
  taxRate: number
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
  
  // Quotation Details
  quotationDate: string
  validUntil: string
  reference: string
  subject: string
  notes: string
  terms: string
  
  // Business Settings
  assignedTo: string
  priority: string
  currency: string
  taxType: string
  paymentTerms: string
  deliveryTerms: string
}

export function AddQuotationModal({ isOpen, onClose, onSave, editingQuotation, isCreateMode = false }: AddQuotationModalProps) {
  console.log('AddQuotationModal render:', { isOpen, hasEditingQuotation: !!editingQuotation })
  
  const { toast } = useToast()
  const [selectedDealId, setSelectedDealId] = useState<string>("")
  const [deals, setDeals] = useState<any[]>([])
  const [loadingDeals, setLoadingDeals] = useState(false)
  
  // Helper function to get default form data
  const getDefaultFormData = (): FormData => ({
    accountName: "",
    contactPerson: "",
    customerEmail: "",
    customerPhone: "",
    billingAddress: "",
    shippingAddress: "",
    quotationDate: new Date().toISOString().split('T')[0],
    validUntil: "",
    reference: "",
    subject: "",
    notes: "",
    terms: "Payment: 50% advance, 50% on delivery\nDelivery: 4-6 weeks from order confirmation\nValidity: 30 days from quotation date",
    assignedTo: "",
    priority: "Medium",
    currency: "INR",
    taxType: "GST",
    paymentTerms: "50% Advance, 50% on Delivery",
    deliveryTerms: "4-6 weeks"
  })

  const getDefaultItems = (): QuotationItem[] => ([
    {
      id: "1",
      product: "",
      description: "",
      quantity: 1,
      unitPrice: 0,
      discount: 0,
      taxRate: 18,
      amount: 0
    }
  ])

  const [formData, setFormData] = useState<FormData>(() => getDefaultFormData())
  const [items, setItems] = useState<QuotationItem[]>(() => getDefaultItems())
  const [activeTab, setActiveTab] = useState("customer")

  // Load deals when modal opens
  useEffect(() => {
    if (isOpen && !editingQuotation) {
      loadDeals().catch(err => console.error('Failed to load deals:', err))
    }
  }, [isOpen, editingQuotation])

  // Populate form when editing quotation
  useEffect(() => {
    if (editingQuotation && isOpen) {
      console.log('Populating form with editing quotation:', editingQuotation)
      
      // Map database fields to form fields
      const mappedFormData: FormData = {
        accountName: editingQuotation.customer_name || editingQuotation.accountName || "",
        contactPerson: editingQuotation.contact_person || editingQuotation.contactPerson || "",
        customerEmail: editingQuotation.customer_email || editingQuotation.customerEmail || "",
        customerPhone: editingQuotation.customer_phone || editingQuotation.customerPhone || "",
        billingAddress: editingQuotation.billing_address || editingQuotation.billingAddress || "",
        shippingAddress: editingQuotation.shipping_address || editingQuotation.shippingAddress || editingQuotation.billing_address || editingQuotation.billingAddress || "",
        quotationDate: editingQuotation.quote_date || editingQuotation.quotationDate || new Date().toISOString().split('T')[0],
        validUntil: editingQuotation.valid_until || editingQuotation.validUntil || "",
        reference: editingQuotation.reference_number || editingQuotation.reference || "",
        subject: editingQuotation.subject || "",
        notes: editingQuotation.notes || "",
        terms: editingQuotation.terms_conditions || editingQuotation.terms || "Payment: 50% advance, 50% on delivery\nDelivery: 4-6 weeks from order confirmation\nValidity: 30 days from quotation date",
        assignedTo: editingQuotation.assigned_to || editingQuotation.assignedTo || "",
        priority: editingQuotation.priority || "Medium",
        currency: editingQuotation.currency || "INR",
        taxType: editingQuotation.tax_type || editingQuotation.taxType || "GST",
        paymentTerms: editingQuotation.payment_terms || editingQuotation.paymentTerms || "50% Advance, 50% on Delivery",
        deliveryTerms: editingQuotation.delivery_terms || editingQuotation.deliveryTerms || "4-6 weeks"
      }
      
      setFormData(mappedFormData)
      
      // Map items from database with cleaned descriptions
      if (editingQuotation.line_items && Array.isArray(editingQuotation.line_items) && editingQuotation.line_items.length > 0) {
        const cleanedItems = editingQuotation.line_items.map((item: any) => {
          // Clean up description by removing "Product from deal:" prefix and account info
          let cleanDescription = item.description || "";
          if (cleanDescription.includes("Product from deal:")) {
            // Extract just the product part after the last " - "
            const parts = cleanDescription.split(" - ");
            if (parts.length > 1) {
              cleanDescription = parts[parts.length - 1]; // Get the last part (product name)
            } else {
              cleanDescription = item.product || "";
            }
          }
          
          return {
            ...item,
            description: cleanDescription
          };
        });
        setItems(cleanedItems)
      } else if (editingQuotation.items && Array.isArray(editingQuotation.items) && editingQuotation.items.length > 0) {
        const cleanedItems = editingQuotation.items.map((item: any) => {
          // Clean up description by removing "Product from deal:" prefix and account info
          let cleanDescription = item.description || "";
          if (cleanDescription.includes("Product from deal:")) {
            // Extract just the product part after the last " - "
            const parts = cleanDescription.split(" - ");
            if (parts.length > 1) {
              cleanDescription = parts[parts.length - 1]; // Get the last part (product name)
            } else {
              cleanDescription = item.product || "";
            }
          }
          
          return {
            ...item,
            description: cleanDescription
          };
        });
        setItems(cleanedItems)
      } else {
        // Create a single item from the main quotation data
        setItems([{
          id: "1",
          product: editingQuotation.products_quoted || editingQuotation.product || "",
          description: "",
          quantity: 1,
          unitPrice: editingQuotation.total_amount || editingQuotation.amount || 0,
          discount: 0,
          taxRate: 18,
          amount: editingQuotation.total_amount || editingQuotation.amount || 0
        }])
      }
      
      console.log('Form populated with data:', mappedFormData)
    }
  }, [editingQuotation, isOpen])

  const loadDeals = async () => {
    setLoadingDeals(true)
    try {
      const response = await fetch('/api/deals')
      if (response.ok) {
        const data = await response.json()
        // Filter only active deals
        const activeDeals = (data.deals || []).filter((deal: any) => 
          deal.status === 'Active' && deal.stage !== 'Closed Won' && deal.stage !== 'Closed Lost'
        )
        setDeals(activeDeals)
      }
    } catch (error) {
      console.error('Error loading deals:', error)
      toast({
        title: "Error",
        description: "Failed to load deals",
        variant: "destructive"
      })
    } finally {
      setLoadingDeals(false)
    }
  }

  // Safety check to prevent crashes
  if (!isOpen) {
    return null
  }

  // Handle input changes safely
  const handleInputChange = (field: keyof FormData, value: string) => {
    try {
      setFormData(prev => ({
        ...prev,
        [field]: value
      }))
    } catch (error) {
      console.error('Error updating form field:', field, error)
    }
  }

  // Handle deal selection and auto-populate form
  const handleDealSelection = async (dealId: string) => {
    setSelectedDealId(dealId)
    
    if (!dealId || dealId === "none") {
      // Clear form if no deal selected
      return
    }
    
    const selectedDeal = deals.find(deal => deal.id === dealId)
    if (selectedDeal) {
      // Auto-populate form with deal data
      setFormData(prev => ({
        ...prev,
        accountName: selectedDeal.account_name || prev.accountName,
        contactPerson: selectedDeal.contact_person || prev.contactPerson,
        customerEmail: selectedDeal.email || prev.customerEmail,
        customerPhone: selectedDeal.phone || prev.customerPhone,
        billingAddress: selectedDeal.city && selectedDeal.state ? 
          `${selectedDeal.city}, ${selectedDeal.state}` : prev.billingAddress,
        subject: `Quotation for ${selectedDeal.deal_name}`,
        notes: `Generated from deal: ${selectedDeal.deal_name}`
      }))
      
      // Fetch deal products and create multiple items
      try {
        const response = await fetch(`/api/deals/${dealId}/products`)
        if (response.ok) {
          const dealProducts = await response.json()
          console.log('Deal products fetched:', dealProducts)
          
          if (dealProducts && dealProducts.length > 0) {
            // Create multiple items from deal products
            const newItems = dealProducts.map((product: any, index: number) => ({
              id: `deal-${index + 1}`,
              product: product.product_name || product.product || '',
              description: `Product from deal: ${selectedDeal.deal_name}`,
              quantity: product.quantity || 1,
              unitPrice: product.price_per_unit || 0,
              discount: 0,
              taxRate: 18,
              amount: (product.quantity || 1) * (product.price_per_unit || 0) * 1.18 // Including 18% tax
            }))
            
            setItems(newItems)
            toast({
              title: "Deal Selected",
              description: `Quotation pre-filled with ${dealProducts.length} products from deal: ${selectedDeal.deal_name}`
            })
          } else {
            // Fallback to legacy single product format if no deal_products found
            if (selectedDeal.product) {
              const dealValue = selectedDeal.value || selectedDeal.budget || 0
              const quantity = selectedDeal.quantity || 1
              const unitPrice = selectedDeal.price_per_unit || (quantity > 0 ? dealValue / quantity : dealValue)
              
              setItems([{
                id: "1",
                product: selectedDeal.product,
                description: `Product/Service from deal: ${selectedDeal.deal_name}`,
                quantity: quantity,
                unitPrice: unitPrice,
                discount: 0,
                taxRate: 18,
                amount: unitPrice * quantity * 1.18 // Including 18% tax
              }])
            }
            
            toast({
              title: "Deal Selected",
              description: `Quotation pre-filled with data from deal: ${selectedDeal.deal_name}`
            })
          }
        } else {
          console.error('Failed to fetch deal products')
          // Fallback to legacy format
          if (selectedDeal.product) {
            const dealValue = selectedDeal.value || selectedDeal.budget || 0
            const quantity = selectedDeal.quantity || 1
            const unitPrice = selectedDeal.price_per_unit || (quantity > 0 ? dealValue / quantity : dealValue)
            
            setItems([{
              id: "1",
              product: selectedDeal.product,
              description: `Product/Service from deal: ${selectedDeal.deal_name}`,
              quantity: quantity,
              unitPrice: unitPrice,
              discount: 0,
              taxRate: 18,
              amount: unitPrice * quantity * 1.18 // Including 18% tax
            }])
          }
          
          toast({
            title: "Deal Selected",
            description: `Quotation pre-filled with data from deal: ${selectedDeal.deal_name}`
          })
        }
      } catch (error) {
        console.error('Error fetching deal products:', error)
        toast({
          title: "Warning",
          description: "Failed to load deal products. Please add items manually.",
          variant: "destructive"
        })
      }
    }
  }

  const addItem = () => {
    const newItem: QuotationItem = {
      id: Date.now().toString(),
      product: "",
      description: "",
      quantity: 1,
      unitPrice: 0,
      discount: 0,
      taxRate: 18,
      amount: 0
    }
    setItems(prev => [...prev, newItem])
  }

  const removeItem = (id: string) => {
    if (items.length > 1) {
      setItems(prev => prev.filter(item => item.id !== id))
    }
  }

  const updateItem = (id: string, field: keyof QuotationItem, value: string | number) => {
    setItems(prev => prev.map(item => {
      if (item.id === id) {
        const updated = { ...item, [field]: value }
        // Calculate amount when quantity, unit price, or discount changes
        if (field === 'quantity' || field === 'unitPrice' || field === 'discount') {
          const subtotal = updated.quantity * updated.unitPrice
          const discountAmount = subtotal * (updated.discount / 100)
          const taxableAmount = subtotal - discountAmount
          const taxAmount = taxableAmount * (updated.taxRate / 100)
          updated.amount = taxableAmount + taxAmount
        }
        return updated
      }
      return item
    }))
  }

  // Calculate totals safely
  const calculateTotals = () => {
    try {
      if (!items || !Array.isArray(items) || items.length === 0) {
        return { subtotal: 0, totalDiscount: 0, taxableAmount: 0, totalTax: 0, grandTotal: 0 }
      }

      const subtotal = items.reduce((sum, item) => {
        if (!item || typeof item.quantity !== 'number' || typeof item.unitPrice !== 'number') {
          return sum
        }
        return sum + (item.quantity * item.unitPrice)
      }, 0)
      
      const totalDiscount = items.reduce((sum, item) => {
        if (!item || typeof item.quantity !== 'number' || typeof item.unitPrice !== 'number' || typeof item.discount !== 'number') {
          return sum
        }
        const itemSubtotal = item.quantity * item.unitPrice
        return sum + (itemSubtotal * item.discount / 100)
      }, 0)
      
      const taxableAmount = subtotal - totalDiscount
      const totalTax = items.reduce((sum, item) => {
        if (!item || typeof item.quantity !== 'number' || typeof item.unitPrice !== 'number' || typeof item.discount !== 'number' || typeof item.taxRate !== 'number') {
          return sum
        }
        const itemSubtotal = item.quantity * item.unitPrice
        const itemDiscount = itemSubtotal * (item.discount / 100)
        const itemTaxable = itemSubtotal - itemDiscount
        return sum + (itemTaxable * item.taxRate / 100)
      }, 0)
      
      const grandTotal = taxableAmount + totalTax
      
      return { subtotal, totalDiscount, taxableAmount, totalTax, grandTotal }
    } catch (error) {
      console.error('Error calculating totals:', error)
      return { subtotal: 0, totalDiscount: 0, taxableAmount: 0, totalTax: 0, grandTotal: 0 }
    }
  }

  const handleSubmit = () => {
    try {
      const totals = calculateTotals()
      
      // Map form fields to Supabase column names
      const quotationData = {
        quote_number: editingQuotation?.quote_number || `QTN-${new Date().getFullYear()}-${String(Math.floor(Math.random() * 1000)).padStart(3, '0')}`,
        quote_date: formData.quotationDate || new Date().toISOString().split('T')[0],
        valid_until: formData.validUntil || null,
        customer_name: formData.accountName || '',
        contact_person: formData.contactPerson || '',
        customer_email: formData.customerEmail || null,
        customer_phone: formData.customerPhone || null,
        billing_address: formData.billingAddress || null,
        shipping_address: formData.shippingAddress || formData.billingAddress || null,
        products_quoted: items.length > 0 ? items.map(item => item.product).join(', ') : 'Multiple Items',
        total_amount: totals.grandTotal || 0,
        subtotal_amount: totals.subtotal || 0,
        tax_amount: totals.totalTax || 0,
        discount_amount: totals.totalDiscount || 0,
        currency: formData.currency || 'INR',
        status: editingQuotation?.status || 'draft',
        assigned_to: formData.assignedTo || null,
        priority: formData.priority || 'Medium',
        reference_number: formData.reference || null,
        subject: formData.subject || null,
        notes: formData.notes || null,
        terms_conditions: formData.terms || null,
        payment_terms: formData.paymentTerms || null,
        delivery_terms: formData.deliveryTerms || null,
        tax_type: formData.taxType || 'GST',
        // Store detailed items as JSON for better data structure
        line_items: items || [],
        // Company ID for filtering
        company_id: 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      }
      
      console.log("Quotation Data (mapped to Supabase):", quotationData)
      
      if (onSave) {
        onSave(quotationData)
      } else {
        onClose()
      }
    } catch (error) {
      console.error('Error submitting quotation:', error)
      toast({
        title: "Error",
        description: "Failed to save quotation. Please try again.",
        variant: "destructive"
      })
    }
  }

  const totals = calculateTotals()

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-5xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center space-x-2 text-xl">
            <FileText className="w-6 h-6 text-blue-600" />
            <span>{editingQuotation && !isCreateMode ? 'Edit Quotation' : 'Create New Quotation'}</span>
          </DialogTitle>
          <DialogDescription>
            {editingQuotation && !isCreateMode ? 'Update the quotation details' : 'Create a professional quotation for your customer'}
          </DialogDescription>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="customer" className="flex items-center space-x-2">
              <User className="w-4 h-4" />
              <span>Customer</span>
            </TabsTrigger>
            <TabsTrigger value="items" className="flex items-center space-x-2">
              <Calculator className="w-4 h-4" />
              <span>Items</span>
            </TabsTrigger>
            <TabsTrigger value="settings" className="flex items-center space-x-2">
              <Settings className="w-4 h-4" />
              <span>Settings</span>
            </TabsTrigger>
          </TabsList>

          <TabsContent value="customer" className="space-y-4">
            {/* Deal Selection Card */}
            {(!editingQuotation || isCreateMode) && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <Handshake className="w-5 h-5 text-green-600" />
                    <span>Create from Deal (Optional)</span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div>
                      <Label htmlFor="dealSelection">Select a Deal to auto-populate customer information</Label>
                      <Select value={selectedDealId} onValueChange={handleDealSelection} disabled={loadingDeals}>
                        <SelectTrigger>
                          <SelectValue placeholder={loadingDeals ? "Loading deals..." : "Select a deal (optional)"} />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="none">Create new quotation (no deal)</SelectItem>
                          {deals.map((deal) => (
                            <SelectItem key={deal.id} value={deal.id}>
                              {deal.deal_name} - {deal.account_name} (₹{deal.deal_value?.toLocaleString()})
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    {selectedDealId && selectedDealId !== "none" && (
                      <div className="p-3 bg-green-50 border border-green-200 rounded-md">
                        <p className="text-sm text-green-800">
                          ✓ Customer information will be auto-populated from the selected deal
                        </p>
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>
            )}

            <Card>
              <CardHeader>
                <CardTitle>Customer Information</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="accountName">Account Name *</Label>
                    <Input
                      id="accountName"
                      value={formData.accountName}
                      onChange={(e) => handleInputChange("accountName", e.target.value)}
                      placeholder="Company name"
                    />
                  </div>
                  <div>
                    <Label htmlFor="contactPerson">Contact Person *</Label>
                    <Input
                      id="contactPerson"
                      value={formData.contactPerson}
                      onChange={(e) => handleInputChange("contactPerson", e.target.value)}
                      placeholder="Primary contact"
                    />
                  </div>
                  <div>
                    <Label htmlFor="customerEmail">Email</Label>
                    <Input
                      id="customerEmail"
                      type="email"
                      value={formData.customerEmail}
                      onChange={(e) => handleInputChange("customerEmail", e.target.value)}
                      placeholder="email@example.com"
                    />
                  </div>
                  <div>
                    <Label htmlFor="customerPhone">Phone</Label>
                    <Input
                      id="customerPhone"
                      value={formData.customerPhone}
                      onChange={(e) => handleInputChange("customerPhone", e.target.value)}
                      placeholder="+91 98765 43210"
                    />
                  </div>
                </div>
                
                <div>
                  <Label htmlFor="billingAddress">Billing Address</Label>
                  <Textarea
                    id="billingAddress"
                    value={formData.billingAddress}
                    onChange={(e) => handleInputChange("billingAddress", e.target.value)}
                    placeholder="Complete billing address"
                    rows={3}
                  />
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="items" className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Items & Pricing</span>
                  <Button 
                    onClick={addItem} 
                    size="sm" 
                    className="bg-green-600 hover:bg-green-700"
                  >
                    <Plus className="w-4 h-4 mr-2" />
                    Add Item
                  </Button>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4 overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-40">Product</TableHead>
                        <TableHead className="w-44">Description</TableHead>
                        <TableHead className="w-14">Qty</TableHead>
                        <TableHead className="w-20">Unit Price</TableHead>
                        <TableHead className="w-16">Disc %</TableHead>
                        <TableHead className="w-16">Tax %</TableHead>
                        <TableHead className="w-20">Amount</TableHead>
                        <TableHead className="w-10"></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {items.map((item) => (
                        <TableRow key={item.id}>
                          <TableCell>
                            <Input
                              value={item.product}
                              onChange={(e) => updateItem(item.id, 'product', e.target.value)}
                              placeholder="Product name"
                              className="w-36"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={item.description}
                              onChange={(e) => updateItem(item.id, 'description', e.target.value)}
                              placeholder="Description"
                              className="w-40"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              value={item.quantity}
                              onChange={(e) => updateItem(item.id, 'quantity', Number(e.target.value))}
                              min="1"
                              className="w-12"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              value={item.unitPrice}
                              onChange={(e) => updateItem(item.id, 'unitPrice', Number(e.target.value))}
                              min="0"
                              step="0.01"
                              className="w-16"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              value={item.discount}
                              onChange={(e) => updateItem(item.id, 'discount', Number(e.target.value))}
                              min="0"
                              max="100"
                              step="0.01"
                              className="w-14"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              type="number"
                              value={item.taxRate}
                              onChange={(e) => updateItem(item.id, 'taxRate', Number(e.target.value))}
                              min="0"
                              max="100"
                              step="0.01"
                              className="w-14"
                            />
                          </TableCell>
                          <TableCell>
                            <div className="text-right font-medium text-sm">
                              ₹{item.amount.toLocaleString('en-IN', { maximumFractionDigits: 0 })}
                            </div>
                          </TableCell>
                          <TableCell>
                            {items.length > 1 && (
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => removeItem(item.id)}
                                className="text-red-600 hover:text-red-700 p-1"
                              >
                                <Minus className="w-3 h-3" />
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                  
                  <div className="border-t pt-4">
                    <div className="space-y-2 text-right">
                      <div className="flex justify-between">
                        <span>Subtotal:</span>
                        <span>₹{totals.subtotal.toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Total Discount:</span>
                        <span className="text-red-600">-₹{totals.totalDiscount.toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Taxable Amount:</span>
                        <span>₹{totals.taxableAmount.toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Total Tax:</span>
                        <span>₹{totals.totalTax.toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span>
                      </div>
                      <div className="flex justify-between text-lg font-semibold border-t pt-2">
                        <span>Grand Total:</span>
                        <span className="text-blue-600">₹{totals.grandTotal.toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span>
                      </div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="settings" className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle>Quotation Details</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="quotationDate">Quotation Date *</Label>
                    <Input
                      id="quotationDate"
                      type="date"
                      value={formData.quotationDate}
                      onChange={(e) => handleInputChange("quotationDate", e.target.value)}
                    />
                  </div>
                  <div>
                    <Label htmlFor="validUntil">Valid Until</Label>
                    <Input
                      id="validUntil"
                      type="date"
                      value={formData.validUntil}
                      onChange={(e) => handleInputChange("validUntil", e.target.value)}
                    />
                  </div>
                  <div>
                    <Label htmlFor="reference">Reference</Label>
                    <Input
                      id="reference"
                      value={formData.reference}
                      onChange={(e) => handleInputChange("reference", e.target.value)}
                      placeholder="Reference number"
                    />
                  </div>
                  <div>
                    <Label htmlFor="subject">Subject</Label>
                    <Input
                      id="subject"
                      value={formData.subject}
                      onChange={(e) => handleInputChange("subject", e.target.value)}
                      placeholder="Quotation subject"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Business Settings</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <Label htmlFor="assignedTo">Assigned To</Label>
                    <Select value={formData.assignedTo} onValueChange={(value) => handleInputChange("assignedTo", value)}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select assignee" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="Hari Kumar K">Hari Kumar K</SelectItem>
                        <SelectItem value="Prashanth Sandilya">Prashanth Sandilya</SelectItem>
                        <SelectItem value="Vijay Muppala">Vijay Muppala</SelectItem>
                        <SelectItem value="Pauline">Pauline</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="priority">Priority</Label>
                    <Select value={formData.priority} onValueChange={(value) => handleInputChange("priority", value)}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select priority" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="Low">Low</SelectItem>
                        <SelectItem value="Medium">Medium</SelectItem>
                        <SelectItem value="High">High</SelectItem>
                        <SelectItem value="Urgent">Urgent</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="currency">Currency</Label>
                    <Select value={formData.currency} onValueChange={(value) => handleInputChange("currency", value)}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select currency" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="INR">INR</SelectItem>
                        <SelectItem value="USD">USD</SelectItem>
                        <SelectItem value="EUR">EUR</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="paymentTerms">Payment Terms</Label>
                    <Select value={formData.paymentTerms} onValueChange={(value) => handleInputChange("paymentTerms", value)}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select payment terms" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="Immediate Payment">Immediate Payment</SelectItem>
                        <SelectItem value="Net 15">Net 15</SelectItem>
                        <SelectItem value="Net 30">Net 30</SelectItem>
                        <SelectItem value="50% Advance, 50% on Delivery">50% Advance, 50% on Delivery</SelectItem>
                        <SelectItem value="25% Advance, 75% on Delivery">25% Advance, 75% on Delivery</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="deliveryTerms">Delivery Terms</Label>
                    <Input
                      id="deliveryTerms"
                      value={formData.deliveryTerms}
                      onChange={(e) => handleInputChange("deliveryTerms", e.target.value)}
                      placeholder="e.g., 4-6 weeks"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Additional Information</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label htmlFor="notes">Notes</Label>
                  <Textarea
                    id="notes"
                    value={formData.notes}
                    onChange={(e) => handleInputChange("notes", e.target.value)}
                    placeholder="Additional notes or instructions"
                    rows={3}
                  />
                </div>
                
                <div>
                  <Label htmlFor="terms">Terms & Conditions</Label>
                  <Textarea
                    id="terms"
                    value={formData.terms}
                    onChange={(e) => handleInputChange("terms", e.target.value)}
                    placeholder="Terms and conditions"
                    rows={4}
                  />
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            <X className="w-4 h-4 mr-2" />
            Cancel
          </Button>
          <Button onClick={handleSubmit} className="bg-blue-600 hover:bg-blue-700">
            <Save className="w-4 h-4 mr-2" />
            {editingQuotation && !isCreateMode ? 'Update Quotation' : 'Create Quotation'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}