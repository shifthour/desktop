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

export function AddQuotationModal({ isOpen, onClose, onSave, editingQuotation }: AddQuotationModalProps) {
  console.log('AddQuotationModal render:', { isOpen, hasEditingQuotation: !!editingQuotation })
  
  const { toast } = useToast()
  const [selectedDealId, setSelectedDealId] = useState<string>("")
  const [deals, setDeals] = useState<any[]>([])
  const [loadingDeals, setLoadingDeals] = useState(false)
  const [accounts, setAccounts] = useState<string[]>([])
  const [products, setProducts] = useState<string[]>([])

  // Debug logging - moved to useEffect to avoid render issues
  useEffect(() => {
    if (isOpen) {
      console.log("AddQuotationModal opened with:", {
        isOpen,
        editingQuotation: editingQuotation ? {
          id: editingQuotation.id,
          accountName: editingQuotation.accountName,
          date: editingQuotation.date,
          quotationDate: editingQuotation.quotationDate,
          keys: Object.keys(editingQuotation)
        } : null
      })
    }
  }, [isOpen, editingQuotation])
  
  // Helper function to convert DD/MM/YYYY to YYYY-MM-DD
  const convertDateToISOFormat = (dateStr: string | undefined | null): string => {
    try {
      if (!dateStr || typeof dateStr !== 'string') {
        return new Date().toISOString().split('T')[0]
      }
      
      // Check if it's already in YYYY-MM-DD format
      if (dateStr.includes('-') && dateStr.length === 10) {
        // Validate the date format
        const testDate = new Date(dateStr)
        if (!isNaN(testDate.getTime())) {
          return dateStr
        }
      }
      
      // Convert DD/MM/YYYY to YYYY-MM-DD
      if (dateStr.includes('/')) {
        const parts = dateStr.split('/')
        if (parts.length === 3) {
          const [day, month, year] = parts
          const isoDate = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`
          // Validate the converted date
          const testDate = new Date(isoDate)
          if (!isNaN(testDate.getTime())) {
            return isoDate
          }
        }
      }
    } catch (error) {
      console.error('Error converting date:', error)
    }
    
    return new Date().toISOString().split('T')[0]
  }

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

  const [formData, setFormData] = useState<FormData>(() => getDefaultFormData())

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

  const [items, setItems] = useState<QuotationItem[]>(() => getDefaultItems())

  const [activeTab, setActiveTab] = useState("customer")

  // Load deals, accounts and products when modal opens
  useEffect(() => {
    if (isOpen) {
      try {
        if (!editingQuotation) {
          loadDeals().catch(err => console.error('Failed to load deals:', err))
        }
        loadAccountsAndProducts().catch(err => console.error('Failed to load accounts/products:', err))
      } catch (error) {
        console.error('Error in modal open effect:', error)
      }
    }
  }, [isOpen, editingQuotation])

  // Reset form data when editingQuotation changes
  useEffect(() => {
    if (editingQuotation) {
      try {
        const defaultData = getDefaultFormData()
        setFormData({
          ...defaultData,
          accountName: editingQuotation?.accountName || "",
          contactPerson: editingQuotation?.contactPerson || editingQuotation?.contactName || "",
          customerEmail: editingQuotation?.customerEmail || "",
          customerPhone: editingQuotation?.customerPhone || "",
          billingAddress: editingQuotation?.billingAddress || "",
          shippingAddress: editingQuotation?.shippingAddress || "",
          quotationDate: convertDateToISOFormat(editingQuotation?.quotationDate || editingQuotation?.date),
          validUntil: convertDateToISOFormat(editingQuotation?.validUntil),
          reference: editingQuotation?.reference || "",
          subject: editingQuotation?.subject || "",
          notes: editingQuotation?.notes || "",
          terms: editingQuotation?.terms || defaultData.terms,
          assignedTo: editingQuotation?.assignedTo || "",
          priority: editingQuotation?.priority || "Medium",
          currency: editingQuotation?.currency || "INR",
          taxType: editingQuotation?.taxType || "GST",
          paymentTerms: editingQuotation?.paymentTerms || "50% Advance, 50% on Delivery",
          deliveryTerms: editingQuotation?.deliveryTerms || "4-6 weeks"
        })
        
        // Reset items data with better error handling
        if (editingQuotation?.items && Array.isArray(editingQuotation.items) && editingQuotation.items.length > 0) {
          const safeItems = editingQuotation.items.map((item: any, index: number) => ({
            id: item.id || `item-${index}`,
            product: String(item.product || ""),
            description: String(item.description || ""),
            quantity: Number(item.quantity) || 1,
            unitPrice: Number(item.unitPrice) || 0,
            discount: Number(item.discount) || 0,
            taxRate: Number(item.taxRate) || 18,
            amount: Number(item.amount) || 0
          }))
          setItems(safeItems)
        } else {
          setItems(getDefaultItems())
        }
      } catch (error) {
        console.error('Error setting up edit form:', error)
        // Fallback to default state
        setFormData(getDefaultFormData())
        setItems(getDefaultItems())
      }
    } else {
      // Reset to defaults when not editing
      setFormData(getDefaultFormData())
      setItems(getDefaultItems())
    }
  }, [editingQuotation])

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

  const loadAccountsAndProducts = async () => {
    try {
      // Load accounts
      const accountsResponse = await fetch('/api/accounts')
      if (accountsResponse.ok) {
        const accountsData = await accountsResponse.json()
        const accountNames = (accountsData.accounts || []).map((acc: any) => acc.name).filter(Boolean)
        setAccounts([...new Set(accountNames)]) // Remove duplicates
      }
    } catch (error) {
      console.error('Error loading accounts:', error)
      setAccounts([])
    }
    
    try {
      // Load products with timeout
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 5000) // 5 second timeout
      
      const productsResponse = await fetch('/api/products', { signal: controller.signal })
      clearTimeout(timeoutId)
      
      if (productsResponse.ok) {
        const productsData = await productsResponse.json()
        if (Array.isArray(productsData)) {
          const productNames = productsData.map((prod: any) => prod.name || prod.product_name).filter(Boolean)
          setProducts([...new Set(productNames)]) // Remove duplicates
        } else {
          console.warn('Products API returned non-array data:', productsData)
          setProducts([])
        }
      } else {
        console.warn('Products API returned non-ok status:', productsResponse.status)
        setProducts([])
      }
    } catch (error: any) {
      if (error.name === 'AbortError') {
        console.error('Products loading timed out')
      } else {
        console.error('Error loading products:', error)
      }
      setProducts([])
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
      // Try to fetch complete account details for better address information
      let accountDetails = null
      if (selectedDeal.account_name) {
        try {
          const accountsResponse = await fetch('/api/accounts')
          if (accountsResponse.ok) {
            const accountsData = await accountsResponse.json()
            accountDetails = (accountsData.accounts || []).find((acc: any) => 
              acc.name === selectedDeal.account_name
            )
          }
        } catch (error) {
          console.error('Error fetching account details:', error)
        }
      }
      
      // Build complete address from account or deal data
      let billingAddress = ""
      if (accountDetails) {
        const addressParts = []
        if (accountDetails.address) addressParts.push(accountDetails.address)
        if (accountDetails.city) addressParts.push(accountDetails.city)
        if (accountDetails.state) addressParts.push(accountDetails.state)
        if (accountDetails.country) addressParts.push(accountDetails.country)
        if (accountDetails.postal_code) addressParts.push(`PIN: ${accountDetails.postal_code}`)
        billingAddress = addressParts.join(", ")
      } else if (selectedDeal.city || selectedDeal.state) {
        billingAddress = [selectedDeal.city, selectedDeal.state].filter(Boolean).join(", ")
      }
      
      // Auto-populate form with deal data
      setFormData(prev => ({
        ...prev,
        accountName: selectedDeal.account_name || prev.accountName,
        contactPerson: selectedDeal.contact_person || prev.contactPerson,
        customerEmail: selectedDeal.email || accountDetails?.email || prev.customerEmail,
        customerPhone: selectedDeal.phone || accountDetails?.phone || prev.customerPhone,
        billingAddress: billingAddress || prev.billingAddress,
        shippingAddress: billingAddress || prev.shippingAddress,
        reference: `Deal: ${selectedDeal.deal_name}`,
        subject: `Quotation for ${selectedDeal.deal_name}`,
        assignedTo: selectedDeal.assigned_to || prev.assignedTo,
        validUntil: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0] // 30 days from today
      }))
      
      // Auto-populate first item with deal product
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
        title: "Deal data loaded",
        description: `Quotation pre-filled with data from deal: ${selectedDeal.deal_name}`
      })
    }
  }

  const handleInputChange = (field: keyof FormData, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }))
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

  const calculateTotals = () => {
    try {
      if (!items || !Array.isArray(items) || items.length === 0) {
        return { subtotal: 0, totalDiscount: 0, taxableAmount: 0, totalTax: 0, grandTotal: 0 }
      }

      const subtotal = items.reduce((sum, item) => {
        if (!item || typeof item.quantity !== 'number' || typeof item.unitPrice !== 'number') {
          return sum
        }
        const itemSubtotal = item.quantity * item.unitPrice
        return sum + itemSubtotal
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
    const totals = calculateTotals()
    const quotationData = {
      ...formData,
      items,
      totals,
      quotationId: editingQuotation?.quotationId || `QTN-${new Date().getFullYear()}-${String(Math.floor(Math.random() * 1000)).padStart(3, '0')}`,
      status: editingQuotation?.status || "Draft",
      date: editingQuotation?.date || new Date().toLocaleDateString('en-GB'),
      amount: `₹${totals.grandTotal.toLocaleString()}`,
      revision: editingQuotation?.revision || "Rev-0",
      // Include product field for display in list
      product: items.length > 0 && items[0].product ? items[0].product : "Multiple Items",
      contactName: formData.contactPerson
    }
    
    console.log("Quotation Data:", quotationData)
    
    if (onSave) {
      onSave(quotationData)
    } else {
      onClose()
    }
  }

  // Options
  const assignees = ["Hari Kumar K", "Prashanth Sandilya", "Vijay Muppala", "Pauline"]
  const priorities = ["Low", "Medium", "High", "Urgent"]
  const currencies = ["INR", "USD", "EUR"]
  const paymentTermsOptions = ["Immediate Payment", "Net 15", "Net 30", "50% Advance, 50% on Delivery", "25% Advance, 75% on Delivery"]

  const totals = calculateTotals()

  // Safety check to prevent crashes
  if (!isOpen) {
    return null
  }

  // Minimal test return to isolate the issue
  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Test Modal</DialogTitle>
        </DialogHeader>
        <div className="p-4">
          <p>This is a test modal to check if the basic dialog works.</p>
          <Button onClick={onClose}>Close</Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
