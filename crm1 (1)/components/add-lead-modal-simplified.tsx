"use client"

import { useState, useEffect, useRef } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { ScrollArea } from "@/components/ui/scroll-area"
import { useToast } from "@/hooks/use-toast"
import { Save, X, Search, ChevronDown, Loader2 } from "lucide-react"
import { Textarea } from "@/components/ui/textarea"
import { cn } from "@/lib/utils"
import { MultipleProductSelector } from "@/components/multiple-product-selector"

interface AddLeadModalProps {
  isOpen: boolean
  onClose: () => void
  onSave?: (leadData: any) => Promise<void>
  editingLead?: any
}

interface Account {
  id: string
  accountName: string
  contactName: string
  city: string
  state: string
  country: string
  phone: string
  email: string
  address: string
}

interface Contact {
  id: string
  contactName: string
  accountName: string
  department: string
  phone: string
  email: string
  address: string
}

interface Product {
  id: string
  productName: string
  category: string
  price: number
}

interface SelectedProduct {
  id: string
  productName: string
  category: string
  price: number
  quantity: number
  totalAmount: number
}

interface User {
  id: string
  full_name: string
  email: string
  is_admin: boolean
}

const leadSources = [
  "Website",
  "Referral",
  "Social Media",
  "Email Campaign",
  "Trade Show",
  "Cold Call",
  "Partner",
  "Direct Mail",
  "Conference",
  "Other"
]

const leadStatuses = [
  "New",
  "Contacted",
  "Qualified",
  "Disqualified"
]

export function AddLeadModalSimplified({ isOpen, onClose, onSave, editingLead }: AddLeadModalProps) {
  const { toast } = useToast()
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [accounts, setAccounts] = useState<Account[]>([])
  const [contacts, setContacts] = useState<Contact[]>([])
  const [users, setUsers] = useState<User[]>([])
  
  // Search states for dropdowns
  const [accountSearchOpen, setAccountSearchOpen] = useState(false)
  const [contactSearchOpen, setContactSearchOpen] = useState(false)
  const [accountSearch, setAccountSearch] = useState("")
  const [contactSearch, setContactSearch] = useState("")
  
  // Keyboard navigation states
  const [selectedAccountIndex, setSelectedAccountIndex] = useState(-1)
  const [selectedContactIndex, setSelectedContactIndex] = useState(-1)
  
  // Refs for scrolling
  const accountListRef = useRef<HTMLDivElement>(null)
  const contactListRef = useRef<HTMLDivElement>(null)
  
  // Form data
  const [formData, setFormData] = useState({
    accountId: "",
    accountName: "",
    contactId: "",
    contactName: "",
    department: "",
    phone: "",
    email: "",
    leadSource: "",
    leadStatus: "New",
    expectedClosingDate: "",
    nextFollowupDate: "",
    city: "",
    state: "",
    country: "",
    address: "",
    assignedTo: "",
    notes: ""
  })

  // Separate state for selected products and budget
  const [selectedProducts, setSelectedProducts] = useState<SelectedProduct[]>([])
  const [totalBudget, setTotalBudget] = useState(0)

  // Load data on mount
  useEffect(() => {
    loadAccounts()
    loadContacts()
    loadUsers()
  }, [])

  // Populate form when editing
  useEffect(() => {
    if (editingLead) {
      setFormData({
        accountId: editingLead.accountId || "",
        accountName: editingLead.accountName || editingLead.leadName || "",
        contactId: editingLead.contactId || "",
        contactName: editingLead.contactName || "",
        department: editingLead.department || "",
        phone: editingLead.phone || editingLead.contactNo || "",
        email: editingLead.email || "",
        leadSource: editingLead.leadSource || "",
        leadStatus: editingLead.leadStatus || editingLead.salesStage || "New",
        expectedClosingDate: editingLead.expectedClosingDate || "",
        nextFollowupDate: editingLead.nextFollowupDate || "",
        city: editingLead.city || "",
        state: editingLead.state || "",
        country: editingLead.country || "",
        address: editingLead.address || "",
        assignedTo: editingLead.assignedTo || "",
        notes: editingLead.notes || ""
      })
      
      // For editing, populate selected products from leadProducts (multiple products support)
      if (editingLead.leadProducts && editingLead.leadProducts.length > 0) {
        console.log('Editing lead with multiple products:', editingLead.leadProducts)
        const existingProducts: SelectedProduct[] = editingLead.leadProducts.map((product: any) => ({
          id: product.product_id || `temp-${Date.now()}-${Math.random()}`,
          productName: product.product_name,
          category: "General", // We might not have category info
          price: product.price_per_unit || 0,
          quantity: product.quantity || 1,
          totalAmount: product.total_amount || ((product.price_per_unit || 0) * (product.quantity || 1))
        }))
        setSelectedProducts(existingProducts)
        setTotalBudget(existingProducts.reduce((sum, p) => sum + p.totalAmount, 0))
      } else if (editingLead.productId && editingLead.productName) {
        // Fallback for legacy single product format
        console.log('Editing lead with legacy single product')
        const existingProduct: SelectedProduct = {
          id: editingLead.productId,
          productName: editingLead.productName || editingLead.product,
          category: "General",
          price: editingLead.pricePerUnit || 0,
          quantity: editingLead.quantity || 1,
          totalAmount: (editingLead.pricePerUnit || 0) * (editingLead.quantity || 1)
        }
        setSelectedProducts([existingProduct])
        setTotalBudget(existingProduct.totalAmount)
      } else {
        console.log('No products found for editing lead')
        setSelectedProducts([])
        setTotalBudget(0)
      }
    } else {
      // Reset form for new lead
      setFormData({
        accountId: "",
        accountName: "",
        contactId: "",
        contactName: "",
        department: "",
        phone: "",
        email: "",
        leadSource: "",
        leadStatus: "New",
        expectedClosingDate: "",
        nextFollowupDate: "",
        city: "",
        state: "",
        country: "",
        address: "",
        assignedTo: "",
        notes: ""
      })
      setSelectedProducts([])
      setTotalBudget(0)
    }
  }, [editingLead, isOpen])


  const loadAccounts = async () => {
    try {
      const companyId = localStorage.getItem('currentCompanyId') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      const response = await fetch(`/api/accounts?companyId=${companyId}`)
      
      if (response.ok) {
        const data = await response.json()
        const accountsArray = data.accounts || data
        const formattedAccounts = accountsArray.map((account: any) => ({
          id: account.id,
          accountName: account.account_name,
          contactName: account.contact_name || '',
          city: account.billing_city || account.city || '',
          state: account.billing_state || account.state || '',
          country: account.billing_country || account.country || '',
          phone: account.phone || '',
          email: account.email || '',
          address: account.billing_street || account.address || ''
        }))
        setAccounts(formattedAccounts)
      }
    } catch (error) {
      console.error('Error loading accounts:', error)
    }
  }

  const loadContacts = async () => {
    try {
      const companyId = localStorage.getItem('currentCompanyId') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      const response = await fetch(`/api/contacts?companyId=${companyId}`)
      
      if (response.ok) {
        const data = await response.json()
        console.log('Raw contacts data:', data)
        
        const formattedContacts = data.contacts?.map((contact: any) => {
          console.log('Processing contact:', contact)
          return {
            id: contact.id,
            contactName: contact.contactName,
            accountName: contact.accountName || '',
            department: contact.department || '',
            phone: contact.phone || '',
            email: contact.email || '',
            address: contact.address || ''
          }
        }) || []
        
        console.log('Formatted contacts:', formattedContacts)
        setContacts(formattedContacts)
      }
    } catch (error) {
      console.error('Error loading contacts:', error)
    }
  }


  const loadUsers = async () => {
    try {
      const companyId = localStorage.getItem('currentCompanyId') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      const response = await fetch(`/api/users?companyId=${companyId}`)
      
      if (response.ok) {
        const data = await response.json()
        setUsers(data)
      }
    } catch (error) {
      console.error('Error loading users:', error)
    }
  }

  const handleAccountSelect = (account: Account) => {
    // Update form with account details
    setFormData(prev => ({
      ...prev,
      accountId: account.id,
      accountName: account.accountName,
      phone: account.phone,
      email: account.email,
      city: account.city,
      state: account.state,
      country: account.country,
      address: account.address
    }))
    
    // Check if there are sub-contacts for this account
    const relatedContacts = contacts.filter(c => c.accountName === account.accountName)
    console.log('Account selected:', account.accountName)
    console.log('All contacts:', contacts)
    console.log('Related contacts found:', relatedContacts)
    
    if (relatedContacts.length === 0) {
      // No sub-contacts exist, auto-populate contact name from account's contact_name field
      console.log('No contacts found, using contact name from account')
      setFormData(prev => ({
        ...prev,
        contactId: "",
        contactName: account.contactName || account.accountName,  // Use contact_name from account, fallback to account name
        department: "None"  // Set department as "None" when no contacts exist
      }))
    } else if (relatedContacts.length === 1) {
      // Only one contact exists, auto-select it
      const relatedContact = relatedContacts[0]
      console.log('One contact found, auto-selecting:', relatedContact)
      setFormData(prev => ({
        ...prev,
        contactId: relatedContact.id,
        contactName: relatedContact.contactName,
        department: relatedContact.department || ""  // Set department from the contact
      }))
    } else {
      // Multiple contacts exist, clear contact selection so user can choose
      console.log('Multiple contacts found, clearing selection')
      setFormData(prev => ({
        ...prev,
        contactId: "",
        contactName: "",
        department: ""  // Clear department when user needs to select
      }))
    }
    
    setAccountSearchOpen(false)
    setSelectedAccountIndex(-1)
  }

  // Auto-scroll to selected item
  const scrollToSelectedAccount = (index: number) => {
    if (accountListRef.current) {
      const listElement = accountListRef.current
      const itemElements = listElement.children
      if (itemElements[index]) {
        const itemElement = itemElements[index] as HTMLElement
        const containerTop = listElement.scrollTop
        const containerBottom = containerTop + listElement.clientHeight
        const itemTop = itemElement.offsetTop
        const itemBottom = itemTop + itemElement.offsetHeight

        if (itemTop < containerTop) {
          listElement.scrollTop = itemTop
        } else if (itemBottom > containerBottom) {
          listElement.scrollTop = itemBottom - listElement.clientHeight
        }
      }
    }
  }

  // Keyboard navigation handler for account dropdown with arrow keys
  const handleAccountKeyDown = (e: React.KeyboardEvent, filteredAccounts: Account[]) => {
    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault()
        const nextIndex = selectedAccountIndex < filteredAccounts.length - 1 ? selectedAccountIndex + 1 : 0
        setSelectedAccountIndex(nextIndex)
        scrollToSelectedAccount(nextIndex)
        break
      case 'ArrowUp':
        e.preventDefault()
        const prevIndex = selectedAccountIndex > 0 ? selectedAccountIndex - 1 : filteredAccounts.length - 1
        setSelectedAccountIndex(prevIndex)
        scrollToSelectedAccount(prevIndex)
        break
      case 'Enter':
        e.preventDefault()
        if (selectedAccountIndex >= 0 && selectedAccountIndex < filteredAccounts.length) {
          handleAccountSelect(filteredAccounts[selectedAccountIndex])
        }
        break
      case 'Escape':
        e.preventDefault()
        setAccountSearchOpen(false)
        setSelectedAccountIndex(-1)
        break
    }
  }

  const handleContactSelect = (contact: Contact) => {
    // Update form with contact details and auto-populate account
    const relatedAccount = accounts.find(a => a.accountName === contact.accountName)
    
    setFormData(prev => ({
      ...prev,
      contactId: contact.id,
      contactName: contact.contactName,
      department: contact.department || "",
      phone: contact.phone,
      email: contact.email,
      // Auto-populate account details when contact is selected
      accountId: relatedAccount?.id || "",
      accountName: contact.accountName,
      city: relatedAccount?.city || "",
      state: relatedAccount?.state || "",
      country: relatedAccount?.country || "",
      address: contact.address || relatedAccount?.address || ""
    }))
    
    setContactSearchOpen(false)
  }


  const handleSubmit = async () => {
    console.log('=== FORM SUBMISSION STARTED ===')
    console.log('Form data:', formData)
    console.log('Selected products:', selectedProducts)
    console.log('Total budget:', totalBudget)
    
    // Validate required fields
    if (!formData.accountName) {
      console.log('Validation failed: Missing account name')
      toast({
        title: "Validation Error",
        description: "Please select an account",
        variant: "destructive"
      })
      return
    }

    if (!formData.leadSource) {
      console.log('Validation failed: Missing lead source')
      toast({
        title: "Validation Error",
        description: "Please select a lead source",
        variant: "destructive"
      })
      return
    }

    if (!formData.assignedTo) {
      console.log('Validation failed: Missing assigned to')
      toast({
        title: "Validation Error",
        description: "Please select who this lead is assigned to",
        variant: "destructive"
      })
      return
    }

    if (selectedProducts.length === 0) {
      console.log('Validation failed: No products selected')
      toast({
        title: "Validation Error",
        description: "Please select at least one product",
        variant: "destructive"
      })
      return
    }

    console.log('Validation passed, setting isSubmitting=true')
    setIsSubmitting(true)

    try {
      // Prepare lead data with multiple products
      const leadData = {
        account_id: formData.accountId,
        account_name: formData.accountName,
        contact_id: formData.contactId,
        contact_name: formData.contactName,
        department: formData.department,
        phone: formData.phone,
        email: formData.email,
        lead_source: formData.leadSource,
        lead_status: formData.leadStatus,
        assigned_to: formData.assignedTo || localStorage.getItem('userName') || 'Sales Team',
        budget: totalBudget,
        expected_closing_date: formData.expectedClosingDate || null,
        next_followup_date: formData.nextFollowupDate || null,
        city: formData.city || null,
        state: formData.state || null,
        country: formData.country || null,
        address: formData.address || null,
        notes: formData.notes || null,
        // Include selected products for the new API
        selected_products: selectedProducts.map(product => ({
          product_id: product.id,
          product_name: product.productName,
          quantity: product.quantity,
          price_per_unit: product.price,
          total_amount: product.totalAmount
        })),
        // For backward compatibility, include primary product info
        product_id: selectedProducts[0]?.id || null,
        product_name: selectedProducts[0]?.productName || null,
        quantity: selectedProducts.reduce((sum, p) => sum + p.quantity, 0),
        price_per_unit: selectedProducts[0]?.price || null
      }

      // Call the onSave callback if provided
      if (onSave) {
        await onSave(leadData)
      }

      // Close modal - parent will handle success message
      onClose()
    } catch (error) {
      console.error('Error saving lead:', error)
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : 'Error saving lead. Please try again.',
        variant: "destructive"
      })
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{editingLead ? 'Edit Lead' : 'Add New Lead'}</DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          {/* Account Selection with Search */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="account">Account Name *</Label>
              <Popover open={accountSearchOpen} onOpenChange={(open) => {
                setAccountSearchOpen(open)
                if (open) {
                  setSelectedAccountIndex(-1)
                }
              }}>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    role="combobox"
                    aria-expanded={accountSearchOpen}
                    className="w-full justify-between text-left font-normal"
                  >
                    {formData.accountName || "Select account..."}
                    <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-[400px] p-2" align="start">
                  <div className="space-y-2">
                    <Input
                      placeholder="Search accounts..."
                      value={accountSearch}
                      onChange={(e) => {
                        setAccountSearch(e.target.value)
                        setSelectedAccountIndex(-1)
                      }}
                      onKeyDown={(e) => {
                        const filteredAccounts = accounts.filter(account => {
                          const searchLower = accountSearch.toLowerCase()
                          const displayName = `${account.accountName} ${account.city || ''}`.toLowerCase()
                          return displayName.includes(searchLower)
                        })
                        handleAccountKeyDown(e, filteredAccounts)
                      }}
                      className="w-full"
                      autoFocus
                    />
                    <ScrollArea 
                      className="h-[200px] border rounded-md"
                      onWheel={(e) => {
                        // Allow the ScrollArea to handle wheel events properly
                        e.stopPropagation()
                      }}
                      onKeyDown={(e) => {
                        const filteredAccounts = accounts.filter(account => {
                          const searchLower = accountSearch.toLowerCase()
                          const displayName = `${account.accountName} ${account.city || ''}`.toLowerCase()
                          return displayName.includes(searchLower)
                        })
                        handleAccountKeyDown(e, filteredAccounts)
                      }}
                      tabIndex={0}
                    >
                      <div ref={accountListRef}>
                      {(() => {
                        const filteredAccounts = accounts.filter(account => {
                          const searchLower = accountSearch.toLowerCase()
                          const displayName = `${account.accountName} ${account.city || ''}`.toLowerCase()
                          return displayName.includes(searchLower)
                        })
                        return filteredAccounts.map((account, index) => (
                          <div
                            key={account.id}
                            className={`px-3 py-2 cursor-pointer text-sm transition-colors duration-150 focus:bg-gray-100 focus:outline-none ${
                              selectedAccountIndex === index 
                                ? 'bg-blue-50 border-l-2 border-blue-500' 
                                : 'hover:bg-gray-100'
                            }`}
                            tabIndex={-1}
                            onClick={() => {
                              handleAccountSelect(account)
                              setAccountSearch("")
                              setAccountSearchOpen(false)
                            }}
                          >
                            {account.accountName} {account.city && `(${account.city})`}
                          </div>
                        ))
                      })()}
                      {(() => {
                        const filteredAccounts = accounts.filter(account => {
                          const searchLower = accountSearch.toLowerCase()
                          const displayName = `${account.accountName} ${account.city || ''}`.toLowerCase()
                          return displayName.includes(searchLower)
                        })
                        return filteredAccounts.length === 0 && (
                          <p className="text-sm text-gray-500 text-center py-2">No accounts found</p>
                        )
                      })()}
                      </div>
                    </ScrollArea>
                  </div>
                </PopoverContent>
              </Popover>
            </div>

            {/* Contact Selection with Search */}
            <div>
              <Label htmlFor="contact">Contact Name</Label>
              <Popover 
                open={contactSearchOpen} 
                onOpenChange={(open) => {
                  // Only allow opening if the selected account has sub-contacts
                  const selectedAccount = accounts.find(a => a.accountName === formData.accountName)
                  const relatedContacts = contacts.filter(c => c.accountName === formData.accountName)
                  if (selectedAccount && relatedContacts.length > 0) {
                    setContactSearchOpen(open)
                  }
                }}
              >
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    role="combobox"
                    aria-expanded={contactSearchOpen}
                    className={`w-full justify-between text-left font-normal ${
                      formData.accountName && 
                      contacts.filter(c => c.accountName === formData.accountName).length === 0 
                        ? 'cursor-not-allowed opacity-60' 
                        : ''
                    }`}
                    disabled={
                      !formData.accountName || 
                      contacts.filter(c => c.accountName === formData.accountName).length === 0
                    }
                  >
                    {formData.contactName || "Select contact..."}
                    <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-[400px] p-2" align="start">
                  <div className="space-y-2">
                    <Input
                      placeholder="Search contacts..."
                      value={contactSearch}
                      onChange={(e) => setContactSearch(e.target.value)}
                      className="w-full"
                    />
                    <ScrollArea 
                      className="h-[200px] border rounded-md"
                      onWheel={(e) => {
                        // Allow the ScrollArea to handle wheel events properly
                        e.stopPropagation()
                      }}
                      tabIndex={0}
                    >
                      {contacts
                        .filter(contact => {
                          // First filter by selected account
                          if (contact.accountName !== formData.accountName) {
                            return false
                          }
                          // Then filter by search term
                          const searchLower = contactSearch.toLowerCase()
                          const displayName = `${contact.accountName} ${contact.department || ''} ${contact.contactName}`.toLowerCase()
                          return displayName.includes(searchLower)
                        })
                        .map((contact, index) => (
                          <div
                            key={contact.id}
                            className="px-3 py-2 hover:bg-gray-100 cursor-pointer text-sm transition-colors duration-150 focus:bg-gray-100 focus:outline-none"
                            tabIndex={0}
                            onClick={() => {
                              handleContactSelect(contact)
                              setContactSearch("")
                              setContactSearchOpen(false)
                            }}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter' || e.key === ' ') {
                                e.preventDefault()
                                handleContactSelect(contact)
                                setContactSearch("")
                                setContactSearchOpen(false)
                              }
                            }}
                          >
                            <div className="flex flex-col">
                              <span className="font-medium">
                                {contact.accountName}
                                {contact.department && ` (${contact.department})`} - {contact.contactName}
                              </span>
                            </div>
                          </div>
                        ))}
                      {contacts.filter(contact => {
                        // First filter by selected account
                        if (contact.accountName !== formData.accountName) {
                          return false
                        }
                        // Then filter by search term
                        const searchLower = contactSearch.toLowerCase()
                        const displayName = `${contact.accountName} ${contact.department || ''} ${contact.contactName}`.toLowerCase()
                        return displayName.includes(searchLower)
                      }).length === 0 && (
                        <p className="text-sm text-gray-500 text-center py-2">No contacts found for this account</p>
                      )}
                    </ScrollArea>
                  </div>
                </PopoverContent>
              </Popover>
            </div>
          </div>

          {/* Department and Phone */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="department">Department</Label>
              <Input
                id="department"
                value={formData.department}
                onChange={(e) => setFormData(prev => ({ ...prev, department: e.target.value }))}
                placeholder="Auto-filled from contact"
                disabled={isSubmitting}
              />
            </div>

            <div>
              <Label htmlFor="phone">Phone</Label>
              <Input
                id="phone"
                value={formData.phone}
                onChange={(e) => setFormData(prev => ({ ...prev, phone: e.target.value }))}
                placeholder="Auto-filled from account/contact"
                disabled={isSubmitting}
              />
            </div>
          </div>

          {/* Email (full width) */}
          <div>
            <Label htmlFor="email">Email</Label>
            <Input
              id="email"
              type="email"
              value={formData.email}
              onChange={(e) => setFormData(prev => ({ ...prev, email: e.target.value }))}
              placeholder="Auto-filled from account/contact"
              disabled={isSubmitting}
            />
          </div>

          {/* Address and City */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="address">Address</Label>
              <Input
                id="address"
                value={formData.address}
                onChange={(e) => setFormData(prev => ({ ...prev, address: e.target.value }))}
                placeholder="Auto-filled from account/contact"
                disabled={isSubmitting}
              />
            </div>

            <div>
              <Label htmlFor="city">City</Label>
              <Input
                id="city"
                value={formData.city}
                onChange={(e) => setFormData(prev => ({ ...prev, city: e.target.value }))}
                placeholder="Enter city"
                disabled={isSubmitting}
              />
            </div>
          </div>

          {/* State and Country */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="state">State</Label>
              <Input
                id="state"
                value={formData.state}
                onChange={(e) => setFormData(prev => ({ ...prev, state: e.target.value }))}
                placeholder="Enter state"
                disabled={isSubmitting}
              />
            </div>

            <div>
              <Label htmlFor="country">Country</Label>
              <Input
                id="country"
                value={formData.country}
                onChange={(e) => setFormData(prev => ({ ...prev, country: e.target.value }))}
                placeholder="Auto-filled from account"
                disabled={isSubmitting}
              />
            </div>
          </div>

          {/* Lead Source and Lead Status */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="leadSource">Lead Source *</Label>
              <Select 
                value={formData.leadSource} 
                onValueChange={(value) => setFormData(prev => ({ ...prev, leadSource: value }))}
                disabled={isSubmitting}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select lead source" />
                </SelectTrigger>
                <SelectContent>
                  {leadSources.map((source) => (
                    <SelectItem key={source} value={source}>
                      {source}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Lead Status */}
            <div>
              <Label htmlFor="leadStatus">Lead Status</Label>
              <Select 
                value={formData.leadStatus} 
                onValueChange={(value) => setFormData(prev => ({ ...prev, leadStatus: value }))}
                disabled={isSubmitting}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select lead status" />
                </SelectTrigger>
                <SelectContent>
                  {leadStatuses.map((status) => (
                    <SelectItem key={status} value={status}>
                      {status}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Multiple Product Selection */}
          <MultipleProductSelector 
            selectedProducts={selectedProducts}
            onProductsChange={setSelectedProducts}
            onTotalBudgetChange={setTotalBudget}
          />

          {/* Date Fields */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="expectedClosingDate">Expected Closing Date</Label>
              <Input
                id="expectedClosingDate"
                type="date"
                value={formData.expectedClosingDate}
                onChange={(e) => setFormData(prev => ({ ...prev, expectedClosingDate: e.target.value }))}
                disabled={isSubmitting}
              />
            </div>

            <div>
              <Label htmlFor="nextFollowupDate">Next Follow-up Date</Label>
              <Input
                id="nextFollowupDate"
                type="date"
                value={formData.nextFollowupDate}
                onChange={(e) => setFormData(prev => ({ ...prev, nextFollowupDate: e.target.value }))}
                disabled={isSubmitting}
              />
            </div>
          </div>

          {/* Assigned To */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="assignedTo">Assigned To *</Label>
              <Select
                value={formData.assignedTo}
                onValueChange={(value) => setFormData(prev => ({ ...prev, assignedTo: value }))}
                disabled={isSubmitting}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select account" />
                </SelectTrigger>
                <SelectContent>
                  {accounts.map((account) => (
                    <SelectItem key={account.id} value={account.accountName}>
                      {account.accountName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div></div>
          </div>

          {/* Notes */}
          <div>
            <Label htmlFor="notes">Notes</Label>
            <Textarea
              id="notes"
              value={formData.notes}
              onChange={(e) => setFormData(prev => ({ ...prev, notes: e.target.value }))}
              placeholder="Enter specific requirements, notes, or additional information..."
              rows={3}
              disabled={isSubmitting}
              className="resize-none"
            />
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={isSubmitting}>
            <X className="w-4 h-4 mr-2" />
            Cancel
          </Button>
          <Button 
            onClick={() => {
              console.log('=== BUTTON CLICKED ===')
              console.log('isSubmitting:', isSubmitting)
              console.log('About to call handleSubmit')
              handleSubmit()
            }} 
            className="bg-blue-600 hover:bg-blue-700" 
            disabled={isSubmitting}
          >
            {isSubmitting ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                {editingLead ? 'Updating...' : 'Adding Lead...'}
              </>
            ) : (
              <>
                <Save className="w-4 h-4 mr-2" />
                {editingLead ? 'Update Lead' : 'Add Lead'}
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}