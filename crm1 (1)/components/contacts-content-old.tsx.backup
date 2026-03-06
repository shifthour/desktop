"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Plus, Search, Download, Edit, Phone, Mail, Users, Upload, Save, X, Building2, User, ChevronDown } from "lucide-react"
import { useRouter } from "next/navigation"
import { useToast } from "@/hooks/use-toast"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import { ContactsFileImport } from "@/components/contacts-file-import"
import { exportToExcel, formatDateForExcel } from "@/lib/excel-export"

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
  </svg>
)

interface Contact {
  id: string
  accountName: string      // Company they work for
  contactName: string      // Person's name
  department: string       // Labs, Research, Quality Control, etc.
  position: string         // Manager, Director, Scientist, etc.
  phone: string           // Contact phone
  email: string           // Contact email
  website?: string        // Company website
  address?: string        // Address
  city?: string           // Location
  state?: string          // State
  assignedTo?: string     // Sales rep assigned
  status: string          // Active/Inactive
  createdAt?: string      // When contact was added
}

const departments = ["All", "Research & Development", "Laboratory", "Quality Control", "Purchase", "Production", "Administration", "Marketing", "Sales", "Technical", "Other"]
const positions = ["All", "Director", "Manager", "Head", "Scientist", "Executive", "Officer", "Engineer", "Analyst", "Assistant", "Other"]
const statuses = ["All", "Active", "Inactive"]

export function ContactsContent() {
  const router = useRouter()
  const { toast } = useToast()
  const [contacts, setContacts] = useState<Contact[]>([])
  const [accounts, setAccounts] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedDepartment, setSelectedDepartment] = useState("All")
  const [selectedPosition, setSelectedPosition] = useState("All") 
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [isImportModalOpen, setIsImportModalOpen] = useState(false)
  const [isAddContactOpen, setIsAddContactOpen] = useState(false)
  const [isEditContactOpen, setIsEditContactOpen] = useState(false)
  const [editingContact, setEditingContact] = useState<Contact | null>(null)
  const [isImporting, setIsImporting] = useState(false)
  const [importProgress, setImportProgress] = useState({ current: 0, total: 0 })
  const [accountComboboxOpen, setAccountComboboxOpen] = useState(false)
  const [editAccountComboboxOpen, setEditAccountComboboxOpen] = useState(false)
  const [accountSearch, setAccountSearch] = useState("")
  const [editAccountSearch, setEditAccountSearch] = useState("")
  const [isLoadingStats, setIsLoadingStats] = useState(true)
  const [contactStats, setContactStats] = useState({
    total: 0,
    active: 0,
    newThisMonth: 0,
    mostActiveDepartment: 'N/A',
    mostActiveDepartmentCount: 0
  })

  // Form state for add/edit
  const [formData, setFormData] = useState<Partial<Contact>>({
    accountName: "",
    contactName: "",
    department: "",
    position: "",
    phone: "",
    email: "",
    website: "",
    address: "",
    city: "",
    state: "",
    assignedTo: "",
    status: "Active"
  })

  useEffect(() => {
    // Clear search term when component mounts to prevent auto-population
    setSearchTerm("")
    
    // Small delay to ensure localStorage is available
    const timer = setTimeout(() => {
      fetchContacts()
      fetchAccounts()
    }, 100)
    
    return () => clearTimeout(timer)
  }, [])
  
  // Calculate stats whenever contacts change
  useEffect(() => {
    if (contacts.length > 0) {
      calculateContactStats()
    }
  }, [contacts])

  const calculateContactStats = () => {
    console.log('Calculating stats for contacts:', contacts.length)
    
    if (!contacts || contacts.length === 0) {
      console.log('No contacts to calculate stats for')
      setContactStats({
        total: 0,
        active: 0,
        newThisMonth: 0,
        mostActiveDepartment: 'N/A',
        mostActiveDepartmentCount: 0
      })
      return
    }
    
    const currentDate = new Date()
    const currentMonth = currentDate.getMonth()
    const currentYear = currentDate.getFullYear()

    const newThisMonth = contacts.filter((contact) => {
      if (!contact.createdAt) return false
      const createdDate = new Date(contact.createdAt)
      return createdDate.getMonth() === currentMonth && createdDate.getFullYear() === currentYear
    }).length

    const activeContacts = contacts.filter(contact => contact.status === 'Active').length

    // Calculate most active department
    const departmentCount = contacts.reduce((acc: any, contact: any) => {
      if (contact.department && contact.department.trim()) {
        acc[contact.department] = (acc[contact.department] || 0) + 1
      }
      return acc
    }, {})

    const mostActiveDepartment = Object.keys(departmentCount).length > 0 
      ? Object.keys(departmentCount).reduce((a, b) => departmentCount[a] > departmentCount[b] ? a : b)
      : 'N/A'
    const mostActiveDepartmentCount = departmentCount[mostActiveDepartment] || 0

    const stats = {
      total: contacts.length,
      active: activeContacts,
      newThisMonth,
      mostActiveDepartment,
      mostActiveDepartmentCount
    }
    
    console.log('Calculated stats:', stats)
    setContactStats(stats)
    setIsLoadingStats(false)
  }

  const fetchAccounts = async () => {
    try {
      // Get user from localStorage to get company_id
      const storedUser = localStorage.getItem('user')
      if (!storedUser) {
        console.log("No user found in localStorage")
        return
      }
      
      const user = JSON.parse(storedUser)
      if (!user.company_id) {
        console.log("No company_id found for user")
        toast({
          title: "Error",
          description: "Company not found. Please login again.",
          variant: "destructive"
        })
        return
      }
      
      console.log("Loading accounts for company:", user.company_id)
      
      // Fetch accounts from backend API with proper company_id
      const response = await fetch(`/api/accounts?companyId=${user.company_id}&limit=1000`)
      if (!response.ok) {
        throw new Error(`Failed to fetch accounts: ${response.status}`)
      }
      
      const data = await response.json()
      console.log('Fetched accounts:', data.accounts)
      
      // Map the accounts to the format needed for dropdown
      const accountsData = data.accounts?.map((account: any) => ({
        id: account.id,
        accountName: account.account_name,
        website: account.website || '',
        billing_street: account.billing_street || '',
        address: account.address || '',
        city: account.billing_city || '',
        state: account.billing_state || ''
      })) || []
      
      setAccounts(accountsData)
      console.log('Processed accounts for dropdown:', accountsData)
      
    } catch (error) {
      console.error('Error fetching accounts:', error)
      toast({
        title: "Error",
        description: "Failed to load accounts from database",
        variant: "destructive"
      })
      // Fallback to empty array if API fails
      setAccounts([])
    }
  }

  const fetchContacts = async () => {
    try {
      // Get user from localStorage to get company_id
      const storedUser = localStorage.getItem('user')
      if (!storedUser) {
        console.log("No user found in localStorage")
        return
      }
      
      const user = JSON.parse(storedUser)
      if (!user.company_id) {
        console.log("No company_id found for user")
        return
      }
      
      // Fetch contacts from backend API
      console.log('Fetching contacts for company:', user.company_id)
      const response = await fetch(`/api/contacts?companyId=${user.company_id}&limit=1000`)
      console.log('Contacts API response status:', response.status)
      
      if (!response.ok) {
        const errorData = await response.json()
        console.error('Contacts API error:', errorData)
        throw new Error(`Failed to fetch contacts: ${response.status} - ${errorData.error || 'Unknown error'}`)
      }
      
      const data = await response.json()
      console.log('Fetched contacts data:', data)
      console.log('Number of contacts:', data.contacts?.length || 0)
      
      if (data.contacts && data.contacts.length > 0) {
        setContacts(data.contacts)
        console.log('Contacts set successfully')
      } else {
        console.log('No contacts found - showing empty state')
        setContacts([])
      }

      // Calculate stats
      const currentDate = new Date()
      const currentMonth = currentDate.getMonth()
      const currentYear = currentDate.getFullYear()

      // Stats will be calculated by useEffect when contacts change

    } catch (error) {
      console.error('Error fetching contacts:', error)
      
      // No fallback data - show empty state
      setContacts([])
      
      toast({
        title: "Error loading contacts",
        description: "Failed to load contacts from database. Please try refreshing the page.",
        variant: "destructive"
      })
      
      // Stats will be calculated by useEffect when contacts change
      
      toast({
        title: "Error",
        description: `Failed to load contacts from database. Using demo data. Error: ${error instanceof Error ? error.message : 'Unknown error'}`,
        variant: "destructive"
      })
    }
  }

  const filteredContacts = contacts.filter((contact) => {
    const matchesSearch = 
      contact.contactName?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      contact.accountName?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      contact.email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      contact.department?.toLowerCase().includes(searchTerm.toLowerCase())
    
    const matchesDepartment = selectedDepartment === "All" || contact.department === selectedDepartment
    const matchesPosition = selectedPosition === "All" || contact.position === selectedPosition
    const matchesStatus = selectedStatus === "All" || contact.status === selectedStatus
    
    return matchesSearch && matchesDepartment && matchesPosition && matchesStatus
  })

  const handleAccountSelect = (accountName: string) => {
    console.log('Selecting account:', accountName)
    const selectedAccount = accounts.find(acc => acc.accountName === accountName)
    console.log('Found account:', selectedAccount)
    setFormData(prev => ({
      ...prev,
      accountName,
      website: selectedAccount?.website || "",
      address: selectedAccount?.billing_street || selectedAccount?.address || "",
      city: selectedAccount?.city || "",
      state: selectedAccount?.state || ""
    }))
  }

  const handleAddContact = () => {
    setFormData({
      accountName: "",
      contactName: "",
      department: "",
      position: "",
      phone: "",
      email: "",
      website: "",
      address: "",
      city: "",
      state: "",
      assignedTo: "",
      status: "Active"
    })
    setAccountSearch("")
    setAccountComboboxOpen(false)
    setIsAddContactOpen(true)
  }

  const handleEditContact = (contact: Contact) => {
    setEditingContact(contact)
    setFormData(contact)
    setEditAccountSearch("")
    setEditAccountComboboxOpen(false)
    setIsEditContactOpen(true)
  }

  const handleSaveContact = async (isEdit: boolean) => {
    try {
      // Get user from localStorage to get company_id
      const storedUser = localStorage.getItem('user')
      if (!storedUser) {
        toast({
          title: "Error",
          description: "Please login again",
          variant: "destructive"
        })
        return
      }
      
      const user = JSON.parse(storedUser)
      if (!user.company_id) {
        toast({
          title: "Error",
          description: "Company not found. Please login again.",
          variant: "destructive"
        })
        return
      }
      
      if (isEdit && editingContact) {
        // Update contact via API
        const response = await fetch('/api/contacts', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            id: editingContact.id,
            companyId: user.company_id,
            ...formData
          })
        })
        
        if (!response.ok) {
          const error = await response.json()
          throw new Error(error.error || 'Failed to update contact')
        }
        
        toast({
          title: "Contact updated",
          description: `Contact ${formData.contactName} has been updated.`
        })
      } else {
        // Add new contact via API
        const response = await fetch('/api/contacts', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            companyId: user.company_id,
            ...formData
          })
        })
        
        if (!response.ok) {
          const error = await response.json()
          throw new Error(error.error || 'Failed to add contact')
        }
        
        toast({
          title: "Contact added",
          description: `Contact ${formData.contactName} has been added.`
        })
      }

      setIsAddContactOpen(false)
      setIsEditContactOpen(false)
      setEditingContact(null)
      setAccountSearch("")
      setEditAccountSearch("")
      setAccountComboboxOpen(false)
      setEditAccountComboboxOpen(false)
      setSearchTerm("") // Clear search term to prevent auto-population
      fetchContacts() // Refresh stats
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to save contact",
        variant: "destructive"
      })
    }
  }

  const handleExport = async () => {
    try {
      const success = await exportToExcel(filteredContacts, {
        filename: `contacts_${new Date().toISOString().split('T')[0]}`,
        sheetName: 'Contacts',
        columns: [
          { key: 'accountName', label: 'Company Name', width: 20 },
          { key: 'contactName', label: 'Contact Name', width: 20 },
          { key: 'department', label: 'Department', width: 18 },
          { key: 'position', label: 'Position', width: 18 },
          { key: 'phone', label: 'Phone', width: 15 },
          { key: 'email', label: 'Email', width: 25 },
          { key: 'website', label: 'Website', width: 25 },
          { key: 'city', label: 'City', width: 15 },
          { key: 'state', label: 'State', width: 15 },
          { key: 'assignedTo', label: 'Assigned To', width: 15 },
          { key: 'status', label: 'Status', width: 12 },
          { key: 'createdAt', label: 'Created Date', width: 15 }
        ]
      })
      
      if (success) {
        toast({
          title: "Export completed",
          description: "Contacts data has been exported to Excel file."
        })
      } else {
        toast({
          title: "Export failed",
          description: "Failed to export contacts data. Please try again.",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Export error:', error)
      toast({
        title: "Export failed",
        description: "Failed to export contacts data. Please try again.",
        variant: "destructive"
      })
    }
  }

  const handleImportData = async (importedData: any[]) => {
    setIsImporting(true)
    setImportProgress({ current: 0, total: importedData.length })

    try {
      // Process imported data to match contact structure
      const processedContacts = importedData.map((item: any) => {
        const accountName = item.accountName || item['Account Name'] || item.account_name || 
                           item.company || item.Company || item['Company Name'] || ''
        
        return {
          accountName,
          contactName: item.contactName || item['Contact Name'] || item.contact_name || 
                      item.name || item.Name || item['Person Name'] || '',
          department: item.department || item.Department || item.dept || '',
          position: item.position || item.Position || item.title || item.Title || 
                   item.designation || item.Designation || '',
          phone: item.phone || item.Phone || item['Contact Phone'] || item.contactPhone || 
                item.mobile || item.Mobile || '',
          email: item.email || item.Email || item['Email Address'] || item.emailAddress || '',
          website: item.website || item.Website || item['Website URL'] || item.websiteUrl || '',
          city: item.city || item.City || item.location || '',
          state: item.state || item.State || '',
          assignedTo: item['Assigned To'] || item.assignedTo || item.assigned_to || '',
          status: item.status || item.Status || 'Active'
        }
      })

      // Simulate processing with progress
      for (let i = 0; i < processedContacts.length; i++) {
        setImportProgress({ current: i + 1, total: processedContacts.length })
        await new Promise(resolve => setTimeout(resolve, 100)) // Simulate processing delay
      }

      // Add to contacts list
      const newContacts = processedContacts.map((contact, index) => ({
        id: Date.now() + index + "",
        ...contact
      }))

      setContacts([...contacts, ...newContacts])
      
      toast({
        title: "Import completed",
        description: `Successfully imported ${newContacts.length} contacts.`
      })

      setIsImportModalOpen(false)
      fetchContacts() // Refresh stats

    } catch (error) {
      console.error('Import error:', error)
      toast({
        title: "Import failed",
        description: "Failed to import contacts data.",
        variant: "destructive"
      })
    } finally {
      setIsImporting(false)
      setImportProgress({ current: 0, total: 0 })
    }
  }

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "active":
        return "bg-green-100 text-green-800"
      case "inactive":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Contacts Management</h1>
          <p className="text-gray-600">Manage contact persons within your accounts</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline" onClick={handleExport}>
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button variant="outline" onClick={() => setIsImportModalOpen(true)}>
            <Upload className="w-4 h-4 mr-2" />
            Import Data
          </Button>
          <Button onClick={handleAddContact} className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Add Contact
          </Button>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-blue-100">
                <Users className="w-6 h-6 text-blue-600" />
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold">
                  {isLoadingStats ? (
                    <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
                  ) : (
                    contactStats.total
                  )}
                </div>
                <p className="text-xs text-muted-foreground">Total Contacts</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-green-100">
                <User className="w-6 h-6 text-green-600" />
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold">
                  {isLoadingStats ? (
                    <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
                  ) : (
                    contactStats.active
                  )}
                </div>
                <p className="text-xs text-muted-foreground">Active Contacts</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-purple-100">
                <Plus className="w-6 h-6 text-purple-600" />
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold">
                  {isLoadingStats ? (
                    <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
                  ) : (
                    contactStats.newThisMonth
                  )}
                </div>
                <p className="text-xs text-muted-foreground">New This Month</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-orange-100">
                <Building2 className="w-6 h-6 text-orange-600" />
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold">
                  {isLoadingStats ? (
                    <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
                  ) : (
                    contactStats.mostActiveDepartmentCount
                  )}
                </div>
                <p className="text-xs text-muted-foreground">Active Department</p>
                <p className="text-xs font-medium text-gray-700">{contactStats.mostActiveDepartment}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Search and Filters */}
      <Card>
        <CardHeader>
          <CardTitle>Contact Search & Filters</CardTitle>
          <CardDescription>Filter and search through your contacts</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex space-x-4 items-center">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
              <Input
                placeholder="Search contacts by name, company, email, or department..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10"
                autoComplete="off"
              />
            </div>
            <Select value={selectedDepartment} onValueChange={setSelectedDepartment}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Departments" />
              </SelectTrigger>
              <SelectContent>
                {departments.map((dept) => (
                  <SelectItem key={dept} value={dept}>{dept}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedPosition} onValueChange={setSelectedPosition}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All Positions" />
              </SelectTrigger>
              <SelectContent>
                {positions.map((pos) => (
                  <SelectItem key={pos} value={pos}>{pos}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedStatus} onValueChange={setSelectedStatus}>
              <SelectTrigger className="w-32">
                <SelectValue placeholder="All Status" />
              </SelectTrigger>
              <SelectContent>
                {statuses.map((status) => (
                  <SelectItem key={status} value={status}>{status}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Contacts List - Card Layout */}
      <Card>
        <CardHeader>
          <div className="flex justify-between items-center">
            <div>
              <CardTitle>Contacts List</CardTitle>
              <CardDescription>List of all contact persons and their current status</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredContacts.map((contact) => (
              <div
                key={contact.id}
                className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center space-x-4">
                  <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                    <User className="w-6 h-6 text-blue-600" />
                  </div>
                  <div>
                    <div className="flex items-center space-x-2">
                      <h3 className="font-semibold">{contact.contactName}</h3>
                      <span className={`px-2 py-1 text-xs rounded-full ${getStatusColor(contact.status)}`}>
                        {contact.status}
                      </span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <p className="text-sm text-gray-600">{contact.accountName}</p>
                      <span className="px-2 py-1 text-xs bg-blue-100 text-blue-800 rounded-full font-medium">
                        {contact.department}
                      </span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <span className="px-2 py-1 text-xs bg-green-100 text-green-800 rounded-full font-medium">
                        {contact.position}
                      </span>
                    </div>
                    <div className="text-xs text-gray-500 space-y-1">
                      <p>{contact.city}, {contact.state}</p>
                    </div>
                  </div>
                </div>
                
                {/* Center section - Contact details */}
                <div className="flex-1 flex flex-col items-center justify-center space-y-1">
                  {contact.phone && (
                    <div className="flex items-center text-xs text-gray-500">
                      <Phone className="w-3 h-3 mr-1" />
                      <span>{contact.phone}</span>
                    </div>
                  )}
                  {contact.email && (
                    <div className="flex items-center text-xs text-gray-500">
                      <Mail className="w-3 h-3 mr-1" />
                      <span>{contact.email}</span>
                    </div>
                  )}
                  {contact.website && (
                    <div className="flex items-center text-xs text-blue-600">
                      <Building2 className="w-3 h-3 mr-1" />
                      <a 
                        href={contact.website} 
                        target="_blank" 
                        rel="noopener noreferrer"
                        className="hover:underline"
                      >
                        {contact.website.replace(/^https?:\/\//, '').replace(/^www\./, '')}
                      </a>
                    </div>
                  )}
                </div>

                {/* Right section - Assigned to and actions */}
                <div className="flex items-center space-x-4">
                  <div className="text-right">
                    <p className="text-xs text-gray-500">
                      Assigned to: <span className="font-medium text-gray-900">{contact.assignedTo || "Unassigned"}</span>
                    </p>
                  </div>
                  <div className="flex space-x-1">
                    {/* Communication Buttons */}
                    {contact.phone && (
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => window.open(`tel:${contact.phone}`, '_self')}
                        className="text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                        title="Call Contact"
                      >
                        <Phone className="w-4 h-4" />
                      </Button>
                    )}
                    {contact.phone && (
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => {
                          const message = `Hello ${contact.contactName}! I'm reaching out regarding our discussion about laboratory equipment for ${contact.accountName}. Could we schedule a time to discuss your requirements?`
                          window.open(`https://wa.me/${contact.phone.replace(/[^\d]/g, '')}?text=${encodeURIComponent(message)}`, '_blank')
                        }}
                        className="text-green-600 hover:text-green-800 hover:bg-green-50"
                        title="WhatsApp Contact"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                    )}
                    {contact.email && (
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => {
                          const subject = `Follow-up: ${contact.accountName} Discussion`
                          const body = `Dear ${contact.contactName},\n\nI hope this email finds you well. I wanted to follow up on our recent discussion.\n\nBest regards,\nSales Team`
                          window.open(`mailto:${contact.email}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`, '_self')
                        }}
                        className="text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                        title="Send Email"
                      >
                        <Mail className="w-4 h-4" />
                      </Button>
                    )}
                    
                    {/* Action Buttons */}
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      title="Edit Contact"
                      onClick={() => handleEditContact(contact)}
                    >
                      <Edit className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              </div>
            ))}
            
            {filteredContacts.length === 0 && (
              <div className="text-center py-8">
                <p className="text-gray-500">No contacts found matching your criteria.</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Import Modal */}
      <ContactsFileImport
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        onImport={handleImportData}
        isImporting={isImporting}
        importProgress={importProgress}
      />

      {/* Add Contact Modal */}
      <Dialog open={isAddContactOpen} onOpenChange={setIsAddContactOpen}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Add New Contact</DialogTitle>
          </DialogHeader>
          
          <div className="space-y-4 py-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="accountName">Company/Account Name</Label>
                <Popover open={accountComboboxOpen} onOpenChange={setAccountComboboxOpen}>
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      role="combobox"
                      aria-expanded={accountComboboxOpen}
                      className="w-full justify-between"
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
                        onChange={(e) => setAccountSearch(e.target.value)}
                        className="w-full"
                      />
                      <div className="max-h-[200px] border rounded-md custom-scrollbar">
                        {accounts
                          .filter(account => {
                            const searchLower = accountSearch.toLowerCase()
                            const displayName = `${account.accountName} ${account.city || ''}`.toLowerCase()
                            return displayName.includes(searchLower)
                          })
                          .map((account) => (
                            <div
                              key={account.id}
                              className="px-3 py-2 hover:bg-gray-100 cursor-pointer text-sm"
                              onClick={() => {
                                console.log('Selecting account:', account.accountName)
                                handleAccountSelect(account.accountName)
                                setAccountSearch("")
                                setAccountComboboxOpen(false)
                              }}
                            >
                              {account.accountName} {account.city && `(${account.city})`}
                            </div>
                          ))}
                        {accounts.filter(account => 
                          account.accountName.toLowerCase().includes(accountSearch.toLowerCase())
                        ).length === 0 && (
                          <p className="text-sm text-gray-500 text-center py-2">No accounts found</p>
                        )}
                      </div>
                    </div>
                  </PopoverContent>
                </Popover>
              </div>
              <div>
                <Label htmlFor="contactName">Contact Person Name</Label>
                <Input
                  id="contactName"
                  value={formData.contactName}
                  onChange={(e) => setFormData({...formData, contactName: e.target.value})}
                  placeholder="Person name"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="department">Department</Label>
                <Input
                  id="department"
                  value={formData.department}
                  onChange={(e) => setFormData({...formData, department: e.target.value})}
                  placeholder="Research & Development, Laboratory, etc."
                />
              </div>
              <div>
                <Label htmlFor="position">Position/Title</Label>
                <Input
                  id="position"
                  value={formData.position}
                  onChange={(e) => setFormData({...formData, position: e.target.value})}
                  placeholder="Manager, Director, etc."
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="phone">Phone Number</Label>
                <Input
                  id="phone"
                  value={formData.phone}
                  onChange={(e) => setFormData({...formData, phone: e.target.value})}
                  placeholder="+91 98765 43210"
                />
              </div>
              <div>
                <Label htmlFor="email">Email Address</Label>
                <Input
                  id="email"
                  type="email"
                  value={formData.email}
                  onChange={(e) => setFormData({...formData, email: e.target.value})}
                  placeholder="email@example.com"
                />
              </div>
            </div>

            <div>
              <Label htmlFor="website">Website (Auto-populated from Account)</Label>
              <Input
                id="website"
                value={formData.website}
                onChange={(e) => setFormData({...formData, website: e.target.value})}
                placeholder="https://www.company.com"
                disabled
                className="bg-gray-50 cursor-not-allowed"
              />
            </div>

            <div>
              <Label htmlFor="address">Address (Auto-populated from Account)</Label>
              <Input
                id="address"
                value={formData.address}
                onChange={(e) => setFormData({...formData, address: e.target.value})}
                placeholder="Complete address"
                disabled
                className="bg-gray-50 cursor-not-allowed"
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="city">City (Auto-populated from Account)</Label>
                <Input
                  id="city"
                  value={formData.city}
                  onChange={(e) => setFormData({...formData, city: e.target.value})}
                  placeholder="City"
                  disabled
                  className="bg-gray-50 cursor-not-allowed"
                />
              </div>
              <div>
                <Label htmlFor="state">State (Auto-populated from Account)</Label>
                <Input
                  id="state"
                  value={formData.state}
                  onChange={(e) => setFormData({...formData, state: e.target.value})}
                  placeholder="State"
                  disabled
                  className="bg-gray-50 cursor-not-allowed"
                />
              </div>
            </div>

            <div>
              <Label htmlFor="assignedTo">Assigned To</Label>
              <Input
                id="assignedTo"
                value={formData.assignedTo}
                onChange={(e) => setFormData({...formData, assignedTo: e.target.value})}
                placeholder="Sales representative"
              />
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setIsAddContactOpen(false)}>
              <X className="w-4 h-4 mr-2" />
              Cancel
            </Button>
            <Button onClick={() => handleSaveContact(false)} className="bg-blue-600 hover:bg-blue-700">
              <Save className="w-4 h-4 mr-2" />
              Add Contact
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Contact Modal */}
      {editingContact && (
        <Dialog open={isEditContactOpen} onOpenChange={setIsEditContactOpen}>
          <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>Edit Contact: {editingContact.contactName}</DialogTitle>
            </DialogHeader>
            
            <div className="space-y-4 py-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="edit-accountName">Company/Account Name</Label>
                  <Popover open={editAccountComboboxOpen} onOpenChange={setEditAccountComboboxOpen}>
                    <PopoverTrigger asChild>
                      <Button
                        variant="outline"
                        role="combobox"
                        aria-expanded={editAccountComboboxOpen}
                        className="w-full justify-between"
                      >
                        {formData.accountName || "Select account..."}
                        <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-[400px] p-2" align="start">
                      <div className="space-y-2">
                        <Input
                          placeholder="Search accounts..."
                          value={editAccountSearch}
                          onChange={(e) => setEditAccountSearch(e.target.value)}
                          className="w-full"
                        />
                        <div className="max-h-[200px] border rounded-md custom-scrollbar">
                          {accounts
                            .filter(account => {
                              const searchLower = editAccountSearch.toLowerCase()
                              const displayName = `${account.accountName} ${account.city || ''}`.toLowerCase()
                              return displayName.includes(searchLower)
                            })
                            .map((account) => (
                              <div
                                key={account.id}
                                className="px-3 py-2 hover:bg-gray-100 cursor-pointer text-sm"
                                onClick={() => {
                                  console.log('Selecting account in edit:', account.accountName)
                                  handleAccountSelect(account.accountName)
                                  setEditAccountSearch("")
                                  setEditAccountComboboxOpen(false)
                                }}
                              >
                                {account.accountName} {account.city && `(${account.city})`}
                              </div>
                            ))}
                          {accounts.filter(account => 
                            account.accountName.toLowerCase().includes(editAccountSearch.toLowerCase())
                          ).length === 0 && (
                            <p className="text-sm text-gray-500 text-center py-2">No accounts found</p>
                          )}
                        </div>
                      </div>
                    </PopoverContent>
                  </Popover>
                </div>
                <div>
                  <Label htmlFor="edit-contactName">Contact Person Name</Label>
                  <Input
                    id="edit-contactName"
                    value={formData.contactName}
                    onChange={(e) => setFormData({...formData, contactName: e.target.value})}
                    placeholder="Person name"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="edit-department">Department</Label>
                  <Input
                    id="edit-department"
                    value={formData.department}
                    onChange={(e) => setFormData({...formData, department: e.target.value})}
                    placeholder="Research & Development, Laboratory, etc."
                  />
                </div>
                <div>
                  <Label htmlFor="edit-position">Position/Title</Label>
                  <Input
                    id="edit-position"
                    value={formData.position}
                    onChange={(e) => setFormData({...formData, position: e.target.value})}
                    placeholder="Manager, Director, etc."
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="edit-phone">Phone Number</Label>
                  <Input
                    id="edit-phone"
                    value={formData.phone}
                    onChange={(e) => setFormData({...formData, phone: e.target.value})}
                    placeholder="+91 98765 43210"
                  />
                </div>
                <div>
                  <Label htmlFor="edit-email">Email Address</Label>
                  <Input
                    id="edit-email"
                    type="email"
                    value={formData.email}
                    onChange={(e) => setFormData({...formData, email: e.target.value})}
                    placeholder="email@example.com"
                  />
                </div>
              </div>

              <div>
                <Label htmlFor="edit-website">Website (Auto-populated from Account)</Label>
                <Input
                  id="edit-website"
                  value={formData.website}
                  onChange={(e) => setFormData({...formData, website: e.target.value})}
                  placeholder="https://www.company.com"
                  disabled
                  className="bg-gray-50 cursor-not-allowed"
                />
              </div>

              <div>
                <Label htmlFor="edit-address">Address (Auto-populated from Account)</Label>
                <Input
                  id="edit-address"
                  value={formData.address}
                  onChange={(e) => setFormData({...formData, address: e.target.value})}
                  placeholder="Complete address"
                  disabled
                  className="bg-gray-50 cursor-not-allowed"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="edit-city">City (Auto-populated from Account)</Label>
                  <Input
                    id="edit-city"
                    value={formData.city}
                    onChange={(e) => setFormData({...formData, city: e.target.value})}
                    placeholder="City"
                    disabled
                    className="bg-gray-50 cursor-not-allowed"
                  />
                </div>
                <div>
                  <Label htmlFor="edit-state">State (Auto-populated from Account)</Label>
                  <Input
                    id="edit-state"
                    value={formData.state}
                    onChange={(e) => setFormData({...formData, state: e.target.value})}
                    placeholder="State"
                    disabled
                    className="bg-gray-50 cursor-not-allowed"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="edit-assignedTo">Assigned To</Label>
                  <Input
                    id="edit-assignedTo"
                    value={formData.assignedTo}
                    onChange={(e) => setFormData({...formData, assignedTo: e.target.value})}
                    placeholder="Sales representative"
                  />
                </div>
                <div>
                  <Label htmlFor="edit-status">Status</Label>
                  <Select value={formData.status} onValueChange={(value) => setFormData({...formData, status: value})}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Active">Active</SelectItem>
                      <SelectItem value="Inactive">Inactive</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>

            <DialogFooter>
              <Button variant="outline" onClick={() => setIsEditContactOpen(false)}>
                <X className="w-4 h-4 mr-2" />
                Cancel
              </Button>
              <Button onClick={() => handleSaveContact(true)} className="bg-blue-600 hover:bg-blue-700">
                <Save className="w-4 h-4 mr-2" />
                Save Changes
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      )}
    </div>
  )
}