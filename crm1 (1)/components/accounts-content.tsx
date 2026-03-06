"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Plus, Search, Download, Edit, Phone, Mail, Building2, Upload, Save, X, Trash2, Users, CheckCircle, Factory, Globe } from "lucide-react"
import { exportToExcel, formatDateForExcel } from "@/lib/excel-export"
import { DynamicImportModal } from "@/components/dynamic-import-modal"
import { IndustrySubIndustryPairs } from "@/components/industry-subindustry-pairs"
import { Label } from "@/components/ui/label"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { useRouter } from "next/navigation"
import { useToast } from "@/hooks/use-toast"

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
  </svg>
)

interface IndustryPair {
  industry: string
  subIndustry: string
}

interface Account {
  id: string
  accountName: string
  city?: string
  state?: string
  country?: string
  area?: string
  address?: string
  billing_street?: string
  contactName?: string
  contactNo?: string
  email?: string
  website?: string
  assignedTo?: string
  industry?: string
  status?: string
  createdAt?: string
  updatedAt?: string
  lastActivity?: string
  customerSegment?: string
  accountType?: string
  accountIndustry?: string
  subIndustry?: string
  industries?: IndustryPair[]
  primaryContactName?: string
}

const industries = ["All", "Biotech Company", "Dealer", "Educational Institutions", "Food and Beverages", "Hair Transplant Clinics/ Hospitals", "Molecular Diagnostics", "Pharmaceutical", "Research", "SRO", "Training Institute", "Universities"]
const statuses = ["All", "Active", "Inactive"]
const countries = ["All", "India", "USA", "UK", "Canada", "Australia", "Germany", "France", "Japan", "Singapore", "UAE", "Other"]

export function AccountsContent() {
  const router = useRouter()
  const { toast } = useToast()
  const [accounts, setAccounts] = useState<Account[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedCountry, setSelectedCountry] = useState("All")
  const [selectedIndustry, setSelectedIndustry] = useState("All")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [isImportModalOpen, setIsImportModalOpen] = useState(false)
  const [isAddAccountOpen, setIsAddAccountOpen] = useState(false)
  const [isEditAccountOpen, setIsEditAccountOpen] = useState(false)
  const [editingAccount, setEditingAccount] = useState<Account | null>(null)
  const [isImporting, setIsImporting] = useState(false)
  const [importProgress, setImportProgress] = useState({ current: 0, total: 0 })
  const [isSaving, setIsSaving] = useState(false)
  const [deletingAccountId, setDeletingAccountId] = useState<string | null>(null)
  const [isLoadingStats, setIsLoadingStats] = useState(true)
  const [accountStats, setAccountStats] = useState({
    total: 0,
    active: 0,
    newThisMonth: 0,
    activeRegions: 0,
    mostActiveIndustry: 'N/A',
    mostActiveIndustryCount: 0,
    mostActiveCountry: 'N/A',
    mostActiveCountryCount: 0
  })

  // Form state for add/edit
  const [formData, setFormData] = useState<Partial<Account>>({
    accountName: "",
    industry: "",
    contactName: "",
    contactNo: "",
    email: "",
    website: "",
    address: "",
    city: "",
    state: "",
    country: "",
    assignedTo: "",
    status: "Active",
    industries: []
  })

  // Load accounts from Supabase API
  useEffect(() => {
    loadAccountsFromAPI()
  }, [])

  // Reload accounts when the page becomes visible (user switches back to tab)
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (!document.hidden) {
        loadAccountsFromAPI()
      }
    }

    document.addEventListener('visibilitychange', handleVisibilityChange)
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange)
    }
  }, [])

  const loadAccountsFromAPI = async () => {
    try {
      console.log("Loading all accounts from database...")
      
      // Fetch ALL accounts from API without any filtering
      const response = await fetch(`/api/accounts?limit=1000&_t=${Date.now()}`)
      if (!response.ok) {
        throw new Error(`Failed to fetch accounts: ${response.status}`)
      }
      
      const data = await response.json()
      console.log("API Response:", data)
      console.log("Number of accounts from API:", data.accounts?.length || 0)
      console.log("Total count from API:", data.total)
      
      // Map API data to component format
      const mappedAccounts = (data.accounts || []).map((account: any) => {
        console.log('Full account data from API:', account)

        // Handle industries for distributors
        let industries = account.industries || []

        // If distributor but industries array is empty, check if old-style data exists
        if (account.account_type === 'Distributor' && industries.length === 0 && account.acct_industry) {
          // Convert old-style comma-separated or single value to industries array
          const industryStr = account.acct_industry || ''
          const subIndustryStr = account.acct_sub_industry || ''

          if (industryStr) {
            // Check if comma-separated (multiple) or single value
            const industryList = industryStr.includes(',')
              ? industryStr.split(',').map((s: string) => s.trim()).filter((s: string) => s)
              : [industryStr.trim()].filter((s: string) => s)

            const subIndustryList = subIndustryStr.includes(',')
              ? subIndustryStr.split(',').map((s: string) => s.trim()).filter((s: string) => s)
              : [subIndustryStr.trim()].filter((s: string) => s)

            // Create industry pairs
            industries = industryList.map((ind: string, idx: number) => ({
              industry: ind,
              subIndustry: subIndustryList[idx] || subIndustryList[0] || ''
            }))

            console.log('Converted old-style distributor data to industries array:', industries)
          }
        }

        return {
          id: account.id,
          accountName: account.account_name,
          city: account.billing_city,
          state: account.billing_state,
          country: account.billing_country || account.country || 'India',
          address: account.address,
          billing_street: account.billing_street,
          contactName: account.contact_name || '',
          contactNo: account.main_phone || account.phone || account.contact_phone || account.billing_phone,
          email: account.email || account.contact_email,
          website: account.website,
          assignedTo: account.owner?.full_name || account.assigned_to || '',
          industry: account.industry,
          status: account.status || 'Active',
          createdAt: account.created_at,
          customerSegment: account.customer_segment || account.acct_customer_segment,
          accountType: account.account_type,
          accountIndustry: account.acct_industry,
          subIndustry: account.acct_sub_industry,
          industries: industries,
          primaryContactName: account.primary_contact_name || account.contact_name || ''
        }
      })
      
      console.log("Mapped accounts:", mappedAccounts)
      setAccounts(mappedAccounts)
      
      // Calculate stats from API data
      const activeAccounts = mappedAccounts.filter((acc: any) => acc.status === 'Active').length
      const uniqueCountries = [...new Set(mappedAccounts.filter((acc: any) => acc.country).map((acc: any) => acc.country))].length
      
      // Calculate new accounts this month
      const currentDate = new Date()
      const currentMonth = currentDate.getMonth()
      const currentYear = currentDate.getFullYear()
      
      const newThisMonth = mappedAccounts.filter((acc: any) => {
        if (!acc.createdAt) return false
        const createdDate = new Date(acc.createdAt)
        return createdDate.getMonth() === currentMonth && createdDate.getFullYear() === currentYear
      }).length

      // Calculate most active industry - check both industry fields
      const industryCount = mappedAccounts.reduce((acc: any, account: any) => {
        const industryValue = account.accountIndustry || account.industry
        if (industryValue && industryValue.trim()) {
          acc[industryValue] = (acc[industryValue] || 0) + 1
        }
        return acc
      }, {})

      const mostActiveIndustry = Object.keys(industryCount).length > 0
        ? Object.keys(industryCount).reduce((a, b) => industryCount[a] > industryCount[b] ? a : b)
        : 'N/A'
      const mostActiveIndustryCount = industryCount[mostActiveIndustry] || 0

      // Calculate most active country
      const countryCount = mappedAccounts.reduce((acc: any, account: any) => {
        if (account.country && account.country.trim()) {
          acc[account.country] = (acc[account.country] || 0) + 1
        }
        return acc
      }, {})
      
      const mostActiveCountry = Object.keys(countryCount).length > 0 
        ? Object.keys(countryCount).reduce((a, b) => countryCount[a] > countryCount[b] ? a : b)
        : 'N/A'
      const mostActiveCountryCount = countryCount[mostActiveCountry] || 0
      
      setAccountStats({
        total: data.total || mappedAccounts.length,
        active: activeAccounts,
        newThisMonth: newThisMonth,
        activeRegions: uniqueCountries,
        mostActiveIndustry: mostActiveIndustry,
        mostActiveIndustryCount: mostActiveIndustryCount,
        mostActiveCountry: mostActiveCountry,
        mostActiveCountryCount: mostActiveCountryCount
      })
      
      setIsLoadingStats(false)
      
    } catch (error) {
      console.error("Error loading accounts:", error)
      toast({
        title: "Error",
        description: "Failed to load accounts from database",
        variant: "destructive"
      })
    }
  }

  // Debug: Log current accounts state
  useEffect(() => {
    console.log("Current accounts state:", accounts)
  }, [accounts])

  // Filter accounts based on search and filters
  const filteredAccounts = accounts.filter(account => {
    const matchesSearch = 
      account.accountName?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      account.contactName?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      account.city?.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesCountry = selectedCountry === "All" || account.country === selectedCountry
    const matchesIndustry = selectedIndustry === "All" || account.industry === selectedIndustry
    const matchesStatus = selectedStatus === "All" || account.status === selectedStatus

    return matchesSearch && matchesCountry && matchesIndustry && matchesStatus
  })

  const handleAddAccount = () => {
    // Navigate to the dynamic add account page
    router.push('/accounts/add')
  }

  const handleEditAccount = (account: Account) => {
    // Navigate to the dynamic edit account page
    router.push(`/accounts/edit/${account.id}`)
  }

  const handleSaveAccount = async () => {
    console.log("handleSaveAccount called with formData:", formData)
    
    if (!formData.accountName) {
      toast({
        title: "Error",
        description: "Account name is required",
        variant: "destructive"
      })
      return
    }

    setIsSaving(true)

    try {
      // Get current user's company ID with fallback
      const storedUser = localStorage.getItem('user')
      let companyId = localStorage.getItem('currentCompanyId') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      let userId = null
      
      if (storedUser) {
        try {
          const user = JSON.parse(storedUser)
          if (user.company_id) {
            companyId = user.company_id
          }
          userId = user.id
        } catch (e) {
          console.log("Error parsing user data, using default company ID")
        }
      }

      if (editingAccount) {
        // Update existing account
        console.log("Updating existing account...")
        const response = await fetch('/api/accounts', {
          method: 'PUT',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            id: editingAccount.id,
            account_name: formData.accountName,
            industry: formData.industry,
            phone: formData.contactNo,
            email: formData.email,
            website: formData.website,
            billing_street: formData.address,
            billing_city: formData.city,
            billing_state: formData.state,
            billing_country: formData.country || 'India',
            status: formData.status || 'Active',
            contact_name: formData.contactName,
            assigned_to: formData.assignedTo,
            industries: formData.industries || [],
            owner_id: userId
          })
        })
        
        if (response.ok) {
          toast({
            title: "Account updated",
            description: "The account has been successfully updated."
          })
          setIsEditAccountOpen(false)
          setEditingAccount(null)
          
          // Add slight delay to ensure database is updated, then reload and route
          setTimeout(async () => {
            await loadAccountsFromAPI()
            router.push('/accounts')
          }, 500)
        } else {
          throw new Error("Failed to update account")
        }
      } else {
        // Create new account
        console.log("Creating new account...")
        const response = await fetch('/api/accounts', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            companyId: companyId,
            account_name: formData.accountName,
            industry: formData.industry,
            phone: formData.contactNo,
            email: formData.email,
            website: formData.website,
            billing_street: formData.address,
            billing_city: formData.city,
            billing_state: formData.state,
            billing_country: formData.country || 'India',
            status: formData.status || 'Active',
            contact_name: formData.contactName,
            assigned_to: formData.assignedTo,
            industries: formData.industries || [],
            owner_id: userId
          })
        })
        
        if (response.ok) {
          toast({
            title: "Account created",
            description: "The account has been successfully created."
          })
          setIsAddAccountOpen(false)
          
          // Reset form
          setFormData({
            accountName: "",
            industry: "",
            contactName: "",
            contactNo: "",
            email: "",
            website: "",
            address: "",
            city: "",
            state: "",
            country: "",
            assignedTo: "",
            status: "Active",
            industries: []
          })
          
          // Add slight delay to ensure database is updated, then reload and route
          setTimeout(async () => {
            await loadAccountsFromAPI()
            router.push('/accounts')
          }, 500)
        } else {
          throw new Error("Failed to create account")
        }
      }
      
    } catch (error) {
      console.error("Error saving account:", error)
      toast({
        title: "Error",
        description: editingAccount ? "Failed to update account" : "Failed to create account",
        variant: "destructive"
      })
    } finally {
      setIsSaving(false)
    }
  }

  const handleImportData = async (data: any[]) => {
    try {
      // Set loading state
      setIsImporting(true)
      setImportProgress({ current: 0, total: data.length })

      // Get current user's company ID
      const storedUser = localStorage.getItem('user')
      if (!storedUser) {
        toast({
          title: "Error",
          description: "User not found. Please login again.",
          variant: "destructive"
        })
        setIsImporting(false)
        return
      }

      const user = JSON.parse(storedUser)
      const companyId = user.company_id
      if (!companyId) {
        toast({
          title: "Error",
          description: "Company not found. Please login again.",
          variant: "destructive"
        })
        setIsImporting(false)
        return
      }

      console.log("Starting import of", data.length, "records")
      console.log("Sample data item:", data[0])

      // Show initial toast
      toast({
        title: "Import started",
        description: `Processing ${data.length} records...`
      })

      // The data comes already mapped from field_name, so we can use it directly
      const accountsToImport = data.map(item => {
        // Check if this is a distributor
        const isDistributor = item.account_type === 'Distributor' || item.accountType === 'Distributor'

        // Start with the base item
        const account: any = { owner_id: user.id }

        // Copy all fields EXCEPT the industry fields (we'll handle them specially)
        Object.keys(item).forEach(key => {
          if (key !== 'acct_industry' && key !== 'accountIndustry' &&
              key !== 'acct_sub_industry' && key !== 'subIndustry') {
            account[key] = item[key]
          }
        })

        // Handle industries based on account type
        if (isDistributor) {
          // For distributors: Convert comma-separated to industries array
          const industriesStr = item.acct_industry || item.accountIndustry || ''
          const subIndustriesStr = item.acct_sub_industry || item.subIndustry || ''

          console.log('Processing distributor:', item.account_name, 'industries:', industriesStr, 'subIndustries:', subIndustriesStr)

          if (industriesStr && subIndustriesStr) {
            const industries = industriesStr.split(',').map((s: string) => s.trim()).filter((s: string) => s)
            const subIndustries = subIndustriesStr.split(',').map((s: string) => s.trim()).filter((s: string) => s)

            // Create industry-subindustry pairs
            const industriesPairs = industries.map((industry: string, index: number) => ({
              industry,
              subIndustry: subIndustries[index] || subIndustries[0] || ''
            }))

            account.industries = industriesPairs
            console.log('Created industries pairs for distributor:', industriesPairs)
          } else {
            account.industries = []
          }

          // DO NOT include acct_industry and acct_sub_industry for distributors
          // They will be set to NULL by the API
        } else {
          // For non-distributors: Use single values only (first value if comma-separated)
          const industriesStr = item.acct_industry || item.accountIndustry || ''
          const subIndustriesStr = item.acct_sub_industry || item.subIndustry || ''

          // If comma-separated, take only the first value
          const singleIndustry = industriesStr.includes(',')
            ? industriesStr.split(',')[0].trim()
            : industriesStr
          const singleSubIndustry = subIndustriesStr.includes(',')
            ? subIndustriesStr.split(',')[0].trim()
            : subIndustriesStr

          if (singleIndustry) {
            account.acct_industry = singleIndustry
          }
          if (singleSubIndustry) {
            account.acct_sub_industry = singleSubIndustry
          }

          console.log('Processing non-distributor:', item.account_name, 'industry:', singleIndustry, 'subIndustry:', singleSubIndustry)
        }

        return account
      })
      
      let successCount = 0
      let failCount = 0
      
      console.log("Processed accounts for import:", accountsToImport.length)
      
      // Import accounts one by one with progress updates
      for (let i = 0; i < accountsToImport.length; i++) {
        const account = accountsToImport[i]
        
        // Update progress
        setImportProgress({ current: i + 1, total: accountsToImport.length })
        
        // Skip if no account name
        const accountName = account.account_name || account.accountName
        if (!accountName || accountName.trim() === '') {
          console.log(`Skipping row ${i + 2} with no account name:`, account)
          failCount++
          continue
        }

        try {
          console.log("Importing account:", accountName, "Full data:", JSON.stringify(account, null, 2))

          const response = await fetch('/api/accounts', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              companyId: companyId,
              userId: user.id,
              ...account
            })
          })

          if (response.ok) {
            console.log("Successfully imported:", account.account_name)
            successCount++
          } else {
            const errorText = await response.text()
            console.log("Failed to import:", account.account_name, "Status:", response.status, "Error:", errorText)

            // Check if it's a duplicate error
            if (response.status === 400 && errorText.includes('already exists')) {
              console.log("Skipping duplicate:", account.account_name)
              // Consider this a "success" since the record exists
              successCount++
            } else {
              failCount++
            }
          }
        } catch (error) {
          console.error('Error importing account:', account.account_name, error)
          failCount++
        }
        
        // Small delay to show progress
        await new Promise(resolve => setTimeout(resolve, 50))
      }
      
      // Wait a moment for database to sync, then reload accounts from API
      setTimeout(async () => {
        await loadAccountsFromAPI()
        console.log("Accounts reloaded after import")
      }, 1000)
      
      const totalRecords = data.length
      const duplicateCount = successCount - (totalRecords - failCount - (successCount + failCount))
      
      console.log(`Import Summary:`)
      console.log(`- Total records in file: ${totalRecords}`)
      console.log(`- Successfully imported: ${successCount}`)
      console.log(`- Failed/Skipped: ${failCount}`)
      console.log(`- Processing rate: ${Math.round((successCount / totalRecords) * 100)}%`)
      
      // Clear loading state and close modal
      setIsImporting(false)
      setImportProgress({ current: 0, total: 0 })
      setIsImportModalOpen(false)
      
      toast({
        title: "Import completed",
        description: `Processed ${successCount}/${totalRecords} accounts successfully. ${failCount > 0 ? `Skipped: ${failCount} (likely duplicates or empty names)` : ''}`
      })
      
    } catch (error) {
      console.error('Import error:', error)
      // Clear loading state on error
      setIsImporting(false)
      setImportProgress({ current: 0, total: 0 })
      
      toast({
        title: "Import failed",
        description: "An error occurred while importing accounts.",
        variant: "destructive"
      })
    }
  }

  const handleExport = async () => {
    try {
      console.log('Export button clicked, accounts data:', accounts)

      // Prepare data for export - handle multiple industries for distributors
      const exportData = accounts.map(account => {
        if (account.accountType === 'Distributor' && account.industries && account.industries.length > 0) {
          return {
            ...account,
            accountIndustry: account.industries.map((pair: IndustryPair) => pair.industry).join(', '),
            subIndustry: account.industries.map((pair: IndustryPair) => pair.subIndustry).join(', ')
          }
        }
        return account
      })

      const success = await exportToExcel(exportData, {
        filename: `accounts_${new Date().toISOString().split('T')[0]}`,
        sheetName: 'Accounts',
        columns: [
          { key: 'accountName', label: 'Company Name', width: 20 },
          { key: 'customerSegment', label: 'Customer Segment', width: 18 },
          { key: 'accountType', label: 'Account Type', width: 18 },
          { key: 'accountIndustry', label: 'Industry', width: 30 },
          { key: 'subIndustry', label: 'Sub-Industry', width: 30 },
          { key: 'city', label: 'City', width: 15 },
          { key: 'state', label: 'State', width: 15 },
          { key: 'country', label: 'Country', width: 15 },
          { key: 'contactNo', label: 'Contact Phone', width: 15 },
          { key: 'email', label: 'Email', width: 25 },
          { key: 'website', label: 'Website', width: 25 },
          { key: 'address', label: 'Address', width: 30 },
          { key: 'status', label: 'Status', width: 12 },
          { key: 'createdAt', label: 'Created Date', width: 15 }
        ]
      })
      
      if (success) {
        toast({
          title: "Data exported",
          description: "Accounts data has been exported to Excel file."
        })
      } else {
        toast({
          title: "Export failed",
          description: "Failed to export accounts data. Please try again.",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Export error:', error)
      toast({
        title: "Export failed",
        description: "Failed to export accounts data. Please try again.",
        variant: "destructive"
      })
    }
  }

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "active":
        return "bg-green-100 text-green-800"
      case "inactive":
        return "bg-red-100 text-red-800"
      case "prospect":
        return "bg-blue-100 text-blue-800"
      case "dormant":
        return "bg-gray-100 text-gray-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const handleDeleteAccount = async (accountId: string, accountName: string) => {
    console.log('=== handleDeleteAccount called ===')
    console.log('Account ID:', accountId)
    console.log('Account Name:', accountName)

    if (!confirm(`Are you sure you want to delete "${accountName}"? This action cannot be undone.`)) {
      console.log('Delete cancelled by user')
      return
    }

    setDeletingAccountId(accountId)

    try {
      const user = localStorage.getItem('user')
      if (!user) {
        console.error('User not found in localStorage')
        toast({
          title: "Error",
          description: "User not found. Please login again.",
          variant: "destructive"
        })
        setDeletingAccountId(null)
        return
      }

      const parsedUser = JSON.parse(user)
      const companyId = parsedUser.company_id

      console.log('Making DELETE request to:', `/api/accounts?id=${accountId}&companyId=${companyId}`)

      const response = await fetch(`/api/accounts?id=${accountId}&companyId=${companyId}`, {
        method: 'DELETE'
      })

      console.log('DELETE response status:', response.status)

      if (!response.ok) {
        const errorData = await response.json()
        console.error('DELETE response error:', errorData)
        throw new Error(errorData.error || 'Failed to delete account')
      }

      const result = await response.json()
      console.log('DELETE response data:', result)

      toast({
        title: "Account deleted",
        description: `"${accountName}" has been deleted successfully.`
      })

      // Reload accounts
      console.log('Reloading accounts...')
      await loadAccountsFromAPI()
    } catch (error) {
      console.error('Error deleting account:', error)
      toast({
        title: "Delete failed",
        description: error instanceof Error ? error.message : "Failed to delete account. Please try again.",
        variant: "destructive"
      })
    } finally {
      setDeletingAccountId(null)
    }
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Accounts Management</h1>
          <p className="text-gray-600">Manage your customer accounts and relationships</p>
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
          <Button onClick={handleAddAccount} className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Add Account
          </Button>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-blue-100">
                <Building2 className="w-6 h-6 text-blue-600" />
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold">
                  {isLoadingStats ? (
                    <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
                  ) : (
                    accountStats.total
                  )}
                </div>
                <p className="text-xs text-muted-foreground">Total Accounts</p>
                <p className="text-xs text-green-600">+{accountStats.newThisMonth} this month</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-green-100">
                <CheckCircle className="w-6 h-6 text-green-600" />
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold">
                  {isLoadingStats ? (
                    <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
                  ) : (
                    accountStats.active
                  )}
                </div>
                <p className="text-xs text-muted-foreground">Active Accounts</p>
                <p className="text-xs text-gray-600">{Math.round((accountStats.active / accountStats.total) * 100) || 0}% of total</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-purple-100">
                <Factory className="w-6 h-6 text-purple-600" />
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold">
                  {isLoadingStats ? (
                    <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
                  ) : (
                    accountStats.mostActiveIndustryCount
                  )}
                </div>
                <p className="text-xs text-muted-foreground">Active Industry</p>
                <p className="text-xs font-medium text-gray-700">{accountStats.mostActiveIndustry}</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center">
              <div className="p-2 rounded-lg bg-orange-100">
                <Globe className="w-6 h-6 text-orange-600" />
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold">
                  {isLoadingStats ? (
                    <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
                  ) : (
                    accountStats.mostActiveCountryCount
                  )}
                </div>
                <p className="text-xs text-muted-foreground">Active Countries</p>
                <p className="text-xs font-medium text-gray-700">{accountStats.mostActiveCountry}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Account Search & Filters</CardTitle>
          <CardDescription>Filter and search through your accounts</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col md:flex-row gap-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <Input
                  placeholder="Search accounts by name, contact, or city..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
            <Select value={selectedCountry} onValueChange={setSelectedCountry}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Country" />
              </SelectTrigger>
              <SelectContent>
                {countries.map((country) => (
                  <SelectItem key={country} value={country}>
                    {country}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedIndustry} onValueChange={setSelectedIndustry}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Industry" />
              </SelectTrigger>
              <SelectContent>
                {industries.map((industry) => (
                  <SelectItem key={industry} value={industry}>
                    {industry}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedStatus} onValueChange={setSelectedStatus}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Status" />
              </SelectTrigger>
              <SelectContent>
                {statuses.map((status) => (
                  <SelectItem key={status} value={status}>
                    {status}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Accounts List</CardTitle>
              <CardDescription>Latest accounts and their current status</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredAccounts.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                No accounts found. Click "Add Account" to create your first account.
              </div>
            ) : (
              filteredAccounts.map((account) => {
                // Build location string combining city, state, and country
                const locationParts = []
                if (account.city) locationParts.push(account.city)
                if (account.state) locationParts.push(account.state)
                if (account.country) locationParts.push(account.country)
                const location = locationParts.join(', ') || "—"
                
                return (
                  <div
                    key={account.id}
                    className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                  >
                    <div className="flex items-center space-x-4">
                      <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                        <Building2 className="w-6 h-6 text-blue-600" />
                      </div>
                      <div>
                        <div className="flex items-center space-x-2">
                          <h3 className="font-semibold">{account.accountName}</h3>
                          <span className="px-2 py-1 text-xs rounded-full bg-green-100 text-green-800">
                            {account.status || 'Active'}
                          </span>
                        </div>
                        <p className="text-sm text-gray-600">{location}</p>
                        {account.contactName && (
                          <p className="text-xs text-gray-500 mt-1">Contact: {account.contactName}</p>
                        )}
                      </div>
                    </div>

                    {/* Center section - Business & Contact details in 3 columns */}
                    <div className="flex-1 grid grid-cols-3 gap-x-6 gap-y-1 px-6">
                      {/* Column 1: Segment & Type */}
                      <div className="space-y-1">
                        {account.customerSegment && (
                          <div className="text-xs">
                            <span className="text-gray-500">Segment:</span>
                            <span className="ml-2 text-gray-900">{account.customerSegment}</span>
                          </div>
                        )}
                        {account.accountType && (
                          <div className="text-xs">
                            <span className="text-gray-500">Type:</span>
                            <span className="ml-2 text-gray-900">{account.accountType}</span>
                          </div>
                        )}
                      </div>

                      {/* Column 2: Industry & Sub-Industry */}
                      <div className="space-y-1">
                        {/* Show multiple industries for distributors */}
                        {account.accountType === 'Distributor' && account.industries && account.industries.length > 0 ? (
                          <>
                            <div className="text-xs">
                              <span className="text-gray-500">Industries:</span>
                              <span className="ml-2 text-gray-900">
                                {account.industries.map((pair: IndustryPair) => pair.industry).join(', ')}
                              </span>
                            </div>
                            <div className="text-xs">
                              <span className="text-gray-500">Sub-Industries:</span>
                              <span className="ml-2 text-gray-900">
                                {account.industries.map((pair: IndustryPair) => pair.subIndustry).join(', ')}
                              </span>
                            </div>
                          </>
                        ) : (
                          <>
                            {account.accountIndustry && (
                              <div className="text-xs">
                                <span className="text-gray-500">Industry:</span>
                                <span className="ml-2 text-gray-900">{account.accountIndustry}</span>
                              </div>
                            )}
                            {account.subIndustry && (
                              <div className="text-xs">
                                <span className="text-gray-500">Sub-Industry:</span>
                                <span className="ml-2 text-gray-900">{account.subIndustry}</span>
                              </div>
                            )}
                          </>
                        )}
                      </div>

                      {/* Column 3: Contact Information */}
                      <div className="space-y-1">
                        {account.primaryContactName && (
                          <div className="text-xs">
                            <span className="text-gray-500">Contact:</span>
                            <span className="ml-2 text-gray-900 font-medium">{account.primaryContactName}</span>
                          </div>
                        )}
                        {account.contactNo && (
                          <div className="text-xs flex items-center">
                            <Phone className="w-3 h-3 mr-1 text-gray-500" />
                            <span className="text-gray-900">{account.contactNo}</span>
                          </div>
                        )}
                        {account.email && (
                          <div className="text-xs flex items-center">
                            <Mail className="w-3 h-3 mr-1 text-gray-500" />
                            <span className="text-gray-900">{account.email}</span>
                          </div>
                        )}
                        {account.website && (
                          <div className="text-xs flex items-center">
                            <Globe className="w-3 h-3 mr-1 text-gray-500" />
                            <span className="text-blue-600">{account.website}</span>
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Right section - actions */}
                    <div className="flex items-center space-x-4">
                      <div className="flex space-x-1">
                        {/* Communication Buttons */}
                        {account.contactNo && (
                          <Button 
                            variant="ghost" 
                            size="sm"
                            onClick={() => window.open(`tel:${account.contactNo}`, '_self')}
                            className="text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                            title="Call Contact"
                          >
                            <Phone className="w-4 h-4" />
                          </Button>
                        )}
                        {account.contactNo && (
                          <Button 
                            variant="ghost" 
                            size="sm"
                            onClick={() => window.open(`https://wa.me/${account.contactNo.replace(/[^\d]/g, '')}`, '_blank')}
                            className="text-green-600 hover:text-green-800 hover:bg-green-50"
                            title="WhatsApp Contact"
                          >
                            <WhatsAppIcon className="w-4 h-4" />
                          </Button>
                        )}
                        {account.email && (
                          <Button 
                            variant="ghost" 
                            size="sm"
                            onClick={() => window.open(`mailto:${account.email}`, '_self')}
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
                          title="Edit Account"
                          onClick={() => handleEditAccount(account)}
                        >
                          <Edit className="w-4 h-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          title="Delete Account"
                          onClick={() => handleDeleteAccount(account.id, account.accountName)}
                          disabled={deletingAccountId === account.id}
                          className="text-red-600 hover:text-red-700 hover:bg-red-50"
                        >
                          {deletingAccountId === account.id ? (
                            <div className="w-4 h-4 border-2 border-red-600 border-t-transparent rounded-full animate-spin" />
                          ) : (
                            <Trash2 className="w-4 h-4" />
                          )}
                        </Button>
                      </div>
                    </div>
                  </div>
                )
              })
            )}
          </div>
        </CardContent>
      </Card>

      {/* Add Account Dialog */}
      <Dialog open={isAddAccountOpen} onOpenChange={setIsAddAccountOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Add New Account</DialogTitle>
          </DialogHeader>
          <div className="grid grid-cols-2 gap-4 py-4">
            <div>
              <Label htmlFor="accountName">Account Name *</Label>
              <Input
                id="accountName"
                value={formData.accountName}
                onChange={(e) => setFormData({...formData, accountName: e.target.value})}
                placeholder="Enter account name"
              />
            </div>
            <div>
              <Label htmlFor="industry">Industry</Label>
              <Select value={formData.industry} onValueChange={(value) => setFormData({...formData, industry: value})}>
                <SelectTrigger>
                  <SelectValue placeholder="Select industry" />
                </SelectTrigger>
                <SelectContent>
                  {industries.filter(i => i !== "All").map((industry) => (
                    <SelectItem key={industry} value={industry}>{industry}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label htmlFor="contactName">Contact Name</Label>
              <Input
                id="contactName"
                value={formData.contactName}
                onChange={(e) => setFormData({...formData, contactName: e.target.value})}
                placeholder="Contact person name"
              />
            </div>
            <div>
              <Label htmlFor="contactNo">Contact Phone</Label>
              <Input
                id="contactNo"
                value={formData.contactNo}
                onChange={(e) => setFormData({...formData, contactNo: e.target.value})}
                placeholder="+91 98765 43210"
              />
            </div>
            <div>
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                value={formData.email}
                onChange={(e) => setFormData({...formData, email: e.target.value})}
                placeholder="email@company.com"
              />
            </div>
            <div>
              <Label htmlFor="website">Website</Label>
              <Input
                id="website"
                value={formData.website}
                onChange={(e) => setFormData({...formData, website: e.target.value})}
                placeholder="www.company.com"
              />
            </div>
          </div>
          
          <div>
            <Label htmlFor="address">Address</Label>
            <Textarea
              id="address"
              value={formData.address}
              onChange={(e) => setFormData({...formData, address: e.target.value})}
              placeholder="Enter complete address"
              rows={3}
            />
          </div>
          
          <div className="grid grid-cols-3 gap-4">
            <div>
              <Label htmlFor="city">City</Label>
              <Input
                id="city"
                value={formData.city}
                onChange={(e) => setFormData({...formData, city: e.target.value})}
                placeholder="City"
              />
            </div>
            <div>
              <Label htmlFor="state">State</Label>
              <Input
                id="state"
                value={formData.state}
                onChange={(e) => setFormData({...formData, state: e.target.value})}
                placeholder="State"
              />
            </div>
            <div>
              <Label htmlFor="country">Country</Label>
              <Select value={formData.country} onValueChange={(value) => setFormData({...formData, country: value})}>
                <SelectTrigger>
                  <SelectValue placeholder="Select country" />
                </SelectTrigger>
                <SelectContent>
                  {countries.filter(c => c !== "All").map((country) => (
                    <SelectItem key={country} value={country}>{country}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
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
            <Button variant="outline" onClick={() => setIsAddAccountOpen(false)}>
              <X className="w-4 h-4 mr-2" />
              Cancel
            </Button>
            <Button onClick={handleSaveAccount} disabled={isSaving} className="bg-blue-600 hover:bg-blue-700">
              {isSaving ? (
                <>
                  <div className="w-4 h-4 mr-2 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  Saving...
                </>
              ) : (
                <>
                  <Save className="w-4 h-4 mr-2" />
                  Save Account
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Account Dialog */}
      <Dialog open={isEditAccountOpen} onOpenChange={setIsEditAccountOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Edit Account</DialogTitle>
          </DialogHeader>
          <div className="grid grid-cols-2 gap-4 py-4">
            <div>
              <Label htmlFor="edit-accountName">Account Name *</Label>
              <Input
                id="edit-accountName"
                value={formData.accountName}
                onChange={(e) => setFormData({...formData, accountName: e.target.value})}
                placeholder="Enter account name"
              />
            </div>
            <div>
              <Label htmlFor="edit-industry">Industry</Label>
              <Select value={formData.industry} onValueChange={(value) => setFormData({...formData, industry: value})}>
                <SelectTrigger>
                  <SelectValue placeholder="Select industry" />
                </SelectTrigger>
                <SelectContent>
                  {industries.filter(i => i !== "All").map((industry) => (
                    <SelectItem key={industry} value={industry}>{industry}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label htmlFor="edit-contactName">Contact Name</Label>
              <Input
                id="edit-contactName"
                value={formData.contactName}
                onChange={(e) => setFormData({...formData, contactName: e.target.value})}
                placeholder="Contact person name"
              />
            </div>
            <div>
              <Label htmlFor="edit-contactNo">Contact Phone</Label>
              <Input
                id="edit-contactNo"
                value={formData.contactNo}
                onChange={(e) => setFormData({...formData, contactNo: e.target.value})}
                placeholder="+91 98765 43210"
              />
            </div>
            <div>
              <Label htmlFor="edit-email">Email</Label>
              <Input
                id="edit-email"
                type="email"
                value={formData.email}
                onChange={(e) => setFormData({...formData, email: e.target.value})}
                placeholder="email@company.com"
              />
            </div>
            <div>
              <Label htmlFor="edit-website">Website</Label>
              <Input
                id="edit-website"
                value={formData.website}
                onChange={(e) => setFormData({...formData, website: e.target.value})}
                placeholder="www.company.com"
              />
            </div>
          </div>
          
          <div>
            <Label htmlFor="edit-address">Address</Label>
            <Textarea
              id="edit-address"
              value={formData.address}
              onChange={(e) => setFormData({...formData, address: e.target.value})}
              placeholder="Enter complete address"
              rows={3}
            />
          </div>
          
          <div className="grid grid-cols-3 gap-4">
            <div>
              <Label htmlFor="edit-city">City</Label>
              <Input
                id="edit-city"
                value={formData.city}
                onChange={(e) => setFormData({...formData, city: e.target.value})}
                placeholder="City"
              />
            </div>
            <div>
              <Label htmlFor="edit-state">State</Label>
              <Input
                id="edit-state"
                value={formData.state}
                onChange={(e) => setFormData({...formData, state: e.target.value})}
                placeholder="State"
              />
            </div>
            <div>
              <Label htmlFor="edit-country">Country</Label>
              <Select value={formData.country} onValueChange={(value) => setFormData({...formData, country: value})}>
                <SelectTrigger>
                  <SelectValue placeholder="Select country" />
                </SelectTrigger>
                <SelectContent>
                  {countries.filter(c => c !== "All").map((country) => (
                    <SelectItem key={country} value={country}>{country}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
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
                  <SelectValue placeholder="Select status" />
                </SelectTrigger>
                <SelectContent>
                  {statuses.filter(s => s !== "All").map((status) => (
                    <SelectItem key={status} value={status}>{status}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsEditAccountOpen(false)}>
              <X className="w-4 h-4 mr-2" />
              Cancel
            </Button>
            <Button onClick={handleSaveAccount} disabled={isSaving} className="bg-blue-600 hover:bg-blue-700">
              {isSaving ? (
                <>
                  <div className="w-4 h-4 mr-2 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  {editingAccount ? 'Updating...' : 'Saving...'}
                </>
              ) : (
                <>
                  <Save className="w-4 h-4 mr-2" />
                  {editingAccount ? 'Update Account' : 'Save Account'}
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Dynamic Import Modal */}
      <DynamicImportModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        onImport={handleImportData}
        moduleType="accounts"
        isImporting={isImporting}
        importProgress={importProgress}
      />
    </div>
  )
}