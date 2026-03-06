"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Plus, Search, Edit, Mail, Phone, User, Users, Building2, Globe, Download, Upload, Trash2 } from "lucide-react"
import { exportToExcel } from "@/lib/excel-export"
import { DynamicImportModal } from "@/components/dynamic-import-modal"

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
  </svg>
)
import { useToast } from "@/hooks/use-toast"

interface Contact {
  id: string
  contact_id?: string
  first_name: string
  last_name: string
  email_primary: string
  phone_mobile: string
  company_name?: string
  job_title?: string
  lifecycle_stage: string
  current_contact_status: string
  created_at?: string
  account?: {
    account_name: string
    customer_segment?: string
    account_type?: string
    acct_industry?: string
    acct_sub_industry?: string
    billing_city?: string
    billing_state?: string
    billing_country?: string
    main_phone?: string
    primary_email?: string
    website?: string
  }
}

export function ContactsContent() {
  const router = useRouter()
  const { toast } = useToast()
  const [contacts, setContacts] = useState<Contact[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [loading, setLoading] = useState(true)
  const [isImportModalOpen, setIsImportModalOpen] = useState(false)
  const [isImporting, setIsImporting] = useState(false)
  const [importProgress, setImportProgress] = useState({ current: 0, total: 0 })
  const [deletingContactId, setDeletingContactId] = useState<string | null>(null)
  const [stats, setStats] = useState({
    total: 0,
    active: 0,
    leads: 0,
    customers: 0
  })

  useEffect(() => {
    fetchContacts()
  }, [])

  const fetchContacts = async () => {
    setLoading(true)
    try {
      const user = localStorage.getItem('user')
      if (!user) {
        router.push('/login')
        return
      }

      const parsedUser = JSON.parse(user)
      const response = await fetch(`/api/contacts?companyId=${parsedUser.company_id}&limit=1000`)

      if (response.ok) {
        const data = await response.json()
        setContacts(data.contacts || [])

        // Calculate stats
        const activeContacts = data.contacts.filter((c: Contact) => c.current_contact_status === 'Active')
        const leads = data.contacts.filter((c: Contact) => c.lifecycle_stage?.includes('Lead'))
        const customers = data.contacts.filter((c: Contact) => c.lifecycle_stage === 'Customer')

        setStats({
          total: data.contacts.length,
          active: activeContacts.length,
          leads: leads.length,
          customers: customers.length
        })
      } else {
        toast({
          title: "Error",
          description: "Failed to load contacts",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error fetching contacts:', error)
      toast({
        title: "Error",
        description: "Failed to load contacts",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleEditContact = (contactId: string) => {
    router.push(`/contacts/edit/${contactId}`)
  }

  const handleDeleteContact = async (contactId: string, contactName: string) => {
    if (!confirm(`Are you sure you want to delete "${contactName}"? This action cannot be undone.`)) {
      return
    }

    setDeletingContactId(contactId)

    try {
      const user = localStorage.getItem('user')
      if (!user) {
        toast({
          title: "Error",
          description: "User not found. Please login again.",
          variant: "destructive"
        })
        setDeletingContactId(null)
        return
      }

      const parsedUser = JSON.parse(user)
      const companyId = parsedUser.company_id

      const response = await fetch(`/api/contacts/${contactId}?companyId=${companyId}`, {
        method: 'DELETE'
      })

      if (!response.ok) {
        throw new Error('Failed to delete contact')
      }

      toast({
        title: "Contact deleted",
        description: `"${contactName}" has been deleted successfully.`
      })

      // Reload contacts
      await fetchContacts()
    } catch (error) {
      console.error('Error deleting contact:', error)
      toast({
        title: "Delete failed",
        description: "Failed to delete contact. Please try again.",
        variant: "destructive"
      })
    } finally {
      setDeletingContactId(null)
    }
  }

  const handleExport = async () => {
    try {
      const success = await exportToExcel(contacts, {
        filename: `contacts_${new Date().toISOString().split('T')[0]}`,
        sheetName: 'Contacts',
        columns: [
          { key: 'first_name', label: 'First Name', width: 15 },
          { key: 'last_name', label: 'Last Name', width: 15 },
          { key: 'email_primary', label: 'Email', width: 25 },
          { key: 'phone_mobile', label: 'Mobile Phone', width: 15 },
          { key: 'job_title', label: 'Job Title', width: 20 },
          { key: 'company_name', label: 'Company Name', width: 20 },
          { key: 'lifecycle_stage', label: 'Lifecycle Stage', width: 15 },
          { key: 'current_contact_status', label: 'Status', width: 12 },
          { key: 'created_at', label: 'Created Date', width: 15 }
        ]
      })

      if (success) {
        toast({
          title: "Data exported",
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

  const handleImportData = async (data: any[]) => {
    try {
      setIsImporting(true)
      setImportProgress({ current: 0, total: data.length })

      const user = localStorage.getItem('user')
      if (!user) {
        toast({
          title: "Error",
          description: "User not found. Please login again.",
          variant: "destructive"
        })
        setIsImporting(false)
        return
      }

      const parsedUser = JSON.parse(user)
      const companyId = parsedUser.company_id

      toast({
        title: "Import started",
        description: `Processing ${data.length} records...`
      })

      let successCount = 0
      let failCount = 0
      const errors: string[] = []

      for (let i = 0; i < data.length; i++) {
        const item = data[i]
        setImportProgress({ current: i + 1, total: data.length })

        const rowNumber = i + 2 // Excel row number (accounting for header)
        const contactIdentifier = item.first_name || item.email_primary || `Row ${rowNumber}`

        // Validate required fields (assuming first_name and email_primary are required)
        if (!item.first_name || !item.email_primary) {
          console.log(`Skipping row ${rowNumber} - missing required fields`)
          failCount++
          errors.push(`Row ${rowNumber} (${contactIdentifier}): Missing required fields (First Name or Email)`)
          continue
        }

        // If company_name is provided, look up the account_id
        let accountId = null
        if (item.company_name) {
          try {
            const accountResponse = await fetch(`/api/accounts?companyId=${companyId}&limit=1000`)
            if (accountResponse.ok) {
              const accountsData = await accountResponse.json()
              const matchingAccount = accountsData.accounts?.find(
                (acc: any) => acc.account_name?.toLowerCase() === item.company_name.toLowerCase()
              )

              if (matchingAccount) {
                accountId = matchingAccount.id
                console.log(`Found account "${item.company_name}" with ID: ${accountId}`)
              } else {
                failCount++
                errors.push(`Row ${rowNumber} (${contactIdentifier}): Account "${item.company_name}" not found. Please create the account first or check the spelling.`)
                continue
              }
            }
          } catch (error) {
            console.error('Error looking up account:', error)
            failCount++
            errors.push(`Row ${rowNumber} (${contactIdentifier}): Failed to lookup account "${item.company_name}"`)
            continue
          }
        }

        // Build contact data with account_id if found
        const contactData = {
          ...item,
          companyId: companyId, // API expects 'companyId' not 'company_id'
          userId: parsedUser.id,
          accountId: accountId // This will be passed to the API
        }

        console.log(`Importing contact for row ${rowNumber}:`, {
          name: `${item.first_name} ${item.last_name}`,
          email: item.email_primary,
          company_name: item.company_name,
          accountId: accountId
        })

        try {
          const response = await fetch('/api/contacts', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify(contactData)
          })

          if (response.ok) {
            successCount++
          } else {
            const errorData = await response.json()
            const errorMessage = errorData.message || errorData.error || 'Unknown error'

            if (response.status === 400 && errorMessage.toLowerCase().includes('already exists')) {
              // For duplicate emails, show as warning but don't fail
              errors.push(`Row ${rowNumber} (${contactData.email_primary}): Email already exists - skipped`)
              failCount++
            } else {
              failCount++
              errors.push(`Row ${rowNumber} (${contactIdentifier}): ${errorMessage}`)
            }
            console.error(`Error importing row ${rowNumber}:`, errorData)
          }
        } catch (error) {
          console.error('Error importing contact:', error)
          failCount++
          errors.push(`Row ${rowNumber} (${contactIdentifier}): Failed to import - ${error instanceof Error ? error.message : 'Unknown error'}`)
        }

        await new Promise(resolve => setTimeout(resolve, 50))
      }

      setTimeout(async () => {
        await fetchContacts()
      }, 1000)

      setIsImporting(false)
      setImportProgress({ current: 0, total: 0 })
      setIsImportModalOpen(false)

      // Show detailed error messages
      console.log('Import completed. Success:', successCount, 'Failed:', failCount, 'Errors:', errors)

      if (errors.length > 0) {
        // Create a more readable error message
        const errorList = errors.slice(0, 3).map((err, idx) => `${idx + 1}. ${err}`).join(' | ')
        const moreErrors = errors.length > 3 ? ` (${errors.length - 3} more errors)` : ''

        toast({
          title: failCount === data.length ? "Import failed" : "Import completed with errors",
          description: `Success: ${successCount}/${data.length} | Failed: ${failCount}. ${errorList}${moreErrors}`,
          variant: failCount === data.length ? "destructive" : "default",
          duration: 15000 // Show for 15 seconds so user can read errors
        })

        // Also log all errors to console for debugging
        console.log('=== IMPORT ERRORS ===')
        errors.forEach((err, idx) => {
          console.log(`${idx + 1}. ${err}`)
        })
        console.log('=====================')
      } else {
        toast({
          title: "Import completed",
          description: `Successfully imported ${successCount}/${data.length} contacts.`
        })
      }

    } catch (error) {
      console.error('Import error:', error)
      setIsImporting(false)
      setImportProgress({ current: 0, total: 0 })

      toast({
        title: "Import failed",
        description: "An error occurred while importing contacts.",
        variant: "destructive"
      })
    }
  }

  const filteredContacts = contacts.filter(contact =>
    contact.first_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
    contact.last_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
    contact.email_primary?.toLowerCase().includes(searchTerm.toLowerCase()) ||
    contact.company_name?.toLowerCase().includes(searchTerm.toLowerCase())
  )

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading contacts...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Contacts</h1>
          <p className="text-gray-600 mt-1">Manage your contacts and relationships</p>
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
          <Button onClick={() => router.push('/contacts/add')} className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Add New Contact
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-600">Total Contacts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <span className="text-2xl font-bold text-gray-900">{stats.total}</span>
              <Users className="w-8 h-8 text-blue-600" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-600">Active Contacts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <span className="text-2xl font-bold text-green-600">{stats.active}</span>
              <User className="w-8 h-8 text-green-600" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-600">Leads</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <span className="text-2xl font-bold text-orange-600">{stats.leads}</span>
              <Building2 className="w-8 h-8 text-orange-600" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-gray-600">Customers</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <span className="text-2xl font-bold text-purple-600">{stats.customers}</span>
              <Users className="w-8 h-8 text-purple-600" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Search */}
      <Card>
        <CardHeader>
          <div className="flex items-center space-x-2">
            <Search className="w-5 h-5 text-gray-400" />
            <Input
              placeholder="Search contacts by name, email, or company..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="flex-1"
            />
          </div>
        </CardHeader>
      </Card>

      {/* Contacts List */}
      <Card>
        <CardHeader>
          <CardTitle>Contacts List</CardTitle>
          <CardDescription>
            {filteredContacts.length} contact{filteredContacts.length !== 1 ? 's' : ''} found
          </CardDescription>
        </CardHeader>
        <CardContent>
          {filteredContacts.length === 0 ? (
            <div className="text-center py-12">
              <Users className="w-16 h-16 mx-auto text-gray-400 mb-4" />
              <h3 className="text-lg font-semibold text-gray-900 mb-2">No contacts found</h3>
              <p className="text-gray-600 mb-4">
                {searchTerm ? 'Try adjusting your search' : 'Get started by adding your first contact'}
              </p>
              {!searchTerm && (
                <Button onClick={() => router.push('/contacts/add')} className="bg-blue-600 hover:bg-blue-700">
                  <Plus className="w-4 h-4 mr-2" />
                  Add Contact
                </Button>
              )}
            </div>
          ) : (
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
                        <h3 className="font-semibold">
                          {contact.first_name} {contact.last_name}
                        </h3>
                        <span className={`px-2 py-1 text-xs rounded-full ${
                          contact.current_contact_status === 'Active' ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'
                        }`}>
                          {contact.current_contact_status}
                        </span>
                      </div>
                      <p className="text-sm text-gray-600">{contact.account?.account_name || contact.company_name || 'No account'}</p>
                      {contact.job_title && (
                        <p className="text-xs text-gray-500 mt-1">
                          {contact.job_title}
                        </p>
                      )}
                    </div>
                  </div>

                  {/* Center section - Account & Contact details in 3 columns */}
                  <div className="flex-1 grid grid-cols-3 gap-x-6 gap-y-1 px-6">
                    {/* Column 1: Account Business Info */}
                    <div className="space-y-1">
                      {contact.account?.customer_segment && (
                        <div className="text-xs">
                          <span className="text-gray-500">Segment:</span>
                          <span className="ml-2 text-gray-900">{contact.account.customer_segment}</span>
                        </div>
                      )}
                      {contact.account?.account_type && (
                        <div className="text-xs">
                          <span className="text-gray-500">Type:</span>
                          <span className="ml-2 text-gray-900">{contact.account.account_type}</span>
                        </div>
                      )}
                      {contact.lifecycle_stage && (
                        <div className="text-xs">
                          <span className="text-gray-500">Stage:</span>
                          <span className="ml-2 text-gray-900">{contact.lifecycle_stage}</span>
                        </div>
                      )}
                    </div>

                    {/* Column 2: Account Industry & Location */}
                    <div className="space-y-1">
                      {contact.account?.acct_industry && (
                        <div className="text-xs">
                          <span className="text-gray-500">Industry:</span>
                          <span className="ml-2 text-gray-900">{contact.account.acct_industry}</span>
                        </div>
                      )}
                      {contact.account?.acct_sub_industry && (
                        <div className="text-xs">
                          <span className="text-gray-500">Sub-Industry:</span>
                          <span className="ml-2 text-gray-900">{contact.account.acct_sub_industry}</span>
                        </div>
                      )}
                      {(contact.account?.billing_city || contact.account?.billing_state || contact.account?.billing_country) && (
                        <div className="text-xs">
                          <span className="text-gray-500">Location:</span>
                          <span className="ml-2 text-gray-900">
                            {[contact.account.billing_city, contact.account.billing_state, contact.account.billing_country].filter(Boolean).join(', ')}
                          </span>
                        </div>
                      )}
                    </div>

                    {/* Column 3: Contact Information */}
                    <div className="space-y-1">
                      {contact.email_primary && (
                        <div className="text-xs flex items-center">
                          <Mail className="w-3 h-3 mr-1 text-gray-500" />
                          <span className="text-gray-900">{contact.email_primary}</span>
                        </div>
                      )}
                      {contact.phone_mobile && (
                        <div className="text-xs flex items-center">
                          <Phone className="w-3 h-3 mr-1 text-gray-500" />
                          <span className="text-gray-900">{contact.phone_mobile}</span>
                        </div>
                      )}
                      {contact.account?.website && (
                        <div className="text-xs flex items-center">
                          <Building2 className="w-3 h-3 mr-1 text-gray-500" />
                          <span className="text-blue-600">{contact.account.website}</span>
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Right section - Actions */}
                  <div className="flex items-center space-x-2">
                    {/* Communication Buttons */}
                    {contact.phone_mobile && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => window.open(`tel:${contact.phone_mobile}`, '_self')}
                        className="text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                        title="Call Contact"
                      >
                        <Phone className="w-4 h-4" />
                      </Button>
                    )}
                    {contact.phone_mobile && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => window.open(`https://wa.me/${contact.phone_mobile.replace(/[^\d]/g, '')}`, '_blank')}
                        className="text-green-600 hover:text-green-800 hover:bg-green-50"
                        title="WhatsApp Contact"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                    )}
                    {contact.email_primary && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => window.open(`mailto:${contact.email_primary}`, '_self')}
                        className="text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                        title="Send Email"
                      >
                        <Mail className="w-4 h-4" />
                      </Button>
                    )}

                    {/* Edit Button */}
                    <Button
                      variant="ghost"
                      size="sm"
                      title="Edit Contact"
                      onClick={() => handleEditContact(contact.id)}
                    >
                      <Edit className="w-4 h-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      title="Delete Contact"
                      onClick={() => handleDeleteContact(contact.id, `${contact.first_name} ${contact.last_name}`)}
                      disabled={deletingContactId === contact.id}
                      className="text-red-600 hover:text-red-700 hover:bg-red-50"
                    >
                      {deletingContactId === contact.id ? (
                        <div className="w-4 h-4 border-2 border-red-600 border-t-transparent rounded-full animate-spin" />
                      ) : (
                        <Trash2 className="w-4 h-4" />
                      )}
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Dynamic Import Modal */}
      <DynamicImportModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        onImport={handleImportData}
        moduleType="contacts"
        isImporting={isImporting}
        importProgress={importProgress}
      />
    </div>
  )
}
