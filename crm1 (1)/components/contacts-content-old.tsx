"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Contact, Users, Phone, Mail, MapPin, Building2, Search, Filter, Plus, Edit, MoreHorizontal, Star, Target, TrendingUp, Calendar, Upload, Download, Save, X, Eye } from "lucide-react"
import { exportToExcel, formatDateForExcel } from "@/lib/excel-export"
import { useToast } from "@/hooks/use-toast"
import storageService from "@/lib/localStorage-service"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { AddContactModal } from "@/components/add-contact-modal"
import { DataImportModal } from "@/components/data-import-modal"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
  </svg>
)

export function ContactsContent() {
  const { toast } = useToast()
  const [contactsList, setContactsList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedTab, setSelectedTab] = useState("all")
  const [isAddContactModalOpen, setIsAddContactModalOpen] = useState(false)
  const [isImportModalOpen, setIsImportModalOpen] = useState(false)
  const [editingContact, setEditingContact] = useState<any>(null)
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [statusFilter, setStatusFilter] = useState("all")
  const [priorityFilter, setPriorityFilter] = useState("all")
  const [contactsStats, setContactsStatsState] = useState({
    total: 0,
    active: 0,
    newThisMonth: 0,
    highValue: 0
  })

  // Load contacts from localStorage on component mount
  useEffect(() => {
    loadContacts()
  }, [])

  const loadContacts = () => {
    const storedContacts = storageService.getAll<any>('contacts')
    console.log("loadContacts - storedContacts:", storedContacts)
    setContactsList(storedContacts)
    
    // Calculate stats
    const stats = storageService.getStats('contacts')
    const activeContacts = storedContacts.filter(contact => contact.status === 'active').length
    const highValueContacts = storedContacts.filter(contact => {
      const value = parseFloat(contact.dealValue?.replace(/[₹,]/g, '') || '0')
      return value > 500000 // High value = > 5L
    }).length
    
    setContactsStatsState({
      total: stats.total,
      active: activeContacts,
      newThisMonth: stats.thisMonthCount,
      highValue: highValueContacts
    })
  }

  const contactsStatsDisplay = [
    {
      title: "Total Contacts",
      value: contactsStats.total.toString(),
      change: `+${contactsStats.newThisMonth} this month`,
      icon: Contact,
      color: "text-blue-600",
      bgColor: "bg-blue-50",
    },
    {
      title: "Active Contacts",
      value: contactsStats.active.toString(),
      change: `${Math.round((contactsStats.active / Math.max(contactsStats.total, 1)) * 100)}% of total`, 
      icon: Users,
      color: "text-green-600",
      bgColor: "bg-green-50",
    },
    {
      title: "New This Month",
      value: contactsStats.newThisMonth.toString(),
      change: "Recent additions",
      icon: TrendingUp,
      color: "text-purple-600",
      bgColor: "bg-purple-50",
    },
    {
      title: "High Value",
      value: contactsStats.highValue.toString(),
      change: "> ₹5L deals",
      icon: Star,
      color: "text-yellow-600",
      bgColor: "bg-yellow-50",
    },
  ]

  const contacts = [
    {
      id: 1,
      name: "Dr. Priya Sharma",
      title: "Research Director",
      company: "Kerala Agricultural University",
      department: "Research & Development",
      phone: "+91 98765 43210",
      email: "priya.sharma@kau.in",
      whatsapp: "+91 98765 43210",
      location: "Thrissur, Kerala",
      status: "active",
      lastContact: "2 days ago",
      dealValue: "₹8,50,000",
      tags: ["Decision Maker", "Research"],
      priority: "high",
      avatar: "PS"
    },
    {
      id: 2,
      name: "Mr. Rajesh Kumar", 
      title: "Lab Manager",
      company: "Eurofins Advinus",
      department: "Laboratory Operations",
      phone: "+91 90123 45678",
      email: "rajesh.k@eurofins.com",
      whatsapp: "+91 90123 45678",
      location: "Bangalore, Karnataka",
      status: "active",
      lastContact: "1 week ago",
      dealValue: "₹12,75,000",
      tags: ["Hot Lead", "Procurement"],
      priority: "high",
      avatar: "RK"
    },
    {
      id: 3,
      name: "Ms. Anjali Menon",
      title: "Purchase Head",
      company: "Thermo Fisher Scientific",
      department: "Procurement",
      phone: "+91 87654 32109",
      email: "anjali.menon@thermofisher.com",
      whatsapp: "+91 87654 32109",
      location: "Chennai, Tamil Nadu",
      status: "active",
      lastContact: "3 days ago",
      dealValue: "₹6,25,000",
      tags: ["Negotiating", "Instruments"],
      priority: "medium",
      avatar: "AM"
    },
    {
      id: 4,
      name: "Dr. Anu Rang",
      title: "Senior Scientist",
      company: "JNCASR",
      department: "Materials Science",
      phone: "+91 91234 56789",
      email: "anu.rang@jncasr.ac.in",
      whatsapp: "+91 91234 56789",
      location: "Bangalore, Karnataka",
      status: "active",
      lastContact: "5 days ago",
      dealValue: "₹4,50,000",
      tags: ["Research", "Academia"],
      priority: "medium",
      avatar: "AR"
    },
    {
      id: 5,
      name: "Mr. Sanjay Patel",
      title: "Operations Manager",
      company: "Bio-Rad Laboratories",
      department: "Operations",
      phone: "+91 98876 54321",
      email: "sanjay.patel@biorad.com",
      whatsapp: "+91 98876 54321",
      location: "Mumbai, Maharashtra",
      status: "inactive",
      lastContact: "2 weeks ago",
      dealValue: "₹9,80,000",
      tags: ["Contract Pending", "Follow-up"],
      priority: "low",
      avatar: "SP"
    },
    {
      id: 6,
      name: "Ms. Pauline D'Souza",
      title: "Quality Head",
      company: "Guna Foods",
      department: "Quality Control",
      phone: "+91 95432 10987",
      email: "pauline@gunafoods.com",
      whatsapp: "+91 95432 10987",
      location: "Goa",
      status: "active",
      lastContact: "4 days ago",
      dealValue: "₹3,25,000",
      tags: ["Food Testing", "Quality"],
      priority: "medium",
      avatar: "PD"
    }
  ]

  const getStatusBadge = (status: string) => {
    const variants = {
      active: "bg-green-100 text-green-800",
      inactive: "bg-red-100 text-red-800",
      pending: "bg-yellow-100 text-yellow-800"
    }
    return variants[status as keyof typeof variants] || "bg-gray-100 text-gray-800"
  }

  const getPriorityBadge = (priority: string) => {
    const variants = {
      high: "bg-red-100 text-red-800",
      medium: "bg-yellow-100 text-yellow-800", 
      low: "bg-gray-100 text-gray-800"
    }
    return variants[priority as keyof typeof variants] || "bg-gray-100 text-gray-800"
  }

  const filteredContacts = contactsList.filter(contact => {
    const matchesSearch = contact.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         contact.company?.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         contact.email?.toLowerCase().includes(searchTerm.toLowerCase())
    
    const matchesStatus = statusFilter === "all" || contact.status === statusFilter
    const matchesPriority = priorityFilter === "all" || contact.priority === priorityFilter
    
    let matchesTab = true
    if (selectedTab === "active") matchesTab = contact.status === "active"
    if (selectedTab === "high-priority") matchesTab = contact.priority === "high"
    if (selectedTab === "recent") matchesTab = ["2 days ago", "3 days ago", "4 days ago", "5 days ago"].includes(contact.lastContact)
    
    return matchesSearch && matchesStatus && matchesPriority && matchesTab
  })

  const handleSaveContact = (contactData: any) => {
    console.log("handleSaveContact called with:", contactData)
    
    const newContact = storageService.create('contacts', contactData)
    console.log("New contact created:", newContact)
    
    if (newContact) {
      // Force immediate refresh from localStorage
      const refreshedContacts = storageService.getAll<any>('contacts')
      console.log("Force refresh - all contacts:", refreshedContacts)
      setContactsList(refreshedContacts)
      
      // Update stats
      loadContacts()
      
      toast({
        title: "Contact created",
        description: `Contact ${newContact.name || newContact.id} has been successfully created.`
      })
      setIsAddContactModalOpen(false)
    } else {
      console.error("Failed to create contact")
      toast({
        title: "Error",
        description: "Failed to create contact",
        variant: "destructive"
      })
    }
  }

  const handleImportData = (importedContacts: any[]) => {
    console.log('Imported contacts:', importedContacts)
    const createdContacts = storageService.createMany('contacts', importedContacts)
    
    // Immediately add imported contacts to state
    setContactsList(prevContacts => [...prevContacts, ...createdContacts])
    
    // Update stats
    loadContacts()
    
    toast({
      title: "Data imported",
      description: `Successfully imported ${createdContacts.length} contacts.`
    })
  }

  const handleExport = async () => {
    try {
      const success = await exportToExcel(contactsList, {
        filename: `contacts_${new Date().toISOString().split('T')[0]}`,
        sheetName: 'Contacts',
        columns: [
          { key: 'firstName', label: 'First Name', width: 15 },
          { key: 'lastName', label: 'Last Name', width: 15 },
          { key: 'email', label: 'Email', width: 25 },
          { key: 'phone', label: 'Phone', width: 15 },
          { key: 'jobTitle', label: 'Job Title', width: 18 },
          { key: 'company', label: 'Company', width: 20 },
          { key: 'location', label: 'Location', width: 20 },
          { key: 'leadSource', label: 'Lead Source', width: 15 },
          { key: 'status', label: 'Status', width: 12 },
          { key: 'assignedTo', label: 'Assigned To', width: 15 },
          { key: 'tags', label: 'Tags', width: 20 },
          { key: 'dateAdded', label: 'Date Added', width: 15 }
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

  const handleEditContact = (contact: any) => {
    setEditingContact(contact)
    setIsEditModalOpen(true)
  }

  const handleSaveEditContact = (updatedContact: any) => {
    console.log('Updating contact:', updatedContact)
    setIsEditModalOpen(false)
    setEditingContact(null)
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Contacts</h1>
          <p className="text-gray-500 mt-1">Manage and track all your customer contacts</p>
        </div>
        <div className="flex items-center space-x-3">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm">
                <Filter className="w-4 h-4 mr-2" />
                Filter
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-48">
              <DropdownMenuLabel>Filter by Status</DropdownMenuLabel>
              <DropdownMenuItem onClick={() => setStatusFilter("all")}>
                All Status
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setStatusFilter("active")}>
                Active
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setStatusFilter("inactive")}>
                Inactive
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuLabel>Filter by Priority</DropdownMenuLabel>
              <DropdownMenuItem onClick={() => setPriorityFilter("all")}>
                All Priority
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setPriorityFilter("high")}>
                High Priority
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setPriorityFilter("medium")}>
                Medium Priority
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setPriorityFilter("low")}>
                Low Priority
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
          <Button variant="outline" size="sm" onClick={handleExport}>
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button variant="outline" size="sm" onClick={() => setIsImportModalOpen(true)}>
            <Upload className="w-4 h-4 mr-2" />
            Import Data
          </Button>
          <Button 
            className="bg-blue-600 hover:bg-blue-700" 
            size="sm"
            onClick={() => setIsAddContactModalOpen(true)}
          >
            <Plus className="w-4 h-4 mr-2" />
            Add Contact
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {contactsStatsDisplay.map((stat, index) => (
          <Card key={index}>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-gray-600">{stat.title}</p>
                  <p className="text-2xl font-bold text-gray-900">{stat.value}</p>
                  <p className="text-xs text-green-600 mt-1">{stat.change} vs last month</p>
                </div>
                <div className={`w-12 h-12 ${stat.bgColor} rounded-lg flex items-center justify-center`}>
                  <stat.icon className={`w-6 h-6 ${stat.color}`} />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Search and Tabs */}
      <div className="space-y-4">
        <div className="flex items-center space-x-4">
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
            <Input
              placeholder="Search contacts by name, company, or email..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10"
            />
          </div>
        </div>

        <Tabs defaultValue="all" className="w-full" onValueChange={setSelectedTab}>
          <TabsList className="grid w-full grid-cols-4 max-w-md">
            <TabsTrigger value="all">All Contacts</TabsTrigger>
            <TabsTrigger value="active">Active</TabsTrigger>
            <TabsTrigger value="high-priority">High Priority</TabsTrigger>
            <TabsTrigger value="recent">Recent</TabsTrigger>
          </TabsList>

          <TabsContent value="all" className="space-y-4">
            <ContactsTable contacts={filteredContacts} onEditContact={handleEditContact} />
          </TabsContent>
          <TabsContent value="active" className="space-y-4">
            <ContactsTable contacts={filteredContacts} onEditContact={handleEditContact} />
          </TabsContent>
          <TabsContent value="high-priority" className="space-y-4">
            <ContactsTable contacts={filteredContacts} onEditContact={handleEditContact} />
          </TabsContent>
          <TabsContent value="recent" className="space-y-4">
            <ContactsTable contacts={filteredContacts} onEditContact={handleEditContact} />
          </TabsContent>
        </Tabs>
      </div>

      {/* Add Contact Modal */}
      <AddContactModal
        isOpen={isAddContactModalOpen}
        onClose={() => setIsAddContactModalOpen(false)}
        onSave={handleSaveContact}
      />

      {/* Import Modal */}
      <DataImportModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        moduleType="contacts"
        onImport={handleImportData}
      />

      {/* Edit Contact Modal */}
      {editingContact && (
        <Dialog open={isEditModalOpen} onOpenChange={setIsEditModalOpen}>
          <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle className="flex items-center space-x-2">
                <Edit className="w-5 h-5 text-blue-600" />
                <span>Edit Contact: {editingContact.name}</span>
              </DialogTitle>
            </DialogHeader>
            
            <div className="space-y-6 py-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="name">Full Name</Label>
                  <Input
                    id="name"
                    defaultValue={editingContact.name}
                    placeholder="Contact name"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="title">Job Title</Label>
                  <Input
                    id="title"
                    defaultValue={editingContact.title}
                    placeholder="Job title"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="company">Company</Label>
                  <Input
                    id="company"
                    defaultValue={editingContact.company}
                    placeholder="Company name"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="location">Location</Label>
                  <Input
                    id="location"
                    defaultValue={editingContact.location}
                    placeholder="City, State"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="phone">Phone Number</Label>
                  <Input
                    id="phone"
                    defaultValue={editingContact.phone}
                    placeholder="+91 98765 43210"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="email">Email Address</Label>
                  <Input
                    id="email"
                    type="email"
                    defaultValue={editingContact.email}
                    placeholder="email@example.com"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="priority">Priority Level</Label>
                  <Select defaultValue={editingContact.priority}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="high">High</SelectItem>
                      <SelectItem value="medium">Medium</SelectItem>
                      <SelectItem value="low">Low</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="status">Contact Status</Label>
                  <Select defaultValue={editingContact.status}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="active">Active</SelectItem>
                      <SelectItem value="inactive">Inactive</SelectItem>
                      <SelectItem value="pending">Pending</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="dealValue">Deal Value</Label>
                <Input
                  id="dealValue"
                  defaultValue={editingContact.dealValue}
                  placeholder="₹5,00,000"
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="tags">Tags</Label>
                <div className="flex flex-wrap gap-2">
                  {editingContact.tags?.map((tag: string, idx: number) => (
                    <Badge key={idx} variant="secondary">
                      {tag}
                    </Badge>
                  ))}
                </div>
              </div>
            </div>

            <DialogFooter className="flex justify-between">
              <Button
                variant="outline"
                onClick={() => setIsEditModalOpen(false)}
              >
                <X className="w-4 h-4 mr-2" />
                Cancel
              </Button>
              <Button
                onClick={() => handleSaveEditContact(editingContact)}
                className="bg-blue-600 hover:bg-blue-700"
              >
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

function ContactsTable({ contacts, onEditContact }: { contacts: any[], onEditContact: (contact: any) => void }) {
  const getStatusBadge = (status: string) => {
    const variants = {
      active: "bg-green-100 text-green-800",
      inactive: "bg-red-100 text-red-800",
      pending: "bg-yellow-100 text-yellow-800"
    }
    return variants[status as keyof typeof variants] || "bg-gray-100 text-gray-800"
  }

  const getPriorityBadge = (priority: string) => {
    const variants = {
      high: "bg-red-100 text-red-800",
      medium: "bg-yellow-100 text-yellow-800", 
      low: "bg-gray-100 text-gray-800"
    }
    return variants[priority as keyof typeof variants] || "bg-gray-100 text-gray-800"
  }

  const handleCommunicationAction = (type: string, contact: any) => {
    switch (type) {
      case 'call':
        if (contact.phone) {
          window.open(`tel:${contact.phone}`, '_self')
        }
        break
      case 'email':
        if (contact.email) {
          const subject = `Follow-up: ${contact.company} Discussion`
          const body = `Dear ${contact.name.split(' ')[0]},\n\nI hope this email finds you well. I wanted to follow up on our recent discussion.\n\nBest regards,\nSales Team`
          window.open(`mailto:${contact.email}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`, '_self')
        }
        break
      case 'whatsapp':
        if (contact.whatsapp) {
          const message = `Hello ${contact.name.split(' ')[0]}! I'm reaching out regarding our discussion about laboratory equipment for ${contact.company}. Could we schedule a time to discuss your requirements?`
          window.open(`https://wa.me/${contact.whatsapp.replace(/[^0-9]/g, '')}?text=${encodeURIComponent(message)}`, '_blank')
        }
        break
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Contact List</CardTitle>
        <CardDescription>Complete overview of your contact database</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[240px]">Contact</TableHead>
                <TableHead className="w-[220px]">Company & Title</TableHead>
                <TableHead className="w-[150px]">Department</TableHead>
                <TableHead className="w-[180px]">Contact Info</TableHead>
                <TableHead className="w-[100px]">Status</TableHead>
                <TableHead className="w-[100px]">Priority</TableHead>
                <TableHead className="w-[110px]">Deal Value</TableHead>
                <TableHead className="w-[140px]">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {contacts.map((contact) => (
                <TableRow key={contact.id} className="hover:bg-gray-50">
                  <TableCell className="py-4">
                    <div className="flex items-center space-x-3">
                      <div className="w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center text-blue-600 font-medium text-sm">
                        {contact.avatar}
                      </div>
                      <div>
                        <p className="font-medium text-gray-900">{contact.name}</p>
                        <div className="flex flex-wrap gap-1 mt-1">
                          {contact.tags.slice(0, 1).map((tag, idx) => (
                            <Badge key={idx} variant="outline" className="text-xs">
                              {tag}
                            </Badge>
                          ))}
                          {contact.tags.length > 1 && (
                            <Badge variant="outline" className="text-xs">
                              +{contact.tags.length - 1}
                            </Badge>
                          )}
                        </div>
                      </div>
                    </div>
                  </TableCell>
                  <TableCell className="py-4">
                    <div>
                      <p className="font-medium text-gray-900 text-sm">{contact.company}</p>
                      <p className="text-sm text-gray-500">{contact.title}</p>
                      <p className="text-xs text-gray-400 mt-1">{contact.location}</p>
                    </div>
                  </TableCell>
                  <TableCell className="py-4">
                    <p className="text-sm text-gray-700">{contact.department}</p>
                  </TableCell>
                  <TableCell className="py-4">
                    <div className="space-y-1">
                      <div className="flex items-center text-sm text-gray-600">
                        <Phone className="w-3 h-3 mr-2 text-blue-600" />
                        <span className="text-xs">{contact.phone}</span>
                      </div>
                      <div className="flex items-center text-sm text-gray-600">
                        <Mail className="w-3 h-3 mr-2 text-green-600" />
                        <span className="text-xs truncate max-w-[140px]">{contact.email}</span>
                      </div>
                    </div>
                  </TableCell>
                  <TableCell className="py-4">
                    <Badge className={`${getStatusBadge(contact.status)} text-xs`}>
                      {contact.status}
                    </Badge>
                  </TableCell>
                  <TableCell className="py-4">
                    <Badge className={`${getPriorityBadge(contact.priority)} text-xs`}>
                      {contact.priority}
                    </Badge>
                  </TableCell>
                  <TableCell className="py-4">
                    <span className="font-medium text-gray-900 text-sm">{contact.dealValue}</span>
                    <p className="text-xs text-gray-500">{contact.lastContact}</p>
                  </TableCell>
                  <TableCell className="py-4">
                    <div className="flex items-center space-x-1">
                      {/* Communication Actions */}
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => handleCommunicationAction('call', contact)}
                        className="h-8 w-8 p-0 text-blue-600 hover:text-blue-800 hover:bg-blue-50"
                        title="Call Contact"
                      >
                        <Phone className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => handleCommunicationAction('email', contact)}
                        className="h-8 w-8 p-0 text-orange-600 hover:text-orange-800 hover:bg-orange-50"
                        title="Send Email"
                      >
                        <Mail className="w-4 h-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => handleCommunicationAction('whatsapp', contact)}
                        className="h-8 w-8 p-0 text-green-600 hover:text-green-800 hover:bg-green-50"
                        title="WhatsApp Contact"
                      >
                        <WhatsAppIcon className="w-4 h-4" />
                      </Button>
                      
                      {/* Action Buttons */}
                      <Button 
                        variant="ghost" 
                        size="sm"
                        onClick={() => onEditContact(contact)}
                        className="h-8 w-8 p-0 text-gray-600 hover:text-gray-800 hover:bg-gray-100"
                        title="Edit Contact"
                      >
                        <Edit className="w-4 h-4" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  )
}