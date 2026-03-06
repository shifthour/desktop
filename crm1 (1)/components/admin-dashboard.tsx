"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
  Plus, Building2, Users, Shield, Edit, Trash2, Mail, User, 
  CheckCircle, XCircle, Clock, Settings, Eye, EyeOff, Copy, AlertTriangle
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"

interface Company {
  id: string
  name: string
  domain: string
  admin_email?: string
  max_users: number
  current_users: number
  subscription_status: 'active' | 'inactive' | 'suspended'
  created_at: string
  expiry_date?: string
}

interface User {
  id: string
  company_id: string
  email: string
  full_name: string
  role_id: string
  is_admin: boolean
  is_active: boolean
  created_at: string
  last_login: string | null
  role?: {
    name: string
    description: string
  }
  company?: {
    name: string
  }
}

interface Role {
  id: string
  name: string
  description: string
  permissions: string[]
}

export function AdminDashboard() {
  const { toast } = useToast()
  const [companies, setCompanies] = useState<Company[]>([])
  const [users, setUsers] = useState<User[]>([])
  const [roles, setRoles] = useState<Role[]>([])
  const [selectedCompany, setSelectedCompany] = useState<string>("")
  const [loading, setLoading] = useState(false)
  const [mounted, setMounted] = useState(false)
  const [isEditCompanyOpen, setIsEditCompanyOpen] = useState(false)
  const [editingCompany, setEditingCompany] = useState<Company | null>(null)
  const [currentUser, setCurrentUser] = useState<any>(null)

  // Get current user from localStorage
  useEffect(() => {
    if (mounted) {
      const storedUser = localStorage.getItem('user')
      if (storedUser) {
        const user = JSON.parse(storedUser)
        setCurrentUser(user)
        // Set the default tab based on user type
        if (user.is_super_admin) {
          setActiveTab('companies')
        } else if (user.is_admin && !user.is_super_admin) {
          setActiveTab('users')
        }
      }
    }
  }, [mounted])

  // Determine if user is super admin
  const isSuperAdmin = currentUser?.is_super_admin || false
  const isCompanyAdmin = currentUser?.is_admin && !currentUser?.is_super_admin

  // Company form state
  const [companyForm, setCompanyForm] = useState({
    name: "",
    adminName: "",
    adminEmail: "",
    adminPassword: "Admin@123",
    maxUsers: 5
  })
  
  const [showPassword, setShowPassword] = useState(false)

  // User form state
  const [userForm, setUserForm] = useState({
    email: "",
    fullName: "",
    roleId: "",
    password: "User@123",  // Default password for all new users
    isAdmin: false
  })
  
  const [showUserPassword, setShowUserPassword] = useState(false)

  const [isCreateCompanyOpen, setIsCreateCompanyOpen] = useState(false)
  const [isCreateUserOpen, setIsCreateUserOpen] = useState(false)
  const [isEditUserOpen, setIsEditUserOpen] = useState(false)
  const [editingUser, setEditingUser] = useState<User | null>(null)
  const [isDeleteCompanyOpen, setIsDeleteCompanyOpen] = useState(false)
  const [deletingCompany, setDeletingCompany] = useState<Company | null>(null)
  const [deleteConfirmText, setDeleteConfirmText] = useState("")
  const [activeTab, setActiveTab] = useState<string>("companies") // Default to companies
  
  useEffect(() => {
    setMounted(true)
  }, [])

  // Debug effect to track dialog state
  useEffect(() => {
    console.log('Dialog state changed - isDeleteCompanyOpen:', isDeleteCompanyOpen)
    console.log('Deleting company:', deletingCompany?.name || 'none')
  }, [isDeleteCompanyOpen, deletingCompany])

  // Load initial data only when component is mounted
  // (moved to mounted useEffect to prevent hydration issues)


  // Fixed default passwords
  const DEFAULT_ADMIN_PASSWORD = 'Admin@123'
  const DEFAULT_USER_PASSWORD = 'User@123'
  
  // Set default password when creating a new company
  useEffect(() => {
    if (isCreateCompanyOpen) {
      setCompanyForm(prev => ({...prev, adminPassword: DEFAULT_ADMIN_PASSWORD}))
    }
  }, [isCreateCompanyOpen])

  const loadCompanies = async () => {
    try {
      const response = await fetch('/api/admin/companies')
      if (response.ok) {
        const data = await response.json()
        setCompanies(data)
        if (data.length > 0 && !selectedCompany) {
          setSelectedCompany(data[0].id)
        }
      }
    } catch (error) {
      console.error('Error loading companies:', error)
      toast({
        title: "Error",
        description: "Failed to load companies",
        variant: "destructive"
      })
    }
  }

  const loadUsers = async (companyId: string) => {
    try {
      const response = await fetch(`/api/admin/users?companyId=${companyId}`)
      if (response.ok) {
        const data = await response.json()
        setUsers(data)
      }
    } catch (error) {
      console.error('Error loading users:', error)
      toast({
        title: "Error",
        description: "Failed to load users",
        variant: "destructive"
      })
    }
  }

  const loadRoles = async () => {
    try {
      const response = await fetch('/api/admin/roles')
      if (response.ok) {
        const data = await response.json()
        setRoles(data.filter((role: Role) => 
          !['super_admin'].includes(role.name) // Hide super admin role from selection
        ))
      }
    } catch (error) {
      console.error('Error loading roles:', error)
    }
  }


  const handleCreateCompany = async () => {
    if (!companyForm.name || !companyForm.adminName || !companyForm.adminEmail) {
      toast({
        title: "Error",
        description: "Please fill in company name, admin name, and admin email",
        variant: "destructive"
      })
      return
    }

    setLoading(true)
    try {
      const response = await fetch('/api/admin/companies', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(companyForm)
      })

      if (response.ok) {
        const newCompany = await response.json()
        setCompanies([newCompany, ...companies])
        setCompanyForm({ name: "", adminName: "", adminEmail: "", adminPassword: "Admin@123", maxUsers: 5 })
        setIsCreateCompanyOpen(false)
        
        // Trigger notification refresh immediately
        window.dispatchEvent(new CustomEvent('refreshNotifications'))
        
        // Refresh companies list to get updated data including admin_email
        setTimeout(() => {
          loadCompanies()
          setMounted(false) // Force re-render to show updated data
          setTimeout(() => setMounted(true), 100)
        }, 500)
        toast({
          title: "Success",
          description: "Company created successfully"
        })
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error,
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error creating company:', error)
      toast({
        title: "Error",
        description: "Failed to create company",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleCreateUser = async () => {
    const companyId = isSuperAdmin ? selectedCompany : currentUser?.company_id
    
    if (!userForm.email || !userForm.fullName || !userForm.roleId || !companyId) {
      toast({
        title: "Error",
        description: "Please fill in all required fields",
        variant: "destructive"
      })
      return
    }

    setLoading(true)
    try {
      const response = await fetch('/api/admin/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...userForm,
          companyId: companyId
        })
      })

      if (response.ok) {
        const newUser = await response.json()
        setUsers([newUser, ...users])
        setUserForm({ email: "", fullName: "", roleId: "", password: DEFAULT_USER_PASSWORD, isAdmin: false })
        setIsCreateUserOpen(false)
        
        // Trigger notification refresh immediately
        window.dispatchEvent(new CustomEvent('refreshNotifications'))
        
        if (isSuperAdmin) {
          loadCompanies() // Reload to update user counts for super admin
        }
        toast({
          title: "Success",
          description: "User created successfully"
        })
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error,
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error creating user:', error)
      toast({
        title: "Error",
        description: "Failed to create user",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleDeleteUser = async (userId: string) => {
    if (!confirm('Are you sure you want to delete this user?')) return

    setLoading(true)
    try {
      const response = await fetch(`/api/admin/users?userId=${userId}`, {
        method: 'DELETE'
      })

      if (response.ok) {
        setUsers(users.filter(u => u.id !== userId))
        
        // Trigger notification refresh immediately
        window.dispatchEvent(new CustomEvent('refreshNotifications'))
        
        if (isSuperAdmin) {
          loadCompanies() // Reload to update user counts for super admin
        }
        toast({
          title: "Success",
          description: "User deleted successfully"
        })
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error,
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error deleting user:', error)
      toast({
        title: "Error",
        description: "Failed to delete user",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleUpdateCompany = async () => {
    if (!editingCompany) return
    
    setLoading(true)
    try {
      const response = await fetch(`/api/admin/companies?companyId=${editingCompany.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companyId: editingCompany.id,
          name: editingCompany.name,
          adminEmail: editingCompany.admin_email,
          maxUsers: editingCompany.max_users
        })
      })

      if (response.ok) {
        const updatedCompany = await response.json()
        setCompanies(companies.map(c => c.id === editingCompany.id ? updatedCompany : c))
        setIsEditCompanyOpen(false)
        setEditingCompany(null)
        toast({
          title: "Success",
          description: "Company updated successfully"
        })
      } else {
        const errorData = await response.json()
        console.error('Update company API error:', response.status, errorData)
        toast({
          title: "Error",
          description: errorData.error || `Failed to update company (${response.status})`,
          variant: "destructive"
        })
      }
    } catch (error: any) {
      console.error('Update company error:', error)
      toast({
        title: "Error",
        description: error.message || "Failed to update company",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleDeleteCompany = (company: Company) => {
    console.log('Delete button clicked for company:', company.name)
    console.log('Setting dialog state...')
    setDeletingCompany(company)
    setDeleteConfirmText("")
    setIsDeleteCompanyOpen(true)
    console.log('Dialog should be open now, isDeleteCompanyOpen:', true)
  }

  const handleConfirmDeleteCompany = async () => {
    if (!deletingCompany || deleteConfirmText !== "Delete") {
      toast({
        title: "Error",
        description: "Please type 'Delete' exactly as shown to confirm",
        variant: "destructive"
      })
      return
    }

    setLoading(true)
    try {
      const response = await fetch(`/api/admin/companies?companyId=${deletingCompany.id}`, {
        method: 'DELETE'
      })

      if (response.ok) {
        setCompanies(companies.filter(c => c.id !== deletingCompany.id))
        setIsDeleteCompanyOpen(false)
        setDeletingCompany(null)
        setDeleteConfirmText("")
        
        // Trigger notification refresh immediately
        window.dispatchEvent(new CustomEvent('refreshNotifications'))
        toast({
          title: "Success",
          description: "Company deleted successfully"
        })
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error || "Failed to delete company",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Delete company error:', error)
      toast({
        title: "Error",
        description: "Failed to delete company",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleEditUser = (user: User) => {
    setEditingUser(user)
    setIsEditUserOpen(true)
  }

  const handleUpdateUser = async () => {
    if (!editingUser) return
    
    setLoading(true)
    try {
      const response = await fetch(`/api/admin/users?userId=${editingUser.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: editingUser.id,
          email: editingUser.email,
          fullName: editingUser.full_name,
          roleId: editingUser.role_id,
          isAdmin: editingUser.is_admin,
          isActive: editingUser.is_active
        })
      })

      if (response.ok) {
        const updatedUser = await response.json()
        setUsers(users.map(u => u.id === editingUser.id ? updatedUser : u))
        setIsEditUserOpen(false)
        setEditingUser(null)
        toast({
          title: "Success",
          description: "User updated successfully"
        })
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error || "Failed to update user",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Update user error:', error)
      toast({
        title: "Error",
        description: "Failed to update user",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'active':
        return <Badge className="bg-green-100 text-green-800">Active</Badge>
      case 'inactive':
        return <Badge className="bg-gray-100 text-gray-800">Inactive</Badge>
      case 'suspended':
        return <Badge className="bg-red-100 text-red-800">Suspended</Badge>
      default:
        return <Badge className="bg-gray-100 text-gray-800">{status}</Badge>
    }
  }

  const selectedCompanyData = companies.find(c => c.id === selectedCompany)


  // Auto-select company for company admins and load their users
  useEffect(() => {
    if (mounted && currentUser) {
      if (isSuperAdmin) {
        loadCompanies()
      } else if (isCompanyAdmin && currentUser.company_id) {
        // For company admins, auto-load their company users
        setSelectedCompany(currentUser.company_id)
        loadUsers(currentUser.company_id)
      }
      loadRoles()
    }
  }, [mounted, currentUser])

  // Load users when company is selected (for super admin)
  useEffect(() => {
    if (selectedCompany && isSuperAdmin) {
      loadUsers(selectedCompany)
    }
  }, [selectedCompany, isSuperAdmin])

  // Don't show anything until mounted to prevent hydration mismatch
  if (!mounted) {
    return null
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">
            {isSuperAdmin ? "Super Admin Dashboard" : "Company Admin Dashboard"}
          </h1>
          <p className="text-gray-600">
            {isSuperAdmin 
              ? "Manage companies and users for your CRM system" 
              : `Manage users and settings for ${currentUser?.company?.name || 'your company'}`
            }
          </p>
        </div>
        <div className="flex space-x-2">
          {isSuperAdmin && (
            <Dialog open={isCreateCompanyOpen} onOpenChange={setIsCreateCompanyOpen}>
              <DialogTrigger asChild>
                <Button className="bg-blue-600 hover:bg-blue-700">
                  <Building2 className="w-4 h-4 mr-2" />
                  Add Company
                </Button>
              </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Create New Company</DialogTitle>
              </DialogHeader>
              <div className="space-y-4">
                <div>
                  <Label htmlFor="companyName">Company Name *</Label>
                  <Input
                    id="companyName"
                    value={companyForm.name}
                    onChange={(e) => setCompanyForm({...companyForm, name: e.target.value})}
                    placeholder="Acme Instruments Ltd."
                  />
                </div>
                <div>
                  <Label htmlFor="adminName">Admin Name *</Label>
                  <div className="relative">
                    <User className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                    <Input
                      id="adminName"
                      value={companyForm.adminName}
                      onChange={(e) => setCompanyForm({...companyForm, adminName: e.target.value})}
                      placeholder="John Admin"
                      className="pl-10"
                    />
                  </div>
                </div>
                <div>
                  <Label htmlFor="adminEmail">Admin Email Address *</Label>
                  <div className="relative">
                    <Mail className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                    <Input
                      id="adminEmail"
                      type="email"
                      value={companyForm.adminEmail}
                      onChange={(e) => setCompanyForm({...companyForm, adminEmail: e.target.value})}
                      placeholder="admin@company.com"
                      className="pl-10"
                    />
                  </div>
                </div>
                <div>
                  <Label htmlFor="adminPassword">Default Admin Password</Label>
                  <div className="relative">
                    <Input
                      id="adminPassword"
                      type={showPassword ? "text" : "password"}
                      value="Admin@123"
                      className="pr-10 bg-gray-50 font-mono"
                      readOnly
                      disabled
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-2 top-1/2 transform -translate-y-1/2 p-1 h-auto"
                    >
                      {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                    </Button>
                  </div>
                  <p className="text-xs text-gray-500 mt-1">
                    All company admins will use this fixed password: <strong className="text-blue-600">Admin@123</strong>
                    <br />
                    They should change it after first login for security.
                  </p>
                </div>
                <div>
                  <Label htmlFor="maxUsers">Maximum Users</Label>
                  <Input
                    id="maxUsers"
                    type="number"
                    min="5"
                    value={companyForm.maxUsers}
                    onChange={(e) => setCompanyForm({...companyForm, maxUsers: parseInt(e.target.value) || 5})}
                  />
                </div>
                <Button onClick={handleCreateCompany} disabled={loading} className="w-full">
                  {loading ? "Creating..." : "Create Company"}
                </Button>
              </div>
            </DialogContent>
          </Dialog>
          )}

          {/* Edit Company Dialog */}
          {isSuperAdmin && (
          <Dialog open={isEditCompanyOpen} onOpenChange={setIsEditCompanyOpen}>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Edit Company</DialogTitle>
              </DialogHeader>
              <div className="space-y-4">
                <div>
                  <Label htmlFor="editCompanyName">Company Name *</Label>
                  <Input
                    id="editCompanyName"
                    value={editingCompany?.name || ''}
                    onChange={(e) => setEditingCompany(editingCompany ? {...editingCompany, name: e.target.value} : null)}
                    placeholder="Company Name"
                  />
                </div>
                <div>
                  <Label htmlFor="editAdminEmail">Admin Email *</Label>
                  <Input
                    id="editAdminEmail"
                    type="email"
                    value={editingCompany?.admin_email || ''}
                    onChange={(e) => setEditingCompany(editingCompany ? {...editingCompany, admin_email: e.target.value} : null)}
                    placeholder="admin@company.com"
                  />
                </div>
                <div>
                  <Label htmlFor="editMaxUsers">Maximum Users</Label>
                  <Input
                    id="editMaxUsers"
                    type="number"
                    min="5"
                    value={editingCompany?.max_users || 5}
                    onChange={(e) => setEditingCompany(editingCompany ? {...editingCompany, max_users: parseInt(e.target.value) || 5} : null)}
                  />
                </div>
                <Button onClick={handleUpdateCompany} disabled={loading} className="w-full">
                  {loading ? "Updating..." : "Update Company"}
                </Button>
              </div>
            </DialogContent>
          </Dialog>
          )}
        </div>
      </div>

      {/* Overview Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        {isSuperAdmin ? (
          // Super Admin Platform Stats
          <>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <Building2 className="w-8 h-8 text-blue-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Total Companies</p>
                    <p className="text-2xl font-bold">{companies.length}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <Users className="w-8 h-8 text-green-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Total Users</p>
                    <p className="text-2xl font-bold">
                      {companies.reduce((sum, c) => sum + (c.current_users || 0), 0)}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <CheckCircle className="w-8 h-8 text-green-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Active Companies</p>
                    <p className="text-2xl font-bold">
                      {companies.filter(c => c.subscription_status === 'active').length}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <Shield className="w-8 h-8 text-purple-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Available Roles</p>
                    <p className="text-2xl font-bold">{roles.length}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </>
        ) : (
          // Company Admin Company-Specific Stats
          <>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <Users className="w-8 h-8 text-blue-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Company Users</p>
                    <p className="text-2xl font-bold">
                      {users.length}/{currentUser?.company?.max_users || 'N/A'}
                    </p>
                    <p className="text-xs text-gray-500">
                      {users.length > 0 && currentUser?.company?.max_users 
                        ? `${Math.round((users.length / currentUser.company.max_users) * 100)}% capacity used`
                        : 'Loading...'
                      }
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <CheckCircle className="w-8 h-8 text-green-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Active Users</p>
                    <p className="text-2xl font-bold">
                      {users.filter(u => u.is_active).length}
                    </p>
                    <p className="text-xs text-gray-500">Currently active</p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <Building2 className="w-8 h-8 text-purple-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Company Status</p>
                    <p className="text-2xl font-bold text-green-600">Active</p>
                    <p className="text-xs text-gray-500">Subscription active</p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <Shield className="w-8 h-8 text-orange-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Your Role</p>
                    <p className="text-lg font-bold text-orange-600">Company Admin</p>
                    <p className="text-xs text-gray-500">Full company access</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </>
        )}
      </div>


      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
        <TabsList>
          {isSuperAdmin && <TabsTrigger value="companies">Companies</TabsTrigger>}
          <TabsTrigger value="users">{isSuperAdmin ? "Users" : "Company Users"}</TabsTrigger>
          <TabsTrigger value="roles">Roles</TabsTrigger>
        </TabsList>

        {isSuperAdmin && (
        <TabsContent value="companies" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Companies Overview</CardTitle>
              <CardDescription>Manage client companies and their subscriptions</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Company Name</TableHead>
                      <TableHead>Admin Email</TableHead>
                      <TableHead>Users</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Created</TableHead>
                      <TableHead>Expiry</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {companies.map((company, index) => (
                      <TableRow key={company.id || `company-${index}`}>
                        <TableCell className="font-medium">{company.name}</TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <Mail className="w-4 h-4 text-gray-400" />
                            <span>{company.admin_email || 'Loading...'}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <span className={`${
                            (company.current_users || 0) >= company.max_users ? 'text-red-600' : 'text-green-600'
                          }`}>
                            {company.current_users || 0}/{company.max_users}
                          </span>
                        </TableCell>
                        <TableCell>{getStatusBadge(company.subscription_status)}</TableCell>
                        <TableCell>
                          {company.created_at ? 
                            new Date(company.created_at).toLocaleDateString('en-US', { 
                              year: 'numeric', 
                              month: '2-digit', 
                              day: '2-digit' 
                            }) : 
                            'Unknown'
                          }
                        </TableCell>
                        <TableCell>
                          <Badge className="bg-green-100 text-green-800">
                            {company.expiry_date ? 
                              new Date(company.expiry_date).toLocaleDateString('en-US', { 
                                year: 'numeric', 
                                month: '2-digit', 
                                day: '2-digit' 
                              }) : 
                              '1 Year'
                            }
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex space-x-1">
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => {
                                setEditingCompany(company)
                                setIsEditCompanyOpen(true)
                              }}
                              title="Edit Company"
                            >
                              <Edit className="w-4 h-4" />
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => {
                                console.log('Delete button clicked for:', company.name)
                                handleDeleteCompany(company)
                              }}
                              title="Delete Company"
                              className="text-red-600 hover:text-red-800"
                            >
                              <Trash2 className="w-4 h-4" />
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
        </TabsContent>
        )}

        <TabsContent value="users" className="space-y-6">
          <div className="flex justify-between items-center">
            {isSuperAdmin ? (
              <div className="flex items-center space-x-4">
                <Label htmlFor="companySelect">Select Company:</Label>
                <Select value={selectedCompany} onValueChange={setSelectedCompany}>
                  <SelectTrigger className="w-64">
                    <SelectValue placeholder="Choose a company" />
                  </SelectTrigger>
                  <SelectContent>
                    {companies.map((company, index) => (
                      <SelectItem key={company.id || `select-company-${index}`} value={company.id}>
                        {company.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            ) : (
              <div>
                <h3 className="text-lg font-semibold">
                  Users in {currentUser?.company?.name || 'Your Company'}
                </h3>
                <p className="text-sm text-gray-600">Manage users in your company</p>
              </div>
            )}
            <Dialog open={isCreateUserOpen} onOpenChange={setIsCreateUserOpen}>
              <DialogTrigger asChild>
                <Button 
                  className="bg-green-600 hover:bg-green-700"
                  disabled={
                    (isSuperAdmin && (!selectedCompany || (selectedCompanyData?.current_users || 0) >= (selectedCompanyData?.max_users || 0))) ||
                    (isCompanyAdmin && !currentUser?.company_id)
                  }
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Add User
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Create New User</DialogTitle>
                </DialogHeader>
                <div className="space-y-4">
                  <div>
                    <Label htmlFor="userEmail">Email *</Label>
                    <Input
                      id="userEmail"
                      type="email"
                      value={userForm.email}
                      onChange={(e) => setUserForm({...userForm, email: e.target.value})}
                      placeholder="john@company.com"
                    />
                  </div>
                  <div>
                    <Label htmlFor="userFullName">Full Name *</Label>
                    <Input
                      id="userFullName"
                      value={userForm.fullName}
                      onChange={(e) => setUserForm({...userForm, fullName: e.target.value})}
                      placeholder="John Smith"
                    />
                  </div>
                  <div>
                    <Label htmlFor="userRole">Role *</Label>
                    <Select value={userForm.roleId} onValueChange={(value) => setUserForm({...userForm, roleId: value})}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select a role" />
                      </SelectTrigger>
                      <SelectContent>
                        {roles.map((role, index) => (
                          <SelectItem key={role.id || `role-${index}`} value={role.id}>
                            {role.name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} - {role.description}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="userPassword">Default Password</Label>
                    <div className="relative">
                      <Input
                        id="userPassword"
                        type={showUserPassword ? "text" : "password"}
                        value="User@123"
                        className="pr-10 bg-gray-50 font-mono"
                        readOnly
                        disabled
                      />
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        onClick={() => setShowUserPassword(!showUserPassword)}
                        className="absolute right-2 top-1/2 transform -translate-y-1/2 p-1 h-auto"
                      >
                        {showUserPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                      </Button>
                    </div>
                    <p className="text-xs text-gray-500 mt-1">
                      All new users will use this fixed password: <strong className="text-blue-600">User@123</strong>
                      <br />
                      They must change it after first login for security.
                    </p>
                  </div>
                  <Button onClick={handleCreateUser} disabled={loading} className="w-full">
                    {loading ? "Creating..." : "Create User"}
                  </Button>
                </div>
              </DialogContent>
            </Dialog>

            {/* Edit User Dialog */}
            <Dialog open={isEditUserOpen} onOpenChange={setIsEditUserOpen}>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Edit User</DialogTitle>
                </DialogHeader>
                {editingUser && (
                  <div className="space-y-4">
                    <div>
                      <Label htmlFor="editUserEmail">Email *</Label>
                      <Input
                        id="editUserEmail"
                        type="email"
                        value={editingUser.email}
                        onChange={(e) => setEditingUser({...editingUser, email: e.target.value})}
                        placeholder="user@company.com"
                      />
                    </div>
                    <div>
                      <Label htmlFor="editUserFullName">Full Name *</Label>
                      <Input
                        id="editUserFullName"
                        value={editingUser.full_name}
                        onChange={(e) => setEditingUser({...editingUser, full_name: e.target.value})}
                        placeholder="John Doe"
                      />
                    </div>
                    <div>
                      <Label htmlFor="editUserRole">Role *</Label>
                      <Select
                        value={editingUser.role_id}
                        onValueChange={(value) => setEditingUser({...editingUser, role_id: value})}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Select role" />
                        </SelectTrigger>
                        <SelectContent>
                          {roles.map((role, index) => (
                            <SelectItem key={role.id || `edit-role-${index}`} value={role.id}>
                              {role.name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="flex items-center space-x-2">
                      <input
                        type="checkbox"
                        id="editIsActive"
                        checked={editingUser.is_active}
                        onChange={(e) => setEditingUser({...editingUser, is_active: e.target.checked})}
                      />
                      <Label htmlFor="editIsActive">Active User</Label>
                    </div>
                    <Button onClick={handleUpdateUser} disabled={loading} className="w-full">
                      {loading ? "Updating..." : "Update User"}
                    </Button>
                  </div>
                )}
              </DialogContent>
            </Dialog>

          </div>

          {(isSuperAdmin ? selectedCompanyData : currentUser?.company) && (
            <Card>
              <CardHeader>
                <CardTitle>
                  Users for {isSuperAdmin ? selectedCompanyData?.name : currentUser?.company?.name}
                </CardTitle>
                <CardDescription>
                  {isSuperAdmin 
                    ? `${selectedCompanyData?.current_users || 0} of ${selectedCompanyData?.max_users || 0} users`
                    : `Company users and administrators`
                  }
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Name</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Role</TableHead>
                        <TableHead>Admin</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead>Last Login</TableHead>
                        <TableHead>Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {users.map((user, index) => (
                        <TableRow key={user.id || `user-${index}`}>
                          <TableCell className="font-medium">{user.full_name}</TableCell>
                          <TableCell>{user.email}</TableCell>
                          <TableCell>
                            <Badge variant="outline">
                              {user.role?.name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                            </Badge>
                          </TableCell>
                          <TableCell>
                            {user.is_admin ? (
                              <Badge className="bg-purple-100 text-purple-800">Admin</Badge>
                            ) : (
                              <Badge className="bg-gray-100 text-gray-800">User</Badge>
                            )}
                          </TableCell>
                          <TableCell>
                            {user.is_active ? (
                              <Badge className="bg-green-100 text-green-800">Active</Badge>
                            ) : (
                              <Badge className="bg-red-100 text-red-800">Inactive</Badge>
                            )}
                          </TableCell>
                          <TableCell>
                            {user.last_login ? new Date(user.last_login).toLocaleDateString() : 'Never'}
                          </TableCell>
                          <TableCell>
                            <div className="flex space-x-1">
                              <Button 
                                variant="outline" 
                                size="sm"
                                onClick={() => handleEditUser(user)}
                                title="Edit User"
                              >
                                <Edit className="w-4 h-4" />
                              </Button>
                              <Button 
                                variant="outline" 
                                size="sm"
                                onClick={() => handleDeleteUser(user.id)}
                                className="text-red-600 hover:text-red-800"
                              >
                                <Trash2 className="w-4 h-4" />
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
          )}
        </TabsContent>

        <TabsContent value="roles" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>User Roles & Permissions</CardTitle>
              <CardDescription>Available roles for instrumental CRM users</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4">
                {roles.map((role, index) => (
                  <Card key={role.id || `role-card-${index}`} className="p-4">
                    <div className="flex justify-between items-start">
                      <div className="space-y-2">
                        <h3 className="text-lg font-semibold">
                          {role.name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                        </h3>
                        <p className="text-sm text-gray-600">{role.description}</p>
                        <div className="flex flex-wrap gap-1 mt-2">
                          {role.permissions.slice(0, 5).map((permission, index) => (
                            <Badge key={`${role.id}-permission-${index}`} variant="outline" className="text-xs">
                              {permission}
                            </Badge>
                          ))}
                          {role.permissions.length > 5 && (
                            <Badge variant="outline" className="text-xs">
                              +{role.permissions.length - 5} more
                            </Badge>
                          )}
                        </div>
                      </div>
                    </div>
                  </Card>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Delete Company Confirmation Dialog - Moved outside of Tabs */}
      <Dialog open={isDeleteCompanyOpen} onOpenChange={(open) => {
        console.log('Dialog onOpenChange called with:', open)
        setIsDeleteCompanyOpen(open)
      }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="text-red-600">Delete Company</DialogTitle>
          </DialogHeader>
          {deletingCompany && (
            <div className="space-y-4">
              <div className="bg-red-50 p-4 rounded-lg border border-red-200">
                <div className="flex items-center space-x-2 mb-2">
                  <AlertTriangle className="w-5 h-5 text-red-600" />
                  <h4 className="text-red-800 font-medium">Warning: This action cannot be undone!</h4>
                </div>
                <p className="text-red-700 text-sm">
                  You are about to delete <strong>{deletingCompany.name}</strong> and all its associated data, including:
                </p>
                <ul className="text-red-700 text-sm mt-2 list-disc list-inside">
                  <li>All company users and their data</li>
                  <li>All company settings and configurations</li>
                  <li>All historical data and records</li>
                </ul>
              </div>
              
              <div>
                <Label htmlFor="deleteConfirmText" className="text-gray-900">
                  To confirm deletion, type <strong className="text-red-600">Delete</strong> in the box below:
                </Label>
                <Input
                  id="deleteConfirmText"
                  value={deleteConfirmText}
                  onChange={(e) => setDeleteConfirmText(e.target.value)}
                  placeholder="Type 'Delete' here"
                  className="mt-2"
                />
              </div>
              
              <div className="flex space-x-3">
                <Button
                  variant="outline"
                  onClick={() => {
                    setIsDeleteCompanyOpen(false)
                    setDeletingCompany(null)
                    setDeleteConfirmText("")
                  }}
                  disabled={loading}
                  className="flex-1"
                >
                  Cancel
                </Button>
                <Button
                  onClick={handleConfirmDeleteCompany}
                  disabled={loading || deleteConfirmText !== "Delete"}
                  className="flex-1 bg-red-600 hover:bg-red-700"
                >
                  {loading ? "Deleting..." : "Delete Company"}
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}