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
import { Textarea } from "@/components/ui/textarea"
import {
  Plus, Users, Shield, Edit, Trash2, Mail, User,
  CheckCircle, XCircle, Building2, Settings, UserPlus, Crown,
  Upload, Globe, Phone, MapPin, Calendar, Hash, Clock, Palette, Save, X, Eye, EyeOff, AlertTriangle, Sliders
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import { AccountFieldsManager } from "./account-fields-manager"
import { ContactFieldsManager } from "./contact-fields-manager"
import { ProductFieldsManager } from "./product-fields-manager"
import { LeadFieldsManager } from "./lead-fields-manager"

interface User {
  id: string
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
}

interface Role {
  id: string
  name: string
  description: string
  permissions: string[]
}

interface CompanyDetails {
  id: string
  name: string
  logo_url?: string
  website?: string
  address?: string
  city?: string
  state?: string
  country?: string
  postal_code?: string
  phone?: string
  email?: string
  industry?: string
  company_size?: string
  founded_year?: number
  tax_id?: string
  description?: string
  business_hours?: string
  primary_color?: string
  secondary_color?: string
  max_users?: number
}

export function CompanyAdminDashboard() {
  const { toast } = useToast()
  const [users, setUsers] = useState<User[]>([])
  const [roles, setRoles] = useState<Role[]>([])
  const [loading, setLoading] = useState(false)
  const [isCreateUserOpen, setIsCreateUserOpen] = useState(false)
  const [isEditUserOpen, setIsEditUserOpen] = useState(false)
  const [isDeleteConfirmOpen, setIsDeleteConfirmOpen] = useState(false)
  const [userToDelete, setUserToDelete] = useState<string>("")
  const [editingUser, setEditingUser] = useState<User | null>(null)
  const [activeTab, setActiveTab] = useState("users")
  const [uploadingLogo, setUploadingLogo] = useState(false)
  const [showPassword, setShowPassword] = useState(false)
  
  // Company details state
  const [companyDetails, setCompanyDetails] = useState<CompanyDetails>({
    id: '',
    name: '',
    logo_url: '',
    website: '',
    address: '',
    city: '',
    state: '',
    country: 'India',
    postal_code: '',
    phone: '',
    email: '',
    industry: '',
    company_size: '',
    founded_year: new Date().getFullYear(),
    tax_id: '',
    description: '',
    business_hours: '9:00 AM - 6:00 PM',
    primary_color: '#3B82F6',
    secondary_color: '#10B981',
    max_users: 5 // Default value, will be fetched from database
  })

  // Get max users from company details, fallback to 5 if not set
  const maxUsers = companyDetails.max_users || 5

  // Get current user from localStorage
  const currentUser = typeof window !== 'undefined' 
    ? JSON.parse(localStorage.getItem('user') || '{}')
    : {}

  // User form state
  const [userForm, setUserForm] = useState({
    email: "",
    fullName: "",
    roleId: "",
    isAdmin: false,
    password: "User@123"
  })

  useEffect(() => {
    loadUsers()
    loadRoles()
    loadCompanyDetails()
  }, [])

  const loadUsers = async () => {
    try {
      if (!currentUser.company_id) return
      
      const response = await fetch(`/api/admin/users?companyId=${currentUser.company_id}`)
      if (response.ok) {
        const data = await response.json()
        setUsers(data || [])
      }
    } catch (error) {
      console.error('Error loading users:', error)
    }
  }

  const loadRoles = async () => {
    try {
      const response = await fetch('/api/admin/roles')
      if (response.ok) {
        const data = await response.json()
        setRoles(data || [])
      }
    } catch (error) {
      console.error('Error loading roles:', error)
    }
  }

  const loadCompanyDetails = async () => {
    try {
      if (!currentUser.company_id) return
      
      // Fetch complete company details from API
      const response = await fetch(`/api/admin/companies?companyId=${currentUser.company_id}`)
      
      if (response.ok) {
        const companyData = await response.json()
        setCompanyDetails(prev => ({
          ...prev,
          id: companyData.id || currentUser.company_id,
          name: companyData.name || currentUser.company?.name || '',
          email: companyData.email || currentUser.email || '',
          max_users: companyData.max_users || 5,
          logo_url: companyData.logo_url || '',
          website: companyData.website || '',
          address: companyData.address || '',
          city: companyData.city || '',
          state: companyData.state || '',
          country: companyData.country || 'India',
          postal_code: companyData.postal_code || '',
          phone: companyData.phone || '',
          industry: companyData.industry || '',
          company_size: companyData.company_size || '',
          founded_year: companyData.founded_year || new Date().getFullYear(),
          tax_id: companyData.tax_id || '',
          description: companyData.description || '',
          business_hours: companyData.business_hours || '9:00 AM - 6:00 PM',
          primary_color: companyData.primary_color || '#3B82F6',
          secondary_color: companyData.secondary_color || '#10B981'
        }))
      } else {
        // Fallback to user object data if API fails
        setCompanyDetails(prev => ({
          ...prev,
          id: currentUser.company_id,
          name: currentUser.company?.name || '',
          email: currentUser.email || '',
          max_users: 5 // Default fallback
        }))
      }
    } catch (error) {
      console.error('Error loading company details:', error)
      // Fallback to user object data if API fails
      setCompanyDetails(prev => ({
        ...prev,
        id: currentUser.company_id,
        name: currentUser.company?.name || '',
        email: currentUser.email || '',
        max_users: 5 // Default fallback
      }))
    }
  }

  const updateCompanyDetails = async () => {
    try {
      setLoading(true)
      const response = await fetch('/api/admin/companies', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companyId: currentUser.company_id,
          ...companyDetails
        })
      })

      if (response.ok) {
        toast({
          title: "Success",
          description: "Company details updated successfully"
        })
        
        // Update localStorage with new company name and logo if changed
        const updatedUser = {
          ...currentUser,
          company: { 
            ...currentUser.company, 
            name: companyDetails.name,
            logo_url: companyDetails.logo_url 
          }
        }
        localStorage.setItem('user', JSON.stringify(updatedUser))
        
        // Trigger notification refresh to update header
        window.dispatchEvent(new CustomEvent('refreshNotifications'))
        window.dispatchEvent(new CustomEvent('companyDetailsUpdated'))
      } else {
        throw new Error('Failed to update company details')
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to update company details",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleLogoUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    // Validate file type
    if (!file.type.startsWith('image/')) {
      toast({
        title: "Error",
        description: "Please upload an image file",
        variant: "destructive"
      })
      return
    }

    // Validate file size (max 2MB)
    if (file.size > 2 * 1024 * 1024) {
      toast({
        title: "Error",
        description: "Image size must be less than 2MB",
        variant: "destructive"
      })
      return
    }

    setUploadingLogo(true)
    try {
      // Convert to base64 for storage (in production, upload to cloud storage)
      const reader = new FileReader()
      reader.onloadend = async () => {
        const base64String = reader.result as string
        setCompanyDetails(prev => ({ ...prev, logo_url: base64String }))
        
        toast({
          title: "Success",
          description: "Logo uploaded successfully. Don't forget to save changes."
        })
      }
      reader.readAsDataURL(file)
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to upload logo",
        variant: "destructive"
      })
    } finally {
      setUploadingLogo(false)
    }
  }

  const handleCreateUser = async () => {
    try {
      setLoading(true)
      const response = await fetch('/api/admin/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companyId: currentUser.company_id,
          ...userForm
        })
      })

      if (response.ok) {
        const newUser = await response.json()
        setUsers([newUser, ...users])
        setUserForm({ email: "", fullName: "", roleId: "", isAdmin: false, password: "User@123" })
        setIsCreateUserOpen(false)
        toast({
          title: "Success",
          description: "User created successfully"
        })
        
        // Trigger notification refresh
        window.dispatchEvent(new CustomEvent('refreshNotifications'))
      } else {
        // Get the specific error message from the API response
        const errorData = await response.json()
        const errorMessage = errorData.error || 'Failed to create user'
        
        // Provide more user-friendly error messages
        let userFriendlyMessage = errorMessage
        
        if (errorMessage.toLowerCase().includes('duplicate') || 
            errorMessage.toLowerCase().includes('already exists') ||
            errorMessage.toLowerCase().includes('unique constraint') ||
            errorMessage.toLowerCase().includes('violates unique constraint') ||
            errorMessage.toLowerCase().includes('users_email_key')) {
          userFriendlyMessage = 'A user with this email address already exists. Please use a different email.'
        } else if (errorMessage.toLowerCase().includes('maximum user limit')) {
          userFriendlyMessage = 'Your company has reached the maximum number of users allowed. Please upgrade your plan or remove inactive users.'
        } else if (errorMessage.toLowerCase().includes('invalid email')) {
          userFriendlyMessage = 'Please enter a valid email address.'
        }
        
        throw new Error(userFriendlyMessage)
      }
    } catch (error) {
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to create user",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleEditUser = (user: User) => {
    setEditingUser(user)
    setUserForm({
      email: user.email,
      fullName: user.full_name,
      roleId: user.role_id,
      isAdmin: user.is_admin,
      password: "User@123" // Keep for create form consistency
    })
    setIsEditUserOpen(true)
  }

  const handleUpdateUser = async () => {
    if (!editingUser) return
    
    setLoading(true)
    try {
      const response = await fetch('/api/admin/users', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: editingUser.id,
          email: userForm.email,
          fullName: userForm.fullName,
          roleId: userForm.roleId,
          isAdmin: userForm.isAdmin,
          isActive: editingUser.is_active
        })
      })

      if (response.ok) {
        const updatedUser = await response.json()
        
        // Password updates removed - only user details can be updated
        
        setUsers(users.map(u => u.id === editingUser.id ? updatedUser : u))
        toast({
          title: "Success",
          description: "User updated successfully"
        })
        setIsEditUserOpen(false)
        setEditingUser(null)
        
        // Reset form
        setUserForm({
          email: "",
          fullName: "",
          roleId: "",
          isAdmin: false,
          password: "User@123"
        })
        
        // Trigger notification refresh
        window.dispatchEvent(new CustomEvent('refreshNotifications'))
      } else {
        const errorData = await response.json()
        const errorMessage = errorData.error || 'Failed to update user'
        
        // Provide more user-friendly error messages
        let userFriendlyMessage = errorMessage
        
        if (errorMessage.toLowerCase().includes('duplicate') || 
            errorMessage.toLowerCase().includes('already exists') ||
            errorMessage.toLowerCase().includes('unique constraint') ||
            errorMessage.toLowerCase().includes('violates unique constraint') ||
            errorMessage.toLowerCase().includes('users_email_key')) {
          userFriendlyMessage = 'A user with this email address already exists. Please use a different email.'
        } else if (errorMessage.toLowerCase().includes('invalid email')) {
          userFriendlyMessage = 'Please enter a valid email address.'
        }
        
        toast({
          title: "Error",
          description: userFriendlyMessage,
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Update user error:', error)
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to update user",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleDeleteUser = (userId: string) => {
    setUserToDelete(userId)
    setIsDeleteConfirmOpen(true)
  }

  const confirmDeleteUser = async () => {
    if (!userToDelete) return
    
    setLoading(true)
    try {
      const response = await fetch(`/api/admin/users?userId=${userToDelete}`, {
        method: 'DELETE'
      })

      if (response.ok) {
        setUsers(users.filter(u => u.id !== userToDelete))
        toast({
          title: "Success",
          description: "User deleted successfully"
        })
        
        // Trigger notification refresh
        window.dispatchEvent(new CustomEvent('refreshNotifications'))
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to delete user",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
      setIsDeleteConfirmOpen(false)
      setUserToDelete("")
    }
  }

  const getStatusBadge = (isActive: boolean) => {
    return isActive 
      ? <Badge className="bg-green-100 text-green-800">Active</Badge>
      : <Badge className="bg-red-100 text-red-800">Inactive</Badge>
  }

  const getRoleColor = (roleName: string) => {
    const roleColors: { [key: string]: string } = {
      company_admin: "bg-purple-100 text-purple-800",
      sales_manager: "bg-blue-100 text-blue-800",
      sales_representative: "bg-cyan-100 text-cyan-800",
      marketing_manager: "bg-pink-100 text-pink-800",
      field_service_manager: "bg-orange-100 text-orange-800",
      field_service_engineer: "bg-yellow-100 text-yellow-800",
      customer_support: "bg-green-100 text-green-800",
      finance_manager: "bg-indigo-100 text-indigo-800",
      inventory_manager: "bg-red-100 text-red-800",
      viewer: "bg-gray-100 text-gray-800"
    }
    
    return (
      <Badge className={roleColors[roleName] || "bg-gray-100 text-gray-800"}>
        {roleName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
      </Badge>
    )
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Company Administration</h1>
          <p className="text-gray-600">Manage your company settings and users</p>
        </div>
      </div>

      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
        <TabsList className="grid w-full max-w-3xl grid-cols-3">
          <TabsTrigger value="users" className="flex items-center gap-2">
            <Users className="w-4 h-4" />
            Users
          </TabsTrigger>
          <TabsTrigger value="settings" className="flex items-center gap-2">
            <Settings className="w-4 h-4" />
            Company Settings
          </TabsTrigger>
          <TabsTrigger value="fields" className="flex items-center gap-2">
            <Sliders className="w-4 h-4" />
            Optional Fields Selection
          </TabsTrigger>
        </TabsList>

        {/* Users Tab */}
        <TabsContent value="users" className="space-y-6">
          <div className="flex justify-between items-center">
            <h2 className="text-xl font-semibold">User Management</h2>
            <Button 
              onClick={() => setIsCreateUserOpen(true)} 
              disabled={users.length >= maxUsers}
              className="bg-green-600 hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
              title={users.length >= maxUsers ? `Maximum users limit reached (${maxUsers})` : "Add new user"}
            >
              <UserPlus className="w-4 h-4 mr-2" />
              Add User {users.length >= maxUsers && `(${users.length}/${maxUsers})`}
            </Button>
          </div>

          {/* User Limit Warning */}
          {users.length >= maxUsers && (
            <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
              <div className="flex items-center space-x-2">
                <AlertTriangle className="w-5 h-5 text-yellow-600" />
                <div>
                  <h3 className="text-sm font-medium text-yellow-800">User Limit Reached</h3>
                  <p className="text-sm text-yellow-700">
                    You have reached the maximum limit of {maxUsers} users. Delete a user to add new ones.
                  </p>
                </div>
              </div>
            </div>
          )}
          
          {users.length === maxUsers - 1 && (
            <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
              <div className="flex items-center space-x-2">
                <AlertTriangle className="w-5 h-5 text-orange-600" />
                <div>
                  <h3 className="text-sm font-medium text-orange-800">Approaching User Limit</h3>
                  <p className="text-sm text-orange-700">
                    You can add {maxUsers - users.length} more user before reaching the limit of {maxUsers} users.
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* User Stats */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <Users className="w-8 h-8 text-blue-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Total Users</p>
                    <p className="text-2xl font-bold">{users.length}/{maxUsers}</p>
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
                    <p className="text-2xl font-bold">{users.filter(u => u.is_active).length}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center space-x-2">
                  <Crown className="w-8 h-8 text-purple-500" />
                  <div>
                    <p className="text-sm font-medium text-gray-600">Administrators</p>
                    <p className="text-2xl font-bold">{users.filter(u => u.is_admin).length}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Users Table */}
          <Card>
            <CardHeader>
              <CardTitle>All Users</CardTitle>
              <CardDescription>Manage user accounts and permissions</CardDescription>
            </CardHeader>
            <CardContent>
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
                  {users.map((user) => (
                    <TableRow key={user.id}>
                      <TableCell className="font-medium">{user.full_name}</TableCell>
                      <TableCell>{user.email}</TableCell>
                      <TableCell>{getRoleColor(user.role?.name || '')}</TableCell>
                      <TableCell>
                        {user.is_admin ? (
                          <Badge className="bg-purple-100 text-purple-800">
                            <Crown className="w-3 h-3 mr-1" />
                            Admin
                          </Badge>
                        ) : (
                          <span className="text-gray-500">-</span>
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
                            className="text-blue-600 hover:text-blue-700"
                          >
                            <Edit className="w-3 h-3" />
                          </Button>
                          <Button 
                            variant="outline" 
                            size="sm"
                            onClick={() => handleDeleteUser(user.id)}
                            className="text-red-600 hover:text-red-700"
                          >
                            <Trash2 className="w-3 h-3" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Company Settings Tab */}
        <TabsContent value="settings" className="space-y-6">
          <div className="flex justify-between items-center">
            <h2 className="text-xl font-semibold">Company Settings</h2>
            <Button onClick={updateCompanyDetails} disabled={loading}>
              <Save className="w-4 h-4 mr-2" />
              {loading ? "Saving..." : "Save Changes"}
            </Button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Basic Information */}
            <Card>
              <CardHeader>
                <CardTitle>Basic Information</CardTitle>
                <CardDescription>Update your company's basic details</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Logo Upload */}
                <div className="space-y-2">
                  <Label>Company Logo</Label>
                  <div className="flex items-center space-x-4">
                    {companyDetails.logo_url ? (
                      <img 
                        src={companyDetails.logo_url} 
                        alt="Company Logo" 
                        className="w-20 h-20 object-contain border rounded"
                      />
                    ) : (
                      <div className="w-20 h-20 bg-gray-100 border rounded flex items-center justify-center">
                        <Building2 className="w-8 h-8 text-gray-400" />
                      </div>
                    )}
                    <div>
                      <Input
                        type="file"
                        accept="image/*"
                        onChange={handleLogoUpload}
                        disabled={uploadingLogo}
                        className="hidden"
                        id="logo-upload"
                      />
                      <Label htmlFor="logo-upload" className="cursor-pointer">
                        <Button variant="outline" size="sm" asChild>
                          <span>
                            <Upload className="w-4 h-4 mr-2" />
                            {uploadingLogo ? "Uploading..." : "Upload Logo"}
                          </span>
                        </Button>
                      </Label>
                      {companyDetails.logo_url && (
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          onClick={() => setCompanyDetails(prev => ({ ...prev, logo_url: '' }))}
                          className="ml-2"
                        >
                          <X className="w-4 h-4" />
                        </Button>
                      )}
                    </div>
                  </div>
                </div>

                <div>
                  <Label htmlFor="companyName">Company Name</Label>
                  <Input
                    id="companyName"
                    value={companyDetails.name}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, name: e.target.value }))}
                    placeholder="Your Company Name"
                  />
                </div>

                <div>
                  <Label htmlFor="website">Website</Label>
                  <Input
                    id="website"
                    type="url"
                    value={companyDetails.website || ''}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, website: e.target.value }))}
                    placeholder="https://www.example.com"
                  />
                </div>

                <div>
                  <Label htmlFor="email">Contact Email</Label>
                  <Input
                    id="email"
                    type="email"
                    value={companyDetails.email || ''}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, email: e.target.value }))}
                    placeholder="contact@company.com"
                  />
                </div>

                <div>
                  <Label htmlFor="phone">Phone Number</Label>
                  <Input
                    id="phone"
                    type="tel"
                    value={companyDetails.phone || ''}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, phone: e.target.value }))}
                    placeholder="+91 98765 43210"
                  />
                </div>

                <div>
                  <Label htmlFor="industry">Industry</Label>
                  <Select 
                    value={companyDetails.industry || ''} 
                    onValueChange={(value) => setCompanyDetails(prev => ({ ...prev, industry: value }))}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select industry" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="technology">Technology</SelectItem>
                      <SelectItem value="manufacturing">Manufacturing</SelectItem>
                      <SelectItem value="healthcare">Healthcare</SelectItem>
                      <SelectItem value="finance">Finance</SelectItem>
                      <SelectItem value="retail">Retail</SelectItem>
                      <SelectItem value="education">Education</SelectItem>
                      <SelectItem value="construction">Construction</SelectItem>
                      <SelectItem value="automotive">Automotive</SelectItem>
                      <SelectItem value="other">Other</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label htmlFor="companySize">Company Size</Label>
                  <Select 
                    value={companyDetails.company_size || ''} 
                    onValueChange={(value) => setCompanyDetails(prev => ({ ...prev, company_size: value }))}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select company size" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="1-10">1-10 employees</SelectItem>
                      <SelectItem value="11-50">11-50 employees</SelectItem>
                      <SelectItem value="51-200">51-200 employees</SelectItem>
                      <SelectItem value="201-500">201-500 employees</SelectItem>
                      <SelectItem value="500+">500+ employees</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </CardContent>
            </Card>

            {/* Address & Additional Details */}
            <Card>
              <CardHeader>
                <CardTitle>Address & Additional Details</CardTitle>
                <CardDescription>Update your company's location and other information</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label htmlFor="address">Street Address</Label>
                  <Input
                    id="address"
                    value={companyDetails.address || ''}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, address: e.target.value }))}
                    placeholder="123 Main Street"
                  />
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="city">City</Label>
                    <Input
                      id="city"
                      value={companyDetails.city || ''}
                      onChange={(e) => setCompanyDetails(prev => ({ ...prev, city: e.target.value }))}
                      placeholder="Mumbai"
                    />
                  </div>
                  <div>
                    <Label htmlFor="state">State</Label>
                    <Input
                      id="state"
                      value={companyDetails.state || ''}
                      onChange={(e) => setCompanyDetails(prev => ({ ...prev, state: e.target.value }))}
                      placeholder="Maharashtra"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="country">Country</Label>
                    <Input
                      id="country"
                      value={companyDetails.country || ''}
                      onChange={(e) => setCompanyDetails(prev => ({ ...prev, country: e.target.value }))}
                      placeholder="India"
                    />
                  </div>
                  <div>
                    <Label htmlFor="postalCode">Postal Code</Label>
                    <Input
                      id="postalCode"
                      value={companyDetails.postal_code || ''}
                      onChange={(e) => setCompanyDetails(prev => ({ ...prev, postal_code: e.target.value }))}
                      placeholder="400001"
                    />
                  </div>
                </div>

                <div>
                  <Label htmlFor="foundedYear">Founded Year</Label>
                  <Input
                    id="foundedYear"
                    type="number"
                    value={companyDetails.founded_year || ''}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, founded_year: parseInt(e.target.value) }))}
                    placeholder="2020"
                    min="1900"
                    max={new Date().getFullYear()}
                  />
                </div>

                <div>
                  <Label htmlFor="taxId">Tax ID / GST Number</Label>
                  <Input
                    id="taxId"
                    value={companyDetails.tax_id || ''}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, tax_id: e.target.value }))}
                    placeholder="GST123456789"
                  />
                </div>

                <div>
                  <Label htmlFor="businessHours">Business Hours</Label>
                  <Input
                    id="businessHours"
                    value={companyDetails.business_hours || ''}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, business_hours: e.target.value }))}
                    placeholder="Monday-Friday, 9:00 AM - 6:00 PM"
                  />
                </div>

                <div>
                  <Label htmlFor="description">Company Description</Label>
                  <Textarea
                    id="description"
                    value={companyDetails.description || ''}
                    onChange={(e) => setCompanyDetails(prev => ({ ...prev, description: e.target.value }))}
                    placeholder="Brief description of your company..."
                    rows={4}
                  />
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Optional Fields Selection Tab */}
        <TabsContent value="fields" className="space-y-6">
          <div>
            <h2 className="text-xl font-semibold mb-2">Optional Fields Configuration</h2>
            <p className="text-gray-600">Configure which fields appear in different modules for data entry</p>
          </div>

          {/* Nested tabs for different modules */}
          <Tabs defaultValue="accounts" className="space-y-4">
            <TabsList>
              <TabsTrigger value="accounts">Accounts Fields</TabsTrigger>
              <TabsTrigger value="contacts">
                Contacts Fields
              </TabsTrigger>
              <TabsTrigger value="products">
                Products Fields
              </TabsTrigger>
              <TabsTrigger value="leads">
                Leads Fields
              </TabsTrigger>
              <TabsTrigger value="deals" disabled>
                Deals Fields <Badge variant="outline" className="ml-2">Coming Soon</Badge>
              </TabsTrigger>
            </TabsList>

            {/* Accounts Fields Tab */}
            <TabsContent value="accounts">
              <AccountFieldsManager />
            </TabsContent>

            {/* Contacts Fields Tab */}
            <TabsContent value="contacts">
              <ContactFieldsManager />
            </TabsContent>

            {/* Products Fields Tab */}
            <TabsContent value="products">
              <ProductFieldsManager />
            </TabsContent>

            {/* Leads Fields Tab */}
            <TabsContent value="leads">
              <LeadFieldsManager />
            </TabsContent>

            {/* Deals Fields Tab - Placeholder */}
            <TabsContent value="deals">
              <Card>
                <CardHeader>
                  <CardTitle>Deals Fields Configuration</CardTitle>
                  <CardDescription>Configure optional fields for deals module</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="text-center py-12">
                    <Shield className="w-16 h-16 mx-auto text-gray-400 mb-4" />
                    <h3 className="text-lg font-semibold text-gray-900 mb-2">Coming Soon</h3>
                    <p className="text-gray-600">
                      Deals field configuration will be available in the next update
                    </p>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </TabsContent>
      </Tabs>

      {/* Create User Dialog */}
      <Dialog open={isCreateUserOpen} onOpenChange={setIsCreateUserOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add New User</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <Label htmlFor="userEmail">Email Address *</Label>
              <Input
                id="userEmail"
                type="email"
                value={userForm.email}
                onChange={(e) => setUserForm({...userForm, email: e.target.value})}
                placeholder="user@yourcompany.com"
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
                  {roles.map((role) => (
                    <SelectItem key={role.id} value={role.id}>
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
                  type={showPassword ? "text" : "password"}
                  value={userForm.password}
                  onChange={(e) => setUserForm({...userForm, password: e.target.value})}
                  placeholder="Default password for new user"
                  className="pr-10"
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-2 top-1/2 transform -translate-y-1/2 p-1 h-auto hover:bg-gray-100"
                >
                  {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </Button>
              </div>
              <p className="text-xs text-gray-500 mt-1">
                User will be required to change this password on first login
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
          <div className="space-y-4">
            <div>
              <Label htmlFor="editUserEmail">Email Address *</Label>
              <Input
                id="editUserEmail"
                type="email"
                value={userForm.email}
                onChange={(e) => setUserForm({...userForm, email: e.target.value})}
                placeholder="user@yourcompany.com"
              />
            </div>
            <div>
              <Label htmlFor="editUserFullName">Full Name *</Label>
              <Input
                id="editUserFullName"
                value={userForm.fullName}
                onChange={(e) => setUserForm({...userForm, fullName: e.target.value})}
                placeholder="John Smith"
              />
            </div>
            <div>
              <Label htmlFor="editUserRole">Role *</Label>
              <Select value={userForm.roleId} onValueChange={(value) => setUserForm({...userForm, roleId: value})}>
                <SelectTrigger>
                  <SelectValue placeholder="Select a role" />
                </SelectTrigger>
                <SelectContent>
                  {roles.map((role) => (
                    <SelectItem key={role.id} value={role.id}>
                      {role.name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} - {role.description}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="flex space-x-3">
              <Button 
                variant="outline" 
                onClick={() => setIsEditUserOpen(false)}
                className="flex-1"
              >
                Cancel
              </Button>
              <Button 
                onClick={handleUpdateUser} 
                disabled={loading} 
                className="flex-1"
              >
                {loading ? "Updating..." : "Update User"}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={isDeleteConfirmOpen} onOpenChange={setIsDeleteConfirmOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center space-x-2 text-red-600">
              <AlertTriangle className="w-5 h-5" />
              <span>Delete User</span>
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <p className="text-gray-600">
              Are you sure you want to delete this user? This action cannot be undone and will permanently remove the user and all their associated data.
            </p>
            <div className="flex space-x-3">
              <Button 
                variant="outline" 
                onClick={() => setIsDeleteConfirmOpen(false)}
                className="flex-1"
                disabled={loading}
              >
                Cancel
              </Button>
              <Button 
                onClick={confirmDeleteUser} 
                disabled={loading} 
                className="flex-1 bg-red-600 hover:bg-red-700 text-white"
              >
                {loading ? "Deleting..." : "Delete User"}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}