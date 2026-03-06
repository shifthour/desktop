"use client"

import type React from "react"
import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Badge } from "@/components/ui/badge"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { Switch } from "@/components/ui/switch"
import { Textarea } from "@/components/ui/textarea"
import { Separator } from "@/components/ui/separator"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Upload,
  Settings,
  Users,
  Shield,
  Database,
  Building2,
  ImageIcon,
  Save,
  Eye,
  Trash2,
  Plus,
  Edit,
  MoreHorizontal,
  Activity,
  Clock,
  DollarSign,
  AlertTriangle,
  Key,
  Lock,
  Bell,
  BarChart3,
  UserCheck,
  UserX,
  Mail,
  Calendar,
  MapPin,
  CreditCard,
  Download,
  Filter,
  Search,
  RefreshCw,
} from "lucide-react"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"

const adminStats = [
  { title: "Total Users", value: "24/50", icon: Users, color: "bg-blue-500", trend: "+12%" },
  { title: "Active Sessions", value: "18", icon: Shield, color: "bg-green-500", trend: "+5%" },
  { title: "Monthly Revenue", value: "₹2,400", icon: DollarSign, color: "bg-purple-500", trend: "+18%" },
  { title: "System Health", value: "98%", icon: Database, color: "bg-orange-500", trend: "+2%" },
]

const users = [
  {
    id: 1,
    name: "Prashanth Sandilya",
    email: "prashanth@labgig.com",
    role: "Admin",
    status: "Active",
    avatar: "PS",
    lastLogin: "2 hours ago",
    department: "Management",
    phone: "+91 98765 43210",
    joinDate: "Jan 2023",
    permissions: ["All Access"],
    billingStatus: "Active",
    sessions: 3,
    location: "Bangalore, IN",
  },
  {
    id: 2,
    name: "Hari Kumar K",
    email: "hari@labgig.com",
    role: "Sales Manager",
    status: "Active",
    avatar: "HK",
    lastLogin: "1 hour ago",
    department: "Sales",
    phone: "+91 98765 43211",
    joinDate: "Feb 2023",
    permissions: ["Sales", "Reports"],
    billingStatus: "Active",
    sessions: 2,
    location: "Chennai, IN",
  },
  {
    id: 3,
    name: "Rajesh Menon",
    email: "rajesh@labgig.com",
    role: "Sales Executive",
    status: "Active",
    avatar: "RM",
    lastLogin: "30 minutes ago",
    department: "Sales",
    phone: "+91 98765 43212",
    joinDate: "Mar 2023",
    permissions: ["Sales"],
    billingStatus: "Active",
    sessions: 1,
    location: "Mumbai, IN",
  },
  {
    id: 4,
    name: "Priya Sharma",
    email: "priya@labgig.com",
    role: "Support",
    status: "Inactive",
    avatar: "PS",
    lastLogin: "2 days ago",
    department: "Support",
    phone: "+91 98765 43213",
    joinDate: "Apr 2023",
    permissions: ["Support", "Tickets"],
    billingStatus: "Suspended",
    sessions: 0,
    location: "Delhi, IN",
  },
]

const roleDefinitions = [
  { name: "Admin", permissions: ["All Access"], color: "bg-red-500", users: 1 },
  { name: "Sales Manager", permissions: ["Sales", "Reports", "Team Management"], color: "bg-blue-500", users: 1 },
  { name: "Sales Executive", permissions: ["Sales", "Leads"], color: "bg-green-500", users: 1 },
  { name: "Support", permissions: ["Support", "Tickets"], color: "bg-yellow-500", users: 1 },
  { name: "Marketing Manager", permissions: ["Marketing", "Campaigns"], color: "bg-purple-500", users: 0 },
  { name: "Project Manager", permissions: ["Projects", "Tasks"], color: "bg-indigo-500", users: 0 },
]

export function AdminContent() {
  const [companyLogo, setCompanyLogo] = useState<string>("/company-logo.png")
  const [companyName, setCompanyName] = useState("LabGig")
  const [companyDescription, setCompanyDescription] = useState("Laboratory Equipment Solutions")
  const [logoFile, setLogoFile] = useState<File | null>(null)
  const [selectedUser, setSelectedUser] = useState<any>(null)
  const [isAddUserOpen, setIsAddUserOpen] = useState(false)
  const [searchTerm, setSearchTerm] = useState("")
  const [filterRole, setFilterRole] = useState("all")
  const [filterStatus, setFilterStatus] = useState("all")

  const handleLogoUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (file) {
      setLogoFile(file)
      const reader = new FileReader()
      reader.onload = (e) => {
        setCompanyLogo(e.target?.result as string)
      }
      reader.readAsDataURL(file)
    }
  }

  const handleSaveCompanySettings = () => {
    console.log("Saving company settings:", {
      companyName,
      companyDescription,
      logoFile,
    })
  }

  const filteredUsers = users.filter((user) => {
    const matchesSearch =
      user.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      user.email.toLowerCase().includes(searchTerm.toLowerCase())
    const matchesRole = filterRole === "all" || user.role === filterRole
    const matchesStatus = filterStatus === "all" || user.status.toLowerCase() === filterStatus
    return matchesSearch && matchesRole && matchesStatus
  })

  return (
    <div className="p-6 space-y-6">
      <div className="bg-gradient-to-r from-primary via-primary/90 to-primary rounded-xl p-6 text-primary-foreground">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold mb-2">User Management Hub</h1>
            <p className="text-primary-foreground/80">Comprehensive user administration for scalable teams</p>
          </div>
          <div className="flex items-center space-x-4">
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3">
              <Settings className="h-6 w-6" />
            </div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {adminStats.map((stat) => (
          <Card key={stat.title} className="hover:shadow-md transition-shadow">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-muted-foreground">{stat.title}</p>
                  <p className="text-2xl font-bold">{stat.value}</p>
                  <p className="text-xs text-green-600 font-medium">{stat.trend}</p>
                </div>
                <div className={`p-3 rounded-lg ${stat.color}`}>
                  <stat.icon className="h-6 w-6 text-white" />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <Tabs defaultValue="users" className="space-y-6">
        <TabsList className="grid w-full grid-cols-8">
          <TabsTrigger value="users">Users</TabsTrigger>
          <TabsTrigger value="roles">Roles</TabsTrigger>
          <TabsTrigger value="billing">Billing</TabsTrigger>
          <TabsTrigger value="security">Security</TabsTrigger>
          <TabsTrigger value="analytics">Analytics</TabsTrigger>
          <TabsTrigger value="notifications">Notifications</TabsTrigger>
          <TabsTrigger value="company">Company</TabsTrigger>
          <TabsTrigger value="system">System</TabsTrigger>
        </TabsList>

        <TabsContent value="users" className="space-y-6">
          <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
            <div>
              <h2 className="text-2xl font-bold">User Management</h2>
              <p className="text-muted-foreground">Manage users, roles, and permissions across your organization</p>
            </div>
            <div className="flex gap-2">
              <Button variant="outline">
                <Download className="w-4 h-4 mr-2" />
                Export
              </Button>
              <Dialog open={isAddUserOpen} onOpenChange={setIsAddUserOpen}>
                <DialogTrigger asChild>
                  <Button>
                    <Plus className="w-4 h-4 mr-2" />
                    Add User
                  </Button>
                </DialogTrigger>
                <DialogContent className="sm:max-w-md">
                  <DialogHeader>
                    <DialogTitle>Add New User</DialogTitle>
                    <DialogDescription>Create a new user account with appropriate permissions.</DialogDescription>
                  </DialogHeader>
                  <div className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label htmlFor="firstName">First Name</Label>
                        <Input id="firstName" placeholder="John" />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="lastName">Last Name</Label>
                        <Input id="lastName" placeholder="Doe" />
                      </div>
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="email">Email</Label>
                      <Input id="email" type="email" placeholder="john@company.com" />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="role">Role</Label>
                      <Select>
                        <SelectTrigger>
                          <SelectValue placeholder="Select role" />
                        </SelectTrigger>
                        <SelectContent>
                          {roleDefinitions.map((role) => (
                            <SelectItem key={role.name} value={role.name}>
                              {role.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="department">Department</Label>
                      <Select>
                        <SelectTrigger>
                          <SelectValue placeholder="Select department" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="sales">Sales</SelectItem>
                          <SelectItem value="marketing">Marketing</SelectItem>
                          <SelectItem value="support">Support</SelectItem>
                          <SelectItem value="management">Management</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <DialogFooter>
                    <Button variant="outline" onClick={() => setIsAddUserOpen(false)}>
                      Cancel
                    </Button>
                    <Button onClick={() => setIsAddUserOpen(false)}>Create User</Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            </div>
          </div>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <Filter className="w-5 h-5 mr-2" />
                Filters & Search
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col sm:flex-row gap-4">
                <div className="flex-1">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground w-4 h-4" />
                    <Input
                      placeholder="Search users by name or email..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="pl-10"
                    />
                  </div>
                </div>
                <Select value={filterRole} onValueChange={setFilterRole}>
                  <SelectTrigger className="w-[180px]">
                    <SelectValue placeholder="Filter by role" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Roles</SelectItem>
                    {roleDefinitions.map((role) => (
                      <SelectItem key={role.name} value={role.name}>
                        {role.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Select value={filterStatus} onValueChange={setFilterStatus}>
                  <SelectTrigger className="w-[180px]">
                    <SelectValue placeholder="Filter by status" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Status</SelectItem>
                    <SelectItem value="active">Active</SelectItem>
                    <SelectItem value="inactive">Inactive</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <div className="flex items-center">
                  <Users className="w-5 h-5 mr-2" />
                  Users ({filteredUsers.length})
                </div>
                <Button variant="outline" size="sm">
                  <RefreshCw className="w-4 h-4 mr-2" />
                  Refresh
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {filteredUsers.map((user) => (
                  <div
                    key={user.id}
                    className="flex items-center justify-between p-4 border rounded-lg hover:bg-muted/50 transition-colors"
                  >
                    <div className="flex items-center space-x-4">
                      <Avatar className="h-12 w-12">
                        <AvatarFallback className="bg-accent text-accent-foreground">{user.avatar}</AvatarFallback>
                      </Avatar>
                      <div className="space-y-1">
                        <div className="flex items-center space-x-2">
                          <p className="font-semibold">{user.name}</p>
                          <Badge variant={user.status === "Active" ? "default" : "secondary"}>{user.status}</Badge>
                        </div>
                        <p className="text-sm text-muted-foreground flex items-center">
                          <Mail className="w-3 h-3 mr-1" />
                          {user.email}
                        </p>
                        <div className="flex items-center space-x-4 text-xs text-muted-foreground">
                          <span className="flex items-center">
                            <Clock className="w-3 h-3 mr-1" />
                            {user.lastLogin}
                          </span>
                          <span className="flex items-center">
                            <MapPin className="w-3 h-3 mr-1" />
                            {user.location}
                          </span>
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center space-x-3">
                      <div className="text-right space-y-1">
                        <Badge variant="outline" className="text-xs">
                          {user.role}
                        </Badge>
                        <p className="text-xs text-muted-foreground">{user.sessions} active sessions</p>
                      </div>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button variant="ghost" size="sm">
                            <MoreHorizontal className="w-4 h-4" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem onClick={() => setSelectedUser(user)}>
                            <Edit className="w-4 h-4 mr-2" />
                            Edit User
                          </DropdownMenuItem>
                          <DropdownMenuItem>
                            <Key className="w-4 h-4 mr-2" />
                            Reset Password
                          </DropdownMenuItem>
                          <DropdownMenuItem>
                            <Activity className="w-4 h-4 mr-2" />
                            View Activity
                          </DropdownMenuItem>
                          <DropdownMenuItem>
                            {user.status === "Active" ? (
                              <UserX className="w-4 h-4 mr-2" />
                            ) : (
                              <UserCheck className="w-4 h-4 mr-2" />
                            )}
                            {user.status === "Active" ? "Deactivate" : "Activate"}
                          </DropdownMenuItem>
                          <DropdownMenuItem className="text-destructive">
                            <Trash2 className="w-4 h-4 mr-2" />
                            Delete User
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="roles" className="space-y-6">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-2xl font-bold">Role Management</h2>
              <p className="text-muted-foreground">Define user roles with precision and manage permissions</p>
            </div>
            <Button>
              <Plus className="w-4 h-4 mr-2" />
              Create Role
            </Button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {roleDefinitions.map((role) => (
              <Card key={role.name} className="hover:shadow-md transition-shadow">
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <div className="flex items-center">
                      <div className={`w-3 h-3 rounded-full ${role.color} mr-2`} />
                      {role.name}
                    </div>
                    <Badge variant="secondary">{role.users} users</Badge>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div>
                      <p className="text-sm font-medium mb-2">Permissions:</p>
                      <div className="flex flex-wrap gap-1">
                        {role.permissions.map((permission) => (
                          <Badge key={permission} variant="outline" className="text-xs">
                            {permission}
                          </Badge>
                        ))}
                      </div>
                    </div>
                    <div className="flex space-x-2">
                      <Button variant="outline" size="sm" className="flex-1 bg-transparent">
                        <Edit className="w-3 h-3 mr-1" />
                        Edit
                      </Button>
                      <Button variant="outline" size="sm">
                        <MoreHorizontal className="w-3 h-3" />
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>

        <TabsContent value="billing" className="space-y-6">
          <div>
            <h2 className="text-2xl font-bold">Billing Management</h2>
            <p className="text-muted-foreground">Transparent billing management for scalable teams</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <CreditCard className="w-5 h-5 mr-2" />
                  Current Plan
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <p className="text-2xl font-bold">Professional</p>
                  <p className="text-muted-foreground">₹50/month per user</p>
                  <p className="text-sm">24/50 users active</p>
                  <Button className="w-full mt-4">Upgrade Plan</Button>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <DollarSign className="w-5 h-5 mr-2" />
                  Monthly Cost
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <p className="text-2xl font-bold">₹1,200</p>
                  <p className="text-muted-foreground">24 active users</p>
                  <p className="text-sm text-green-600">Save ₹300 with annual billing</p>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Calendar className="w-5 h-5 mr-2" />
                  Next Billing
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <p className="text-2xl font-bold">Dec 15</p>
                  <p className="text-muted-foreground">2024</p>
                  <p className="text-sm">Auto-renewal enabled</p>
                </div>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>User Seat Management</CardTitle>
              <CardDescription>Track and manage user seats across your organization</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="flex justify-between items-center">
                  <span>Active Users</span>
                  <span className="font-semibold">24/50</span>
                </div>
                <div className="w-full bg-secondary rounded-full h-2">
                  <div className="bg-accent h-2 rounded-full" style={{ width: "48%" }} />
                </div>
                <div className="flex justify-between text-sm text-muted-foreground">
                  <span>26 seats available</span>
                  <span>48% utilized</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="security" className="space-y-6">
          <div>
            <h2 className="text-2xl font-bold">Security Settings</h2>
            <p className="text-muted-foreground">Robust security for peace of mind</p>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Shield className="w-5 h-5 mr-2" />
                  Authentication Settings
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Two-Factor Authentication</Label>
                    <p className="text-sm text-muted-foreground">Require 2FA for all users</p>
                  </div>
                  <Switch defaultChecked />
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Single Sign-On (SSO)</Label>
                    <p className="text-sm text-muted-foreground">Enable SSO integration</p>
                  </div>
                  <Switch />
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Password Complexity</Label>
                    <p className="text-sm text-muted-foreground">Enforce strong passwords</p>
                  </div>
                  <Switch defaultChecked />
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Lock className="w-5 h-5 mr-2" />
                  Access Control
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label>Session Timeout (hours)</Label>
                  <Input type="number" defaultValue="8" />
                </div>
                <div className="space-y-2">
                  <Label>Max Login Attempts</Label>
                  <Input type="number" defaultValue="5" />
                </div>
                <div className="space-y-2">
                  <Label>IP Whitelist</Label>
                  <Textarea placeholder="Enter IP addresses (one per line)" rows={3} />
                </div>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <AlertTriangle className="w-5 h-5 mr-2" />
                Security Alerts
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div className="flex items-center justify-between p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
                  <div className="flex items-center">
                    <AlertTriangle className="w-4 h-4 text-yellow-600 mr-2" />
                    <span className="text-sm">3 failed login attempts from unknown IP</span>
                  </div>
                  <Button variant="outline" size="sm">
                    Review
                  </Button>
                </div>
                <div className="flex items-center justify-between p-3 bg-blue-50 border border-blue-200 rounded-lg">
                  <div className="flex items-center">
                    <Shield className="w-4 h-4 text-blue-600 mr-2" />
                    <span className="text-sm">2FA enabled for 18/24 users</span>
                  </div>
                  <Button variant="outline" size="sm">
                    Enforce
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="analytics" className="space-y-6">
          <div>
            <h2 className="text-2xl font-bold">Analytics Dashboard</h2>
            <p className="text-muted-foreground">Insights at your fingertips</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Daily Active Users</p>
                    <p className="text-2xl font-bold">18</p>
                    <p className="text-xs text-green-600">+12% from yesterday</p>
                  </div>
                  <BarChart3 className="h-8 w-8 text-muted-foreground" />
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Avg Session Duration</p>
                    <p className="text-2xl font-bold">4.2h</p>
                    <p className="text-xs text-green-600">+8% this week</p>
                  </div>
                  <Clock className="h-8 w-8 text-muted-foreground" />
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Feature Adoption</p>
                    <p className="text-2xl font-bold">87%</p>
                    <p className="text-xs text-green-600">+5% this month</p>
                  </div>
                  <Activity className="h-8 w-8 text-muted-foreground" />
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">Support Tickets</p>
                    <p className="text-2xl font-bold">12</p>
                    <p className="text-xs text-red-600">+3 this week</p>
                  </div>
                  <AlertTriangle className="h-8 w-8 text-muted-foreground" />
                </div>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>User Activity Overview</CardTitle>
              <CardDescription>Track user engagement and system usage patterns</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-64 flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <BarChart3 className="h-12 w-12 mx-auto mb-2" />
                  <p>Analytics charts will be displayed here</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="notifications" className="space-y-6">
          <div>
            <h2 className="text-2xl font-bold">Notification Settings</h2>
            <p className="text-muted-foreground">Configure system notifications and alerts</p>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Bell className="w-5 h-5 mr-2" />
                  Email Notifications
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Welcome Emails</Label>
                    <p className="text-sm text-muted-foreground">Send welcome emails to new users</p>
                  </div>
                  <Switch defaultChecked />
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Security Alerts</Label>
                    <p className="text-sm text-muted-foreground">Email security notifications</p>
                  </div>
                  <Switch defaultChecked />
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Billing Reminders</Label>
                    <p className="text-sm text-muted-foreground">Send billing notifications</p>
                  </div>
                  <Switch defaultChecked />
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Bell className="w-5 h-5 mr-2" />
                  In-App Notifications
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <Label>System Updates</Label>
                    <p className="text-sm text-muted-foreground">Notify about system updates</p>
                  </div>
                  <Switch defaultChecked />
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <Label>User Activity</Label>
                    <p className="text-sm text-muted-foreground">Notify about user activities</p>
                  </div>
                  <Switch />
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Performance Alerts</Label>
                    <p className="text-sm text-muted-foreground">System performance notifications</p>
                  </div>
                  <Switch defaultChecked />
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Company Settings Tab - keeping existing implementation */}
        <TabsContent value="company" className="space-y-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Building2 className="w-5 h-5 mr-2" />
                  Company Logo
                </CardTitle>
                <CardDescription>
                  Upload your company logo. This will appear in the header across all pages.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center space-x-4">
                  <div className="w-20 h-20 border-2 border-dashed border-gray-300 rounded-lg flex items-center justify-center overflow-hidden">
                    {companyLogo ? (
                      <img
                        src={companyLogo || "/placeholder.svg"}
                        alt="Company Logo"
                        className="w-full h-full object-contain"
                      />
                    ) : (
                      <ImageIcon className="w-8 h-8 text-gray-400" />
                    )}
                  </div>
                  <div className="flex-1">
                    <Label htmlFor="logo-upload" className="cursor-pointer">
                      <div className="flex items-center space-x-2 text-sm text-blue-600 hover:text-blue-700">
                        <Upload className="w-4 h-4" />
                        <span>Upload Logo</span>
                      </div>
                    </Label>
                    <Input
                      id="logo-upload"
                      type="file"
                      accept="image/*"
                      className="hidden"
                      onChange={handleLogoUpload}
                    />
                    <p className="text-xs text-muted-foreground mt-1">PNG, JPG up to 2MB. Recommended: 200x200px</p>
                  </div>
                </div>

                {companyLogo && (
                  <div className="flex items-center space-x-2">
                    <Button variant="outline" size="sm">
                      <Eye className="w-4 h-4 mr-2" />
                      Preview
                    </Button>
                    <Button variant="outline" size="sm" onClick={() => setCompanyLogo("")}>
                      <Trash2 className="w-4 h-4 mr-2" />
                      Remove
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Building2 className="w-5 h-5 mr-2" />
                  Company Information
                </CardTitle>
                <CardDescription>Basic company information displayed throughout the system.</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="company-name">Company Name</Label>
                  <Input
                    id="company-name"
                    value={companyName}
                    onChange={(e) => setCompanyName(e.target.value)}
                    placeholder="Enter company name"
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="company-description">Description</Label>
                  <Textarea
                    id="company-description"
                    value={companyDescription}
                    onChange={(e) => setCompanyDescription(e.target.value)}
                    placeholder="Brief description of your company"
                    rows={3}
                  />
                </div>

                <Separator />

                <Button onClick={handleSaveCompanySettings} className="w-full">
                  <Save className="w-4 h-4 mr-2" />
                  Save Company Settings
                </Button>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* System Settings Tab - keeping existing implementation */}
        <TabsContent value="system" className="space-y-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Database className="w-5 h-5 mr-2" />
                  System Configuration
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Maintenance Mode</Label>
                    <p className="text-sm text-muted-foreground">Enable system maintenance mode</p>
                  </div>
                  <Switch />
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Auto Backup</Label>
                    <p className="text-sm text-muted-foreground">Automatic daily backups</p>
                  </div>
                  <Switch defaultChecked />
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Debug Mode</Label>
                    <p className="text-sm text-muted-foreground">Enable debug logging</p>
                  </div>
                  <Switch />
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Settings className="w-5 h-5 mr-2" />
                  Performance Settings
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label>Cache Duration (minutes)</Label>
                  <Input type="number" defaultValue="60" />
                </div>
                <div className="space-y-2">
                  <Label>Session Timeout (hours)</Label>
                  <Input type="number" defaultValue="8" />
                </div>
                <div className="space-y-2">
                  <Label>Max File Upload Size (MB)</Label>
                  <Input type="number" defaultValue="10" />
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  )
}
