"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Progress } from "@/components/ui/progress"
import {
  Plus,
  Search,
  Download,
  Eye,
  Edit,
  Phone,
  Mail,
  Store,
  DollarSign,
  Target,
  CreditCard,
  Award,
  ShoppingCart,
} from "lucide-react"

const dealers = [
  {
    id: "DLR-001",
    name: "TechLab Solutions Pvt Ltd",
    type: "Distributor",
    region: "South India",
    city: "Bangalore",
    state: "Karnataka",
    contactPerson: "Mr. Rajesh Kumar",
    phone: "+91-80-12345678",
    email: "rajesh@techlab.com",
    status: "Active",
    creditLimit: "₹50,00,000",
    creditUsed: "₹32,50,000",
    creditUtilization: 65,
    ytdSales: "₹2,45,67,890",
    ytdTarget: "₹3,00,00,000",
    targetAchievement: 82,
    lastOrder: "2025-01-10",
    rating: 4.5,
    tier: "Platinum",
    subDealers: 12,
    territory: ["Bangalore", "Mysore", "Mangalore"],
  },
  {
    id: "DLR-002",
    name: "Scientific Instruments Co.",
    type: "Franchisee",
    region: "West India",
    city: "Mumbai",
    state: "Maharashtra",
    contactPerson: "Ms. Priya Sharma",
    phone: "+91-22-87654321",
    email: "priya@scientific.com",
    status: "Active",
    creditLimit: "₹30,00,000",
    creditUsed: "₹18,75,000",
    creditUtilization: 63,
    ytdSales: "₹1,89,45,670",
    ytdTarget: "₹2,50,00,000",
    targetAchievement: 76,
    lastOrder: "2025-01-08",
    rating: 4.2,
    tier: "Gold",
    subDealers: 8,
    territory: ["Mumbai", "Pune", "Nashik"],
  },
  {
    id: "DLR-003",
    name: "LabEquip North",
    type: "Sub-Dealer",
    region: "North India",
    city: "Delhi",
    state: "Delhi",
    contactPerson: "Mr. Amit Singh",
    phone: "+91-11-98765432",
    email: "amit@labequip.com",
    status: "Active",
    creditLimit: "₹15,00,000",
    creditUsed: "₹8,25,000",
    creditUtilization: 55,
    ytdSales: "₹95,23,450",
    ytdTarget: "₹1,20,00,000",
    targetAchievement: 79,
    lastOrder: "2025-01-12",
    rating: 4.0,
    tier: "Silver",
    subDealers: 0,
    territory: ["Delhi", "Gurgaon", "Noida"],
  },
]

const dealerStats = [
  {
    title: "Total Dealers",
    value: "89",
    change: { value: "+5", type: "positive" as const, period: "this quarter" },
    icon: Store,
  },
  {
    title: "Active Revenue",
    value: "₹8.45Cr",
    change: { value: "+18.2%", type: "positive" as const, period: "YTD" },
    icon: DollarSign,
  },
  {
    title: "Avg Target Achievement",
    value: "79%",
    change: { value: "+3.5%", type: "positive" as const, period: "vs last year" },
    icon: Target,
  },
  {
    title: "Credit Utilization",
    value: "61%",
    change: { value: "-2.1%", type: "positive" as const, period: "vs last month" },
    icon: CreditCard,
  },
]

const recentActivities = [
  {
    dealer: "TechLab Solutions",
    activity: "Placed order for ₹2.5L worth of analytical balances",
    time: "2 hours ago",
    type: "order",
  },
  {
    dealer: "Scientific Instruments Co.",
    activity: "Requested credit limit increase to ₹40L",
    time: "4 hours ago",
    type: "credit",
  },
  {
    dealer: "LabEquip North",
    activity: "Completed training program for new product line",
    time: "1 day ago",
    type: "training",
  },
]

export function DealersContent() {
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedType, setSelectedType] = useState("All")
  const [selectedRegion, setSelectedRegion] = useState("All")
  const [selectedStatus, setSelectedStatus] = useState("All")

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "active":
        return "bg-green-100 text-green-800"
      case "inactive":
        return "bg-red-100 text-red-800"
      case "pending":
        return "bg-yellow-100 text-yellow-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getTierColor = (tier: string) => {
    switch (tier.toLowerCase()) {
      case "platinum":
        return "bg-purple-100 text-purple-800"
      case "gold":
        return "bg-yellow-100 text-yellow-800"
      case "silver":
        return "bg-gray-100 text-gray-800"
      default:
        return "bg-blue-100 text-blue-800"
    }
  }

  const getCreditUtilizationColor = (utilization: number) => {
    if (utilization >= 90) return "text-red-600"
    if (utilization >= 70) return "text-yellow-600"
    return "text-green-600"
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dealers & Franchisees Management</h1>
          <p className="text-gray-600">Manage your distribution network and partner relationships</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Add Dealer
          </Button>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {dealerStats.map((stat) => (
          <Card key={stat.title}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">{stat.title}</CardTitle>
              <stat.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
              <p className="text-xs text-muted-foreground">
                <span className={stat.change.type === "positive" ? "text-green-600" : "text-red-600"}>
                  {stat.change.value}
                </span>{" "}
                {stat.change.period}
              </p>
            </CardContent>
          </Card>
        ))}
      </div>

      <Tabs defaultValue="overview" className="space-y-6">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="performance">Performance</TabsTrigger>
          <TabsTrigger value="territory">Territory Map</TabsTrigger>
          <TabsTrigger value="activities">Activities</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="space-y-6">
          {/* Search and Filters */}
          <Card>
            <CardHeader>
              <CardTitle>Dealer Search & Filters</CardTitle>
              <CardDescription>Filter and search through your dealer network</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col md:flex-row gap-4">
                <div className="flex-1">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                    <Input
                      placeholder="Search dealers by name, contact, or location..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="pl-10"
                    />
                  </div>
                </div>
                <Select value={selectedType} onValueChange={setSelectedType}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Dealer Type" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Types</SelectItem>
                    <SelectItem value="Distributor">Distributor</SelectItem>
                    <SelectItem value="Franchisee">Franchisee</SelectItem>
                    <SelectItem value="Sub-Dealer">Sub-Dealer</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={selectedRegion} onValueChange={setSelectedRegion}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Region" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Regions</SelectItem>
                    <SelectItem value="North India">North India</SelectItem>
                    <SelectItem value="South India">South India</SelectItem>
                    <SelectItem value="East India">East India</SelectItem>
                    <SelectItem value="West India">West India</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={selectedStatus} onValueChange={setSelectedStatus}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Status" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Status</SelectItem>
                    <SelectItem value="Active">Active</SelectItem>
                    <SelectItem value="Inactive">Inactive</SelectItem>
                    <SelectItem value="Pending">Pending</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          {/* Dealers Table */}
          <Card>
            <CardHeader>
              <CardTitle>Dealer Network</CardTitle>
              <CardDescription>Complete overview of your distribution partners</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Dealer Info</TableHead>
                      <TableHead>Type & Region</TableHead>
                      <TableHead>Contact</TableHead>
                      <TableHead>Credit Status</TableHead>
                      <TableHead>Performance</TableHead>
                      <TableHead>Territory</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {dealers.map((dealer) => (
                      <TableRow key={dealer.id}>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="font-medium">{dealer.name}</div>
                            <div className="text-sm text-gray-500">{dealer.id}</div>
                            <div className="flex items-center space-x-2">
                              <Badge className={getStatusColor(dealer.status)}>{dealer.status}</Badge>
                              <Badge className={getTierColor(dealer.tier)}>{dealer.tier}</Badge>
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="font-medium">{dealer.type}</div>
                            <div className="text-sm text-gray-500">{dealer.region}</div>
                            <div className="text-sm text-gray-500">
                              {dealer.city}, {dealer.state}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="font-medium">{dealer.contactPerson}</div>
                            <div className="text-sm text-gray-500">{dealer.phone}</div>
                            <div className="text-sm text-gray-500">{dealer.email}</div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-2">
                            <div className="text-sm">
                              <span className="font-medium">Limit:</span> {dealer.creditLimit}
                            </div>
                            <div className="text-sm">
                              <span className="font-medium">Used:</span> {dealer.creditUsed}
                            </div>
                            <Progress value={dealer.creditUtilization} className="h-2" />
                            <div
                              className={`text-xs font-medium ${getCreditUtilizationColor(dealer.creditUtilization)}`}
                            >
                              {dealer.creditUtilization}% utilized
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-2">
                            <div className="text-sm">
                              <span className="font-medium">YTD Sales:</span> {dealer.ytdSales}
                            </div>
                            <div className="text-sm">
                              <span className="font-medium">Target:</span> {dealer.ytdTarget}
                            </div>
                            <Progress value={dealer.targetAchievement} className="h-2" />
                            <div className="text-xs font-medium text-green-600">
                              {dealer.targetAchievement}% achieved
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="text-sm font-medium">{dealer.subDealers} Sub-dealers</div>
                            <div className="text-xs text-gray-500">{dealer.territory.join(", ")}</div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex space-x-1">
                            <Button variant="ghost" size="sm" title="View Details">
                              <Eye className="w-4 h-4" />
                            </Button>
                            <Button variant="ghost" size="sm" title="Edit">
                              <Edit className="w-4 h-4" />
                            </Button>
                            <Button variant="ghost" size="sm" title="Call">
                              <Phone className="w-4 h-4" />
                            </Button>
                            <Button variant="ghost" size="sm" title="Email">
                              <Mail className="w-4 h-4" />
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

        <TabsContent value="activities" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Recent Dealer Activities</CardTitle>
              <CardDescription>Latest activities from your dealer network</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {recentActivities.map((activity, index) => (
                  <div key={index} className="flex items-start space-x-4 p-4 border rounded-lg">
                    <div className="w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center">
                      {activity.type === "order" && <ShoppingCart className="w-5 h-5 text-blue-600" />}
                      {activity.type === "credit" && <CreditCard className="w-5 h-5 text-blue-600" />}
                      {activity.type === "training" && <Award className="w-5 h-5 text-blue-600" />}
                    </div>
                    <div className="flex-1">
                      <div className="font-medium">{activity.dealer}</div>
                      <div className="text-sm text-gray-600">{activity.activity}</div>
                      <div className="text-xs text-gray-500 mt-1">{activity.time}</div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
