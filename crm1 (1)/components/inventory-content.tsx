"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Package, FileText, ShoppingCart, Receipt, Plus, ArrowRight, TrendingUp, TrendingDown, AlertTriangle, ArrowDownToLine, ArrowUpFromLine } from "lucide-react"

export function InventoryContent() {
  const inventoryStats = [
    {
      title: "Total Products",
      value: "2,456",
      change: "+8.2%",
      icon: Package,
      color: "text-blue-600",
      bgColor: "bg-blue-50",
      trend: "up"
    },
    {
      title: "Active Quotations",
      value: "89",
      change: "+12.5%", 
      icon: FileText,
      color: "text-green-600",
      bgColor: "bg-green-50",
      trend: "up"
    },
    {
      title: "Pending Orders",
      value: "156",
      change: "+15.3%",
      icon: ShoppingCart,
      color: "text-purple-600",
      bgColor: "bg-purple-50",
      trend: "up"
    },
    {
      title: "Outstanding Invoices",
      value: "₹12.5L",
      change: "-5.2%",
      icon: Receipt,
      color: "text-orange-600",
      bgColor: "bg-orange-50",
      trend: "down"
    },
  ]

  const quickActions = [
    {
      title: "Products",
      description: "Manage your product catalog and inventory",
      icon: Package,
      href: "/products",
      badge: "2K+",
      color: "bg-blue-600"
    },
    {
      title: "Stock Entries",
      description: "Manage stock inward and outward entries",
      icon: ArrowDownToLine,
      href: "/stock-entries",
      badge: "New",
      color: "bg-purple-600"
    },
    {
      title: "Quotations",
      description: "Create and manage price quotations",
      icon: FileText,
      href: "/quotations",
      badge: "12",
      color: "bg-green-600"
    },
    {
      title: "Sales Orders",
      description: "Track and process customer orders",
      icon: ShoppingCart,
      href: "/sales-orders",
      badge: "23",
      color: "bg-teal-600"
    },
    {
      title: "Invoices",
      description: "Generate and manage customer invoices",
      icon: Receipt,
      href: "/invoices",
      badge: "45",
      color: "bg-orange-600"
    }
  ]

  const recentActivity = [
    {
      type: "product",
      title: "New product added: Advanced Centrifuge Model X200",
      time: "2 hours ago",
      icon: Package,
      color: "text-blue-600"
    },
    {
      type: "quotation",
      title: "Quotation QT-2024-089 sent to Kerala Agricultural University",
      time: "4 hours ago", 
      icon: FileText,
      color: "text-green-600"
    },
    {
      type: "order",
      title: "Sales Order SO-2024-156 confirmed by TSAR Labcare",
      time: "6 hours ago",
      icon: ShoppingCart,
      color: "text-purple-600"
    },
    {
      type: "invoice",
      title: "Invoice INV-2024-045 generated for Eurofins Advinus",
      time: "8 hours ago",
      icon: Receipt,
      color: "text-orange-600"
    },
    {
      type: "alert",
      title: "Low stock alert: Fibrinometer accessories (5 units remaining)",
      time: "1 day ago",
      icon: AlertTriangle,
      color: "text-red-600"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Inventory Management</h1>
          <p className="text-gray-500 mt-1">Manage products, quotations, orders, and invoices</p>
        </div>
        <div className="flex items-center space-x-3">
          <Button className="bg-blue-600 hover:bg-blue-700" size="sm">
            <Plus className="w-4 h-4 mr-2" />
            Quick Add
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {inventoryStats.map((stat, index) => (
          <Card key={index}>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-gray-600">{stat.title}</p>
                  <p className="text-2xl font-bold text-gray-900">{stat.value}</p>
                  <div className="flex items-center mt-1">
                    {stat.trend === "up" ? (
                      <TrendingUp className="w-3 h-3 text-green-600 mr-1" />
                    ) : (
                      <TrendingDown className="w-3 h-3 text-red-600 mr-1" />
                    )}
                    <p className={`text-xs ${stat.trend === "up" ? "text-green-600" : "text-red-600"}`}>
                      {stat.change} vs last month
                    </p>
                  </div>
                </div>
                <div className={`w-12 h-12 ${stat.bgColor} rounded-lg flex items-center justify-center`}>
                  <stat.icon className={`w-6 h-6 ${stat.color}`} />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Quick Actions */}
      <Card>
        <CardHeader>
          <CardTitle>Quick Actions</CardTitle>
          <CardDescription>Access key inventory management functions</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
            {quickActions.map((action, index) => (
              <Card key={index} className="hover:shadow-md transition-shadow cursor-pointer">
                <CardContent className="p-6">
                  <div className="flex items-start justify-between mb-4">
                    <div className={`w-12 h-12 ${action.color} rounded-lg flex items-center justify-center`}>
                      <action.icon className="w-6 h-6 text-white" />
                    </div>
                    <Badge variant="outline" className="text-xs">
                      {action.badge}
                    </Badge>
                  </div>
                  <div className="space-y-2">
                    <h3 className="font-semibold text-gray-900">{action.title}</h3>
                    <p className="text-sm text-gray-600">{action.description}</p>
                  </div>
                  <div className="mt-4">
                    <Button variant="ghost" size="sm" className="w-full justify-between p-0">
                      <span>Manage</span>
                      <ArrowRight className="w-4 h-4" />
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Recent Activity */}
      <Card>
        <CardHeader>
          <CardTitle>Recent Activity</CardTitle>
          <CardDescription>Latest inventory-related updates and changes</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {recentActivity.map((activity, index) => (
              <div key={index} className="flex items-start space-x-4 p-4 hover:bg-gray-50 rounded-lg transition-colors">
                <div className={`w-10 h-10 bg-gray-100 rounded-lg flex items-center justify-center flex-shrink-0`}>
                  <activity.icon className={`w-5 h-5 ${activity.color}`} />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-900">{activity.title}</p>
                  <p className="text-xs text-gray-500 mt-1">{activity.time}</p>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}