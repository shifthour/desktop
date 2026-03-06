"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import {
  ArrowDownToLine,
  ArrowUpFromLine,
  Package,
  CheckCircle2,
  Clock,
  XCircle,
  Eye,
  Trash2,
  TrendingUp,
  TrendingDown,
  AlertTriangle,
  Plus
} from "lucide-react"
import { StockEntryModal } from "./stock-entry-modal"
import { format } from "date-fns"
import { toast } from "sonner"

export function StockEntriesContent() {
  const [activeTab, setActiveTab] = useState("all")
  const [stockEntries, setStockEntries] = useState<any[]>([])
  const [products, setProducts] = useState<any[]>([])
  const [stockSummary, setStockSummary] = useState<any>({ summary: [], stats: {} })
  const [loading, setLoading] = useState(false)
  const [showInwardModal, setShowInwardModal] = useState(false)
  const [showOutwardModal, setShowOutwardModal] = useState(false)

  // Get actual user data from localStorage
  const [companyId, setCompanyId] = useState("")
  const [userId, setUserId] = useState("")

  useEffect(() => {
    const storedUser = localStorage.getItem('user')
    if (storedUser) {
      const user = JSON.parse(storedUser)
      setCompanyId(user.company_id || "")
      setUserId(user.id || "")
    }
  }, [])

  useEffect(() => {
    fetchProducts()
    fetchStockEntries()
    fetchStockSummary()
  }, [])

  useEffect(() => {
    console.log('Modal states - Inward:', showInwardModal, 'Outward:', showOutwardModal)
  }, [showInwardModal, showOutwardModal])

  const fetchProducts = async () => {
    try {
      const response = await fetch('/api/products')
      const data = await response.json()
      setProducts(data)
    } catch (error) {
      console.error('Error fetching products:', error)
      toast.error('Failed to fetch products')
    }
  }

  const fetchStockEntries = async (type?: string) => {
    try {
      setLoading(true)
      let url = `/api/stock-entries?companyId=${companyId}`
      if (type && type !== 'all') {
        url += `&entry_type=${type}`
      }
      const response = await fetch(url)
      const data = await response.json()
      setStockEntries(data)
    } catch (error) {
      console.error('Error fetching stock entries:', error)
      toast.error('Failed to fetch stock entries')
    } finally {
      setLoading(false)
    }
  }

  const fetchStockSummary = async () => {
    try {
      const response = await fetch(`/api/stock-summary?companyId=${companyId}`)
      const data = await response.json()
      setStockSummary(data)
    } catch (error) {
      console.error('Error fetching stock summary:', error)
    }
  }

  const handleSaveInward = async (entryData: any) => {
    try {
      const response = await fetch('/api/stock-entries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(entryData)
      })

      if (response.ok) {
        toast.success('Stock inward entry created successfully')
        fetchProducts() // Refresh products to get latest stock
        fetchStockEntries()
        fetchStockSummary()
      } else {
        toast.error('Failed to create stock inward entry')
      }
    } catch (error) {
      console.error('Error creating stock inward entry:', error)
      toast.error('Failed to create stock inward entry')
    }
  }

  const handleSaveOutward = async (entryData: any) => {
    try {
      const response = await fetch('/api/stock-entries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(entryData)
      })

      if (response.ok) {
        toast.success('Stock outward entry created successfully')
        fetchProducts() // Refresh products to get latest stock
        fetchStockEntries()
        fetchStockSummary()
      } else {
        toast.error('Failed to create stock outward entry')
      }
    } catch (error) {
      console.error('Error creating stock outward entry:', error)
      toast.error('Failed to create stock outward entry')
    }
  }

  const handleApproveEntry = async (entryId: string) => {
    try {
      const response = await fetch('/api/stock-entries/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: entryId, companyId, userId })
      })

      if (response.ok) {
        toast.success('Stock entry approved successfully')
        fetchProducts() // Refresh products to get updated stock after approval
        fetchStockEntries()
        fetchStockSummary()
      } else {
        const error = await response.json()
        toast.error(error.error || 'Failed to approve stock entry')
      }
    } catch (error) {
      console.error('Error approving stock entry:', error)
      toast.error('Failed to approve stock entry')
    }
  }

  const handleDeleteEntry = async (entryId: string) => {
    if (!confirm('Are you sure you want to delete this entry?')) return

    try {
      const response = await fetch(`/api/stock-entries?id=${entryId}&companyId=${companyId}`, {
        method: 'DELETE'
      })

      if (response.ok) {
        toast.success('Stock entry deleted successfully')
        fetchStockEntries()
      } else {
        const error = await response.json()
        toast.error(error.error || 'Failed to delete stock entry')
      }
    } catch (error) {
      console.error('Error deleting stock entry:', error)
      toast.error('Failed to delete stock entry')
    }
  }

  const getProductName = (productId: string) => {
    const product = products.find(p => p.id === productId)
    return product ? product.product_name : 'Unknown Product'
  }

  const getProductDetails = (items: any[]) => {
    if (!items || items.length === 0) return { display: 'No items', details: [] }

    const details = items.map(item => ({
      name: getProductName(item.product_id),
      quantity: item.quantity,
      price: item.unit_price
    }))

    if (items.length === 1) {
      return {
        display: details[0].name,
        details
      }
    }

    return {
      display: `${items.length} items: ${details[0].name}${items.length > 1 ? `, +${items.length - 1} more` : ''}`,
      details
    }
  }

  const getStatusBadge = (status: string) => {
    const statusConfig: any = {
      draft: { color: 'bg-gray-100 text-gray-800', icon: Clock },
      submitted: { color: 'bg-blue-100 text-blue-800', icon: Clock },
      approved: { color: 'bg-green-100 text-green-800', icon: CheckCircle2 },
      rejected: { color: 'bg-red-100 text-red-800', icon: XCircle },
      completed: { color: 'bg-green-100 text-green-800', icon: CheckCircle2 }
    }

    const config = statusConfig[status] || statusConfig.draft
    const Icon = config.icon

    return (
      <Badge className={config.color}>
        <Icon className="w-3 h-3 mr-1" />
        {status.charAt(0).toUpperCase() + status.slice(1)}
      </Badge>
    )
  }

  const stats = [
    {
      title: "Total Products",
      value: stockSummary.stats.total_products || 0,
      icon: Package,
      color: "text-blue-600",
      bgColor: "bg-blue-50"
    },
    {
      title: "Total Inward Entries",
      value: stockEntries.filter(e => e.entry_type === 'inward').length || 0,
      icon: ArrowDownToLine,
      color: "text-green-600",
      bgColor: "bg-green-50"
    },
    {
      title: "Total Outward Entries",
      value: stockEntries.filter(e => e.entry_type === 'outward').length || 0,
      icon: ArrowUpFromLine,
      color: "text-orange-600",
      bgColor: "bg-orange-50"
    },
    {
      title: "Total Stock Value",
      value: `₹${(stockSummary.stats.total_stock_value || 0).toFixed(2)}`,
      icon: TrendingUp,
      color: "text-purple-600",
      bgColor: "bg-purple-50"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Stock Entries</h1>
          <p className="text-gray-500 mt-1">Manage stock inward and outward entries</p>
        </div>
        <div className="flex items-center space-x-3">
          <Button
            onClick={() => {
              console.log('Stock Inward button clicked')
              fetchProducts() // Refresh products before opening modal
              setShowInwardModal(true)
            }}
            className="bg-green-600 hover:bg-green-700"
          >
            <ArrowDownToLine className="w-4 h-4 mr-2" />
            Stock Inward
          </Button>
          <Button
            onClick={() => {
              console.log('Stock Outward button clicked')
              fetchProducts() // Refresh products before opening modal to get latest stock
              setShowOutwardModal(true)
            }}
            className="bg-orange-600 hover:bg-orange-700"
          >
            <ArrowUpFromLine className="w-4 h-4 mr-2" />
            Stock Outward
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {stats.map((stat, index) => (
          <Card key={index}>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-gray-600">{stat.title}</p>
                  <p className="text-2xl font-bold text-gray-900">{stat.value}</p>
                </div>
                <div className={`w-12 h-12 ${stat.bgColor} rounded-lg flex items-center justify-center`}>
                  <stat.icon className={`w-6 h-6 ${stat.color}`} />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Stock Entries Table */}
      <Card>
        <CardHeader>
          <CardTitle>Stock Entry Records</CardTitle>
          <CardDescription>View and manage all stock entries</CardDescription>
        </CardHeader>
        <CardContent>
          <Tabs value={activeTab} onValueChange={(value) => {
            setActiveTab(value)
            fetchStockEntries(value === 'all' ? undefined : value)
          }}>
            <TabsList>
              <TabsTrigger value="all">All Entries</TabsTrigger>
              <TabsTrigger value="inward">Inward</TabsTrigger>
              <TabsTrigger value="outward">Outward</TabsTrigger>
            </TabsList>

            <TabsContent value={activeTab} className="mt-6">
              <div className="rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Entry Number</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Date</TableHead>
                      <TableHead>Party Name</TableHead>
                      <TableHead>Items</TableHead>
                      <TableHead className="text-right">Total Qty</TableHead>
                      <TableHead className="text-right">Total Value</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {loading ? (
                      <TableRow>
                        <TableCell colSpan={9} className="text-center py-8">
                          Loading...
                        </TableCell>
                      </TableRow>
                    ) : stockEntries.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={9} className="text-center py-8">
                          No stock entries found
                        </TableCell>
                      </TableRow>
                    ) : (
                      stockEntries.map((entry) => {
                        const productInfo = getProductDetails(entry.stock_entry_items || [])
                        return (
                          <TableRow key={entry.id}>
                            <TableCell className="font-medium">{entry.entry_number}</TableCell>
                            <TableCell>
                              <Badge variant={entry.entry_type === 'inward' ? 'default' : 'secondary'}>
                                {entry.entry_type === 'inward' ? (
                                  <ArrowDownToLine className="w-3 h-3 mr-1" />
                                ) : (
                                  <ArrowUpFromLine className="w-3 h-3 mr-1" />
                                )}
                                {entry.entry_type}
                              </Badge>
                            </TableCell>
                            <TableCell>{format(new Date(entry.entry_date), 'dd MMM yyyy')}</TableCell>
                            <TableCell>{entry.party_name || '-'}</TableCell>
                            <TableCell>
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <div className="cursor-help">
                                      {productInfo.display}
                                    </div>
                                  </TooltipTrigger>
                                  <TooltipContent className="max-w-sm">
                                    <div className="space-y-2">
                                      <p className="font-semibold text-sm">Product Details:</p>
                                      {productInfo.details.map((detail, idx) => (
                                        <div key={idx} className="text-xs border-b pb-1 last:border-b-0">
                                          <p className="font-medium">{detail.name}</p>
                                          <p className="text-gray-600">Qty: {detail.quantity} | Price: ₹{detail.price?.toFixed(2)}</p>
                                        </div>
                                      ))}
                                    </div>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            </TableCell>
                            <TableCell className="text-right">{entry.total_quantity}</TableCell>
                            <TableCell className="text-right">₹{entry.total_value?.toFixed(2) || '0.00'}</TableCell>
                            <TableCell>{getStatusBadge(entry.status)}</TableCell>
                          <TableCell className="text-right">
                            <div className="flex items-center justify-end space-x-2">
                              {(entry.status === 'draft' || entry.status === 'submitted') && (
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() => handleApproveEntry(entry.id)}
                                >
                                  <CheckCircle2 className="w-4 h-4" />
                                </Button>
                              )}
                              {(entry.status === 'draft') && (
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() => handleDeleteEntry(entry.id)}
                                >
                                  <Trash2 className="w-4 h-4 text-red-600" />
                                </Button>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                        )
                      })
                    )}
                  </TableBody>
                </Table>
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {/* Stock Summary */}
      <Card>
        <CardHeader>
          <CardTitle>Stock Summary</CardTitle>
          <CardDescription>Current stock levels by product</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Product Name</TableHead>
                  <TableHead>Reference No.</TableHead>
                  <TableHead>Category</TableHead>
                  <TableHead className="text-right">Current Stock</TableHead>
                  <TableHead>Location</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {stockSummary.summary?.map((item: any) => (
                  <TableRow key={item.product_id}>
                    <TableCell className="font-medium">{item.product_name}</TableCell>
                    <TableCell>{item.product_reference_no || '-'}</TableCell>
                    <TableCell>{item.category || '-'}</TableCell>
                    <TableCell className="text-right">{item.stock_quantity}</TableCell>
                    <TableCell>{item.bin_location || '-'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      {/* Modals */}
      {showInwardModal && (
        <StockEntryModal
          open={showInwardModal}
          onClose={() => setShowInwardModal(false)}
          onSave={handleSaveInward}
          entryType="inward"
          products={products}
          companyId={companyId}
          userId={userId}
        />
      )}

      {showOutwardModal && (
        <StockEntryModal
          open={showOutwardModal}
          onClose={() => setShowOutwardModal(false)}
          onSave={handleSaveOutward}
          entryType="outward"
          products={products}
          companyId={companyId}
          userId={userId}
        />
      )}
    </div>
  )
}
