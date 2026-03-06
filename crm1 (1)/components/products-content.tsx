"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Plus, Search, Download, Edit, Package, Upload, ChevronDown, ChevronUp, X, ShoppingCart, CheckCircle, Grid3X3, IndianRupee, Trash2 } from "lucide-react"
// import { AIProductRecommendations } from "@/components/ai-product-recommendations"
import { DynamicImportModal } from "@/components/dynamic-import-modal"
import { useToast } from "@/hooks/use-toast"
import { exportToExcel } from "@/lib/excel-export"

// All product data now comes from Supabase backend - no mock data

// Dynamic categories will be populated from actual product data
const statuses = ["All", "Active", "Inactive"]

export function ProductsContent() {
  const router = useRouter()
  const { toast } = useToast()
  const [productsList, setProductsList] = useState<any[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [isLoadingStats, setIsLoadingStats] = useState(true)
  const [productsStats, setProductsStats] = useState({
    total: 0,
    active: 0,
    categories: 0,
    avgValue: 0
  })
  const [selectedPrincipal, setSelectedPrincipal] = useState("All")
  const [selectedCategory, setSelectedCategory] = useState("All")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [availablePrincipals, setAvailablePrincipals] = useState<string[]>(["All"])
  const [availableCategories, setAvailableCategories] = useState<string[]>(["All"])
  const [isDataImportModalOpen, setIsDataImportModalOpen] = useState(false)
  const [expandedDescriptions, setExpandedDescriptions] = useState<Set<number>>(new Set())
  const [isImporting, setIsImporting] = useState(false)
  const [importProgress, setImportProgress] = useState({ current: 0, total: 0 })
  const [zoomedImage, setZoomedImage] = useState<{ url: string; name: string } | null>(null)
  const [deletingProductId, setDeletingProductId] = useState<string | null>(null)

  // Load products from database on component mount
  useEffect(() => {
    loadProducts()
  }, [])

  const loadProducts = async () => {
    try {
      // Load ALL products without any filtering
      const response = await fetch(`/api/products`)
      
      if (!response.ok) {
        console.error('Failed to load products')
        // No fallback data - show empty state
        setProductsList([])
        calculateStats([])
        extractFilterOptions([])
        
        toast({
          title: "Error loading products",
          description: "Failed to load products from database. Please try refreshing the page.",
          variant: "destructive"
        })
        return
      }
      
      const data = await response.json()
      console.log('Raw API data:', data)
      
      const formattedProducts = data.map((product: any) => {
        console.log('Processing product:', product)
        return {
          id: product.id,
          branch: product.branch || '',
          category: product.category || '',
          subCategory: product.sub_category || '',
          principal: product.principal || '',
          productName: product.product_name,
          productCode: product.product_code || '',
          refNo: product.product_reference_no || '',
          assignedTo: product.assigned_to || 'Hari Kumar K, Prashanth Sandily',
          status: product.status || 'Active',
          productStatus: product.product_status || '',
          price: product.price ? `₹${product.price.toLocaleString()}` : null,
          basePrice: product.base_price || null,
          costPrice: product.cost_price || null,
          profitMargin: product.profit_margin || null,
          currency: product.currency || 'INR',
          description: product.description || product.product_description || '',
          shortDescription: product.short_description || '',
          productTags: product.product_tags || '',
          inventoryType: product.inventory_type || '',
          currentStockLevel: product.current_stock_level || '',
          reorderPoint: product.reorder_point || '',
          technicalSpecifications: product.technical_specifications || '',
          dataClassification: product.data_classification || '',
          product_picture: product.product_picture || null,
          primaryImageUrl: product.primary_image_url || null,
        }
      })
      
      console.log('Formatted products:', formattedProducts)
      setProductsList(formattedProducts)
      calculateStats(formattedProducts)
      extractFilterOptions(formattedProducts)
    } catch (error) {
      console.error('Error loading products:', error)
      // No fallback data - show empty state
      setProductsList([])
      calculateStats([])
      extractFilterOptions([])
      
      toast({
        title: "Error loading products",
        description: "Failed to load products from database. Please try refreshing the page.",
        variant: "destructive"
      })
    }
  }
  
  const calculateStats = (productsList: any[]) => {
    const activeProducts = productsList.filter(product => product.status === 'Active').length
    const categories = [...new Set(productsList.map(product => product.category))].length
    const avgValue = productsList.length > 0 
      ? productsList.reduce((sum, product) => {
          const value = parseFloat(product.price?.replace(/[₹,]/g, '') || '0')
          return sum + value
        }, 0) / productsList.length 
      : 0
    
    setProductsStats({
      total: productsList.length,
      active: activeProducts,
      categories: categories,
      avgValue: avgValue
    })
    
    setIsLoadingStats(false)
  }

  const extractFilterOptions = (productsList: any[]) => {
    // Extract unique principals
    const principals = [...new Set(productsList
      .map(product => product.principal)
      .filter(principal => principal && principal.trim() !== ''))]
      .sort()
    setAvailablePrincipals(["All", ...principals])

    // Extract unique categories  
    const categories = [...new Set(productsList
      .map(product => product.category)
      .filter(category => category && category.trim() !== ''))]
      .sort()
    setAvailableCategories(["All", ...categories])
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
      if (!user.company_id) {
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
        description: `Processing ${data.length} products...`
      })
      
      // Function to download and convert image to base64
      const downloadImageAsBase64 = async (imageUrl: string): Promise<string | null> => {
        try {
          if (!imageUrl || imageUrl.trim() === '') return null
          
          const response = await fetch(imageUrl)
          if (!response.ok) return null
          
          const blob = await response.blob()
          return new Promise((resolve) => {
            const reader = new FileReader()
            reader.onload = () => resolve(reader.result as string)
            reader.onerror = () => resolve(null)
            reader.readAsDataURL(blob)
          })
        } catch (error) {
          console.error('Error downloading image:', error)
          return null
        }
      }

      // Prepare products for import - flexible field mapping
      const productsToImport = data.map(item => {
        // Try multiple possible field names for product name
        const productName = item.product_name || item['Product Name'] || item.productName ||
                           item['Product name'] || item.name || item.Name || ''

        // Helper to parse numeric values
        const parseNum = (value: any) => {
          const parsed = parseFloat(value)
          return isNaN(parsed) ? null : parsed
        }

        return {
          product_name: productName,
          product_reference_no: item.product_reference_no || item['Product Reference No/ID'] || item.productReferenceNo ||
                               item.refNo || item['Ref No'] || item.reference_no || '',
          product_code: item.product_code || item['Product Code'] || item.productCode || item.code || item.Code || '',
          description: item.description || item.Description || item['Product Description'] || '',
          product_description: item.product_description || item['Product Description'] || item.description || '',
          short_description: item.short_description || item['Short Description'] || item.shortDescription || '',
          principal: item.principal || item.Principal || item['Principal Company'] || '',
          category: item.category || item.Category || item['Product Category'] || '',
          sub_category: item.sub_category || item['Sub Category'] || item.subCategory || item.subcategory || '',
          price: parseNum(item.price || item.Price || item['Product Price']),
          base_price: parseNum(item.base_price || item['Base Price'] || item.basePrice),
          cost_price: parseNum(item.cost_price || item['Cost Price'] || item.costPrice),
          profit_margin: parseNum(item.profit_margin || item['Profit Margin'] || item.profitMargin),
          currency: item.currency || item.Currency || 'INR',
          branch: item.branch || item.Branch || item['Branch/Division'] || '',
          status: item.status || item.Status || 'Active',
          product_status: item.product_status || item['Product Status'] || item.productStatus || '',
          assigned_to: item.assigned_to || item['Assigned To'] || item.assignedTo || 'Hari Kumar K, Prashanth Sandily',
          inventory_type: item.inventory_type || item['Inventory Type'] || item.inventoryType || '',
          current_stock_level: parseNum(item.current_stock_level || item['Current Stock Level'] || item.currentStockLevel || item.stock),
          reorder_point: parseNum(item.reorder_point || item['Reorder Point'] || item.reorderPoint),
          technical_specifications: item.technical_specifications || item['Technical Specifications'] ||
                                   item.technicalSpecifications || item.tech_specs || item['Tech Specs'] || '',
          data_classification: item.data_classification || item['Data Classification'] ||
                              item.dataClassification || item.classification || '',
          product_tags: item.product_tags || item['Product Tags'] || item.productTags || item.tags || '',
          image_file_url: item.image_file_url || item['Image File URL'] || item.imageFileUrl || item['Image URL'] || '',
          company_id: user.company_id
        }
      })
      
      let successCount = 0
      let failCount = 0
      const errors: string[] = []

      console.log("Processed products for import:", productsToImport.length)
      
      // Load existing products once for duplicate checking
      const existingResponse = await fetch(`/api/products?companyId=${user.company_id}`)
      let existingProducts = []
      if (existingResponse.ok) {
        const existingData = await existingResponse.json()
        existingProducts = existingData.map((product: any) => ({
          productName: product.product_name,
          refNo: product.product_reference_no || '',
        }))
      }
      
      console.log("Existing products for duplicate check:", existingProducts.length)
      
      // Import products one by one with progress updates
      for (let i = 0; i < productsToImport.length; i++) {
        const product = productsToImport[i]

        // Update progress
        setImportProgress({ current: i + 1, total: productsToImport.length })

        const rowNumber = i + 2 // Excel row number (accounting for header)
        const productIdentifier = product.product_name || `Row ${rowNumber}`

        if (!product.product_name || product.product_name.trim() === '') {
          console.log(`Skipping row ${rowNumber} with no product name:`, product)
          failCount++
          errors.push(`Row ${rowNumber}: Missing required field (Product Name)`)
          continue
        }

        // Check for duplicates based on product name + reference ID combination
        const isDuplicate = existingProducts.some(existingProduct => {
          const sameName = existingProduct.productName?.toLowerCase() === product.product_name?.toLowerCase()
          const sameRefNo = existingProduct.refNo?.toLowerCase() === product.product_reference_no?.toLowerCase()
          
          // Consider duplicate if both name and reference number match
          // If reference number is empty for both, only check name
          if (!product.product_reference_no?.trim() && !existingProduct.refNo?.trim()) {
            return sameName
          }
          return sameName && sameRefNo
        })

        if (isDuplicate) {
          console.log(`Skipping duplicate product: ${product.product_name} (Ref: ${product.product_reference_no})`)
          failCount++
          errors.push(`Row ${rowNumber} (${productIdentifier}): Duplicate product - already exists`)
          continue
        }
        
        try {
          console.log("Importing product:", product.product_name)
          
          // Download image if URL is provided
          let productPicture = null
          if (product.image_file_url && product.image_file_url.trim() !== '') {
            console.log("Downloading image for:", product.product_name)
            productPicture = await downloadImageAsBase64(product.image_file_url)
            if (productPicture) {
              console.log("Image downloaded successfully for:", product.product_name)
            } else {
              console.log("Failed to download image for:", product.product_name)
            }
          }
          
          // Prepare product data with image
          const productData = {
            ...product,
            product_picture: productPicture,
            companyId: user.company_id
          }
          
          // Remove image_file_url from the data being sent to API
          delete productData.image_file_url
          
          const response = await fetch('/api/products', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify(productData),
          })
          
          if (response.ok) {
            successCount++
            console.log(`Successfully imported product: ${product.product_name}`)

            // Add to existing products list to prevent duplicates within this import batch
            existingProducts.push({
              productName: product.product_name,
              refNo: product.product_reference_no || ''
            })
          } else {
            const errorData = await response.text()
            console.error(`Failed to import product ${product.product_name}:`, errorData)
            failCount++

            // Try to parse error message from API response
            try {
              const errorJson = JSON.parse(errorData)
              const errorMessage = errorJson.message || errorJson.error || 'API error'
              errors.push(`Row ${rowNumber} (${productIdentifier}): ${errorMessage}`)
            } catch {
              errors.push(`Row ${rowNumber} (${productIdentifier}): Failed to import - ${errorData.substring(0, 100)}`)
            }
          }

          // Small delay to prevent overwhelming the server
          await new Promise(resolve => setTimeout(resolve, 100))

        } catch (error) {
          console.error(`Error importing product ${product.product_name}:`, error)
          failCount++
          errors.push(`Row ${rowNumber} (${productIdentifier}): Failed to import - ${error instanceof Error ? error.message : 'Unknown error'}`)
        }
      }
      
      // Clear loading state and close modal
      setIsImporting(false)
      setImportProgress({ current: 0, total: 0 })
      setIsDataImportModalOpen(false)

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
          description: `Successfully imported ${successCount}/${data.length} products.`
        })
      }

      // Reload products to show the imported ones
      await loadProducts()
      
    } catch (error) {
      console.error('Error during import:', error)
      
      // Clear loading state on error
      setIsImporting(false)
      setImportProgress({ current: 0, total: 0 })
      
      toast({
        title: "Import failed", 
        description: "There was an error during the import process. Please try again.",
        variant: "destructive"
      })
    }
  }

  const handleAddProduct = () => {
    router.push('/products/add')
  }

  const handleEditProduct = (product: any) => {
    router.push(`/products/edit/${product.id}`)
  }

  const toggleDescription = (productId: number) => {
    setExpandedDescriptions(prev => {
      const newSet = new Set(prev)
      if (newSet.has(productId)) {
        newSet.delete(productId)
      } else {
        newSet.add(productId)
      }
      return newSet
    })
  }

  const handleImageClick = (imageUrl: string, productName: string) => {
    setZoomedImage({ url: imageUrl, name: productName })
  }

  const closeImageZoom = () => {
    setZoomedImage(null)
  }

  const handleExport = async () => {
    try {
      const success = await exportToExcel(productsList, {
        filename: `products_${new Date().toISOString().split('T')[0]}`,
        sheetName: 'Products',
        columns: [
          { key: 'productName', label: 'Product Name', width: 30 },
          { key: 'refNo', label: 'Reference No', width: 15 },
          { key: 'category', label: 'Category', width: 20 },
          { key: 'principal', label: 'Principal', width: 20 },
          { key: 'branch', label: 'Branch', width: 15 },
          { key: 'description', label: 'Description', width: 40 },
          { key: 'price', label: 'Price', width: 12 },
          { key: 'status', label: 'Status', width: 12 },
          { key: 'assignedTo', label: 'Assigned To', width: 20 }
        ]
      })

      if (success) {
        toast({
          title: "Data exported",
          description: "Products data has been exported to Excel file."
        })
      } else {
        toast({
          title: "Export failed",
          description: "Failed to export products data. Please try again.",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Export error:', error)
      toast({
        title: "Export failed",
        description: "Failed to export products data. Please try again.",
        variant: "destructive"
      })
    }
  }

  const handleDeleteProduct = async (productId: string, productName: string) => {
    if (!confirm(`Are you sure you want to delete "${productName}"? This action cannot be undone.`)) {
      return
    }

    setDeletingProductId(productId)

    try {
      const user = localStorage.getItem('user')
      if (!user) {
        toast({
          title: "Error",
          description: "User not found. Please login again.",
          variant: "destructive"
        })
        setDeletingProductId(null)
        return
      }

      const parsedUser = JSON.parse(user)
      const companyId = parsedUser.company_id

      const response = await fetch(`/api/products?id=${productId}&companyId=${companyId}`, {
        method: 'DELETE'
      })

      if (!response.ok) {
        throw new Error('Failed to delete product')
      }

      toast({
        title: "Product deleted",
        description: `"${productName}" has been deleted successfully.`
      })

      // Reload products
      await loadProducts()
    } catch (error) {
      console.error('Error deleting product:', error)
      toast({
        title: "Delete failed",
        description: "Failed to delete product. Please try again.",
        variant: "destructive"
      })
    } finally {
      setDeletingProductId(null)
    }
  }

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "active":
        return "bg-green-100 text-green-800"
      case "inactive":
        return "bg-red-100 text-red-800"
      case "discontinued":
        return "bg-gray-100 text-gray-800"
      default:
        return "bg-blue-100 text-blue-800"
    }
  }

  // Filter products based on search and filters
  const filteredProducts = productsList.filter(product => {
    const matchesSearch = searchTerm === "" || 
      product.productName.toLowerCase().includes(searchTerm.toLowerCase()) ||
      product.category.toLowerCase().includes(searchTerm.toLowerCase()) ||
      product.refNo.toLowerCase().includes(searchTerm.toLowerCase()) ||
      product.principal.toLowerCase().includes(searchTerm.toLowerCase())

    const matchesCategory = selectedCategory === "All" || 
      product.category === selectedCategory

    const matchesPrincipal = selectedPrincipal === "All" || 
      product.principal === selectedPrincipal

    const matchesStatus = selectedStatus === "All" || 
      product.status === selectedStatus

    return matchesSearch && matchesCategory && matchesPrincipal && matchesStatus
  })

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Products Catalog</h1>
          <p className="text-gray-600">Manage your laboratory equipment and products</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline" onClick={handleExport}>
            <Download className="w-4 h-4 mr-2" />
            Export Catalog
          </Button>
          <Button onClick={() => setIsDataImportModalOpen(true)} variant="outline">
            <Upload className="w-4 h-4 mr-2" />
            Import Products
          </Button>
          <Button onClick={handleAddProduct} className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Add Product
          </Button>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Products</CardTitle>
            <ShoppingCart className="h-4 w-4 text-blue-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {isLoadingStats ? (
                <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
              ) : (
                productsStats.total
              )}
            </div>
            <p className="text-xs text-muted-foreground">Total in catalog</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Active Products</CardTitle>
            <CheckCircle className="h-4 w-4 text-green-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {isLoadingStats ? (
                <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
              ) : (
                productsStats.active
              )}
            </div>
            <p className="text-xs text-muted-foreground">{productsStats.total > 0 ? Math.round((productsStats.active / productsStats.total) * 100) : 0}% of catalog</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Product Categories</CardTitle>
            <Grid3X3 className="h-4 w-4 text-purple-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {isLoadingStats ? (
                <div className="w-12 h-8 bg-gray-200 animate-pulse rounded"></div>
              ) : (
                productsStats.categories
              )}
            </div>
            <p className="text-xs text-muted-foreground">Well organized</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Avg. Product Value</CardTitle>
            <IndianRupee className="h-4 w-4 text-emerald-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {isLoadingStats ? (
                <div className="w-16 h-8 bg-gray-200 animate-pulse rounded"></div>
              ) : (
                `₹${productsStats.avgValue.toLocaleString()}`
              )}
            </div>
            <p className="text-xs text-muted-foreground">Laboratory equipment</p>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Product Search & Filters</CardTitle>
          <CardDescription>Filter and search through your product catalog</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col md:flex-row gap-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <Input
                  placeholder="Search products by name, category, or reference number..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
            <Select value={selectedPrincipal} onValueChange={setSelectedPrincipal}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Principal" />
              </SelectTrigger>
              <SelectContent>
                {availablePrincipals.map((principal) => (
                  <SelectItem key={principal} value={principal}>
                    {principal}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={selectedCategory} onValueChange={setSelectedCategory}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Category" />
              </SelectTrigger>
              <SelectContent>
                {availableCategories.map((category) => (
                  <SelectItem key={category} value={category}>
                    {category}
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
          <CardTitle>Products List</CardTitle>
          <CardDescription>Showing {filteredProducts.length} of {productsList.length} products</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {filteredProducts.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                No products found. Click "Add Product" to create your first product.
              </div>
            ) : (
              filteredProducts.map((product) => {
                console.log('Rendering product:', product)
                return (
                  <div
                    key={product.id}
                    className="flex items-start justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
                  >
                    <div className="flex items-start space-x-4 flex-1">
                      {/* Product Image */}
                      <div className="w-20 h-20 bg-gray-100 rounded-lg flex items-center justify-center overflow-hidden flex-shrink-0 relative cursor-pointer hover:bg-gray-200 transition-colors"
                           onClick={() => {
                             if (product.product_picture && 
                                 product.product_picture !== 'pending_upload' && 
                                 product.product_picture !== null &&
                                 product.product_picture.trim() !== '') {
                               handleImageClick(product.product_picture, product.productName)
                             }
                           }}
                      >
                        {product.product_picture && 
                         product.product_picture !== 'pending_upload' && 
                         product.product_picture !== null &&
                         product.product_picture.trim() !== '' ? (
                          <img 
                            src={product.product_picture} 
                            alt={product.productName}
                            className="w-full h-full object-cover hover:opacity-90 transition-opacity"
                            onError={(e) => {
                              console.log('Image load error for product:', product.productName)
                              // If image fails to load, show default icon
                              const target = e.target as HTMLImageElement;
                              target.style.display = 'none';
                              const parent = target.parentElement;
                              if (parent) {
                                const icon = parent.querySelector('.default-icon');
                                if (icon) {
                                  icon.classList.remove('hidden');
                                }
                              }
                            }}
                            onLoad={() => {
                              console.log('Image loaded successfully for product:', product.productName)
                            }}
                          />
                        ) : null}
                        <Package className={`default-icon w-10 h-10 text-gray-400 ${
                          product.product_picture && 
                          product.product_picture !== 'pending_upload' && 
                          product.product_picture !== null &&
                          product.product_picture.trim() !== '' 
                            ? 'hidden' : ''
                        }`} />
                      </div>
                    
                      {/* Product Details */}
                      <div className="flex-1 min-w-0">
                        {/* Product Name and Status */}
                        <div className="flex items-center space-x-2 mb-3">
                          <h3 className="font-semibold text-lg truncate">{product.productName}</h3>
                          <Badge className={getStatusColor(product.status)}>
                            {product.status}
                          </Badge>
                        </div>

                        {/* Grid Layout for Product Details */}
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-x-6 gap-y-2">
                          {/* Column 1 */}
                          <div className="space-y-1">
                            {/* Product Code and Reference Number */}
                            {product.productCode && (
                              <div className="flex items-center space-x-1">
                                <Grid3X3 className="w-4 h-4 text-gray-500" />
                                <span className="text-sm text-gray-600 font-medium">
                                  Code: {product.productCode}
                                </span>
                              </div>
                            )}
                            {product.refNo && (
                              <div className="flex items-center space-x-1">
                                <Grid3X3 className="w-4 h-4 text-gray-500" />
                                <span className="text-sm text-gray-600 font-medium">
                                  Ref: {product.refNo}
                                </span>
                              </div>
                            )}

                            {/* Category and Sub-Category */}
                            {product.category && (
                              <div>
                                <span className="px-2 py-1 text-xs bg-green-100 text-green-800 rounded-full font-medium">
                                  {product.category}
                                </span>
                              </div>
                            )}
                            {product.subCategory && (
                              <div>
                                <span className="px-2 py-1 text-xs bg-green-50 text-green-700 rounded-full font-medium">
                                  {product.subCategory}
                                </span>
                              </div>
                            )}

                            {/* Description inline */}
                            {product.description && (
                              <div className="text-sm text-gray-600">
                                <span className="font-semibold">Desc: </span>
                                <span className="line-clamp-2">{product.description}</span>
                              </div>
                            )}
                          </div>

                          {/* Column 2 */}
                          <div className="space-y-1">
                            {/* Branch and Principal */}
                            {product.branch && (
                              <div>
                                <span className="px-2 py-1 text-xs bg-purple-100 text-purple-800 rounded-full font-medium">
                                  Branch: {product.branch}
                                </span>
                              </div>
                            )}
                            {product.principal && (
                              <div>
                                <span className="px-2 py-1 text-xs bg-blue-100 text-blue-800 rounded-full font-medium">
                                  Principal: {product.principal}
                                </span>
                              </div>
                            )}

                            {/* Inventory Type - with label */}
                            {product.inventoryType && (
                              <div className="text-sm text-gray-600">
                                <span className="font-medium">Inventory: </span>
                                <span className="text-xs bg-orange-100 text-orange-800 px-2 py-1 rounded-full font-medium">
                                  {product.inventoryType}
                                </span>
                              </div>
                            )}

                            {/* Data Classification - with label */}
                            {product.dataClassification && (
                              <div className="text-sm text-gray-600">
                                <span className="font-medium">Classification: </span>
                                <span className="px-2 py-1 text-xs bg-indigo-100 text-indigo-800 rounded-full font-medium">
                                  {product.dataClassification}
                                </span>
                              </div>
                            )}
                          </div>

                          {/* Column 3 */}
                          <div className="space-y-1">
                            {/* Pricing Information */}
                            {product.price && (
                              <div className="flex items-center space-x-1">
                                <IndianRupee className="w-4 h-4 text-green-600" />
                                <span className="text-base font-bold text-green-600">
                                  {product.price}
                                </span>
                              </div>
                            )}
                            {product.basePrice && (
                              <div className="text-sm text-gray-600">
                                <span className="font-medium">Base: </span>
                                <span>₹{product.basePrice}</span>
                              </div>
                            )}
                            {product.costPrice && (
                              <div className="text-sm text-gray-600">
                                <span className="font-medium">Cost: </span>
                                <span>₹{product.costPrice}</span>
                              </div>
                            )}
                            {product.profitMargin && (
                              <div className="text-sm text-green-600 font-medium">
                                Margin: {product.profitMargin}%
                              </div>
                            )}

                            {/* Stock Information */}
                            {product.currentStockLevel && (
                              <div className="text-sm text-gray-600">
                                <span className="font-medium">Stock: </span>
                                <span>{product.currentStockLevel} units</span>
                              </div>
                            )}
                            {product.reorderPoint && (
                              <div className="text-sm text-gray-600">
                                <span className="font-medium">Reorder: </span>
                                <span>{product.reorderPoint} units</span>
                              </div>
                            )}
                          </div>
                        </div>

                        {/* Full-width items below the grid */}
                        {product.productTags && (
                          <div className="mt-1">
                            <span className="text-xs text-gray-500">Tags: </span>
                            <span className="text-xs text-gray-700 bg-gray-100 px-2 py-1 rounded">
                              {product.productTags}
                            </span>
                          </div>
                        )}

                        {product.technicalSpecifications && (
                          <div className="mt-1">
                            <span className="text-xs font-semibold text-gray-700">Tech Specs: </span>
                            <span className="text-xs text-gray-600">{product.technicalSpecifications}</span>
                          </div>
                        )}
                      </div>
                    </div>

                  {/* Actions */}
                  <div className="flex flex-col items-end space-y-2 pt-2">
                    <div className="flex space-x-2">
                      <Button variant="outline" size="sm" onClick={() => handleEditProduct(product)}>
                        <Edit className="w-4 h-4 mr-1" />
                        Edit
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleDeleteProduct(product.id, product.productName)}
                        disabled={deletingProductId === product.id}
                        className="text-red-600 hover:text-red-700 hover:bg-red-50"
                      >
                        {deletingProductId === product.id ? (
                          <>
                            <div className="w-4 h-4 mr-1 border-2 border-red-600 border-t-transparent rounded-full animate-spin" />
                            Deleting...
                          </>
                        ) : (
                          <>
                            <Trash2 className="w-4 h-4 mr-1" />
                            Delete
                          </>
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

      {/* AI Product Recommendations Section - Temporarily Commented */}
      {/* <AIProductRecommendations
        currentProduct="LABORATORY FREEZE DRYER/LYOPHILIZER"
        customerType="Research Institution"
        context="cross-sell"
      /> */}

      {/* Dynamic Import Modal */}
      <DynamicImportModal
        isOpen={isDataImportModalOpen}
        onClose={() => setIsDataImportModalOpen(false)}
        onImport={handleImportData}
        moduleType="products"
        isImporting={isImporting}
        importProgress={importProgress}
      />

      {/* Image Zoom Modal */}
      <Dialog open={!!zoomedImage} onOpenChange={closeImageZoom}>
        <DialogContent className="max-w-4xl max-h-[90vh] p-0">
          <DialogHeader className="p-4 pb-0">
            <div className="flex items-center justify-between">
              <DialogTitle className="text-lg font-semibold truncate">
                {zoomedImage?.name}
              </DialogTitle>
              <Button 
                variant="ghost" 
                size="sm" 
                onClick={closeImageZoom}
                className="h-8 w-8 p-0"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          </DialogHeader>
          
          {zoomedImage && (
            <div className="p-4 pt-0">
              <div className="flex items-center justify-center bg-gray-50 rounded-lg overflow-hidden">
                <img 
                  src={zoomedImage.url} 
                  alt={zoomedImage.name}
                  className="max-w-full max-h-[70vh] object-contain"
                  style={{ minHeight: '200px' }}
                />
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
