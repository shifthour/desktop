"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Checkbox } from "@/components/ui/checkbox"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Search, Plus, Minus, Package, IndianRupee } from "lucide-react"
import { cn } from "@/lib/utils"

interface Product {
  id: string
  productName: string
  category: string
  price: number
  base_price?: number
  cost_price?: number
}

interface SelectedProduct {
  id: string
  productName: string
  category: string
  price: number
  quantity: number
  totalAmount: number
}

interface MultipleProductSelectorProps {
  selectedProducts: SelectedProduct[]
  onProductsChange: (products: SelectedProduct[]) => void
  onTotalBudgetChange: (totalBudget: number) => void
}

export function MultipleProductSelector({ 
  selectedProducts, 
  onProductsChange, 
  onTotalBudgetChange 
}: MultipleProductSelectorProps) {
  const [products, setProducts] = useState<Product[]>([])
  const [searchQuery, setSearchQuery] = useState("")
  const [isLoading, setIsLoading] = useState(true)

  // Load products on component mount
  useEffect(() => {
    loadProducts()
  }, [])

  // Calculate total budget whenever selected products change
  useEffect(() => {
    const totalBudget = selectedProducts.reduce((sum, product) => sum + product.totalAmount, 0)
    onTotalBudgetChange(totalBudget)
  }, [selectedProducts, onTotalBudgetChange])

  const loadProducts = async () => {
    try {
      setIsLoading(true)
      const companyId = localStorage.getItem('currentCompanyId') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      const response = await fetch(`/api/products?companyId=${companyId}`)
      
      if (response.ok) {
        const data = await response.json()
        const formattedProducts = data.map((product: any) => ({
          id: product.id,
          productName: product.product_name,
          category: product.category || 'General',
          price: product.price || product.base_price || product.cost_price || 0,
          base_price: product.base_price,
          cost_price: product.cost_price
        }))
        setProducts(formattedProducts)
      }
    } catch (error) {
      console.error('Error loading products:', error)
    } finally {
      setIsLoading(false)
    }
  }

  // Filter products based on search query
  const filteredProducts = products.filter(product => 
    product.productName.toLowerCase().includes(searchQuery.toLowerCase()) ||
    product.category.toLowerCase().includes(searchQuery.toLowerCase())
  )

  // Check if a product is selected
  const isProductSelected = (productId: string) => {
    return selectedProducts.some(sp => sp.id === productId)
  }

  // Get selected product details
  const getSelectedProduct = (productId: string) => {
    return selectedProducts.find(sp => sp.id === productId)
  }

  // Handle product selection/deselection
  const handleProductToggle = (product: Product, checked: boolean) => {
    if (checked) {
      // Add product with default quantity of 1
      const newSelectedProduct: SelectedProduct = {
        ...product,
        quantity: 1,
        totalAmount: product.price * 1
      }
      const updatedProducts = [...selectedProducts, newSelectedProduct]
      onProductsChange(updatedProducts)
    } else {
      // Remove product
      const updatedProducts = selectedProducts.filter(sp => sp.id !== product.id)
      onProductsChange(updatedProducts)
    }
  }

  // Handle quantity change
  const handleQuantityChange = (productId: string, newQuantity: number) => {
    if (newQuantity < 1) return // Minimum quantity is 1

    const updatedProducts = selectedProducts.map(sp => {
      if (sp.id === productId) {
        return {
          ...sp,
          quantity: newQuantity,
          totalAmount: sp.price * newQuantity
        }
      }
      return sp
    })
    onProductsChange(updatedProducts)
  }

  // Handle direct quantity input
  const handleQuantityInputChange = (productId: string, value: string) => {
    const newQuantity = parseInt(value) || 1
    handleQuantityChange(productId, newQuantity)
  }

  // Format currency
  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(amount)
  }

  // Calculate total budget
  const totalBudget = selectedProducts.reduce((sum, product) => sum + product.totalAmount, 0)

  return (
    <div className="space-y-4">
      {/* Header with search */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <Label className="text-base font-semibold">Product Selection *</Label>
          <Badge variant="outline" className="ml-2">
            {selectedProducts.length} selected
          </Badge>
        </div>
        
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
          <Input
            placeholder="Search products by name or category..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10"
          />
        </div>
      </div>

      {/* Selected Products Summary */}
      {selectedProducts.length > 0 && (
        <Card className="bg-blue-50 border-blue-200">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm flex items-center">
              <Package className="w-4 h-4 mr-2" />
              Selected Products ({selectedProducts.length})
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="space-y-2">
              {selectedProducts.map((product) => (
                <div key={product.id} className="flex items-center justify-between text-sm">
                  <span className="font-medium">{product.productName}</span>
                  <div className="flex items-center space-x-2">
                    <span className="text-gray-600">Qty: {product.quantity}</span>
                    <span className="font-semibold">{formatCurrency(product.totalAmount)}</span>
                  </div>
                </div>
              ))}
              <div className="border-t pt-2 mt-2">
                <div className="flex items-center justify-between font-semibold">
                  <span>Total Budget:</span>
                  <span className="text-lg text-blue-600">{formatCurrency(totalBudget)}</span>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Product Selection List */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Available Products</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <ScrollArea className="h-64">
            {isLoading ? (
              <div className="flex items-center justify-center py-8">
                <div className="text-sm text-gray-500">Loading products...</div>
              </div>
            ) : filteredProducts.length === 0 ? (
              <div className="flex items-center justify-center py-8">
                <div className="text-sm text-gray-500">
                  {searchQuery ? 'No products found matching your search' : 'No products available'}
                </div>
              </div>
            ) : (
              <div className="space-y-0">
                {filteredProducts.map((product) => {
                  const selected = isProductSelected(product.id)
                  const selectedProduct = getSelectedProduct(product.id)
                  
                  return (
                    <div
                      key={product.id}
                      className={cn(
                        "flex items-center justify-between p-4 border-b hover:bg-gray-50 transition-colors",
                        selected && "bg-blue-50"
                      )}
                    >
                      <div className="flex items-center space-x-3 flex-1">
                        <Checkbox
                          checked={selected}
                          onCheckedChange={(checked) => handleProductToggle(product, checked as boolean)}
                        />
                        <div className="flex-1">
                          <div className="font-medium text-sm">{product.productName}</div>
                          <div className="text-xs text-gray-500 mt-1">
                            <Badge variant="outline" className="mr-2 text-xs">
                              {product.category}
                            </Badge>
                            <span className="text-green-600 font-medium">
                              {formatCurrency(product.price)} per unit
                            </span>
                          </div>
                        </div>
                      </div>

                      {/* Quantity Controls - Only show if product is selected */}
                      {selected && selectedProduct && (
                        <div className="flex items-center space-x-2 ml-4">
                          <Label className="text-xs text-gray-600">Qty:</Label>
                          <div className="flex items-center space-x-1">
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              className="h-8 w-8 p-0"
                              onClick={() => handleQuantityChange(product.id, selectedProduct.quantity - 1)}
                              disabled={selectedProduct.quantity <= 1}
                            >
                              <Minus className="w-3 h-3" />
                            </Button>
                            
                            <Input
                              type="number"
                              min="1"
                              value={selectedProduct.quantity}
                              onChange={(e) => handleQuantityInputChange(product.id, e.target.value)}
                              className="h-8 w-16 text-center text-sm"
                            />
                            
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              className="h-8 w-8 p-0"
                              onClick={() => handleQuantityChange(product.id, selectedProduct.quantity + 1)}
                            >
                              <Plus className="w-3 h-3" />
                            </Button>
                          </div>
                          
                          <div className="text-sm font-semibold text-blue-600 min-w-[80px] text-right">
                            {formatCurrency(selectedProduct.totalAmount)}
                          </div>
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </ScrollArea>
        </CardContent>
      </Card>

      {/* Validation Message */}
      {selectedProducts.length === 0 && (
        <div className="text-sm text-amber-600 bg-amber-50 border border-amber-200 rounded-md p-3">
          Please select at least one product to proceed.
        </div>
      )}
    </div>
  )
}