"use client"

import { useState, useEffect } from "react"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Checkbox } from "@/components/ui/checkbox"
import { ScrollArea } from "@/components/ui/scroll-area"

interface Product {
  id: string
  product_name: string
  product_code?: string
  product_reference_no?: string
  price?: number
  base_price?: number
  cost_price?: number
}

interface SelectedProduct {
  product_id: string
  product_name: string
  quantity: number
  price_per_unit?: number
  notes?: string
}

interface MultiProductSelectorProps {
  value: SelectedProduct[]
  onChange: (products: SelectedProduct[]) => void
  error?: string
  isMandatory?: boolean
}

export function MultiProductSelector({
  value = [],
  onChange,
  error,
  isMandatory = false
}: MultiProductSelectorProps) {
  const [products, setProducts] = useState<Product[]>([])
  const [loading, setLoading] = useState(false)
  const [selectedProducts, setSelectedProducts] = useState<SelectedProduct[]>(value)

  useEffect(() => {
    fetchProducts()
  }, [])

  useEffect(() => {
    setSelectedProducts(value)
  }, [value])

  const fetchProducts = async () => {
    try {
      setLoading(true)
      const response = await fetch('/api/products')
      if (response.ok) {
        const data = await response.json()
        console.log('Products fetched:', data)
        setProducts(data || [])
      } else {
        console.error('Failed to fetch products:', response.status)
      }
    } catch (error) {
      console.error('Error fetching products:', error)
    } finally {
      setLoading(false)
    }
  }

  const isProductSelected = (productId: string) => {
    return selectedProducts.some(p => p.product_id === productId)
  }

  const getSelectedProduct = (productId: string) => {
    return selectedProducts.find(p => p.product_id === productId)
  }

  const handleCheckboxChange = (product: Product, checked: boolean) => {
    if (checked) {
      // Add product to selection
      console.log('Product selected:', product)
      console.log('Price:', product.price)
      console.log('Base Price:', product.base_price)
      console.log('Cost Price:', product.cost_price)
      console.log('All product keys:', Object.keys(product))

      // Use the first available price field: price, base_price, or cost_price
      const productPrice = product.price || product.base_price || product.cost_price || 0

      const newProduct: SelectedProduct = {
        product_id: product.id,
        product_name: product.product_name,
        quantity: 1,
        price_per_unit: productPrice,
        notes: ''
      }
      console.log('New product created with price:', newProduct)

      const updated = [...selectedProducts, newProduct]
      setSelectedProducts(updated)
      onChange(updated)
    } else {
      // Remove product from selection
      const updated = selectedProducts.filter(p => p.product_id !== product.id)
      setSelectedProducts(updated)
      onChange(updated)
    }
  }

  const handleQuantityChange = (productId: string, quantity: number) => {
    const updated = selectedProducts.map(p =>
      p.product_id === productId ? { ...p, quantity: quantity || 1 } : p
    )
    setSelectedProducts(updated)
    onChange(updated)
  }

  const handlePriceChange = (productId: string, price: number) => {
    const updated = selectedProducts.map(p =>
      p.product_id === productId ? { ...p, price_per_unit: price } : p
    )
    setSelectedProducts(updated)
    onChange(updated)
  }

  const handleNotesChange = (productId: string, notes: string) => {
    const updated = selectedProducts.map(p =>
      p.product_id === productId ? { ...p, notes } : p
    )
    setSelectedProducts(updated)
    onChange(updated)
  }

  const calculateTotal = () => {
    return selectedProducts.reduce(
      (sum, p) => sum + (p.quantity || 0) * (p.price_per_unit || 0),
      0
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <Label className="text-base font-semibold">
          Products {isMandatory && <span className="text-red-500 ml-1">*</span>}
        </Label>
        <div className="text-sm text-gray-600">
          {selectedProducts.length} product(s) selected
        </div>
      </div>

      {error && (
        <p className="text-xs text-red-500">{error}</p>
      )}

      {loading ? (
        <div className="text-center py-8 text-gray-500">
          Loading products...
        </div>
      ) : products.length === 0 ? (
        <div className="text-center py-8 text-gray-500 border-2 border-dashed rounded-lg">
          No products available
        </div>
      ) : (
        <>
          <ScrollArea className="h-[400px] border rounded-lg p-4">
            <div className="space-y-4">
              {products.map((product) => {
                const isSelected = isProductSelected(product.id)
                const selectedProduct = getSelectedProduct(product.id)

                return (
                  <div
                    key={product.id}
                    className={`p-4 border rounded-lg transition-colors ${
                      isSelected ? 'bg-blue-50 border-blue-300' : 'bg-white'
                    }`}
                  >
                    <div className="flex items-start gap-3">
                      <Checkbox
                        checked={isSelected}
                        onCheckedChange={(checked) =>
                          handleCheckboxChange(product, checked as boolean)
                        }
                        className="mt-1"
                      />
                      <div className="flex-1 space-y-3">
                        <div>
                          <div className="font-medium text-sm">
                            {product.product_name}
                            {(product.product_code || product.product_reference_no) && (
                              <span className="text-gray-500 ml-2">
                                ({product.product_code || product.product_reference_no})
                              </span>
                            )}
                          </div>
                          <div className="text-xs text-gray-600 mt-1">
                            Price: ₹{product.price || product.base_price || product.cost_price || 0}
                          </div>
                        </div>

                        {isSelected && selectedProduct && (
                          <div className="grid grid-cols-3 gap-3">
                            <div>
                              <Label className="text-xs">Quantity</Label>
                              <Input
                                type="number"
                                min="1"
                                value={selectedProduct.quantity}
                                onChange={(e) =>
                                  handleQuantityChange(
                                    product.id,
                                    parseInt(e.target.value) || 1
                                  )
                                }
                                className="h-8 text-sm"
                              />
                            </div>
                            <div>
                              <Label className="text-xs">Price (₹)</Label>
                              <Input
                                type="number"
                                step="0.01"
                                min="0"
                                value={selectedProduct.price_per_unit || ''}
                                onChange={(e) =>
                                  handlePriceChange(
                                    product.id,
                                    parseFloat(e.target.value) || 0
                                  )
                                }
                                className="h-8 text-sm"
                              />
                            </div>
                            <div>
                              <Label className="text-xs">Subtotal</Label>
                              <div className="h-8 flex items-center text-sm font-medium text-blue-600">
                                ₹{((selectedProduct.quantity || 0) * (selectedProduct.price_per_unit || 0)).toFixed(2)}
                              </div>
                            </div>
                            <div className="col-span-3">
                              <Label className="text-xs">Notes (Optional)</Label>
                              <Input
                                type="text"
                                value={selectedProduct.notes || ''}
                                onChange={(e) =>
                                  handleNotesChange(product.id, e.target.value)
                                }
                                placeholder="Add notes about this product"
                                className="h-8 text-sm"
                              />
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </ScrollArea>

          {selectedProducts.length > 0 && (
            <div className="flex justify-between items-center pt-3 border-t">
              <div className="text-sm text-gray-600">
                Total items: {selectedProducts.reduce((sum, p) => sum + (p.quantity || 0), 0)}
              </div>
              <div className="text-lg font-bold text-blue-600">
                Total: ₹{calculateTotal().toFixed(2)}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
