"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Card, CardContent } from "@/components/ui/card"
import { Upload, Package, X, Save, Image, Loader2 } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

interface AddProductModalProps {
  isOpen: boolean
  onClose: () => void
  onSave?: (productData: any) => Promise<void>
  editingProduct?: any
}

interface FormData {
  productName: string
  productReferenceNo: string
  description: string
  principal: string
  category: string
  subCategory: string
  price: string
  assignedTo: string
  status: string
  productPicture: File | null
}

export function AddProductModal({ isOpen, onClose, onSave, editingProduct }: AddProductModalProps) {
  const { toast } = useToast()
  const [formData, setFormData] = useState<FormData>({
    productName: "",
    productReferenceNo: "",
    description: "",
    principal: "",
    category: "",
    subCategory: "",
    price: "",
    assignedTo: "",
    status: "Active",
    productPicture: null
  })
  const [isSubmitting, setIsSubmitting] = useState(false)

  // Populate form when editing a product
  useEffect(() => {
    if (editingProduct) {
      setFormData({
        productName: editingProduct.productName || "",
        productReferenceNo: editingProduct.refNo || "",
        description: editingProduct.description || "",
        principal: editingProduct.principal || "",
        category: editingProduct.category || "",
        subCategory: editingProduct.sub_category || "",
        price: editingProduct.price?.replace(/[₹,]/g, '') || "",
        assignedTo: editingProduct.assignedTo || "",
        status: editingProduct.status || "Active",
        productPicture: null // Will handle file separately
      })
    } else {
      // Reset form for new product
      setFormData({
        productName: "",
        productReferenceNo: "",
        description: "",
        principal: "",
        category: "",
        subCategory: "",
        price: "",
        assignedTo: "",
        status: "Active",
        productPicture: null
      })
    }
    // Reset loading state when modal opens/closes
    setIsSubmitting(false)
  }, [editingProduct, isOpen])

  const handleInputChange = (field: keyof FormData, value: string | number) => {
    if (field === 'price' && typeof value === 'string') {
      // Ensure price is a valid number
      const numericValue = value.replace(/[^\d]/g, '')
      setFormData(prev => ({ ...prev, [field]: numericValue }))
    } else {
      setFormData(prev => ({ ...prev, [field]: value }))
    }
  }

  const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (file) {
      // Check if file is an image
      if (file.type.startsWith('image/')) {
        setFormData(prev => ({ ...prev, productPicture: file }))
      } else {
        alert('Please select an image file (JPG, PNG, GIF, etc.)')
      }
    }
  }

  const convertToBase64 = (file: File): Promise<string> => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader()
      reader.readAsDataURL(file)
      reader.onload = () => resolve(reader.result as string)
      reader.onerror = error => reject(error)
    })
  }

  const handleSubmit = async () => {
    // Validate required fields (only productName and price are required)
    if (!formData.productName || !formData.price) {
      toast({
        title: "Validation Error",
        description: "Please fill in Product Name and Price",
        variant: "destructive"
      })
      return
    }

    setIsSubmitting(true)

    try {
      // Convert image to base64 if present, or retain existing image
      let imageData = null
      if (formData.productPicture) {
        // New image uploaded - convert to base64
        imageData = await convertToBase64(formData.productPicture)
        console.log('Image converted to base64, length:', imageData.length)
        console.log('Image data preview:', imageData.substring(0, 100) + '...')
      } else if (editingProduct && editingProduct.product_picture) {
        // No new image uploaded but editing existing product - retain existing image
        imageData = editingProduct.product_picture
        console.log('Retaining existing image for product:', editingProduct.productName)
      }

      // Prepare product data (convert field names to match database)
      const productData = {
        product_name: formData.productName,
        product_reference_no: formData.productReferenceNo,
        description: formData.description,
        principal: formData.principal,
        category: formData.category,
        sub_category: formData.subCategory,
        price: parseFloat(formData.price),
        assigned_to: formData.assignedTo,
        status: formData.status,
        product_picture: imageData
      }

      // Call the onSave callback if provided and wait for it to complete
      if (onSave) {
        await onSave(productData)
      }

      // Note: Form will be reset when modal is closed by parent component
    } catch (error) {
      console.error('Error saving product:', error)
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : 'Error saving product. Please try again.',
        variant: "destructive"
      })
    } finally {
      setIsSubmitting(false)
    }
  }


  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center space-x-2 text-xl">
            <Package className="w-6 h-6 text-blue-600" />
            <span>{editingProduct ? 'Edit Product' : 'Add New Product'}</span>
          </DialogTitle>
          <DialogDescription>
            Fill in the product details below to add it to your inventory
          </DialogDescription>
        </DialogHeader>

        <Card>
          <CardContent className="pt-6 space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Product Name */}
              <div>
                <Label htmlFor="productName" className="text-sm font-medium">
                  Product Name <span className="text-red-500">*</span>
                </Label>
                <Input
                  id="productName"
                  value={formData.productName}
                  onChange={(e) => handleInputChange("productName", e.target.value)}
                  placeholder="Enter product name"
                  className="mt-1"
                  disabled={isSubmitting}
                />
              </div>

              {/* Product Reference No/ID */}
              <div>
                <Label htmlFor="productReferenceNo" className="text-sm font-medium">
                  Product Reference No/ID
                </Label>
                <Input
                  id="productReferenceNo"
                  value={formData.productReferenceNo}
                  onChange={(e) => handleInputChange("productReferenceNo", e.target.value)}
                  placeholder="e.g., P4PRO & AFFIUMP"
                  className="mt-1"
                />
              </div>

              {/* Principal */}
              <div>
                <Label htmlFor="principal" className="text-sm font-medium">
                  Principal
                </Label>
                <Input
                  id="principal"
                  value={formData.principal}
                  onChange={(e) => handleInputChange("principal", e.target.value)}
                  placeholder="Enter principal name"
                  className="mt-1"
                />
              </div>

              {/* Category */}
              <div>
                <Label htmlFor="category" className="text-sm font-medium">
                  Category
                </Label>
                <Input
                  id="category"
                  value={formData.category}
                  onChange={(e) => handleInputChange("category", e.target.value)}
                  placeholder="Enter category"
                  className="mt-1"
                />
              </div>

              {/* Sub Category */}
              <div>
                <Label htmlFor="subCategory" className="text-sm font-medium">Sub Category</Label>
                <Input
                  id="subCategory"
                  value={formData.subCategory}
                  onChange={(e) => handleInputChange("subCategory", e.target.value)}
                  placeholder="Enter sub-category"
                  className="mt-1"
                />
              </div>

              {/* Price */}
              <div>
                <Label htmlFor="price" className="text-sm font-medium">
                  Price (₹) <span className="text-red-500">*</span>
                </Label>
                <Input
                  id="price"
                  type="text"
                  value={formData.price}
                  onChange={(e) => handleInputChange("price", e.target.value)}
                  placeholder="Enter price in rupees"
                  className="mt-1"
                  disabled={isSubmitting}
                />
              </div>

              {/* Assigned To */}
              <div>
                <Label htmlFor="assignedTo" className="text-sm font-medium">Assigned To</Label>
                <Input
                  id="assignedTo"
                  value={formData.assignedTo}
                  onChange={(e) => handleInputChange("assignedTo", e.target.value)}
                  placeholder="Enter assigned person"
                  className="mt-1"
                />
              </div>

              {/* Status - Only show in edit mode */}
              {editingProduct && (
                <div>
                  <Label htmlFor="status" className="text-sm font-medium">Status</Label>
                  <Select value={formData.status} onValueChange={(value) => handleInputChange("status", value)}>
                    <SelectTrigger className="mt-1">
                      <SelectValue placeholder="Select status" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Active">Active</SelectItem>
                      <SelectItem value="Inactive">Inactive</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              )}
            </div>

            {/* Description */}
            <div>
              <Label htmlFor="description" className="text-sm font-medium">Description</Label>
              <Textarea
                id="description"
                value={formData.description}
                onChange={(e) => handleInputChange("description", e.target.value)}
                placeholder="Enter product description, features, and specifications..."
                className="mt-1 min-h-[100px]"
                maxLength={1000}
                disabled={isSubmitting}
              />
              <p className="text-sm text-gray-500 mt-1">
                {1000 - formData.description.length} characters remaining
              </p>
            </div>

            {/* Product Picture Upload */}
            <div>
              <Label className="text-sm font-medium">Product Picture</Label>
              <div className="mt-1 border-2 border-dashed border-gray-300 rounded-lg p-6">
                <div className="text-center">
                  <Image className="w-8 h-8 text-gray-400 mx-auto mb-2" />
                  <div className="space-y-1">
                    <label htmlFor="picture-upload" className="cursor-pointer">
                      <span className="text-blue-600 hover:text-blue-500">Upload image</span>
                      <span className="text-gray-500"> or drag and drop</span>
                    </label>
                    <p className="text-sm text-gray-500">PNG, JPG, GIF up to 5MB</p>
                  </div>
                  <input
                    id="picture-upload"
                    type="file"
                    onChange={handleFileUpload}
                    className="hidden"
                    accept="image/*"
                  />
                </div>
              </div>
              
              {formData.productPicture && (
                <div className="mt-3 p-3 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-start space-x-3">
                    {/* Image Preview */}
                    <div className="w-16 h-16 bg-white rounded-lg border overflow-hidden flex-shrink-0">
                      <img 
                        src={URL.createObjectURL(formData.productPicture)}
                        alt="Product preview"
                        className="w-full h-full object-cover"
                      />
                    </div>
                    
                    {/* File Info */}
                    <div className="flex-1">
                      <div className="flex items-start justify-between">
                        <div>
                          <p className="text-sm font-medium text-green-800">
                            {formData.productPicture.name}
                          </p>
                          <p className="text-xs text-green-600">
                            {(formData.productPicture.size / 1024 / 1024).toFixed(2)} MB
                          </p>
                        </div>
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          onClick={() => setFormData(prev => ({ ...prev, productPicture: null }))}
                          className="text-green-600 hover:text-green-800"
                        >
                          <X className="w-4 h-4" />
                        </Button>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        <DialogFooter className="flex justify-end space-x-2">
          <Button variant="outline" onClick={onClose} disabled={isSubmitting}>
            <X className="w-4 h-4 mr-2" />
            Cancel
          </Button>
          <Button onClick={handleSubmit} className="bg-blue-600 hover:bg-blue-700" disabled={isSubmitting}>
            {isSubmitting ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                {editingProduct ? 'Updating...' : 'Adding Product...'}
              </>
            ) : (
              <>
                <Save className="w-4 h-4 mr-2" />
                {editingProduct ? 'Update Product' : 'Add Product'}
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}