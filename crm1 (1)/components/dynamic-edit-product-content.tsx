"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  ArrowLeft,
  Save,
  CheckCircle2,
  Package,
  ArrowRight
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import { DynamicProductField } from "./dynamic-product-field"

interface FieldConfig {
  id: string
  field_name: string
  field_label: string
  field_type: string
  is_mandatory: boolean
  is_enabled: boolean
  field_section: string
  display_order: number
  field_options?: string[]
  placeholder?: string
  validation_rules?: any
  help_text?: string
}

const SECTION_LABELS: Record<string, string> = {
  basic_info: 'Basic Information',
  pricing: 'Pricing',
  inventory: 'Inventory Management',
  technical: 'Technical Specifications',
  media: 'Media & Assets',
  dates: 'Dates & Timeline',
  analytics: 'Analytics & Performance'
}

// Define section display order
const SECTION_ORDER = [
  'basic_info',
  'pricing',
  'inventory',
  'technical',
  'media',
  'dates',
  'analytics'
]

interface DynamicEditProductContentProps {
  productId: string
}

export function DynamicEditProductContent({ productId }: DynamicEditProductContentProps) {
  const router = useRouter()
  const { toast } = useToast()
  const [showReview, setShowReview] = useState(false)
  const [fieldConfigs, setFieldConfigs] = useState<FieldConfig[]>([])
  const [formData, setFormData] = useState<Record<string, any>>({})
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [currentUser, setCurrentUser] = useState<any>(null)
  const [productData, setProductData] = useState<any>(null)

  useEffect(() => {
    const user = localStorage.getItem('user')
    if (user) {
      const parsedUser = JSON.parse(user)
      setCurrentUser(parsedUser)
      loadFieldConfigurations(parsedUser.company_id)
      loadProductData(productId)
    }
  }, [productId])

  const loadProductData = async (id: string) => {
    try {
      const response = await fetch(`/api/products/${id}`)
      if (response.ok) {
        const data = await response.json()
        setProductData(data)
        // Form data will be populated after field configs are loaded
      } else {
        toast({
          title: "Error",
          description: "Failed to load product data",
          variant: "destructive"
        })
        router.push('/products')
      }
    } catch (error) {
      console.error('Error loading product:', error)
      toast({
        title: "Error",
        description: "Failed to load product data",
        variant: "destructive"
      })
      router.push('/products')
    }
  }

  const loadFieldConfigurations = async (companyId: string) => {
    setLoading(true)
    try {
      const response = await fetch(`/api/admin/product-fields?companyId=${companyId}`)
      if (response.ok) {
        const data: FieldConfig[] = await response.json()
        setFieldConfigs(data)

        // Initialize form data with product values if available
        if (productData) {
          const initialData: Record<string, any> = {}
          data
            .filter(f => f.is_enabled && f.field_name !== 'product_id')
            .forEach(field => {
              initialData[field.field_name] = productData[field.field_name] || ''
            })
          setFormData(initialData)
        }
      } else {
        toast({
          title: "Error",
          description: "Failed to load form configuration",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error loading field configs:', error)
      toast({
        title: "Error",
        description: "Failed to load form configuration",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  // Update form data when product data is loaded
  useEffect(() => {
    if (productData && fieldConfigs.length > 0) {
      const initialData: Record<string, any> = {}
      fieldConfigs
        .filter(f => f.is_enabled && f.field_name !== 'product_id')
        .forEach(field => {
          initialData[field.field_name] = productData[field.field_name] || ''
        })
      setFormData(initialData)
    }
  }, [productData, fieldConfigs])

  const handleFieldChange = (fieldName: string, value: any) => {
    setFormData(prev => ({
      ...prev,
      [fieldName]: value
    }))
    // Clear error when field is updated
    if (errors[fieldName]) {
      setErrors(prev => {
        const newErrors = { ...prev }
        delete newErrors[fieldName]
        return newErrors
      })
    }
  }

  const validateForm = () => {
    const newErrors: Record<string, string> = {}

    fieldConfigs
      .filter(f => f.is_enabled && f.is_mandatory && f.field_name !== 'product_id')
      .forEach(field => {
        const value = formData[field.field_name]
        if (!value || (typeof value === 'string' && value.trim() === '')) {
          newErrors[field.field_name] = `${field.field_label} is required`
        }
      })

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleReview = () => {
    if (!validateForm()) {
      toast({
        title: "Validation Error",
        description: "Please fill in all required fields",
        variant: "destructive"
      })
      // Scroll to first error
      const firstError = Object.keys(errors)[0]
      if (firstError) {
        document.getElementById(`field-${firstError}`)?.scrollIntoView({ behavior: 'smooth', block: 'center' })
      }
      return
    }
    setShowReview(true)
  }

  const handleSave = async () => {
    if (!currentUser?.company_id || !currentUser?.id) {
      toast({
        title: "Error",
        description: "User information not found",
        variant: "destructive"
      })
      return
    }

    setSaving(true)
    try {
      const response = await fetch('/api/products', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: productId,
          ...formData
        })
      })

      if (response.ok) {
        toast({
          title: "Success",
          description: "Product updated successfully"
        })
        router.push("/products")
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error || "Failed to update product",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error updating product:', error)
      toast({
        title: "Error",
        description: "Failed to update product",
        variant: "destructive"
      })
    } finally {
      setSaving(false)
    }
  }

  const getSectionFields = (section: string) => {
    return fieldConfigs
      .filter(f => f.field_section === section && f.is_enabled && f.field_name !== 'product_id')
      .sort((a, b) => a.display_order - b.display_order)
  }

  const getEnabledSections = () => {
    const sections = Array.from(
      new Set(
        fieldConfigs
          .filter(f => f.is_enabled && f.field_name !== 'product_id')
          .map(f => f.field_section)
      )
    )
    // Sort sections according to SECTION_ORDER
    return sections.sort((a, b) => {
      const indexA = SECTION_ORDER.indexOf(a)
      const indexB = SECTION_ORDER.indexOf(b)
      return indexA - indexB
    })
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading form...</p>
        </div>
      </div>
    )
  }

  const enabledSections = getEnabledSections()
  const totalFields = fieldConfigs.filter(f => f.is_enabled && f.field_name !== 'product_id').length
  const filledFields = Object.keys(formData).filter(key => formData[key] && formData[key] !== '').length
  const progressPercentage = totalFields > 0 ? (filledFields / totalFields) * 100 : 0

  // Review page
  if (showReview) {
    return (
      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <div className="bg-white border-b border-gray-200 px-6 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 flex items-center">
                <CheckCircle2 className="w-7 h-7 mr-3 text-green-600" />
                Review Product Changes
              </h1>
              <p className="text-gray-600 mt-1">Please verify all changes before updating the product</p>
            </div>
            <Badge variant="outline" className="text-sm">
              Step 2 of 2
            </Badge>
          </div>
        </div>

        <div className="max-w-6xl mx-auto p-6">
          <ScrollArea className="h-[calc(100vh-250px)]">
            <div className="space-y-6 pr-4">
              {/* Summary Card */}
              {formData.product_name && (
                <Card className="border-blue-200 bg-blue-50">
                  <CardContent className="p-6">
                    <div className="flex items-center space-x-3">
                      <Package className="w-8 h-8 text-blue-600" />
                      <div>
                        <h2 className="text-2xl font-bold text-blue-900">{formData.product_name}</h2>
                        <p className="text-sm text-blue-700">
                          {formData.product_code || 'No code'} • {formData.product_status || 'Active'}
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Review all sections */}
              {enabledSections.map(section => {
                const sectionFields = getSectionFields(section)
                const filledSectionFields = sectionFields.filter(f => formData[f.field_name])

                if (filledSectionFields.length === 0) return null

                return (
                  <Card key={section}>
                    <CardHeader>
                      <CardTitle className="text-lg">{SECTION_LABELS[section] || section}</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {sectionFields.map(field => {
                          const value = formData[field.field_name]
                          if (!value) return null

                          return (
                            <div key={field.id} className={field.field_type === 'textarea' ? 'md:col-span-2' : ''}>
                              <p className="text-sm font-medium text-gray-600">{field.field_label}</p>
                              <p className="text-gray-900 mt-1">
                                {value || '-'}
                              </p>
                            </div>
                          )
                        })}
                      </div>
                    </CardContent>
                  </Card>
                )
              })}
            </div>
          </ScrollArea>

          {/* Action Buttons */}
          <div className="flex justify-between mt-6 pt-6 border-t">
            <Button
              variant="outline"
              onClick={() => setShowReview(false)}
              disabled={saving}
            >
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back to Edit
            </Button>
            <Button
              onClick={() => handleSave()}
              disabled={saving}
              className="bg-blue-600 hover:bg-blue-700"
            >
              <Save className="w-4 h-4 mr-2" />
              {saving ? 'Updating...' : 'Update Product'}
            </Button>
          </div>
        </div>
      </div>
    )
  }

  // Form page - All fields on single page
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Button variant="ghost" onClick={() => router.push("/products")}>
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back
            </Button>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Edit Product</h1>
              <p className="text-gray-600">Update the product details below</p>
            </div>
          </div>
          <Badge variant="outline" className="text-sm">
            Step 1 of 2
          </Badge>
        </div>

        {/* Progress Bar */}
        <div className="mt-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm text-gray-600">
              {filledFields} of {totalFields} fields filled
            </span>
            <span className="text-sm font-medium text-blue-600">
              {Math.round(progressPercentage)}% Complete
            </span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${progressPercentage}%` }}
            />
          </div>
        </div>
      </div>

      <div className="max-w-6xl mx-auto p-6">
        <ScrollArea className="h-[calc(100vh-250px)]">
          <div className="space-y-6 pr-4">
            {/* Render all sections on one page */}
            {enabledSections.map(section => {
              const sectionFields = getSectionFields(section)
              if (sectionFields.length === 0) return null

              return (
                <Card key={section}>
                  <CardHeader>
                    <CardTitle>{SECTION_LABELS[section] || section}</CardTitle>
                    <CardDescription>
                      {sectionFields.filter(f => f.is_mandatory).length > 0 && (
                        <span className="text-red-600">* Required fields</span>
                      )}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      {sectionFields.map(field => (
                        <div
                          key={field.id}
                          id={`field-${field.field_name}`}
                          className={field.field_type === 'textarea' ? 'md:col-span-2' : ''}
                        >
                          <DynamicProductField
                            config={field}
                            value={formData[field.field_name]}
                            onChange={handleFieldChange}
                            error={errors[field.field_name]}
                            dependentValues={formData}
                          />
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )
            })}
          </div>
        </ScrollArea>

        {/* Action Buttons */}
        <div className="flex justify-between mt-6 pt-6 border-t">
          <Button variant="outline" onClick={() => router.push("/products")}>
            <ArrowLeft className="w-4 h-4 mr-2" />
            Cancel
          </Button>
          <Button
            onClick={handleReview}
            className="bg-blue-600 hover:bg-blue-700"
          >
            Continue to Review
            <ArrowRight className="w-4 h-4 ml-2" />
          </Button>
        </div>
      </div>
    </div>
  )
}
