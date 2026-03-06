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
  Plus,
  CheckCircle2,
  Building2,
  ArrowRight
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import { DynamicAccountField } from "./dynamic-account-field"

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
  contact: 'Contact Information',
  addresses: 'Addresses',
  business_details: 'Business Details',
  financial: 'Financial Information',
  advanced: 'Advanced Fields'
}

// Define section display order
const SECTION_ORDER = [
  'basic_info',
  'contact',
  'addresses',
  'business_details',
  'financial',
  'advanced'
]

export function DynamicAddAccountContent() {
  const router = useRouter()
  const { toast } = useToast()
  const [showReview, setShowReview] = useState(false)
  const [fieldConfigs, setFieldConfigs] = useState<FieldConfig[]>([])
  const [formData, setFormData] = useState<Record<string, any>>({})
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [currentUser, setCurrentUser] = useState<any>(null)
  const [industries, setIndustries] = useState<{ industry: string; subIndustry: string }[]>([{ industry: '', subIndustry: '' }])

  const handleIndustriesChange = (newIndustries: { industry: string; subIndustry: string }[]) => {
    setIndustries(newIndustries)
    // Clear industries error when user makes changes
    if (errors['industries']) {
      setErrors(prev => {
        const newErrors = { ...prev }
        delete newErrors['industries']
        return newErrors
      })
    }
  }

  useEffect(() => {
    const user = localStorage.getItem('user')
    if (user) {
      const parsedUser = JSON.parse(user)
      setCurrentUser(parsedUser)
      loadFieldConfigurations(parsedUser.company_id)
    }
  }, [])

  const loadFieldConfigurations = async (companyId: string) => {
    setLoading(true)
    try {
      const response = await fetch(`/api/admin/account-fields?companyId=${companyId}`)
      if (response.ok) {
        const data: FieldConfig[] = await response.json()
        setFieldConfigs(data)

        // Initialize form data with empty values for enabled fields (excluding account_id)
        const initialData: Record<string, any> = {}
        data
          .filter(f => f.is_enabled && f.field_name !== 'account_id')
          .forEach(field => {
            initialData[field.field_name] = ''
          })
        setFormData(initialData)
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
    const isDistributor = formData.account_type === 'Distributor' || formData.accountType === 'Distributor'

    fieldConfigs
      .filter(f => f.is_enabled && f.is_mandatory && f.field_name !== 'account_id')
      .forEach(field => {
        // Skip acct_industry validation for distributors (we'll validate industries array instead)
        // Skip acct_sub_industry for distributors (it's included in industries array)
        if (isDistributor && (field.field_name === 'acct_industry' || field.field_name === 'acct_sub_industry')) {
          return
        }

        const value = formData[field.field_name]
        if (!value || (typeof value === 'string' && value.trim() === '')) {
          newErrors[field.field_name] = `${field.field_label} is required`
        }
      })

    // Validate industries for distributors
    if (isDistributor) {
      const hasValidIndustry = industries.some(pair => pair.industry && pair.subIndustry)
      if (!hasValidIndustry) {
        newErrors['industries'] = 'At least one complete industry-subindustry pair is required for distributors'
      }
    }

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

  const handleSave = async (saveAndNew: boolean = false) => {
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
      const response = await fetch('/api/accounts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companyId: currentUser.company_id,
          userId: currentUser.id,
          ...formData,
          industries: formData.account_type === 'Distributor' ? industries : []
        })
      })

      if (response.ok) {
        toast({
          title: "Success",
          description: "Account created successfully"
        })

        if (saveAndNew) {
          // Reset form for new entry
          const resetData: Record<string, any> = {}
          fieldConfigs
            .filter(f => f.is_enabled && f.field_name !== 'account_id')
            .forEach(field => {
              resetData[field.field_name] = ''
            })
          setFormData(resetData)
          setIndustries([{ industry: '', subIndustry: '' }])
          setShowReview(false)
          setErrors({})
        } else {
          router.push("/accounts")
        }
      } else {
        const errorData = await response.json()
        console.error('Account creation failed:', errorData)

        // Build detailed error message
        let errorMessage = errorData.error || 'Failed to create account'
        if (errorData.details) {
          errorMessage += `\n\nDetails: ${errorData.details}`
        }
        if (errorData.hint) {
          errorMessage += `\n\nSuggestion: ${errorData.hint}`
        }

        toast({
          title: `Error (${response.status})`,
          description: errorMessage,
          variant: "destructive",
          duration: 10000,
        })
      }
    } catch (error: any) {
      console.error('Error creating account:', error)
      const errorMessage = error.message || 'An unexpected error occurred while creating the account. Please check the console for details.'

      toast({
        title: "Error",
        description: errorMessage,
        variant: "destructive",
        duration: 10000,
      })
    } finally {
      setSaving(false)
    }
  }

  const getSectionFields = (section: string) => {
    return fieldConfigs
      .filter(f => f.field_section === section && f.is_enabled && f.field_name !== 'account_id')
      .sort((a, b) => a.display_order - b.display_order)
  }

  const getEnabledSections = () => {
    const sections = Array.from(
      new Set(
        fieldConfigs
          .filter(f => f.is_enabled && f.field_name !== 'account_id')
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
  const totalFields = fieldConfigs.filter(f => f.is_enabled && f.field_name !== 'account_id').length
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
                Review Account Information
              </h1>
              <p className="text-gray-600 mt-1">Please verify all information before creating the account</p>
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
              {formData.account_name && (
                <Card className="border-blue-200 bg-blue-50">
                  <CardContent className="p-6">
                    <div className="flex items-center space-x-3">
                      <Building2 className="w-8 h-8 text-blue-600" />
                      <div>
                        <h2 className="text-2xl font-bold text-blue-900">{formData.account_name}</h2>
                        <p className="text-sm text-blue-700">
                          {formData.acct_industry || 'No industry'} • {formData.account_status || 'Active'}
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

              {/* Distributor Industries - Review */}
              {(formData.account_type === 'Distributor' || formData.accountType === 'Distributor') && industries.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">Distributor Industries</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {industries.map((pair, index) => (
                        <div key={index} className="flex items-center gap-2 p-3 bg-gray-50 rounded-lg">
                          <span className="font-medium text-gray-900">{pair.industry}</span>
                          <span className="text-gray-500">→</span>
                          <span className="text-gray-700">{pair.subIndustry}</span>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}
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
              onClick={() => handleSave(false)}
              disabled={saving}
              className="bg-blue-600 hover:bg-blue-700"
            >
              <Save className="w-4 h-4 mr-2" />
              {saving ? 'Creating...' : 'Create Account'}
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
            <Button variant="ghost" onClick={() => router.push("/accounts")}>
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back
            </Button>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Create New Account</h1>
              <p className="text-gray-600">Fill in the account details below</p>
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
                      {sectionFields.map((field, index) => (
                        <div
                          key={field.id}
                          id={`field-${field.field_name}`}
                          className={field.field_type === 'textarea' ? 'md:col-span-2' : ''}
                        >
                          <DynamicAccountField
                            config={field}
                            value={formData[field.field_name]}
                            onChange={handleFieldChange}
                            error={errors[field.field_name]}
                            dependentValues={formData}
                            industries={industries}
                            onIndustriesChange={handleIndustriesChange}
                            industriesError={errors['industries']}
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
          <Button variant="outline" onClick={() => router.push("/accounts")}>
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
