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
  UserPlus,
  ArrowRight
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import { DynamicLeadField } from "./dynamic-lead-field"

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
  lead_details: 'Lead Details',
  contact_info: 'Contact Information',
  additional_info: 'Additional Information'
}

// Define section display order
const SECTION_ORDER = [
  'basic_info',
  'lead_details',
  'contact_info',
  'additional_info'
]

interface DynamicEditLeadContentProps {
  leadId: string
}

export function DynamicEditLeadContent({ leadId }: DynamicEditLeadContentProps) {
  const router = useRouter()
  const { toast } = useToast()
  const [showReview, setShowReview] = useState(false)
  const [fieldConfigs, setFieldConfigs] = useState<FieldConfig[]>([])
  const [formData, setFormData] = useState<Record<string, any>>({})
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [currentUser, setCurrentUser] = useState<any>(null)
  const [leadData, setLeadData] = useState<any>(null)

  useEffect(() => {
    const user = localStorage.getItem('user')
    if (user) {
      const parsedUser = JSON.parse(user)
      setCurrentUser(parsedUser)
      loadFieldConfigurations(parsedUser.company_id)
      loadLeadData(leadId)
    }
  }, [leadId])

  const loadLeadData = async (id: string) => {
    try {
      const response = await fetch(`/api/leads/${id}`)
      if (response.ok) {
        const data = await response.json()
        console.log('=== LEAD DATA RECEIVED FROM API ===')
        console.log('account:', data.account)
        console.log('account_name:', data.account_name)
        console.log('account_id:', data.account_id)
        console.log('contact:', data.contact)
        console.log('contact_name:', data.contact_name)
        console.log('contact_id:', data.contact_id)
        console.log('Full lead data:', data)
        setLeadData(data)
        // Form data will be populated after field configs are loaded
      } else {
        toast({
          title: "Error",
          description: "Failed to load lead data",
          variant: "destructive"
        })
        router.push('/leads')
      }
    } catch (error) {
      console.error('Error loading lead:', error)
      toast({
        title: "Error",
        description: "Failed to load lead data",
        variant: "destructive"
      })
      router.push('/leads')
    }
  }

  const loadFieldConfigurations = async (companyId: string) => {
    setLoading(true)
    try {
      const response = await fetch(`/api/admin/lead-fields?companyId=${companyId}`)
      if (response.ok) {
        const data: FieldConfig[] = await response.json()
        setFieldConfigs(data)

        // Initialize form data with lead values if available
        if (leadData) {
          const initialData: Record<string, any> = {}
          data
            .filter(f => f.is_enabled && f.field_name !== 'lead_id')
            .forEach(field => {
              // For select_dependent fields (account, contact), the API already mapped the IDs
              initialData[field.field_name] = leadData[field.field_name] || ''
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

  // Update form data when lead data is loaded
  useEffect(() => {
    if (leadData && fieldConfigs.length > 0) {
      const initializeFormData = async () => {
        const initialData: Record<string, any> = {}

        console.log('=== INITIALIZING FORM DATA ===')
        console.log('Lead data account:', leadData.account)
        console.log('Lead data account_id:', leadData.account_id)
        console.log('Lead data contact:', leadData.contact)
        console.log('Lead data contact_id:', leadData.contact_id)

        // First, populate all basic fields from lead data
        fieldConfigs
          .filter(f => f.is_enabled && f.field_name !== 'lead_id')
          .forEach(field => {
            // For select_dependent fields (account, contact), the API already mapped the IDs
            // For other fields, use the direct value
            initialData[field.field_name] = leadData[field.field_name] || ''

            if (field.field_name === 'account' || field.field_name === 'contact') {
              console.log(`Setting ${field.field_name} = ${leadData[field.field_name]}`)
            }
          })

        // Ensure account_id and contact_id are set for dependent field lookups
        if (leadData.account_id) {
          initialData.account_id = leadData.account_id
        }

        if (leadData.contact_id) {
          initialData.contact_id = leadData.contact_id
        }

        // If there's a contact_id, fetch contact details to populate contact-related fields
        if (leadData.contact_id) {
          try {
            const companyId = currentUser?.company_id
            const response = await fetch(`/api/contacts?companyId=${companyId}`)
            if (response.ok) {
              const contactsData = await response.json()
              const contacts = contactsData.contacts || contactsData || []
              const contact = contacts.find((c: any) => c.id === leadData.contact_id)

              if (contact) {
                // Populate contact fields if they exist in field configs
                if (fieldConfigs.some(f => f.field_name === 'first_name' && f.is_enabled)) {
                  initialData.first_name = contact.first_name || leadData.first_name || ''
                }
                if (fieldConfigs.some(f => f.field_name === 'last_name' && f.is_enabled)) {
                  initialData.last_name = contact.last_name || leadData.last_name || ''
                }
                if (fieldConfigs.some(f => f.field_name === 'phone_number' && f.is_enabled)) {
                  initialData.phone_number = contact.phone_mobile || contact.phone_work || leadData.phone_number || leadData.phone || ''
                }
                if (fieldConfigs.some(f => f.field_name === 'email_address' && f.is_enabled)) {
                  initialData.email_address = contact.email_primary || leadData.email_address || leadData.email || ''
                }
                if (fieldConfigs.some(f => f.field_name === 'department' && f.is_enabled)) {
                  initialData.department = contact.department || leadData.department || ''
                }
              }
            }
          } catch (error) {
            console.error('Error fetching contact details:', error)
          }
        }

        console.log('Initialized form data:', initialData)
        console.log('account_id in form data:', initialData.account_id)
        console.log('account in form data:', initialData.account)
        console.log('contact_id in form data:', initialData.contact_id)
        console.log('contact in form data:', initialData.contact)
        setFormData(initialData)
      }

      initializeFormData()
    }
  }, [leadData, fieldConfigs, currentUser])

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

  const handleAccountSelect = (accountData: any) => {
    // Auto-populate account-related fields if they exist
    setFormData(prev => ({
      ...prev,
      account_id: accountData.id
    }))
  }

  const handleContactSelect = (contactData: any) => {
    // Auto-populate contact-related fields
    setFormData(prev => {
      const updates: Record<string, any> = {}

      // Populate first_name if field exists
      if (contactData.first_name && fieldConfigs.some(f => f.field_name === 'first_name' && f.is_enabled)) {
        updates.first_name = contactData.first_name
      }

      // Populate last_name if field exists
      if (contactData.last_name && fieldConfigs.some(f => f.field_name === 'last_name' && f.is_enabled)) {
        updates.last_name = contactData.last_name
      }

      // Populate phone_number if field exists
      if (contactData.phone_number && fieldConfigs.some(f => f.field_name === 'phone_number' && f.is_enabled)) {
        updates.phone_number = contactData.phone_number
      }

      // Populate email_address if field exists
      if (contactData.email && fieldConfigs.some(f => f.field_name === 'email_address' && f.is_enabled)) {
        updates.email_address = contactData.email
      }

      return {
        ...prev,
        ...updates
      }
    })

    // Clear errors for auto-populated fields
    setErrors(prev => {
      const newErrors = { ...prev }
      delete newErrors.first_name
      delete newErrors.last_name
      delete newErrors.phone_number
      delete newErrors.email_address
      return newErrors
    })
  }

  const validateForm = () => {
    const newErrors: Record<string, string> = {}

    fieldConfigs
      .filter(f => f.is_enabled && f.is_mandatory && f.field_name !== 'lead_id')
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
      console.log('Lead data before save:', leadData)
      console.log('Form data before save:', formData)

      // Check if lead is being qualified (case-insensitive check for various field names)
      const wasQualified = leadData?.lead_status?.toLowerCase() === 'qualified' ||
                          leadData?.salesStage?.toLowerCase() === 'qualified' ||
                          leadData?.sales_stage?.toLowerCase() === 'qualified'
      const isNowQualified = formData.lead_status?.toLowerCase() === 'qualified' ||
                            formData.salesStage?.toLowerCase() === 'qualified' ||
                            formData.sales_stage?.toLowerCase() === 'qualified'
      const shouldCreateDeal = !wasQualified && isNowQualified

      console.log('Qualification check:', {
        wasQualified,
        isNowQualified,
        shouldCreateDeal,
        leadData_lead_status: leadData?.lead_status,
        leadData_salesStage: leadData?.salesStage,
        leadData_sales_stage: leadData?.sales_stage,
        formData_lead_status: formData.lead_status,
        formData_salesStage: formData.salesStage,
        formData_sales_stage: formData.sales_stage
      })

      const response = await fetch('/api/leads', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: leadId,
          companyId: currentUser.company_id,
          ...formData
        })
      })

      if (response.ok) {
        const savedLead = await response.json()

        // If lead was qualified, create a deal automatically
        if (shouldCreateDeal) {
          console.log('Creating deal from qualified lead...')
          try {
            // Fetch complete lead data with products
            console.log('Fetching complete lead data from API...')
            const leadResponse = await fetch(`/api/leads/${leadId}`)
            console.log('Lead fetch response status:', leadResponse.status)
            const completeLead = leadResponse.ok ? await leadResponse.json() : savedLead
            console.log('Complete lead data:', completeLead)

            console.log('Calling createDealFromLead...')
            const createdDeal = await createDealFromLead(completeLead)
            console.log('Deal created successfully:', createdDeal)

            toast({
              title: "Success",
              description: "Lead qualified and deal created successfully! Redirecting to Deals page..."
            })
            // Redirect to deals page after qualification
            router.push("/deals")
          } catch (dealError) {
            console.error('Error creating deal from qualified lead:', dealError)
            console.error('Error stack:', dealError instanceof Error ? dealError.stack : 'No stack trace')
            toast({
              title: "Warning",
              description: `Lead updated but failed to create deal: ${dealError instanceof Error ? dealError.message : 'Unknown error'}`,
              variant: "destructive",
              duration: 10000
            })
            router.push("/leads")
          }
        } else {
          console.log('Not creating deal - shouldCreateDeal is false')
          toast({
            title: "Success",
            description: "Lead updated successfully"
          })
          router.push("/leads")
        }
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error || "Failed to update lead",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error updating lead:', error)
      toast({
        title: "Error",
        description: "Failed to update lead",
        variant: "destructive"
      })
    } finally {
      setSaving(false)
    }
  }

  const createDealFromLead = async (lead: any) => {
    console.log('createDealFromLead called with lead:', lead)

    // Determine primary product name for deal name
    const primaryProductName = lead.leadProducts && lead.leadProducts.length > 0
      ? lead.leadProducts[0].product_name
      : lead.product_name || 'Products'

    console.log('Primary product name:', primaryProductName)

    // Account and contact are mandatory fields in leads
    console.log('Account name:', lead.account_name)
    console.log('Contact name:', lead.contact_name)

    const dealData = {
      deal_name: `${lead.account_name} - ${primaryProductName}${lead.leadProducts && lead.leadProducts.length > 1 ? ` +${lead.leadProducts.length - 1} more` : ''}`,
      account_name: lead.account_name,
      contact_person: lead.contact_name,
      product: primaryProductName,
      value: lead.calculatedBudget || lead.budget || 0,
      stage: 'Qualification',
      probability: 25,
      expected_close_date: lead.expected_closing_date,
      assigned_to: lead.assigned_to,
      priority: 'Medium',
      status: 'Active',
      source: lead.lead_source,
      source_lead_id: lead.id,
      last_activity: 'Created from qualified lead',
      notes: `Converted from lead: ${lead.id}. Original notes: ${lead.notes || 'None'}`,

      // Copy contact information
      phone: lead.phone,
      email: lead.email,
      whatsapp: lead.whatsapp,

      // Copy lead relations
      product_id: lead.product_id,
      account_id: lead.account_id,
      contact_id: lead.contact_id,

      // Copy location information
      location: lead.location,
      city: lead.city,
      state: lead.state,
      department: lead.department,

      // Products will be fetched and added below
      selected_products: [],

      // Copy lead tracking and dates
      lead_date: lead.lead_date,
      closing_date: lead.closing_date,
      next_followup_date: lead.next_followup_date,
      buyer_ref: lead.buyer_ref,

      // Copy financial details
      budget: lead.budget,
      quantity: lead.quantity,
      price_per_unit: lead.price_per_unit
    }

    // Get products from lead_products table
    try {
      console.log(`Fetching products for lead ${lead.id}`)
      const leadProductsResponse = await fetch(`/api/leads/${lead.id}/products`)
      const leadProductsData = await leadProductsResponse.json()
      const realProducts = leadProductsData.products || []

      console.log(`Lead has ${realProducts.length} products:`, realProducts)

      if (realProducts.length > 0) {
        dealData.selected_products = realProducts.map((product: any) => ({
          product_id: null,
          product_name: product.product_name,
          quantity: product.quantity || 1,
          price_per_unit: product.price_per_unit || 0,
          total_amount: (product.quantity || 1) * (product.price_per_unit || 0)
        }))
      } else if (lead.product_name) {
        dealData.selected_products = [{
          product_id: null,
          product_name: lead.product_name,
          quantity: lead.quantity || 1,
          price_per_unit: lead.price_per_unit || 0,
          total_amount: (lead.quantity || 1) * (lead.price_per_unit || 0)
        }]
      }
    } catch (error) {
      console.error('Error fetching lead products:', error)
      if (lead.product_name) {
        dealData.selected_products = [{
          product_id: null,
          product_name: lead.product_name,
          quantity: lead.quantity || 1,
          price_per_unit: lead.price_per_unit || 0,
          total_amount: (lead.quantity || 1) * (lead.price_per_unit || 0)
        }]
      }
    }

    console.log('Creating deal with data:', dealData)

    // Log the stringified data to check for serialization issues
    try {
      const stringifiedData = JSON.stringify(dealData)
      console.log('Stringified deal data length:', stringifiedData.length)
      console.log('Stringified deal data preview:', stringifiedData.substring(0, 500))
    } catch (stringifyError) {
      console.error('ERROR: Cannot stringify dealData:', stringifyError)
      throw new Error('Deal data contains invalid values that cannot be serialized')
    }

    console.log('About to send POST request to /api/deals...')

    let response
    try {
      response = await fetch('/api/deals', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(dealData)
      })
      console.log('Fetch completed, response received')
    } catch (fetchError) {
      console.error('ERROR during fetch:', fetchError)
      throw new Error(`Network error while creating deal: ${fetchError instanceof Error ? fetchError.message : 'Unknown error'}`)
    }

    console.log('Deal creation response status:', response.status)
    console.log('Deal creation response ok:', response.ok)

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ error: 'Unknown error' }))
      console.error('Deal creation failed with response:', errorData)
      console.error('Error details:', errorData.details)
      console.error('Error hint:', errorData.hint)
      console.error('Error code:', errorData.code)
      throw new Error(`Failed to create deal: ${errorData.details || errorData.error || 'Unknown error'}`)
    }

    const createdDeal = await response.json()
    console.log('Deal created in database:', createdDeal)
    return createdDeal
  }

  const getSectionFields = (section: string) => {
    return fieldConfigs
      .filter(f => f.field_section === section && f.is_enabled && f.field_name !== 'lead_id')
      .sort((a, b) => a.display_order - b.display_order)
  }

  const getEnabledSections = () => {
    const sections = Array.from(
      new Set(
        fieldConfigs
          .filter(f => f.is_enabled && f.field_name !== 'lead_id')
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
  const totalFields = fieldConfigs.filter(f => f.is_enabled && f.field_name !== 'lead_id').length
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
                Review Lead Changes
              </h1>
              <p className="text-gray-600 mt-1">Please verify all changes before updating the lead</p>
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
              {formData.contact_name && (
                <Card className="border-blue-200 bg-blue-50">
                  <CardContent className="p-6">
                    <div className="flex items-center space-x-3">
                      <UserPlus className="w-8 h-8 text-blue-600" />
                      <div>
                        <h2 className="text-2xl font-bold text-blue-900">{formData.contact_name}</h2>
                        <p className="text-sm text-blue-700">
                          {formData.account_name || 'No company'} • {formData.lead_status || 'New'}
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
              {saving ? 'Updating...' : 'Update Lead'}
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
            <Button variant="ghost" onClick={() => router.push("/leads")}>
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back
            </Button>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Edit Lead</h1>
              <p className="text-gray-600">Update the lead details below</p>
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
                          <DynamicLeadField
                            config={field}
                            value={formData[field.field_name]}
                            onChange={handleFieldChange}
                            error={errors[field.field_name]}
                            dependentValues={formData}
                            onAccountSelect={handleAccountSelect}
                            onContactSelect={handleContactSelect}
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
          <Button variant="outline" onClick={() => router.push("/leads")}>
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
