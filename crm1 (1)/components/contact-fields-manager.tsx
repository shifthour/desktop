"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Switch } from "@/components/ui/switch"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
  Settings,
  Save,
  Eye,
  CheckCircle2,
  AlertCircle,
  GripVertical,
  ChevronDown,
  ChevronUp,
  RotateCcw,
  Maximize2,
  Minimize2
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"

interface FieldConfig {
  id: string
  company_id: string
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
  address: 'Address',
  professional: 'Professional Information',
  social: 'Social Media & Web'
}

const SECTION_ORDER = ['basic_info', 'contact', 'professional', 'address', 'social']

export function ContactFieldsManager() {
  const { toast } = useToast()
  const [fieldConfigs, setFieldConfigs] = useState<FieldConfig[]>([])
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [currentUser, setCurrentUser] = useState<any>(null)
  const [selectedSection, setSelectedSection] = useState<string>('basic_info')
  const [hasChanges, setHasChanges] = useState(false)
  const [selectedSectionFilter, setSelectedSectionFilter] = useState<string>('all')
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({
    basic_info: true,
    contact: true,
    address: true,
    professional: true,
    social: true
  })

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
      const response = await fetch(`/api/admin/contact-fields?companyId=${companyId}`)
      if (response.ok) {
        const data = await response.json()
        setFieldConfigs(data)
      } else {
        toast({
          title: "Error",
          description: "Failed to load field configurations",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error loading field configs:', error)
      toast({
        title: "Error",
        description: "Failed to load field configurations",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const handleToggleField = (fieldName: string) => {
    setFieldConfigs(prev => prev.map(field => {
      if (field.field_name === fieldName && !field.is_mandatory) {
        return { ...field, is_enabled: !field.is_enabled }
      }
      return field
    }))
    setHasChanges(true)
  }

  const handleReorder = (fieldName: string, direction: 'up' | 'down') => {
    setFieldConfigs(prev => {
      const sectionFields = prev.filter(f => f.field_section === selectedSection)
      const fieldIndex = sectionFields.findIndex(f => f.field_name === fieldName)

      if (fieldIndex === -1) return prev
      if (direction === 'up' && fieldIndex === 0) return prev
      if (direction === 'down' && fieldIndex === sectionFields.length - 1) return prev

      const newIndex = direction === 'up' ? fieldIndex - 1 : fieldIndex + 1

      // Swap display orders
      const field1 = sectionFields[fieldIndex]
      const field2 = sectionFields[newIndex]

      return prev.map(f => {
        if (f.field_name === field1.field_name) {
          return { ...f, display_order: field2.display_order }
        }
        if (f.field_name === field2.field_name) {
          return { ...f, display_order: field1.display_order }
        }
        return f
      })
    })
    setHasChanges(true)
  }

  const handleSave = async () => {
    if (!currentUser?.company_id) return

    setSaving(true)
    try {
      const response = await fetch('/api/admin/contact-fields', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companyId: currentUser.company_id,
          fieldConfigs: fieldConfigs.map(f => ({
            field_name: f.field_name,
            is_enabled: f.is_enabled,
            display_order: f.display_order,
            field_label: f.field_label,
            placeholder: f.placeholder,
            help_text: f.help_text
          }))
        })
      })

      if (response.ok) {
        toast({
          title: "Success",
          description: "Field configurations saved successfully"
        })
        setHasChanges(false)
        loadFieldConfigurations(currentUser.company_id)
      } else {
        const error = await response.json()
        toast({
          title: "Error",
          description: error.error || "Failed to save configurations",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error saving configs:', error)
      toast({
        title: "Error",
        description: "Failed to save configurations",
        variant: "destructive"
      })
    } finally {
      setSaving(false)
    }
  }

  const handleReset = () => {
    if (confirm('Are you sure you want to reset to default configuration? This will discard all unsaved changes.')) {
      if (currentUser?.company_id) {
        loadFieldConfigurations(currentUser.company_id)
        setHasChanges(false)
      }
    }
  }

  const toggleSection = (section: string) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }))
  }

  const expandAll = () => {
    setExpandedSections({
      basic_info: true,
      contact: true,
      address: true,
      professional: true,
      social: true
    })
  }

  const collapseAll = () => {
    setExpandedSections({
      basic_info: false,
      contact: false,
      address: false,
      professional: false,
      social: false
    })
  }

  const mandatoryFields = fieldConfigs.filter(f => f.is_mandatory)
  const optionalFields = fieldConfigs.filter(f => !f.is_mandatory)
  const enabledCount = fieldConfigs.filter(f => f.is_enabled).length
  const totalCount = fieldConfigs.length

  const getSectionFields = (section: string) => {
    return fieldConfigs
      .filter(f => f.field_section === section)
      .sort((a, b) => a.display_order - b.display_order)
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading field configurations...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 flex items-center">
            <Settings className="w-8 h-8 mr-3 text-blue-600" />
            Contact Field Configuration
          </h1>
          <p className="text-gray-600 mt-2">
            Customize which fields appear in the contact creation form
          </p>
        </div>
        <div className="flex space-x-3">
          <Button
            variant="outline"
            onClick={handleReset}
            disabled={!hasChanges || saving}
          >
            <RotateCcw className="w-4 h-4 mr-2" />
            Reset
          </Button>
          <Button
            onClick={handleSave}
            disabled={!hasChanges || saving}
            className="bg-blue-600 hover:bg-blue-700"
          >
            <Save className="w-4 h-4 mr-2" />
            {saving ? 'Saving...' : 'Save Changes'}
          </Button>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-600">Total Fields</p>
                <p className="text-2xl font-bold text-gray-900">{totalCount}</p>
              </div>
              <Settings className="w-8 h-8 text-gray-400" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-600">Mandatory Fields</p>
                <p className="text-2xl font-bold text-orange-600">{mandatoryFields.length}</p>
              </div>
              <AlertCircle className="w-8 h-8 text-orange-400" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-600">Enabled Fields</p>
                <p className="text-2xl font-bold text-green-600">{enabledCount}</p>
              </div>
              <CheckCircle2 className="w-8 h-8 text-green-400" />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-600">Optional Fields</p>
                <p className="text-2xl font-bold text-blue-600">{optionalFields.length}</p>
              </div>
              <Eye className="w-8 h-8 text-blue-400" />
            </div>
          </CardContent>
        </Card>
      </div>

      {hasChanges && (
        <Card className="border-yellow-300 bg-yellow-50">
          <CardContent className="p-4">
            <div className="flex items-center">
              <AlertCircle className="w-5 h-5 text-yellow-600 mr-3" />
              <p className="text-yellow-800 font-medium">
                You have unsaved changes. Click "Save Changes" to apply your configuration.
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      <div className="max-w-7xl mx-auto">
        {/* Field Configuration */}
        <div className="space-y-6">
          <Tabs defaultValue="mandatory" className="w-full">
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="mandatory">
                Mandatory Fields ({mandatoryFields.length})
              </TabsTrigger>
              <TabsTrigger value="optional">
                Optional Fields ({optionalFields.length})
              </TabsTrigger>
            </TabsList>

            <TabsContent value="mandatory" className="space-y-4">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center">
                    <AlertCircle className="w-5 h-5 mr-2 text-orange-600" />
                    Mandatory Fields
                  </CardTitle>
                  <CardDescription>
                    These fields are required for all contacts and cannot be disabled
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <ScrollArea className="h-[600px] pr-4">
                    <div className="space-y-3">
                      {mandatoryFields.map((field) => (
                        <Card key={field.id} className="border-orange-200 bg-orange-50">
                          <CardContent className="p-4">
                            <div className="flex items-center justify-between">
                              <div className="flex-1">
                                <div className="flex items-center space-x-3">
                                  <Badge className="bg-orange-600">Required</Badge>
                                  <h3 className="font-semibold text-gray-900">{field.field_label}</h3>
                                </div>
                                <p className="text-sm text-gray-600 mt-1">
                                  Type: <span className="font-mono text-xs bg-gray-200 px-2 py-1 rounded">{field.field_type}</span>
                                </p>
                                {field.help_text && (
                                  <p className="text-xs text-gray-500 mt-1">{field.help_text}</p>
                                )}
                              </div>
                              <div className="flex items-center space-x-2">
                                <CheckCircle2 className="w-5 h-5 text-orange-600" />
                              </div>
                            </div>
                          </CardContent>
                        </Card>
                      ))}
                    </div>
                  </ScrollArea>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="optional" className="space-y-4">
              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <CardTitle className="flex items-center">
                        <Eye className="w-5 h-5 mr-2 text-blue-600" />
                        Optional Fields
                      </CardTitle>
                      <CardDescription>
                        Toggle these fields on/off to customize your contact form
                      </CardDescription>
                    </div>
                    <div className="flex space-x-2">
                      <Button variant="outline" size="sm" onClick={expandAll}>
                        <Maximize2 className="w-4 h-4 mr-2" />
                        Expand All
                      </Button>
                      <Button variant="outline" size="sm" onClick={collapseAll}>
                        <Minimize2 className="w-4 h-4 mr-2" />
                        Collapse All
                      </Button>
                    </div>
                  </div>

                  {/* Section Filter */}
                  <div className="flex items-center space-x-3">
                    <Label className="text-sm font-medium">Filter by Section:</Label>
                    <Select value={selectedSectionFilter} onValueChange={setSelectedSectionFilter}>
                      <SelectTrigger className="w-[250px]">
                        <SelectValue placeholder="All Sections" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All Sections</SelectItem>
                        <SelectItem value="basic_info">Basic Information</SelectItem>
                        <SelectItem value="contact">Contact Information</SelectItem>
                        <SelectItem value="professional">Professional Information</SelectItem>
                        <SelectItem value="address">Address</SelectItem>
                        <SelectItem value="social">Social Media & Web</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </CardHeader>
                <CardContent>
                  {optionalFields.length === 0 ? (
                    <div className="text-center py-12">
                      <AlertCircle className="w-16 h-16 mx-auto text-gray-400 mb-4" />
                      <h3 className="text-lg font-semibold text-gray-900 mb-2">No Optional Fields Found</h3>
                      <p className="text-gray-600 mb-4">
                        Field configurations may not have been seeded yet.
                        <br />
                        Please ensure the SQL setup script was executed successfully.
                      </p>
                      <Button onClick={() => loadFieldConfigurations(currentUser?.company_id)}>
                        <RotateCcw className="w-4 h-4 mr-2" />
                        Retry Loading
                      </Button>
                    </div>
                  ) : (
                    <ScrollArea className="h-[600px] pr-4">
                      {SECTION_ORDER
                        .filter(section => selectedSectionFilter === 'all' || section === selectedSectionFilter)
                        .map(section => {
                        const sectionFields = optionalFields.filter(f => f.field_section === section)
                        if (sectionFields.length === 0) return null

                      return (
                        <div key={section} className="mb-4">
                          <button
                            onClick={() => toggleSection(section)}
                            className="w-full flex items-center justify-between p-3 bg-gray-100 hover:bg-gray-200 rounded-lg mb-2"
                          >
                            <div className="flex items-center space-x-2">
                              <h3 className="font-semibold text-gray-900">{SECTION_LABELS[section]}</h3>
                              <Badge variant="outline">{sectionFields.length} fields</Badge>
                            </div>
                            {expandedSections[section] ? (
                              <ChevronUp className="w-5 h-5" />
                            ) : (
                              <ChevronDown className="w-5 h-5" />
                            )}
                          </button>

                          {expandedSections[section] && (
                            <div className="space-y-2 pl-2">
                              {sectionFields
                                .sort((a, b) => a.display_order - b.display_order)
                                .map((field, index) => (
                                <Card key={field.id} className={field.is_enabled ? 'border-blue-200' : 'border-gray-200 opacity-60'}>
                                  <CardContent className="p-3">
                                    <div className="flex items-center justify-between">
                                      <div className="flex items-center space-x-3 flex-1">
                                        <GripVertical className="w-4 h-4 text-gray-400" />
                                        <div className="flex-1">
                                          <div className="flex items-center space-x-2">
                                            <h4 className="font-medium text-gray-900">{field.field_label}</h4>
                                            <span className="text-xs text-gray-500 font-mono">{field.field_type}</span>
                                          </div>
                                          {field.placeholder && (
                                            <p className="text-xs text-gray-500">Placeholder: {field.placeholder}</p>
                                          )}
                                        </div>
                                      </div>
                                      <div className="flex items-center space-x-2">
                                        <div className="flex flex-col space-y-1">
                                          <Button
                                            variant="ghost"
                                            size="sm"
                                            onClick={() => handleReorder(field.field_name, 'up')}
                                            disabled={index === 0}
                                            className="h-6 w-6 p-0"
                                          >
                                            <ChevronUp className="w-4 h-4" />
                                          </Button>
                                          <Button
                                            variant="ghost"
                                            size="sm"
                                            onClick={() => handleReorder(field.field_name, 'down')}
                                            disabled={index === sectionFields.length - 1}
                                            className="h-6 w-6 p-0"
                                          >
                                            <ChevronDown className="w-4 h-4" />
                                          </Button>
                                        </div>
                                        <Switch
                                          checked={field.is_enabled}
                                          onCheckedChange={() => handleToggleField(field.field_name)}
                                        />
                                      </div>
                                    </div>
                                  </CardContent>
                                </Card>
                              ))}
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </ScrollArea>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </div>
      </div>
    </div>
  )
}
