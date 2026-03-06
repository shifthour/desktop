"use client"

import { useState, useEffect } from "react"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Button } from "@/components/ui/button"
import { Plus, X } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"

interface FieldConfig {
  field_name: string
  field_label: string
  field_type: string
  is_mandatory: boolean
  is_enabled: boolean
  field_options?: string[]
  placeholder?: string
  validation_rules?: any
  help_text?: string
}

interface DynamicFieldProps {
  config: FieldConfig
  value: any
  onChange: (fieldName: string, value: any) => void
  error?: string
  // For dependent dropdowns like subindustry
  dependentValues?: Record<string, any>
  // For multi-select industries (distributor)
  onIndustriesChange?: (industries: { industry: string; subIndustry: string }[]) => void
  industries?: { industry: string; subIndustry: string }[]
  industriesError?: string
}

export function DynamicAccountField({
  config,
  value,
  onChange,
  error,
  dependentValues = {},
  onIndustriesChange,
  industries = [],
  industriesError
}: DynamicFieldProps) {
  const [subIndustries, setSubIndustries] = useState<string[]>([])
  const [loadingSubIndustries, setLoadingSubIndustries] = useState(false)
  const [industrySubIndustriesMap, setIndustrySubIndustriesMap] = useState<Record<number, string[]>>({})

  const isDistributor = dependentValues.account_type === 'Distributor' || dependentValues.accountType === 'Distributor'

  // Load sub-industries when industry changes
  useEffect(() => {
    if (config.field_name === 'acct_sub_industry' && dependentValues.acct_industry) {
      loadSubIndustries(dependentValues.acct_industry)
    }
  }, [dependentValues.acct_industry, config.field_name])

  const loadSubIndustries = async (industry: string, pairIndex?: number) => {
    if (pairIndex !== undefined) {
      // Loading for a specific pair in multi-select
      try {
        const response = await fetch(`/api/industries?industry=${encodeURIComponent(industry)}`)
        if (response.ok) {
          const data = await response.json()
          setIndustrySubIndustriesMap(prev => ({
            ...prev,
            [pairIndex]: data
          }))
        }
      } catch (error) {
        console.error('Error loading sub-industries:', error)
      }
    } else {
      // Loading for single select
      setLoadingSubIndustries(true)
      try {
        const response = await fetch(`/api/industries?industry=${encodeURIComponent(industry)}`)
        if (response.ok) {
          const data = await response.json()
          setSubIndustries(data)
        }
      } catch (error) {
        console.error('Error loading sub-industries:', error)
      } finally {
        setLoadingSubIndustries(false)
      }
    }
  }

  // Multi-select industry handlers
  const handleAddIndustryPair = () => {
    if (onIndustriesChange) {
      onIndustriesChange([...industries, { industry: '', subIndustry: '' }])
    }
  }

  const handleRemoveIndustryPair = (index: number) => {
    if (onIndustriesChange) {
      const newIndustries = industries.filter((_, i) => i !== index)
      onIndustriesChange(newIndustries)
      // Clean up sub-industries map
      const newMap = { ...industrySubIndustriesMap }
      delete newMap[index]
      setIndustrySubIndustriesMap(newMap)
    }
  }

  const handleIndustryChange = (index: number, industry: string) => {
    if (onIndustriesChange) {
      const newIndustries = [...industries]
      newIndustries[index] = { industry, subIndustry: '' }
      onIndustriesChange(newIndustries)
      // Load sub-industries for this industry
      if (industry) {
        loadSubIndustries(industry, index)
      }
    }
  }

  const handleSubIndustryChange = (index: number, subIndustry: string) => {
    if (onIndustriesChange) {
      const newIndustries = [...industries]
      newIndustries[index] = { ...newIndustries[index], subIndustry }
      onIndustriesChange(newIndustries)
    }
  }

  const handleChange = (newValue: any) => {
    onChange(config.field_name, newValue)
  }

  const getOptions = () => {
    // For dependent sub-industry dropdown
    if (config.field_name === 'acct_sub_industry') {
      return subIndustries
    }
    // For regular select fields
    return config.field_options || []
  }

  const renderField = () => {
    switch (config.field_type) {
      case 'text':
      case 'email':
      case 'tel':
      case 'url':
        return (
          <Input
            id={config.field_name}
            type={config.field_type}
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            placeholder={config.placeholder}
            className={error ? 'border-red-500' : ''}
          />
        )

      case 'number':
        return (
          <Input
            id={config.field_name}
            type="number"
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            placeholder={config.placeholder}
            className={error ? 'border-red-500' : ''}
          />
        )

      case 'textarea':
        return (
          <Textarea
            id={config.field_name}
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            placeholder={config.placeholder}
            className={error ? 'border-red-500' : ''}
            rows={3}
          />
        )

      case 'date':
        return (
          <Input
            id={config.field_name}
            type="date"
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            className={error ? 'border-red-500' : ''}
          />
        )

      case 'select':
      case 'select_dependent':
        const options = getOptions()
        const isDisabled = config.field_name === 'acct_sub_industry' && !dependentValues.acct_industry

        return (
          <Select
            value={value || ''}
            onValueChange={handleChange}
            disabled={isDisabled || loadingSubIndustries}
          >
            <SelectTrigger className={error ? 'border-red-500' : ''}>
              <SelectValue
                placeholder={
                  loadingSubIndustries
                    ? 'Loading...'
                    : isDisabled
                    ? 'Select industry first'
                    : config.placeholder || `Select ${config.field_label.toLowerCase()}`
                }
              />
            </SelectTrigger>
            <SelectContent>
              {options.map((option, index) => (
                <SelectItem key={`${config.field_name}-${index}`} value={option}>
                  {option}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )

      default:
        return (
          <Input
            id={config.field_name}
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            placeholder={config.placeholder}
            className={error ? 'border-red-500' : ''}
          />
        )
    }
  }

  if (!config.is_enabled) {
    return null
  }

  // Hide acct_sub_industry if account_type is Distributor (it's included in the multi-select)
  if (isDistributor && config.field_name === 'acct_sub_industry') {
    return null
  }

  // Show multi-select Account Industry for distributors
  if (config.field_name === 'acct_industry' && isDistributor && onIndustriesChange) {
    return (
      <div className="space-y-4 md:col-span-2">
        <div>
          <Label>
            Account Industry
            <span className="text-red-500 ml-1">*</span>
          </Label>
          <p className="text-xs text-gray-500 mt-1">Select all industries this distributor operates in</p>
        </div>

        <div className="space-y-3">
          {industries.map((pair, index) => (
            <Card key={index} className="border-blue-200">
              <CardContent className="pt-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Industry {index + 1}</Label>
                    <Select
                      value={pair.industry}
                      onValueChange={(value) => handleIndustryChange(index, value)}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select industry" />
                      </SelectTrigger>
                      <SelectContent>
                        {(config.field_options || []).map((option, optIndex) => (
                          <SelectItem key={`industry-${index}-${optIndex}`} value={option}>
                            {option}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="space-y-2">
                    <Label>Sub-Industry</Label>
                    <div className="flex gap-2">
                      <Select
                        value={pair.subIndustry}
                        onValueChange={(value) => handleSubIndustryChange(index, value)}
                        disabled={!pair.industry}
                      >
                        <SelectTrigger>
                          <SelectValue
                            placeholder={pair.industry ? "Select sub-industry" : "Select industry first"}
                          />
                        </SelectTrigger>
                        <SelectContent>
                          {(industrySubIndustriesMap[index] || []).map((subInd, subIndex) => (
                            <SelectItem key={`subindustry-${index}-${subIndex}`} value={subInd}>
                              {subInd}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      {industries.length > 1 && (
                        <Button
                          type="button"
                          variant="outline"
                          size="icon"
                          onClick={() => handleRemoveIndustryPair(index)}
                          className="shrink-0"
                        >
                          <X className="h-4 w-4" />
                        </Button>
                      )}
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>

        <Button
          type="button"
          variant="outline"
          onClick={handleAddIndustryPair}
          className="w-full"
        >
          <Plus className="h-4 w-4 mr-2" />
          Add Another Industry
        </Button>

        {industriesError && (
          <p className="text-xs text-red-500">{industriesError}</p>
        )}
      </div>
    )
  }

  return (
    <div className="space-y-2">
      <Label htmlFor={config.field_name}>
        {config.field_label}
        {config.is_mandatory && <span className="text-red-500 ml-1">*</span>}
      </Label>
      {renderField()}
      {config.help_text && (
        <p className="text-xs text-gray-500">{config.help_text}</p>
      )}
      {error && (
        <p className="text-xs text-red-500">{error}</p>
      )}
    </div>
  )
}
