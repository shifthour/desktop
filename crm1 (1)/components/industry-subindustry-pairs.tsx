"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Plus, X } from "lucide-react"

interface IndustryPair {
  industry: string
  subIndustry: string
}

interface IndustrySubIndustryPairsProps {
  value: IndustryPair[]
  onChange: (pairs: IndustryPair[]) => void
  error?: string
}

// Industry and SubIndustry options
const INDUSTRIES = [
  'Agriculture',
  'Automotive',
  'Banking & Finance',
  'Construction',
  'Education',
  'Energy & Utilities',
  'Healthcare',
  'Hospitality',
  'Information Technology',
  'Insurance',
  'Manufacturing',
  'Media & Entertainment',
  'Pharmaceuticals',
  'Real Estate',
  'Retail',
  'Technology',
  'Telecommunications',
  'Transportation & Logistics',
  'Other'
]

const SUB_INDUSTRIES: Record<string, string[]> = {
  'Agriculture': ['Crop Production', 'Livestock', 'Forestry', 'Fishing', 'Agricultural Equipment'],
  'Automotive': ['Auto Manufacturing', 'Auto Parts', 'Auto Dealerships', 'Auto Repair', 'Electric Vehicles'],
  'Banking & Finance': ['Commercial Banking', 'Investment Banking', 'Asset Management', 'Insurance', 'Fintech'],
  'Construction': ['Residential Construction', 'Commercial Construction', 'Infrastructure', 'Specialty Trade'],
  'Education': ['K-12 Education', 'Higher Education', 'Online Education', 'Training & Development'],
  'Energy & Utilities': ['Oil & Gas', 'Renewable Energy', 'Electric Utilities', 'Water Utilities'],
  'Healthcare': ['Hospitals', 'Clinics', 'Medical Devices', 'Healthcare IT', 'Pharmaceuticals'],
  'Hospitality': ['Hotels', 'Restaurants', 'Travel & Tourism', 'Event Management'],
  'Information Technology': ['Software Development', 'IT Services', 'Cloud Computing', 'Cybersecurity'],
  'Insurance': ['Life Insurance', 'Property & Casualty', 'Health Insurance', 'Reinsurance'],
  'Manufacturing': ['Electronics', 'Machinery', 'Consumer Goods', 'Industrial Equipment', 'Chemicals'],
  'Media & Entertainment': ['Broadcasting', 'Publishing', 'Film & Video', 'Gaming', 'Music'],
  'Pharmaceuticals': ['Drug Manufacturing', 'Biotechnology', 'Medical Research', 'Generic Drugs'],
  'Real Estate': ['Residential', 'Commercial', 'Industrial', 'Property Management', 'REITs'],
  'Retail': ['E-commerce', 'Department Stores', 'Specialty Retail', 'Grocery', 'Fashion'],
  'Technology': ['Hardware', 'Software', 'Semiconductors', 'Internet Services'],
  'Telecommunications': ['Wireless', 'Broadband', 'Satellite', 'VoIP', '5G'],
  'Transportation & Logistics': ['Shipping', 'Trucking', 'Airlines', 'Warehousing', 'Supply Chain'],
  'Other': ['Consulting', 'Legal Services', 'Accounting', 'Marketing', 'Other Services']
}

export function IndustrySubIndustryPairs({ value, onChange, error }: IndustrySubIndustryPairsProps) {
  const [pairs, setPairs] = useState<IndustryPair[]>(
    value && value.length > 0 ? value : [{ industry: '', subIndustry: '' }]
  )

  // Update parent when pairs change
  useEffect(() => {
    onChange(pairs)
  }, [pairs])

  const addPair = () => {
    setPairs([...pairs, { industry: '', subIndustry: '' }])
  }

  const removePair = (index: number) => {
    if (pairs.length > 1) {
      setPairs(pairs.filter((_, i) => i !== index))
    }
  }

  const updateIndustry = (index: number, industry: string) => {
    const newPairs = [...pairs]
    newPairs[index] = { industry, subIndustry: '' } // Reset sub-industry when industry changes
    setPairs(newPairs)
  }

  const updateSubIndustry = (index: number, subIndustry: string) => {
    const newPairs = [...pairs]
    newPairs[index].subIndustry = subIndustry
    setPairs(newPairs)
  }

  const getSubIndustries = (industry: string): string[] => {
    return SUB_INDUSTRIES[industry] || []
  }

  return (
    <div className="space-y-3">
      <Label>Industries & Sub-Industries *</Label>
      <p className="text-sm text-gray-500">
        Add all industries and sub-industries that this distributor operates in
      </p>

      {pairs.map((pair, index) => (
        <div key={index} className="flex items-start gap-3 p-4 border rounded-lg bg-gray-50">
          <div className="flex-1 grid grid-cols-2 gap-3">
            {/* Industry Select */}
            <div>
              <Label className="text-xs text-gray-600">Industry {index + 1}</Label>
              <Select
                value={pair.industry}
                onValueChange={(value) => updateIndustry(index, value)}
              >
                <SelectTrigger className="bg-white">
                  <SelectValue placeholder="Select industry" />
                </SelectTrigger>
                <SelectContent>
                  {INDUSTRIES.map((industry) => (
                    <SelectItem key={industry} value={industry}>
                      {industry}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Sub-Industry Select */}
            <div>
              <Label className="text-xs text-gray-600">Sub-Industry {index + 1}</Label>
              <Select
                value={pair.subIndustry}
                onValueChange={(value) => updateSubIndustry(index, value)}
                disabled={!pair.industry}
              >
                <SelectTrigger className="bg-white">
                  <SelectValue placeholder={pair.industry ? "Select sub-industry" : "Select industry first"} />
                </SelectTrigger>
                <SelectContent>
                  {getSubIndustries(pair.industry).map((subIndustry) => (
                    <SelectItem key={subIndustry} value={subIndustry}>
                      {subIndustry}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Remove Button */}
          <Button
            type="button"
            variant="ghost"
            size="icon"
            onClick={() => removePair(index)}
            disabled={pairs.length === 1}
            className="mt-6 text-red-600 hover:text-red-700 hover:bg-red-50"
          >
            <X className="w-4 h-4" />
          </Button>
        </div>
      ))}

      {/* Add Button */}
      <Button
        type="button"
        variant="outline"
        onClick={addPair}
        className="w-full"
      >
        <Plus className="w-4 h-4 mr-2" />
        Add Another Industry
      </Button>

      {error && (
        <p className="text-sm text-red-600">{error}</p>
      )}
    </div>
  )
}
