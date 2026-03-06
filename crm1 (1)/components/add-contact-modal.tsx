"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { X, User, Building2, Phone, Mail, MapPin, Briefcase, Calendar, Star, Users, UserCheck, Globe } from "lucide-react"

interface Account {
  id: number
  accountName: string
  industry: string
  city: string
}

interface AddContactModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (contactData: any) => void
}

export function AddContactModal({ isOpen, onClose, onSave }: AddContactModalProps) {
  const [currentStep, setCurrentStep] = useState(1)
  const [formData, setFormData] = useState({
    // Personal Information
    firstName: "",
    lastName: "",
    title: "",
    department: "",
    reportingTo: "",
    
    // Account & Company
    accountId: "",
    accountName: "",
    
    // Contact Information
    phone: "",
    mobile: "",
    email: "",
    whatsapp: "",
    linkedin: "",
    
    // Address
    address: "",
    city: "",
    state: "",
    country: "",
    postalCode: "",
    
    // CRM Details
    contactSource: "",
    priority: "medium",
    status: "active",
    tags: [] as string[],
    notes: "",
    birthday: "",
    
    // Sales Information
    dealValue: "",
    lastContactDate: "",
    nextFollowUp: "",
    preferredCommunication: "email"
  })

  const [errors, setErrors] = useState<Record<string, string>>({})
  const [newTag, setNewTag] = useState("")

  // Mock accounts data - replace with actual API call
  const accounts: Account[] = [
    { id: 1, accountName: "3B BlackBio Biotech India Ltd.", industry: "Biotechnology", city: "BHOPAL" },
    { id: 2, accountName: "3M India Ltd.", industry: "Manufacturing", city: "BANGALORE" },
    { id: 3, accountName: "A E International", industry: "Trading", city: "DHAKA" },
    { id: 4, accountName: "A Molecular Research Center", industry: "Research", city: "Ahmedabad" },
    { id: 5, accountName: "TSAR Labcare", industry: "Laboratory Services", city: "BANGALORE" }
  ]

  const departments = [
    "Research & Development",
    "Quality Control",
    "Quality Assurance", 
    "Production",
    "Procurement",
    "Sales & Marketing",
    "Technical Support",
    "Laboratory Services",
    "Operations",
    "Finance",
    "Human Resources",
    "IT",
    "Management",
    "Regulatory Affairs",
    "Supply Chain"
  ]

  const contactSources = [
    "Website Inquiry",
    "Cold Call",
    "Referral",
    "Trade Show",
    "LinkedIn",
    "Email Campaign",
    "Partner",
    "Existing Customer",
    "Social Media",
    "Advertisement"
  ]

  const priorities = [
    { value: "high", label: "High", color: "bg-red-100 text-red-800" },
    { value: "medium", label: "Medium", color: "bg-yellow-100 text-yellow-800" },
    { value: "low", label: "Low", color: "bg-gray-100 text-gray-800" }
  ]

  const communicationMethods = [
    { value: "email", label: "Email" },
    { value: "phone", label: "Phone" },
    { value: "whatsapp", label: "WhatsApp" },
    { value: "linkedin", label: "LinkedIn" }
  ]

  const validateStep = (step: number) => {
    const newErrors: Record<string, string> = {}

    if (step === 1) {
      if (!formData.firstName.trim()) newErrors.firstName = "First name is required"
      if (!formData.lastName.trim()) newErrors.lastName = "Last name is required"
      if (!formData.title.trim()) newErrors.title = "Title is required"
      if (!formData.department.trim()) newErrors.department = "Department is required"
      if (!formData.accountId.trim()) newErrors.accountId = "Account selection is mandatory"
    }

    if (step === 2) {
      if (!formData.email.trim()) {
        newErrors.email = "Email is required"
      } else if (!/\S+@\S+\.\S+/.test(formData.email)) {
        newErrors.email = "Invalid email format"
      }
      if (!formData.phone.trim()) newErrors.phone = "Phone number is required"
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleNext = () => {
    if (validateStep(currentStep)) {
      setCurrentStep(currentStep + 1)
    }
  }

  const handlePrevious = () => {
    setCurrentStep(currentStep - 1)
  }

  const handleSave = () => {
    if (validateStep(currentStep)) {
      onSave(formData)
      resetForm()
      onClose()
    }
  }

  const resetForm = () => {
    setFormData({
      firstName: "",
      lastName: "",
      title: "",
      department: "",
      reportingTo: "",
      accountId: "",
      accountName: "",
      phone: "",
      mobile: "",
      email: "",
      whatsapp: "",
      linkedin: "",
      address: "",
      city: "",
      state: "",
      country: "",
      postalCode: "",
      contactSource: "",
      priority: "medium",
      status: "active",
      tags: [],
      notes: "",
      birthday: "",
      dealValue: "",
      lastContactDate: "",
      nextFollowUp: "",
      preferredCommunication: "email"
    })
    setCurrentStep(1)
    setErrors({})
    setNewTag("")
  }

  const handleAccountSelect = (accountId: string) => {
    const selectedAccount = accounts.find(acc => acc.id.toString() === accountId)
    setFormData(prev => ({
      ...prev,
      accountId,
      accountName: selectedAccount?.accountName || ""
    }))
  }

  const addTag = () => {
    if (newTag.trim() && !formData.tags.includes(newTag.trim())) {
      setFormData(prev => ({
        ...prev,
        tags: [...prev.tags, newTag.trim()]
      }))
      setNewTag("")
    }
  }

  const removeTag = (tagToRemove: string) => {
    setFormData(prev => ({
      ...prev,
      tags: prev.tags.filter(tag => tag !== tagToRemove)
    }))
  }

  const totalSteps = 4
  const progressPercentage = (currentStep / totalSteps) * 100

  const renderStep1 = () => (
    <div className="space-y-6">
      <div className="flex items-center space-x-2 mb-4">
        <User className="w-5 h-5 text-blue-600" />
        <h3 className="text-lg font-semibold">Personal & Professional Details</h3>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="firstName">First Name *</Label>
          <Input
            id="firstName"
            value={formData.firstName}
            onChange={(e) => setFormData(prev => ({ ...prev, firstName: e.target.value }))}
            className={errors.firstName ? "border-red-500" : ""}
          />
          {errors.firstName && <p className="text-sm text-red-600">{errors.firstName}</p>}
        </div>

        <div className="space-y-2">
          <Label htmlFor="lastName">Last Name *</Label>
          <Input
            id="lastName"
            value={formData.lastName}
            onChange={(e) => setFormData(prev => ({ ...prev, lastName: e.target.value }))}
            className={errors.lastName ? "border-red-500" : ""}
          />
          {errors.lastName && <p className="text-sm text-red-600">{errors.lastName}</p>}
        </div>
      </div>

      <div className="space-y-2">
        <Label htmlFor="title">Job Title *</Label>
        <Input
          id="title"
          placeholder="e.g., Senior Research Scientist, Lab Manager"
          value={formData.title}
          onChange={(e) => setFormData(prev => ({ ...prev, title: e.target.value }))}
          className={errors.title ? "border-red-500" : ""}
        />
        {errors.title && <p className="text-sm text-red-600">{errors.title}</p>}
      </div>

      <div className="space-y-2">
        <Label htmlFor="department">Department *</Label>
        <Select 
          value={formData.department} 
          onValueChange={(value) => setFormData(prev => ({ ...prev, department: value }))}
        >
          <SelectTrigger className={errors.department ? "border-red-500" : ""}>
            <SelectValue placeholder="Select department" />
          </SelectTrigger>
          <SelectContent>
            {departments.map(dept => (
              <SelectItem key={dept} value={dept}>{dept}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        {errors.department && <p className="text-sm text-red-600">{errors.department}</p>}
      </div>

      <div className="space-y-2">
        <Label htmlFor="account">Account *</Label>
        <Select 
          value={formData.accountId} 
          onValueChange={handleAccountSelect}
        >
          <SelectTrigger className={errors.accountId ? "border-red-500" : ""}>
            <SelectValue placeholder="Select account (mandatory)" />
          </SelectTrigger>
          <SelectContent>
            {accounts.map(account => (
              <SelectItem key={account.id} value={account.id.toString()}>
                <div className="flex flex-col">
                  <span className="font-medium">{account.accountName}</span>
                  <span className="text-sm text-gray-500">{account.industry} • {account.city}</span>
                </div>
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        {errors.accountId && <p className="text-sm text-red-600">{errors.accountId}</p>}
      </div>

      <div className="space-y-2">
        <Label htmlFor="reportingTo">Reports To</Label>
        <Input
          id="reportingTo"
          placeholder="Manager or supervisor name"
          value={formData.reportingTo}
          onChange={(e) => setFormData(prev => ({ ...prev, reportingTo: e.target.value }))}
        />
      </div>
    </div>
  )

  const renderStep2 = () => (
    <div className="space-y-6">
      <div className="flex items-center space-x-2 mb-4">
        <Phone className="w-5 h-5 text-green-600" />
        <h3 className="text-lg font-semibold">Contact Information</h3>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="phone">Phone Number *</Label>
          <Input
            id="phone"
            placeholder="+91 98765 43210"
            value={formData.phone}
            onChange={(e) => setFormData(prev => ({ ...prev, phone: e.target.value }))}
            className={errors.phone ? "border-red-500" : ""}
          />
          {errors.phone && <p className="text-sm text-red-600">{errors.phone}</p>}
        </div>

        <div className="space-y-2">
          <Label htmlFor="mobile">Mobile Number</Label>
          <Input
            id="mobile"
            placeholder="+91 87654 32109"
            value={formData.mobile}
            onChange={(e) => setFormData(prev => ({ ...prev, mobile: e.target.value }))}
          />
        </div>
      </div>

      <div className="space-y-2">
        <Label htmlFor="email">Email Address *</Label>
        <Input
          id="email"
          type="email"
          placeholder="contact@example.com"
          value={formData.email}
          onChange={(e) => setFormData(prev => ({ ...prev, email: e.target.value }))}
          className={errors.email ? "border-red-500" : ""}
        />
        {errors.email && <p className="text-sm text-red-600">{errors.email}</p>}
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="whatsapp">WhatsApp Number</Label>
          <Input
            id="whatsapp"
            placeholder="+91 98765 43210"
            value={formData.whatsapp}
            onChange={(e) => setFormData(prev => ({ ...prev, whatsapp: e.target.value }))}
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="linkedin">LinkedIn Profile</Label>
          <Input
            id="linkedin"
            placeholder="https://linkedin.com/in/username"
            value={formData.linkedin}
            onChange={(e) => setFormData(prev => ({ ...prev, linkedin: e.target.value }))}
          />
        </div>
      </div>

      <div className="space-y-2">
        <Label htmlFor="preferredCommunication">Preferred Communication</Label>
        <Select 
          value={formData.preferredCommunication} 
          onValueChange={(value) => setFormData(prev => ({ ...prev, preferredCommunication: value }))}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {communicationMethods.map(method => (
              <SelectItem key={method.value} value={method.value}>
                {method.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    </div>
  )

  const renderStep3 = () => (
    <div className="space-y-6">
      <div className="flex items-center space-x-2 mb-4">
        <MapPin className="w-5 h-5 text-purple-600" />
        <h3 className="text-lg font-semibold">Address & Additional Info</h3>
      </div>

      <div className="space-y-2">
        <Label htmlFor="address">Street Address</Label>
        <Textarea
          id="address"
          placeholder="Street address, building, suite"
          value={formData.address}
          onChange={(e) => setFormData(prev => ({ ...prev, address: e.target.value }))}
          rows={2}
        />
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="city">City</Label>
          <Input
            id="city"
            value={formData.city}
            onChange={(e) => setFormData(prev => ({ ...prev, city: e.target.value }))}
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="state">State/Province</Label>
          <Input
            id="state"
            value={formData.state}
            onChange={(e) => setFormData(prev => ({ ...prev, state: e.target.value }))}
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="country">Country</Label>
          <Input
            id="country"
            value={formData.country}
            onChange={(e) => setFormData(prev => ({ ...prev, country: e.target.value }))}
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="postalCode">Postal Code</Label>
          <Input
            id="postalCode"
            value={formData.postalCode}
            onChange={(e) => setFormData(prev => ({ ...prev, postalCode: e.target.value }))}
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="birthday">Birthday</Label>
          <Input
            id="birthday"
            type="date"
            value={formData.birthday}
            onChange={(e) => setFormData(prev => ({ ...prev, birthday: e.target.value }))}
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="contactSource">Contact Source</Label>
          <Select 
            value={formData.contactSource} 
            onValueChange={(value) => setFormData(prev => ({ ...prev, contactSource: value }))}
          >
            <SelectTrigger>
              <SelectValue placeholder="How did you find them?" />
            </SelectTrigger>
            <SelectContent>
              {contactSources.map(source => (
                <SelectItem key={source} value={source}>{source}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>
  )

  const renderStep4 = () => (
    <div className="space-y-6">
      <div className="flex items-center space-x-2 mb-4">
        <Briefcase className="w-5 h-5 text-orange-600" />
        <h3 className="text-lg font-semibold">CRM & Sales Information</h3>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="priority">Priority Level</Label>
          <Select 
            value={formData.priority} 
            onValueChange={(value) => setFormData(prev => ({ ...prev, priority: value }))}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {priorities.map(priority => (
                <SelectItem key={priority.value} value={priority.value}>
                  <div className="flex items-center space-x-2">
                    <Badge className={priority.color}>{priority.label}</Badge>
                  </div>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="dealValue">Potential Deal Value</Label>
          <Input
            id="dealValue"
            placeholder="₹5,00,000"
            value={formData.dealValue}
            onChange={(e) => setFormData(prev => ({ ...prev, dealValue: e.target.value }))}
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="lastContactDate">Last Contact Date</Label>
          <Input
            id="lastContactDate"
            type="date"
            value={formData.lastContactDate}
            onChange={(e) => setFormData(prev => ({ ...prev, lastContactDate: e.target.value }))}
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="nextFollowUp">Next Follow-up</Label>
          <Input
            id="nextFollowUp"
            type="date"
            value={formData.nextFollowUp}
            onChange={(e) => setFormData(prev => ({ ...prev, nextFollowUp: e.target.value }))}
          />
        </div>
      </div>

      <div className="space-y-2">
        <Label htmlFor="tags">Tags</Label>
        <div className="flex flex-wrap gap-2 mb-2">
          {formData.tags.map(tag => (
            <Badge key={tag} variant="secondary" className="flex items-center gap-1">
              {tag}
              <X 
                className="w-3 h-3 cursor-pointer hover:text-red-600" 
                onClick={() => removeTag(tag)}
              />
            </Badge>
          ))}
        </div>
        <div className="flex gap-2">
          <Input
            placeholder="Add a tag"
            value={newTag}
            onChange={(e) => setNewTag(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && addTag()}
          />
          <Button type="button" variant="outline" onClick={addTag}>Add</Button>
        </div>
      </div>

      <div className="space-y-2">
        <Label htmlFor="notes">Notes</Label>
        <Textarea
          id="notes"
          placeholder="Additional notes about this contact..."
          value={formData.notes}
          onChange={(e) => setFormData(prev => ({ ...prev, notes: e.target.value }))}
          rows={3}
        />
      </div>
    </div>
  )

  if (!isOpen) return null

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center space-x-2">
            <UserCheck className="w-5 h-5 text-blue-600" />
            <span>Add New Contact</span>
          </DialogTitle>
          <DialogDescription>
            Create a comprehensive contact profile for your CRM pipeline
          </DialogDescription>
        </DialogHeader>

        {/* Progress Bar */}
        <div className="mb-6">
          <div className="flex justify-between text-sm text-gray-600 mb-2">
            <span>Step {currentStep} of {totalSteps}</span>
            <span>{Math.round(progressPercentage)}% Complete</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div 
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${progressPercentage}%` }}
            />
          </div>
        </div>

        <div className="min-h-[400px]">
          {currentStep === 1 && renderStep1()}
          {currentStep === 2 && renderStep2()}
          {currentStep === 3 && renderStep3()}
          {currentStep === 4 && renderStep4()}
        </div>

        <DialogFooter className="flex justify-between">
          <div>
            {currentStep > 1 && (
              <Button variant="outline" onClick={handlePrevious}>
                Previous
              </Button>
            )}
          </div>
          <div className="flex space-x-2">
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            {currentStep < totalSteps ? (
              <Button onClick={handleNext}>
                Next Step
              </Button>
            ) : (
              <Button onClick={handleSave} className="bg-blue-600 hover:bg-blue-700">
                Create Contact
              </Button>
            )}
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}