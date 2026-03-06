"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { useRouter } from "next/navigation"
import {
  Building2,
  User,
  Phone,
  Mail,
  MapPin,
  Briefcase,
  Target,
  Users,
  ArrowLeft,
  CheckCircle,
  Calendar,
  DollarSign,
  Star,
  Globe,
  Lightbulb,
  X
} from "lucide-react"

export function AddLeadContent() {
  const router = useRouter()
  const [currentStep, setCurrentStep] = useState(1)
  const [formData, setFormData] = useState({
    // Step 1: Basic Lead Information
    leadName: "",
    website: "",
    companyType: "",
    industry: "",
    subIndustry: "",
    leadSource: "",
    leadOwner: "",
    region: "",
    turnover: "",
    
    // Contact Information
    contactName: "",
    contactTitle: "",
    department: "",
    phone: "",
    mobile: "",
    email: "",
    whatsapp: "",
    linkedin: "",
    
    // Address
    street: "",
    city: "",
    state: "",
    pincode: "",
    country: "India",
    
    // Step 2: Business & Opportunity Details
    salesStage: "prospecting",
    leadType: "",
    leadSubType: "",
    priority: "medium",
    closingDate: "",
    estimatedValue: "",
    probability: "",
    
    // Products/Services Interest
    primaryProduct: "",
    secondaryProducts: [] as string[],
    budget: "",
    timeline: "",
    decisionMakers: "",
    
    // Step 3: Qualifying Information
    currentSolution: "",
    painPoints: "",
    competitorInfo: "",
    buyingProcess: "",
    nextSteps: "",
    
    // Additional Details
    buyerReference: "",
    otherReference: "",
    tags: [] as string[],
    notes: "",
    
    // Follow-up Information
    lastContactDate: "",
    nextFollowUpDate: "",
    preferredContactMethod: "email"
  })

  const [errors, setErrors] = useState<Record<string, string>>({})
  const [newTag, setNewTag] = useState("")
  const [newProduct, setNewProduct] = useState("")

  const leadSources = [
    "Website Inquiry",
    "Cold Call", 
    "Email Campaign",
    "Social Media",
    "Referral",
    "Trade Show",
    "Advertisement",
    "Partner",
    "Existing Customer",
    "LinkedIn",
    "Google Ads",
    "Content Marketing",
    "Webinar",
    "Direct Mail"
  ]

  const industries = [
    "Pharmaceuticals",
    "Biotechnology", 
    "Food & Beverage",
    "Healthcare",
    "Research & Development",
    "Education",
    "Manufacturing",
    "Chemical",
    "Environmental",
    "Quality Control",
    "Agriculture",
    "Cosmetics",
    "Government",
    "Non-Profit"
  ]

  const companyTypes = [
    "Private Limited",
    "Public Limited", 
    "Partnership",
    "Proprietorship",
    "LLP",
    "Government",
    "Research Institute",
    "University",
    "Non-Profit"
  ]

  const salesStages = [
    { value: "prospecting", label: "Prospecting", color: "bg-blue-100 text-blue-800" },
    { value: "qualified", label: "Qualified", color: "bg-purple-100 text-purple-800" },
    { value: "proposal", label: "Proposal", color: "bg-orange-100 text-orange-800" },
    { value: "negotiation", label: "Negotiation", color: "bg-yellow-100 text-yellow-800" },
    { value: "closed-won", label: "Closed Won", color: "bg-green-100 text-green-800" },
    { value: "closed-lost", label: "Closed Lost", color: "bg-red-100 text-red-800" }
  ]

  const priorities = [
    { value: "high", label: "High Priority", color: "bg-red-100 text-red-800" },
    { value: "medium", label: "Medium Priority", color: "bg-yellow-100 text-yellow-800" },
    { value: "low", label: "Low Priority", color: "bg-gray-100 text-gray-800" }
  ]

  const regions = [
    "North India",
    "South India", 
    "East India",
    "West India",
    "Central India",
    "International"
  ]

  const leadOwners = [
    "Hari Kumar K",
    "Vijay Muppala",
    "Prashanth Sandilya",
    "Pauline D'Souza",
    "Arvind K"
  ]

  const commonProducts = [
    "Analytical Balance",
    "Laboratory Freeze Dryer",
    "Micro-Volume Spectrophotometer", 
    "HPLC System",
    "GC-MS System",
    "pH Meter",
    "Centrifuge",
    "Incubator",
    "Autoclave",
    "Microscope",
    "Chromatography System",
    "Mass Spectrometer"
  ]

  const validateStep = (step: number) => {
    const newErrors: Record<string, string> = {}

    if (step === 1) {
      if (!formData.leadName.trim()) newErrors.leadName = "Lead name is required"
      if (!formData.leadSource.trim()) newErrors.leadSource = "Lead source is required"
      if (!formData.leadOwner.trim()) newErrors.leadOwner = "Lead owner is required"
      if (!formData.industry.trim()) newErrors.industry = "Industry is required"
      if (!formData.contactName.trim()) newErrors.contactName = "Contact name is required"
      if (!formData.phone.trim() && !formData.mobile.trim() && !formData.email.trim()) {
        newErrors.contact = "At least one contact method (phone, mobile, or email) is required"
      }
    }

    if (step === 2) {
      if (!formData.salesStage.trim()) newErrors.salesStage = "Sales stage is required"
      if (!formData.primaryProduct.trim()) newErrors.primaryProduct = "Primary product interest is required"
      if (!formData.estimatedValue.trim()) newErrors.estimatedValue = "Estimated deal value is required"
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
      console.log("Saving lead:", formData)
      router.push("/leads")
    }
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

  const addSecondaryProduct = () => {
    if (newProduct.trim() && !formData.secondaryProducts.includes(newProduct.trim())) {
      setFormData(prev => ({
        ...prev,
        secondaryProducts: [...prev.secondaryProducts, newProduct.trim()]
      }))
      setNewProduct("")
    }
  }

  const removeSecondaryProduct = (productToRemove: string) => {
    setFormData(prev => ({
      ...prev,
      secondaryProducts: prev.secondaryProducts.filter(product => product !== productToRemove)
    }))
  }

  const totalSteps = 4
  const progressPercentage = (currentStep / totalSteps) * 100

  const renderStep1 = () => (
    <div className="space-y-8">
      <div className="flex items-center space-x-3 mb-6">
        <Building2 className="w-6 h-6 text-blue-600" />
        <h2 className="text-2xl font-bold text-gray-900">Basic Lead Information</h2>
      </div>

      {/* Company Information */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Building2 className="w-5 h-5 text-blue-600" />
            <span>Company Details</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="leadName">Company/Lead Name *</Label>
              <Input
                id="leadName"
                placeholder="e.g., ABC Pharmaceuticals Ltd."
                value={formData.leadName}
                onChange={(e) => setFormData(prev => ({ ...prev, leadName: e.target.value }))}
                className={errors.leadName ? "border-red-500" : ""}
              />
              {errors.leadName && <p className="text-sm text-red-600">{errors.leadName}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="website">Company Website</Label>
              <Input
                id="website"
                placeholder="https://www.example.com"
                value={formData.website}
                onChange={(e) => setFormData(prev => ({ ...prev, website: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="companyType">Company Type</Label>
              <Select value={formData.companyType} onValueChange={(value) => setFormData(prev => ({ ...prev, companyType: value }))}>
                <SelectTrigger>
                  <SelectValue placeholder="Select company type" />
                </SelectTrigger>
                <SelectContent>
                  {companyTypes.map(type => (
                    <SelectItem key={type} value={type}>{type}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="industry">Industry *</Label>
              <Select value={formData.industry} onValueChange={(value) => setFormData(prev => ({ ...prev, industry: value }))}>
                <SelectTrigger className={errors.industry ? "border-red-500" : ""}>
                  <SelectValue placeholder="Select industry" />
                </SelectTrigger>
                <SelectContent>
                  {industries.map(industry => (
                    <SelectItem key={industry} value={industry}>{industry}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {errors.industry && <p className="text-sm text-red-600">{errors.industry}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="turnover">Annual Turnover</Label>
              <Select value={formData.turnover} onValueChange={(value) => setFormData(prev => ({ ...prev, turnover: value }))}>
                <SelectTrigger>
                  <SelectValue placeholder="Select turnover range" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="0-1cr">₹0 - 1 Crore</SelectItem>
                  <SelectItem value="1-5cr">₹1 - 5 Crores</SelectItem>
                  <SelectItem value="5-10cr">₹5 - 10 Crores</SelectItem>
                  <SelectItem value="10-50cr">₹10 - 50 Crores</SelectItem>
                  <SelectItem value="50cr+">₹50+ Crores</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="region">Region</Label>
              <Select value={formData.region} onValueChange={(value) => setFormData(prev => ({ ...prev, region: value }))}>
                <SelectTrigger>
                  <SelectValue placeholder="Select region" />
                </SelectTrigger>
                <SelectContent>
                  {regions.map(region => (
                    <SelectItem key={region} value={region}>{region}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Lead Source & Ownership */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Target className="w-5 h-5 text-green-600" />
            <span>Lead Source & Ownership</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="leadSource">Lead Source *</Label>
              <Select value={formData.leadSource} onValueChange={(value) => setFormData(prev => ({ ...prev, leadSource: value }))}>
                <SelectTrigger className={errors.leadSource ? "border-red-500" : ""}>
                  <SelectValue placeholder="How did you find this lead?" />
                </SelectTrigger>
                <SelectContent>
                  {leadSources.map(source => (
                    <SelectItem key={source} value={source}>{source}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {errors.leadSource && <p className="text-sm text-red-600">{errors.leadSource}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="leadOwner">Lead Owner *</Label>
              <Select value={formData.leadOwner} onValueChange={(value) => setFormData(prev => ({ ...prev, leadOwner: value }))}>
                <SelectTrigger className={errors.leadOwner ? "border-red-500" : ""}>
                  <SelectValue placeholder="Assign lead to..." />
                </SelectTrigger>
                <SelectContent>
                  {leadOwners.map(owner => (
                    <SelectItem key={owner} value={owner}>{owner}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {errors.leadOwner && <p className="text-sm text-red-600">{errors.leadOwner}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="buyerReference">Buyer's Reference</Label>
              <Input
                id="buyerReference"
                placeholder="Reference number or code"
                value={formData.buyerReference}
                onChange={(e) => setFormData(prev => ({ ...prev, buyerReference: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="otherReference">Other Reference</Label>
              <Input
                id="otherReference"
                placeholder="Additional reference information"
                value={formData.otherReference}
                onChange={(e) => setFormData(prev => ({ ...prev, otherReference: e.target.value }))}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Contact Information */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <User className="w-5 h-5 text-purple-600" />
            <span>Primary Contact Information</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="contactName">Contact Name *</Label>
              <Input
                id="contactName"
                placeholder="e.g., Dr. Priya Sharma"
                value={formData.contactName}
                onChange={(e) => setFormData(prev => ({ ...prev, contactName: e.target.value }))}
                className={errors.contactName ? "border-red-500" : ""}
              />
              {errors.contactName && <p className="text-sm text-red-600">{errors.contactName}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="contactTitle">Contact Title/Position</Label>
              <Input
                id="contactTitle"
                placeholder="e.g., Research Director, Lab Manager"
                value={formData.contactTitle}
                onChange={(e) => setFormData(prev => ({ ...prev, contactTitle: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="department">Department</Label>
              <Input
                id="department"
                placeholder="e.g., Research & Development, QC"
                value={formData.department}
                onChange={(e) => setFormData(prev => ({ ...prev, department: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="email">Email Address</Label>
              <Input
                id="email"
                type="email"
                placeholder="contact@example.com"
                value={formData.email}
                onChange={(e) => setFormData(prev => ({ ...prev, email: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="phone">Phone Number</Label>
              <Input
                id="phone"
                placeholder="+91 80 1234 5678"
                value={formData.phone}
                onChange={(e) => setFormData(prev => ({ ...prev, phone: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="mobile">Mobile Number</Label>
              <Input
                id="mobile"
                placeholder="+91 98765 43210"
                value={formData.mobile}
                onChange={(e) => setFormData(prev => ({ ...prev, mobile: e.target.value }))}
              />
            </div>

            {errors.contact && (
              <div className="lg:col-span-2">
                <p className="text-sm text-red-600">{errors.contact}</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Address Information */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <MapPin className="w-5 h-5 text-orange-600" />
            <span>Address Information</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="space-y-2 lg:col-span-2">
              <Label htmlFor="street">Street Address</Label>
              <Textarea
                id="street"
                placeholder="Building name, street address"
                value={formData.street}
                onChange={(e) => setFormData(prev => ({ ...prev, street: e.target.value }))}
                rows={2}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="city">City</Label>
              <Input
                id="city"
                placeholder="e.g., Bangalore"
                value={formData.city}
                onChange={(e) => setFormData(prev => ({ ...prev, city: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="state">State</Label>
              <Input
                id="state"
                placeholder="e.g., Karnataka"
                value={formData.state}
                onChange={(e) => setFormData(prev => ({ ...prev, state: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="pincode">Pin Code</Label>
              <Input
                id="pincode"
                placeholder="560001"
                value={formData.pincode}
                onChange={(e) => setFormData(prev => ({ ...prev, pincode: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="country">Country</Label>
              <Select value={formData.country} onValueChange={(value) => setFormData(prev => ({ ...prev, country: value }))}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="India">India</SelectItem>
                  <SelectItem value="USA">USA</SelectItem>
                  <SelectItem value="UK">UK</SelectItem>
                  <SelectItem value="Canada">Canada</SelectItem>
                  <SelectItem value="Australia">Australia</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )

  const renderStep2 = () => (
    <div className="space-y-8">
      <div className="flex items-center space-x-3 mb-6">
        <Target className="w-6 h-6 text-green-600" />
        <h2 className="text-2xl font-bold text-gray-900">Business & Opportunity Details</h2>
      </div>

      {/* Sales Information */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Briefcase className="w-5 h-5 text-blue-600" />
            <span>Sales Pipeline Information</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label htmlFor="salesStage">Sales Stage *</Label>
              <Select value={formData.salesStage} onValueChange={(value) => setFormData(prev => ({ ...prev, salesStage: value }))}>
                <SelectTrigger className={errors.salesStage ? "border-red-500" : ""}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {salesStages.map(stage => (
                    <SelectItem key={stage.value} value={stage.value}>
                      <div className="flex items-center space-x-2">
                        <Badge className={stage.color}>{stage.label}</Badge>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {errors.salesStage && <p className="text-sm text-red-600">{errors.salesStage}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="priority">Priority Level</Label>
              <Select value={formData.priority} onValueChange={(value) => setFormData(prev => ({ ...prev, priority: value }))}>
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
              <Label htmlFor="probability">Win Probability (%)</Label>
              <Input
                id="probability"
                type="number"
                min="0"
                max="100"
                placeholder="75"
                value={formData.probability}
                onChange={(e) => setFormData(prev => ({ ...prev, probability: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="closingDate">Expected Closing Date</Label>
              <Input
                id="closingDate"
                type="date"
                value={formData.closingDate}
                onChange={(e) => setFormData(prev => ({ ...prev, closingDate: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="estimatedValue">Estimated Deal Value *</Label>
              <Input
                id="estimatedValue"
                placeholder="₹5,00,000"
                value={formData.estimatedValue}
                onChange={(e) => setFormData(prev => ({ ...prev, estimatedValue: e.target.value }))}
                className={errors.estimatedValue ? "border-red-500" : ""}
              />
              {errors.estimatedValue && <p className="text-sm text-red-600">{errors.estimatedValue}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="budget">Approved Budget Range</Label>
              <Select value={formData.budget} onValueChange={(value) => setFormData(prev => ({ ...prev, budget: value }))}>
                <SelectTrigger>
                  <SelectValue placeholder="Select budget range" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1-5l">₹1 - 5 Lakhs</SelectItem>
                  <SelectItem value="5-10l">₹5 - 10 Lakhs</SelectItem>
                  <SelectItem value="10-25l">₹10 - 25 Lakhs</SelectItem>
                  <SelectItem value="25-50l">₹25 - 50 Lakhs</SelectItem>
                  <SelectItem value="50l+">₹50+ Lakhs</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Product Interest */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Star className="w-5 h-5 text-yellow-600" />
            <span>Product/Service Interest</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="primaryProduct">Primary Product Interest *</Label>
              <Select value={formData.primaryProduct} onValueChange={(value) => setFormData(prev => ({ ...prev, primaryProduct: value }))}>
                <SelectTrigger className={errors.primaryProduct ? "border-red-500" : ""}>
                  <SelectValue placeholder="Select primary product" />
                </SelectTrigger>
                <SelectContent>
                  {commonProducts.map(product => (
                    <SelectItem key={product} value={product}>{product}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {errors.primaryProduct && <p className="text-sm text-red-600">{errors.primaryProduct}</p>}
            </div>

            <div className="space-y-2">
              <Label htmlFor="timeline">Purchase Timeline</Label>
              <Select value={formData.timeline} onValueChange={(value) => setFormData(prev => ({ ...prev, timeline: value }))}>
                <SelectTrigger>
                  <SelectValue placeholder="When do they plan to buy?" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="immediate">Immediate (Within 1 month)</SelectItem>
                  <SelectItem value="short-term">Short Term (1-3 months)</SelectItem>
                  <SelectItem value="medium-term">Medium Term (3-6 months)</SelectItem>
                  <SelectItem value="long-term">Long Term (6+ months)</SelectItem>
                  <SelectItem value="unknown">Timeline Unknown</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Secondary Products */}
          <div className="space-y-2">
            <Label>Additional Products of Interest</Label>
            <div className="flex flex-wrap gap-2 mb-2">
              {formData.secondaryProducts.map(product => (
                <Badge key={product} variant="secondary" className="flex items-center gap-1">
                  {product}
                  <X 
                    className="w-3 h-3 cursor-pointer hover:text-red-600" 
                    onClick={() => removeSecondaryProduct(product)}
                  />
                </Badge>
              ))}
            </div>
            <div className="flex gap-2">
              <Select value={newProduct} onValueChange={setNewProduct}>
                <SelectTrigger className="flex-1">
                  <SelectValue placeholder="Select additional product" />
                </SelectTrigger>
                <SelectContent>
                  {commonProducts.filter(p => p !== formData.primaryProduct && !formData.secondaryProducts.includes(p)).map(product => (
                    <SelectItem key={product} value={product}>{product}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button type="button" variant="outline" onClick={addSecondaryProduct}>Add</Button>
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="decisionMakers">Key Decision Makers</Label>
            <Textarea
              id="decisionMakers"
              placeholder="List the key decision makers involved in this purchase..."
              value={formData.decisionMakers}
              onChange={(e) => setFormData(prev => ({ ...prev, decisionMakers: e.target.value }))}
              rows={3}
            />
          </div>
        </CardContent>
      </Card>
    </div>
  )

  const renderStep3 = () => (
    <div className="space-y-8">
      <div className="flex items-center space-x-3 mb-6">
        <Lightbulb className="w-6 h-6 text-orange-600" />
        <h2 className="text-2xl font-bold text-gray-900">Qualifying Information</h2>
      </div>

      {/* Current Situation */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Target className="w-5 h-5 text-blue-600" />
            <span>Current Situation & Needs</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="currentSolution">Current Solution/Equipment</Label>
            <Textarea
              id="currentSolution"
              placeholder="What are they currently using? What equipment/solutions do they have?"
              value={formData.currentSolution}
              onChange={(e) => setFormData(prev => ({ ...prev, currentSolution: e.target.value }))}
              rows={3}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="painPoints">Pain Points & Challenges</Label>
            <Textarea
              id="painPoints"
              placeholder="What problems are they trying to solve? What are their main challenges?"
              value={formData.painPoints}
              onChange={(e) => setFormData(prev => ({ ...prev, painPoints: e.target.value }))}
              rows={3}
            />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="competitorInfo">Competitor Information</Label>
              <Textarea
                id="competitorInfo"
                placeholder="Are they considering other suppliers? Who are the competitors?"
                value={formData.competitorInfo}
                onChange={(e) => setFormData(prev => ({ ...prev, competitorInfo: e.target.value }))}
                rows={3}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="buyingProcess">Buying Process</Label>
              <Textarea
                id="buyingProcess"
                placeholder="What is their typical buying process? Who approves purchases?"
                value={formData.buyingProcess}
                onChange={(e) => setFormData(prev => ({ ...prev, buyingProcess: e.target.value }))}
                rows={3}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Next Steps & Follow-up */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Calendar className="w-5 h-5 text-green-600" />
            <span>Follow-up & Next Steps</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="nextSteps">Agreed Next Steps</Label>
            <Textarea
              id="nextSteps"
              placeholder="What are the next steps? What was agreed upon?"
              value={formData.nextSteps}
              onChange={(e) => setFormData(prev => ({ ...prev, nextSteps: e.target.value }))}
              rows={3}
            />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
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
              <Label htmlFor="nextFollowUpDate">Next Follow-up Date</Label>
              <Input
                id="nextFollowUpDate"
                type="date"
                value={formData.nextFollowUpDate}
                onChange={(e) => setFormData(prev => ({ ...prev, nextFollowUpDate: e.target.value }))}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="preferredContactMethod">Preferred Contact Method</Label>
              <Select value={formData.preferredContactMethod} onValueChange={(value) => setFormData(prev => ({ ...prev, preferredContactMethod: value }))}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="email">Email</SelectItem>
                  <SelectItem value="phone">Phone Call</SelectItem>
                  <SelectItem value="whatsapp">WhatsApp</SelectItem>
                  <SelectItem value="linkedin">LinkedIn</SelectItem>
                  <SelectItem value="in-person">In-Person Meeting</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Additional Information */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Star className="w-5 h-5 text-purple-600" />
            <span>Additional Information</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Tags */}
          <div className="space-y-2">
            <Label>Tags</Label>
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
                placeholder="Add a tag (e.g., Hot Lead, Demo Required)"
                value={newTag}
                onChange={(e) => setNewTag(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && addTag()}
              />
              <Button type="button" variant="outline" onClick={addTag}>Add</Button>
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="notes">Additional Notes</Label>
            <Textarea
              id="notes"
              placeholder="Any additional information, observations, or notes about this lead..."
              value={formData.notes}
              onChange={(e) => setFormData(prev => ({ ...prev, notes: e.target.value }))}
              rows={4}
            />
          </div>
        </CardContent>
      </Card>
    </div>
  )

  const renderStep4 = () => (
    <div className="space-y-8">
      <div className="flex items-center space-x-3 mb-6">
        <CheckCircle className="w-6 h-6 text-green-600" />
        <h2 className="text-2xl font-bold text-gray-900">Review & Confirmation</h2>
      </div>

      {/* Lead Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Building2 className="w-5 h-5 text-blue-600" />
            <span>Lead Summary</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-3">
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Company:</span>
                <span className="text-gray-900">{formData.leadName || "Not specified"}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Industry:</span>
                <span className="text-gray-900">{formData.industry || "Not specified"}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Contact:</span>
                <span className="text-gray-900">{formData.contactName || "Not specified"}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Lead Source:</span>
                <span className="text-gray-900">{formData.leadSource || "Not specified"}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Lead Owner:</span>
                <span className="text-gray-900">{formData.leadOwner || "Not specified"}</span>
              </div>
            </div>
            
            <div className="space-y-3">
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Sales Stage:</span>
                <Badge className={salesStages.find(s => s.value === formData.salesStage)?.color}>
                  {salesStages.find(s => s.value === formData.salesStage)?.label}
                </Badge>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Priority:</span>
                <Badge className={priorities.find(p => p.value === formData.priority)?.color}>
                  {priorities.find(p => p.value === formData.priority)?.label}
                </Badge>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Estimated Value:</span>
                <span className="text-gray-900">{formData.estimatedValue || "Not specified"}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Primary Product:</span>
                <span className="text-gray-900">{formData.primaryProduct || "Not specified"}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-gray-600">Expected Closing:</span>
                <span className="text-gray-900">{formData.closingDate || "Not specified"}</span>
              </div>
            </div>
          </div>

          {/* Tags */}
          {formData.tags.length > 0 && (
            <div className="pt-4 border-t">
              <span className="font-medium text-gray-600 block mb-2">Tags:</span>
              <div className="flex flex-wrap gap-2">
                {formData.tags.map(tag => (
                  <Badge key={tag} variant="secondary">{tag}</Badge>
                ))}
              </div>
            </div>
          )}

          {/* Additional Products */}
          {formData.secondaryProducts.length > 0 && (
            <div className="pt-4 border-t">
              <span className="font-medium text-gray-600 block mb-2">Additional Products:</span>
              <div className="flex flex-wrap gap-2">
                {formData.secondaryProducts.map(product => (
                  <Badge key={product} variant="outline">{product}</Badge>
                ))}
              </div>
            </div>
          )}

          {/* Notes Preview */}
          {formData.notes && (
            <div className="pt-4 border-t">
              <span className="font-medium text-gray-600 block mb-2">Notes:</span>
              <p className="text-sm text-gray-700 bg-gray-50 p-3 rounded-md">
                {formData.notes}
              </p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Confirmation Note */}
      <Card className="border-green-200 bg-green-50">
        <CardContent className="p-6">
          <div className="flex items-start space-x-3">
            <CheckCircle className="w-6 h-6 text-green-600 flex-shrink-0 mt-1" />
            <div>
              <h3 className="font-semibold text-green-800 mb-2">Ready to Create Lead</h3>
              <p className="text-sm text-green-700 mb-3">
                Your lead information looks complete. Once you create this lead, it will be added to your CRM pipeline
                and assigned to {formData.leadOwner}. You can always edit the information later.
              </p>
              <div className="text-xs text-green-600">
                • Lead will be created with status: {salesStages.find(s => s.value === formData.salesStage)?.label}
                <br />
                • Automatic follow-up reminders will be set based on the timeline
                <br />
                • {formData.leadOwner} will be notified about the new lead assignment
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => router.push("/leads")}
              className="text-gray-600 hover:text-gray-900"
            >
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back to Leads
            </Button>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Add New Lead</h1>
              <p className="text-sm text-gray-600 mt-1">Create a comprehensive lead profile for your sales pipeline</p>
            </div>
          </div>
          <div className="text-right">
            <div className="text-sm text-gray-500 mb-1">Step {currentStep} of {totalSteps}</div>
            <div className="text-lg font-semibold text-gray-900">{Math.round(progressPercentage)}% Complete</div>
          </div>
        </div>
        
        {/* Progress Bar */}
        <div className="mt-4">
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div 
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${progressPercentage}%` }}
            />
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-6xl mx-auto px-6 py-8">
        <div className="min-h-[600px] mb-8">
          {currentStep === 1 && renderStep1()}
          {currentStep === 2 && renderStep2()}
          {currentStep === 3 && renderStep3()}
          {currentStep === 4 && renderStep4()}
        </div>

        {/* Navigation */}
        <div className="flex justify-between items-center pt-6 border-t border-gray-200">
          <div>
            {currentStep > 1 && (
              <Button variant="outline" onClick={handlePrevious}>
                <ArrowLeft className="w-4 h-4 mr-2" />
                Previous Step
              </Button>
            )}
          </div>
          <div className="flex space-x-3">
            <Button variant="outline" onClick={() => router.push("/leads")}>
              Cancel
            </Button>
            {currentStep < totalSteps ? (
              <Button onClick={handleNext} className="bg-blue-600 hover:bg-blue-700">
                Next Step
                <ArrowLeft className="w-4 h-4 ml-2 rotate-180" />
              </Button>
            ) : (
              <Button onClick={handleSave} className="bg-green-600 hover:bg-green-700">
                <CheckCircle className="w-4 h-4 mr-2" />
                Create Lead
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}