"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
  Building2, 
  MapPin, 
  Users, 
  Phone, 
  Mail, 
  Globe, 
  IndianRupee, 
  FileText, 
  ArrowLeft, 
  Save, 
  Plus,
  X,
  Star,
  TrendingUp,
  Target,
  Brain,
  CheckCircle2,
  AlertCircle,
  Linkedin,
  Twitter,
  Facebook,
  Instagram
} from "lucide-react"

interface Contact {
  id: string
  title: string
  firstName: string
  lastName: string
  email: string
  phone: string
  mobile: string
  designation: string
  department: string
  linkedin: string
  isPrimary: boolean
}

interface Address {
  id: string
  type: string
  addressLine1: string
  addressLine2: string
  city: string
  state: string
  country: string
  pincode: string
  phone: string
  email: string
}

export function AddAccountContent() {
  const router = useRouter()
  const [currentStep, setCurrentStep] = useState(1)
  const totalSteps = 4

  // Form state
  const [formData, setFormData] = useState({
    // Basic Information
    accountName: "",
    displayName: "",
    accountCode: "",
    website: "",
    industry: "",
    subIndustry: "",
    companyType: "",
    parentAccount: "",
    
    // Business Details
    annualRevenue: "",
    employeeCount: "",
    businessModel: "",
    territory: "",
    region: "",
    accountSource: "",
    leadSource: "",
    
    // Relationship Information
    accountOwner: "",
    accountStatus: "prospect",
    priority: "medium",
    rating: "",
    accountScore: 0,
    
    // Financial Information
    creditLimit: "",
    paymentTerms: "",
    currency: "INR",
    taxId: "",
    gstNumber: "",
    panNumber: "",
    
    // Social & Additional
    description: "",
    tags: "",
    competitors: "",
    socialMedia: {
      linkedin: "",
      twitter: "",
      facebook: "",
      instagram: ""
    }
  })

  const [contacts, setContacts] = useState<Contact[]>([
    {
      id: "1",
      title: "Mr.",
      firstName: "",
      lastName: "",
      email: "",
      phone: "",
      mobile: "",
      designation: "",
      department: "",
      linkedin: "",
      isPrimary: true
    }
  ])

  const [addresses, setAddresses] = useState<Address[]>([
    {
      id: "1",
      type: "Billing",
      addressLine1: "",
      addressLine2: "",
      city: "",
      state: "",
      country: "India",
      pincode: "",
      phone: "",
      email: ""
    }
  ])

  const progressPercentage = (currentStep / totalSteps) * 100

  const addContact = () => {
    const newContact: Contact = {
      id: Date.now().toString(),
      title: "Mr.",
      firstName: "",
      lastName: "",
      email: "",
      phone: "",
      mobile: "",
      designation: "",
      department: "",
      linkedin: "",
      isPrimary: false
    }
    setContacts([...contacts, newContact])
  }

  const removeContact = (id: string) => {
    if (contacts.length > 1) {
      setContacts(contacts.filter(contact => contact.id !== id))
    }
  }

  const addAddress = () => {
    const newAddress: Address = {
      id: Date.now().toString(),
      type: "Shipping",
      addressLine1: "",
      addressLine2: "",
      city: "",
      state: "",
      country: "India",
      pincode: "",
      phone: "",
      email: ""
    }
    setAddresses([...addresses, newAddress])
  }

  const removeAddress = (id: string) => {
    if (addresses.length > 1) {
      setAddresses(addresses.filter(address => address.id !== id))
    }
  }

  const handleSave = () => {
    // Handle form submission
    console.log("Account saved:", { formData, contacts, addresses })
    router.push("/accounts")
  }

  const handleSaveAndNew = () => {
    // Handle save and create new
    console.log("Account saved, creating new:", { formData, contacts, addresses })
    // Reset form or redirect
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Button variant="ghost" onClick={() => router.push("/accounts")}>
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back to Accounts
            </Button>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Create New Account</h1>
              <p className="text-gray-600">Add a new customer account to your CRM pipeline</p>
            </div>
          </div>
          <div className="flex items-center space-x-3">
            <Badge variant="outline" className="text-sm">
              Step {currentStep} of {totalSteps}
            </Badge>
            <Button variant="outline" onClick={handleSave}>
              <Save className="w-4 h-4 mr-2" />
              Save Draft
            </Button>
          </div>
        </div>
        
        {/* Progress Bar */}
        <div className="mt-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm text-gray-600">Form Progress</span>
            <span className="text-sm text-gray-600">{Math.round(progressPercentage)}% Complete</span>
          </div>
          <Progress value={progressPercentage} className="h-2" />
        </div>
      </div>

      <div className="max-w-7xl mx-auto p-6">
        <Tabs value={`step-${currentStep}`} className="space-y-6">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="step-1" onClick={() => setCurrentStep(1)}>
              <Building2 className="w-4 h-4 mr-2" />
              Basic Info
            </TabsTrigger>
            <TabsTrigger value="step-2" onClick={() => setCurrentStep(2)}>
              <TrendingUp className="w-4 h-4 mr-2" />
              Business Details
            </TabsTrigger>
            <TabsTrigger value="step-3" onClick={() => setCurrentStep(3)}>
              <Users className="w-4 h-4 mr-2" />
              Contacts & Addresses
            </TabsTrigger>
            <TabsTrigger value="step-4" onClick={() => setCurrentStep(4)}>
              <CheckCircle2 className="w-4 h-4 mr-2" />
              Review & Submit
            </TabsTrigger>
          </TabsList>

          {/* Step 1: Basic Information */}
          <TabsContent value="step-1" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Building2 className="w-5 h-5 mr-2" />
                  Company Information
                </CardTitle>
                <CardDescription>
                  Enter the basic company details and identification information
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <Label htmlFor="accountName">Account Name *</Label>
                    <Input 
                      id="accountName" 
                      value={formData.accountName}
                      onChange={(e) => setFormData({...formData, accountName: e.target.value})}
                      placeholder="Enter company name"
                      required
                    />
                  </div>
                  <div>
                    <Label htmlFor="displayName">Display Name</Label>
                    <Input 
                      id="displayName" 
                      value={formData.displayName}
                      onChange={(e) => setFormData({...formData, displayName: e.target.value})}
                      placeholder="How should this appear in lists?"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                  <div>
                    <Label htmlFor="accountCode">Account Code</Label>
                    <Input 
                      id="accountCode" 
                      value={formData.accountCode}
                      onChange={(e) => setFormData({...formData, accountCode: e.target.value})}
                      placeholder="AUTO-GEN-001"
                    />
                  </div>
                  <div>
                    <Label htmlFor="website">Website</Label>
                    <Input 
                      id="website" 
                      type="url"
                      value={formData.website}
                      onChange={(e) => setFormData({...formData, website: e.target.value})}
                      placeholder="https://company.com"
                    />
                  </div>
                  <div>
                    <Label htmlFor="companyType">Company Type</Label>
                    <Select value={formData.companyType} onValueChange={(value) => setFormData({...formData, companyType: value})}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select type" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="private-limited">Private Limited</SelectItem>
                        <SelectItem value="public-limited">Public Limited</SelectItem>
                        <SelectItem value="partnership">Partnership</SelectItem>
                        <SelectItem value="proprietorship">Proprietorship</SelectItem>
                        <SelectItem value="llp">LLP</SelectItem>
                        <SelectItem value="government">Government</SelectItem>
                        <SelectItem value="ngo">NGO</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <Label htmlFor="industry">Industry *</Label>
                    <Select value={formData.industry} onValueChange={(value) => setFormData({...formData, industry: value})}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select industry" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="biotechnology">Biotechnology</SelectItem>
                        <SelectItem value="pharmaceuticals">Pharmaceuticals</SelectItem>
                        <SelectItem value="healthcare">Healthcare</SelectItem>
                        <SelectItem value="medical-devices">Medical Devices</SelectItem>
                        <SelectItem value="diagnostics">Diagnostics</SelectItem>
                        <SelectItem value="research">Research & Development</SelectItem>
                        <SelectItem value="manufacturing">Manufacturing</SelectItem>
                        <SelectItem value="education">Education</SelectItem>
                        <SelectItem value="government">Government</SelectItem>
                        <SelectItem value="food-beverage">Food & Beverage</SelectItem>
                        <SelectItem value="agriculture">Agriculture</SelectItem>
                        <SelectItem value="environmental">Environmental</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="subIndustry">Sub-Industry</Label>
                    <Select value={formData.subIndustry} onValueChange={(value) => setFormData({...formData, subIndustry: value})}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select sub-industry" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="clinical-diagnostics">Clinical Diagnostics</SelectItem>
                        <SelectItem value="molecular-biology">Molecular Biology</SelectItem>
                        <SelectItem value="analytical-chemistry">Analytical Chemistry</SelectItem>
                        <SelectItem value="microbiology">Microbiology</SelectItem>
                        <SelectItem value="pathology">Pathology</SelectItem>
                        <SelectItem value="genetics">Genetics</SelectItem>
                        <SelectItem value="quality-control">Quality Control</SelectItem>
                        <SelectItem value="research-development">R&D Labs</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* AI-Powered Suggestions */}
                <Card className="bg-purple-50 border-purple-200">
                  <CardHeader>
                    <CardTitle className="text-sm flex items-center">
                      <Brain className="w-4 h-4 mr-2 text-purple-600" />
                      🤖 AI Suggestions
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-sm text-purple-800">
                      Based on "{formData.accountName}", I suggest:
                    </p>
                    <ul className="text-xs text-purple-700 mt-2 space-y-1">
                      <li>• Industry: {formData.accountName.toLowerCase().includes('lab') ? 'Biotechnology' : 'Healthcare'}</li>
                      <li>• Display Name: {formData.accountName}</li>
                      <li>• Expected Annual Revenue: ₹50L - ₹5Cr</li>
                    </ul>
                  </CardContent>
                </Card>
              </CardContent>
            </Card>

            <div className="flex justify-end">
              <Button onClick={() => setCurrentStep(2)}>
                Next: Business Details
                <ArrowLeft className="w-4 h-4 ml-2 rotate-180" />
              </Button>
            </div>
          </TabsContent>

          {/* Step 2: Business Details */}
          <TabsContent value="step-2" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Business Metrics */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center">
                    <TrendingUp className="w-5 h-5 mr-2" />
                    Business Metrics
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="annualRevenue">Annual Revenue</Label>
                      <Select value={formData.annualRevenue} onValueChange={(value) => setFormData({...formData, annualRevenue: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select range" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="0-25L">₹0 - ₹25 Lakhs</SelectItem>
                          <SelectItem value="25L-1Cr">₹25 Lakhs - ₹1 Crore</SelectItem>
                          <SelectItem value="1-5Cr">₹1 - ₹5 Crores</SelectItem>
                          <SelectItem value="5-10Cr">₹5 - ₹10 Crores</SelectItem>
                          <SelectItem value="10-50Cr">₹10 - ₹50 Crores</SelectItem>
                          <SelectItem value="50Cr+">₹50+ Crores</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="employeeCount">Employee Count</Label>
                      <Select value={formData.employeeCount} onValueChange={(value) => setFormData({...formData, employeeCount: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select range" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="1-10">1-10</SelectItem>
                          <SelectItem value="11-50">11-50</SelectItem>
                          <SelectItem value="51-200">51-200</SelectItem>
                          <SelectItem value="201-500">201-500</SelectItem>
                          <SelectItem value="501-1000">501-1000</SelectItem>
                          <SelectItem value="1000+">1000+</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="territory">Territory</Label>
                      <Select value={formData.territory} onValueChange={(value) => setFormData({...formData, territory: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select territory" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="north">North India</SelectItem>
                          <SelectItem value="south">South India</SelectItem>
                          <SelectItem value="east">East India</SelectItem>
                          <SelectItem value="west">West India</SelectItem>
                          <SelectItem value="central">Central India</SelectItem>
                          <SelectItem value="international">International</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="businessModel">Business Model</Label>
                      <Select value={formData.businessModel} onValueChange={(value) => setFormData({...formData, businessModel: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select model" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="b2b">B2B</SelectItem>
                          <SelectItem value="b2c">B2C</SelectItem>
                          <SelectItem value="b2g">B2G (Government)</SelectItem>
                          <SelectItem value="hybrid">Hybrid</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* CRM Information */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center">
                    <Target className="w-5 h-5 mr-2" />
                    CRM Information
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="accountStatus">Account Status</Label>
                      <Select value={formData.accountStatus} onValueChange={(value) => setFormData({...formData, accountStatus: value})}>
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="prospect">Prospect</SelectItem>
                          <SelectItem value="qualified">Qualified Lead</SelectItem>
                          <SelectItem value="customer">Customer</SelectItem>
                          <SelectItem value="partner">Partner</SelectItem>
                          <SelectItem value="inactive">Inactive</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="priority">Priority Level</Label>
                      <Select value={formData.priority} onValueChange={(value) => setFormData({...formData, priority: value})}>
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="high">High</SelectItem>
                          <SelectItem value="medium">Medium</SelectItem>
                          <SelectItem value="low">Low</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="accountSource">Account Source</Label>
                      <Select value={formData.accountSource} onValueChange={(value) => setFormData({...formData, accountSource: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="How did we find them?" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="website">Website Inquiry</SelectItem>
                          <SelectItem value="referral">Referral</SelectItem>
                          <SelectItem value="cold-outreach">Cold Outreach</SelectItem>
                          <SelectItem value="trade-show">Trade Show</SelectItem>
                          <SelectItem value="social-media">Social Media</SelectItem>
                          <SelectItem value="partner">Partner</SelectItem>
                          <SelectItem value="existing-customer">Existing Customer</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="accountOwner">Account Owner</Label>
                      <Select value={formData.accountOwner} onValueChange={(value) => setFormData({...formData, accountOwner: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Assign to..." />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="hari-kumar">Hari Kumar K</SelectItem>
                          <SelectItem value="arvind-k">Arvind K</SelectItem>
                          <SelectItem value="pauline">Pauline</SelectItem>
                          <SelectItem value="prashanth">Prashanth S</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  {/* Account Score */}
                  <div>
                    <Label>Account Score (AI-Generated)</Label>
                    <div className="flex items-center space-x-4 mt-2">
                      <div className="flex space-x-1">
                        {[1, 2, 3, 4, 5].map((star) => (
                          <Star 
                            key={star} 
                            className={`w-5 h-5 ${star <= 3 ? 'text-yellow-400 fill-current' : 'text-gray-300'}`} 
                          />
                        ))}
                      </div>
                      <Badge variant="outline">Score: 3/5</Badge>
                      <span className="text-xs text-gray-600">Based on company size, industry, and revenue</span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Financial Information */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <IndianRupee className="w-5 h-5 mr-2" />
                  Financial Information
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  <div>
                    <Label htmlFor="creditLimit">Credit Limit (₹)</Label>
                    <Input 
                      id="creditLimit" 
                      type="number"
                      value={formData.creditLimit}
                      onChange={(e) => setFormData({...formData, creditLimit: e.target.value})}
                      placeholder="500000"
                    />
                  </div>
                  <div>
                    <Label htmlFor="paymentTerms">Payment Terms</Label>
                    <Select value={formData.paymentTerms} onValueChange={(value) => setFormData({...formData, paymentTerms: value})}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select terms" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="net-30">Net 30</SelectItem>
                        <SelectItem value="net-15">Net 15</SelectItem>
                        <SelectItem value="advance">Advance Payment</SelectItem>
                        <SelectItem value="cod">Cash on Delivery</SelectItem>
                        <SelectItem value="custom">Custom Terms</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="gstNumber">GST Number</Label>
                    <Input 
                      id="gstNumber" 
                      value={formData.gstNumber}
                      onChange={(e) => setFormData({...formData, gstNumber: e.target.value})}
                      placeholder="22AAAAA0000A1Z5"
                    />
                  </div>
                  <div>
                    <Label htmlFor="panNumber">PAN Number</Label>
                    <Input 
                      id="panNumber" 
                      value={formData.panNumber}
                      onChange={(e) => setFormData({...formData, panNumber: e.target.value})}
                      placeholder="AAAAA0000A"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setCurrentStep(1)}>
                <ArrowLeft className="w-4 h-4 mr-2" />
                Previous
              </Button>
              <Button onClick={() => setCurrentStep(3)}>
                Next: Contacts & Addresses
                <ArrowLeft className="w-4 h-4 ml-2 rotate-180" />
              </Button>
            </div>
          </TabsContent>

          {/* Step 3: Contacts & Addresses */}
          <TabsContent value="step-3" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Contacts */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <div className="flex items-center">
                      <Users className="w-5 h-5 mr-2" />
                      Contact Persons
                    </div>
                    <Button onClick={addContact} size="sm">
                      <Plus className="w-4 h-4 mr-2" />
                      Add Contact
                    </Button>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {contacts.map((contact, index) => (
                    <div key={contact.id} className="border rounded-lg p-4 space-y-4">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-2">
                          <Badge variant={contact.isPrimary ? "default" : "outline"}>
                            {contact.isPrimary ? "Primary Contact" : `Contact ${index + 1}`}
                          </Badge>
                        </div>
                        {contacts.length > 1 && (
                          <Button variant="ghost" size="sm" onClick={() => removeContact(contact.id)}>
                            <X className="w-4 h-4" />
                          </Button>
                        )}
                      </div>
                      
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <div>
                          <Label>Title</Label>
                          <Select defaultValue={contact.title}>
                            <SelectTrigger>
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="Mr.">Mr.</SelectItem>
                              <SelectItem value="Ms.">Ms.</SelectItem>
                              <SelectItem value="Mrs.">Mrs.</SelectItem>
                              <SelectItem value="Dr.">Dr.</SelectItem>
                              <SelectItem value="Prof.">Prof.</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div>
                          <Label>First Name *</Label>
                          <Input placeholder="First name" />
                        </div>
                        <div>
                          <Label>Last Name *</Label>
                          <Input placeholder="Last name" />
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <Label>Email *</Label>
                          <Input type="email" placeholder="email@company.com" />
                        </div>
                        <div>
                          <Label>Mobile Number *</Label>
                          <Input placeholder="+91 9876543210" />
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <Label>Designation</Label>
                          <Input placeholder="Manager, Director, etc." />
                        </div>
                        <div>
                          <Label>Department</Label>
                          <Input placeholder="Purchase, Quality, etc." />
                        </div>
                      </div>

                      <div>
                        <Label>LinkedIn Profile</Label>
                        <Input placeholder="https://linkedin.com/in/..." />
                      </div>
                    </div>
                  ))}
                </CardContent>
              </Card>

              {/* Addresses */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <div className="flex items-center">
                      <MapPin className="w-5 h-5 mr-2" />
                      Addresses
                    </div>
                    <Button onClick={addAddress} size="sm">
                      <Plus className="w-4 h-4 mr-2" />
                      Add Address
                    </Button>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {addresses.map((address, index) => (
                    <div key={address.id} className="border rounded-lg p-4 space-y-4">
                      <div className="flex items-center justify-between">
                        <Badge variant="outline">{address.type} Address</Badge>
                        {addresses.length > 1 && (
                          <Button variant="ghost" size="sm" onClick={() => removeAddress(address.id)}>
                            <X className="w-4 h-4" />
                          </Button>
                        )}
                      </div>

                      <div>
                        <Label>Address Type</Label>
                        <Select defaultValue={address.type}>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="Billing">Billing</SelectItem>
                            <SelectItem value="Shipping">Shipping</SelectItem>
                            <SelectItem value="Registered">Registered Office</SelectItem>
                            <SelectItem value="Factory">Factory</SelectItem>
                            <SelectItem value="Warehouse">Warehouse</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      <div className="space-y-4">
                        <div>
                          <Label>Address Line 1 *</Label>
                          <Input placeholder="Building, Street" />
                        </div>
                        <div>
                          <Label>Address Line 2</Label>
                          <Input placeholder="Area, Landmark" />
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <Label>City *</Label>
                          <Input placeholder="City" />
                        </div>
                        <div>
                          <Label>State *</Label>
                          <Select>
                            <SelectTrigger>
                              <SelectValue placeholder="Select state" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="karnataka">Karnataka</SelectItem>
                              <SelectItem value="maharashtra">Maharashtra</SelectItem>
                              <SelectItem value="tamil-nadu">Tamil Nadu</SelectItem>
                              <SelectItem value="gujarat">Gujarat</SelectItem>
                              <SelectItem value="rajasthan">Rajasthan</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <div>
                          <Label>PIN Code *</Label>
                          <Input placeholder="560001" />
                        </div>
                        <div>
                          <Label>Phone</Label>
                          <Input placeholder="080-12345678" />
                        </div>
                        <div>
                          <Label>Email</Label>
                          <Input type="email" placeholder="billing@company.com" />
                        </div>
                      </div>
                    </div>
                  ))}
                </CardContent>
              </Card>
            </div>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setCurrentStep(2)}>
                <ArrowLeft className="w-4 h-4 mr-2" />
                Previous
              </Button>
              <Button onClick={() => setCurrentStep(4)}>
                Next: Review & Submit
                <ArrowLeft className="w-4 h-4 ml-2 rotate-180" />
              </Button>
            </div>
          </TabsContent>

          {/* Step 4: Review & Submit */}
          <TabsContent value="step-4" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <CheckCircle2 className="w-5 h-5 mr-2" />
                  Review Account Information
                </CardTitle>
                <CardDescription>
                  Please review all information before creating the account
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Summary Cards */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <Card className="border-blue-200 bg-blue-50">
                    <CardContent className="p-4">
                      <div className="flex items-center space-x-2">
                        <Building2 className="w-5 h-5 text-blue-600" />
                        <div>
                          <p className="font-medium text-blue-900">{formData.accountName || "Account Name"}</p>
                          <p className="text-sm text-blue-700">{formData.industry || "Industry"}</p>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                  
                  <Card className="border-green-200 bg-green-50">
                    <CardContent className="p-4">
                      <div className="flex items-center space-x-2">
                        <Users className="w-5 h-5 text-green-600" />
                        <div>
                          <p className="font-medium text-green-900">{contacts.length} Contact{contacts.length !== 1 ? 's' : ''}</p>
                          <p className="text-sm text-green-700">{addresses.length} Address{addresses.length !== 1 ? 'es' : ''}</p>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                  
                  <Card className="border-purple-200 bg-purple-50">
                    <CardContent className="p-4">
                      <div className="flex items-center space-x-2">
                        <Star className="w-5 h-5 text-purple-600" />
                        <div>
                          <p className="font-medium text-purple-900">{formData.priority || "Medium"} Priority</p>
                          <p className="text-sm text-purple-700">{formData.accountStatus || "Prospect"}</p>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>

                {/* Validation Status */}
                <Card className="border-green-200 bg-green-50">
                  <CardContent className="p-4">
                    <div className="flex items-center space-x-2">
                      <CheckCircle2 className="w-5 h-5 text-green-600" />
                      <div>
                        <p className="font-medium text-green-900">Account Ready to Create</p>
                        <p className="text-sm text-green-700">All required fields have been completed</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                {/* Additional Options */}
                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">Additional Options</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div>
                      <Label htmlFor="description">Account Description</Label>
                      <Textarea 
                        id="description" 
                        placeholder="Add any additional notes about this account..."
                        className="min-h-[100px]"
                      />
                    </div>
                    
                    <div>
                      <Label htmlFor="tags">Tags (comma-separated)</Label>
                      <Input 
                        id="tags" 
                        placeholder="biotech, high-value, decision-pending"
                      />
                    </div>

                    <div>
                      <Label htmlFor="competitors">Known Competitors</Label>
                      <Input 
                        id="competitors" 
                        placeholder="Competitor companies, if known"
                      />
                    </div>
                  </CardContent>
                </Card>
              </CardContent>
            </Card>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setCurrentStep(3)}>
                <ArrowLeft className="w-4 h-4 mr-2" />
                Previous
              </Button>
              <div className="flex space-x-3">
                <Button onClick={handleSave}>
                  <Save className="w-4 h-4 mr-2" />
                  Save Account
                </Button>
                <Button onClick={handleSaveAndNew} className="bg-green-600 hover:bg-green-700">
                  <Plus className="w-4 h-4 mr-2" />
                  Save & Create New
                </Button>
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}