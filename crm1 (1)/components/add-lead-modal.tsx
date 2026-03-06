"use client"

import type React from "react"

import { useState } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { ChevronDown, ChevronRight, Plus, Trash2, Mail } from "lucide-react"
import { Badge } from "@/components/ui/badge"

interface AddLeadModalProps {
  isOpen: boolean
  onClose: () => void
}

interface ContactDetail {
  id: string
  account: string
  name: string
  gender: string
  title: string
  email: string
  phone: string
  mobile: string
  contactType: string
}

interface ItemDetail {
  id: string
  item: string
  quantity: number
  unitRate: number
  amount: number
}

export function AddLeadModal({ isOpen, onClose }: AddLeadModalProps) {
  const [isAddressOpen, setIsAddressOpen] = useState(false)
  const [isStatutoryOpen, setIsStatutoryOpen] = useState(false)
  const [isItemDetailsOpen, setIsItemDetailsOpen] = useState(true)
  const [isContactDetailsOpen, setIsContactDetailsOpen] = useState(true)

  const [contactDetails, setContactDetails] = useState<ContactDetail[]>([
    {
      id: "1",
      account: "",
      name: "",
      gender: "",
      title: "",
      email: "",
      phone: "",
      mobile: "",
      contactType: "",
    },
  ])

  const [itemDetails, setItemDetails] = useState<ItemDetail[]>([
    {
      id: "1",
      item: "",
      quantity: 1,
      unitRate: 0,
      amount: 0,
    },
  ])

  const addContactDetail = () => {
    const newContact: ContactDetail = {
      id: Date.now().toString(),
      account: "",
      name: "",
      gender: "",
      title: "",
      email: "",
      phone: "",
      mobile: "",
      contactType: "",
    }
    setContactDetails([...contactDetails, newContact])
  }

  const removeContactDetail = (id: string) => {
    setContactDetails(contactDetails.filter((contact) => contact.id !== id))
  }

  const addItemDetail = () => {
    const newItem: ItemDetail = {
      id: Date.now().toString(),
      item: "",
      quantity: 1,
      unitRate: 0,
      amount: 0,
    }
    setItemDetails([...itemDetails, newItem])
  }

  const removeItemDetail = (id: string) => {
    setItemDetails(itemDetails.filter((item) => item.id !== id))
  }

  const updateItemAmount = (id: string, quantity: number, unitRate: number) => {
    setItemDetails(
      itemDetails.map((item) => (item.id === id ? { ...item, quantity, unitRate, amount: quantity * unitRate } : item)),
    )
  }

  const totalAmount = itemDetails.reduce((sum, item) => sum + item.amount, 0)

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    // Handle form submission here
    console.log("Lead submitted")
    onClose()
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-xl font-semibold text-gray-900">Add New Lead</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Lead Information Section */}
          <Card>
            <CardHeader className="bg-blue-50">
              <CardTitle className="text-lg text-blue-800">Lead Information</CardTitle>
            </CardHeader>
            <CardContent className="p-6">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <Label htmlFor="leadNo">Lead No.</Label>
                  <Input id="leadNo" placeholder="Auto-generated" disabled />
                </div>
                <div>
                  <Label htmlFor="leadDate">Lead Date *</Label>
                  <Input id="leadDate" type="date" required />
                </div>
                <div>
                  <Label htmlFor="closingDate">Closing Date</Label>
                  <Input id="closingDate" type="date" />
                </div>
                <div>
                  <Label htmlFor="leadName">Lead Name *</Label>
                  <Input id="leadName" placeholder="Enter lead name" required />
                </div>
                <div>
                  <Label htmlFor="website">Website</Label>
                  <Input id="website" placeholder="https://example.com" />
                </div>
                <div>
                  <Label htmlFor="companyType">Company Type</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select company type" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="private">Private Limited</SelectItem>
                      <SelectItem value="public">Public Limited</SelectItem>
                      <SelectItem value="partnership">Partnership</SelectItem>
                      <SelectItem value="proprietorship">Proprietorship</SelectItem>
                      <SelectItem value="llp">LLP</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="acCategory">A/C Category</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select category" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="a">Category A</SelectItem>
                      <SelectItem value="b">Category B</SelectItem>
                      <SelectItem value="c">Category C</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="leadSource">Lead Source *</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select lead source" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="website">Website</SelectItem>
                      <SelectItem value="referral">Referral</SelectItem>
                      <SelectItem value="social-media">Social Media</SelectItem>
                      <SelectItem value="cold-call">Cold Call</SelectItem>
                      <SelectItem value="trade-show">Trade Show</SelectItem>
                      <SelectItem value="advertisement">Advertisement</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="industry">Industry</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select industry" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="manufacturing">Manufacturing</SelectItem>
                      <SelectItem value="healthcare">Healthcare</SelectItem>
                      <SelectItem value="education">Education</SelectItem>
                      <SelectItem value="technology">Technology</SelectItem>
                      <SelectItem value="retail">Retail</SelectItem>
                      <SelectItem value="finance">Finance</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="subIndustry">Sub Industry</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select sub industry" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="pharmaceuticals">Pharmaceuticals</SelectItem>
                      <SelectItem value="biotechnology">Biotechnology</SelectItem>
                      <SelectItem value="medical-devices">Medical Devices</SelectItem>
                      <SelectItem value="food-processing">Food Processing</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="turnOver">Turn Over</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select turnover range" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0-1cr">0 - 1 Crore</SelectItem>
                      <SelectItem value="1-5cr">1 - 5 Crores</SelectItem>
                      <SelectItem value="5-10cr">5 - 10 Crores</SelectItem>
                      <SelectItem value="10-50cr">10 - 50 Crores</SelectItem>
                      <SelectItem value="50cr+">50+ Crores</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="salesStage">Sales Stage *</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select sales stage" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="prospecting">Prospecting</SelectItem>
                      <SelectItem value="qualified">Qualified</SelectItem>
                      <SelectItem value="proposal">Proposal</SelectItem>
                      <SelectItem value="negotiation">Negotiation</SelectItem>
                      <SelectItem value="closed-won">Closed Won</SelectItem>
                      <SelectItem value="closed-lost">Closed Lost</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="leadOwner">Lead Owner *</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select lead owner" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="hari-kumar">Hari Kumar K</SelectItem>
                      <SelectItem value="vijay-muppala">Vijay Muppala</SelectItem>
                      <SelectItem value="prashanth-sandilya">Prashanth Sandilya</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="leadType">Lead Type</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select lead type" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="hot">Hot</SelectItem>
                      <SelectItem value="warm">Warm</SelectItem>
                      <SelectItem value="cold">Cold</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="leadSubType">Lead Sub-Type</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select sub-type" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="new-business">New Business</SelectItem>
                      <SelectItem value="existing-customer">Existing Customer</SelectItem>
                      <SelectItem value="referral">Referral</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="region">Region</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select region" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="north">North</SelectItem>
                      <SelectItem value="south">South</SelectItem>
                      <SelectItem value="east">East</SelectItem>
                      <SelectItem value="west">West</SelectItem>
                      <SelectItem value="central">Central</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="buyerRef">Buyer's Reference</Label>
                  <Input id="buyerRef" placeholder="Enter buyer reference" />
                </div>
                <div>
                  <Label htmlFor="otherRef">Other Reference</Label>
                  <Input id="otherRef" placeholder="Enter other reference" />
                </div>
                <div className="md:col-span-2">
                  <Label htmlFor="assignedTo">Assigned To</Label>
                  <div className="flex items-center space-x-2">
                    <Input id="assignedTo" placeholder="Click to Edit" />
                    <Button type="button" variant="outline" size="sm">
                      <Mail className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Address Information Section */}
          <Collapsible open={isAddressOpen} onOpenChange={setIsAddressOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-green-50 cursor-pointer hover:bg-green-100 transition-colors">
                  <CardTitle className="text-lg text-green-800 flex items-center">
                    {isAddressOpen ? (
                      <ChevronDown className="w-5 h-5 mr-2" />
                    ) : (
                      <ChevronRight className="w-5 h-5 mr-2" />
                    )}
                    Address Information
                  </CardTitle>
                </CardHeader>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <CardContent className="p-6">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="street">Street Address</Label>
                      <Input id="street" placeholder="Enter street address" />
                    </div>
                    <div>
                      <Label htmlFor="city">City</Label>
                      <Input id="city" placeholder="Enter city" />
                    </div>
                    <div>
                      <Label htmlFor="state">State</Label>
                      <Input id="state" placeholder="Enter state" />
                    </div>
                    <div>
                      <Label htmlFor="pincode">Pin Code</Label>
                      <Input id="pincode" placeholder="Enter pin code" />
                    </div>
                    <div>
                      <Label htmlFor="country">Country</Label>
                      <Select>
                        <SelectTrigger>
                          <SelectValue placeholder="Select country" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="india">India</SelectItem>
                          <SelectItem value="usa">USA</SelectItem>
                          <SelectItem value="uk">UK</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Statutory Information Section */}
          <Collapsible open={isStatutoryOpen} onOpenChange={setIsStatutoryOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-yellow-50 cursor-pointer hover:bg-yellow-100 transition-colors">
                  <CardTitle className="text-lg text-yellow-800 flex items-center">
                    {isStatutoryOpen ? (
                      <ChevronDown className="w-5 h-5 mr-2" />
                    ) : (
                      <ChevronRight className="w-5 h-5 mr-2" />
                    )}
                    Statutory Information
                  </CardTitle>
                </CardHeader>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <CardContent className="p-6">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="gstNumber">GST Number</Label>
                      <Input id="gstNumber" placeholder="Enter GST number" />
                    </div>
                    <div>
                      <Label htmlFor="panNumber">PAN Number</Label>
                      <Input id="panNumber" placeholder="Enter PAN number" />
                    </div>
                    <div>
                      <Label htmlFor="cinNumber">CIN Number</Label>
                      <Input id="cinNumber" placeholder="Enter CIN number" />
                    </div>
                    <div>
                      <Label htmlFor="tanNumber">TAN Number</Label>
                      <Input id="tanNumber" placeholder="Enter TAN number" />
                    </div>
                  </div>
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Item Details Section */}
          <Collapsible open={isItemDetailsOpen} onOpenChange={setIsItemDetailsOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-purple-50 cursor-pointer hover:bg-purple-100 transition-colors">
                  <CardTitle className="text-lg text-purple-800 flex items-center justify-between">
                    <div className="flex items-center">
                      {isItemDetailsOpen ? (
                        <ChevronDown className="w-5 h-5 mr-2" />
                      ) : (
                        <ChevronRight className="w-5 h-5 mr-2" />
                      )}
                      Item Details
                    </div>
                    <div className="flex items-center space-x-2">
                      <Button type="button" variant="outline" size="sm" onClick={addItemDetail}>
                        <Plus className="w-4 h-4 mr-1" />
                        Add Item
                      </Button>
                      <Badge variant="secondary">Currency: INR</Badge>
                    </div>
                  </CardTitle>
                </CardHeader>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <CardContent className="p-6">
                  <div className="space-y-4">
                    <div className="grid grid-cols-12 gap-2 text-sm font-medium text-gray-700 border-b pb-2">
                      <div className="col-span-1">No.</div>
                      <div className="col-span-4">Item</div>
                      <div className="col-span-2">QTY</div>
                      <div className="col-span-2">Unit Rate</div>
                      <div className="col-span-2">Amount</div>
                      <div className="col-span-1">Action</div>
                    </div>
                    {itemDetails.map((item, index) => (
                      <div key={item.id} className="grid grid-cols-12 gap-2 items-center">
                        <div className="col-span-1 text-center">{index + 1}</div>
                        <div className="col-span-4">
                          <Input
                            placeholder="Enter item name"
                            value={item.item}
                            onChange={(e) =>
                              setItemDetails(
                                itemDetails.map((i) => (i.id === item.id ? { ...i, item: e.target.value } : i)),
                              )
                            }
                          />
                        </div>
                        <div className="col-span-2">
                          <Input
                            type="number"
                            placeholder="1"
                            value={item.quantity}
                            onChange={(e) => {
                              const quantity = Number.parseInt(e.target.value) || 0
                              updateItemAmount(item.id, quantity, item.unitRate)
                            }}
                          />
                        </div>
                        <div className="col-span-2">
                          <Input
                            type="number"
                            placeholder="0.00"
                            value={item.unitRate}
                            onChange={(e) => {
                              const unitRate = Number.parseFloat(e.target.value) || 0
                              updateItemAmount(item.id, item.quantity, unitRate)
                            }}
                          />
                        </div>
                        <div className="col-span-2">
                          <Input value={item.amount.toFixed(2)} disabled className="bg-gray-50" />
                        </div>
                        <div className="col-span-1">
                          {itemDetails.length > 1 && (
                            <Button type="button" variant="ghost" size="sm" onClick={() => removeItemDetail(item.id)}>
                              <Trash2 className="w-4 h-4 text-red-500" />
                            </Button>
                          )}
                        </div>
                      </div>
                    ))}
                    <div className="border-t pt-4">
                      <div className="flex justify-between items-center">
                        <span className="font-medium">Total:</span>
                        <span className="font-bold text-lg">₹{totalAmount.toFixed(2)}</span>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Contact Details Section */}
          <Collapsible open={isContactDetailsOpen} onOpenChange={setIsContactDetailsOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-indigo-50 cursor-pointer hover:bg-indigo-100 transition-colors">
                  <CardTitle className="text-lg text-indigo-800 flex items-center justify-between">
                    <div className="flex items-center">
                      {isContactDetailsOpen ? (
                        <ChevronDown className="w-5 h-5 mr-2" />
                      ) : (
                        <ChevronRight className="w-5 h-5 mr-2" />
                      )}
                      Contact Details
                    </div>
                    <Button type="button" variant="outline" size="sm" onClick={addContactDetail}>
                      <Plus className="w-4 h-4 mr-1" />
                      Add Contact
                    </Button>
                  </CardTitle>
                </CardHeader>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <CardContent className="p-6">
                  <div className="space-y-4">
                    <div className="grid grid-cols-9 gap-2 text-sm font-medium text-gray-700 border-b pb-2">
                      <div>Account</div>
                      <div>Name</div>
                      <div>Gender</div>
                      <div>Title/Dept</div>
                      <div>Email</div>
                      <div>Phone</div>
                      <div>Mobile</div>
                      <div>Contact Type</div>
                      <div>Action</div>
                    </div>
                    {contactDetails.map((contact) => (
                      <div key={contact.id} className="grid grid-cols-9 gap-2 items-center">
                        <Input
                          placeholder="Account"
                          value={contact.account}
                          onChange={(e) =>
                            setContactDetails(
                              contactDetails.map((c) => (c.id === contact.id ? { ...c, account: e.target.value } : c)),
                            )
                          }
                        />
                        <Input
                          placeholder="Name"
                          value={contact.name}
                          onChange={(e) =>
                            setContactDetails(
                              contactDetails.map((c) => (c.id === contact.id ? { ...c, name: e.target.value } : c)),
                            )
                          }
                        />
                        <Select
                          value={contact.gender}
                          onValueChange={(value) =>
                            setContactDetails(
                              contactDetails.map((c) => (c.id === contact.id ? { ...c, gender: value } : c)),
                            )
                          }
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="Gender" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="male">Male</SelectItem>
                            <SelectItem value="female">Female</SelectItem>
                            <SelectItem value="other">Other</SelectItem>
                          </SelectContent>
                        </Select>
                        <Input
                          placeholder="Title/Dept"
                          value={contact.title}
                          onChange={(e) =>
                            setContactDetails(
                              contactDetails.map((c) => (c.id === contact.id ? { ...c, title: e.target.value } : c)),
                            )
                          }
                        />
                        <Input
                          type="email"
                          placeholder="Email"
                          value={contact.email}
                          onChange={(e) =>
                            setContactDetails(
                              contactDetails.map((c) => (c.id === contact.id ? { ...c, email: e.target.value } : c)),
                            )
                          }
                        />
                        <Input
                          placeholder="Phone"
                          value={contact.phone}
                          onChange={(e) =>
                            setContactDetails(
                              contactDetails.map((c) => (c.id === contact.id ? { ...c, phone: e.target.value } : c)),
                            )
                          }
                        />
                        <Input
                          placeholder="Mobile"
                          value={contact.mobile}
                          onChange={(e) =>
                            setContactDetails(
                              contactDetails.map((c) => (c.id === contact.id ? { ...c, mobile: e.target.value } : c)),
                            )
                          }
                        />
                        <Select
                          value={contact.contactType}
                          onValueChange={(value) =>
                            setContactDetails(
                              contactDetails.map((c) => (c.id === contact.id ? { ...c, contactType: value } : c)),
                            )
                          }
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="Type" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="primary">Primary</SelectItem>
                            <SelectItem value="secondary">Secondary</SelectItem>
                            <SelectItem value="technical">Technical</SelectItem>
                            <SelectItem value="financial">Financial</SelectItem>
                          </SelectContent>
                        </Select>
                        <div>
                          {contactDetails.length > 1 && (
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              onClick={() => removeContactDetail(contact.id)}
                            >
                              <Trash2 className="w-4 h-4 text-red-500" />
                            </Button>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Description Section */}
          <Card>
            <CardHeader className="bg-gray-50">
              <CardTitle className="text-lg text-gray-800">Description (Max Length: 500)</CardTitle>
            </CardHeader>
            <CardContent className="p-6">
              <Textarea
                placeholder="Enter lead description, notes, or additional information..."
                maxLength={500}
                rows={4}
                className="resize-none"
              />
            </CardContent>
          </Card>

          {/* Form Actions */}
          <div className="flex justify-end space-x-4 pt-6 border-t">
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit" className="bg-blue-600 hover:bg-blue-700">
              Save Lead
            </Button>
            <Button type="button" className="bg-green-600 hover:bg-green-700">
              Save & New
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  )
}
