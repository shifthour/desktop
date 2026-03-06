"use client"

import type React from "react"

import { useState } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Checkbox } from "@/components/ui/checkbox"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Plus, ChevronDown, ChevronRight, Upload, X } from "lucide-react"

interface AddAccountModalProps {
  isOpen: boolean
  onClose: () => void
}

interface Contact {
  id: string
  name: string
  gender: string
  birthday: string
  department: string
  title: string
  contactTypes: string
  email: string
  countryAreaPhone: string
  countryMobile: string
  linkedin: string
  twitter: string
  remarks: string
}

interface Address {
  id: string
  addressName: string
  address: string
  country: string
  state: string
  district: string
  city: string
  pincode: string
  email: string
  phoneNumber: string
}

interface Item {
  id: string
  item: string
  srlNo: string
  qty: string
  rate: string
  soNumber: string
  soDate: string
  remarks: string
}

export function AddAccountModal({ isOpen, onClose }: AddAccountModalProps) {
  const [isAddressOpen, setIsAddressOpen] = useState(true)
  const [isItemDetailsOpen, setIsItemDetailsOpen] = useState(true)
  const [isContactDetailsOpen, setIsContactDetailsOpen] = useState(true)
  const [isDocumentListOpen, setIsDocumentListOpen] = useState(true)
  const [isNotesOpen, setIsNotesOpen] = useState(true)

  const [addresses, setAddresses] = useState<Address[]>([
    {
      id: "1",
      addressName: "",
      address: "",
      country: "India",
      state: "",
      district: "",
      city: "",
      pincode: "",
      email: "",
      phoneNumber: "",
    },
  ])

  const [contacts, setContacts] = useState<Contact[]>([
    {
      id: "1",
      name: "",
      gender: "",
      birthday: "",
      department: "",
      title: "",
      contactTypes: "",
      email: "",
      countryAreaPhone: "",
      countryMobile: "",
      linkedin: "",
      twitter: "",
      remarks: "",
    },
  ])

  const [items, setItems] = useState<Item[]>([
    {
      id: "1",
      item: "",
      srlNo: "",
      qty: "",
      rate: "",
      soNumber: "",
      soDate: "",
      remarks: "",
    },
  ])

  const addAddress = () => {
    const newAddress: Address = {
      id: Date.now().toString(),
      addressName: "",
      address: "",
      country: "India",
      state: "",
      district: "",
      city: "",
      pincode: "",
      email: "",
      phoneNumber: "",
    }
    setAddresses([...addresses, newAddress])
  }

  const removeAddress = (id: string) => {
    if (addresses.length > 1) {
      setAddresses(addresses.filter((addr) => addr.id !== id))
    }
  }

  const addContact = () => {
    const newContact: Contact = {
      id: Date.now().toString(),
      name: "",
      gender: "",
      birthday: "",
      department: "",
      title: "",
      contactTypes: "",
      email: "",
      countryAreaPhone: "",
      countryMobile: "",
      linkedin: "",
      twitter: "",
      remarks: "",
    }
    setContacts([...contacts, newContact])
  }

  const removeContact = (id: string) => {
    if (contacts.length > 1) {
      setContacts(contacts.filter((contact) => contact.id !== id))
    }
  }

  const addItem = () => {
    const newItem: Item = {
      id: Date.now().toString(),
      item: "",
      srlNo: "",
      qty: "",
      rate: "",
      soNumber: "",
      soDate: "",
      remarks: "",
    }
    setItems([...items, newItem])
  }

  const removeItem = (id: string) => {
    if (items.length > 1) {
      setItems(items.filter((item) => item.id !== id))
    }
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    // Handle form submission here
    console.log("Account form submitted")
    onClose()
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-xl font-semibold text-gray-800">Add New Account</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Account Details */}
          <Card>
            <CardHeader className="bg-blue-600 text-white">
              <CardTitle className="text-lg">Account Details</CardTitle>
            </CardHeader>
            <CardContent className="p-6 space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <Label htmlFor="accountCode">Account Code</Label>
                  <Input id="accountCode" placeholder="Auto-generated" />
                </div>
                <div>
                  <Label htmlFor="accountName">Account Name *</Label>
                  <Input id="accountName" required />
                </div>
                <div>
                  <Label htmlFor="displayName">Display Name</Label>
                  <Input id="displayName" />
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
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
                  <Label htmlFor="website">Website</Label>
                  <Input id="website" type="url" placeholder="https://" />
                </div>
                <div>
                  <Label htmlFor="address">Address</Label>
                  <Textarea id="address" placeholder="Enter complete address" rows={3} />
                </div>
                <div>
                  <Label htmlFor="parentAccount">Parent Account</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select parent account" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="none">None</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                  <Label htmlFor="category">Category</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select category" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="customer">Customer</SelectItem>
                      <SelectItem value="prospect">Prospect</SelectItem>
                      <SelectItem value="partner">Partner</SelectItem>
                      <SelectItem value="vendor">Vendor</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="creditTime">Credit Time (in Days)</Label>
                  <Input id="creditTime" type="number" />
                </div>
                <div>
                  <Label htmlFor="amount">Amount (in INR)</Label>
                  <Input id="amount" type="number" />
                </div>
                <div>
                  <Label htmlFor="leadSource">Lead Source</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select lead source" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="website">Website</SelectItem>
                      <SelectItem value="referral">Referral</SelectItem>
                      <SelectItem value="cold-call">Cold Call</SelectItem>
                      <SelectItem value="trade-show">Trade Show</SelectItem>
                      <SelectItem value="social-media">Social Media</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <Label htmlFor="turnOver">Turn Over</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select turnover range" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0-1cr">0 - 1 Crore</SelectItem>
                      <SelectItem value="1-10cr">1 - 10 Crores</SelectItem>
                      <SelectItem value="10-50cr">10 - 50 Crores</SelectItem>
                      <SelectItem value="50-100cr">50 - 100 Crores</SelectItem>
                      <SelectItem value="100cr+">100+ Crores</SelectItem>
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
                      <SelectItem value="biotech-company">Biotech Company</SelectItem>
                      <SelectItem value="dealer">Dealer</SelectItem>
                      <SelectItem value="educational-institutions">Educational Institutions</SelectItem>
                      <SelectItem value="food-beverages">Food and Beverages</SelectItem>
                      <SelectItem value="hair-transplant-hospitals">Hair Transplant Clinics/ Hospitals</SelectItem>
                      <SelectItem value="molecular-diagnostics">Molecular Diagnostics</SelectItem>
                      <SelectItem value="pharmaceutical">Pharmaceutical</SelectItem>
                      <SelectItem value="research">Research</SelectItem>
                      <SelectItem value="sro">SRO</SelectItem>
                      <SelectItem value="training-institute">Training Institute</SelectItem>
                      <SelectItem value="universities">Universities</SelectItem>
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
                      <SelectItem value="medical-devices">Medical Devices</SelectItem>
                      <SelectItem value="diagnostics">Diagnostics</SelectItem>
                      <SelectItem value="research">Research</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <div className="flex items-center space-x-2">
                    <Checkbox id="assignedTo" />
                    <Label htmlFor="assignedTo">Assigned To (Click to Edit)</Label>
                  </div>
                  <Input className="mt-2" defaultValue="Current User" />
                </div>
                <div className="space-y-2">
                  <div className="text-sm text-gray-600">
                    <div>Created by: Current User</div>
                    <div>Last Modified by: Current User</div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Address Information */}
          <Collapsible open={isAddressOpen} onOpenChange={setIsAddressOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-blue-600 text-white cursor-pointer hover:bg-blue-700">
                  <CardTitle className="text-lg flex items-center">
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
                  <div className="flex justify-between items-center mb-4">
                    <h4 className="font-medium">Address List</h4>
                    <Button type="button" onClick={addAddress} size="sm">
                      <Plus className="w-4 h-4 mr-2" />
                      Add Address
                    </Button>
                  </div>

                  {addresses.map((address, index) => (
                    <div key={address.id} className="border rounded-lg p-4 mb-4">
                      <div className="flex justify-between items-center mb-4">
                        <h5 className="font-medium">Address {index + 1}</h5>
                        {addresses.length > 1 && (
                          <Button type="button" variant="outline" size="sm" onClick={() => removeAddress(address.id)}>
                            <X className="w-4 h-4" />
                          </Button>
                        )}
                      </div>

                      <div className="space-y-4">
                        <div>
                          <Label>Address Name (eg: Billing, Shipping, HO, Factory...)</Label>
                          <Input placeholder="e.g., Billing Address" />
                        </div>

                        <div>
                          <Label>Address</Label>
                          <Textarea placeholder="Enter full address (Max Characters 250)" maxLength={250} />
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                          <div>
                            <Label>Country</Label>
                            <Select defaultValue="india">
                              <SelectTrigger>
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="india">India</SelectItem>
                                <SelectItem value="usa">USA</SelectItem>
                                <SelectItem value="uk">UK</SelectItem>
                                <SelectItem value="canada">Canada</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                          <div>
                            <Label>State</Label>
                            <Select>
                              <SelectTrigger>
                                <SelectValue placeholder="Select state" />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="maharashtra">Maharashtra</SelectItem>
                                <SelectItem value="karnataka">Karnataka</SelectItem>
                                <SelectItem value="tamil-nadu">Tamil Nadu</SelectItem>
                                <SelectItem value="gujarat">Gujarat</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                          <div>
                            <Label>District</Label>
                            <Select>
                              <SelectTrigger>
                                <SelectValue placeholder="Select district" />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="mumbai">Mumbai</SelectItem>
                                <SelectItem value="pune">Pune</SelectItem>
                                <SelectItem value="bangalore">Bangalore</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                          <div>
                            <Label>City</Label>
                            <Input placeholder="Max Characters 32" maxLength={32} />
                          </div>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                          <div>
                            <Label>Pincode</Label>
                            <Input />
                          </div>
                          <div>
                            <Label>Email</Label>
                            <Input type="email" />
                          </div>
                          <div>
                            <Label>Phone Number</Label>
                            <Input />
                          </div>
                        </div>

                        <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
                          <div>
                            <Label>PAN No:</Label>
                            <Input />
                          </div>
                          <div>
                            <Label>Drug Lic. No:</Label>
                            <Input />
                          </div>
                          <div>
                            <Label>CIN No:</Label>
                            <Input />
                          </div>
                          <div>
                            <Label>GSTIN:</Label>
                            <Input />
                          </div>
                          <div>
                            <Label>MSME No:</Label>
                            <Input />
                          </div>
                          <div>
                            <Label>DUNS No:</Label>
                            <Input />
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Item Details */}
          <Collapsible open={isItemDetailsOpen} onOpenChange={setIsItemDetailsOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-blue-600 text-white cursor-pointer hover:bg-blue-700">
                  <CardTitle className="text-lg flex items-center">
                    {isItemDetailsOpen ? (
                      <ChevronDown className="w-5 h-5 mr-2" />
                    ) : (
                      <ChevronRight className="w-5 h-5 mr-2" />
                    )}
                    Item Details
                  </CardTitle>
                </CardHeader>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <CardContent className="p-6">
                  <div className="flex justify-between items-center mb-4">
                    <div className="flex space-x-2">
                      <Button type="button" onClick={addItem} size="sm">
                        <Plus className="w-4 h-4 mr-2" />
                        Add Item
                      </Button>
                      <Button type="button" variant="outline" size="sm">
                        PriceList
                      </Button>
                    </div>
                    <div className="flex items-center space-x-2">
                      <Label>Currency:</Label>
                      <Select defaultValue="inr">
                        <SelectTrigger className="w-20">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="inr">INR</SelectItem>
                          <SelectItem value="usd">USD</SelectItem>
                          <SelectItem value="eur">EUR</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>No.</TableHead>
                          <TableHead>Item</TableHead>
                          <TableHead>SRLNO</TableHead>
                          <TableHead>QTY</TableHead>
                          <TableHead>Rate</TableHead>
                          <TableHead>SO#</TableHead>
                          <TableHead>SoDate</TableHead>
                          <TableHead>Remarks</TableHead>
                          <TableHead>Actions</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {items.map((item, index) => (
                          <TableRow key={item.id}>
                            <TableCell>{index + 1}</TableCell>
                            <TableCell>
                              <Input placeholder="Item name" />
                            </TableCell>
                            <TableCell>
                              <Input placeholder="Serial No" />
                            </TableCell>
                            <TableCell>
                              <Input type="number" placeholder="Qty" />
                            </TableCell>
                            <TableCell>
                              <Input type="number" placeholder="Rate" />
                            </TableCell>
                            <TableCell>
                              <Input placeholder="SO Number" />
                            </TableCell>
                            <TableCell>
                              <Input type="date" />
                            </TableCell>
                            <TableCell>
                              <Input placeholder="Remarks" />
                            </TableCell>
                            <TableCell>
                              {items.length > 1 && (
                                <Button type="button" variant="outline" size="sm" onClick={() => removeItem(item.id)}>
                                  <X className="w-4 h-4" />
                                </Button>
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Contact Details */}
          <Collapsible open={isContactDetailsOpen} onOpenChange={setIsContactDetailsOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-blue-600 text-white cursor-pointer hover:bg-blue-700">
                  <CardTitle className="text-lg flex items-center">
                    {isContactDetailsOpen ? (
                      <ChevronDown className="w-5 h-5 mr-2" />
                    ) : (
                      <ChevronRight className="w-5 h-5 mr-2" />
                    )}
                    Contact Details
                  </CardTitle>
                </CardHeader>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <CardContent className="p-6">
                  <div className="flex justify-between items-center mb-4">
                    <h4 className="font-medium">Contact List</h4>
                    <Button type="button" onClick={addContact} size="sm">
                      <Plus className="w-4 h-4 mr-2" />
                      Add Contact
                    </Button>
                  </div>

                  {contacts.map((contact, index) => (
                    <div key={contact.id} className="border rounded-lg p-4 mb-4">
                      <div className="flex justify-between items-center mb-4">
                        <h5 className="font-medium">Contact {index + 1}</h5>
                        {contacts.length > 1 && (
                          <Button type="button" variant="outline" size="sm" onClick={() => removeContact(contact.id)}>
                            <X className="w-4 h-4" />
                          </Button>
                        )}
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                        <div>
                          <Label>Name</Label>
                          <Select>
                            <SelectTrigger>
                              <SelectValue placeholder="Title" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="mr">Mr.</SelectItem>
                              <SelectItem value="ms">Ms.</SelectItem>
                              <SelectItem value="mrs">Mrs.</SelectItem>
                              <SelectItem value="dr">Dr.</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div>
                          <Label>&nbsp;</Label>
                          <Input placeholder="Full Name" />
                        </div>
                        <div>
                          <Label>Gender</Label>
                          <Select>
                            <SelectTrigger>
                              <SelectValue placeholder="Select gender" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="male">Male</SelectItem>
                              <SelectItem value="female">Female</SelectItem>
                              <SelectItem value="other">Other</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                        <div>
                          <Label>Birthday</Label>
                          <Input type="date" placeholder="dd/mm/yyyy" />
                        </div>
                        <div>
                          <Label>Department</Label>
                          <Input placeholder="Department" />
                        </div>
                        <div>
                          <Label>Title</Label>
                          <Input placeholder="Job Title" />
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                        <div>
                          <Label>Contact Types</Label>
                          <Select>
                            <SelectTrigger>
                              <SelectValue placeholder="Select contact type" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="primary">Primary</SelectItem>
                              <SelectItem value="secondary">Secondary</SelectItem>
                              <SelectItem value="billing">Billing</SelectItem>
                              <SelectItem value="technical">Technical</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div>
                          <Label>Email</Label>
                          <Input type="email" placeholder="Email address" />
                        </div>
                        <div>
                          <Label>Country Area Phone Number</Label>
                          <Input placeholder="Phone number" />
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                        <div>
                          <Label>Country Mobile Number</Label>
                          <Input placeholder="Mobile number" />
                        </div>
                        <div>
                          <Label>LinkedIn</Label>
                          <Input placeholder="LinkedIn profile URL" />
                        </div>
                        <div>
                          <Label>Twitter</Label>
                          <Input placeholder="Twitter handle" />
                        </div>
                      </div>

                      <div>
                        <Label>Remarks</Label>
                        <Textarea placeholder="Additional remarks" />
                      </div>
                    </div>
                  ))}
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Document List */}
          <Collapsible open={isDocumentListOpen} onOpenChange={setIsDocumentListOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-blue-600 text-white cursor-pointer hover:bg-blue-700">
                  <CardTitle className="text-lg flex items-center">
                    {isDocumentListOpen ? (
                      <ChevronDown className="w-5 h-5 mr-2" />
                    ) : (
                      <ChevronRight className="w-5 h-5 mr-2" />
                    )}
                    Document List
                  </CardTitle>
                </CardHeader>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <CardContent className="p-6">
                  <div className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center">
                    <Upload className="w-12 h-12 mx-auto text-gray-400 mb-4" />
                    <p className="text-gray-600 mb-4">Drag and drop files here or click to browse</p>
                    <Button type="button" variant="outline">
                      <Upload className="w-4 h-4 mr-2" />
                      Attach Document
                    </Button>
                  </div>
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Notes */}
          <Collapsible open={isNotesOpen} onOpenChange={setIsNotesOpen}>
            <Card>
              <CollapsibleTrigger asChild>
                <CardHeader className="bg-blue-600 text-white cursor-pointer hover:bg-blue-700">
                  <CardTitle className="text-lg flex items-center">
                    {isNotesOpen ? <ChevronDown className="w-5 h-5 mr-2" /> : <ChevronRight className="w-5 h-5 mr-2" />}
                    Notes List - No Notes List
                  </CardTitle>
                </CardHeader>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <CardContent className="p-6">
                  <Textarea placeholder="Add notes about this account..." className="min-h-[100px]" />
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          {/* Description */}
          <Card>
            <CardHeader className="bg-blue-600 text-white">
              <CardTitle className="text-lg">Description</CardTitle>
            </CardHeader>
            <CardContent className="p-6">
              <Textarea placeholder="Enter account description..." className="min-h-[100px]" />
            </CardContent>
          </Card>

          {/* Form Actions */}
          <div className="flex justify-end space-x-4 pt-6 border-t">
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit" className="bg-green-600 hover:bg-green-700">
              Save
            </Button>
            <Button type="submit" className="bg-green-600 hover:bg-green-700">
              Save & New
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  )
}
