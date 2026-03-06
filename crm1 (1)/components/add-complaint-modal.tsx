"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Checkbox } from "@/components/ui/checkbox"
import { useToast } from "@/hooks/use-toast"
import { Separator } from "@/components/ui/separator"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

interface AddComplaintModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (complaintData: any) => void
  editingComplaint?: any
  sourceData?: any // Installation or AMC data when creating complaint
  sourceType?: 'installation' | 'amc' | 'direct'
}

export function AddComplaintModal({ 
  isOpen, 
  onClose, 
  onSave, 
  editingComplaint, 
  sourceData,
  sourceType = 'direct'
}: AddComplaintModalProps) {
  const { toast } = useToast()
  const [formData, setFormData] = useState({
    // Reference Information
    installation_id: '',
    amc_contract_id: '',
    source_reference: '',
    
    // Customer Information
    customer_name: '',
    contact_person: '',
    customer_phone: '',
    customer_email: '',
    customer_address: '',
    city: '',
    state: '',
    pincode: '',
    
    // Complaint Details
    complaint_title: '',
    category: 'Technical',
    complaint_description: '',
    complaint_type: 'product_quality',
    severity: 'Medium',
    
    // Product/Service Information
    product_name: '',
    product_model: '',
    serial_number: '',
    purchase_date: '',
    warranty_status: 'Under Warranty',
    
    // Status and Assignment
    status: 'Open',
    priority: 'Medium',
    assigned_to: '',
    department: 'Technical',
    
    // Resolution Details
    resolution_description: '',
    resolution_date: '',
    resolution_time_hours: '',
    customer_satisfaction_rating: '',
    
    // Follow-up
    follow_up_required: false,
    follow_up_date: '',
    follow_up_notes: '',
    
    // Communication History (JSON array)
    communication_history: '[]',
    
    // Escalation
    escalation_level: 0,
    escalated_to: '',
    escalation_date: '',
    escalation_reason: '',
    
    // Financial Impact
    compensation_amount: '',
    refund_amount: '',
    cost_to_resolve: '',
    
    // Additional Information
    root_cause: '',
    preventive_action: '',
    lessons_learned: ''
  })

  // Populate form from installation or AMC data
  useEffect(() => {
    if (sourceData) {
      console.log('Populating complaint form from source:', sourceType, sourceData)
      console.log('Source data keys:', Object.keys(sourceData))
      console.log('Product name:', sourceData.product_name)
      console.log('Equipment name:', sourceData.equipment_name)
      
      let updatedFormData = { ...formData }
      
      if (sourceType === 'installation') {
        updatedFormData = {
          ...updatedFormData,
          installation_id: sourceData.id || '',
          source_reference: sourceData.installation_number || '',
          
          // Customer information from installation
          customer_name: sourceData.customer_name || sourceData.account_name || '',
          contact_person: sourceData.contact_person || '',
          customer_phone: sourceData.customer_phone || sourceData.contact_phone || '',
          customer_email: sourceData.customer_email || sourceData.contact_email || '',
          customer_address: sourceData.installation_address || '',
          city: sourceData.city || '',
          state: sourceData.state || '',
          pincode: sourceData.pincode || '',
          
          // Product information
          product_name: sourceData.product_name || sourceData.equipment_name || 'Equipment from Installation',
          product_model: sourceData.product_model || sourceData.model_number || '',
          serial_number: sourceData.serial_number || '',
          purchase_date: sourceData.warranty_start_date || sourceData.scheduled_date || '',
          warranty_status: sourceData.warranty_end_date && new Date(sourceData.warranty_end_date) > new Date() 
            ? 'Under Warranty' 
            : 'Warranty Expired',
          
          // Set category based on installation
          category: 'Technical',
          complaint_type: 'installation_issue'
        }
      } else if (sourceType === 'amc') {
        updatedFormData = {
          ...updatedFormData,
          amc_contract_id: sourceData.id || '',
          source_reference: sourceData.amc_number || '',
          
          // Customer information from AMC
          customer_name: sourceData.customer_name || '',
          contact_person: sourceData.contact_person || '',
          customer_phone: sourceData.customer_phone || '',
          customer_email: sourceData.customer_email || '',
          customer_address: sourceData.service_address || '',
          city: sourceData.city || '',
          state: sourceData.state || '',
          pincode: sourceData.pincode || '',
          
          // Equipment details from AMC
          product_name: sourceData.equipment_details && sourceData.equipment_details.length > 0 
            ? sourceData.equipment_details[0].equipment || '' 
            : '',
          product_model: sourceData.equipment_details && sourceData.equipment_details.length > 0 
            ? sourceData.equipment_details[0].model || '' 
            : '',
          serial_number: sourceData.equipment_details && sourceData.equipment_details.length > 0 
            ? sourceData.equipment_details[0].serial || '' 
            : '',
          purchase_date: sourceData.start_date || '',
          warranty_status: sourceData.status === 'Active' ? 'Under Warranty' : 'Warranty Expired',
          
          // Set category based on AMC
          category: 'Service',
          complaint_type: 'service_quality'
        }
      }
      
      setFormData(updatedFormData)
    }
  }, [sourceData, sourceType])

  // Pre-populate form when editing
  useEffect(() => {
    if (editingComplaint) {
      setFormData({
        ...editingComplaint,
        communication_history: typeof editingComplaint.communication_history === 'string' 
          ? editingComplaint.communication_history 
          : JSON.stringify(editingComplaint.communication_history || [])
      })
    }
  }, [editingComplaint])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    
    try {
      // Validate required fields
      if (!formData.customer_name?.trim()) {
        toast({ title: "Error", description: "Customer name is required", variant: "destructive" })
        return
      }
      
      if (!formData.contact_person?.trim()) {
        toast({ title: "Error", description: "Contact person is required", variant: "destructive" })
        return
      }
      
      if (!formData.complaint_title?.trim()) {
        toast({ title: "Error", description: "Complaint title is required", variant: "destructive" })
        return
      }
      
      if (!formData.complaint_description?.trim()) {
        toast({ title: "Error", description: "Complaint description is required", variant: "destructive" })
        return
      }

      // Prepare data for submission
      const submitData = {
        ...formData,
        escalation_level: formData.escalation_level ? parseInt(formData.escalation_level.toString()) : 0,
        customer_satisfaction_rating: formData.customer_satisfaction_rating ? parseInt(formData.customer_satisfaction_rating) : null,
        resolution_time_hours: formData.resolution_time_hours ? parseFloat(formData.resolution_time_hours) : null,
        compensation_amount: formData.compensation_amount ? parseFloat(formData.compensation_amount) : null,
        refund_amount: formData.refund_amount ? parseFloat(formData.refund_amount) : null,
        cost_to_resolve: formData.cost_to_resolve ? parseFloat(formData.cost_to_resolve) : null,
        communication_history: formData.communication_history ? 
          (typeof formData.communication_history === 'string' ? 
            JSON.parse(formData.communication_history || '[]') : 
            formData.communication_history) : 
          [],
        installation_id: formData.installation_id || null,
        amc_contract_id: formData.amc_contract_id || null,
        companyId: 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      }

      // Remove empty strings and convert to null for optional fields
      Object.keys(submitData).forEach(key => {
        if ((submitData as any)[key] === '') {
          (submitData as any)[key] = null
        }
      })

      console.log('Submitting complaint:', submitData)

      try {
        await onSave(submitData)
        onClose()
        
        // Reset form
        setFormData({
          installation_id: '',
          amc_contract_id: '',
          source_reference: '',
          customer_name: '',
          contact_person: '',
          customer_phone: '',
          customer_email: '',
          customer_address: '',
          city: '',
          state: '',
          pincode: '',
          complaint_title: '',
          category: 'Technical',
          complaint_description: '',
          complaint_type: 'product_quality',
          severity: 'Medium',
          product_name: '',
          product_model: '',
          serial_number: '',
          purchase_date: '',
          warranty_status: 'Under Warranty',
          status: 'Open',
          priority: 'Medium',
          assigned_to: '',
          department: 'Technical',
          resolution_description: '',
          resolution_date: '',
          resolution_time_hours: '',
          customer_satisfaction_rating: '',
          follow_up_required: false,
          follow_up_date: '',
          follow_up_notes: '',
          communication_history: '[]',
          escalation_level: 0,
          escalated_to: '',
          escalation_date: '',
          escalation_reason: '',
          compensation_amount: '',
          refund_amount: '',
          cost_to_resolve: '',
          root_cause: '',
          preventive_action: '',
          lessons_learned: ''
        })
      } catch (saveError) {
        console.error('Error saving complaint:', saveError)
        toast({ 
          title: "Error", 
          description: "Failed to create complaint. Please try again.", 
          variant: "destructive" 
        })
      }

    } catch (error) {
      console.error('Error processing complaint form:', error)
      toast({ 
        title: "Error", 
        description: "Failed to process complaint. Please check the form and try again.", 
        variant: "destructive" 
      })
    }
  }

  const handleInputChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            {editingComplaint ? 'Edit Complaint' : 'Create New Complaint'}
            {sourceType !== 'direct' && sourceData && (
              <span className="text-sm font-normal text-gray-600 ml-2">
                (From {sourceType === 'installation' ? 'Installation' : 'AMC'}: {sourceData.installation_number || sourceData.amc_number})
              </span>
            )}
          </DialogTitle>
        </DialogHeader>
        
        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Customer Information */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Customer Information</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <Label htmlFor="customer_name">Account Name *</Label>
                <Input
                  id="customer_name"
                  value={formData.customer_name}
                  onChange={(e) => handleInputChange('customer_name', e.target.value)}
                  required
                />
              </div>
              <div>
                <Label htmlFor="contact_person">Contact Person *</Label>
                <Input
                  id="contact_person"
                  value={formData.contact_person}
                  onChange={(e) => handleInputChange('contact_person', e.target.value)}
                  required
                />
              </div>
              <div>
                <Label htmlFor="customer_phone">Phone</Label>
                <Input
                  id="customer_phone"
                  value={formData.customer_phone}
                  onChange={(e) => handleInputChange('customer_phone', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="customer_email">Email</Label>
                <Input
                  id="customer_email"
                  type="email"
                  value={formData.customer_email}
                  onChange={(e) => handleInputChange('customer_email', e.target.value)}
                />
              </div>
              <div className="md:col-span-2">
                <Label htmlFor="customer_address">Address</Label>
                <Textarea
                  id="customer_address"
                  value={formData.customer_address}
                  onChange={(e) => handleInputChange('customer_address', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="city">City</Label>
                <Input
                  id="city"
                  value={formData.city}
                  onChange={(e) => handleInputChange('city', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="state">State</Label>
                <Input
                  id="state"
                  value={formData.state}
                  onChange={(e) => handleInputChange('state', e.target.value)}
                />
              </div>
            </CardContent>
          </Card>

          {/* Complaint Details */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Complaint Details</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label htmlFor="complaint_title">Complaint Title *</Label>
                <Input
                  id="complaint_title"
                  value={formData.complaint_title}
                  onChange={(e) => handleInputChange('complaint_title', e.target.value)}
                  required
                />
              </div>
              <div>
                <Label htmlFor="complaint_description">Description *</Label>
                <Textarea
                  id="complaint_description"
                  value={formData.complaint_description}
                  onChange={(e) => handleInputChange('complaint_description', e.target.value)}
                  rows={4}
                  required
                />
              </div>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <Label htmlFor="complaint_type">Type</Label>
                  <Select value={formData.complaint_type} onValueChange={(value) => handleInputChange('complaint_type', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="product_quality">Product Quality</SelectItem>
                      <SelectItem value="service_quality">Service Quality</SelectItem>
                      <SelectItem value="installation_issue">Installation Issue</SelectItem>
                      <SelectItem value="billing_issue">Billing Issue</SelectItem>
                      <SelectItem value="delivery_issue">Delivery Issue</SelectItem>
                      <SelectItem value="warranty_claim">Warranty Claim</SelectItem>
                      <SelectItem value="other">Other</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="severity">Severity</Label>
                  <Select value={formData.severity} onValueChange={(value) => handleInputChange('severity', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Low">Low</SelectItem>
                      <SelectItem value="Medium">Medium</SelectItem>
                      <SelectItem value="High">High</SelectItem>
                      <SelectItem value="Critical">Critical</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="category">Category</Label>
                  <Select value={formData.category} onValueChange={(value) => handleInputChange('category', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Technical">Technical</SelectItem>
                      <SelectItem value="Commercial">Commercial</SelectItem>
                      <SelectItem value="Service">Service</SelectItem>
                      <SelectItem value="Quality">Quality</SelectItem>
                      <SelectItem value="Delivery">Delivery</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Product Information */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Product Information</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <Label htmlFor="product_name">Product Name</Label>
                <Input
                  id="product_name"
                  value={formData.product_name}
                  onChange={(e) => handleInputChange('product_name', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="product_model">Model</Label>
                <Input
                  id="product_model"
                  value={formData.product_model}
                  onChange={(e) => handleInputChange('product_model', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="serial_number">Serial Number</Label>
                <Input
                  id="serial_number"
                  value={formData.serial_number}
                  onChange={(e) => handleInputChange('serial_number', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="warranty_status">Warranty Status</Label>
                <Select value={formData.warranty_status} onValueChange={(value) => handleInputChange('warranty_status', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Under Warranty">Under Warranty</SelectItem>
                    <SelectItem value="Warranty Expired">Warranty Expired</SelectItem>
                    <SelectItem value="No Warranty">No Warranty</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          {/* Assignment & Status */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Assignment & Status</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <Label htmlFor="status">Status</Label>
                <Select value={formData.status} onValueChange={(value) => handleInputChange('status', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Open">Open</SelectItem>
                    <SelectItem value="Assigned">Assigned</SelectItem>
                    <SelectItem value="In Progress">In Progress</SelectItem>
                    <SelectItem value="Resolved">Resolved</SelectItem>
                    <SelectItem value="Closed">Closed</SelectItem>
                    <SelectItem value="Escalated">Escalated</SelectItem>
                    <SelectItem value="On Hold">On Hold</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label htmlFor="priority">Priority</Label>
                <Select value={formData.priority} onValueChange={(value) => handleInputChange('priority', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Low">Low</SelectItem>
                    <SelectItem value="Medium">Medium</SelectItem>
                    <SelectItem value="High">High</SelectItem>
                    <SelectItem value="Critical">Critical</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label htmlFor="assigned_to">Assigned To</Label>
                <Input
                  id="assigned_to"
                  value={formData.assigned_to}
                  onChange={(e) => handleInputChange('assigned_to', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="department">Department</Label>
                <Select value={formData.department} onValueChange={(value) => handleInputChange('department', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Technical">Technical</SelectItem>
                    <SelectItem value="Sales">Sales</SelectItem>
                    <SelectItem value="Service">Service</SelectItem>
                    <SelectItem value="Quality">Quality</SelectItem>
                    <SelectItem value="Management">Management</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          {/* Resolution Details (for editing existing complaints) */}
          {editingComplaint && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Resolution Details</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label htmlFor="resolution_description">Resolution Description</Label>
                  <Textarea
                    id="resolution_description"
                    value={formData.resolution_description}
                    onChange={(e) => handleInputChange('resolution_description', e.target.value)}
                    rows={3}
                  />
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="resolution_date">Resolution Date</Label>
                    <Input
                      id="resolution_date"
                      type="date"
                      value={formData.resolution_date}
                      onChange={(e) => handleInputChange('resolution_date', e.target.value)}
                    />
                  </div>
                  <div>
                    <Label htmlFor="customer_satisfaction_rating">Customer Rating (1-5)</Label>
                    <Input
                      id="customer_satisfaction_rating"
                      type="number"
                      min="1"
                      max="5"
                      value={formData.customer_satisfaction_rating}
                      onChange={(e) => handleInputChange('customer_satisfaction_rating', e.target.value)}
                    />
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit">
              {editingComplaint ? 'Update' : 'Create'} Complaint
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}