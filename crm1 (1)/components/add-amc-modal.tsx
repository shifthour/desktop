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

interface AddAMCModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (amcData: any) => void
  editingAMC?: any
  sourceData?: any // Installation data when creating AMC from installation
  sourceType?: 'installation' | 'direct'
}

export function AddAMCModal({ 
  isOpen, 
  onClose, 
  onSave, 
  editingAMC, 
  sourceData,
  sourceType = 'direct'
}: AddAMCModalProps) {
  const { toast } = useToast()
  const [formData, setFormData] = useState({
    // Reference
    installation_id: '',
    
    // Customer Information
    customer_name: '',
    contact_person: '',
    customer_phone: '',
    customer_email: '',
    service_address: '',
    city: '',
    state: '',
    pincode: '',
    
    // Equipment Information (will be JSONB array)
    equipment_details: '[]',
    
    // Contract Details
    contract_type: 'comprehensive',
    contract_value: '',
    payment_terms: '',
    
    // Contract Period
    start_date: '',
    end_date: '',
    duration_months: 12,
    
    // Service Details
    service_frequency: 'quarterly',
    number_of_services: 4,
    services_completed: 0,
    
    // Coverage Details
    labour_charges_included: true,
    spare_parts_included: false,
    emergency_support: false,
    response_time_hours: 24,
    
    // Status
    status: 'Active',
    
    // Assignment
    assigned_technician: '',
    service_manager: '',
    
    // Additional Information
    terms_and_conditions: '',
    exclusions: '',
    special_instructions: '',
    
    // Auto-renewal
    auto_renewal: false,
    renewal_notice_days: 30
  })

  // Populate form from installation data
  useEffect(() => {
    if (sourceData && sourceType === 'installation') {
      console.log('Populating AMC form from installation:', sourceData)
      
      // Create equipment details from installation
      const equipmentDetails = [{
        equipment: sourceData.product_name || 'Equipment',
        model: sourceData.product_model || '',
        serial: sourceData.serial_number || '',
        installation_date: sourceData.actual_end_time || sourceData.scheduled_date
      }]

      const updatedFormData = {
        ...formData,
        installation_id: sourceData.id || '',
        
        // Customer information from installation
        customer_name: sourceData.customer_name || '',
        contact_person: sourceData.contact_person || '',
        customer_phone: sourceData.customer_phone || '',
        customer_email: sourceData.customer_email || '',
        service_address: sourceData.installation_address || '',
        city: sourceData.city || '',
        state: sourceData.state || '',
        pincode: sourceData.pincode || '',
        
        // Equipment details as JSON string
        equipment_details: JSON.stringify(equipmentDetails),
        
        // Set contract start date as installation completion date or tomorrow
        start_date: sourceData.actual_end_time 
          ? new Date(sourceData.actual_end_time).toISOString().split('T')[0]
          : new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().split('T')[0],
        
        // Pre-fill notes with installation reference
        special_instructions: `AMC for installation: ${sourceData.installation_number || sourceData.id}`,
      }
      
      // Calculate end date based on duration
      if (updatedFormData.start_date) {
        const startDate = new Date(updatedFormData.start_date)
        const endDate = new Date(startDate)
        endDate.setMonth(endDate.getMonth() + updatedFormData.duration_months)
        updatedFormData.end_date = endDate.toISOString().split('T')[0]
      }
      
      setFormData(updatedFormData)
    }
  }, [sourceData, sourceType])

  // Pre-populate form when editing
  useEffect(() => {
    if (editingAMC) {
      setFormData({
        ...editingAMC,
        equipment_details: typeof editingAMC.equipment_details === 'string' 
          ? editingAMC.equipment_details 
          : JSON.stringify(editingAMC.equipment_details || [])
      })
    }
  }, [editingAMC])

  // Calculate number of services based on frequency and duration
  useEffect(() => {
    const calculateServices = () => {
      const { service_frequency, duration_months } = formData
      let servicesPerYear = 4 // quarterly default
      
      switch (service_frequency) {
        case 'monthly': servicesPerYear = 12; break
        case 'quarterly': servicesPerYear = 4; break
        case 'half_yearly': servicesPerYear = 2; break
        case 'yearly': servicesPerYear = 1; break
      }
      
      const totalServices = Math.ceil((duration_months / 12) * servicesPerYear)
      setFormData(prev => ({ ...prev, number_of_services: totalServices }))
    }
    
    if (formData.service_frequency && formData.duration_months) {
      calculateServices()
    }
  }, [formData.service_frequency, formData.duration_months])

  // Calculate end date when start date or duration changes
  useEffect(() => {
    if (formData.start_date && formData.duration_months) {
      const startDate = new Date(formData.start_date)
      const endDate = new Date(startDate)
      endDate.setMonth(endDate.getMonth() + formData.duration_months)
      setFormData(prev => ({ ...prev, end_date: endDate.toISOString().split('T')[0] }))
    }
  }, [formData.start_date, formData.duration_months])

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
      
      if (!formData.service_address?.trim()) {
        toast({ title: "Error", description: "Service address is required", variant: "destructive" })
        return
      }
      
      if (!formData.contract_value || parseFloat(formData.contract_value) <= 0) {
        toast({ title: "Error", description: "Contract value must be greater than 0", variant: "destructive" })
        return
      }
      
      if (!formData.start_date) {
        toast({ title: "Error", description: "Start date is required", variant: "destructive" })
        return
      }

      // Prepare data for submission
      const submitData = {
        ...formData,
        contract_value: parseFloat(formData.contract_value),
        duration_months: parseInt(formData.duration_months.toString()),
        number_of_services: parseInt(formData.number_of_services.toString()),
        services_completed: parseInt(formData.services_completed.toString()),
        response_time_hours: parseInt(formData.response_time_hours.toString()),
        renewal_notice_days: parseInt(formData.renewal_notice_days.toString()),
        equipment_details: JSON.parse(formData.equipment_details || '[]'),
        installation_id: formData.installation_id || null,
        companyId: 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      }

      // Remove empty strings and convert to null for optional fields
      Object.keys(submitData).forEach(key => {
        if ((submitData as any)[key] === '') {
          (submitData as any)[key] = null
        }
      })

      console.log('Submitting AMC contract:', submitData)

      await onSave(submitData)
      onClose()
      
      // Reset form
      setFormData({
        installation_id: '',
        customer_name: '',
        contact_person: '',
        customer_phone: '',
        customer_email: '',
        service_address: '',
        city: '',
        state: '',
        pincode: '',
        equipment_details: '[]',
        contract_type: 'comprehensive',
        contract_value: '',
        payment_terms: '',
        start_date: '',
        end_date: '',
        duration_months: 12,
        service_frequency: 'quarterly',
        number_of_services: 4,
        services_completed: 0,
        labour_charges_included: true,
        spare_parts_included: false,
        emergency_support: false,
        response_time_hours: 24,
        status: 'Active',
        assigned_technician: '',
        service_manager: '',
        terms_and_conditions: '',
        exclusions: '',
        special_instructions: '',
        auto_renewal: false,
        renewal_notice_days: 30
      })

    } catch (error) {
      console.error('Error processing AMC form:', error)
      toast({ 
        title: "Error", 
        description: "Failed to process AMC contract. Please check the form and try again.", 
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
            {editingAMC ? 'Edit AMC Contract' : 'Create New AMC Contract'}
            {sourceType === 'installation' && sourceData && (
              <span className="text-sm font-normal text-gray-600 ml-2">
                (From Installation: {sourceData.installation_number})
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
                <Label htmlFor="customer_name">Customer Name *</Label>
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
                <Label htmlFor="service_address">Service Address *</Label>
                <Textarea
                  id="service_address"
                  value={formData.service_address}
                  onChange={(e) => handleInputChange('service_address', e.target.value)}
                  required
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

          {/* Contract Details */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Contract Details</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <Label htmlFor="contract_type">Contract Type *</Label>
                <Select value={formData.contract_type} onValueChange={(value) => handleInputChange('contract_type', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="comprehensive">Comprehensive</SelectItem>
                    <SelectItem value="non_comprehensive">Non-Comprehensive</SelectItem>
                    <SelectItem value="preventive_only">Preventive Only</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label htmlFor="contract_value">Contract Value (₹) *</Label>
                <Input
                  id="contract_value"
                  type="number"
                  step="0.01"
                  value={formData.contract_value}
                  onChange={(e) => handleInputChange('contract_value', e.target.value)}
                  required
                />
              </div>
              <div>
                <Label htmlFor="start_date">Start Date *</Label>
                <Input
                  id="start_date"
                  type="date"
                  value={formData.start_date}
                  onChange={(e) => handleInputChange('start_date', e.target.value)}
                  required
                />
              </div>
              <div>
                <Label htmlFor="duration_months">Duration (Months)</Label>
                <Input
                  id="duration_months"
                  type="number"
                  value={formData.duration_months}
                  onChange={(e) => handleInputChange('duration_months', parseInt(e.target.value))}
                />
              </div>
              <div>
                <Label htmlFor="end_date">End Date</Label>
                <Input
                  id="end_date"
                  type="date"
                  value={formData.end_date}
                  onChange={(e) => handleInputChange('end_date', e.target.value)}
                  disabled
                />
              </div>
              <div>
                <Label htmlFor="status">Status</Label>
                <Select value={formData.status} onValueChange={(value) => handleInputChange('status', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Active">Active</SelectItem>
                    <SelectItem value="Expired">Expired</SelectItem>
                    <SelectItem value="Cancelled">Cancelled</SelectItem>
                    <SelectItem value="Suspended">Suspended</SelectItem>
                    <SelectItem value="Renewal Due">Renewal Due</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          {/* Service Details */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Service Details</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <Label htmlFor="service_frequency">Service Frequency</Label>
                <Select value={formData.service_frequency} onValueChange={(value) => handleInputChange('service_frequency', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="monthly">Monthly</SelectItem>
                    <SelectItem value="quarterly">Quarterly</SelectItem>
                    <SelectItem value="half_yearly">Half-Yearly</SelectItem>
                    <SelectItem value="yearly">Yearly</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label htmlFor="number_of_services">Total Services</Label>
                <Input
                  id="number_of_services"
                  type="number"
                  value={formData.number_of_services}
                  onChange={(e) => handleInputChange('number_of_services', parseInt(e.target.value))}
                  disabled
                />
              </div>
              <div>
                <Label htmlFor="assigned_technician">Assigned Technician</Label>
                <Input
                  id="assigned_technician"
                  value={formData.assigned_technician}
                  onChange={(e) => handleInputChange('assigned_technician', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="service_manager">Service Manager</Label>
                <Input
                  id="service_manager"
                  value={formData.service_manager}
                  onChange={(e) => handleInputChange('service_manager', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="response_time_hours">Response Time (Hours)</Label>
                <Input
                  id="response_time_hours"
                  type="number"
                  value={formData.response_time_hours}
                  onChange={(e) => handleInputChange('response_time_hours', parseInt(e.target.value))}
                />
              </div>
            </CardContent>
          </Card>

          {/* Coverage & Terms */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Coverage & Terms</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="labour_charges_included"
                    checked={formData.labour_charges_included}
                    onCheckedChange={(checked) => handleInputChange('labour_charges_included', checked)}
                  />
                  <Label htmlFor="labour_charges_included">Labour Charges Included</Label>
                </div>
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="spare_parts_included"
                    checked={formData.spare_parts_included}
                    onCheckedChange={(checked) => handleInputChange('spare_parts_included', checked)}
                  />
                  <Label htmlFor="spare_parts_included">Spare Parts Included</Label>
                </div>
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="emergency_support"
                    checked={formData.emergency_support}
                    onCheckedChange={(checked) => handleInputChange('emergency_support', checked)}
                  />
                  <Label htmlFor="emergency_support">Emergency Support</Label>
                </div>
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="auto_renewal"
                    checked={formData.auto_renewal}
                    onCheckedChange={(checked) => handleInputChange('auto_renewal', checked)}
                  />
                  <Label htmlFor="auto_renewal">Auto Renewal</Label>
                </div>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="payment_terms">Payment Terms</Label>
                  <Textarea
                    id="payment_terms"
                    value={formData.payment_terms}
                    onChange={(e) => handleInputChange('payment_terms', e.target.value)}
                    rows={3}
                  />
                </div>
                <div>
                  <Label htmlFor="terms_and_conditions">Terms & Conditions</Label>
                  <Textarea
                    id="terms_and_conditions"
                    value={formData.terms_and_conditions}
                    onChange={(e) => handleInputChange('terms_and_conditions', e.target.value)}
                    rows={3}
                  />
                </div>
                <div>
                  <Label htmlFor="exclusions">Exclusions</Label>
                  <Textarea
                    id="exclusions"
                    value={formData.exclusions}
                    onChange={(e) => handleInputChange('exclusions', e.target.value)}
                    rows={3}
                  />
                </div>
                <div>
                  <Label htmlFor="special_instructions">Special Instructions</Label>
                  <Textarea
                    id="special_instructions"
                    value={formData.special_instructions}
                    onChange={(e) => handleInputChange('special_instructions', e.target.value)}
                    rows={3}
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit">
              {editingAMC ? 'Update' : 'Create'} AMC Contract
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}