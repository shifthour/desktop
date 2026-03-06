"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Checkbox } from "@/components/ui/checkbox"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { useToast } from "@/hooks/use-toast"

interface AddCaseModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (caseData: any) => void
  complaintsData?: any[]
  installationsData?: any[]
  amcData?: any[]
}

export function AddCaseModal({
  isOpen,
  onClose,
  onSave,
  complaintsData = [],
  installationsData = [],
  amcData = []
}: AddCaseModalProps) {
  const { toast } = useToast()
  const [formData, setFormData] = useState({
    // Basic Information
    title: '',
    description: '',
    solution: '',
    case_category: 'Technical Support',
    issue_type: 'Software Bug',
    severity: 'Medium',
    priority: 'Medium',
    status: 'Open',
    
    // Customer Information
    customer_name: '',
    contact_person: '',
    contact_phone: '',
    contact_email: '',
    
    // Product Information
    product_name: '',
    serial_number: '',
    model_number: '',
    
    // Assignment
    assigned_to: '',
    
    // Additional Details
    symptoms: '',
    environment_details: '',
    error_messages: '',
    reproduction_steps: '',
    customer_impact: '',
    workaround: '',
    
    // Source Link
    source_type: '', // 'complaint', 'installation', 'amc', 'direct'
    source_id: '',
    
    // Flags
    resolution_required: true,
    follow_up_required: false,
    follow_up_date: ''
  })

  const handleInputChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  const handleSourceSelection = (sourceType: string, sourceId: string) => {
    let sourceData = null
    
    if (sourceType === 'complaint') {
      sourceData = complaintsData.find(c => c.id === sourceId)
      if (sourceData) {
        setFormData(prev => ({
          ...prev,
          source_type: sourceType,
          source_id: sourceId,
          customer_name: sourceData.account_name || '',
          contact_person: sourceData.contact_person || '',
          contact_phone: sourceData.contact_phone || '',
          contact_email: sourceData.contact_email || '',
          product_name: sourceData.product_name || '',
          serial_number: sourceData.serial_number || '',
          title: `Support for complaint: ${sourceData.subject || sourceData.complaint_number}`,
          description: sourceData.description || '',
          case_category: 'Technical Support',
          priority: 'High'
        }))
      }
    } else if (sourceType === 'installation') {
      sourceData = installationsData.find(i => i.id === sourceId)
      if (sourceData) {
        setFormData(prev => ({
          ...prev,
          source_type: sourceType,
          source_id: sourceId,
          customer_name: sourceData.account_name || '',
          contact_person: sourceData.contact_person || '',
          contact_phone: sourceData.contact_phone || '',
          contact_email: sourceData.contact_email || '',
          product_name: sourceData.product_name || '',
          serial_number: sourceData.serial_number || '',
          title: `Installation support: ${sourceData.installation_number}`,
          description: `Support case for installation ${sourceData.installation_number}`,
          case_category: 'Installation',
          priority: 'Medium'
        }))
      }
    } else if (sourceType === 'amc') {
      sourceData = amcData.find(a => a.id === sourceId)
      if (sourceData) {
        setFormData(prev => ({
          ...prev,
          source_type: sourceType,
          source_id: sourceId,
          customer_name: sourceData.customer_name || '',
          contact_person: sourceData.contact_person || '',
          contact_phone: sourceData.customer_phone || '',
          contact_email: sourceData.customer_email || '',
          product_name: sourceData.product_name || '',
          title: `AMC support: ${sourceData.amc_number}`,
          description: `Support case for AMC contract ${sourceData.amc_number}`,
          case_category: 'Training',
          priority: 'Medium'
        }))
      }
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    // Validation
    if (!formData.title?.trim()) {
      toast({ title: "Error", description: "Case title is required", variant: "destructive" })
      return
    }
    if (!formData.description?.trim()) {
      toast({ title: "Error", description: "Case description is required", variant: "destructive" })
      return
    }
    if (!formData.customer_name?.trim()) {
      toast({ title: "Error", description: "Customer name is required", variant: "destructive" })
      return
    }

    try {
      // Prepare case data - remove fields not in database schema
      const { source_type, source_id, ...caseFormData } = formData
      const caseData = {
        ...caseFormData,
        case_date: new Date().toISOString().split('T')[0],
        company_id: 'de19ccb7-e90d-4507-861d-a3aecf5e3f29',
        // Convert empty date strings to null
        follow_up_date: caseFormData.follow_up_date || null
      }

      await onSave(caseData)
      
      // Reset form
      setFormData({
        title: '',
        description: '',
        solution: '',
        case_category: 'Technical Support',
        issue_type: 'Software Bug',
        severity: 'Medium',
        priority: 'Medium',
        status: 'Open',
        customer_name: '',
        contact_person: '',
        contact_phone: '',
        contact_email: '',
        product_name: '',
        serial_number: '',
        model_number: '',
        assigned_to: '',
        symptoms: '',
        environment_details: '',
        error_messages: '',
        reproduction_steps: '',
        customer_impact: '',
        workaround: '',
        source_type: '',
        source_id: '',
        resolution_required: true,
        follow_up_required: false,
        follow_up_date: ''
      })

    } catch (error) {
      console.error('Error creating case:', error)
    }
  }

  const resetForm = () => {
    setFormData({
      title: '',
      description: '',
      solution: '',
      case_category: 'Technical Support',
      issue_type: 'Software Bug',
      severity: 'Medium',
      priority: 'Medium',
      status: 'Open',
      customer_name: '',
      contact_person: '',
      contact_phone: '',
      contact_email: '',
      product_name: '',
      serial_number: '',
      model_number: '',
      assigned_to: '',
      symptoms: '',
      environment_details: '',
      error_messages: '',
      reproduction_steps: '',
      customer_impact: '',
      workaround: '',
      source_type: '',
      source_id: '',
      resolution_required: true,
      follow_up_required: false,
      follow_up_date: ''
    })
  }

  useEffect(() => {
    if (!isOpen) {
      resetForm()
    }
  }, [isOpen])

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Create New Support Case</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Link to Existing Records */}
          {(complaintsData.length > 0 || installationsData.length > 0 || amcData.length > 0) && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Link to Existing Record (Optional)</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  {/* Link to Complaint */}
                  {complaintsData.length > 0 && (
                    <div>
                      <Label>Link to Complaint</Label>
                      <Select onValueChange={(value) => handleSourceSelection('complaint', value)}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select complaint" />
                        </SelectTrigger>
                        <SelectContent>
                          {complaintsData.slice(0, 10).map((complaint) => (
                            <SelectItem key={complaint.id} value={complaint.id}>
                              {complaint.complaint_number} - {complaint.account_name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  )}

                  {/* Link to Installation */}
                  {installationsData.length > 0 && (
                    <div>
                      <Label>Link to Installation</Label>
                      <Select onValueChange={(value) => handleSourceSelection('installation', value)}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select installation" />
                        </SelectTrigger>
                        <SelectContent>
                          {installationsData.slice(0, 10).map((installation) => (
                            <SelectItem key={installation.id} value={installation.id}>
                              {installation.installation_number} - {installation.account_name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  )}

                  {/* Link to AMC */}
                  {amcData.length > 0 && (
                    <div>
                      <Label>Link to AMC</Label>
                      <Select onValueChange={(value) => handleSourceSelection('amc', value)}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select AMC" />
                        </SelectTrigger>
                        <SelectContent>
                          {amcData.slice(0, 10).map((amc) => (
                            <SelectItem key={amc.id} value={amc.id}>
                              {amc.amc_number} - {amc.customer_name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Basic Information */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Case Information</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label htmlFor="title">Case Title *</Label>
                <Input
                  id="title"
                  value={formData.title}
                  onChange={(e) => handleInputChange('title', e.target.value)}
                  placeholder="Brief description of the issue"
                  required
                />
              </div>
              
              <div>
                <Label htmlFor="description">Description *</Label>
                <Textarea
                  id="description"
                  value={formData.description}
                  onChange={(e) => handleInputChange('description', e.target.value)}
                  placeholder="Detailed description of the issue"
                  rows={4}
                  required
                />
              </div>

              <div>
                <Label htmlFor="solution">Solution</Label>
                <Textarea
                  id="solution"
                  value={formData.solution || ''}
                  onChange={(e) => handleInputChange('solution', e.target.value)}
                  placeholder="Describe the solution or steps taken to resolve this issue"
                  rows={4}
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                  <Label htmlFor="case_category">Category</Label>
                  <Select value={formData.case_category} onValueChange={(value) => handleInputChange('case_category', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Technical Support">Technical Support</SelectItem>
                      <SelectItem value="Delivery Issue">Delivery Issue</SelectItem>
                      <SelectItem value="Software Issue">Software Issue</SelectItem>
                      <SelectItem value="Hardware Issue">Hardware Issue</SelectItem>
                      <SelectItem value="Training">Training</SelectItem>
                      <SelectItem value="Warranty">Warranty</SelectItem>
                      <SelectItem value="Installation">Installation</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label htmlFor="issue_type">Issue Type</Label>
                  <Select value={formData.issue_type} onValueChange={(value) => handleInputChange('issue_type', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Software Bug">Software Bug</SelectItem>
                      <SelectItem value="Hardware Issue">Hardware Issue</SelectItem>
                      <SelectItem value="Configuration Issue">Configuration Issue</SelectItem>
                      <SelectItem value="Installation Problem">Installation Problem</SelectItem>
                      <SelectItem value="Training Need">Training Need</SelectItem>
                      <SelectItem value="Performance Issue">Performance Issue</SelectItem>
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
              </div>
            </CardContent>
          </Card>

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
                <Label htmlFor="contact_person">Contact Person</Label>
                <Input
                  id="contact_person"
                  value={formData.contact_person}
                  onChange={(e) => handleInputChange('contact_person', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="contact_phone">Phone</Label>
                <Input
                  id="contact_phone"
                  value={formData.contact_phone}
                  onChange={(e) => handleInputChange('contact_phone', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="contact_email">Email</Label>
                <Input
                  id="contact_email"
                  type="email"
                  value={formData.contact_email}
                  onChange={(e) => handleInputChange('contact_email', e.target.value)}
                />
              </div>
            </CardContent>
          </Card>

          {/* Product Information */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Product Information</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <Label htmlFor="product_name">Product Name</Label>
                <Input
                  id="product_name"
                  value={formData.product_name}
                  onChange={(e) => handleInputChange('product_name', e.target.value)}
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
                <Label htmlFor="model_number">Model Number</Label>
                <Input
                  id="model_number"
                  value={formData.model_number}
                  onChange={(e) => handleInputChange('model_number', e.target.value)}
                />
              </div>
            </CardContent>
          </Card>

          {/* Technical Details */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Technical Details</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label htmlFor="symptoms">Symptoms</Label>
                <Textarea
                  id="symptoms"
                  value={formData.symptoms}
                  onChange={(e) => handleInputChange('symptoms', e.target.value)}
                  placeholder="Describe the symptoms or error messages"
                  rows={2}
                />
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="environment_details">Environment Details</Label>
                  <Textarea
                    id="environment_details"
                    value={formData.environment_details}
                    onChange={(e) => handleInputChange('environment_details', e.target.value)}
                    placeholder="OS, browser, network setup etc."
                    rows={2}
                  />
                </div>
                <div>
                  <Label htmlFor="reproduction_steps">Reproduction Steps</Label>
                  <Textarea
                    id="reproduction_steps"
                    value={formData.reproduction_steps}
                    onChange={(e) => handleInputChange('reproduction_steps', e.target.value)}
                    placeholder="Steps to reproduce the issue"
                    rows={2}
                  />
                </div>
              </div>

              <div>
                <Label htmlFor="assigned_to">Assigned To</Label>
                <Input
                  id="assigned_to"
                  value={formData.assigned_to}
                  onChange={(e) => handleInputChange('assigned_to', e.target.value)}
                  placeholder="Technician or team member name"
                />
              </div>
            </CardContent>
          </Card>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit">
              Create Case
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}