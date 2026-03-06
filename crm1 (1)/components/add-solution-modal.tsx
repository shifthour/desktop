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
import { Badge } from "@/components/ui/badge"
import { useToast } from "@/hooks/use-toast"

interface AddSolutionModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (solutionData: any) => void
  linkedCase?: any
}

export function AddSolutionModal({
  isOpen,
  onClose,
  onSave,
  linkedCase
}: AddSolutionModalProps) {
  const { toast } = useToast()
  const [formData, setFormData] = useState({
    // Basic Information
    title: '',
    description: '',
    solution_type: 'Bug Fix',
    solution_category: 'Technical Support',
    problem_statement: '',
    root_cause: '',
    
    // Solution Details
    solution_steps: '',
    implementation_details: '',
    tools_required: '',
    parts_required: '',
    software_versions: '',
    configuration_changes: '',
    testing_performed: '',
    validation_steps: '',
    
    // Linked Case Info (if any)
    case_id: '',
    case_number: '',
    
    // Customer/Product Info
    customer_name: '',
    contact_person: '',
    product_name: '',
    product_version: '',
    environment: '',
    
    // Implementation Details
    implementation_time_hours: '',
    difficulty_level: 'Medium',
    success_rate: 'High',
    status: 'Draft',
    effectiveness: 'Effective',
    
    // Assignment
    implemented_by: '',
    verified_by: '',
    approved_by: '',
    
    // Dates
    implementation_date: '',
    verification_date: '',
    approval_date: '',
    
    // Follow-up
    follow_up_required: false,
    follow_up_date: '',
    follow_up_notes: '',
    
    // Knowledge Base
    knowledge_base_entry: true,
    public_solution: false,
    reusable: true,
    
    // Additional Info
    cost_involved: '',
    time_saved_hours: '',
    prevention_measures: '',
    lessons_learned: '',
    recommendations: '',
    applicable_products: '',
    applicable_versions: '',
    prerequisites: '',
    warnings: '',
    notes: '',
    
    // Version Control
    version: '1.0',
    superseded_by: '',
    supersedes: ''
  })

  const handleInputChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  // Auto-populate from linked case
  useEffect(() => {
    if (linkedCase) {
      setFormData(prev => ({
        ...prev,
        case_id: linkedCase.id,
        case_number: linkedCase.case_number,
        customer_name: linkedCase.customer_name || '',
        contact_person: linkedCase.contact_person || '',
        product_name: linkedCase.product_name || '',
        title: `Solution for ${linkedCase.case_number}: ${linkedCase.title}`,
        problem_statement: linkedCase.description || '',
        solution_category: linkedCase.case_category || 'Technical Support',
        environment: linkedCase.environment_details || '',
        symptoms: linkedCase.symptoms || '',
        created_from_case: true
      }))
    }
  }, [linkedCase])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    // Validation
    if (!formData.title?.trim()) {
      toast({ title: "Error", description: "Solution title is required", variant: "destructive" })
      return
    }
    if (!formData.description?.trim()) {
      toast({ title: "Error", description: "Solution description is required", variant: "destructive" })
      return
    }
    if (!formData.problem_statement?.trim()) {
      toast({ title: "Error", description: "Problem statement is required", variant: "destructive" })
      return
    }
    if (!formData.solution_steps?.trim()) {
      toast({ title: "Error", description: "Solution steps are required", variant: "destructive" })
      return
    }

    try {
      // Prepare solution data
      const solutionData = {
        ...formData,
        solution_date: new Date().toISOString().split('T')[0],
        cost_involved: formData.cost_involved ? parseFloat(formData.cost_involved) : 0,
        time_saved_hours: formData.time_saved_hours ? parseFloat(formData.time_saved_hours) : null,
        implementation_time_hours: formData.implementation_time_hours ? parseFloat(formData.implementation_time_hours) : null,
        created_from_case: !!linkedCase,
        company_id: 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
      }

      await onSave(solutionData)
      
      // Reset form
      resetForm()

    } catch (error) {
      console.error('Error creating solution:', error)
    }
  }

  const resetForm = () => {
    setFormData({
      title: '',
      description: '',
      solution_type: 'Bug Fix',
      solution_category: 'Technical Support',
      problem_statement: '',
      root_cause: '',
      solution_steps: '',
      implementation_details: '',
      tools_required: '',
      parts_required: '',
      software_versions: '',
      configuration_changes: '',
      testing_performed: '',
      validation_steps: '',
      case_id: '',
      case_number: '',
      customer_name: '',
      contact_person: '',
      product_name: '',
      product_version: '',
      environment: '',
      implementation_time_hours: '',
      difficulty_level: 'Medium',
      success_rate: 'High',
      status: 'Draft',
      effectiveness: 'Effective',
      implemented_by: '',
      verified_by: '',
      approved_by: '',
      implementation_date: '',
      verification_date: '',
      approval_date: '',
      follow_up_required: false,
      follow_up_date: '',
      follow_up_notes: '',
      knowledge_base_entry: true,
      public_solution: false,
      reusable: true,
      cost_involved: '',
      time_saved_hours: '',
      prevention_measures: '',
      lessons_learned: '',
      recommendations: '',
      applicable_products: '',
      applicable_versions: '',
      prerequisites: '',
      warnings: '',
      notes: '',
      version: '1.0',
      superseded_by: '',
      supersedes: ''
    })
  }

  useEffect(() => {
    if (!isOpen && !linkedCase) {
      resetForm()
    }
  }, [isOpen])

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            Create New Solution
            {linkedCase && (
              <Badge variant="outline" className="text-blue-600">
                Linked to {linkedCase.case_number}
              </Badge>
            )}
          </DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Linked Case Info */}
          {linkedCase && (
            <Card className="bg-blue-50 border-blue-200">
              <CardHeader>
                <CardTitle className="text-lg text-blue-800">Linked Case Information</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                  <div><strong>Case Number:</strong> {linkedCase.case_number}</div>
                  <div><strong>Customer:</strong> {linkedCase.customer_name}</div>
                  <div><strong>Title:</strong> {linkedCase.title}</div>
                  <div><strong>Category:</strong> {linkedCase.case_category}</div>
                  <div><strong>Priority:</strong> {linkedCase.priority}</div>
                  <div><strong>Status:</strong> {linkedCase.status}</div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Basic Solution Information */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Solution Information</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label htmlFor="title">Solution Title *</Label>
                <Input
                  id="title"
                  value={formData.title}
                  onChange={(e) => handleInputChange('title', e.target.value)}
                  placeholder="Brief title for the solution"
                  required
                />
              </div>
              
              <div>
                <Label htmlFor="description">Solution Description *</Label>
                <Textarea
                  id="description"
                  value={formData.description}
                  onChange={(e) => handleInputChange('description', e.target.value)}
                  placeholder="Detailed description of the solution"
                  rows={3}
                  required
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <Label htmlFor="solution_type">Solution Type</Label>
                  <Select value={formData.solution_type} onValueChange={(value) => handleInputChange('solution_type', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Bug Fix">Bug Fix</SelectItem>
                      <SelectItem value="Configuration Change">Configuration Change</SelectItem>
                      <SelectItem value="Hardware Replacement">Hardware Replacement</SelectItem>
                      <SelectItem value="Software Update">Software Update</SelectItem>
                      <SelectItem value="Process Improvement">Process Improvement</SelectItem>
                      <SelectItem value="Training">Training</SelectItem>
                      <SelectItem value="Documentation Update">Documentation Update</SelectItem>
                      <SelectItem value="Preventive Measure">Preventive Measure</SelectItem>
                      <SelectItem value="Workaround">Workaround</SelectItem>
                      <SelectItem value="Permanent Fix">Permanent Fix</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label htmlFor="solution_category">Category</Label>
                  <Select value={formData.solution_category} onValueChange={(value) => handleInputChange('solution_category', value)}>
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
                  <Label htmlFor="difficulty_level">Difficulty Level</Label>
                  <Select value={formData.difficulty_level} onValueChange={(value) => handleInputChange('difficulty_level', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Easy">Easy</SelectItem>
                      <SelectItem value="Medium">Medium</SelectItem>
                      <SelectItem value="Hard">Hard</SelectItem>
                      <SelectItem value="Expert">Expert</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Problem Analysis */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Problem Analysis</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label htmlFor="problem_statement">Problem Statement *</Label>
                <Textarea
                  id="problem_statement"
                  value={formData.problem_statement}
                  onChange={(e) => handleInputChange('problem_statement', e.target.value)}
                  placeholder="Clear description of the problem"
                  rows={3}
                  required
                />
              </div>
              
              <div>
                <Label htmlFor="root_cause">Root Cause</Label>
                <Textarea
                  id="root_cause"
                  value={formData.root_cause}
                  onChange={(e) => handleInputChange('root_cause', e.target.value)}
                  placeholder="Identified root cause of the problem"
                  rows={2}
                />
              </div>
            </CardContent>
          </Card>

          {/* Solution Steps */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Solution Implementation</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label htmlFor="solution_steps">Solution Steps *</Label>
                <Textarea
                  id="solution_steps"
                  value={formData.solution_steps}
                  onChange={(e) => handleInputChange('solution_steps', e.target.value)}
                  placeholder="Step-by-step solution process (e.g., STEP 1: ..., STEP 2: ...)"
                  rows={6}
                  required
                />
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="tools_required">Tools Required</Label>
                  <Textarea
                    id="tools_required"
                    value={formData.tools_required}
                    onChange={(e) => handleInputChange('tools_required', e.target.value)}
                    placeholder="List of tools needed"
                    rows={2}
                  />
                </div>
                <div>
                  <Label htmlFor="parts_required">Parts Required</Label>
                  <Textarea
                    id="parts_required"
                    value={formData.parts_required}
                    onChange={(e) => handleInputChange('parts_required', e.target.value)}
                    placeholder="List of parts or components needed"
                    rows={2}
                  />
                </div>
              </div>

              <div>
                <Label htmlFor="testing_performed">Testing & Validation</Label>
                <Textarea
                  id="testing_performed"
                  value={formData.testing_performed}
                  onChange={(e) => handleInputChange('testing_performed', e.target.value)}
                  placeholder="Testing steps and validation performed"
                  rows={3}
                />
              </div>
            </CardContent>
          </Card>

          {/* Customer & Product Info */}
          {!linkedCase && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Customer & Product Information</CardTitle>
              </CardHeader>
              <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="customer_name">Customer Name</Label>
                  <Input
                    id="customer_name"
                    value={formData.customer_name}
                    onChange={(e) => handleInputChange('customer_name', e.target.value)}
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
                  <Label htmlFor="product_name">Product Name</Label>
                  <Input
                    id="product_name"
                    value={formData.product_name}
                    onChange={(e) => handleInputChange('product_name', e.target.value)}
                  />
                </div>
                <div>
                  <Label htmlFor="product_version">Product Version</Label>
                  <Input
                    id="product_version"
                    value={formData.product_version}
                    onChange={(e) => handleInputChange('product_version', e.target.value)}
                  />
                </div>
              </CardContent>
            </Card>
          )}

          {/* Solution Management */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Solution Management</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                  <Label htmlFor="status">Status</Label>
                  <Select value={formData.status} onValueChange={(value) => handleInputChange('status', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Draft">Draft</SelectItem>
                      <SelectItem value="Under Review">Under Review</SelectItem>
                      <SelectItem value="Approved">Approved</SelectItem>
                      <SelectItem value="Implemented">Implemented</SelectItem>
                      <SelectItem value="Tested">Tested</SelectItem>
                      <SelectItem value="Verified">Verified</SelectItem>
                      <SelectItem value="Published">Published</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label htmlFor="effectiveness">Effectiveness</Label>
                  <Select value={formData.effectiveness} onValueChange={(value) => handleInputChange('effectiveness', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Not Effective">Not Effective</SelectItem>
                      <SelectItem value="Partially Effective">Partially Effective</SelectItem>
                      <SelectItem value="Effective">Effective</SelectItem>
                      <SelectItem value="Highly Effective">Highly Effective</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label htmlFor="success_rate">Success Rate</Label>
                  <Select value={formData.success_rate} onValueChange={(value) => handleInputChange('success_rate', value)}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Low">Low</SelectItem>
                      <SelectItem value="Medium">Medium</SelectItem>
                      <SelectItem value="High">High</SelectItem>
                      <SelectItem value="Very High">Very High</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label htmlFor="implemented_by">Implemented By</Label>
                  <Input
                    id="implemented_by"
                    value={formData.implemented_by}
                    onChange={(e) => handleInputChange('implemented_by', e.target.value)}
                    placeholder="Team member name"
                  />
                </div>
              </div>

              <div className="flex items-center space-x-6">
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="knowledge_base_entry"
                    checked={formData.knowledge_base_entry}
                    onCheckedChange={(checked) => handleInputChange('knowledge_base_entry', checked)}
                  />
                  <Label htmlFor="knowledge_base_entry">Add to Knowledge Base</Label>
                </div>
                
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="reusable"
                    checked={formData.reusable}
                    onCheckedChange={(checked) => handleInputChange('reusable', checked)}
                  />
                  <Label htmlFor="reusable">Reusable Solution</Label>
                </div>
                
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="public_solution"
                    checked={formData.public_solution}
                    onCheckedChange={(checked) => handleInputChange('public_solution', checked)}
                  />
                  <Label htmlFor="public_solution">Public Solution</Label>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Additional Information */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Additional Information</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="prevention_measures">Prevention Measures</Label>
                  <Textarea
                    id="prevention_measures"
                    value={formData.prevention_measures}
                    onChange={(e) => handleInputChange('prevention_measures', e.target.value)}
                    placeholder="How to prevent this issue in future"
                    rows={2}
                  />
                </div>
                <div>
                  <Label htmlFor="lessons_learned">Lessons Learned</Label>
                  <Textarea
                    id="lessons_learned"
                    value={formData.lessons_learned}
                    onChange={(e) => handleInputChange('lessons_learned', e.target.value)}
                    placeholder="Key takeaways from this solution"
                    rows={2}
                  />
                </div>
              </div>
              
              <div>
                <Label htmlFor="recommendations">Recommendations</Label>
                <Textarea
                  id="recommendations"
                  value={formData.recommendations}
                  onChange={(e) => handleInputChange('recommendations', e.target.value)}
                  placeholder="Recommendations for similar issues"
                  rows={2}
                />
              </div>
            </CardContent>
          </Card>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit">
              Create Solution
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}