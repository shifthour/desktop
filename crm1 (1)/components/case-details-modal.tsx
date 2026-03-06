"use client"

import { useState } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Separator } from "@/components/ui/separator"
import { 
  AlertCircle, Clock, CheckCircle, User, Building, Package, Phone, Mail,
  Lightbulb, FileText, Settings, Calendar, Target, AlertTriangle
} from "lucide-react"

interface CaseDetailsModalProps {
  isOpen: boolean
  onClose: () => void
  caseData: any
  onCreateSolution: () => void
}

export function CaseDetailsModal({
  isOpen,
  onClose,
  caseData,
  onCreateSolution
}: CaseDetailsModalProps) {
  const [activeTab, setActiveTab] = useState("details")

  if (!caseData) return null

  // Debug: Log the case data to see if solution is present
  console.log('Case data in modal:', caseData)
  console.log('Solution field:', caseData.solution)

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "open": return "bg-blue-100 text-blue-800"
      case "in progress": return "bg-yellow-100 text-yellow-800"
      case "resolved": return "bg-green-100 text-green-800"
      case "closed": return "bg-gray-100 text-gray-800"
      case "escalated": return "bg-red-100 text-red-800"
      default: return "bg-gray-100 text-gray-800"
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority?.toLowerCase()) {
      case "low": return "bg-green-100 text-green-800"
      case "medium": return "bg-yellow-100 text-yellow-800"
      case "high": return "bg-orange-100 text-orange-800"
      case "critical": return "bg-red-100 text-red-800"
      default: return "bg-gray-100 text-gray-800"
    }
  }

  const formatDate = (dateString: string) => {
    if (!dateString) return 'N/A'
    return new Date(dateString).toLocaleDateString('en-IN', {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    })
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-5xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-3">
            <AlertCircle className="w-6 h-6" />
            Case Details: {caseData.case_number}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-6">
          {/* Case Header */}
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-start justify-between">
                <div className="space-y-2">
                  <h2 className="text-xl font-semibold">{caseData.title}</h2>
                  <div className="flex items-center gap-2">
                    <Badge className={getStatusColor(caseData.status)}>
                      {caseData.status}
                    </Badge>
                    <Badge className={getPriorityColor(caseData.priority)}>
                      {caseData.priority} Priority
                    </Badge>
                    <Badge variant="outline">
                      {caseData.case_category}
                    </Badge>
                    {caseData.severity && (
                      <Badge className={getPriorityColor(caseData.severity)}>
                        {caseData.severity} Severity
                      </Badge>
                    )}
                  </div>
                </div>
                <div className="text-right text-sm text-gray-600">
                  <div>Created: {formatDate(caseData.case_date)}</div>
                  <div>Case #: {caseData.case_number}</div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Tabs for detailed information */}
          <Tabs value={activeTab} onValueChange={setActiveTab}>
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="details">Case Details</TabsTrigger>
              <TabsTrigger value="technical">Technical Info</TabsTrigger>
              <TabsTrigger value="solutions">
                Solutions ({caseData.relatedSolutions?.length || 0})
              </TabsTrigger>
              <TabsTrigger value="related">Related Records</TabsTrigger>
            </TabsList>

            {/* Case Details Tab */}
            <TabsContent value="details" className="space-y-4">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                {/* Customer Information */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Building className="w-5 h-5" />
                      Customer Information
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex items-center gap-2">
                      <Building className="w-4 h-4 text-gray-400" />
                      <span className="font-medium">{caseData.customer_name || 'N/A'}</span>
                    </div>
                    {caseData.contact_person && (
                      <div className="flex items-center gap-2">
                        <User className="w-4 h-4 text-gray-400" />
                        <span>{caseData.contact_person}</span>
                      </div>
                    )}
                    {caseData.contact_phone && (
                      <div className="flex items-center gap-2">
                        <Phone className="w-4 h-4 text-gray-400" />
                        <span>{caseData.contact_phone}</span>
                      </div>
                    )}
                    {caseData.contact_email && (
                      <div className="flex items-center gap-2">
                        <Mail className="w-4 h-4 text-gray-400" />
                        <span>{caseData.contact_email}</span>
                      </div>
                    )}
                  </CardContent>
                </Card>

                {/* Product Information */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Package className="w-5 h-5" />
                      Product Information
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    {caseData.product_name && (
                      <div className="flex items-center gap-2">
                        <Package className="w-4 h-4 text-gray-400" />
                        <span>{caseData.product_name}</span>
                      </div>
                    )}
                    {caseData.serial_number && (
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-400 w-4">S/N:</span>
                        <span>{caseData.serial_number}</span>
                      </div>
                    )}
                    {caseData.model_number && (
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-400 w-4">Model:</span>
                        <span>{caseData.model_number}</span>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>

              {/* Description */}
              <Card>
                <CardHeader>
                  <CardTitle>Case Description</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-gray-700 whitespace-pre-wrap">
                    {caseData.description || 'No description provided'}
                  </p>
                </CardContent>
              </Card>

              {/* Solution */}
              {caseData.solution && caseData.solution.trim() !== '' && (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <CheckCircle className="w-5 h-5 text-green-600" />
                      Solution Provided
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-gray-700 whitespace-pre-wrap">
                      {caseData.solution}
                    </p>
                  </CardContent>
                </Card>
              )}
              
              {/* Debug: Always show solution debug info */}
              <Card style={{border: '2px solid red'}}>
                <CardHeader>
                  <CardTitle>DEBUG: Solution Data</CardTitle>
                </CardHeader>
                <CardContent>
                  <p><strong>Solution exists:</strong> {caseData.solution ? 'YES' : 'NO'}</p>
                  <p><strong>Solution value:</strong> "{caseData.solution}"</p>
                  <p><strong>Solution type:</strong> {typeof caseData.solution}</p>
                  <p><strong>Solution length:</strong> {caseData.solution ? caseData.solution.length : 'N/A'}</p>
                </CardContent>
              </Card>

              {/* Assignment & Status */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Settings className="w-5 h-5" />
                    Assignment & Status
                  </CardTitle>
                </CardHeader>
                <CardContent className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div>
                    <Label className="text-sm font-medium text-gray-600">Assigned To</Label>
                    <p className="mt-1">{caseData.assigned_to || 'Unassigned'}</p>
                  </div>
                  <div>
                    <Label className="text-sm font-medium text-gray-600">Issue Type</Label>
                    <p className="mt-1">{caseData.issue_type || 'N/A'}</p>
                  </div>
                  <div>
                    <Label className="text-sm font-medium text-gray-600">Category</Label>
                    <p className="mt-1">{caseData.case_category || 'N/A'}</p>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* Technical Information Tab */}
            <TabsContent value="technical" className="space-y-4">
              {/* Symptoms & Environment */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <AlertTriangle className="w-5 h-5" />
                      Symptoms
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-gray-700 whitespace-pre-wrap">
                      {caseData.symptoms || 'No symptoms recorded'}
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Settings className="w-5 h-5" />
                      Environment Details
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-gray-700 whitespace-pre-wrap">
                      {caseData.environment_details || 'No environment details recorded'}
                    </p>
                  </CardContent>
                </Card>
              </div>

              {/* Error Messages */}
              {caseData.error_messages && (
                <Card>
                  <CardHeader>
                    <CardTitle>Error Messages</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <pre className="bg-gray-100 p-3 rounded text-sm overflow-x-auto">
                      {caseData.error_messages}
                    </pre>
                  </CardContent>
                </Card>
              )}

              {/* Reproduction Steps */}
              {caseData.reproduction_steps && (
                <Card>
                  <CardHeader>
                    <CardTitle>Reproduction Steps</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-gray-700 whitespace-pre-wrap">
                      {caseData.reproduction_steps}
                    </p>
                  </CardContent>
                </Card>
              )}

              {/* Customer Impact & Workaround */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                {caseData.customer_impact && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Customer Impact</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <p className="text-gray-700 whitespace-pre-wrap">
                        {caseData.customer_impact}
                      </p>
                    </CardContent>
                  </Card>
                )}

                {caseData.workaround && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Workaround</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <p className="text-gray-700 whitespace-pre-wrap">
                        {caseData.workaround}
                      </p>
                    </CardContent>
                  </Card>
                )}
              </div>
            </TabsContent>

            {/* Solutions Tab */}
            <TabsContent value="solutions" className="space-y-4">
              {caseData.relatedSolutions && caseData.relatedSolutions.length > 0 ? (
                <div className="space-y-4">
                  {caseData.relatedSolutions.map((solution: any) => (
                    <Card key={solution.id}>
                      <CardHeader>
                        <div className="flex items-center justify-between">
                          <CardTitle className="flex items-center gap-2">
                            <Lightbulb className="w-5 h-5" />
                            {solution.solution_number}: {solution.title}
                          </CardTitle>
                          <div className="flex gap-2">
                            <Badge className="bg-blue-100 text-blue-800">
                              {solution.status}
                            </Badge>
                            {solution.reusable && (
                              <Badge className="bg-green-100 text-green-800">
                                Reusable
                              </Badge>
                            )}
                          </div>
                        </div>
                      </CardHeader>
                      <CardContent>
                        <p className="text-gray-700 mb-3">{solution.description}</p>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                          <div>
                            <span className="font-medium">Type:</span> {solution.solution_type}
                          </div>
                          <div>
                            <span className="font-medium">Difficulty:</span> {solution.difficulty_level}
                          </div>
                          <div>
                            <span className="font-medium">Effectiveness:</span> {solution.effectiveness}
                          </div>
                        </div>
                        {solution.solution_steps && (
                          <div className="mt-3">
                            <span className="font-medium">Solution Steps:</span>
                            <pre className="mt-1 bg-gray-50 p-2 rounded text-sm whitespace-pre-wrap">
                              {solution.solution_steps}
                            </pre>
                          </div>
                        )}
                      </CardContent>
                    </Card>
                  ))}
                </div>
              ) : (
                <Card>
                  <CardContent className="pt-6">
                    <div className="text-center py-8">
                      <Lightbulb className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                      <p className="text-gray-500 mb-4">No solutions found for this case</p>
                      <Button onClick={onCreateSolution}>
                        <Lightbulb className="w-4 h-4 mr-2" />
                        Create Solution
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>

            {/* Related Records Tab */}
            <TabsContent value="related" className="space-y-4">
              {/* Related Complaints */}
              {caseData.relatedComplaints && caseData.relatedComplaints.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <AlertCircle className="w-5 h-5" />
                      Related Complaints
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      {caseData.relatedComplaints.map((complaint: any) => (
                        <div key={complaint.id} className="flex items-center justify-between p-3 border rounded">
                          <div>
                            <div className="font-medium">{complaint.complaint_number}</div>
                            <div className="text-sm text-gray-600">{complaint.subject}</div>
                            <div className="text-xs text-gray-500">{complaint.account_name}</div>
                          </div>
                          <Badge className={getStatusColor(complaint.status)}>
                            {complaint.status}
                          </Badge>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Case Notes */}
              {caseData.internal_notes && (
                <Card>
                  <CardHeader>
                    <CardTitle>Internal Notes</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-gray-700 whitespace-pre-wrap">
                      {caseData.internal_notes}
                    </p>
                  </CardContent>
                </Card>
              )}

              {/* No related records */}
              {(!caseData.relatedComplaints || caseData.relatedComplaints.length === 0) && !caseData.internal_notes && (
                <Card>
                  <CardContent className="pt-6">
                    <div className="text-center py-8">
                      <FileText className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                      <p className="text-gray-500">No related records found</p>
                    </div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>
          </Tabs>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Close
          </Button>
          {(!caseData.relatedSolutions || caseData.relatedSolutions.length === 0) && (
            <Button onClick={onCreateSolution}>
              <Lightbulb className="w-4 h-4 mr-2" />
              Create Solution
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// Helper component for labels
function Label({ children, className = "" }: { children: React.ReactNode, className?: string }) {
  return <span className={`block text-sm font-medium text-gray-700 ${className}`}>{children}</span>
}