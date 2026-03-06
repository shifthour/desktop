"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Badge } from "@/components/ui/badge"
import { 
  Mail, 
  Wand2, 
  Copy, 
  Send, 
  RefreshCw,
  Sparkles,
  Clock,
  Target
} from "lucide-react"
import { AIEmailService, LeadData } from "@/lib/ai-services"

interface AIEmailGeneratorProps {
  lead?: LeadData
  customerName?: string
  productName?: string
  context?: 'follow-up' | 'quotation' | 'maintenance' | 'general'
}

export function AIEmailGenerator({ 
  lead, 
  customerName, 
  productName, 
  context = 'follow-up' 
}: AIEmailGeneratorProps) {
  const [emailContent, setEmailContent] = useState("")
  const [selectedTemplate, setSelectedTemplate] = useState("auto")
  const [isGenerating, setIsGenerating] = useState(false)
  const [copied, setCopied] = useState(false)
  
  const generateEmail = () => {
    setIsGenerating(true)
    
    setTimeout(() => {
      if (lead) {
        const generated = AIEmailService.generateFollowUpEmail(lead)
        setEmailContent(generated)
      } else if (customerName && productName && context === 'maintenance') {
        const generated = AIEmailService.generateMaintenanceReminder(customerName, productName, "6 months ago")
        setEmailContent(generated)
      } else {
        setEmailContent("AI email generation requires lead data or customer details.")
      }
      setIsGenerating(false)
    }, 1500)
  }
  
  const copyToClipboard = () => {
    navigator.clipboard.writeText(emailContent)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }
  
  const emailTemplates = [
    { value: "auto", label: "Auto-detect", description: "AI chooses best template" },
    { value: "prospecting", label: "Initial Contact", description: "First touchpoint with lead" },
    { value: "qualified", label: "Qualified Follow-up", description: "Post-qualification engagement" },
    { value: "proposal", label: "Proposal Follow-up", description: "After sending quotation" },
    { value: "maintenance", label: "Maintenance Reminder", description: "Service and support" }
  ]
  
  return (
    <Card className="border-l-4 border-l-blue-500">
      <CardHeader>
        <CardTitle className="flex items-center">
          <div className="p-2 rounded-lg bg-gradient-to-r from-blue-500 to-blue-600 mr-3">
            <Wand2 className="w-5 h-5 text-white" />
          </div>
          AI Email Generator
        </CardTitle>
        <CardDescription>
          Generate personalized emails using AI based on customer context and sales stage
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Template Selection */}
        <div className="space-y-2">
          <label className="text-sm font-medium">Email Template</label>
          <Select value={selectedTemplate} onValueChange={setSelectedTemplate}>
            <SelectTrigger>
              <SelectValue placeholder="Select template type" />
            </SelectTrigger>
            <SelectContent>
              {emailTemplates.map((template) => (
                <SelectItem key={template.value} value={template.value}>
                  <div className="flex flex-col">
                    <span>{template.label}</span>
                    <span className="text-xs text-gray-500">{template.description}</span>
                  </div>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        
        {/* Context Information */}
        {lead && (
          <div className="p-3 bg-gray-50 rounded-lg space-y-2">
            <div className="flex items-center space-x-2">
              <Target className="w-4 h-4 text-blue-600" />
              <span className="text-sm font-medium">Context Information</span>
            </div>
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div>
                <span className="font-medium">Lead:</span> {lead.leadName}
              </div>
              <div>
                <span className="font-medium">Contact:</span> {lead.contactName}
              </div>
              <div>
                <span className="font-medium">Product:</span> {lead.product}
              </div>
              <div>
                <span className="font-medium">Stage:</span>
                <Badge variant="outline" className="ml-1 text-xs">
                  {lead.salesStage}
                </Badge>
              </div>
            </div>
          </div>
        )}
        
        {/* Generate Button */}
        <Button 
          onClick={generateEmail}
          disabled={isGenerating}
          className="w-full"
        >
          {isGenerating ? (
            <>
              <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
              Generating AI Email...
            </>
          ) : (
            <>
              <Sparkles className="w-4 h-4 mr-2" />
              Generate Personalized Email
            </>
          )}
        </Button>
        
        {/* Generated Email Content */}
        {emailContent && (
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">Generated Email</label>
              <div className="flex items-center space-x-2">
                <Badge variant="outline" className="text-xs">
                  <Clock className="w-3 h-3 mr-1" />
                  Generated now
                </Badge>
                <Button 
                  variant="outline" 
                  size="sm"
                  onClick={copyToClipboard}
                >
                  {copied ? (
                    <>
                      <span className="text-green-600">Copied!</span>
                    </>
                  ) : (
                    <>
                      <Copy className="w-3 h-3 mr-1" />
                      Copy
                    </>
                  )}
                </Button>
              </div>
            </div>
            
            <Textarea
              value={emailContent}
              onChange={(e) => setEmailContent(e.target.value)}
              className="min-h-[300px] font-mono text-sm"
              placeholder="Generated email will appear here..."
            />
            
            <div className="flex space-x-2">
              <Button className="flex-1">
                <Mail className="w-4 h-4 mr-2" />
                Send Email
              </Button>
              <Button variant="outline" onClick={generateEmail}>
                <RefreshCw className="w-4 h-4 mr-2" />
                Regenerate
              </Button>
            </div>
          </div>
        )}
        
        {/* AI Features Info */}
        <div className="p-3 bg-blue-50 rounded-lg">
          <div className="flex items-start space-x-2">
            <Sparkles className="w-4 h-4 text-blue-600 mt-0.5" />
            <div className="text-xs text-blue-700">
              <p className="font-medium mb-1">AI-Powered Features:</p>
              <ul className="space-y-1">
                <li>• Personalized content based on customer context</li>
                <li>• Sales stage-appropriate messaging</li>
                <li>• Product-specific technical details</li>
                <li>• Professional tone and formatting</li>
              </ul>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
