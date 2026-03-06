"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Progress } from "@/components/ui/progress"
import { 
  Brain, 
  AlertTriangle, 
  CheckCircle, 
  Clock, 
  Lightbulb,
  Search,
  Wrench,
  User,
  MessageSquare,
  ArrowRight,
  Zap
} from "lucide-react"
import { ComplaintData } from "@/lib/ai-services"

interface AIComplaintResolutionProps {
  complaint?: ComplaintData
  onResolutionGenerated?: (resolution: string) => void
}

interface ResolutionSuggestion {
  id: string
  title: string
  description: string
  steps: string[]
  estimatedTime: string
  confidence: number
  category: 'technical' | 'warranty' | 'training' | 'replacement'
  priority: 'low' | 'medium' | 'high' | 'critical'
}

interface KnowledgeBaseEntry {
  id: string
  title: string
  issue: string
  solution: string
  relevanceScore: number
  category: string
}

export function AIComplaintResolution({ complaint, onResolutionGenerated }: AIComplaintResolutionProps) {
  const [isAnalyzing, setIsAnalyzing] = useState(false)
  const [resolutionSuggestions, setResolutionSuggestions] = useState<ResolutionSuggestion[]>([])
  const [selectedResolution, setSelectedResolution] = useState<string>("")
  const [customDescription, setCustomDescription] = useState("")
  const [knowledgeBaseResults, setKnowledgeBaseResults] = useState<KnowledgeBaseEntry[]>([])
  
  const analyzeComplaint = () => {
    setIsAnalyzing(true)
    
    // Simulate AI analysis
    setTimeout(() => {
      const suggestions = generateResolutionSuggestions(complaint)
      const kbResults = searchKnowledgeBase(complaint)
      
      setResolutionSuggestions(suggestions)
      setKnowledgeBaseResults(kbResults)
      setIsAnalyzing(false)
    }, 2000)
  }
  
  const generateResolutionSuggestions = (complaint?: ComplaintData): ResolutionSuggestion[] => {
    if (!complaint) return []
    
    const baseResolutions: ResolutionSuggestion[] = []
    
    // Analyze complaint type and generate appropriate suggestions
    if (complaint.productService.toLowerCase().includes('spectrophotometer')) {
      baseResolutions.push({
        id: '1',
        title: 'Calibration and Maintenance',
        description: 'Standard calibration procedure for spectrophotometer issues',
        steps: [
          'Check lamp intensity and wavelength accuracy',
          'Clean cuvette holder and optical path',
          'Run calibration with reference standards',
          'Update software and firmware if needed',
          'Provide refresher training to operators'
        ],
        estimatedTime: '2-3 hours',
        confidence: 85,
        category: 'technical',
        priority: 'high'
      })
      
      baseResolutions.push({
        id: '2',
        title: 'Remote Diagnostics',
        description: 'Perform remote system diagnostics and configuration',
        steps: [
          'Connect to instrument via remote access',
          'Run comprehensive system diagnostics',
          'Check error logs and performance data',
          'Configure optimal settings for lab environment',
          'Schedule follow-up monitoring'
        ],
        estimatedTime: '1 hour',
        confidence: 78,
        category: 'technical',
        priority: 'medium'
      })
    }
    
    if (complaint.productService.toLowerCase().includes('autoclave')) {
      baseResolutions.push({
        id: '3',
        title: 'Steam System Inspection',
        description: 'Comprehensive steam generation and circulation check',
        steps: [
          'Inspect steam generator and heating elements',
          'Check door seals and gaskets for integrity',
          'Verify pressure sensors and safety systems',
          'Test sterilization cycles with biological indicators',
          'Replace consumable parts if necessary'
        ],
        estimatedTime: '3-4 hours',
        confidence: 92,
        category: 'technical',
        priority: 'high'
      })
    }
    
    if (complaint.productService.toLowerCase().includes('freeze dryer')) {
      baseResolutions.push({
        id: '4',
        title: 'Vacuum System Service',
        description: 'Complete vacuum pump and chamber maintenance',
        steps: [
          'Service vacuum pump and check oil levels',
          'Inspect vacuum chamber for leaks',
          'Calibrate temperature and pressure sensors',
          'Test freeze-drying cycles with sample loads',
          'Update control software parameters'
        ],
        estimatedTime: '4-6 hours',
        confidence: 88,
        category: 'technical',
        priority: 'critical'
      })
    }
    
    // Add warranty-based resolutions
    if (complaint.complaintType === 'Under Warranty') {
      baseResolutions.push({
        id: '5',
        title: 'Warranty Replacement',
        description: 'Process warranty claim for component replacement',
        steps: [
          'Verify warranty coverage and terms',
          'Document issue with photos and diagnostics',
          'Order replacement parts from manufacturer',
          'Schedule installation by certified technician',
          'Extend warranty period for replaced components'
        ],
        estimatedTime: '5-7 business days',
        confidence: 95,
        category: 'warranty',
        priority: 'high'
      })
    }
    
    // Add training-based resolutions
    baseResolutions.push({
      id: '6',
      title: 'Operator Training Session',
      description: 'Comprehensive training on proper equipment usage',
      steps: [
        'Assess current operator knowledge and practices',
        'Conduct hands-on training session',
        'Provide updated operation manuals and SOPs',
        'Set up preventive maintenance schedule',
        'Schedule follow-up training in 3 months'
      ],
      estimatedTime: '2-3 hours',
      confidence: 82,
      category: 'training',
      priority: 'medium'
    })
    
    return baseResolutions
  }
  
  const searchKnowledgeBase = (complaint?: ComplaintData): KnowledgeBaseEntry[] => {
    if (!complaint) return []
    
    return [
      {
        id: 'kb1',
        title: 'Common Spectrophotometer Issues',
        issue: 'Unstable readings and drift in measurements',
        solution: 'Clean optical components and recalibrate with certified standards',
        relevanceScore: 94,
        category: 'Technical'
      },
      {
        id: 'kb2',
        title: 'Autoclave Door Seal Problems',
        issue: 'Steam leakage from door area during sterilization',
        solution: 'Replace door gasket and lubricate sealing surfaces',
        relevanceScore: 89,
        category: 'Maintenance'
      },
      {
        id: 'kb3',
        title: 'Freeze Dryer Vacuum Issues',
        issue: 'Unable to achieve required vacuum levels',
        solution: 'Service vacuum pump, check for leaks, replace pump oil',
        relevanceScore: 91,
        category: 'Technical'
      }
    ]
  }
  
  const getCategoryIcon = (category: string) => {
    switch (category) {
      case 'technical': return <Wrench className="w-4 h-4" />
      case 'warranty': return <CheckCircle className="w-4 h-4" />
      case 'training': return <User className="w-4 h-4" />
      case 'replacement': return <ArrowRight className="w-4 h-4" />
      default: return <Lightbulb className="w-4 h-4" />
    }
  }
  
  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'critical': return 'bg-red-100 text-red-800 border-red-200'
      case 'high': return 'bg-orange-100 text-orange-800 border-orange-200'
      case 'medium': return 'bg-blue-100 text-blue-800 border-blue-200'
      case 'low': return 'bg-gray-100 text-gray-800 border-gray-200'
      default: return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }
  
  return (
    <div className="space-y-6">
      <Card className="border-l-4 border-l-orange-500">
        <CardHeader>
          <CardTitle className="flex items-center">
            <div className="p-2 rounded-lg bg-gradient-to-r from-orange-500 to-orange-600 mr-3">
              <Brain className="w-5 h-5 text-white" />
            </div>
            AI Complaint Resolution Assistant
          </CardTitle>
          <CardDescription>
            Intelligent analysis and resolution suggestions for customer complaints
          </CardDescription>
        </CardHeader>
        <CardContent>
          {complaint && (
            <div className="p-4 bg-gray-50 rounded-lg space-y-2 mb-4">
              <div className="flex items-center justify-between">
                <h4 className="font-medium">Complaint Details</h4>
                <Badge className={getPriorityColor(complaint.severity.toLowerCase())}>
                  {complaint.severity} Severity
                </Badge>
              </div>
              <div className="grid grid-cols-2 gap-2 text-sm">
                <div><span className="font-medium">ID:</span> {complaint.id}</div>
                <div><span className="font-medium">Account:</span> {complaint.accountName}</div>
                <div><span className="font-medium">Product:</span> {complaint.productService}</div>
                <div><span className="font-medium">Type:</span> {complaint.complaintType}</div>
              </div>
              <div className="mt-2">
                <span className="font-medium text-sm">Description:</span>
                <p className="text-sm text-gray-600 mt-1">{complaint.description}</p>
              </div>
            </div>
          )}
          
          <div className="flex space-x-2">
            <Button 
              onClick={analyzeComplaint}
              disabled={isAnalyzing}
              className="flex-1"
            >
              {isAnalyzing ? (
                <>
                  <Brain className="w-4 h-4 mr-2 animate-pulse" />
                  Analyzing Complaint...
                </>
              ) : (
                <>
                  <Zap className="w-4 h-4 mr-2" />
                  Generate AI Resolution
                </>
              )}
            </Button>
            <Button variant="outline">
              <Search className="w-4 h-4 mr-2" />
              Search KB
            </Button>
          </div>
        </CardContent>
      </Card>
      
      {resolutionSuggestions.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center">
              <Lightbulb className="w-5 h-5 mr-2 text-yellow-600" />
              AI Resolution Suggestions
            </CardTitle>
            <CardDescription>
              Ranked recommendations based on complaint analysis
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {resolutionSuggestions.map((suggestion) => (
              <div key={suggestion.id} className="border rounded-lg p-4 space-y-3">
                <div className="flex items-start justify-between">
                  <div className="flex items-center space-x-2">
                    <div className="p-1.5 rounded bg-blue-100 text-blue-600">
                      {getCategoryIcon(suggestion.category)}
                    </div>
                    <div>
                      <h4 className="font-medium">{suggestion.title}</h4>
                      <p className="text-sm text-gray-600">{suggestion.description}</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-2">
                    <Badge className={getPriorityColor(suggestion.priority)}>
                      {suggestion.priority.toUpperCase()}
                    </Badge>
                    <div className="text-right text-xs">
                      <div className="text-gray-500">Confidence</div>
                      <div className="font-medium">{suggestion.confidence}%</div>
                    </div>
                  </div>
                </div>
                
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-sm">
                    <span className="font-medium">Resolution Steps:</span>
                    <div className="flex items-center space-x-1 text-gray-500">
                      <Clock className="w-3 h-3" />
                      <span>{suggestion.estimatedTime}</span>
                    </div>
                  </div>
                  <ol className="text-sm space-y-1 ml-4">
                    {suggestion.steps.map((step, index) => (
                      <li key={index} className="flex items-start">
                        <span className="text-blue-600 font-medium mr-2">{index + 1}.</span>
                        <span className="text-gray-700">{step}</span>
                      </li>
                    ))}
                  </ol>
                </div>
                
                <div className="flex items-center justify-between pt-2 border-t">
                  <Progress value={suggestion.confidence} className="w-24 h-2" />
                  <div className="flex space-x-2">
                    <Button size="sm" variant="outline">
                      <MessageSquare className="w-3 h-3 mr-1" />
                      Add Note
                    </Button>
                    <Button size="sm">
                      Apply Solution
                    </Button>
                  </div>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      )}
      
      {knowledgeBaseResults.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center">
              <Search className="w-5 h-5 mr-2 text-green-600" />
              Knowledge Base Results
            </CardTitle>
            <CardDescription>
              Similar issues and proven solutions from knowledge base
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {knowledgeBaseResults.map((entry) => (
              <div key={entry.id} className="border rounded-lg p-3">
                <div className="flex items-start justify-between mb-2">
                  <h4 className="font-medium text-sm">{entry.title}</h4>
                  <div className="flex items-center space-x-2">
                    <Badge variant="outline" className="text-xs">
                      {entry.category}
                    </Badge>
                    <div className="text-xs text-green-600 font-medium">
                      {entry.relevanceScore}% match
                    </div>
                  </div>
                </div>
                <div className="space-y-2 text-sm">
                  <div>
                    <span className="font-medium text-red-600">Issue:</span>
                    <p className="text-gray-600">{entry.issue}</p>
                  </div>
                  <div>
                    <span className="font-medium text-green-600">Solution:</span>
                    <p className="text-gray-700">{entry.solution}</p>
                  </div>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      )}
      
      <Card>
        <CardHeader>
          <CardTitle>Custom Resolution Notes</CardTitle>
          <CardDescription>
            Add additional context or custom resolution steps
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <Textarea
            value={customDescription}
            onChange={(e) => setCustomDescription(e.target.value)}
            placeholder="Add custom resolution notes, special considerations, or additional steps..."
            className="min-h-[120px]"
          />
          <div className="flex justify-between">
            <div className="text-xs text-gray-500 flex items-center space-x-4">
              <div className="flex items-center space-x-1">
                <Brain className="w-3 h-3" />
                <span>AI-powered suggestions</span>
              </div>
              <div className="flex items-center space-x-1">
                <CheckCircle className="w-3 h-3" />
                <span>Knowledge base verified</span>
              </div>
            </div>
            <Button>
              Save Resolution Plan
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
