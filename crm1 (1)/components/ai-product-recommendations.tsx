"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
  ShoppingCart, 
  TrendingUp, 
  Package, 
  Zap,
  Star,
  ArrowRight,
  DollarSign,
  Users,
  Target,
  Lightbulb,
  Bot,
  CheckCircle
} from "lucide-react"
import { AIRecommendationService } from "@/lib/ai-services"

interface ProductRecommendation {
  id: string
  name: string
  category: string
  description: string
  price: string
  compatibility: number
  demandScore: number
  crossSellProbability: number
  reason: string
  benefits: string[]
  similarCustomers: number
  avgRating: number
  inStock: boolean
}

interface AIProductRecommendationsProps {
  currentProduct?: string
  customerType?: string
  customerHistory?: string[]
  context?: 'cross-sell' | 'up-sell' | 'replacement' | 'complementary'
}

export function AIProductRecommendations({ 
  currentProduct = "LABORATORY FREEZE DRYER/LYOPHILIZER",
  customerType = "Research Institution",
  customerHistory = [],
  context = 'cross-sell'
}: AIProductRecommendationsProps) {
  const [selectedRecommendation, setSelectedRecommendation] = useState<string>("")
  const [isGenerating, setIsGenerating] = useState(false)
  
  // Generate AI-powered recommendations
  const recommendations: ProductRecommendation[] = [
    {
      id: "REC001",
      name: "Laboratory Freeze Drying Accessories Kit",
      category: "Accessories & Consumables",
      description: "Complete set of vials, stoppers, and shelving systems for freeze dryer optimization",
      price: "₹2,45,000",
      compatibility: 95,
      demandScore: 88,
      crossSellProbability: 92,
      reason: "Commonly purchased with freeze dryers for optimal operation",
      benefits: [
        "Improves freeze-drying efficiency by 25%",
        "Reduces contamination risk",
        "Standardizes sample processing"
      ],
      similarCustomers: 24,
      avgRating: 4.7,
      inStock: true
    },
    {
      id: "REC002", 
      name: "Temperature Monitoring & Control System",
      category: "Monitoring Equipment",
      description: "Advanced temperature monitoring with cloud connectivity and alerts",
      price: "₹1,85,000",
      compatibility: 89,
      demandScore: 82,
      crossSellProbability: 78,
      reason: "Essential for maintaining precise temperature control during freeze drying",
      benefits: [
        "24/7 remote monitoring",
        "Automated alerts and logging",
        "Compliance documentation"
      ],
      similarCustomers: 18,
      avgRating: 4.5,
      inStock: true
    },
    {
      id: "REC003",
      name: "Sample Preparation Workstation",
      category: "Laboratory Furniture",
      description: "Specialized workstation for sample prep before freeze drying process",
      price: "₹3,75,000",
      compatibility: 91,
      demandScore: 75,
      crossSellProbability: 65,
      reason: "Streamlines workflow for freeze-drying operations",
      benefits: [
        "Ergonomic design for efficiency",
        "Integrated storage solutions",
        "Easy to clean surfaces"
      ],
      similarCustomers: 15,
      avgRating: 4.3,
      inStock: false
    },
    {
      id: "REC004",
      name: "Vacuum Pump Upgrade Kit",
      category: "Upgrade Components",
      description: "High-performance vacuum pump with enhanced efficiency and reliability",
      price: "₹4,25,000",
      compatibility: 87,
      demandScore: 79,
      crossSellProbability: 71,
      reason: "Improves vacuum performance and reduces maintenance needs",
      benefits: [
        "40% faster pump-down time",
        "Lower maintenance requirements", 
        "Extended equipment lifecycle"
      ],
      similarCustomers: 12,
      avgRating: 4.6,
      inStock: true
    }
  ]
  
  const generateRecommendations = () => {
    setIsGenerating(true)
    setTimeout(() => {
      setIsGenerating(false)
    }, 1500)
  }
  
  const getScoreColor = (score: number) => {
    if (score >= 85) return "text-green-600 bg-green-50"
    if (score >= 70) return "text-blue-600 bg-blue-50" 
    if (score >= 55) return "text-yellow-600 bg-yellow-50"
    return "text-red-600 bg-red-50"
  }
  
  const getCompatibilityLevel = (score: number) => {
    if (score >= 90) return "Perfect Match"
    if (score >= 80) return "High Compatibility"
    if (score >= 70) return "Good Compatibility"
    return "Moderate Compatibility"
  }
  
  const totalRecommendedValue = recommendations.reduce((sum, rec) => 
    sum + parseFloat(rec.price.replace(/[₹,]/g, '')), 0
  )
  
  const avgCrossSellProbability = recommendations.reduce((sum, rec) => 
    sum + rec.crossSellProbability, 0) / recommendations.length
  
  return (
    <div className="space-y-6">
      <Card className="border-l-4 border-l-indigo-500">
        <CardHeader>
          <CardTitle className="flex items-center">
            <div className="p-2 rounded-lg bg-gradient-to-r from-indigo-500 to-indigo-600 mr-3">
              <Bot className="w-5 h-5 text-white" />
            </div>
            AI Product Recommendations
          </CardTitle>
          <CardDescription>
            Intelligent cross-sell and up-sell suggestions based on customer profile and purchase history
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="flex items-center space-x-2 mb-2">
                <Package className="w-4 h-4 text-indigo-600" />
                <span className="text-sm font-medium">Current Product</span>
              </div>
              <p className="text-sm text-gray-700">{currentProduct}</p>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="flex items-center space-x-2 mb-2">
                <Users className="w-4 h-4 text-indigo-600" />
                <span className="text-sm font-medium">Customer Type</span>
              </div>
              <p className="text-sm text-gray-700">{customerType}</p>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="flex items-center space-x-2 mb-2">
                <Target className="w-4 h-4 text-indigo-600" />
                <span className="text-sm font-medium">Context</span>
              </div>
              <p className="text-sm text-gray-700 capitalize">{context}</p>
            </div>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
            <Card>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-600">Total Opportunity</p>
                    <p className="text-2xl font-bold text-indigo-600">
                      ₹{(totalRecommendedValue / 100000).toFixed(1)}L
                    </p>
                  </div>
                  <DollarSign className="w-8 h-8 text-indigo-600" />
                </div>
              </CardContent>
            </Card>
            
            <Card>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-600">Avg Success Rate</p>
                    <p className="text-2xl font-bold text-green-600">
                      {avgCrossSellProbability.toFixed(0)}%
                    </p>
                  </div>
                  <TrendingUp className="w-8 h-8 text-green-600" />
                </div>
              </CardContent>
            </Card>
            
            <Card>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-600">Recommendations</p>
                    <p className="text-2xl font-bold text-blue-600">
                      {recommendations.length}
                    </p>
                  </div>
                  <Lightbulb className="w-8 h-8 text-blue-600" />
                </div>
              </CardContent>
            </Card>
          </div>
          
          <Button 
            onClick={generateRecommendations}
            disabled={isGenerating}
            className="w-full mb-6"
          >
            {isGenerating ? (
              <>
                <Bot className="w-4 h-4 mr-2 animate-pulse" />
                Generating AI Recommendations...
              </>
            ) : (
              <>
                <Zap className="w-4 h-4 mr-2" />
                Refresh Recommendations
              </>
            )}
          </Button>
        </CardContent>
      </Card>
      
      <Tabs defaultValue="all" className="w-full">
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="all">All Recommendations</TabsTrigger>
          <TabsTrigger value="high">High Priority</TabsTrigger>
          <TabsTrigger value="accessories">Accessories</TabsTrigger>
          <TabsTrigger value="upgrades">Upgrades</TabsTrigger>
        </TabsList>
        
        <TabsContent value="all" className="space-y-4">
          {recommendations.map((rec, index) => (
            <Card key={rec.id} className="overflow-hidden">
              <CardContent className="p-6">
                <div className="flex items-start justify-between mb-4">
                  <div className="flex items-start space-x-4">
                    <div className="w-12 h-12 rounded-lg bg-indigo-100 flex items-center justify-center">
                      <span className="text-lg font-bold text-indigo-600">
                        {index + 1}
                      </span>
                    </div>
                    <div className="flex-1">
                      <div className="flex items-center space-x-2 mb-1">
                        <h3 className="font-semibold text-lg">{rec.name}</h3>
                        {!rec.inStock && (
                          <Badge variant="outline" className="text-red-600 border-red-200">
                            Out of Stock
                          </Badge>
                        )}
                      </div>
                      <p className="text-gray-600 text-sm mb-2">{rec.description}</p>
                      <div className="flex items-center space-x-4 text-sm">
                        <Badge variant="outline">{rec.category}</Badge>
                        <div className="flex items-center space-x-1">
                          <Star className="w-3 h-3 text-yellow-500" />
                          <span>{rec.avgRating}/5</span>
                        </div>
                        <span className="text-gray-500">
                          {rec.similarCustomers} similar customers
                        </span>
                      </div>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="text-2xl font-bold text-gray-900">{rec.price}</p>
                  </div>
                </div>
                
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span>Compatibility</span>
                      <span className={`font-medium ${getScoreColor(rec.compatibility)}`}>
                        {rec.compatibility}%
                      </span>
                    </div>
                    <Progress value={rec.compatibility} className="h-2" />
                    <p className="text-xs text-gray-500">
                      {getCompatibilityLevel(rec.compatibility)}
                    </p>
                  </div>
                  
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span>Demand Score</span>
                      <span className={`font-medium ${getScoreColor(rec.demandScore)}`}>
                        {rec.demandScore}%
                      </span>
                    </div>
                    <Progress value={rec.demandScore} className="h-2" />
                    <p className="text-xs text-gray-500">Market demand</p>
                  </div>
                  
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span>Success Rate</span>
                      <span className={`font-medium ${getScoreColor(rec.crossSellProbability)}`}>
                        {rec.crossSellProbability}%
                      </span>
                    </div>
                    <Progress value={rec.crossSellProbability} className="h-2" />
                    <p className="text-xs text-gray-500">Cross-sell probability</p>
                  </div>
                </div>
                
                <div className="mb-4 p-3 bg-blue-50 rounded-lg">
                  <div className="flex items-start space-x-2">
                    <Lightbulb className="w-4 h-4 text-blue-600 mt-0.5" />
                    <div>
                      <p className="text-sm font-medium text-blue-800 mb-1">AI Recommendation Reason</p>
                      <p className="text-sm text-blue-700">{rec.reason}</p>
                    </div>
                  </div>
                </div>
                
                <div className="space-y-2 mb-4">
                  <h4 className="text-sm font-medium flex items-center">
                    <CheckCircle className="w-4 h-4 mr-1 text-green-600" />
                    Key Benefits
                  </h4>
                  <ul className="space-y-1">
                    {rec.benefits.map((benefit, idx) => (
                      <li key={idx} className="text-sm text-gray-700 flex items-start">
                        <span className="text-green-500 mr-2">•</span>
                        {benefit}
                      </li>
                    ))}
                  </ul>
                </div>
                
                <div className="flex items-center justify-between pt-4 border-t">
                  <div className="text-sm text-gray-500">
                    AI Confidence: {rec.crossSellProbability}%
                  </div>
                  <div className="flex space-x-2">
                    <Button variant="outline" size="sm">
                      <Package className="w-3 h-3 mr-1" />
                      View Details
                    </Button>
                    <Button 
                      size="sm"
                      disabled={!rec.inStock}
                      className={rec.inStock ? "" : "opacity-50 cursor-not-allowed"}
                    >
                      <ShoppingCart className="w-3 h-3 mr-1" />
                      Add to Quote
                      <ArrowRight className="w-3 h-3 ml-1" />
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </TabsContent>
        
        <TabsContent value="high" className="space-y-4">
          {recommendations
            .filter(rec => rec.crossSellProbability >= 80)
            .map((rec, index) => (
            <Card key={rec.id}>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <div className="w-8 h-8 rounded bg-red-100 flex items-center justify-center">
                      <Star className="w-4 h-4 text-red-600" />
                    </div>
                    <div>
                      <h3 className="font-medium">{rec.name}</h3>
                      <p className="text-sm text-gray-600">{rec.price}</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-2">
                    <Badge className="bg-red-500">
                      {rec.crossSellProbability}% Success Rate
                    </Badge>
                    <Button size="sm">Add to Quote</Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </TabsContent>
        
        <TabsContent value="accessories" className="space-y-4">
          {recommendations
            .filter(rec => rec.category.includes('Accessories') || rec.category.includes('Consumables'))
            .map((rec) => (
            <Card key={rec.id}>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="font-medium">{rec.name}</h3>
                    <p className="text-sm text-gray-600">{rec.description}</p>
                    <p className="text-sm font-medium">{rec.price}</p>
                  </div>
                  <Button size="sm">
                    <Package className="w-3 h-3 mr-1" />
                    Add Accessory
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </TabsContent>
        
        <TabsContent value="upgrades" className="space-y-4">
          {recommendations
            .filter(rec => rec.category.includes('Upgrade'))
            .map((rec) => (
            <Card key={rec.id}>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="font-medium">{rec.name}</h3>
                    <p className="text-sm text-gray-600">{rec.description}</p>
                    <p className="text-sm font-medium">{rec.price}</p>
                  </div>
                  <Button size="sm">
                    <TrendingUp className="w-3 h-3 mr-1" />
                    Upgrade Now
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </TabsContent>
      </Tabs>
    </div>
  )
}
