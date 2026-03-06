"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { 
  Users, Package, AlertTriangle, Calendar, Coins, Target, Plus, ArrowRight, Brain, Sparkles, 
  Building2, Handshake, TrendingUp, TrendingDown, Activity, Eye, Edit, Phone, Mail, MessageSquare,
  MapPin, Star, Clock, ChevronUp, ChevronDown, Filter, Search, Zap
} from "lucide-react"

// Enhanced AI Dashboard based on PDF recommendations
export function DashboardContent() {
  const [mounted, setMounted] = useState(false)
  const [currentTime, setCurrentTime] = useState(new Date())
  const [aiChatOpen, setAiChatOpen] = useState(false)
  const [currentUser, setCurrentUser] = useState<any>(null)

  useEffect(() => {
    setMounted(true)
    const timer = setInterval(() => setCurrentTime(new Date()), 1000)
    
    // Load current user from localStorage
    const storedUser = localStorage.getItem('user')
    if (storedUser) {
      try {
        setCurrentUser(JSON.parse(storedUser))
      } catch (error) {
        console.error('Error parsing user data:', error)
      }
    }
    
    return () => clearInterval(timer)
  }, [])

  // Prevent hydration mismatch by not rendering time-sensitive content on server
  if (!mounted) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50">
        <div className="p-6 space-y-6">
          <div className="animate-pulse">
            <div className="h-8 bg-gray-200 rounded w-1/3 mb-4"></div>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="h-32 bg-gray-200 rounded-lg"></div>
              ))}
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50">
      {/* Header */}
      <div className="sticky top-0 z-50 bg-white/80 backdrop-blur-sm border-b border-gray-200">
        <div className="p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              {/* AI Chat Toggle */}
              <div className="flex items-center space-x-2">
                <Button 
                  variant="ghost" 
                  size="sm"
                  onClick={() => setAiChatOpen(!aiChatOpen)}
                  className={`hover:bg-purple-100 ${aiChatOpen ? 'bg-purple-100 text-purple-700' : ''}`}
                >
                  <MessageSquare className="w-4 h-4 mr-2" />
                  Ask LabGig AI
                </Button>
              </div>
            </div>
            
            <div className="flex items-center space-x-3">
              <div className="text-sm text-gray-600">
                {currentTime.toLocaleTimeString()}
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="p-6 space-y-6">
        {/* AI-Powered Welcome Section */}
        <Card className="text-white overflow-hidden relative bg-gradient-to-r from-blue-600 via-blue-700 to-blue-800">
          <CardContent className="p-6 relative z-10">
            <div className="flex items-center justify-between">
              <div>
                <div className="flex items-center space-x-3 mb-2">
                  <h1 className="text-2xl font-bold">
                    {(() => {
                      const hour = currentTime.getHours()
                      let greeting = 'Good morning'
                      if (hour >= 12 && hour < 17) {
                        greeting = 'Good afternoon'
                      } else if (hour >= 17) {
                        greeting = 'Good evening'
                      }
                      return `${greeting}, ${currentUser?.full_name || 'User'}!`
                    })()}
                  </h1>
                  <Badge className="bg-white/20 text-white border border-white/30">
                    🤖 AI Active
                  </Badge>
                </div>
                <p className="text-white/90 mb-4">Ready to boost your sales today!</p>
                
                {/* AI Summary Pills */}
                <div className="flex flex-wrap gap-3">
                  <div className="bg-white/10 backdrop-blur-sm rounded-full px-4 py-2 flex items-center space-x-2">
                    <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
                    <span className="text-sm">12 high-priority tasks</span>
                  </div>
                  <div className="bg-white/10 backdrop-blur-sm rounded-full px-4 py-2 flex items-center space-x-2">
                    <div className="w-2 h-2 bg-yellow-400 rounded-full"></div>
                    <span className="text-sm">₹8.5L deals need attention</span>
                  </div>
                  <div className="bg-white/10 backdrop-blur-sm rounded-full px-4 py-2 flex items-center space-x-2">
                    <div className="w-2 h-2 bg-blue-400 rounded-full"></div>
                    <span className="text-sm">Tamil Nadu: +40% performance</span>
                  </div>
                </div>
              </div>
              
              <div className="relative">
                <div className="bg-white/10 backdrop-blur-sm rounded-xl p-4 hover:bg-white/20 transition-colors cursor-pointer">
                  <Brain className="w-8 h-8" />
                  <Badge className="absolute -top-2 -right-2 bg-red-500 text-white min-w-[20px] h-5 flex items-center justify-center text-xs">
                    3
                  </Badge>
                </div>
              </div>
            </div>
          </CardContent>
          {/* Decorative elements */}
          <div className="absolute top-0 right-0 w-64 h-64 bg-white/5 rounded-full -translate-y-32 translate-x-32"></div>
          <div className="absolute bottom-0 left-0 w-48 h-48 bg-white/5 rounded-full translate-y-24 -translate-x-24"></div>
        </Card>

        {/* Modular Card Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {/* Revenue Card with Visual Confidence Indicator */}
          <Card className="hover:shadow-lg transition-all duration-300 border-0 shadow-md bg-gradient-to-br from-emerald-50 to-green-100">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div className="space-y-2">
                  <p className="text-sm font-medium text-gray-600">Monthly Revenue</p>
                  <div className="flex items-baseline space-x-2">
                    <p className="text-2xl font-bold text-gray-900">₹72.8L</p>
                    <div className="flex items-center space-x-1 text-green-600">
                      <TrendingUp className="w-4 h-4" />
                      <span className="text-sm font-medium">+12.5%</span>
                    </div>
                  </div>
                  {/* AI Confidence Ring */}
                  <div className="flex items-center space-x-2">
                    <div className="relative w-8 h-8">
                      <svg className="w-8 h-8 transform -rotate-90" viewBox="0 0 32 32">
                        <circle cx="16" cy="16" r="14" stroke="#e5e7eb" strokeWidth="3" fill="none" />
                        <circle 
                          cx="16" cy="16" r="14" 
                          stroke="#10b981" strokeWidth="3" fill="none"
                          strokeDasharray={`${85 * 0.87} 87.96`}
                          className="transition-all duration-500"
                        />
                      </svg>
                      <div className="absolute inset-0 flex items-center justify-center">
                        <span className="text-xs font-medium text-green-600">85%</span>
                      </div>
                    </div>
                    <span className="text-xs text-gray-500">AI Confidence</span>
                  </div>
                </div>
                <div className="w-12 h-12 bg-green-500 rounded-xl flex items-center justify-center">
                  <Coins className="w-6 h-6 text-white" />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Leads Card with Sparkline */}
          <Card className="hover:shadow-lg transition-all duration-300 border-0 shadow-md bg-gradient-to-br from-blue-50 to-indigo-100">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div className="space-y-2">
                  <p className="text-sm font-medium text-gray-600">Active Leads</p>
                  <div className="flex items-baseline space-x-2">
                    <p className="text-2xl font-bold text-gray-900">298</p>
                    <div className="flex items-center space-x-1 text-blue-600">
                      <TrendingUp className="w-4 h-4" />
                      <span className="text-sm font-medium">+8.2%</span>
                    </div>
                  </div>
                  {/* Mini Sparkline */}
                  <div className="w-20 h-6">
                    <svg className="w-full h-full" viewBox="0 0 80 24">
                      <polyline
                        fill="none"
                        stroke="#3b82f6"
                        strokeWidth="2"
                        points="0,20 10,16 20,18 30,12 40,8 50,6 60,10 70,4 80,6"
                      />
                    </svg>
                  </div>
                </div>
                <div className="w-12 h-12 bg-blue-500 rounded-xl flex items-center justify-center">
                  <Users className="w-6 h-6 text-white" />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Deals Card */}
          <Card className="hover:shadow-lg transition-all duration-300 border-0 shadow-md bg-gradient-to-br from-purple-50 to-violet-100">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div className="space-y-2">
                  <p className="text-sm font-medium text-gray-600">Active Deals</p>
                  <div className="flex items-baseline space-x-2">
                    <p className="text-2xl font-bold text-gray-900">156</p>
                    <div className="flex items-center space-x-1 text-purple-600">
                      <TrendingUp className="w-4 h-4" />
                      <span className="text-sm font-medium">+18.7%</span>
                    </div>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className="w-2 h-2 bg-red-500 rounded-full"></div>
                    <span className="text-xs text-gray-500">3 at risk</span>
                  </div>
                </div>
                <div className="w-12 h-12 bg-purple-500 rounded-xl flex items-center justify-center">
                  <Handshake className="w-6 h-6 text-white" />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Complaints Card */}
          <Card className="hover:shadow-lg transition-all duration-300 border-0 shadow-md bg-gradient-to-br from-red-50 to-pink-100">
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div className="space-y-2">
                  <p className="text-sm font-medium text-gray-600">Open Complaints</p>
                  <div className="flex items-baseline space-x-2">
                    <p className="text-2xl font-bold text-gray-900">23</p>
                    <div className="flex items-center space-x-1 text-green-600">
                      <TrendingDown className="w-4 h-4" />
                      <span className="text-sm font-medium">-5.1%</span>
                    </div>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className="w-2 h-2 bg-red-500 rounded-full animate-pulse"></div>
                    <span className="text-xs text-gray-500">5 critical</span>
                  </div>
                </div>
                <div className="w-12 h-12 bg-red-500 rounded-xl flex items-center justify-center">
                  <AlertTriangle className="w-6 h-6 text-white" />
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* AI Conversational Interface Sidebar */}
        {aiChatOpen && (
          <Card className="fixed right-6 top-20 w-80 h-96 shadow-2xl border-2 border-purple-200 z-50 bg-white">
            <CardHeader className="bg-gradient-to-r from-purple-600 to-blue-600 text-white rounded-t-lg">
              <CardTitle className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <Brain className="w-5 h-5" />
                  <span>LabGig AI Assistant</span>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setAiChatOpen(false)}
                  className="text-white hover:bg-white/20"
                >
                  ×
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent className="p-4 h-full overflow-y-auto">
              <div className="space-y-3">
                <div className="bg-gray-100 rounded-lg p-3">
                  <p className="text-sm">Hi! Ask me anything about your pipeline:</p>
                  <div className="mt-2 space-y-1">
                    <Button variant="ghost" size="sm" className="text-xs text-left w-full justify-start">
                      "Show me Tamil Nadu leads"
                    </Button>
                    <Button variant="ghost" size="sm" className="text-xs text-left w-full justify-start">
                      "What deals need attention?"
                    </Button>
                    <Button variant="ghost" size="sm" className="text-xs text-left w-full justify-start">
                      "Schedule call with Eurofins"
                    </Button>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Priority Tasks - Smart Action Cards */}
        <Card className="border-l-4 border-l-red-500 shadow-lg">
          <CardHeader className="bg-gradient-to-r from-red-50 to-pink-50">
            <CardTitle className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-red-500 rounded-xl flex items-center justify-center">
                  <Zap className="w-6 h-6 text-white" />
                </div>
                <div>
                  <h3 className="text-xl font-bold text-gray-900">🎯 Priority Actions</h3>
                  <p className="text-sm text-gray-600">AI-ranked by revenue impact</p>
                </div>
              </div>
              <Badge className="bg-red-100 text-red-800">3 Critical</Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="p-6">
            <div className="space-y-4">
              {/* Critical Task Card */}
              <Card className="border-2 border-red-200 bg-gradient-to-r from-red-50 to-pink-50 hover:shadow-md transition-all cursor-pointer">
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex-1">
                      <div className="flex items-center space-x-3 mb-3">
                        <div className="w-3 h-3 bg-red-500 rounded-full animate-pulse"></div>
                        <h4 className="font-bold text-gray-900">Follow up with TSAR Labcare</h4>
                        <Badge className="bg-red-500 text-white text-xs">₹8.5L AT RISK</Badge>
                      </div>
                      <div className="ml-6 space-y-2">
                        <div className="flex items-center space-x-4 text-sm text-gray-700">
                          <div className="flex items-center space-x-1">
                            <Clock className="w-4 h-4" />
                            <span>Last contact: 5 days ago</span>
                          </div>
                          <div className="flex items-center space-x-1">
                            <Star className="w-4 h-4 text-yellow-500" />
                            <span>AI Score: 85%</span>
                          </div>
                        </div>
                        <div className="bg-white/50 rounded-lg p-3 border border-red-200">
                          <p className="text-sm font-medium text-red-700 mb-2">🤖 AI Recommendation:</p>
                          <p className="text-sm text-gray-700">Call TODAY before 3 PM. Customer availability pattern: 2-4 PM. Mention fibrinometer discussion and 15% early-bird discount.</p>
                        </div>
                      </div>
                    </div>
                    <div className="flex flex-col space-y-2">
                      <Button className="bg-red-500 hover:bg-red-600 text-white">
                        <Phone className="w-4 h-4 mr-2" />
                        Call Now
                      </Button>
                      <Button variant="outline" size="sm">
                        <Mail className="w-4 h-4 mr-2" />
                        Email
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* High Priority Task */}
              <Card className="border border-yellow-200 bg-gradient-to-r from-yellow-50 to-orange-50 hover:shadow-md transition-all cursor-pointer">
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex-1">
                      <div className="flex items-center space-x-3 mb-2">
                        <div className="w-3 h-3 bg-yellow-500 rounded-full"></div>
                        <h4 className="font-semibold text-gray-900">Generate quotation for Eurofins</h4>
                        <Badge className="bg-green-100 text-green-800 text-xs">🔥 HOT LEAD</Badge>
                      </div>
                      <div className="ml-6 space-y-1">
                        <p className="text-sm text-gray-700">Similar deals: ₹12L avg • Win rate: 78% • Template QT-LAB-001 ready</p>
                      </div>
                    </div>
                    <Button variant="outline" className="border-yellow-400 text-yellow-700 hover:bg-yellow-50">
                      📄 Review Quote
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </div>
          </CardContent>
        </Card>

        {/* Interactive Pipeline Funnel Visualization */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card className="shadow-lg">
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <Activity className="w-5 h-5 text-blue-600" />
                <span>Sales Pipeline Funnel</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="relative">
                  <div className="flex items-center justify-between bg-blue-100 rounded-lg p-3">
                    <span className="font-medium">Prospects</span>
                    <span className="font-bold">298 • ₹45.2L</span>
                  </div>
                  <div className="absolute right-2 top-1/2 transform -translate-y-1/2">
                    <ChevronDown className="w-4 h-4 text-blue-600" />
                  </div>
                </div>
                <div className="relative ml-4">
                  <div className="flex items-center justify-between bg-yellow-100 rounded-lg p-3">
                    <span className="font-medium">Qualified</span>
                    <span className="font-bold">156 • ₹32.8L</span>
                  </div>
                  <div className="absolute right-2 top-1/2 transform -translate-y-1/2">
                    <ChevronDown className="w-4 h-4 text-yellow-600" />
                  </div>
                </div>
                <div className="relative ml-8">
                  <div className="flex items-center justify-between bg-orange-100 rounded-lg p-3">
                    <span className="font-medium">Proposal</span>
                    <span className="font-bold">89 • ₹28.4L</span>
                  </div>
                  <div className="absolute right-2 top-1/2 transform -translate-y-1/2">
                    <ChevronDown className="w-4 h-4 text-orange-600" />
                  </div>
                </div>
                <div className="relative ml-12">
                  <div className="flex items-center justify-between bg-green-100 rounded-lg p-3">
                    <span className="font-medium">Negotiation</span>
                    <span className="font-bold">34 • ₹8.5L</span>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Geographic Performance Heat Map */}
          <Card className="shadow-lg">
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <MapPin className="w-5 h-5 text-green-600" />
                <span>Regional Performance</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div className="flex items-center justify-between p-3 bg-green-100 rounded-lg border-l-4 border-green-500">
                  <div>
                    <p className="font-semibold">Tamil Nadu</p>
                    <p className="text-sm text-gray-600">40% conversion rate</p>
                  </div>
                  <div className="text-right">
                    <p className="font-bold text-green-700">₹18.5L</p>
                    <Badge className="bg-green-500 text-white text-xs">HOT</Badge>
                  </div>
                </div>
                <div className="flex items-center justify-between p-3 bg-blue-100 rounded-lg">
                  <div>
                    <p className="font-semibold">Karnataka</p>
                    <p className="text-sm text-gray-600">32% conversion rate</p>
                  </div>
                  <div className="text-right">
                    <p className="font-bold text-blue-700">₹12.3L</p>
                    <Badge className="bg-blue-500 text-white text-xs">GOOD</Badge>
                  </div>
                </div>
                <div className="flex items-center justify-between p-3 bg-yellow-100 rounded-lg">
                  <div>
                    <p className="font-semibold">Maharashtra</p>
                    <p className="text-sm text-gray-600">28% conversion rate</p>
                  </div>
                  <div className="text-right">
                    <p className="font-bold text-yellow-700">₹9.8L</p>
                    <Badge className="bg-yellow-500 text-white text-xs">AVG</Badge>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Live Intelligence Feed */}
        <Card className="border-l-4 border-l-purple-500 bg-gradient-to-r from-purple-50 to-blue-50">
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <div className="w-8 h-8 bg-gradient-to-r from-purple-600 to-blue-600 rounded-full flex items-center justify-center">
                <Brain className="w-5 h-5 text-white" />
              </div>
              <span>🤖 Live Intelligence Feed</span>
              <Badge className="bg-green-500 text-white animate-pulse">LIVE</Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="p-4 bg-white rounded-xl border border-purple-200 shadow-sm">
                <div className="flex items-center space-x-2 mb-2">
                  <div className="w-2 h-2 bg-red-500 rounded-full animate-ping"></div>
                  <span className="font-medium text-red-800">Real-time Alert</span>
                </div>
                <p className="text-sm text-gray-700">Eurofins visited pricing page 3x in last hour. High buying intent detected!</p>
                <Button size="sm" className="mt-2 w-full bg-red-500 hover:bg-red-600">
                  Contact Immediately
                </Button>
              </div>
              
              <div className="p-4 bg-white rounded-xl border border-green-200 shadow-sm">
                <div className="flex items-center space-x-2 mb-2">
                  <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                  <span className="font-medium text-green-800">AI Prediction</span>
                </div>
                <p className="text-sm text-gray-700">Next month revenue forecast: ₹85.2L (+17% growth)</p>
                <div className="mt-2 flex items-center space-x-2">
                  <div className="flex-1 bg-gray-200 rounded-full h-2">
                    <div className="bg-green-500 h-2 rounded-full" style={{width: '87%'}}></div>
                  </div>
                  <span className="text-xs font-medium">87%</span>
                </div>
              </div>
              
              <div className="p-4 bg-white rounded-xl border border-blue-200 shadow-sm">
                <div className="flex items-center space-x-2 mb-2">
                  <div className="w-2 h-2 bg-blue-500 rounded-full"></div>
                  <span className="font-medium text-blue-800">Lead Scoring</span>
                </div>
                <p className="text-sm text-gray-700">15 new leads scored. 5 marked as high-priority for immediate follow-up.</p>
                <Button size="sm" variant="outline" className="mt-2 w-full border-blue-400 text-blue-700">
                  View Hot Leads
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>

      </div>
    </div>
  )
}