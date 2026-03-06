"use client"

// Updated sidebar with categorized navigation

import { useState, useEffect } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Badge } from "@/components/ui/badge"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import {
  Home,
  Package,
  UserCheck,
  Activity,
  FileText,
  AlertTriangle,
  Wrench,
  Settings,
  BarChart3,
  Building2,
  Calendar,
  ChevronLeft,
  ChevronRight,
  Beaker,
  Target,
  ShoppingCart,
  FolderOpen,
  Bell,
  MessageSquare,
  Users,
  Zap,
  ChevronDown,
  ChevronUp,
  Headphones,
} from "lucide-react"


export function Sidebar() {
  const [collapsed, setCollapsed] = useState(false)
  const [expandedCategories, setExpandedCategories] = useState<string[]>(["Sales"])
  const [navigationStats, setNavigationStats] = useState<any>(null)
  const [statsLoaded, setStatsLoaded] = useState(false)
  const pathname = usePathname()
  
  // Load navigation stats with caching
  useEffect(() => {
    loadNavigationStats()
  }, [])
  
  const loadNavigationStats = async () => {
    try {
      // Check if this is a page refresh (performance.navigation.type === 1)
      // or if page was reloaded (F5, Cmd+R)
      const isPageRefresh = performance.navigation.type === 1 || 
                           (performance.getEntriesByType('navigation')[0] as any)?.type === 'reload'
      
      // Check cache first (without company ID) - but skip cache on manual refresh
      if (!isPageRefresh) {
        const cachedStats = localStorage.getItem(`navigation-stats`)
        const cacheTime = localStorage.getItem(`navigation-stats-time`)
        
        if (cachedStats && cacheTime) {
          const cacheAge = Date.now() - parseInt(cacheTime)
          const fiveMinutes = 5 * 60 * 1000
          
          if (cacheAge < fiveMinutes) {
            // Use cached data immediately
            setNavigationStats(JSON.parse(cachedStats))
            setStatsLoaded(true)
            return
          }
        }
      }
      
      // Fetch fresh data - ALL counts without filtering
      const response = await fetch(`/api/navigation-stats`)
      
      if (response.ok) {
        const data = await response.json()
        setNavigationStats(data.stats)
        setStatsLoaded(true)
        
        // Cache the results for 5 minutes
        localStorage.setItem(`navigation-stats`, JSON.stringify(data.stats))
        localStorage.setItem(`navigation-stats-time`, Date.now().toString())
      }
    } catch (error) {
      console.error('Error loading navigation stats:', error)
      setStatsLoaded(true) // Show UI even if stats fail
    }
  }
  
  const formatCount = (count: number) => {
    if (count >= 1000) {
      return `${(count / 1000).toFixed(1)}K`
    }
    return count.toString()
  }
  
  const getBadge = (routePath: string) => {
    // Badges removed from navigation
    return null
  }

  // Create navigation with dynamic badges
  const navigation = [
    { name: "Dashboard", href: "/", icon: Home, badge: null, type: "item" },
    {
      name: "Sales",
      icon: Target,
      type: "category",
      children: [
        { name: "Leads", href: "/leads", icon: UserCheck, badge: getBadge("/leads") },
        { name: "Accounts", href: "/accounts", icon: Building2, badge: getBadge("/accounts") },
        { name: "Deals", href: "/deals", icon: Target, badge: getBadge("/deals") },
        { name: "Quotations", href: "/quotations", icon: FileText, badge: getBadge("/quotations") },
        { name: "Sales Orders", href: "/sales-orders", icon: ShoppingCart, badge: getBadge("/sales-orders") },
      ]
    },
    {
      name: "Operations",
      icon: Wrench,
      type: "category",
      children: [
        { name: "Installations", href: "/installations", icon: Wrench, badge: getBadge("/installations") },
        { name: "AMC", href: "/amc", icon: Calendar, badge: getBadge("/amc") },
        { name: "Complaints", href: "/complaints", icon: AlertTriangle, badge: getBadge("/complaints") },
        { name: "Support", href: "/support", icon: Headphones, badge: getBadge("/support") },
        { name: "Activities", href: "/activities", icon: Activity, badge: getBadge("/activities") },
      ]
    },
    {
      name: "Management",
      icon: Package,
      type: "category",
      children: [
        { name: "Products", href: "/products", icon: Package, badge: getBadge("/products") },
        { name: "Solutions", href: "/solutions", icon: Beaker, badge: null },
        { name: "Doc Library", href: "/doc-library", icon: FolderOpen, badge: null },
        { name: "MIS Reports", href: "/mis-reports", icon: BarChart3, badge: null },
      ]
    },
    {
      name: "Communication",
      icon: MessageSquare,
      type: "category",
      children: [
        { name: "Conversations", href: "/conversations", icon: MessageSquare, badge: null },
        // { name: "Gig Workspace", href: "/gig-workspace", icon: Users, badge: null },
      ]
    },
    { name: "Integrations", href: "/integrations", icon: Zap, badge: null, type: "item" },
    { name: "Admin", href: "/admin", icon: Settings, badge: null, type: "item" },
  ]

  const toggleCategory = (categoryName: string) => {
    setExpandedCategories(prev => 
      prev.includes(categoryName) 
        ? prev.filter(name => name !== categoryName)
        : [...prev, categoryName]
    )
  }

  return (
    <TooltipProvider>
      <div
        className={cn(
          "bg-white border-r border-gray-200 flex flex-col transition-all duration-300 shadow-sm",
          collapsed ? "w-16" : "w-64",
        )}
      >
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between">
            {!collapsed && (
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-gradient-to-br from-blue-600 to-blue-800 rounded-xl flex items-center justify-center shadow-lg">
                  <Beaker className="w-6 h-6 text-white" />
                </div>
                <div>
                  <h1 className="text-xl font-bold text-gray-900">LabGig</h1>
                  <p className="text-xs text-gray-500">
                    CRM System.
                    <br />
                    AI Powered
                  </p>
                </div>
              </div>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setCollapsed(!collapsed)}
              className={cn("ml-auto hover:bg-gray-100", collapsed && "mx-auto")}
            >
              {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
            </Button>
          </div>
        </div>

        <ScrollArea className="flex-1 px-3 py-4">
          <nav className="space-y-1">
            {navigation.map((item) => {
              if (item.type === "category") {
                const isExpanded = expandedCategories.includes(item.name)
                const hasActiveChild = item.children?.some(child => pathname === child.href)
                
                return (
                  <div key={item.name}>
                    <button
                      onClick={() => toggleCategory(item.name)}
                      className={cn(
                        "w-full flex items-center px-3 py-2.5 text-sm font-medium rounded-lg transition-all duration-200 group",
                        hasActiveChild
                          ? "bg-blue-50 text-blue-700 shadow-sm"
                          : "text-gray-600 hover:bg-gray-50 hover:text-gray-900",
                        collapsed && "justify-center",
                      )}
                    >
                      <item.icon
                        className={cn(
                          "flex-shrink-0 w-5 h-5 transition-colors",
                          collapsed ? "mr-0" : "mr-3",
                          hasActiveChild ? "text-blue-700" : "text-gray-400 group-hover:text-gray-600",
                        )}
                      />
                      {!collapsed && (
                        <>
                          <span className="flex-1 text-left">{item.name}</span>
                          {isExpanded ? (
                            <ChevronUp className="w-4 h-4 text-gray-400" />
                          ) : (
                            <ChevronDown className="w-4 h-4 text-gray-400" />
                          )}
                        </>
                      )}
                    </button>
                    
                    {!collapsed && isExpanded && item.children && (
                      <div className="ml-6 mt-1 space-y-1">
                        {item.children.map((child) => {
                          const isActive = pathname === child.href
                          return (
                            <Link
                              key={child.name}
                              href={child.href}
                              className={cn(
                                "flex items-center px-3 py-2 text-sm font-medium rounded-lg transition-all duration-200 group",
                                isActive
                                  ? "bg-blue-50 text-blue-700 border-r-2 border-blue-700 shadow-sm"
                                  : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                              )}
                            >
                              <child.icon
                                className={cn(
                                  "flex-shrink-0 w-4 h-4 mr-3 transition-colors",
                                  isActive ? "text-blue-700" : "text-gray-400 group-hover:text-gray-600"
                                )}
                              />
                              <span className="flex-1">{child.name}</span>
                              {child.badge && (
                                <Badge
                                  variant="secondary"
                                  className={cn(
                                    "ml-auto text-xs",
                                    isActive ? "bg-blue-100 text-blue-700" : "bg-gray-100 text-gray-600"
                                  )}
                                >
                                  {child.badge}
                                </Badge>
                              )}
                            </Link>
                          )
                        })}
                      </div>
                    )}
                  </div>
                )
              } else {
                // Regular navigation item
                const isActive = pathname === item.href
                const NavItem = (
                  <Link
                    key={item.name}
                    href={item.href}
                    className={cn(
                      "flex items-center px-3 py-2.5 text-sm font-medium rounded-lg transition-all duration-200 group",
                      isActive
                        ? "bg-blue-50 text-blue-700 border-r-2 border-blue-700 shadow-sm"
                        : "text-gray-600 hover:bg-gray-50 hover:text-gray-900",
                      collapsed && "justify-center",
                    )}
                  >
                    <item.icon
                      className={cn(
                        "flex-shrink-0 w-5 h-5 transition-colors",
                        collapsed ? "mr-0" : "mr-3",
                        isActive ? "text-blue-700" : "text-gray-400 group-hover:text-gray-600",
                      )}
                    />
                    {!collapsed && (
                      <>
                        <span className="flex-1">{item.name}</span>
                        {item.badge && (
                          <Badge
                            variant="secondary"
                            className={cn(
                              "ml-auto text-xs",
                              isActive ? "bg-blue-100 text-blue-700" : "bg-gray-100 text-gray-600",
                            )}
                          >
                            {item.badge}
                          </Badge>
                        )}
                      </>
                    )}
                  </Link>
                )

                if (collapsed) {
                  return (
                    <Tooltip key={item.name}>
                      <TooltipTrigger asChild>{NavItem}</TooltipTrigger>
                      <TooltipContent side="right" className="flex items-center gap-2">
                        {item.name}
                        {item.badge && (
                          <Badge variant="secondary" className="text-xs">
                            {item.badge}
                          </Badge>
                        )}
                      </TooltipContent>
                    </Tooltip>
                  )
                }

                return NavItem
              }
            })}
          </nav>
        </ScrollArea>

        {/* Footer with user info */}
        <div className="p-3 border-t border-gray-200">
          {!collapsed ? (
            <div className="flex items-center space-x-3 p-2 rounded-lg hover:bg-gray-50">
              <div className="w-8 h-8 bg-gradient-to-br from-green-400 to-green-600 rounded-full flex items-center justify-center text-white text-sm font-medium">
                PS
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-gray-900 truncate">Prashanth S.</p>
                <p className="text-xs text-gray-500 truncate">Sales Manager</p>
              </div>
              <div className="relative">
                <Bell className="w-4 h-4 text-gray-400" />
                <div className="absolute -top-1 -right-1 w-2 h-2 bg-red-500 rounded-full"></div>
              </div>
            </div>
          ) : (
            <Tooltip>
              <TooltipTrigger asChild>
                <div className="w-8 h-8 bg-gradient-to-br from-green-400 to-green-600 rounded-full flex items-center justify-center text-white text-sm font-medium mx-auto cursor-pointer">
                  PS
                </div>
              </TooltipTrigger>
              <TooltipContent side="right">
                <p>Prashanth Sandilya</p>
                <p className="text-xs text-muted-foreground">Sales Manager</p>
              </TooltipContent>
            </Tooltip>
          )}
        </div>
      </div>
    </TooltipProvider>
  )
}
