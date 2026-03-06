"use client"

// Force refresh - updated categorized navigation

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
  Handshake,
  ShoppingCart,
  FolderOpen,
  Bell,
  MessageSquare,
  Users,
  Zap,
  Briefcase,
  Store,
  Mic,
  ChevronDown,
  ChevronUp,
  ShoppingBag,
  Brain,
  Contact,
  LifeBuoy,
  Ticket,
  Lightbulb,
  Receipt,
  CheckCircle,
  ArrowDownToLine,
} from "lucide-react"

// Define user roles and their accessible modules with categorized structure based on Zoho CRM design
const roleBasedNavigation = {
  "Sales Manager": [
    { name: "Dashboard", href: "/", icon: Home, badge: null, type: "item" },
    { name: "Activities", href: "/activities", icon: Activity, badge: null, type: "item" },
    {
      name: "Sales",
      icon: ShoppingBag,
      type: "category", 
      children: [
        { name: "Leads", href: "/leads", icon: UserCheck, badge: null },
        { name: "Contacts", href: "/contacts", icon: Contact, badge: null },
        { name: "Accounts", href: "/accounts", icon: Building2, badge: null },
        { name: "Deals", href: "/deals", icon: Handshake, badge: null },
      ]
    },
    {
      name: "Inventory",
      icon: Package,
      type: "category",
      children: [
        { name: "Products", href: "/products", icon: Package, badge: null },
        { name: "Stock Entries", href: "/stock-entries", icon: ArrowDownToLine, badge: null },
        { name: "Quotations", href: "/quotations", icon: FileText, badge: null },
        { name: "Sales Orders", href: "/sales-orders", icon: ShoppingCart, badge: null },
        { name: "Invoices", href: "/invoices", icon: FileText, badge: null },
      ]
    },
    {
      name: "Services", 
      icon: Wrench,
      type: "category",
      children: [
        { name: "Installations", href: "/installations", icon: Wrench, badge: null },
        { name: "AMC", href: "/amc", icon: Calendar, badge: null },
        { name: "Complaints", href: "/complaints", icon: AlertTriangle, badge: null },
      ]
    },
    // {
    //   name: "Projects",
    //   icon: Briefcase, 
    //   type: "category",
    //   children: [
    //     { name: "Projects", href: "/projects", icon: Briefcase, badge: null },
    //     { name: "Gig Workspace", href: "/gig-workspace", icon: Users, badge: null },
    //   ]
    // },
    {
      name: "Support",
      icon: LifeBuoy,
      type: "category",
      children: [
        { name: "Support Center", href: "/support", icon: LifeBuoy, badge: null },
        // { name: "Cases", href: "/cases", icon: Ticket, badge: null },
        // { name: "Solutions", href: "/solutions", icon: Lightbulb, badge: null },
      ]
    },
    {
      name: "Reports & Admin",
      icon: BarChart3,
      type: "category",
      children: [
        { name: "MIS Reports", href: "/mis-reports", icon: BarChart3, badge: null },
        { name: "Doc Library", href: "/doc-library", icon: FolderOpen, badge: null },
        { name: "Admin", href: "/admin", icon: Settings, badge: null },
      ]
    },
  ],
  "Service Manager": [
    { name: "Dashboard", href: "/", icon: Home, badge: null, type: "item" },
    { name: "Activities", href: "/activities", icon: Activity, badge: null, type: "item" },
    {
      name: "Services",
      icon: Wrench,
      type: "category",
      children: [
        { name: "Installations", href: "/installations", icon: Wrench, badge: null },
        { name: "AMC", href: "/amc", icon: Calendar, badge: null },
        { name: "Complaints", href: "/complaints", icon: AlertTriangle, badge: null },
      ]
    },
    // {
    //   name: "Projects",
    //   icon: Briefcase,
    //   type: "category",
    //   children: [
    //     { name: "Projects", href: "/projects", icon: Briefcase, badge: null },
    //     { name: "Gig Workspace", href: "/gig-workspace", icon: Users, badge: null },
    //   ]
    // },
    {
      name: "Support",
      icon: LifeBuoy,
      type: "category",
      children: [
        { name: "Support Center", href: "/support", icon: LifeBuoy, badge: null },
        // { name: "Cases", href: "/cases", icon: Ticket, badge: null },
        // { name: "Solutions", href: "/solutions", icon: Lightbulb, badge: null },
      ]
    },
    {
      name: "Reports & Admin",
      icon: BarChart3,
      type: "category",
      children: [
        { name: "Doc Library", href: "/doc-library", icon: FolderOpen, badge: null },
      ]
    },
  ],
  "Marketing Manager": [
    { name: "Dashboard", href: "/", icon: Home, badge: null, type: "item" },
    { name: "Activities", href: "/activities", icon: Activity, badge: null, type: "item" },
    {
      name: "Sales",
      icon: ShoppingBag,
      type: "category",
      children: [
        { name: "Leads", href: "/leads", icon: UserCheck, badge: null },
        { name: "Contacts", href: "/contacts", icon: Contact, badge: null },
        { name: "Accounts", href: "/accounts", icon: Building2, badge: null },
        { name: "Deals", href: "/deals", icon: Handshake, badge: null },
      ]
    },
    {
      name: "Inventory",
      icon: Package,
      type: "category",
      children: [
        { name: "Products", href: "/products", icon: Package, badge: null },
        { name: "Quotations", href: "/quotations", icon: FileText, badge: null },
        { name: "Sales Orders", href: "/sales-orders", icon: ShoppingCart, badge: null },
        { name: "Invoices", href: "/invoices", icon: Receipt, badge: null },
      ]
    },
    {
      name: "Services",
      icon: Wrench,
      type: "category",
      children: [
        { name: "Installations", href: "/installations", icon: Wrench, badge: null },
        { name: "AMC", href: "/amc", icon: Calendar, badge: null },
        { name: "Complaints", href: "/complaints", icon: AlertTriangle, badge: null },
      ]
    },
    // {
    //   name: "Projects",
    //   icon: Briefcase,
    //   type: "category",
    //   children: [
    //     { name: "Projects", href: "/projects", icon: Briefcase, badge: null },
    //     { name: "Gig Workspace", href: "/gig-workspace", icon: Users, badge: null },
    //   ]
    // },
    {
      name: "Support",
      icon: LifeBuoy,
      type: "category",
      children: [
        { name: "Support Center", href: "/support", icon: LifeBuoy, badge: null },
        // { name: "Cases", href: "/cases", icon: Ticket, badge: null },
        // { name: "Solutions", href: "/solutions", icon: Lightbulb, badge: null },
      ]
    },
    {
      name: "Reports & Admin",
      icon: BarChart3,
      type: "category",
      children: [
        { name: "MIS Reports", href: "/mis-reports", icon: BarChart3, badge: null },
        { name: "Doc Library", href: "/doc-library", icon: FolderOpen, badge: null },
      ]
    },
  ],
  "Project Manager": [
    { name: "Dashboard", href: "/", icon: Home, badge: null, type: "item" },
    { name: "Activities", href: "/activities", icon: Activity, badge: null, type: "item" },
    // {
    //   name: "Projects",
    //   icon: Briefcase,
    //   type: "category",
    //   children: [
    //     { name: "Projects", href: "/projects", icon: Briefcase, badge: null },
    //     { name: "Installations", href: "/installations", icon: Wrench, badge: null },
    //     { name: "Gig Workspace", href: "/gig-workspace", icon: Users, badge: null },
    //   ]
    // },
    {
      name: "Support",
      icon: LifeBuoy,
      type: "category",
      children: [
        { name: "Support Center", href: "/support", icon: LifeBuoy, badge: null },
        // { name: "Cases", href: "/cases", icon: Ticket, badge: null },
        // { name: "Solutions", href: "/solutions", icon: Lightbulb, badge: null },
      ]
    },
    {
      name: "Reports & Admin",
      icon: BarChart3,
      type: "category",
      children: [
        { name: "Doc Library", href: "/doc-library", icon: FolderOpen, badge: null },
      ]
    },
    { name: "Accounts", href: "/accounts", icon: Building2, badge: null, type: "item" },
  ],
  "Sales Rep": [
    { name: "Dashboard", href: "/", icon: Home, badge: null, type: "item" },
    { name: "Activities", href: "/activities", icon: Activity, badge: null, type: "item" },
    {
      name: "Sales",
      icon: ShoppingBag,
      type: "category",
      children: [
        { name: "Leads", href: "/leads", icon: UserCheck, badge: null },
        { name: "Contacts", href: "/contacts", icon: Contact, badge: null },
        { name: "Accounts", href: "/accounts", icon: Building2, badge: null },
        { name: "Deals", href: "/deals", icon: Handshake, badge: null },
      ]
    },
    {
      name: "Support",
      icon: LifeBuoy,
      type: "category",
      children: [
        { name: "Support Center", href: "/support", icon: LifeBuoy, badge: null },
        // { name: "Cases", href: "/cases", icon: Ticket, badge: null },
        // { name: "Solutions", href: "/solutions", icon: Lightbulb, badge: null },
      ]
    },
    {
      name: "Reports & Admin",
      icon: BarChart3,
      type: "category",
      children: [
        { name: "Doc Library", href: "/doc-library", icon: FolderOpen, badge: null },
      ]
    },
  ],
  Admin: [
    { name: "Dashboard", href: "/", icon: Home, badge: null, type: "item" },
    { name: "Activities", href: "/activities", icon: Activity, badge: null, type: "item" },
    {
      name: "Sales",
      icon: ShoppingBag,
      type: "category",
      children: [
        { name: "Leads", href: "/leads", icon: UserCheck, badge: null },
        { name: "Contacts", href: "/contacts", icon: Contact, badge: null },
        { name: "Accounts", href: "/accounts", icon: Building2, badge: null },
        { name: "Deals", href: "/deals", icon: Handshake, badge: null },
      ]
    },
    {
      name: "Inventory",
      icon: Package,
      type: "category",
      children: [
        { name: "Products", href: "/products", icon: Package, badge: null },
        { name: "Stock Entries", href: "/stock-entries", icon: ArrowDownToLine, badge: null },
        { name: "Quotations", href: "/quotations", icon: FileText, badge: null },
        { name: "Sales Orders", href: "/sales-orders", icon: ShoppingCart, badge: null },
        { name: "Invoices", href: "/invoices", icon: FileText, badge: null },
      ]
    },
    {
      name: "Services",
      icon: Wrench,
      type: "category",
      children: [
        { name: "Installations", href: "/installations", icon: Wrench, badge: null },
        { name: "AMC", href: "/amc", icon: Calendar, badge: null },
        { name: "Complaints", href: "/complaints", icon: AlertTriangle, badge: null },
      ]
    },
    // {
    //   name: "Projects",
    //   icon: Briefcase,
    //   type: "category",
    //   children: [
    //     { name: "Projects", href: "/projects", icon: Briefcase, badge: null },
    //     { name: "Gig Workspace", href: "/gig-workspace", icon: Users, badge: null },
    //   ]
    // },
    {
      name: "Support",
      icon: LifeBuoy,
      type: "category",
      children: [
        { name: "Support Center", href: "/support", icon: LifeBuoy, badge: null },
        // { name: "Cases", href: "/cases", icon: Ticket, badge: null },
        // { name: "Solutions", href: "/solutions", icon: Lightbulb, badge: null },
      ]
    },
    {
      name: "Reports & Admin",
      icon: BarChart3,
      type: "category",
      children: [
        { name: "MIS Reports", href: "/mis-reports", icon: BarChart3, badge: null },
        { name: "Doc Library", href: "/doc-library", icon: FolderOpen, badge: null },
        { name: "Admin", href: "/admin", icon: Settings, badge: null },
      ]
    },
  ],
}

export function RoleBasedSidebar() {
  const [collapsed, setCollapsed] = useState(false)
  const [currentUser, setCurrentUser] = useState<any>(null)
  const [navigationStats, setNavigationStats] = useState<any>(null)
  const [statsLoaded, setStatsLoaded] = useState(false)
  const pathname = usePathname()
  
  // Get current user from localStorage
  useEffect(() => {
    const storedUser = localStorage.getItem('user')
    if (storedUser) {
      setCurrentUser(JSON.parse(storedUser))
    }
  }, [])
  
  // Load navigation stats with caching
  useEffect(() => {
    loadNavigationStats()
  }, [])
  
  const loadNavigationStats = async (forceRefresh = false) => {
    try {
      // Check cache first (unless forcing refresh)
      if (!forceRefresh) {
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
        
        // Cache the results
        localStorage.setItem(`navigation-stats`, JSON.stringify(data.stats))
        localStorage.setItem(`navigation-stats-time`, Date.now().toString())
      }
    } catch (error) {
      console.error('Error loading navigation stats:', error)
      setStatsLoaded(true) // Show UI even if stats fail
    }
  }
  
  // Expose refresh function globally for other components to trigger
  useEffect(() => {
    window.refreshNavigationStats = () => loadNavigationStats(true)
    return () => {
      delete window.refreshNavigationStats
    }
  }, [])
  
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
  
  // Use current user's role, fallback to Sales Manager
  const currentRole = currentUser?.role?.name?.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase()) || "Sales Manager"
  let navigation = roleBasedNavigation[currentRole] || roleBasedNavigation["Sales Manager"] || []
  
  // Badges removed - keep original navigation structure
  // navigation = navigation (no mapping needed)
  
  // Update admin links based on user type
  if (currentUser?.is_admin || currentUser?.is_super_admin) {
    navigation = navigation.map(item => {
      if (item.type === "category" && item.children) {
        return {
          ...item,
          children: item.children.map(child => {
            if (child.href === "/admin") {
              // Company admins go to company-admin page, super admins go to admin page
              return {
                ...child,
                name: currentUser?.is_super_admin ? "Admin" : "Company Admin",
                href: currentUser?.is_super_admin ? "/admin" : "/company-admin"
              }
            }
            return child
          })
        }
      }
      return item
    })
  } else {
    // Filter out Admin link for non-admin users
    navigation = navigation.map(item => {
      if (item.type === "category" && item.children) {
        return {
          ...item,
          children: item.children.filter(child => child.href !== "/admin")
        }
      }
      return item
    }).filter(item => item.href !== "/admin") // Also remove direct admin links
  }
  
  // Initialize with no categories expanded by default  
  const [expandedCategories, setExpandedCategories] = useState<string[]>([])

  // Get active category based on current path
  const getActiveCategory = () => {
    for (const item of navigation) {
      if (item.type === "category" && item.children) {
        const hasActiveChild = item.children.some(child => pathname === child.href)
        if (hasActiveChild) {
          return item.name
        }
      }
    }
    return null
  }

  // Update expanded categories when navigation changes
  useEffect(() => {
    const activeCategory = getActiveCategory()
    
    setExpandedCategories(prev => {
      const newExpanded = []
      
      // Only expand the category if we're actively in one of its child pages
      if (activeCategory) {
        newExpanded.push(activeCategory)
      }
      
      // Don't expand anything by default on dashboard
      // User must click to expand categories
      
      return newExpanded
    })
  }, [pathname])

  const toggleCategory = (categoryName: string) => {
    setExpandedCategories(prev => {
      if (prev.includes(categoryName)) {
        // Remove the category from expanded list (allow all categories to be minimized)
        return prev.filter(name => name !== categoryName)
      } else {
        return [...prev, categoryName]
      }
    })
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
                                "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                              )}
                            >
                              <child.icon
                                className={cn(
                                  "flex-shrink-0 w-4 h-4 mr-3 transition-colors",
                                  "text-gray-400 group-hover:text-gray-600"
                                )}
                              />
                              <span className="flex-1">{child.name}</span>
                              {child.badge && (
                                <Badge
                                  variant="secondary"
                                  className={cn(
                                    "ml-auto text-xs",
                                    "bg-gray-100 text-gray-600"
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

      </div>
    </TooltipProvider>
  )
}
