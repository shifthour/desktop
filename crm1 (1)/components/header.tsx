"use client"

import { useState, useEffect } from "react"
import { LogOut, Key, Bell, CheckCircle, Clock, AlertTriangle, Users, Building2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"

interface Notification {
  id: string
  title: string
  message: string
  type: 'info' | 'success' | 'warning' | 'error'
  is_read: boolean
  created_at: string
}

export function Header() {
  const [user, setUser] = useState<any>({})
  const [realNotifications, setRealNotifications] = useState<Notification[]>([])
  const [loading, setLoading] = useState(false)
  const [companyLogo, setCompanyLogo] = useState<string>("")

  // Load user data from localStorage
  const loadUserData = () => {
    if (typeof window !== 'undefined') {
      const storedUser = localStorage.getItem('user')
      if (!storedUser) return
      try {
        const parsed = JSON.parse(storedUser)
        // If user has fake ID, clear localStorage and redirect to login
        if (parsed.id === 'super-admin-id') {
          localStorage.removeItem('user')
          window.location.href = '/adminlogin'
          return
        }
        setUser(parsed)
        // Load company logo from Supabase
        loadCompanyLogo(parsed.company_id)
      } catch {
        localStorage.removeItem('user')
        setUser({})
      }
    }
  }

  // Load company logo from Supabase
  const loadCompanyLogo = async (companyId: string) => {
    if (!companyId) return
    
    try {
      const response = await fetch(`/api/admin/companies?companyId=${companyId}`)
      if (response.ok) {
        const company = await response.json()
        if (company.logo_url) {
          setCompanyLogo(company.logo_url)
        }
      }
    } catch (error) {
      console.error('Error loading company logo:', error)
    }
  }

  useEffect(() => {
    loadUserData()
    
    // Listen for company details updates
    const handleCompanyUpdate = () => {
      loadUserData()
    }
    
    window.addEventListener('refreshNotifications', handleCompanyUpdate)
    window.addEventListener('companyDetailsUpdated', handleCompanyUpdate)
    
    return () => {
      window.removeEventListener('refreshNotifications', handleCompanyUpdate)
      window.removeEventListener('companyDetailsUpdated', handleCompanyUpdate)
    }
  }, [])

  const handleLogout = () => {
    localStorage.removeItem('user')
    // Redirect to appropriate login page based on user type
    if (user.is_super_admin) {
      window.location.href = '/adminlogin'
    } else {
      window.location.href = '/login'  
    }
  }

  const handleChangePassword = () => {
    window.location.href = '/change-password'
  }

  // Fetch real notifications from API
  const fetchNotifications = async () => {
    if (!user.id) {
      console.log('No user ID available for fetching notifications')
      return
    }
    
    console.log('Fetching notifications for user:', user.id, 'isSuperAdmin:', user.is_super_admin)
    setLoading(true)
    try {
      const response = await fetch(`/api/notifications?userId=${user.id}&isSuperAdmin=${user.is_super_admin}&limit=10`)
      if (response.ok) {
        const data = await response.json()
        console.log('Received notifications:', data)
        setRealNotifications(data || [])
      } else {
        console.error('Failed to fetch notifications:', response.status, response.statusText)
      }
    } catch (error) {
      console.error('Error fetching notifications:', error)
    } finally {
      setLoading(false)
    }
  }

  // Load notifications when component mounts and user is available
  useEffect(() => {
    if (user.id) {
      fetchNotifications()
      // Refresh notifications every 5 seconds for real-time updates
      const interval = setInterval(fetchNotifications, 5000)
      return () => clearInterval(interval)
    }
  }, [user.id])

  // Listen for custom events to refresh notifications immediately
  useEffect(() => {
    const handleRefreshNotifications = () => {
      console.log('Refreshing notifications due to action...')
      fetchNotifications()
    }

    window.addEventListener('refreshNotifications', handleRefreshNotifications)
    return () => window.removeEventListener('refreshNotifications', handleRefreshNotifications)
  }, [])

  // Mark notification as read
  const markAsRead = async (notificationId: string) => {
    try {
      await fetch('/api/notifications', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ notificationId })
      })
      // Update local state
      setRealNotifications(prev => 
        prev.map(n => n.id === notificationId ? { ...n, is_read: true } : n)
      )
    } catch (error) {
      console.error('Error marking notification as read:', error)
    }
  }

  // Helper function to get time ago
  const getTimeAgo = (dateString: string) => {
    const date = new Date(dateString)
    const now = new Date()
    const diffInMs = now.getTime() - date.getTime()
    const diffInMinutes = Math.floor(diffInMs / 60000)
    
    if (diffInMinutes < 1) return 'Just now'
    if (diffInMinutes < 60) return `${diffInMinutes}m ago`
    
    const diffInHours = Math.floor(diffInMinutes / 60)
    if (diffInHours < 24) return `${diffInHours}h ago`
    
    const diffInDays = Math.floor(diffInHours / 24)
    return `${diffInDays}d ago`
  }

  // Helper function to get icon for notification type
  const getIconForType = (type: string) => {
    switch(type) {
      case 'success': return CheckCircle
      case 'warning': return AlertTriangle
      case 'error': return AlertTriangle
      case 'info': 
      default: return Users
    }
  }

  // ONLY show real notifications - no dummy data fallback
  const getNotifications = () => {
    // Always return real notifications only
    return realNotifications.map(n => ({
      id: n.id,
      type: n.type,
      title: n.title,
      message: n.message,
      time: getTimeAgo(n.created_at),
      icon: getIconForType(n.type),
      unread: !n.is_read
    }))
  }

  const notifications = getNotifications()
  const unreadCount = notifications.filter(n => n.unread).length

  const getNotificationIcon = (type: string) => {
    switch(type) {
      case 'success': return 'text-green-600'
      case 'warning': return 'text-yellow-600'
      case 'error': return 'text-red-600'
      default: return 'text-blue-600'
    }
  }

  const getNotificationBg = (type: string) => {
    switch(type) {
      case 'success': return 'bg-green-50 border-green-200'
      case 'warning': return 'bg-yellow-50 border-yellow-200' 
      case 'error': return 'bg-red-50 border-red-200'
      default: return 'bg-blue-50 border-blue-200'
    }
  }

  // Get role display name
  const getRoleDisplayName = () => {
    if (user.is_super_admin) return 'Super Admin'
    if (user.is_admin) return 'Company Admin'
    if (user.role?.name) {
      return user.role.name
        .replace(/_/g, ' ')
        .replace(/\b\w/g, (l: string) => l.toUpperCase())
    }
    return 'User'
  }

  const finalCompanyLogo = companyLogo || user.company?.logo_url || "/company-logo.png"
  const companyName = user.company?.name || "LabGig"

  return (
    <header className="bg-white border-b border-gray-200 px-6 py-4 shadow-sm">
      <div className="flex items-center justify-between">
        {/* Company Logo Section */}
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-3">
            <div className="w-10 h-10 bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg flex items-center justify-center overflow-hidden">
              <img
                src={finalCompanyLogo || "/placeholder.svg"}
                alt={`${companyName} Logo`}
                className="w-full h-full object-contain"
                onError={(e) => {
                  // Fallback to text logo if image fails to load
                  const target = e.target as HTMLImageElement
                  target.style.display = "none"
                  target.nextElementSibling?.classList.remove("hidden")
                }}
              />
              <span className="text-white font-bold text-sm hidden">{companyName.charAt(0)}</span>
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-900">{companyName}</h1>
              <p className="text-xs text-gray-500">
                CRM System.
                <br />
                AI Powered
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center space-x-4">

          {/* Notifications Dropdown */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="relative hover:bg-gray-50 rounded-full p-2">
                <Bell className="w-5 h-5 text-gray-600" />
                {unreadCount > 0 && (
                  <Badge 
                    className="absolute -top-1 -right-1 w-5 h-5 p-0 text-xs bg-red-500 text-white rounded-full flex items-center justify-center"
                  >
                    {unreadCount > 9 ? '9+' : unreadCount}
                  </Badge>
                )}
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-80">
              <DropdownMenuLabel>
                <div className="flex items-center justify-between">
                  <span>Notifications</span>
                  {unreadCount > 0 && (
                    <Badge variant="secondary" className="text-xs">
                      {unreadCount} new
                    </Badge>
                  )}
                </div>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <ScrollArea className="h-96">
                <div className="p-1">
                  {notifications.length > 0 ? (
                    notifications.map((notification) => {
                      const IconComponent = notification.icon
                      return (
                        <div
                          key={notification.id}
                          className={`p-3 mb-2 rounded-lg border cursor-pointer transition-colors hover:bg-gray-50 ${
                            notification.unread ? getNotificationBg(notification.type) : 'bg-gray-50 border-gray-200'
                          }`}
                          onClick={() => {
                            if (notification.unread && typeof notification.id === 'string') {
                              markAsRead(notification.id)
                            }
                          }}
                        >
                          <div className="flex items-start space-x-3">
                            <div className={`p-1 rounded-full ${notification.unread ? 'bg-white' : 'bg-gray-200'}`}>
                              <IconComponent className={`w-4 h-4 ${getNotificationIcon(notification.type)}`} />
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between">
                                <h4 className={`text-sm font-medium truncate ${
                                  notification.unread ? 'text-gray-900' : 'text-gray-600'
                                }`}>
                                  {notification.title}
                                </h4>
                                {notification.unread && (
                                  <div className="w-2 h-2 bg-blue-500 rounded-full ml-2 flex-shrink-0"></div>
                                )}
                              </div>
                              <p className={`text-sm mt-1 ${
                                notification.unread ? 'text-gray-700' : 'text-gray-500'
                              }`}>
                                {notification.message}
                              </p>
                              <p className="text-xs text-gray-400 mt-1">{notification.time}</p>
                            </div>
                          </div>
                        </div>
                      )
                    })
                  ) : (
                    <div className="p-6 text-center text-gray-500">
                      <Bell className="w-8 h-8 mx-auto mb-2 text-gray-300" />
                      <p className="text-sm">No notifications</p>
                    </div>
                  )}
                </div>
              </ScrollArea>
              {notifications.length > 0 && (
                <>
                  <DropdownMenuSeparator />
                  <div className="p-2">
                    <Button variant="ghost" className="w-full text-sm text-blue-600 hover:text-blue-700">
                      View all notifications
                    </Button>
                  </div>
                </>
              )}
            </DropdownMenuContent>
          </DropdownMenu>

          {/* Enhanced User Menu */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="flex items-center space-x-3 hover:bg-gray-50 rounded-lg p-2">
                <Avatar className="w-8 h-8">
                  <AvatarImage src="/placeholder-user.jpg" />
                  <AvatarFallback className="bg-gradient-to-br from-blue-500 to-blue-600 text-white">
                    {user.full_name ? user.full_name.split(' ').map(n => n[0]).join('') : 'U'}
                  </AvatarFallback>
                </Avatar>
                <div className="text-left hidden md:block">
                  <p className="text-sm font-medium">{user.full_name || 'User'}</p>
                  <p className="text-xs text-gray-500">{getRoleDisplayName()}</p>
                </div>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-56">
              <DropdownMenuLabel>
                <div className="flex flex-col space-y-1">
                  <p className="text-sm font-medium">{user.full_name || 'User'}</p>
                  <p className="text-xs text-muted-foreground">{user.email || 'user@labgig.com'}</p>
                </div>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={handleChangePassword} className="cursor-pointer">
                <Key className="mr-2 h-4 w-4" />
                Change Password
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={handleLogout} className="text-red-600 cursor-pointer">
                <LogOut className="mr-2 h-4 w-4" />
                Log out
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </header>
  )
}
