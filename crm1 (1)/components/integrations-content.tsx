"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Switch } from "@/components/ui/switch"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { StatusIndicator } from "@/components/ui/status-indicator"
import {
  Zap,
  Mail,
  FileText,
  Phone,
  MessageSquare,
  AlertCircle,
  Plus,
  Download,
  Workflow,
  Bot,
  Clock,
} from "lucide-react"

const integrations = [
  {
    name: "Google Workspace",
    description: "Email, Calendar, and Drive integration",
    icon: Mail,
    status: "connected",
    lastSync: "2 minutes ago",
    features: ["Email sync", "Calendar integration", "Document sharing"],
    enabled: true,
  },
  {
    name: "WhatsApp Business",
    description: "Customer communication via WhatsApp",
    icon: MessageSquare,
    status: "connected",
    lastSync: "5 minutes ago",
    features: ["Message sync", "Media sharing", "Group messaging"],
    enabled: true,
  },
  {
    name: "Zoom",
    description: "Video conferencing and meetings",
    icon: Phone,
    status: "available",
    lastSync: "Not connected",
    features: ["Meeting scheduling", "Recording", "Screen sharing"],
    enabled: false,
  },
  {
    name: "DocuSign",
    description: "Digital document signing",
    icon: FileText,
    status: "available",
    lastSync: "Not connected",
    features: ["E-signatures", "Document tracking", "Templates"],
    enabled: false,
  },
]

const automations = [
  {
    name: "Lead Assignment",
    description: "Automatically assign leads based on territory and expertise",
    trigger: "New lead created",
    action: "Assign to sales rep",
    status: "active",
    runsToday: 12,
  },
  {
    name: "Follow-up Reminders",
    description: "Send automatic follow-up reminders for pending activities",
    trigger: "Activity overdue",
    action: "Send notification",
    status: "active",
    runsToday: 8,
  },
  {
    name: "Quote Expiry Alerts",
    description: "Alert customers before quote expiration",
    trigger: "Quote expires in 3 days",
    action: "Send email reminder",
    status: "active",
    runsToday: 3,
  },
  {
    name: "Customer Satisfaction Survey",
    description: "Send satisfaction surveys after service completion",
    trigger: "Service marked complete",
    action: "Send survey email",
    status: "paused",
    runsToday: 0,
  },
]

const stats = [
  {
    title: "Active Integrations",
    value: "8",
    change: { value: "+2", type: "positive" as const, period: "this month" },
    icon: Zap,
  },
  {
    title: "Automated Tasks",
    value: "156",
    change: { value: "+23", type: "positive" as const, period: "today" },
    icon: Bot,
  },
  {
    title: "Time Saved",
    value: "24h",
    change: { value: "+3h", type: "positive" as const, period: "this week" },
    icon: Clock,
  },
  {
    title: "Error Rate",
    value: "0.2%",
    change: { value: "-0.1%", type: "positive" as const, period: "this month" },
    icon: AlertCircle,
  },
]

export function IntegrationsContent() {
  const getStatusColor = (status: string) => {
    switch (status) {
      case "connected":
        return "success"
      case "available":
        return "default"
      case "error":
        return "error"
      default:
        return "default"
    }
  }

  const getAutomationStatusColor = (status: string) => {
    switch (status) {
      case "active":
        return "success"
      case "paused":
        return "warning"
      case "error":
        return "error"
      default:
        return "default"
    }
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Integrations & Automation</h1>
          <p className="text-gray-600">Connect your tools and automate workflows</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export Logs
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Add Integration
          </Button>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stats.map((stat) => (
          <EnhancedCard key={stat.title} title={stat.title} value={stat.value} change={stat.change} icon={stat.icon} />
        ))}
      </div>

      {/* Available Integrations */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Zap className="w-5 h-5 mr-2" />
            Available Integrations
          </CardTitle>
          <CardDescription>Connect your favorite tools and services</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {integrations.map((integration) => (
              <div key={integration.name} className="border rounded-lg p-4 hover:bg-gray-50">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center space-x-3">
                    <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                      <integration.icon className="w-5 h-5 text-blue-600" />
                    </div>
                    <div>
                      <h3 className="font-semibold">{integration.name}</h3>
                      <p className="text-sm text-gray-600">{integration.description}</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-2">
                    <StatusIndicator
                      status={integration.status}
                      variant={getStatusColor(integration.status)}
                      size="sm"
                    />
                    <Switch checked={integration.enabled} />
                  </div>
                </div>
                <div className="space-y-2">
                  <div className="flex flex-wrap gap-1">
                    {integration.features.map((feature) => (
                      <Badge key={feature} variant="outline" className="text-xs">
                        {feature}
                      </Badge>
                    ))}
                  </div>
                  <p className="text-xs text-gray-500">Last sync: {integration.lastSync}</p>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Workflow Automations */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Workflow className="w-5 h-5 mr-2" />
            Workflow Automations
          </CardTitle>
          <CardDescription>Automated processes to streamline your operations</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {automations.map((automation, index) => (
              <div key={index} className="border rounded-lg p-4 hover:bg-gray-50">
                <div className="flex items-center justify-between mb-3">
                  <div>
                    <h3 className="font-semibold">{automation.name}</h3>
                    <p className="text-sm text-gray-600">{automation.description}</p>
                  </div>
                  <div className="flex items-center space-x-2">
                    <StatusIndicator
                      status={automation.status}
                      variant={getAutomationStatusColor(automation.status)}
                      size="sm"
                    />
                    <Switch checked={automation.status === "active"} />
                  </div>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                  <div>
                    <p className="text-gray-500">Trigger</p>
                    <p className="font-medium">{automation.trigger}</p>
                  </div>
                  <div>
                    <p className="text-gray-500">Action</p>
                    <p className="font-medium">{automation.action}</p>
                  </div>
                  <div>
                    <p className="text-gray-500">Runs Today</p>
                    <p className="font-medium">{automation.runsToday}</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
