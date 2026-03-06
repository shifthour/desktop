"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Progress } from "@/components/ui/progress"
import {
  Plus,
  Search,
  Download,
  Eye,
  Edit,
  Briefcase,
  Calendar,
  Users,
  DollarSign,
  Clock,
  CheckCircle,
  MapPin,
} from "lucide-react"

const projects = [
  {
    id: "PRJ-001",
    title: "Complete QC Laboratory Setup for PharmaCorp Ltd",
    type: "QC Laboratory",
    client: "PharmaCorp Ltd",
    location: "Mumbai, Maharashtra",
    description:
      "End-to-end QC laboratory setup including analytical instruments, compliance systems, and staff training",
    status: "In Progress",
    priority: "High",
    startDate: "2025-01-15",
    endDate: "2025-04-30",
    budget: "₹67,50,000",
    spent: "₹32,25,000",
    progress: 48,
    projectManager: "Hari Kumar K",
    teamSize: 8,
    milestones: [
      { name: "Site Survey", status: "Completed", date: "2025-01-20" },
      { name: "Equipment Procurement", status: "In Progress", date: "2025-02-15" },
      { name: "Installation", status: "Pending", date: "2025-03-10" },
      { name: "Testing & Validation", status: "Pending", date: "2025-04-15" },
      { name: "Training & Handover", status: "Pending", date: "2025-04-30" },
    ],
    equipment: ["HPLC System", "Spectrophotometer", "Dissolution Tester", "Analytical Balance"],
    riskLevel: "Medium",
  },
  {
    id: "PRJ-002",
    title: "Clean Room Construction for Biotech Research Institute",
    type: "Clean Room",
    client: "Biotech Research Institute",
    location: "Bangalore, Karnataka",
    description: "ISO Class 7 clean room construction with HVAC, monitoring systems, and validation",
    status: "Planning",
    priority: "High",
    startDate: "2025-02-01",
    endDate: "2025-07-31",
    budget: "₹1,25,00,000",
    spent: "₹15,00,000",
    progress: 12,
    projectManager: "Pauline",
    teamSize: 12,
    milestones: [
      { name: "Design Approval", status: "Completed", date: "2025-01-25" },
      { name: "Permits & Approvals", status: "In Progress", date: "2025-02-10" },
      { name: "Construction", status: "Pending", date: "2025-03-01" },
      { name: "HVAC Installation", status: "Pending", date: "2025-05-15" },
      { name: "Validation & Certification", status: "Pending", date: "2025-07-15" },
    ],
    equipment: ["HVAC System", "Air Filtration", "Monitoring System", "Pass-through Chambers"],
    riskLevel: "High",
  },
  {
    id: "PRJ-003",
    title: "Food Testing Laboratory Modernization",
    type: "Food Testing Lab",
    client: "Food Safety Authority",
    location: "Delhi, NCR",
    description: "Upgrade existing food testing lab with modern equipment and automation systems",
    status: "Completed",
    priority: "Medium",
    startDate: "2024-10-01",
    endDate: "2025-01-15",
    budget: "₹32,75,000",
    spent: "₹31,50,000",
    progress: 100,
    projectManager: "Arvind K",
    teamSize: 6,
    milestones: [
      { name: "Assessment", status: "Completed", date: "2024-10-15" },
      { name: "Equipment Selection", status: "Completed", date: "2024-11-01" },
      { name: "Installation", status: "Completed", date: "2024-12-15" },
      { name: "Testing", status: "Completed", date: "2025-01-05" },
      { name: "Training & Handover", status: "Completed", date: "2025-01-15" },
    ],
    equipment: ["PCR System", "Microbiological Analyzer", "Chromatography System"],
    riskLevel: "Low",
  },
]

const projectStats = [
  {
    title: "Active Projects",
    value: "12",
    change: { value: "+3", type: "positive" as const, period: "this quarter" },
    icon: Briefcase,
  },
  {
    title: "Total Value",
    value: "₹8.5Cr",
    change: { value: "+15.2%", type: "positive" as const, period: "this year" },
    icon: DollarSign,
  },
  {
    title: "Completion Rate",
    value: "94%",
    change: { value: "+2.1%", type: "positive" as const, period: "vs last year" },
    icon: CheckCircle,
  },
  {
    title: "Avg Duration",
    value: "4.2 months",
    change: { value: "-0.3", type: "positive" as const, period: "vs target" },
    icon: Clock,
  },
]

export function ProjectsContent() {
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedType, setSelectedType] = useState("All")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedPriority, setSelectedPriority] = useState("All")

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "completed":
        return "bg-green-100 text-green-800"
      case "in progress":
        return "bg-blue-100 text-blue-800"
      case "planning":
        return "bg-yellow-100 text-yellow-800"
      case "on hold":
        return "bg-orange-100 text-orange-800"
      case "cancelled":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority.toLowerCase()) {
      case "high":
        return "bg-red-100 text-red-800"
      case "medium":
        return "bg-yellow-100 text-yellow-800"
      case "low":
        return "bg-green-100 text-green-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getRiskColor = (risk: string) => {
    switch (risk.toLowerCase()) {
      case "high":
        return "bg-red-100 text-red-800"
      case "medium":
        return "bg-yellow-100 text-yellow-800"
      case "low":
        return "bg-green-100 text-green-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Projects Management</h1>
          <p className="text-gray-600">Manage clean room projects, QC labs, and laboratory construction</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            New Project
          </Button>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {projectStats.map((stat) => (
          <Card key={stat.title}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">{stat.title}</CardTitle>
              <stat.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
              <p className="text-xs text-muted-foreground">
                <span className={stat.change.type === "positive" ? "text-green-600" : "text-red-600"}>
                  {stat.change.value}
                </span>{" "}
                {stat.change.period}
              </p>
            </CardContent>
          </Card>
        ))}
      </div>

      <Tabs defaultValue="overview" className="space-y-6">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="timeline">Timeline</TabsTrigger>
          <TabsTrigger value="resources">Resources</TabsTrigger>
          <TabsTrigger value="reports">Reports</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="space-y-6">
          {/* Search and Filters */}
          <Card>
            <CardHeader>
              <CardTitle>Project Search & Filters</CardTitle>
              <CardDescription>Filter and search through your projects</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col md:flex-row gap-4">
                <div className="flex-1">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                    <Input
                      placeholder="Search projects by title, client, or description..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="pl-10"
                    />
                  </div>
                </div>
                <Select value={selectedType} onValueChange={setSelectedType}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Project Type" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Types</SelectItem>
                    <SelectItem value="QC Laboratory">QC Laboratory</SelectItem>
                    <SelectItem value="Clean Room">Clean Room</SelectItem>
                    <SelectItem value="Food Testing Lab">Food Testing Lab</SelectItem>
                    <SelectItem value="Research Lab">Research Lab</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={selectedStatus} onValueChange={setSelectedStatus}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Status" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Status</SelectItem>
                    <SelectItem value="Planning">Planning</SelectItem>
                    <SelectItem value="In Progress">In Progress</SelectItem>
                    <SelectItem value="Completed">Completed</SelectItem>
                    <SelectItem value="On Hold">On Hold</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={selectedPriority} onValueChange={setSelectedPriority}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Priority" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Priority</SelectItem>
                    <SelectItem value="High">High</SelectItem>
                    <SelectItem value="Medium">Medium</SelectItem>
                    <SelectItem value="Low">Low</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          {/* Projects Table */}
          <Card>
            <CardHeader>
              <CardTitle>Active Projects</CardTitle>
              <CardDescription>Laboratory construction and setup projects</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Project Details</TableHead>
                      <TableHead>Client & Location</TableHead>
                      <TableHead>Timeline</TableHead>
                      <TableHead>Budget & Progress</TableHead>
                      <TableHead>Team</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Risk</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {projects.map((project) => (
                      <TableRow key={project.id}>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="font-medium">{project.title}</div>
                            <div className="text-sm text-gray-500">{project.id}</div>
                            <Badge variant="outline">{project.type}</Badge>
                            <div className="text-xs text-gray-400 max-w-xs">{project.description}</div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="font-medium">{project.client}</div>
                            <div className="text-sm text-gray-500 flex items-center">
                              <MapPin className="w-3 h-3 mr-1" />
                              {project.location}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="text-sm">
                              <span className="font-medium">Start:</span> {project.startDate}
                            </div>
                            <div className="text-sm">
                              <span className="font-medium">End:</span> {project.endDate}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-2">
                            <div className="text-sm">
                              <span className="font-medium">Budget:</span> {project.budget}
                            </div>
                            <div className="text-sm">
                              <span className="font-medium">Spent:</span> {project.spent}
                            </div>
                            <Progress value={project.progress} className="h-2" />
                            <div className="text-xs text-gray-500">{project.progress}% complete</div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="text-sm font-medium">{project.projectManager}</div>
                            <div className="text-xs text-gray-500 flex items-center">
                              <Users className="w-3 h-3 mr-1" />
                              {project.teamSize} members
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <Badge className={getStatusColor(project.status)}>{project.status}</Badge>
                            <Badge className={getPriorityColor(project.priority)} variant="outline">
                              {project.priority}
                            </Badge>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge className={getRiskColor(project.riskLevel)}>{project.riskLevel}</Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex space-x-1">
                            <Button variant="ghost" size="sm" title="View Details">
                              <Eye className="w-4 h-4" />
                            </Button>
                            <Button variant="ghost" size="sm" title="Edit">
                              <Edit className="w-4 h-4" />
                            </Button>
                            <Button variant="ghost" size="sm" title="Timeline">
                              <Calendar className="w-4 h-4" />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="timeline" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Project Timeline & Milestones</CardTitle>
              <CardDescription>Track project milestones and key deliverables</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-6">
                {projects.map((project) => (
                  <div key={project.id} className="border rounded-lg p-4">
                    <div className="flex items-center justify-between mb-4">
                      <div>
                        <h3 className="font-semibold">{project.title}</h3>
                        <p className="text-sm text-gray-500">{project.client}</p>
                      </div>
                      <Badge className={getStatusColor(project.status)}>{project.status}</Badge>
                    </div>
                    <div className="space-y-3">
                      {project.milestones.map((milestone, index) => (
                        <div key={index} className="flex items-center space-x-4">
                          <div className="w-4 h-4 rounded-full border-2 border-gray-300 flex items-center justify-center">
                            {milestone.status === "Completed" && <CheckCircle className="w-3 h-3 text-green-600" />}
                            {milestone.status === "In Progress" && <div className="w-2 h-2 bg-blue-600 rounded-full" />}
                          </div>
                          <div className="flex-1">
                            <div className="flex items-center justify-between">
                              <span className="font-medium">{milestone.name}</span>
                              <span className="text-sm text-gray-500">{milestone.date}</span>
                            </div>
                            <Badge
                              variant="outline"
                              className={
                                milestone.status === "Completed"
                                  ? "text-green-600"
                                  : milestone.status === "In Progress"
                                    ? "text-blue-600"
                                    : "text-gray-600"
                              }
                            >
                              {milestone.status}
                            </Badge>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
