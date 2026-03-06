"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { StatusIndicator } from "@/components/ui/status-indicator"
import {
  Plus,
  Search,
  Download,
  Eye,
  Edit,
  Users,
  Target,
  Star,
  MapPin,
  Briefcase,
  Award,
  TrendingUp,
  Calendar,
} from "lucide-react"

const gigWorkers = [
  {
    id: "GW-001",
    name: "Dr. Rajesh Kumar",
    specialization: "Laboratory Setup Consultant",
    rating: 4.9,
    completedProjects: 23,
    location: "Bangalore, Karnataka",
    availability: "Available",
    hourlyRate: "₹2,500",
    skills: ["Clean Room Design", "HVAC Systems", "Compliance"],
    lastActive: "2 hours ago",
    currentProject: "Biotech Lab Setup - Phase 2",
  },
  {
    id: "GW-002",
    name: "Ms. Priya Sharma",
    specialization: "Analytical Instrument Specialist",
    rating: 4.8,
    completedProjects: 31,
    location: "Mumbai, Maharashtra",
    availability: "Busy",
    hourlyRate: "₹2,200",
    skills: ["HPLC", "GC-MS", "Method Development"],
    lastActive: "1 day ago",
    currentProject: "Pharma QC Lab Validation",
  },
  {
    id: "GW-003",
    name: "Mr. Arun Patel",
    specialization: "Service Engineer",
    rating: 4.7,
    completedProjects: 45,
    location: "Ahmedabad, Gujarat",
    availability: "Available",
    hourlyRate: "₹1,800",
    skills: ["Equipment Maintenance", "Troubleshooting", "Training"],
    lastActive: "30 minutes ago",
    currentProject: null,
  },
]

const activeProjects = [
  {
    id: "PRJ-001",
    title: "Complete Laboratory Setup - Biotech Research Institute",
    client: "Biotech Research Institute",
    assignedWorkers: 3,
    progress: 75,
    deadline: "2025-08-15",
    budget: "₹45,00,000",
    status: "In Progress",
    priority: "High",
  },
  {
    id: "PRJ-002",
    title: "QC Laboratory Modernization - PharmaCorp",
    client: "PharmaCorp Ltd",
    assignedWorkers: 2,
    progress: 45,
    deadline: "2025-09-30",
    budget: "₹32,50,000",
    status: "Planning",
    priority: "Medium",
  },
]

const stats = [
  {
    title: "Active Gig Workers",
    value: "156",
    change: { value: "+12", type: "positive" as const, period: "this month" },
    icon: Users,
  },
  {
    title: "Completed Projects",
    value: "89",
    change: { value: "+8", type: "positive" as const, period: "this month" },
    icon: Target,
  },
  {
    title: "Average Rating",
    value: "4.8",
    change: { value: "+0.2", type: "positive" as const, period: "this quarter" },
    icon: Star,
  },
  {
    title: "Revenue Generated",
    value: "₹2.8Cr",
    change: { value: "+15%", type: "positive" as const, period: "this quarter" },
    icon: TrendingUp,
  },
]

export function GigWorkspaceContent() {
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedSpecialization, setSelectedSpecialization] = useState("All")
  const [selectedAvailability, setSelectedAvailability] = useState("All")

  const getAvailabilityColor = (availability: string) => {
    switch (availability.toLowerCase()) {
      case "available":
        return "success"
      case "busy":
        return "warning"
      case "offline":
        return "error"
      default:
        return "default"
    }
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Gig Workspace</h1>
          <p className="text-gray-600">Manage freelancers, projects, and gig economy workforce</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700">
            <Plus className="w-4 h-4 mr-2" />
            Add Gig Worker
          </Button>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stats.map((stat) => (
          <EnhancedCard key={stat.title} title={stat.title} value={stat.value} change={stat.change} icon={stat.icon} />
        ))}
      </div>

      {/* Active Projects Overview */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Briefcase className="w-5 h-5 mr-2" />
            Active Projects
          </CardTitle>
          <CardDescription>Current gig-based projects and their progress</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {activeProjects.map((project) => (
              <div key={project.id} className="border rounded-lg p-4 hover:bg-gray-50">
                <div className="flex items-center justify-between mb-3">
                  <div>
                    <h3 className="font-semibold">{project.title}</h3>
                    <p className="text-sm text-gray-600">Client: {project.client}</p>
                  </div>
                  <div className="flex items-center space-x-2">
                    <StatusIndicator
                      status={project.priority}
                      variant={project.priority === "High" ? "error" : "warning"}
                      size="sm"
                    />
                    <Badge variant="outline">{project.status}</Badge>
                  </div>
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                  <div>
                    <p className="text-gray-500">Workers</p>
                    <p className="font-medium">{project.assignedWorkers}</p>
                  </div>
                  <div>
                    <p className="text-gray-500">Progress</p>
                    <p className="font-medium">{project.progress}%</p>
                  </div>
                  <div>
                    <p className="text-gray-500">Budget</p>
                    <p className="font-medium">{project.budget}</p>
                  </div>
                  <div>
                    <p className="text-gray-500">Deadline</p>
                    <p className="font-medium">{project.deadline}</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Gig Workers Management */}
      <Card>
        <CardHeader>
          <CardTitle>Gig Workers Directory</CardTitle>
          <CardDescription>Manage your network of freelance specialists</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col md:flex-row gap-4 mb-6">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <Input
                  placeholder="Search by name, skills, or location..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
            <Select value={selectedSpecialization} onValueChange={setSelectedSpecialization}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Specialization" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="All">All Specializations</SelectItem>
                <SelectItem value="Laboratory Setup">Laboratory Setup</SelectItem>
                <SelectItem value="Analytical Instruments">Analytical Instruments</SelectItem>
                <SelectItem value="Service Engineering">Service Engineering</SelectItem>
              </SelectContent>
            </Select>
            <Select value={selectedAvailability} onValueChange={setSelectedAvailability}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Availability" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="All">All</SelectItem>
                <SelectItem value="Available">Available</SelectItem>
                <SelectItem value="Busy">Busy</SelectItem>
                <SelectItem value="Offline">Offline</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-4">
            {gigWorkers.map((worker) => (
              <div key={worker.id} className="border rounded-lg p-4 hover:bg-gray-50">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-4">
                    <div className="w-12 h-12 bg-gradient-to-br from-blue-500 to-blue-600 rounded-full flex items-center justify-center text-white font-semibold">
                      {worker.name
                        .split(" ")
                        .map((n) => n[0])
                        .join("")}
                    </div>
                    <div>
                      <h3 className="font-semibold">{worker.name}</h3>
                      <p className="text-sm text-gray-600">{worker.specialization}</p>
                      <div className="flex items-center space-x-4 mt-1">
                        <div className="flex items-center space-x-1">
                          <Star className="w-4 h-4 text-yellow-400 fill-current" />
                          <span className="text-sm font-medium">{worker.rating}</span>
                        </div>
                        <div className="flex items-center space-x-1">
                          <Award className="w-4 h-4 text-gray-400" />
                          <span className="text-sm text-gray-600">{worker.completedProjects} projects</span>
                        </div>
                        <div className="flex items-center space-x-1">
                          <MapPin className="w-4 h-4 text-gray-400" />
                          <span className="text-sm text-gray-600">{worker.location}</span>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="flex items-center space-x-2 mb-2">
                      <StatusIndicator
                        status={worker.availability}
                        variant={getAvailabilityColor(worker.availability)}
                        size="sm"
                      />
                      <span className="text-sm font-medium">{worker.hourlyRate}/hr</span>
                    </div>
                    <div className="flex space-x-1">
                      <Button variant="ghost" size="sm">
                        <Eye className="w-4 h-4" />
                      </Button>
                      <Button variant="ghost" size="sm">
                        <Edit className="w-4 h-4" />
                      </Button>
                      <Button variant="ghost" size="sm">
                        <Calendar className="w-4 h-4" />
                      </Button>
                    </div>
                  </div>
                </div>
                <div className="mt-3">
                  <div className="flex flex-wrap gap-2">
                    {worker.skills.map((skill) => (
                      <Badge key={skill} variant="outline" className="text-xs">
                        {skill}
                      </Badge>
                    ))}
                  </div>
                  {worker.currentProject && (
                    <p className="text-sm text-blue-600 mt-2">Current: {worker.currentProject}</p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
