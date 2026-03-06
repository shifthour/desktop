"use client"

import { useState, useRef, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Mic,
  Play,
  Square,
  Download,
  Upload,
  Search,
  Clock,
  FileAudio,
  MessageSquare,
  CheckCircle,
  Eye,
  Edit,
  Volume2,
} from "lucide-react"

const voiceReports = [
  {
    id: "VR-001",
    title: "Customer Visit Report - TSAR Labcare",
    employee: "Prashanth S.",
    role: "Sales Manager",
    date: "2025-01-15",
    time: "14:30",
    duration: "5:42",
    status: "Transcribed",
    category: "Customer Visit",
    priority: "High",
    transcription:
      "Visited TSAR Labcare today to discuss their requirement for analytical balance. Customer is interested in AS 220.R2 model. They need quotation by end of this week. Follow up required with technical specifications.",
    audioUrl: "/audio/sample1.mp3",
    tags: ["customer-visit", "quotation", "analytical-balance"],
    relatedLead: "LEAD-298",
    relatedAccount: "ACC-1245",
  },
  {
    id: "VR-002",
    title: "Installation Feedback - Eurofins Lab",
    employee: "Hari Kumar K",
    role: "Service Engineer",
    date: "2025-01-14",
    time: "16:15",
    duration: "3:28",
    status: "Processing",
    category: "Installation",
    priority: "Medium",
    transcription:
      "Installation completed successfully at Eurofins. Customer satisfied with the setup. Minor calibration issue resolved. Training provided to lab technicians. Warranty documentation handed over.",
    audioUrl: "/audio/sample2.mp3",
    tags: ["installation", "training", "warranty"],
    relatedLead: null,
    relatedAccount: "ACC-0892",
  },
  {
    id: "VR-003",
    title: "Complaint Resolution Update",
    employee: "Pauline",
    role: "Technical Support",
    date: "2025-01-13",
    time: "11:20",
    duration: "4:15",
    status: "Transcribed",
    category: "Complaint",
    priority: "High",
    transcription:
      "Resolved the calibration issue reported by customer. Replaced faulty sensor. Customer testing ongoing. Will follow up tomorrow for confirmation. Preventive maintenance scheduled for next month.",
    audioUrl: "/audio/sample3.mp3",
    tags: ["complaint", "calibration", "sensor-replacement"],
    relatedLead: null,
    relatedAccount: "ACC-0567",
  },
]

const voiceStats = [
  {
    title: "Total Reports",
    value: "156",
    change: { value: "+23", type: "positive" as const, period: "this week" },
    icon: FileAudio,
  },
  {
    title: "Transcribed",
    value: "142",
    change: { value: "+19", type: "positive" as const, period: "this week" },
    icon: MessageSquare,
  },
  {
    title: "Processing",
    value: "14",
    change: { value: "+4", type: "neutral" as const, period: "pending" },
    icon: Clock,
  },
  {
    title: "Avg Duration",
    value: "4:32",
    change: { value: "-0:15", type: "positive" as const, period: "vs last week" },
    icon: Volume2,
  },
]

export function VoiceReportsContent() {
  const [isRecording, setIsRecording] = useState(false)
  const [recordingTime, setRecordingTime] = useState(0)
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedCategory, setSelectedCategory] = useState("All")
  const [selectedStatus, setSelectedStatus] = useState("All")
  const [selectedEmployee, setSelectedEmployee] = useState("All")
  const [newReportTitle, setNewReportTitle] = useState("")
  const [newReportCategory, setNewReportCategory] = useState("")
  const [transcriptionText, setTranscriptionText] = useState("")

  const mediaRecorderRef = useRef<MediaRecorder | null>(null)
  const audioChunksRef = useRef<Blob[]>([])

  useEffect(() => {
    let interval: NodeJS.Timeout
    if (isRecording) {
      interval = setInterval(() => {
        setRecordingTime((prev) => prev + 1)
      }, 1000)
    }
    return () => clearInterval(interval)
  }, [isRecording])

  const startRecording = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      const mediaRecorder = new MediaRecorder(stream)
      mediaRecorderRef.current = mediaRecorder
      audioChunksRef.current = []

      mediaRecorder.ondataavailable = (event) => {
        audioChunksRef.current.push(event.data)
      }

      mediaRecorder.onstop = () => {
        const audioBlob = new Blob(audioChunksRef.current, { type: "audio/wav" })
        // Here you would typically upload the audio for transcription
        console.log("Recording stopped, audio blob created:", audioBlob)

        // Simulate transcription process
        setTimeout(() => {
          setTranscriptionText(
            "This is a simulated transcription of your voice report. In a real implementation, this would be processed by a speech-to-text service.",
          )
        }, 2000)
      }

      mediaRecorder.start()
      setIsRecording(true)
      setRecordingTime(0)
    } catch (error) {
      console.error("Error starting recording:", error)
    }
  }

  const stopRecording = () => {
    if (mediaRecorderRef.current && isRecording) {
      mediaRecorderRef.current.stop()
      mediaRecorderRef.current.stream.getTracks().forEach((track) => track.stop())
      setIsRecording(false)
    }
  }

  const formatTime = (seconds: number) => {
    const mins = Math.floor(seconds / 60)
    const secs = seconds % 60
    return `${mins}:${secs.toString().padStart(2, "0")}`
  }

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "transcribed":
        return "bg-green-100 text-green-800"
      case "processing":
        return "bg-yellow-100 text-yellow-800"
      case "failed":
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

  return (
    <div className="p-6 space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Voice Reports & Transcription</h1>
          <p className="text-gray-600">Record, transcribe, and manage voice reports from field activities</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button variant="outline">
            <Upload className="w-4 h-4 mr-2" />
            Upload Audio
          </Button>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {voiceStats.map((stat) => (
          <Card key={stat.title}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">{stat.title}</CardTitle>
              <stat.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
              <p className="text-xs text-muted-foreground">
                <span
                  className={
                    stat.change.type === "positive"
                      ? "text-green-600"
                      : stat.change.type === "negative"
                        ? "text-red-600"
                        : "text-gray-600"
                  }
                >
                  {stat.change.value}
                </span>{" "}
                {stat.change.period}
              </p>
            </CardContent>
          </Card>
        ))}
      </div>

      <Tabs defaultValue="record" className="space-y-6">
        <TabsList>
          <TabsTrigger value="record">Record New</TabsTrigger>
          <TabsTrigger value="reports">Voice Reports</TabsTrigger>
          <TabsTrigger value="analytics">Analytics</TabsTrigger>
        </TabsList>

        <TabsContent value="record" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Record New Voice Report</CardTitle>
              <CardDescription>Record your field activities, customer visits, or updates</CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="text-sm font-medium">Report Title</label>
                  <Input
                    placeholder="Enter report title..."
                    value={newReportTitle}
                    onChange={(e) => setNewReportTitle(e.target.value)}
                  />
                </div>
                <div>
                  <label className="text-sm font-medium">Category</label>
                  <Select value={newReportCategory} onValueChange={setNewReportCategory}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select category" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="customer-visit">Customer Visit</SelectItem>
                      <SelectItem value="installation">Installation</SelectItem>
                      <SelectItem value="complaint">Complaint</SelectItem>
                      <SelectItem value="training">Training</SelectItem>
                      <SelectItem value="maintenance">Maintenance</SelectItem>
                      <SelectItem value="sales-update">Sales Update</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* Recording Interface */}
              <div className="flex flex-col items-center space-y-4 p-8 border-2 border-dashed border-gray-300 rounded-lg">
                <div className="w-24 h-24 bg-blue-100 rounded-full flex items-center justify-center">
                  {isRecording ? (
                    <div className="w-6 h-6 bg-red-500 rounded-full animate-pulse" />
                  ) : (
                    <Mic className="w-8 h-8 text-blue-600" />
                  )}
                </div>

                <div className="text-center">
                  <div className="text-2xl font-bold text-gray-900">{formatTime(recordingTime)}</div>
                  <div className="text-sm text-gray-500">
                    {isRecording ? "Recording in progress..." : "Ready to record"}
                  </div>
                </div>

                <div className="flex space-x-4">
                  {!isRecording ? (
                    <Button onClick={startRecording} className="bg-red-600 hover:bg-red-700">
                      <Mic className="w-4 h-4 mr-2" />
                      Start Recording
                    </Button>
                  ) : (
                    <Button onClick={stopRecording} variant="outline">
                      <Square className="w-4 h-4 mr-2" />
                      Stop Recording
                    </Button>
                  )}
                </div>
              </div>

              {/* Transcription Area */}
              {transcriptionText && (
                <div className="space-y-2">
                  <label className="text-sm font-medium">Transcription</label>
                  <Textarea
                    placeholder="Transcription will appear here..."
                    value={transcriptionText}
                    onChange={(e) => setTranscriptionText(e.target.value)}
                    rows={6}
                  />
                  <div className="flex justify-end space-x-2">
                    <Button variant="outline">
                      <Edit className="w-4 h-4 mr-2" />
                      Edit
                    </Button>
                    <Button className="bg-green-600 hover:bg-green-700">
                      <CheckCircle className="w-4 h-4 mr-2" />
                      Save Report
                    </Button>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="reports" className="space-y-6">
          {/* Search and Filters */}
          <Card>
            <CardHeader>
              <CardTitle>Search & Filter Reports</CardTitle>
              <CardDescription>Find and filter voice reports</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col md:flex-row gap-4">
                <div className="flex-1">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                    <Input
                      placeholder="Search reports by title, employee, or content..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="pl-10"
                    />
                  </div>
                </div>
                <Select value={selectedCategory} onValueChange={setSelectedCategory}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Category" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Categories</SelectItem>
                    <SelectItem value="Customer Visit">Customer Visit</SelectItem>
                    <SelectItem value="Installation">Installation</SelectItem>
                    <SelectItem value="Complaint">Complaint</SelectItem>
                    <SelectItem value="Training">Training</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={selectedStatus} onValueChange={setSelectedStatus}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Status" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Status</SelectItem>
                    <SelectItem value="Transcribed">Transcribed</SelectItem>
                    <SelectItem value="Processing">Processing</SelectItem>
                    <SelectItem value="Failed">Failed</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={selectedEmployee} onValueChange={setSelectedEmployee}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Employee" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Employees</SelectItem>
                    <SelectItem value="Prashanth S.">Prashanth S.</SelectItem>
                    <SelectItem value="Hari Kumar K">Hari Kumar K</SelectItem>
                    <SelectItem value="Pauline">Pauline</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          {/* Reports Table */}
          <Card>
            <CardHeader>
              <CardTitle>Voice Reports</CardTitle>
              <CardDescription>All recorded voice reports and their transcriptions</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Report Details</TableHead>
                      <TableHead>Employee</TableHead>
                      <TableHead>Date & Time</TableHead>
                      <TableHead>Duration</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Category</TableHead>
                      <TableHead>Priority</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {voiceReports.map((report) => (
                      <TableRow key={report.id}>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="font-medium">{report.title}</div>
                            <div className="text-sm text-gray-500">{report.id}</div>
                            <div className="text-xs text-gray-400 max-w-xs truncate">{report.transcription}</div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="font-medium">{report.employee}</div>
                            <div className="text-sm text-gray-500">{report.role}</div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <div className="font-medium">{report.date}</div>
                            <div className="text-sm text-gray-500">{report.time}</div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline">{report.duration}</Badge>
                        </TableCell>
                        <TableCell>
                          <Badge className={getStatusColor(report.status)}>{report.status}</Badge>
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline">{report.category}</Badge>
                        </TableCell>
                        <TableCell>
                          <Badge className={getPriorityColor(report.priority)}>{report.priority}</Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex space-x-1">
                            <Button variant="ghost" size="sm" title="View/Listen">
                              <Eye className="w-4 h-4" />
                            </Button>
                            <Button variant="ghost" size="sm" title="Play Audio">
                              <Play className="w-4 h-4" />
                            </Button>
                            <Button variant="ghost" size="sm" title="Edit">
                              <Edit className="w-4 h-4" />
                            </Button>
                            <Button variant="ghost" size="sm" title="Download">
                              <Download className="w-4 h-4" />
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
      </Tabs>
    </div>
  )
}
