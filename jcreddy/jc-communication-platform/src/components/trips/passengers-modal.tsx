'use client'

import { useState, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import {
  X,
  MapPin,
  Clock,
  Phone,
  User,
  Check,
  Send,
  Loader2,
  ChevronDown,
  ChevronRight,
  Users,
  CheckSquare,
  Square,
  Video,
  ArrowLeft,
  Eye,
  AlertCircle,
  Upload,
  FileVideo,
  Image,
  FileImage,
} from 'lucide-react'
import { cn } from '@/lib/utils'

interface Passenger {
  id: string
  full_name: string
  mobile: string
  email?: string
  seat_number: string
  pnr_number: string
  fare: number
  is_boarded: boolean
  booking_id: string
  origin?: string
  destination?: string
  booked_by?: string
}

interface BoardingPoint {
  name: string
  time: string | null
  address: string | null
  passengers: Passenger[]
}

interface PassengersModalProps {
  isOpen: boolean
  onClose: () => void
  tripId: string
  tripInfo?: {
    service_number: string
    origin: string
    destination: string
    travel_date: string
    departure_time: string
  }
}

interface Template {
  id: string
  template_name: string
  template_code: string
  whatsapp_template_name: string
  notification_type: string
  body: string
  variables?: string[]
}

type ModalStep = 'select' | 'configure' | 'preview' | 'done'

async function fetchTripPassengers(tripId: string) {
  const res = await fetch(`/api/trips/${tripId}/passengers`)
  if (!res.ok) throw new Error('Failed to fetch passengers')
  return res.json()
}

async function fetchTemplates() {
  const res = await fetch('/api/templates?channel=whatsapp&is_active=true')
  if (!res.ok) throw new Error('Failed to fetch templates')
  return res.json()
}

async function sendBulkWhatsApp(data: {
  passengerIds: string[]
  campaignName: string
  tripId?: string  // Added tripId for message tracking
  mediaUrl?: string
  customParams?: { [key: string]: string }
}) {
  const res = await fetch('/api/whatsapp/send-bulk', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!res.ok) throw new Error('Failed to send messages')
  return res.json()
}

export function PassengersModal({ isOpen, onClose, tripId, tripInfo }: PassengersModalProps) {
  const [selectedPassengers, setSelectedPassengers] = useState<Set<string>>(new Set())
  const [expandedBoardingPoints, setExpandedBoardingPoints] = useState<Set<string>>(new Set())
  const [selectedTemplateId, setSelectedTemplateId] = useState<string>('')
  const [step, setStep] = useState<ModalStep>('select')
  const [sendResult, setSendResult] = useState<any>(null)

  // Configuration for templates that need extra input
  const [videoUrl, setVideoUrl] = useState('')
  const [videoFile, setVideoFile] = useState<File | null>(null)
  const [imageUrl, setImageUrl] = useState('')
  const [imageFile, setImageFile] = useState<File | null>(null)
  const [uploadingVideo, setUploadingVideo] = useState(false)
  const [uploadingImage, setUploadingImage] = useState(false)
  const [uploadError, setUploadError] = useState('')
  const [customVar2, setCustomVar2] = useState('') // e.g., delay reason
  const [customVar3, setCustomVar3] = useState('') // e.g., area

  // Fetch passengers for this trip
  const { data, isLoading, error } = useQuery({
    queryKey: ['tripPassengers', tripId],
    queryFn: () => fetchTripPassengers(tripId),
    enabled: isOpen && !!tripId,
  })

  // Fetch available templates
  const { data: templatesData } = useQuery({
    queryKey: ['whatsappTemplates'],
    queryFn: fetchTemplates,
    enabled: isOpen,
  })

  // Send mutation
  const sendMutation = useMutation({
    mutationFn: sendBulkWhatsApp,
    onSuccess: (result) => {
      setStep('done')
      setSendResult(result)
    },
    onError: (error) => {
      alert('Failed to send messages: ' + (error as Error).message)
    },
  })

  // Expand all boarding points by default
  useEffect(() => {
    if (data?.boardingPoints) {
      setExpandedBoardingPoints(new Set(data.boardingPoints.map((bp: BoardingPoint) => bp.name)))
    }
  }, [data])

  // Reset state when modal closes
  useEffect(() => {
    if (!isOpen) {
      setSelectedPassengers(new Set())
      setSelectedTemplateId('')
      setStep('select')
      setSendResult(null)
      setVideoUrl('')
      setVideoFile(null)
      setUploadingVideo(false)
      setImageUrl('')
      setImageFile(null)
      setUploadingImage(false)
      setUploadError('')
      setCustomVar2('')
      setCustomVar3('')
    }
  }, [isOpen])

  // Handle video file upload
  const handleVideoUpload = async (file: File) => {
    setVideoFile(file)
    setUploadError('')
    setUploadingVideo(true)

    try {
      const formData = new FormData()
      formData.append('file', file)

      const response = await fetch('/api/upload', {
        method: 'POST',
        body: formData,
      })

      const result = await response.json()

      if (!response.ok) {
        throw new Error(result.error || 'Upload failed')
      }

      setVideoUrl(result.url)
      setUploadingVideo(false)
    } catch (error: any) {
      setUploadError(error.message || 'Failed to upload video')
      setUploadingVideo(false)
      setVideoFile(null)
    }
  }

  // Handle image file upload
  const handleImageUpload = async (file: File) => {
    setImageFile(file)
    setUploadError('')
    setUploadingImage(true)

    try {
      const formData = new FormData()
      formData.append('file', file)

      const response = await fetch('/api/upload', {
        method: 'POST',
        body: formData,
      })

      const result = await response.json()

      if (!response.ok) {
        throw new Error(result.error || 'Upload failed')
      }

      setImageUrl(result.url)
      setUploadingImage(false)
    } catch (error: any) {
      setUploadError(error.message || 'Failed to upload image')
      setUploadingImage(false)
      setImageFile(null)
    }
  }

  if (!isOpen) return null

  const boardingPoints: BoardingPoint[] = data?.boardingPoints || []
  const totalPassengers = data?.totalPassengers || 0
  const templates: Template[] = templatesData?.templates || []
  const selectedTemplate = templates.find((t) => t.id === selectedTemplateId)

  // Check if template needs additional configuration (video, image, custom vars)
  const templateNeedsConfig = (template: Template | undefined): boolean => {
    if (!template) return false
    const campaignName = template.whatsapp_template_name?.toLowerCase() || ''
    const templateName = template.template_name?.toLowerCase() || ''
    // Templates with video (delay/jam) or image (rating) need configuration
    const hasDelayOrJam = campaignName.includes('delay') || campaignName.includes('jam')
    const hasRating = campaignName.includes('rating') || templateName.includes('rating')
    const hasMultipleVars = template.variables ? template.variables.length > 1 : false
    return hasDelayOrJam || hasRating || hasMultipleVars
  }

  // Check if template needs image (rating templates)
  const templateNeedsImage = (template: Template | undefined): boolean => {
    if (!template) return false
    const campaignName = template.whatsapp_template_name?.toLowerCase() || ''
    const templateName = template.template_name?.toLowerCase() || ''
    return campaignName.includes('rating') || templateName.includes('rating')
  }

  // Check if template needs video (delay templates)
  const templateNeedsVideo = (template: Template | undefined): boolean => {
    if (!template) return false
    const campaignName = template.whatsapp_template_name?.toLowerCase() || ''
    return campaignName.includes('delay') || campaignName.includes('jam')
  }

  const toggleBoardingPoint = (name: string) => {
    const newExpanded = new Set(expandedBoardingPoints)
    if (newExpanded.has(name)) {
      newExpanded.delete(name)
    } else {
      newExpanded.add(name)
    }
    setExpandedBoardingPoints(newExpanded)
  }

  const togglePassenger = (passengerId: string) => {
    const newSelected = new Set(selectedPassengers)
    if (newSelected.has(passengerId)) {
      newSelected.delete(passengerId)
    } else {
      newSelected.add(passengerId)
    }
    setSelectedPassengers(newSelected)
  }

  const toggleAllInBoardingPoint = (bp: BoardingPoint) => {
    const passengerIds = bp.passengers.map((p) => p.id)
    const allSelected = passengerIds.every((id) => selectedPassengers.has(id))

    const newSelected = new Set(selectedPassengers)
    if (allSelected) {
      passengerIds.forEach((id) => newSelected.delete(id))
    } else {
      passengerIds.forEach((id) => newSelected.add(id))
    }
    setSelectedPassengers(newSelected)
  }

  const selectAll = () => {
    const allIds = boardingPoints.flatMap((bp) => bp.passengers.map((p) => p.id))
    setSelectedPassengers(new Set(allIds))
  }

  const deselectAll = () => {
    setSelectedPassengers(new Set())
  }

  const handleNextStep = () => {
    if (selectedPassengers.size === 0) {
      alert('Please select at least one passenger')
      return
    }
    if (!selectedTemplateId) {
      alert('Please select a template')
      return
    }

    if (templateNeedsConfig(selectedTemplate)) {
      setStep('configure')
    } else {
      setStep('preview')
    }
  }

  const handleSendMessages = () => {
    if (!selectedTemplate?.whatsapp_template_name) {
      alert('Selected template does not have an AISensy campaign name')
      return
    }

    // Use imageUrl for rating templates, videoUrl for delay templates
    const mediaUrl = templateNeedsImage(selectedTemplate) ? imageUrl : videoUrl

    // Debug: Log the tripId being sent
    console.log('[PassengersModal] Sending messages with tripId:', tripId)

    sendMutation.mutate({
      passengerIds: Array.from(selectedPassengers),
      campaignName: selectedTemplate.whatsapp_template_name,
      tripId: tripId,  // Pass tripId for message tracking
      mediaUrl: mediaUrl || undefined,
      customParams: {
        var2: customVar2,
        var3: customVar3,
      },
    })
  }

  const formatTime = (timeStr: string | null) => {
    if (!timeStr) return '--:--'
    try {
      const [hours, minutes] = timeStr.split(':').map(Number)
      const period = hours >= 12 ? 'PM' : 'AM'
      const displayHours = hours % 12 || 12
      return `${displayHours}:${minutes.toString().padStart(2, '0')} ${period}`
    } catch {
      return timeStr
    }
  }

  // Get selected passenger names for preview
  const getSelectedPassengerNames = () => {
    const names: string[] = []
    boardingPoints.forEach((bp) => {
      bp.passengers.forEach((p) => {
        if (selectedPassengers.has(p.id)) {
          names.push(p.full_name)
        }
      })
    })
    return names
  }

  // Generate preview message
  const getPreviewMessage = () => {
    if (!selectedTemplate) return ''
    let preview = selectedTemplate.body
    preview = preview.replace('{{1}}', '[Passenger Name]')
    preview = preview.replace('{{2}}', customVar2 || '[Variable 2]')
    preview = preview.replace('{{3}}', customVar3 || '[Variable 3]')
    return preview
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />

      {/* Modal */}
      <div className="relative w-full max-w-3xl max-h-[90vh] bg-white rounded-xl shadow-2xl flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b bg-gradient-to-r from-blue-50 to-indigo-50 rounded-t-xl">
          <div className="flex items-center gap-3">
            {step !== 'select' && step !== 'done' && (
              <button
                onClick={() => setStep(step === 'preview' && templateNeedsConfig(selectedTemplate) ? 'configure' : 'select')}
                className="p-1 hover:bg-white/50 rounded-lg"
              >
                <ArrowLeft className="h-5 w-5 text-gray-500" />
              </button>
            )}
            <div>
              <h2 className="text-lg font-semibold text-gray-900">
                {step === 'select' && 'Trip Passengers'}
                {step === 'configure' && 'Configure Message'}
                {step === 'preview' && 'Preview & Send'}
                {step === 'done' && 'Messages Sent'}
              </h2>
              {tripInfo && step === 'select' && (
                <p className="text-sm text-gray-600">
                  {tripInfo.origin} → {tripInfo.destination} | {tripInfo.travel_date}
                  <span className="ml-2 text-xs text-gray-400">(ID: {tripId?.substring(0, 8)}...)</span>
                </p>
              )}
            </div>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-white/50 rounded-lg transition-colors">
            <X className="h-5 w-5 text-gray-500" />
          </button>
        </div>

        {/* Step 1: Select Passengers */}
        {step === 'select' && (
          <>
            <div className="flex-1 overflow-y-auto p-4">
              {isLoading ? (
                <div className="flex items-center justify-center py-12">
                  <Loader2 className="h-8 w-8 animate-spin text-blue-500" />
                  <span className="ml-2 text-gray-600">Loading passengers...</span>
                </div>
              ) : error ? (
                <div className="text-center py-12 text-red-500">Failed to load passengers</div>
              ) : boardingPoints.length === 0 ? (
                <div className="text-center py-12 text-gray-500">No passengers found for this trip</div>
              ) : (
                <>
                  {/* Selection Actions */}
                  <div className="flex items-center justify-between mb-4 pb-4 border-b">
                    <div className="flex items-center gap-2">
                      <Users className="h-5 w-5 text-gray-400" />
                      <span className="text-sm text-gray-600">
                        {selectedPassengers.size} of {totalPassengers} selected
                      </span>
                    </div>
                    <div className="flex gap-2">
                      <button onClick={selectAll} className="px-3 py-1.5 text-sm text-blue-600 hover:bg-blue-50 rounded-lg">
                        Select All
                      </button>
                      <button onClick={deselectAll} className="px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 rounded-lg">
                        Deselect All
                      </button>
                    </div>
                  </div>

                  {/* Boarding Points Accordion */}
                  <div className="space-y-3">
                    {boardingPoints.map((bp) => {
                      const isExpanded = expandedBoardingPoints.has(bp.name)
                      const passengerIds = bp.passengers.map((p) => p.id)
                      const selectedCount = passengerIds.filter((id) => selectedPassengers.has(id)).length
                      const allSelected = selectedCount === bp.passengers.length && bp.passengers.length > 0

                      return (
                        <div key={bp.name} className="border rounded-lg overflow-hidden">
                          <div
                            className={cn(
                              'flex items-center justify-between p-3 cursor-pointer transition-colors',
                              isExpanded ? 'bg-blue-50' : 'bg-gray-50 hover:bg-gray-100'
                            )}
                            onClick={() => toggleBoardingPoint(bp.name)}
                          >
                            <div className="flex items-center gap-3">
                              <button
                                onClick={(e) => { e.stopPropagation(); toggleAllInBoardingPoint(bp) }}
                                className="text-blue-600 hover:text-blue-700"
                              >
                                {allSelected ? <CheckSquare className="h-5 w-5" /> : <Square className="h-5 w-5" />}
                              </button>
                              <div>
                                <div className="flex items-center gap-2">
                                  <MapPin className="h-4 w-4 text-orange-500" />
                                  <span className="font-medium text-gray-900">{bp.name}</span>
                                  <span className="text-xs bg-gray-200 text-gray-600 px-2 py-0.5 rounded-full">
                                    {bp.passengers.length} passengers
                                  </span>
                                </div>
                                {bp.time && (
                                  <div className="flex items-center gap-1 mt-1 text-xs text-gray-500">
                                    <Clock className="h-3 w-3" />
                                    <span>{formatTime(bp.time)}</span>
                                  </div>
                                )}
                              </div>
                            </div>
                            <div className="flex items-center gap-2">
                              {selectedCount > 0 && (
                                <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full">
                                  {selectedCount} selected
                                </span>
                              )}
                              {isExpanded ? <ChevronDown className="h-5 w-5 text-gray-400" /> : <ChevronRight className="h-5 w-5 text-gray-400" />}
                            </div>
                          </div>

                          {isExpanded && (
                            <div className="divide-y">
                              {bp.passengers.map((passenger) => {
                                const isSelected = selectedPassengers.has(passenger.id)
                                return (
                                  <div
                                    key={passenger.id}
                                    className={cn('flex items-center gap-3 p-3 hover:bg-gray-50 cursor-pointer', isSelected && 'bg-blue-50/50')}
                                    onClick={() => togglePassenger(passenger.id)}
                                  >
                                    <button onClick={(e) => { e.stopPropagation(); togglePassenger(passenger.id) }} className="text-blue-600">
                                      {isSelected ? <CheckSquare className="h-5 w-5" /> : <Square className="h-5 w-5 text-gray-400" />}
                                    </button>
                                    <div className="flex-1 min-w-0">
                                      <div className="flex items-center gap-2 flex-wrap">
                                        <User className="h-4 w-4 text-gray-400" />
                                        <span className="font-medium text-gray-900 truncate">{passenger.full_name}</span>
                                        <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded">Seat {passenger.seat_number}</span>
                                        {passenger.booked_by && (
                                          <span className={cn(
                                            "text-xs px-2 py-0.5 rounded font-medium",
                                            passenger.booked_by.toLowerCase().includes('redbus') ? 'bg-red-100 text-red-700' :
                                            passenger.booked_by.toLowerCase().includes('paytm') ? 'bg-blue-100 text-blue-700' :
                                            passenger.booked_by.toLowerCase().includes('abhibus') ? 'bg-orange-100 text-orange-700' :
                                            passenger.booked_by.toLowerCase().includes('makemytrip') ? 'bg-purple-100 text-purple-700' :
                                            passenger.booked_by.toLowerCase().includes('goibibo') ? 'bg-green-100 text-green-700' :
                                            'bg-gray-100 text-gray-600'
                                          )}>
                                            {passenger.booked_by}
                                          </span>
                                        )}
                                      </div>
                                      <div className="flex items-center gap-4 mt-1 text-xs text-gray-500">
                                        <span className="flex items-center gap-1"><Phone className="h-3 w-3" />{passenger.mobile}</span>
                                        <span>PNR: {passenger.pnr_number}</span>
                                      </div>
                                    </div>
                                  </div>
                                )
                              })}
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                </>
              )}
            </div>

            {/* Footer */}
            {!isLoading && boardingPoints.length > 0 && (
              <div className="border-t p-4 bg-gray-50 rounded-b-xl">
                <div className="flex items-center gap-4">
                  <div className="flex-1">
                    <label className="block text-xs text-gray-500 mb-1">Select Template</label>
                    <select
                      value={selectedTemplateId}
                      onChange={(e) => setSelectedTemplateId(e.target.value)}
                      className="w-full px-3 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      <option value="">Choose a template...</option>
                      {templates.map((template) => (
                        <option key={template.id} value={template.id}>
                          {template.template_name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <button
                    onClick={handleNextStep}
                    disabled={selectedPassengers.size === 0 || !selectedTemplateId}
                    className={cn(
                      'flex items-center gap-2 px-6 py-2 rounded-lg font-medium transition-colors',
                      selectedPassengers.size > 0 && selectedTemplateId
                        ? 'bg-blue-600 text-white hover:bg-blue-700'
                        : 'bg-gray-200 text-gray-400 cursor-not-allowed'
                    )}
                  >
                    Next
                    <ChevronRight className="h-4 w-4" />
                  </button>
                </div>
              </div>
            )}
          </>
        )}

        {/* Step 2: Configure (Image for rating, Video + Custom Variables for delay) */}
        {step === 'configure' && selectedTemplate && (
          <>
            <div className="flex-1 overflow-y-auto p-4 space-y-6">
              {/* Template Info */}
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <h3 className="font-medium text-blue-900 mb-1">{selectedTemplate.template_name}</h3>
                <p className="text-sm text-blue-700">Sending to {selectedPassengers.size} passengers</p>
              </div>

              {/* Image Upload (for rating templates) */}
              {templateNeedsImage(selectedTemplate) && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    <Image className="inline h-4 w-4 mr-1" />
                    Upload Image *
                  </label>

                  {/* File Upload Area */}
                  {!imageFile && !imageUrl ? (
                    <label className="flex flex-col items-center justify-center w-full h-32 border-2 border-dashed border-gray-300 rounded-lg cursor-pointer hover:border-blue-500 hover:bg-blue-50 transition-colors">
                      <div className="flex flex-col items-center justify-center pt-5 pb-6">
                        <Upload className="w-8 h-8 mb-2 text-gray-400" />
                        <p className="mb-1 text-sm text-gray-500">
                          <span className="font-semibold">Click to upload</span> or drag and drop
                        </p>
                        <p className="text-xs text-gray-400">JPG, PNG, WebP (Max 5MB)</p>
                      </div>
                      <input
                        type="file"
                        className="hidden"
                        accept="image/jpeg,image/png,image/webp"
                        onChange={(e) => {
                          const file = e.target.files?.[0]
                          if (file) handleImageUpload(file)
                        }}
                      />
                    </label>
                  ) : uploadingImage ? (
                    <div className="flex items-center justify-center w-full h-32 border-2 border-blue-300 rounded-lg bg-blue-50">
                      <div className="flex flex-col items-center">
                        <Loader2 className="w-8 h-8 animate-spin text-blue-500 mb-2" />
                        <p className="text-sm text-blue-600">Uploading image...</p>
                      </div>
                    </div>
                  ) : imageUrl ? (
                    <div className="flex items-center justify-between w-full p-4 border-2 border-green-300 rounded-lg bg-green-50">
                      <div className="flex items-center gap-3">
                        <div className="w-16 h-16 rounded overflow-hidden bg-gray-100">
                          <img src={imageUrl} alt="Preview" className="w-full h-full object-cover" />
                        </div>
                        <div>
                          <p className="text-sm font-medium text-green-800">
                            {imageFile?.name || 'Image uploaded'}
                          </p>
                          <p className="text-xs text-green-600">
                            {imageFile ? `${(imageFile.size / 1024 / 1024).toFixed(2)} MB` : 'Ready to send'}
                          </p>
                        </div>
                      </div>
                      <button
                        onClick={() => {
                          setImageUrl('')
                          setImageFile(null)
                        }}
                        className="p-1 hover:bg-green-100 rounded"
                      >
                        <X className="w-5 h-5 text-green-600" />
                      </button>
                    </div>
                  ) : null}

                  {/* Error message */}
                  {uploadError && (
                    <p className="text-sm text-red-600 mt-2 flex items-center gap-1">
                      <AlertCircle className="w-4 h-4" />
                      {uploadError}
                    </p>
                  )}
                </div>
              )}

              {/* Video Upload (for delay templates) */}
              {templateNeedsVideo(selectedTemplate) && (
                <>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      <Video className="inline h-4 w-4 mr-1" />
                      Upload Video *
                    </label>

                    {/* File Upload Area */}
                    {!videoFile && !videoUrl ? (
                      <label className="flex flex-col items-center justify-center w-full h-32 border-2 border-dashed border-gray-300 rounded-lg cursor-pointer hover:border-blue-500 hover:bg-blue-50 transition-colors">
                        <div className="flex flex-col items-center justify-center pt-5 pb-6">
                          <Upload className="w-8 h-8 mb-2 text-gray-400" />
                          <p className="mb-1 text-sm text-gray-500">
                            <span className="font-semibold">Click to upload</span> or drag and drop
                          </p>
                          <p className="text-xs text-gray-400">MP4, WebM, MOV (Max 50MB)</p>
                        </div>
                        <input
                          type="file"
                          className="hidden"
                          accept="video/mp4,video/webm,video/quicktime"
                          onChange={(e) => {
                            const file = e.target.files?.[0]
                            if (file) handleVideoUpload(file)
                          }}
                        />
                      </label>
                    ) : uploadingVideo ? (
                      <div className="flex items-center justify-center w-full h-32 border-2 border-blue-300 rounded-lg bg-blue-50">
                        <div className="flex flex-col items-center">
                          <Loader2 className="w-8 h-8 animate-spin text-blue-500 mb-2" />
                          <p className="text-sm text-blue-600">Uploading video...</p>
                        </div>
                      </div>
                    ) : videoUrl ? (
                      <div className="flex items-center justify-between w-full p-4 border-2 border-green-300 rounded-lg bg-green-50">
                        <div className="flex items-center gap-3">
                          <FileVideo className="w-8 h-8 text-green-600" />
                          <div>
                            <p className="text-sm font-medium text-green-800">
                              {videoFile?.name || 'Video uploaded'}
                            </p>
                            <p className="text-xs text-green-600">
                              {videoFile ? `${(videoFile.size / 1024 / 1024).toFixed(2)} MB` : 'Ready to send'}
                            </p>
                          </div>
                        </div>
                        <button
                          onClick={() => {
                            setVideoUrl('')
                            setVideoFile(null)
                          }}
                          className="p-1 hover:bg-green-100 rounded"
                        >
                          <X className="w-5 h-5 text-green-600" />
                        </button>
                      </div>
                    ) : null}

                    {/* Error message */}
                    {uploadError && (
                      <p className="text-sm text-red-600 mt-2 flex items-center gap-1">
                        <AlertCircle className="w-4 h-4" />
                        {uploadError}
                      </p>
                    )}
                  </div>

                  {/* Custom Variable 2 */}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Delay Reason (Variable 2) *
                    </label>
                    <input
                      type="text"
                      value={customVar2}
                      onChange={(e) => setCustomVar2(e.target.value)}
                      placeholder="e.g., heavy traffic jam"
                      className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      This will replace {"{{2}}"} in the message
                    </p>
                  </div>

                  {/* Custom Variable 3 */}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Area Name (Variable 3) *
                    </label>
                    <input
                      type="text"
                      value={customVar3}
                      onChange={(e) => setCustomVar3(e.target.value)}
                      placeholder="e.g., Hyderabad outer ring road"
                      className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      This will replace {"{{3}}"} in the message
                    </p>
                  </div>
                </>
              )}
            </div>

            {/* Footer - different validation for image vs video templates */}
            <div className="border-t p-4 bg-gray-50 rounded-b-xl">
              <button
                onClick={() => setStep('preview')}
                disabled={
                  templateNeedsImage(selectedTemplate)
                    ? !imageUrl || uploadingImage
                    : !videoUrl || !customVar2 || !customVar3 || uploadingVideo
                }
                className={cn(
                  'w-full flex items-center justify-center gap-2 px-6 py-3 rounded-lg font-medium transition-colors',
                  (templateNeedsImage(selectedTemplate)
                    ? imageUrl && !uploadingImage
                    : videoUrl && customVar2 && customVar3 && !uploadingVideo)
                    ? 'bg-blue-600 text-white hover:bg-blue-700'
                    : 'bg-gray-200 text-gray-400 cursor-not-allowed'
                )}
              >
                <Eye className="h-4 w-4" />
                Preview Message
              </button>
            </div>
          </>
        )}

        {/* Step 3: Preview */}
        {step === 'preview' && selectedTemplate && (
          <>
            <div className="flex-1 overflow-y-auto p-4 space-y-6">
              {/* Summary */}
              <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                <h3 className="font-medium text-green-900 mb-2">Ready to Send</h3>
                <div className="text-sm text-green-700 space-y-1">
                  <p>📱 <strong>{selectedPassengers.size}</strong> recipients</p>
                  <p>📝 Template: <strong>{selectedTemplate.template_name}</strong></p>
                  {imageUrl && <p>🖼️ Image attached</p>}
                  {videoUrl && <p>🎥 Video attached</p>}
                </div>
              </div>

              {/* Image Preview (for rating templates) */}
              {imageUrl && (
                <div>
                  <h4 className="text-sm font-medium text-gray-700 mb-2">Image Preview</h4>
                  <div className="bg-gray-100 rounded-lg p-3">
                    <img src={imageUrl} alt="Attached" className="max-h-40 rounded mx-auto" />
                  </div>
                </div>
              )}

              {/* Message Preview */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 mb-2">Message Preview</h4>
                <div className="bg-gray-900 rounded-lg p-4 text-green-400 font-mono text-sm whitespace-pre-wrap">
                  {getPreviewMessage()}
                </div>
              </div>

              {/* Recipients List */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 mb-2">Recipients ({selectedPassengers.size})</h4>
                <div className="bg-gray-50 rounded-lg p-3 max-h-40 overflow-y-auto">
                  <div className="flex flex-wrap gap-2">
                    {getSelectedPassengerNames().map((name, idx) => (
                      <span key={idx} className="px-2 py-1 bg-white border rounded text-xs text-gray-700">
                        {name}
                      </span>
                    ))}
                  </div>
                </div>
              </div>

              {/* Warning */}
              <div className="flex items-start gap-2 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
                <AlertCircle className="h-5 w-5 text-yellow-600 flex-shrink-0 mt-0.5" />
                <div className="text-sm text-yellow-800">
                  <strong>Please verify:</strong> Once sent, messages cannot be recalled. Each passenger will receive this message on their WhatsApp.
                </div>
              </div>
            </div>

            {/* Footer */}
            <div className="border-t p-4 bg-gray-50 rounded-b-xl">
              <button
                onClick={handleSendMessages}
                disabled={sendMutation.isPending}
                className="w-full flex items-center justify-center gap-2 px-6 py-3 rounded-lg font-medium bg-green-600 text-white hover:bg-green-700 transition-colors disabled:opacity-50"
              >
                {sendMutation.isPending ? (
                  <>
                    <Loader2 className="h-5 w-5 animate-spin" />
                    Sending to {selectedPassengers.size} passengers...
                  </>
                ) : (
                  <>
                    <Send className="h-5 w-5" />
                    Send to {selectedPassengers.size} Passengers
                  </>
                )}
              </button>
            </div>
          </>
        )}

        {/* Step 4: Done */}
        {step === 'done' && sendResult && (
          <div className="flex-1 p-8 flex flex-col items-center justify-center">
            <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mb-4">
              <Check className="h-8 w-8 text-green-600" />
            </div>
            <h3 className="text-xl font-semibold text-gray-900 mb-2">Messages Sent!</h3>
            <p className="text-gray-600 mb-6">
              Successfully sent {sendResult.results?.success || 0} of {sendResult.results?.total || 0} messages
            </p>
            {sendResult.results?.failed > 0 && (
              <p className="text-red-600 text-sm mb-4">
                {sendResult.results.failed} messages failed to send
              </p>
            )}
            {/* Show trip ID info */}
            <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg text-left w-full max-w-md">
              <p className="text-xs text-blue-800 font-medium">
                Trip ID: {sendResult.tripId || tripId || 'Unknown'}
              </p>
              <p className="text-xs text-blue-600">
                Batch ID: {sendResult.batchId || 'Not created'}
              </p>
              {sendResult.tripIdDebug?.mismatch && (
                <p className="text-xs text-red-600 font-bold mt-1">
                  WARNING: Stored trip ID mismatch!
                </p>
              )}
            </div>
            {/* Show database errors if any */}
            {(sendResult.dbErrors?.batchError || sendResult.dbErrors?.logsError) && (
              <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg text-left w-full max-w-md">
                <p className="text-sm font-medium text-red-800 mb-1">Database Errors:</p>
                {sendResult.dbErrors?.batchError && (
                  <p className="text-xs text-red-600">Batch: {sendResult.dbErrors.batchError}</p>
                )}
                {sendResult.dbErrors?.logsError && (
                  <p className="text-xs text-red-600">Logs: {sendResult.dbErrors.logsError}</p>
                )}
              </div>
            )}
            <button
              onClick={onClose}
              className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              Close
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
