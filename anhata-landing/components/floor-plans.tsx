"use client"

import type React from "react"

import { useState, useEffect, useRef } from "react"
import Image from "next/image"
import { X, ZoomIn, ZoomOut, RotateCcw } from "lucide-react"
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog"
import { useRouter } from "next/navigation"
import { getImagePath } from "@/lib/utils-path"

interface FloorPlansProps {
  filterType?: '2bhk' | '3bhk' | 'all'
}

const getFloorPlans = (filterType: '2bhk' | '3bhk' | 'all') => {
  if (filterType === '3bhk') {
    return [
      {
        title: "Master Plan",
        image: getImagePath("/images/floor-plans/master-plan.png"),
        description: "View the complete view of the five towers with amenities.",
      },
      {
        title: "Block A",
        image: getImagePath("/images/floor-plans/block-a.png"),
        description: "Detailed floor plans for 3 bhk apartments",
      },
      {
        title: "Block B",
        image: getImagePath("/images/floor-plans/block-b.png"),
        description: "Modern apartment layouts designed for comfort.",
      },
      {
        title: "Block C",
        image: getImagePath("/images/floor-plans/block-c.png"),
        description: "A well-planned apartment with open spaces",
      },
      {
        title: "Block D",
        image: getImagePath("/images/floor-plans/block-d.png"),
        description: "Explore home layouts for 3 bhk apartments.",
      },
      {
        title: "Block E",
        image: getImagePath("/images/floor-plans/block-e.png"),
        description: "Layout options for the best use of space",
      },
    ]
  }

  if (filterType === '2bhk') {
    return [
      {
        title: "Master Plan",
        image: getImagePath("/images/floor-plans/master-plan.png"),
        description: "View the complete view of the five towers with amenities.",
      },
      {
        title: "Block A",
        image: getImagePath("/images/floor-plans/block-a.png"),
        description: "Detailed floor plans for 2 bhk apartments",
      },
      {
        title: "Block B",
        image: getImagePath("/images/floor-plans/block-b.png"),
        description: "Modern apartment layouts designed for comfort.",
      },
      {
        title: "Block C",
        image: getImagePath("/images/floor-plans/block-c.png"),
        description: "A well-planned apartment with open spaces",
      },
      {
        title: "Block D",
        image: getImagePath("/images/floor-plans/block-d.png"),
        description: "Explore home layouts for 2 bhk apartments.",
      },
      {
        title: "Block E",
        image: getImagePath("/images/floor-plans/block-e.png"),
        description: "Layout options for the best use of space",
      },
    ]
  }

  return [
    {
      title: "Master Plan",
      image: getImagePath("/images/floor-plans/master-plan.png"),
      description: "The complete project layout includes all five towers, open spaces, and amenities",
    },
    {
      title: "Block A",
      image: getImagePath("/images/floor-plans/block-a.png"),
      description: "Floor plans featuring premium 2BHK and 3BHK apartment layouts.",
    },
    {
      title: "Block B",
      image: getImagePath("/images/floor-plans/block-b.png"),
      description: "Thoughtfully designed apartments with modern floor plans and function.",
    },
    {
      title: "Block C",
      image: getImagePath("/images/floor-plans/block-c.png"),
      description: "Generous living spaces and optimal floor plans for your dream home.",
    },
    {
      title: "Block D",
      image: getImagePath("/images/floor-plans/block-d.png"),
      description: "Elegant design made for modern living in Bangalore.",
    },
    {
      title: "Block E",
      image: getImagePath("/images/floor-plans/block-e.png"),
      description: "Private apartments that offer space and comfort in perfect balance.",
    },
  ]
}

export default function FloorPlans({ filterType = 'all' }: FloorPlansProps) {
  const floorPlans = getFloorPlans(filterType)
  const router = useRouter()
  const [hasAccess, setHasAccess] = useState(true) // Default to unlocked
  const [selectedPlan, setSelectedPlan] = useState<(typeof floorPlans)[0] | null>(null)
  const [isZoomModalOpen, setIsZoomModalOpen] = useState(false)
  const [zoomLevel, setZoomLevel] = useState(1)
  const [position, setPosition] = useState({ x: 0, y: 0 })
  const [isDragging, setIsDragging] = useState(false)
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 })
  const containerRef = useRef<HTMLDivElement>(null)
  const imageRef = useRef<HTMLImageElement>(null)
  const floorPlansScrollRef = useRef<HTMLDivElement>(null)

  // Function to check access status
  const checkAccess = () => {
    if (typeof window !== "undefined") {
      const userVerified = localStorage.getItem("userVerified")
      console.log("Floor Plans - Checking access:", userVerified) // Debug log
      if (userVerified === "true") {
        setHasAccess(true)
        return true
      }
    }
    // Default to true (unlocked) if no verification status found
    return true
  }

  // Check unified access on component mount and set up listeners
  useEffect(() => {
    checkAccess()

    // Listen for storage changes to update access in real-time
    const handleStorageChange = () => {
      console.log("Floor Plans - Storage changed") // Debug log
      checkAccess()
    }

    // Listen for custom events
    const handleAccessGranted = () => {
      console.log("Floor Plans - Access granted event") // Debug log
      setHasAccess(true)
    }

    window.addEventListener("storage", handleStorageChange)
    window.addEventListener("accessGranted", handleAccessGranted)

    // Also check periodically in case of timing issues
    const interval = setInterval(checkAccess, 1000)

    return () => {
      window.removeEventListener("storage", handleStorageChange)
      window.removeEventListener("accessGranted", handleAccessGranted)
      clearInterval(interval)
    }
  }, [])

  // Auto-scroll for mobile
  useEffect(() => {
    // Only auto-scroll on mobile
    if (window.innerWidth >= 768) return

    let intervalId: NodeJS.Timeout | null = null
    let isUserInteracting = false
    let resumeTimeout: NodeJS.Timeout | null = null

    const startAutoScroll = () => {
      if (!floorPlansScrollRef.current) return

      let scrollAmount = 0
      const scrollSpeed = 1
      const container = floorPlansScrollRef.current
      const maxScroll = container.scrollWidth - container.clientWidth

      intervalId = setInterval(() => {
        if (container && !isUserInteracting) {
          scrollAmount += scrollSpeed
          if (scrollAmount >= maxScroll) {
            scrollAmount = 0
          }
          container.scrollLeft = scrollAmount
        }
      }, 30)
    }

    const handleTouchStart = () => {
      isUserInteracting = true
      if (resumeTimeout) clearTimeout(resumeTimeout)
    }

    const handleTouchEnd = () => {
      if (resumeTimeout) clearTimeout(resumeTimeout)
      resumeTimeout = setTimeout(() => {
        isUserInteracting = false
      }, 3000)
    }

    const timeoutId = setTimeout(() => {
      startAutoScroll()
      if (floorPlansScrollRef.current) {
        floorPlansScrollRef.current.addEventListener('touchstart', handleTouchStart)
        floorPlansScrollRef.current.addEventListener('touchend', handleTouchEnd)
      }
    }, 100)

    return () => {
      clearTimeout(timeoutId)
      if (intervalId) clearInterval(intervalId)
      if (resumeTimeout) clearTimeout(resumeTimeout)
      if (floorPlansScrollRef.current) {
        floorPlansScrollRef.current.removeEventListener('touchstart', handleTouchStart)
        floorPlansScrollRef.current.removeEventListener('touchend', handleTouchEnd)
      }
    }
  }, [])

  const handlePlanClick = (plan: (typeof floorPlans)[0]) => {
    // Check if user has provided details before
    const userVerified = typeof window !== "undefined" ? localStorage.getItem("userVerified") : null

    if (userVerified !== "true") {
      // User hasn't provided details yet, navigate to form page
      router.push('/Booksitevisit?source=floor_plans')
    } else {
      // User has already provided details, open zoom modal directly
      setSelectedPlan(plan)
      setIsZoomModalOpen(true)
      setZoomLevel(1)
      setPosition({ x: 0, y: 0 })
    }
  }

  const handleZoomIn = () => {
    setZoomLevel((prev) => Math.min(prev + 0.25, 5))
  }

  const handleZoomOut = () => {
    setZoomLevel((prev) => Math.max(prev - 0.25, 0.25))
  }

  const handleReset = () => {
    setZoomLevel(1)
    setPosition({ x: 0, y: 0 })
    if (containerRef.current) {
      containerRef.current.scrollLeft = 0
      containerRef.current.scrollTop = 0
    }
  }

  // Mouse wheel zoom functionality
  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault()
    const delta = e.deltaY > 0 ? -0.1 : 0.1
    const newZoom = Math.min(Math.max(zoomLevel + delta, 0.25), 5)
    setZoomLevel(newZoom)
  }

  const handleMouseDown = (e: React.MouseEvent) => {
    if (zoomLevel > 1) {
      setIsDragging(true)
      setDragStart({
        x: e.clientX - position.x,
        y: e.clientY - position.y,
      })
    }
  }

  const handleMouseMove = (e: React.MouseEvent) => {
    if (isDragging && zoomLevel > 1) {
      setPosition({
        x: e.clientX - dragStart.x,
        y: e.clientY - dragStart.y,
      })
    }
  }

  const handleMouseUp = () => {
    setIsDragging(false)
  }

  const closeZoomModal = () => {
    setIsZoomModalOpen(false)
    setSelectedPlan(null)
    setZoomLevel(1)
    setPosition({ x: 0, y: 0 })
  }

  const getHeading = () => {
    if (filterType === '3bhk') {
      return "Spacious 3BHK Floor Plans for Today's Families"
    }
    if (filterType === '2bhk') {
      return "Spacious 2BHK Floor Plans for Today's Families"
    }
    return "2BHK & 3BHK Block & Floor Plans"
  }

  const getDescription = () => {
    if (filterType === '3bhk') {
      return "Step into 3 bhk apartments for sale in Bangalore with spacious, practical designs. Every block at Anahata is designed for comfort and modern living."
    }
    if (filterType === '2bhk') {
      return "Step into 2 bhk apartments for sale in Bangalore with spacious, practical designs. Every block at Anahata is designed for comfort and modern living."
    }
    return "Here are the floor plans and master plan of Anahata Towers in Whitefield. Each 2BHK apartments in Whitefield and 3BHK apartment for sale in Whitefield offers thoughtfully designed with optimal space utilization."
  }

  return (
    <>
      <section id="floor_plan" className="py-16 relative">
        {/* Background Image with Overlay */}
        <div className="absolute inset-0 z-0">
          <Image
            src={getImagePath("/images/floor-plans-bg.png")}
            alt="Floor Plans Background"
            fill
            className="object-cover"
            quality={90}
          />
          <div className="absolute inset-0 bg-black opacity-40"></div>
        </div>

        <div className="container mx-auto px-4 relative z-10">
          <div className="text-center mb-12">
            <h2 className="text-3xl md:text-4xl font-bold mb-6 text-[#E67E22]">{getHeading()}</h2>
            <p className="text-white max-w-2xl mx-auto">
              {getDescription()}
            </p>
          </div>

          {/* Mobile: Horizontal Scroll */}
          <div ref={floorPlansScrollRef} className="md:hidden overflow-x-auto pb-4 -mx-4 px-4 scrollbar-hide mb-12">
            <div className="flex gap-6 min-w-max">
              {floorPlans.map((plan, index) => (
                <div
                  key={index}
                  className="bg-white rounded-lg shadow-md overflow-hidden hover:shadow-[0_0_20px_rgba(230,126,34,0.5)] hover:ring-2 hover:ring-[#E67E22] transition-all cursor-pointer group flex-shrink-0 w-72"
                  onClick={() => handlePlanClick(plan)}
                >
                  <div className="relative h-64 overflow-hidden">
                    <img
                      src={plan.image || "/placeholder.svg"}
                      alt={`${plan.title} - ${plan.description}`}
                      loading="lazy"
                      className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                    />
                    <div className="absolute inset-0 bg-black bg-opacity-0 group-hover:bg-opacity-30 flex items-center justify-center transition-all duration-300">
                      <div className="text-center text-white opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                        <ZoomIn className="mx-auto mb-2" size={32} />
                        <p className="text-sm font-medium">Click to Zoom</p>
                      </div>
                    </div>
                  </div>
                  <div className="p-6">
                    <h3 className="text-xl font-semibold mb-2 text-center">{plan.title}</h3>
                    <p className="text-gray-600 text-sm text-center">{plan.description}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Desktop: Grid */}
          <div className="hidden md:grid md:grid-cols-2 lg:grid-cols-3 gap-8">
            {floorPlans.map((plan, index) => (
              <div
                key={index}
                className="bg-white rounded-lg shadow-md overflow-hidden hover:shadow-[0_0_20px_rgba(230,126,34,0.5)] hover:ring-2 hover:ring-[#E67E22] transition-all cursor-pointer group"
                onClick={() => handlePlanClick(plan)}
              >
                <div className="relative h-64 overflow-hidden">
                  <img
                    src={plan.image || "/placeholder.svg"}
                    alt={`${plan.title} - ${plan.description}`}
                    loading="lazy"
                    className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                  />
                  <div className="absolute inset-0 bg-black bg-opacity-0 group-hover:bg-opacity-30 flex items-center justify-center transition-all duration-300">
                    <div className="text-center text-white opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                      <ZoomIn className="mx-auto mb-2" size={32} />
                      <p className="text-sm font-medium">Click to Zoom</p>
                    </div>
                  </div>
                </div>
                <div className="p-6">
                  <h3 className="text-xl font-semibold mb-2 text-center">{plan.title}</h3>
                  <p className="text-gray-600 text-sm text-center">{plan.description}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Zoom Modal */}
      <Dialog open={isZoomModalOpen} onOpenChange={closeZoomModal}>
        <DialogContent className="max-w-7xl w-full h-[95vh] p-0 bg-black">
          <DialogTitle className="sr-only">{selectedPlan?.title || 'Floor Plan Viewer'}</DialogTitle>
          {selectedPlan && (
            <>
              {/* Header with controls */}
              <div className="absolute top-0 left-0 right-0 bg-black bg-opacity-90 text-white p-4 z-20 flex items-center justify-between border-b border-[#E67E22]">
                <h3 className="text-xl font-semibold">{selectedPlan.title}</h3>
                <div className="flex items-center space-x-4">
                  <button
                    onClick={handleZoomOut}
                    className="p-2 hover:bg-[#E67E22] rounded-lg transition-colors"
                    disabled={zoomLevel <= 0.25}
                  >
                    <ZoomOut size={20} />
                  </button>
                  <span className="text-sm font-medium min-w-[60px] text-center">{Math.round(zoomLevel * 100)}%</span>
                  <button
                    onClick={handleZoomIn}
                    className="p-2 hover:bg-[#E67E22] rounded-lg transition-colors"
                    disabled={zoomLevel >= 5}
                  >
                    <ZoomIn size={20} />
                  </button>
                  <button
                    onClick={handleReset}
                    className="p-2 hover:bg-[#E67E22] rounded-lg transition-colors"
                    title="Reset zoom and position"
                  >
                    <RotateCcw size={20} />
                  </button>
                  <button
                    onClick={closeZoomModal}
                    className="p-2 hover:bg-[#E67E22] rounded-lg transition-colors"
                  >
                    <X size={20} />
                  </button>
                </div>
              </div>

              {/* Scrollable image container */}
              <div
                ref={containerRef}
                className="w-full h-full pt-16 overflow-auto"
                style={{
                  scrollbarWidth: "thin",
                  scrollbarColor: "#4B5563 #1F2937",
                }}
                onWheel={handleWheel}
              >
                <div
                  className="flex items-center justify-center min-h-full p-4"
                  style={{
                    minWidth: `${Math.max(100, zoomLevel * 100)}%`,
                    minHeight: `${Math.max(100, zoomLevel * 100)}%`,
                  }}
                >
                  <img
                    ref={imageRef}
                    src={selectedPlan.image || "/placeholder.svg"}
                    alt={selectedPlan.title}
                    className="max-w-none transition-transform duration-200 select-none shadow-2xl"
                    style={{
                      transform: `scale(${zoomLevel})`,
                      cursor: zoomLevel > 1 ? (isDragging ? "grabbing" : "grab") : "default",
                    }}
                    onMouseDown={handleMouseDown}
                    onMouseMove={handleMouseMove}
                    onMouseUp={handleMouseUp}
                    onMouseLeave={handleMouseUp}
                    draggable={false}
                  />
                </div>
              </div>

              {/* Instructions */}
              <div className="absolute bottom-4 left-1/2 transform -translate-x-1/2 bg-black bg-opacity-90 text-white px-6 py-3 rounded-lg text-sm border border-gray-600">
                <div className="flex items-center space-x-4 text-center">
                  <span>🖱️ Mouse wheel: Zoom</span>
                  <span>•</span>
                  <span>📱 Drag: Pan</span>
                  <span>•</span>
                  <span>📜 Scroll: Navigate</span>
                </div>
              </div>

              {/* Custom scrollbar styles */}
              <style jsx>{`
                div::-webkit-scrollbar {
                  width: 12px;
                  height: 12px;
                }
                div::-webkit-scrollbar-track {
                  background: #1f2937;
                  border-radius: 6px;
                }
                div::-webkit-scrollbar-thumb {
                  background: #4b5563;
                  border-radius: 6px;
                  border: 2px solid #1f2937;
                }
                div::-webkit-scrollbar-thumb:hover {
                  background: #6b7280;
                }
                div::-webkit-scrollbar-corner {
                  background: #1f2937;
                }
              `}</style>
            </>
          )}
        </DialogContent>
      </Dialog>
    </>
  )
}
