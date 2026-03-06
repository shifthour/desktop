"use client"

import { useState, useEffect, useRef } from "react"
import { ExternalLink, Navigation, MapPin } from "lucide-react"
import { useRouter } from "next/navigation"

interface LocationProps {
  filterType?: '2bhk' | '3bhk' | 'all'
}

export default function Location({ filterType = 'all' }: LocationProps) {
  const router = useRouter()
  const [hasAccess, setHasAccess] = useState(false) // Start with false to check properly
  const highlightsScrollRef = useRef<HTMLDivElement>(null)

  const getDescription = () => {
    if (filterType === '3bhk') {
      return "Located on Soukya Road, Whitefield, one of Bangalore's top residential areas with great connectivity and modern infrastructure."
    }
    if (filterType === '2bhk') {
      return "Located on Soukya Road, Whitefield, one of Bangalore's top residential areas with great connectivity and modern infrastructure."
    }
    return "Strategically located at Soukya Road, Whitefield - one of Bengaluru's most sought-after residential destinations with excellent connectivity and infrastructure."
  }

  const getLocationHighlights = () => {
    if (filterType === '3bhk' || filterType === '2bhk') {
      return [
        {
          icon: "🏢",
          title: "Close to IT Hubs",
          description: "Nearby major IT offices and tech parks in Whitefield",
          color: "blue"
        },
        {
          icon: "🚇",
          title: "Metro Nearby",
          description: "Quick access to Namma Metro Purple Line",
          color: "green"
        },
        {
          icon: "🏫",
          title: "Schools & Colleges",
          description: "Top schools and colleges within reach",
          color: "purple"
        },
        {
          icon: "🛒",
          title: "Shopping & Food",
          description: "VR Mall, Hopefarm Channasandra Metro, and local restaurants",
          color: "orange"
        }
      ]
    }

    return [
      {
        icon: "🏢",
        title: "IT Hub Proximity",
        description: "Close to major IT companies and tech parks in Whitefield",
        color: "blue"
      },
      {
        icon: "🚇",
        title: "Metro Connectivity",
        description: "Easy access to Namma Metro Purple Line stations",
        color: "green"
      },
      {
        icon: "🏫",
        title: "Educational Hubs",
        description: "Renowned schools and colleges in the vicinity",
        color: "purple"
      },
      {
        icon: "🛒",
        title: "Shopping & Dining",
        description: "Hopefarm Channasandra metro, VR Mall, and fine dining options",
        color: "orange"
      }
    ]
  }

  const locationHighlights = getLocationHighlights()

  // Function to check access status
  const checkAccess = () => {
    if (typeof window !== "undefined") {
      const userVerified = localStorage.getItem("userVerified")
      console.log("Location - Checking access:", userVerified) // Debug log
      if (userVerified === "true") {
        setHasAccess(true)
        return true
      } else {
        setHasAccess(false)
        return false
      }
    }
    return false
  }

  // Check unified access on component mount and set up listeners
  useEffect(() => {
    checkAccess()

    // Listen for storage changes to update access in real-time
    const handleStorageChange = () => {
      console.log("Location - Storage changed") // Debug log
      checkAccess()
    }

    // Listen for custom events
    const handleAccessGranted = () => {
      console.log("Location - Access granted event") // Debug log
      checkAccess() // Re-check access status
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

  // Auto-scroll for location highlights on mobile
  useEffect(() => {
    // Only auto-scroll on mobile
    if (typeof window !== 'undefined' && window.innerWidth >= 768) return

    let intervalId: NodeJS.Timeout | null = null
    let isUserInteracting = false
    let resumeTimeout: NodeJS.Timeout | null = null

    const startAutoScroll = () => {
      if (!highlightsScrollRef.current) return

      let scrollAmount = 0
      const scrollSpeed = 1
      const container = highlightsScrollRef.current
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
      if (highlightsScrollRef.current) {
        highlightsScrollRef.current.addEventListener('touchstart', handleTouchStart)
        highlightsScrollRef.current.addEventListener('touchend', handleTouchEnd)
      }
    }, 1000)

    return () => {
      clearTimeout(timeoutId)
      if (intervalId) clearInterval(intervalId)
      if (resumeTimeout) clearTimeout(resumeTimeout)
      if (highlightsScrollRef.current) {
        highlightsScrollRef.current.removeEventListener('touchstart', handleTouchStart)
        highlightsScrollRef.current.removeEventListener('touchend', handleTouchEnd)
      }
    }
  }, [])

  const handleMapClick = () => {
    // Check if user has provided details before
    if (!hasAccess) {
      // User hasn't provided details yet, navigate to form page
      router.push('/Booksitevisit?source=location')
    }
    // If user is verified, do nothing (map is already fully accessible)
  }

  // Anahata location details
  const anahataLocation = {
    address: "Soukya Road, Whitefield, Bengaluru, Karnataka 560066",
    coordinates: "12.9813819, 77.7939833",
    googleMapsUrl:
      "https://www.google.com/maps/place/ISHTIKA+ANAHATA/@12.9813819,77.7939833,17z/data=!3m1!4b1!4m6!3m5!1s0x3bae0f003caafefd:0x66354407102896fe!8m2!3d12.9813819!4d77.7939833!16s%2Fg%2F11xfhll4fn?entry=ttu&g_ep=EgoyMDI1MDcwOC4wIKXMDSoASAFQAw%3D%3D",
    directionsUrl: "https://www.google.com/maps/dir/?api=1&destination=12.9813819,77.7939833",
  }

  return (
    <>
      <section id="location" className="py-16 bg-white">
        <div className="container mx-auto px-4">
          <div className="text-center mb-12">
            <h2 className="text-3xl md:text-4xl font-bold mb-6 text-[#E67E22]">Location</h2>
            <p className="text-[#E67E22] max-w-2xl mx-auto mb-4">
              {getDescription()}
            </p>
            {!hasAccess && (
              <p className="text-[#E67E22] font-medium mt-4">Click on the map to access detailed location information</p>
            )}
          </div>

          <div className="max-w-6xl mx-auto">
            <div className="space-y-8">
              {/* Address Info */}
              <div className="bg-white rounded-lg shadow-md p-6 text-center">
                <h3 className="text-xl font-semibold mb-2 text-[#E67E22]">Anahata - Realm of Living</h3>
                <p className="text-gray-600 mb-2">{anahataLocation.address}</p>
                <p className="text-sm text-gray-500">Coordinates: {anahataLocation.coordinates}</p>
              </div>

              {/* Google Maps with Custom Marker */}
              <div className="bg-white rounded-lg shadow-lg overflow-hidden">
                <div className={`relative w-full h-96 ${!hasAccess ? "cursor-pointer" : ""}`} onClick={handleMapClick}>
                  {/* Google Maps iframe */}
                  <iframe
                    src={`https://www.google.com/maps/embed?pb=!1m14!1m12!1m3!1d3887.8!2d77.7939833!3d12.9813819!2m3!1f0!2f0!3f0!3m2!1i1024!2i768!4f13.1!5e0!3m2!1sen!2sin!4v1234567890123!5m2!1sen!2sin&q=12.9813819,77.7939833`}
                    width="100%"
                    height="384"
                    style={{ border: 0 }}
                    allowFullScreen
                    loading="lazy"
                    referrerPolicy="no-referrer-when-downgrade"
                    title="Anahata Location - Soukya Road, Whitefield, Bengaluru"
                    className="w-full h-full"
                  />

                  {/* Custom Anahata Marker Overlay */}
                  <div className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-full pointer-events-none z-10">
                    <div className="relative">
                      {/* Marker Pin */}
                      <div className="bg-[#E67E22] rounded-full p-3 shadow-lg animate-bounce">
                        <MapPin className="text-white" size={24} />
                      </div>
                      {/* Marker Label */}
                      <div className="absolute -top-12 left-1/2 transform -translate-x-1/2 bg-white px-3 py-1 rounded-lg shadow-md border-2 border-[#E67E22] whitespace-nowrap">
                        <div className="text-sm font-semibold text-[#E67E22]">🏢 Anahata</div>
                        <div className="absolute top-full left-1/2 transform -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-[#E67E22]"></div>
                      </div>
                    </div>
                  </div>

                  {/* Location Info Overlay */}
                  <div className="absolute bottom-4 left-4 bg-white bg-opacity-95 rounded-lg p-3 shadow-lg max-w-xs">
                    <div className="flex items-center space-x-2">
                      <div className="bg-[#E67E22] rounded-full p-1">
                        <MapPin className="text-white" size={16} />
                      </div>
                      <div>
                        <div className="font-semibold text-sm text-gray-800">Anahata - Realm of Living</div>
                        <div className="text-xs text-gray-600">Soukya Road, Whitefield</div>
                      </div>
                    </div>
                  </div>

                  {/* Overlay for non-verified users - ONLY show when hasAccess is false */}
                  {!hasAccess && (
                    <div className="absolute inset-0 bg-black bg-opacity-40 flex items-center justify-center cursor-pointer z-20">
                      <div className="bg-white rounded-lg p-6 shadow-lg text-center max-w-sm">
                        <MapPin className="mx-auto mb-3 text-[#E67E22]" size={32} />
                        <h4 className="text-lg font-semibold text-gray-800 mb-2">Location Details Locked</h4>
                        <p className="text-sm text-gray-600 mb-2">Click to access detailed location information</p>
                        <p className="text-xs text-gray-500">Get connectivity details and nearby landmarks</p>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Map Action Buttons */}
              <div className="flex justify-center">
                {hasAccess ? (
                  <a
                    href={anahataLocation.googleMapsUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center bg-[#E67E22] hover:bg-[#c96b05] text-white px-6 py-3 rounded-lg font-semibold transition-colors"
                  >
                    <ExternalLink className="mr-2" size={18} />
                    View on Google Maps
                  </a>
                ) : (
                  <button
                    onClick={() => router.push('/Booksitevisit?source=location')}
                    className="inline-flex items-center bg-[#E67E22] hover:bg-[#c96b05] text-white px-6 py-3 rounded-lg font-semibold transition-colors"
                  >
                    <ExternalLink className="mr-2" size={18} />
                    View on Google Maps
                  </button>
                )}
              </div>

              {/* Location Highlights - Mobile: Horizontal Scroll, Desktop: Grid */}
              <div ref={highlightsScrollRef} className="md:hidden overflow-x-auto scrollbar-hide pb-4">
                <div className="flex gap-4 min-w-max px-4">
                  {locationHighlights.map((highlight, index) => (
                    <div key={index} className="bg-white rounded-lg shadow-md p-6 text-center hover:shadow-[0_0_20px_rgba(230,126,34,0.5)] hover:ring-2 hover:ring-[#E67E22] transition-all flex-shrink-0 w-64">
                      <div className="bg-orange-50 w-16 h-16 rounded-xl mx-auto mb-3 flex items-center justify-center text-3xl">{highlight.icon}</div>
                      <h3 className="font-semibold mb-2">{highlight.title}</h3>
                      <p className="text-sm text-gray-600">{highlight.description}</p>
                    </div>
                  ))}
                </div>
              </div>

              {/* Desktop: Grid */}
              <div className="hidden md:grid md:grid-cols-2 lg:grid-cols-4 gap-6">
                {locationHighlights.map((highlight, index) => (
                  <div key={index} className="bg-white rounded-lg shadow-md p-6 text-center hover:shadow-[0_0_20px_rgba(230,126,34,0.5)] hover:ring-2 hover:ring-[#E67E22] transition-all">
                    <div className="bg-orange-50 w-16 h-16 rounded-xl mx-auto mb-3 flex items-center justify-center text-3xl">{highlight.icon}</div>
                    <h3 className="font-semibold mb-2">{highlight.title}</h3>
                    <p className="text-sm text-gray-600">{highlight.description}</p>
                  </div>
                ))}
              </div>

              {/* Nearby Landmarks */}
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-xl font-semibold mb-4 text-center text-[#E67E22]">Nearby Landmarks & Distances</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="flex justify-between items-center py-2 border-b border-gray-100">
                    <span className="text-gray-700">Whitefield Railway Station</span>
                    <span className="text-[#E67E22] font-medium">5.3 km</span>
                  </div>
                  <div className="flex justify-between items-center py-2 border-b border-gray-100">
                    <span className="text-gray-700">Hopefarm Channasandra metro</span>
                    <span className="text-[#E67E22] font-medium">5.4 km</span>
                  </div>
                  <div className="flex justify-between items-center py-2 border-b border-gray-100">
                    <span className="text-gray-700">ITPL (IT Park)</span>
                    <span className="text-[#E67E22] font-medium">6.1 km</span>
                  </div>
                  <div className="flex justify-between items-center py-2 border-b border-gray-100">
                    <span className="text-gray-700">Hopefarm junction</span>
                    <span className="text-[#E67E22] font-medium">5 km</span>
                  </div>
                  <div className="flex justify-between items-center py-2 border-b border-gray-100">
                    <span className="text-gray-700">Bangalore International Academy</span>
                    <span className="text-[#E67E22] font-medium">1.7 km</span>
                  </div>
                  <div className="flex justify-between items-center py-2 border-b border-gray-100">
                    <span className="text-gray-700">Shanthinikethan mall</span>
                    <span className="text-[#E67E22] font-medium">7 km</span>
                  </div>
                </div>
              </div>

              {hasAccess && (
                <div className="text-center mt-8">
                  <p className="text-[#E67E22] font-medium">
                    ✓ You have access to detailed location information and connectivity details
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>
      </section>
    </>
  )
}
