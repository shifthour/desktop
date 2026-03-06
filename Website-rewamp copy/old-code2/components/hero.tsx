"use client"
import { getImagePath } from "@/lib/utils-path"

import { useState, useEffect, useRef } from "react"
import { ChevronLeft, ChevronRight, Volume2, VolumeX } from "lucide-react"
import { useRouter } from "next/navigation"

// Detect if device is mobile and connection is slow
const isMobileDevice = () => {
  if (typeof window === 'undefined') return false
  return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent)
}

const isSlowConnection = () => {
  if (typeof window === 'undefined') return false
  const connection = (navigator as any).connection || (navigator as any).mozConnection || (navigator as any).webkitConnection
  if (connection) {
    return connection.effectiveType === 'slow-2g' || connection.effectiveType === '2g' || connection.effectiveType === '3g'
  }
  return false
}

const getSlides = (filterType: '2bhk' | '3bhk' | 'all') => [
  {
    type: "video",
    src: getImagePath("/project-video.mp4"),
    poster: getImagePath("/images/hero-poster.jpeg"),
    title: filterType === '3bhk'
      ? "Luxury 3BHK Apartments for Sale in Bangalore Whitefield"
      : filterType === '2bhk'
      ? "Luxury 2BHK Apartments for Sale in Bangalore Whitefield"
      : "",
    subtitle: filterType === '3bhk'
      ? "3BHK gated community apartment sale @Saukya Road, Whitefield, Bangalore"
      : filterType === '2bhk'
      ? "2BHK gated community apartment sale @Saukya Road, Whitefield, Bangalore"
      : "2BHK & 3BHK Luxury Gated Community Apartments @ Soukya Road, Whitefield, Bengaluru",
    cta: "Book Site Visit",
    showContent: true,
    showPricing: true,
  },
]

const pricingData = [
  { type: "2 BHK", price: "Starting from ₹89 Lakhs*", area: "1164 - 1279 sq ft" },
  { type: "3 BHK 2T", price: "Starting from ₹1.15 Cr*", area: "1511 - 1591 sq ft" },
  { type: "3 BHK 3T", price: "Starting from ₹1.3 Cr*", area: "1670 - 1758 sq ft" },
]

interface HeroProps {
  filterType?: '2bhk' | '3bhk' | 'all'
}

export default function Hero({ filterType = 'all' }: HeroProps) {
  const router = useRouter()

  // Get slides based on filter type
  const slides = getSlides(filterType)

  // Filter pricing data based on filterType
  const filteredPricingData = filterType === '2bhk'
    ? pricingData.filter(item => item.type === '2 BHK')
    : filterType === '3bhk'
    ? pricingData.filter(item => item.type.includes('3 BHK'))
    : pricingData
  const [currentSlide, setCurrentSlide] = useState(0)
  const videoRefs = useRef<(HTMLVideoElement | null)[]>([])
  const timerRef = useRef<NodeJS.Timeout | null>(null)
  const [shouldPlayVideo, setShouldPlayVideo] = useState(true)
  const pricingScrollRef = useRef<HTMLDivElement>(null)
  const [isMuted, setIsMuted] = useState(true)

  // Check device and connection on mount
  useEffect(() => {
    const checkVideoPlayback = () => {
      const isMobile = isMobileDevice()
      const isSlow = isSlowConnection()

      // Disable video on mobile with slow connection
      if (isMobile && isSlow) {
        setShouldPlayVideo(false)
      }

      // On iOS, we need to handle video playback differently
      const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) && !(window as any).MSStream
      if (isIOS) {
        // iOS requires user interaction for video, but playsInline helps
        setShouldPlayVideo(true) // Keep enabled but with optimizations
      }
    }

    checkVideoPlayback()
  }, [])

  // Auto-scroll for mobile pricing cards
  useEffect(() => {
    // Only auto-scroll on mobile
    if (typeof window !== 'undefined' && window.innerWidth >= 768) return

    let intervalId: NodeJS.Timeout | null = null
    let isUserInteracting = false
    let resumeTimeout: NodeJS.Timeout | null = null

    const startAutoScroll = () => {
      if (!pricingScrollRef.current) return

      let scrollAmount = 0
      const scrollSpeed = 1
      const container = pricingScrollRef.current
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

    // Handle user interaction
    const handleTouchStart = () => {
      isUserInteracting = true
      if (resumeTimeout) clearTimeout(resumeTimeout)
    }

    const handleTouchEnd = () => {
      if (resumeTimeout) clearTimeout(resumeTimeout)
      resumeTimeout = setTimeout(() => {
        isUserInteracting = false
      }, 5000) // Resume auto-scroll 5 seconds after user stops touching
    }

    // Start after a delay to ensure DOM is ready
    const timeoutId = setTimeout(() => {
      startAutoScroll()

      if (pricingScrollRef.current) {
        pricingScrollRef.current.addEventListener('touchstart', handleTouchStart)
        pricingScrollRef.current.addEventListener('touchend', handleTouchEnd)
      }
    }, 500)

    return () => {
      clearTimeout(timeoutId)
      if (intervalId) clearInterval(intervalId)
      if (resumeTimeout) clearTimeout(resumeTimeout)
      if (pricingScrollRef.current) {
        pricingScrollRef.current.removeEventListener('touchstart', handleTouchStart)
        pricingScrollRef.current.removeEventListener('touchend', handleTouchEnd)
      }
    }
  }, [currentSlide])

  const startSlideTimer = () => {
    if (timerRef.current) {
      clearTimeout(timerRef.current)
    }

    const currentSlideData = slides[currentSlide]
    let duration = 5000 // Default 5 seconds for images

    if (currentSlideData.type === "video" && videoRefs.current[currentSlide]) {
      // Use video duration if available, otherwise default to 15 seconds for videos
      const videoDuration = videoRefs.current[currentSlide]?.duration
      duration = videoDuration && !isNaN(videoDuration) ? videoDuration * 1000 : 15000
    }

    timerRef.current = setTimeout(() => {
      setCurrentSlide((prev) => (prev + 1) % slides.length)
    }, duration)
  }

  useEffect(() => {
    startSlideTimer()
    return () => {
      if (timerRef.current) {
        clearTimeout(timerRef.current)
      }
    }
  }, [currentSlide])

  const nextSlide = () => {
    setCurrentSlide((prev) => (prev + 1) % slides.length)
  }

  const prevSlide = () => {
    setCurrentSlide((prev) => (prev - 1 + slides.length) % slides.length)
  }

  const handleCTAClick = (ctaText: string) => {
    // Track button click event in both GTM and GA4
    if (typeof window !== 'undefined') {
      console.log('Tracking CTA button click:', ctaText)

      // Method 1: GTM dataLayer
      if (window.dataLayer) {
        window.dataLayer.push({
          event: 'cta_click',
          event_category: 'User Interaction',
          event_label: ctaText,
          cta_type: 'site_visit'
        })
      }

      // Method 2: Direct GA4 tracking
      if (window.gtag) {
        window.gtag('event', 'cta_click', {
          event_category: 'User Interaction',
          event_label: ctaText,
          cta_type: 'site_visit'
        })
      }
    }

    router.push('/Booksitevisit?source=site_visit')
  }

  const handlePricingCardClick = (apartmentType: string) => {
    // Track the card click for Google Analytics
    if (typeof window !== 'undefined' && window.gtag) {
      window.gtag('event', 'apartment_card_click', {
        'event_category': 'Engagement',
        'event_label': apartmentType,
        'value': apartmentType
      })
    }

    const source = `pricing_card_${apartmentType.toLowerCase().replace(/\s+/g, '_')}`
    router.push(`/Booksitevisit?source=${source}&apartmentType=${encodeURIComponent(apartmentType)}`)
  }

  const handleVideoLoaded = (index: number) => {
    // Restart timer when video metadata is loaded
    if (index === currentSlide) {
      startSlideTimer()
    }
  }

  const handleVideoEnded = (index: number) => {
    // When video ends naturally, move to next slide
    if (index === currentSlide) {
      setCurrentSlide((prev) => (prev + 1) % slides.length)
    }
  }

  return (
    <>
      <section id="home" className="relative h-screen overflow-hidden">
        {slides.map((slide, index) => (
          <div
            key={index}
            className={`absolute inset-0 transition-opacity duration-1000 ${
              index === currentSlide ? "opacity-100" : "opacity-0"
            }`}
          >
            {slide.type === "video" && shouldPlayVideo ? (
              <div className="absolute inset-0">
                <video
                  ref={(el) => { videoRefs.current[index] = el }}
                  className="w-full h-full object-cover"
                  autoPlay
                  muted={isMuted}
                  loop
                  playsInline
                  webkit-playsinline="true"
                  x-webkit-airplay="allow"
                  preload="metadata"
                  poster={slide.poster}
                  onLoadedMetadata={() => handleVideoLoaded(index)}
                  onEnded={() => handleVideoEnded(index)}
                  onCanPlay={(e) => {
                    // Ensure smooth playback on mobile
                    const video = e.target as HTMLVideoElement
                    if (video && index === currentSlide) {
                      video.playbackRate = 1.0
                      video.play().catch(() => {
                        console.log('Video autoplay failed')
                      })
                    }
                  }}
                  style={{
                    WebkitTransform: 'translateZ(0)',
                    transform: 'translateZ(0)',
                    WebkitBackfaceVisibility: 'hidden',
                    backfaceVisibility: 'hidden',
                    perspective: 1000,
                    backgroundColor: 'black'
                  }}
                >
                  <source src={slide.src} type="video/mp4" />
                  Your browser does not support the video tag.
                </video>
                <div className="absolute inset-0 bg-black bg-opacity-40" />
                {/* Audio toggle button */}
                <button
                  onClick={() => setIsMuted(!isMuted)}
                  className="absolute top-24 md:top-24 left-4 z-50 w-12 h-12 flex items-center justify-center bg-gray-800 bg-opacity-80 hover:bg-opacity-100 rounded-full shadow-lg transition-all duration-300 cursor-pointer"
                  aria-label={isMuted ? "Unmute video" : "Mute video"}
                >
                  {isMuted ? (
                    <VolumeX className="w-5 h-5 text-white" />
                  ) : (
                    <Volume2 className="w-5 h-5 text-white" />
                  )}
                </button>
              </div>
            ) : slide.type === "video" && !shouldPlayVideo ? (
              // Show static image on mobile with slow connection
              <div className="absolute inset-0 bg-cover bg-center" style={{ backgroundImage: `url(${slide.poster})` }}>
                <div className="absolute inset-0 bg-black bg-opacity-40" />
              </div>
            ) : (
              <div className="absolute inset-0 bg-cover bg-center" style={{ backgroundImage: `url(${slide.src})` }}>
                <div className="absolute inset-0 bg-black bg-opacity-40" />
              </div>
            )}

            {/* Content for Video 1 (Full content with pricing) */}
            {slide.showContent && (
              <div className="relative h-full flex flex-col justify-between text-center text-white z-10 pb-2 md:pb-8">
                {/* Top section - Text */}
                <div className="max-w-6xl px-4 w-full mx-auto mt-20 md:mt-32">
                  {slide.title && (
                    <h1 className="text-lg xs:text-xl sm:text-2xl md:text-4xl lg:text-6xl font-bold mb-2 md:mb-4 animate-fade-in leading-tight px-2">
                      {slide.title}
                    </h1>
                  )}
                  <p className="text-xs xs:text-sm sm:text-base md:text-xl lg:text-2xl mb-2 md:mb-4 animate-fade-in-delay leading-relaxed px-2">
                    {slide.subtitle}
                  </p>

                  {/* Pre-EMI text */}
                  <p className="text-sm sm:text-base md:text-xl mb-3 md:mb-4 text-[#E67E22] font-semibold animate-fade-in-delay">
                    No Pre-EMI till possession (March 2028)
                  </p>
                </div>

                {/* Bottom section - Button and Pricing */}
                <div className="max-w-6xl px-4 w-full mx-auto mb-4 md:mb-8">
                  <button
                    className="relative z-50 bg-[#E67E22] hover:bg-[#c96b05] text-white text-sm md:text-lg px-6 py-3 md:px-8 md:py-4 mb-4 md:mb-8 w-full sm:w-auto max-w-xs mx-auto rounded-lg cursor-pointer transition-colors touch-manipulation"
                    onClick={() => handleCTAClick(slide.cta)}
                  >
                    {slide.cta}
                  </button>

                  {/* Pricing Cards - Desktop only, mobile will be outside */}
                  {slide.showPricing && (
                    <div className={`hidden md:grid gap-6 max-w-3xl md:max-w-4xl mx-auto px-4 ${
                      filteredPricingData.length === 1
                        ? 'md:grid-cols-1 place-items-center'
                        : filteredPricingData.length === 2
                        ? 'md:grid-cols-2 place-items-center'
                        : 'md:grid-cols-3'
                    }`}>
                      {filteredPricingData.map((item, index) => (
                        <div
                          key={index}
                          onClick={() => handlePricingCardClick(item.type)}
                          className="flex items-center space-x-4 bg-white bg-opacity-95 text-gray-800 p-4 rounded-lg shadow-md backdrop-blur-sm cursor-pointer transform transition-all duration-200 hover:scale-105 hover:shadow-xl hover:bg-opacity-100 active:scale-[1.02] relative group w-full max-w-sm"
                        >
                          <div className="text-left min-w-0 flex-1">
                            <p className="font-semibold text-lg group-hover:text-blue-700 transition-colors">{item.type}</p>
                            <p className="text-gray-600 text-sm leading-tight">{item.price}</p>
                            <p className="text-xs text-gray-500 leading-tight">{item.area}</p>
                          </div>
                          <div className="absolute right-3 top-1/2 -translate-y-1/2 opacity-0 group-hover:opacity-100 transition-opacity">
                            <ChevronRight className="text-blue-600" size={20} />
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        ))}
      </section>

      {/* Mobile Pricing Cards - Below Hero */}
      <div className="md:hidden bg-gray-50 py-4">
        <div ref={pricingScrollRef} className="overflow-x-auto scrollbar-hide">
          <div className="flex gap-4 px-4 min-w-max">
            {filteredPricingData.map((item, index) => (
              <div
                key={index}
                onClick={() => handlePricingCardClick(item.type)}
                className="flex items-center space-x-2 bg-white text-gray-800 p-3 rounded-lg shadow-md cursor-pointer transform transition-all duration-200 active:scale-95 relative group flex-shrink-0 w-72"
              >
                <div className="text-left min-w-0 flex-1">
                  <p className="font-semibold text-sm group-hover:text-blue-700 transition-colors">{item.type}</p>
                  <p className="text-gray-600 text-xs leading-tight">{item.price}</p>
                  <p className="text-xs text-gray-500 leading-tight">{item.area}</p>
                </div>
                <div className="flex-shrink-0">
                  <ChevronRight className="text-blue-600" size={20} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </>
  )
}
