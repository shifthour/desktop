"use client"
import { getImagePath } from "@/lib/utils-path"

import { useState, useEffect, useRef } from "react"
import Image from "next/image"
import Link from "next/link"

const galleryImages = [
  {
    src: getImagePath("/images/gallery/amphitheater-night.png"),
    alt: "Beautifully lit amphitheater seating area with tiered landscaping and residential towers at night",
    title: "Amphitheater & Evening Ambiance",
  },
  {
    src: getImagePath("/images/gallery/sports-facilities-day.png"),
    alt: "Outdoor sports courts with volleyball/badminton nets and residential towers in daylight",
    title: "Sports Facilities & Recreation",
  },
  {
    src: getImagePath("/images/gallery/landscaped-pathway.png"),
    alt: "Sunset view through landscaped walkways with lush greenery and flowering plants",
    title: "Landscaped Walkways & Gardens",
  },
  {
    src: getImagePath("/images/gallery/aerial-complex-view.png"),
    alt: "Aerial view showing multiple residential towers, swimming pool, sports courts and green spaces",
    title: "Aerial View of Complete Complex",
  },
  {
    src: getImagePath("/images/gallery/tennis-courts.png"),
    alt: "Ground level view of tennis and badminton courts with residential towers under blue sky",
    title: "Tennis & Badminton Courts",
  },
  {
    src: getImagePath("/images/gallery/master-plan.png"),
    alt: "Detailed master plan showing layout of the entire residential complex",
    title: "Master Plan",
  },
  {
    src: getImagePath("/images/gallery/kids-play-area.png"),
    alt: "Vibrant children's play area with modern equipment and safety features",
    title: "Kids Play Area",
  },
  {
    src: getImagePath("/images/gallery/exterior-landscaping.png"),
    alt: "Beautiful exterior landscaping with manicured gardens and pathways",
    title: "Exterior Landscaping",
  },
  {
    src: getImagePath("/images/gallery/cover-entrance.png"),
    alt: "Grand entrance with covered portico and elegant design",
    title: "Entrance & Facade",
  },
  {
    src: getImagePath("/images/gallery/elite-specifications.png"),
    alt: "Elite specifications and premium finishes showcase",
    title: "Elite Specifications",
  },
  {
    src: getImagePath("/images/gallery/outdoor-clubhouse.png"),
    alt: "Luxurious outdoor clubhouse with seating areas and amenities",
    title: "Outdoor Clubhouse",
  },
  {
    src: getImagePath("/images/gallery/leisure-amenities.png"),
    alt: "Comprehensive leisure amenities for relaxation and recreation",
    title: "Leisure Amenities",
  },
  {
    src: getImagePath("/images/gallery/outdoor-amenities.png"),
    alt: "Wide range of outdoor amenities and recreational facilities",
    title: "Outdoor Amenities",
  },
  {
    src: getImagePath("/images/gallery/WhatsApp Image 2025-10-01 at 1.23.21 PM.jpeg"),
    alt: "Anahata residential project view",
    title: "Project View 1",
  },
  {
    src: getImagePath("/images/gallery/WhatsApp Image 2025-10-01 at 1.23.22 PM.jpeg"),
    alt: "Anahata residential project view",
    title: "Project View 2",
  },
  {
    src: getImagePath("/images/gallery/WhatsApp Image 2025-10-01 at 1.23.22 PM (1).jpeg"),
    alt: "Anahata residential project view",
    title: "Project View 3",
  },
  {
    src: getImagePath("/images/gallery/WhatsApp Image 2025-10-01 at 1.23.22 PM (2).jpeg"),
    alt: "Anahata residential project view",
    title: "Project View 4",
  },
  {
    src: getImagePath("/images/gallery/WhatsApp Image 2025-10-01 at 1.23.23 PM.jpeg"),
    alt: "Anahata residential project view",
    title: "Project View 5",
  },
  {
    src: getImagePath("/images/gallery/WhatsApp Image 2025-10-01 at 1.23.23 PM (1).jpeg"),
    alt: "Anahata residential project view",
    title: "Project View 6",
  },
]

interface GalleryProps {
  filterType?: '2bhk' | '3bhk' | 'all'
}

export default function Gallery({ filterType = 'all' }: GalleryProps) {
  const [selectedImage, setSelectedImage] = useState<number | null>(null)
  const galleryScrollRef = useRef<HTMLDivElement>(null)

  const getHeading = () => {
    if (filterType === '3bhk') {
      return "Modern 3BHK Apartment View with Luxury Features"
    }
    if (filterType === '2bhk') {
      return "Modern 2BHK Apartment View with Luxury Features"
    }
    return "Project Gallery"
  }

  const getDescription = () => {
    if (filterType === '3bhk') {
      return "3 bhk apartments in Whitefield with landscaped gardens, sports facilities, and outdoor lifestyle spaces for modern living."
    }
    if (filterType === '2bhk') {
      return "2 bhk apartments in Whitefield with landscaped gardens, sports facilities, and outdoor lifestyle spaces for modern living."
    }
    return "Explore the stunning visuals of Anahata - from luxurious outdoor spaces to world-class sports facilities and beautiful landscaping of 2BHK and 3BHK Apartments in Whitefield."
  }

  useEffect(() => {
    // Only auto-scroll on mobile
    if (window.innerWidth >= 768) return

    let intervalId: NodeJS.Timeout | null = null
    let isUserInteracting = false
    let resumeTimeout: NodeJS.Timeout | null = null

    const startAutoScroll = () => {
      if (!galleryScrollRef.current) return

      let scrollAmount = 0
      const scrollSpeed = 1
      const container = galleryScrollRef.current
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
      if (galleryScrollRef.current) {
        galleryScrollRef.current.addEventListener('touchstart', handleTouchStart)
        galleryScrollRef.current.addEventListener('touchend', handleTouchEnd)
      }
    }, 100)

    return () => {
      clearTimeout(timeoutId)
      if (intervalId) clearInterval(intervalId)
      if (resumeTimeout) clearTimeout(resumeTimeout)
      if (galleryScrollRef.current) {
        galleryScrollRef.current.removeEventListener('touchstart', handleTouchStart)
        galleryScrollRef.current.removeEventListener('touchend', handleTouchEnd)
      }
    }
  }, [])

  return (
    <>
      <section id="gallery" className="py-16 bg-white">
        <div className="container mx-auto px-4">
          <div className="text-center mb-12">
            <h2 className="text-3xl md:text-4xl font-bold mb-6 text-[#E67E22]">{getHeading()}</h2>
            <p className="text-[#E67E22]">
              {getDescription()}
            </p>
          </div>

          {/* Mobile: Horizontal Scroll */}
          <div ref={galleryScrollRef} className="md:hidden overflow-x-auto pb-4 -mx-4 px-4 scrollbar-hide mb-12">
            <div className="flex gap-4 min-w-max">
              {galleryImages.map((image, index) => (
                <div
                  key={index}
                  onClick={() => setSelectedImage(index)}
                  className="group relative overflow-hidden rounded-lg shadow-md hover:shadow-[0_0_20px_rgba(230,126,34,0.5)] hover:ring-2 hover:ring-[#E67E22] transition-all duration-300 cursor-pointer flex-shrink-0 w-64"
                >
                  <div className="aspect-square relative">
                    <Image
                      src={image.src || "/placeholder.svg"}
                      alt={image.alt}
                      fill
                      loading="lazy"
                      sizes="256px"
                      className="object-cover group-hover:scale-110 transition-transform duration-300"
                    />
                    <div className="absolute inset-0 bg-gradient-to-t from-[#E67E22] to-transparent opacity-0 group-hover:opacity-70 transition-all duration-300 flex items-end">
                      <div className="p-4 text-white transform translate-y-full group-hover:translate-y-0 transition-transform duration-300">
                        <h3 className="font-semibold text-sm">{image.title}</h3>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Desktop: Grid */}
          <div className="hidden md:grid md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-6">
            {galleryImages.map((image, index) => (
              <div
                key={index}
                onClick={() => setSelectedImage(index)}
                className="group relative overflow-hidden rounded-lg shadow-md hover:shadow-[0_0_20px_rgba(230,126,34,0.5)] hover:ring-2 hover:ring-[#E67E22] transition-all duration-300 cursor-pointer"
              >
                <div className="aspect-square relative">
                  <Image
                    src={image.src || "/placeholder.svg"}
                    alt={image.alt}
                    fill
                    loading="lazy"
                    sizes="(max-width: 640px) 100vw, (max-width: 768px) 50vw, (max-width: 1024px) 33vw, 20vw"
                    className="object-cover group-hover:scale-110 transition-transform duration-300"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-[#E67E22] to-transparent opacity-0 group-hover:opacity-70 transition-all duration-300 flex items-end">
                    <div className="p-4 text-white transform translate-y-full group-hover:translate-y-0 transition-transform duration-300">
                      <h3 className="font-semibold text-sm">{image.title}</h3>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>

          <div className="text-center mt-12">
            <p className="text-[#E67E22] mb-4">
              Want to see more? Schedule a site visit to experience Anahata in person.
            </p>
            <Link
              href="/Booksitevisit?source=gallery"
              className="inline-block bg-[#E67E22] hover:bg-[#c96b05] text-white px-8 py-3 rounded-lg font-semibold transition-colors"
            >
              Schedule Site Visit
            </Link>
          </div>
        </div>
      </section>

      {/* Image Zoom Modal */}
      {selectedImage !== null && (
        <div
          className="fixed inset-0 z-50 bg-black bg-opacity-90 flex items-center justify-center p-4"
          onClick={() => setSelectedImage(null)}
        >
          <button
            onClick={() => setSelectedImage(null)}
            className="absolute top-4 right-4 text-white text-4xl hover:text-[#E67E22] transition-colors z-10"
            aria-label="Close"
          >
            ×
          </button>

          {/* Previous Button */}
          {selectedImage > 0 && (
            <button
              onClick={(e) => {
                e.stopPropagation()
                setSelectedImage(selectedImage - 1)
              }}
              className="absolute left-4 text-white text-4xl hover:text-[#E67E22] transition-colors z-10"
              aria-label="Previous image"
            >
              ‹
            </button>
          )}

          {/* Next Button */}
          {selectedImage < galleryImages.length - 1 && (
            <button
              onClick={(e) => {
                e.stopPropagation()
                setSelectedImage(selectedImage + 1)
              }}
              className="absolute right-4 text-white text-4xl hover:text-[#E67E22] transition-colors z-10"
              aria-label="Next image"
            >
              ›
            </button>
          )}

          <div className="relative max-w-7xl max-h-[90vh] w-full h-full" onClick={(e) => e.stopPropagation()}>
            <Image
              src={galleryImages[selectedImage].src}
              alt={galleryImages[selectedImage].alt}
              fill
              className="object-contain"
              sizes="100vw"
            />
            <div className="absolute bottom-0 left-0 right-0 bg-[#E67E22] bg-opacity-90 text-white p-4 text-center">
              <h3 className="text-lg font-semibold">{galleryImages[selectedImage].title}</h3>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
