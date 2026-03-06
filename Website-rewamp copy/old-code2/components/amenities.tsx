"use client"

import { useEffect, useRef } from "react"

const outdoorAmenities = [
  { name: "Mini Football Court", icon: "⚽", color: "bg-orange-50" },
  { name: "Basketball Court", icon: "🏀", color: "bg-orange-50" },
  { name: "Lawn Tennis Court", icon: "🎾", color: "bg-orange-50" },
  { name: "Sand Volleyball", icon: "🏐", color: "bg-orange-50" },
  { name: "Swimming Pool", icon: "🏊", color: "bg-orange-50" },
  { name: "Toddlers Pool", icon: "🧒", color: "bg-orange-50" },
  { name: "Children's Play Area", icon: "🎪", color: "bg-orange-50" },
  { name: "Jogging Track", icon: "🏃", color: "bg-orange-50" },
  { name: "Cycling Track", icon: "🚴", color: "bg-orange-50" },
  { name: "Elder's Corner", icon: "👴", color: "bg-orange-50" },
  { name: "Skating Rink", icon: "⛸️", color: "bg-orange-50" },
  { name: "Cricket Practice Net", icon: "🏏", color: "bg-orange-50" },
  { name: "Outdoor Fitness Station", icon: "💪", color: "bg-orange-50" },
  { name: "Pets Park", icon: "🐕", color: "bg-orange-50" },
  { name: "Amphitheater", icon: "🎭", color: "bg-orange-50" },
  { name: "Bonfire Area", icon: "🔥", color: "bg-orange-50" },
]

const clubhouseAmenities = [
  { name: "Guest Rooms", icon: "🏨", color: "bg-orange-50" },
  { name: "Swimming Pool", icon: "🏊", color: "bg-orange-50" },
  { name: "Snooker", icon: "🎱", color: "bg-orange-50" },
  { name: "Table Tennis", icon: "🏓", color: "bg-orange-50" },
  { name: "Party Hall", icon: "🎉", color: "bg-orange-50" },
  { name: "Yoga/Aerobics Room", icon: "🧘", color: "bg-orange-50" },
  { name: "Indoor Games", icon: "🎮", color: "bg-orange-50" },
  { name: "Mini Theatre", icon: "🎬", color: "bg-orange-50" },
  { name: "Gymnasium", icon: "💪", color: "bg-orange-50" },
  { name: "Indoor Badminton Court", icon: "🏸", color: "bg-orange-50" },
  { name: "Cafe", icon: "☕", color: "bg-orange-50" },
  { name: "Convenience Store", icon: "🏪", color: "bg-orange-50" },
  { name: "Library", icon: "📚", color: "bg-orange-50" },
  { name: "Health Centre", icon: "🏥", color: "bg-orange-50" },
  { name: "Kids Creche", icon: "👶", color: "bg-orange-50" },
  { name: "Coworking Space", icon: "💻", color: "bg-orange-50" },
  { name: "Spa/Salon", icon: "💆", color: "bg-orange-50" },
  { name: "Party Deck", icon: "🎊", color: "bg-orange-50" },
]

export default function Amenities() {
  const outdoorScrollRef = useRef<HTMLDivElement>(null)
  const clubhouseScrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    // Only auto-scroll on mobile
    if (window.innerWidth >= 768) return

    let outdoorIntervalId: NodeJS.Timeout | null = null
    let clubhouseIntervalId: NodeJS.Timeout | null = null
    let isOutdoorInteracting = false
    let isClubhouseInteracting = false
    let outdoorResumeTimeout: NodeJS.Timeout | null = null
    let clubhouseResumeTimeout: NodeJS.Timeout | null = null

    const startAutoScroll = (ref: React.RefObject<HTMLDivElement | null>, isInteracting: () => boolean) => {
      if (!ref.current) return null

      let scrollAmount = 0
      const scrollSpeed = 1
      const container = ref.current
      const maxScroll = container.scrollWidth - container.clientWidth

      const intervalId = setInterval(() => {
        if (container && !isInteracting()) {
          scrollAmount += scrollSpeed
          if (scrollAmount >= maxScroll) {
            scrollAmount = 0
          }
          container.scrollLeft = scrollAmount
        }
      }, 30)

      return intervalId
    }

    const handleOutdoorTouchStart = () => {
      isOutdoorInteracting = true
      if (outdoorResumeTimeout) clearTimeout(outdoorResumeTimeout)
    }

    const handleOutdoorTouchEnd = () => {
      if (outdoorResumeTimeout) clearTimeout(outdoorResumeTimeout)
      outdoorResumeTimeout = setTimeout(() => {
        isOutdoorInteracting = false
      }, 3000)
    }

    const handleClubhouseTouchStart = () => {
      isClubhouseInteracting = true
      if (clubhouseResumeTimeout) clearTimeout(clubhouseResumeTimeout)
    }

    const handleClubhouseTouchEnd = () => {
      if (clubhouseResumeTimeout) clearTimeout(clubhouseResumeTimeout)
      clubhouseResumeTimeout = setTimeout(() => {
        isClubhouseInteracting = false
      }, 3000)
    }

    const timeoutId = setTimeout(() => {
      outdoorIntervalId = startAutoScroll(outdoorScrollRef, () => isOutdoorInteracting)
      clubhouseIntervalId = startAutoScroll(clubhouseScrollRef, () => isClubhouseInteracting)

      if (outdoorScrollRef.current) {
        outdoorScrollRef.current.addEventListener('touchstart', handleOutdoorTouchStart)
        outdoorScrollRef.current.addEventListener('touchend', handleOutdoorTouchEnd)
      }
      if (clubhouseScrollRef.current) {
        clubhouseScrollRef.current.addEventListener('touchstart', handleClubhouseTouchStart)
        clubhouseScrollRef.current.addEventListener('touchend', handleClubhouseTouchEnd)
      }
    }, 100)

    return () => {
      clearTimeout(timeoutId)
      if (outdoorIntervalId) clearInterval(outdoorIntervalId)
      if (clubhouseIntervalId) clearInterval(clubhouseIntervalId)
      if (outdoorResumeTimeout) clearTimeout(outdoorResumeTimeout)
      if (clubhouseResumeTimeout) clearTimeout(clubhouseResumeTimeout)
      if (outdoorScrollRef.current) {
        outdoorScrollRef.current.removeEventListener('touchstart', handleOutdoorTouchStart)
        outdoorScrollRef.current.removeEventListener('touchend', handleOutdoorTouchEnd)
      }
      if (clubhouseScrollRef.current) {
        clubhouseScrollRef.current.removeEventListener('touchstart', handleClubhouseTouchStart)
        clubhouseScrollRef.current.removeEventListener('touchend', handleClubhouseTouchEnd)
      }
    }
  }, [])

  return (
    <section id="amenities" className="py-16">
      <div className="container mx-auto px-4">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold mb-6 text-[#E67E22]">50+ Premium Amenities</h2>
          <p className="text-[#E67E22] mb-4">
            Play Unlimited Anytime, Anywhere! Whether indoors or outdoors the game never stops!
          </p>
          <p className="text-lg font-semibold text-[#E67E22]">
            Experience Luxury, Leisure & Lifestyle – All Under One Roof!
          </p>
        </div>

        {/* Outdoor Amenities */}
        <div className="mb-16">
          <h3 className="text-2xl font-bold mb-8 text-center text-[#E67E22]">Outdoor Amenities</h3>
          <p className="text-center text-[#E67E22] mb-8">Spaces to play, breathe, unwind - Joy all around.</p>
          {/* Mobile: Horizontal Scroll */}
          <div ref={outdoorScrollRef} className="md:hidden overflow-x-auto pb-4 -mx-4 px-4 scrollbar-hide">
            <div className="flex gap-4 min-w-max">
              {outdoorAmenities.map((amenity, index) => (
                <div key={index} className="text-center flex-shrink-0 w-20">
                  <div
                    className={`${amenity.color} w-16 h-16 rounded-xl mx-auto mb-2 flex items-center justify-center text-xl`}
                  >
                    {amenity.icon}
                  </div>
                  <h4 className="text-xs font-medium text-gray-700">{amenity.name}</h4>
                </div>
              ))}
            </div>
          </div>
          {/* Desktop: Grid */}
          <div className="hidden md:grid md:grid-cols-4 lg:grid-cols-8 gap-4">
            {outdoorAmenities.map((amenity, index) => (
              <div key={index} className="text-center">
                <div
                  className={`${amenity.color} w-16 h-16 rounded-xl mx-auto mb-2 flex items-center justify-center text-xl`}
                >
                  {amenity.icon}
                </div>
                <h4 className="text-xs font-medium text-gray-700">{amenity.name}</h4>
              </div>
            ))}
          </div>
        </div>

        {/* Clubhouse Amenities */}
        <div className="bg-white rounded-lg p-8 border-2 border-[#E67E22]">
          <h3 className="text-2xl font-bold mb-8 text-center text-[#E67E22]">20,000 sq ft Clubhouse Amenities</h3>
          <p className="text-center text-[#E67E22] mb-8">
            Whether for culture or leisure, We have carefully crafted your space
          </p>
          {/* Mobile: Horizontal Scroll */}
          <div ref={clubhouseScrollRef} className="md:hidden overflow-x-auto pb-4 -mx-4 px-4 scrollbar-hide">
            <div className="flex gap-4 min-w-max">
              {clubhouseAmenities.map((amenity, index) => (
                <div key={index} className="text-center flex-shrink-0 w-20">
                  <div
                    className={`${amenity.color} w-16 h-16 rounded-xl mx-auto mb-2 flex items-center justify-center text-xl`}
                  >
                    {amenity.icon}
                  </div>
                  <h4 className="text-xs font-medium text-gray-700">{amenity.name}</h4>
                </div>
              ))}
            </div>
          </div>
          {/* Desktop: Grid */}
          <div className="hidden md:grid md:grid-cols-3 lg:grid-cols-6 gap-6">
            {clubhouseAmenities.map((amenity, index) => (
              <div key={index} className="text-center">
                <div
                  className={`${amenity.color} w-16 h-16 rounded-xl mx-auto mb-2 flex items-center justify-center text-xl`}
                >
                  {amenity.icon}
                </div>
                <h4 className="text-xs font-medium text-gray-700">{amenity.name}</h4>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
