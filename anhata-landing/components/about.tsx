"use client"
import { getImagePath } from "@/lib/utils-path"

import Image from "next/image"
import { useState } from "react"
import { ChevronDown } from "lucide-react"

interface AboutProps {
  filterType?: '2bhk' | '3bhk' | 'all'
}

const stats = [
  { value: "5 Acres", label: "PROJECT AREA" },
  { value: "440", label: "FAMILIES" },
  { value: "5", label: "TOWERS" },
  { value: "80%", label: "OPEN SPACE" },
  { value: "50+", label: "AMENITIES" },
  { value: "20,000 sq ft", label: "CLUB HOUSE" },
]

export default function About({ filterType = 'all' }: AboutProps) {
  const [showMore, setShowMore] = useState(false)

  const getHeading = () => {
    if (filterType === '3bhk') {
      return "Experience Refined 3BHK Living at Anahata, Whitefield"
    }
    if (filterType === '2bhk') {
      return "Experience Refined 2BHK Living at Anahata, Whitefield"
    }
    return "About Anahata"
  }

  const getIntroContent = () => {
    if (filterType === '3bhk') {
      return (
        <p className="text-white leading-relaxed text-lg mb-6">
          Are you looking for 3 bhk apartments in Whitefield Bangalore? Welcome to Ishtika Anahata Towers - a luxury gated community in the heart of Whitefield. The project consists of 3bhk apartments in Bangalore with a modern amenities. It's situated on over 5 acres of land with spacious layouts, and 80% of green open spaces for a healthy lifestyle.
          Anahata has connectivity to ITPL, Marathahalli and the Outer Ring Road. It's the perfect location for professionals looking for 3 bhk in Whitefield for sale. Enjoy urban luxury with green spaces with Pleasant homes
        </p>
      )
    }
    if (filterType === '2bhk') {
      return (
        <p className="text-white leading-relaxed text-lg mb-6">
          Are you looking for 2 bhk apartments in Whitefield Bangalore? Welcome to Ishtika Anahata Towers - a luxury gated community in the heart of Whitefield. The project consists of 2bhk apartments in Bangalore with a modern amenities. It's situated on over 5 acres of land with spacious layouts, and 80% of green open spaces for a healthy lifestyle.
          Anahata has connectivity to ITPL, Marathahalli and the Outer Ring Road. It's the perfect location for professionals looking for 2 bhk in Whitefield for sale. Enjoy urban luxury with green spaces with Pleasant homes
        </p>
      )
    }
    return (
      <>
        <p className="text-white leading-relaxed text-lg mb-6">
          Ishtika's Anahata Towers offers thoughtfully configured 2BHK in Whitefield, and spacious 3 BHK in Whitefield, Bangalore, all within a beautifully landscaped 5-acre gated community that fits perfectly into the modern urban lifestyle. Offering over 80% of its land as open green space, the community consists of five carefully planned residential towers which house 440 exclusive families, creating an environment that promotes community living in one of the fastest-growing neighborhoods of Bangalore.
        </p>
        <p className="text-white leading-relaxed text-lg mb-6">
          Anahata Towers is located near Soukya Road, which allows easy access to major business districts – ITPL, Marathahalli, and Outer Ring Road. The combination of contemporary layouts, quality amenities, and abundant green space make residents enjoy an unprecedented green living experience rarely found in urban Bangalore.
        </p>
      </>
    )
  }

  return (
    <section id="about" className="py-16 relative">
      {/* Background Image without Overlay */}
      <div className="absolute inset-0 z-0">
        <Image
          src={getImagePath("/images/aerial-view-bg.jpg")}
          alt="Aerial view of Anahata Towers"
          fill
          className="object-cover md:object-cover object-contain"
          quality={90}
        />
        <div className="absolute inset-0 bg-black opacity-40"></div>
      </div>

      <div className="container mx-auto px-4 relative z-10">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold mb-6 text-[#E67E22]">{getHeading()}</h2>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-center mb-12">
          {/* Text Content */}
          <div>
            {getIntroContent()}

            {showMore && (
              <>
                <h3 className="text-2xl font-semibold mb-4 text-white">Why Choose Anahata?</h3>
                <ul className="space-y-3 text-white mb-6">
                  <li className="flex items-start">
                    <span className="text-white mr-2">✓</span>
                    <span><strong>Prime Location:</strong> Strategic position in Whitefield with excellent connectivity to IT hubs, schools, hospitals, and shopping centers</span>
                  </li>
                  <li className="flex items-start">
                    <span className="text-white mr-2">✓</span>
                    <span><strong>Spacious Homes:</strong> Thoughtfully designed 2BHK (1164-1279 sq ft) and 3BHK (1511-1758 sq ft) apartments with premium specifications</span>
                  </li>
                  <li className="flex items-start">
                    <span className="text-white mr-2">✓</span>
                    <span><strong>World-Class Amenities:</strong> 50+ lifestyle amenities including a 20,000 sq ft clubhouse, swimming pool, sports facilities, and landscaped gardens</span>
                  </li>
                  <li className="flex items-start">
                    <span className="text-white mr-2">✓</span>
                    <span><strong>Trusted Developer:</strong> Backed by Ishtika's legacy of quality construction and timely delivery</span>
                  </li>
                  <li className="flex items-start">
                    <span className="text-white mr-2">✓</span>
                    <span><strong>Investment Opportunity:</strong> High appreciation potential in Bangalore's fastest-growing IT corridor</span>
                  </li>
                </ul>
                <p className="text-white leading-relaxed text-lg">
                  Every apartment at Anahata is designed for expansive living, featuring airy balconies, abundant natural light, and cross-ventilation. The project adheres to Vaastu principles and incorporates sustainable features for eco-friendly living.
                </p>
              </>
            )}

            {!showMore && (
              <button
                onClick={() => setShowMore(true)}
                className="inline-flex items-center px-6 py-3 bg-white hover:bg-gray-100 text-[#E67E22] rounded-lg font-semibold transition-colors"
              >
                Read More
                <ChevronDown className="ml-2 h-5 w-5" />
              </button>
            )}
          </div>

          {/* Image - Hidden since background is now aerial view */}
          <div className="relative aspect-video hidden">
            <Image
              src={getImagePath("/images/modern-living-room.jpg")}
              alt="Modern luxury living room interior showcasing premium finishes and contemporary design at Anahata Whitefield"
              fill
              loading="lazy"
              sizes="(max-width: 1024px) 100vw, 50vw"
              className="object-cover rounded-lg shadow-lg"
            />
          </div>
        </div>

        {/* Stats Section */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-6">
          {stats.map((stat, index) => (
            <div key={index} className="text-center p-4">
              <p className="text-2xl md:text-3xl font-bold text-[#E67E22]">{stat.value}</p>
              <p className="text-sm text-white uppercase mt-2 font-semibold">{stat.label}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
