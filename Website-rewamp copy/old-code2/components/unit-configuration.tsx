"use client"

import { useState } from "react"
import Image from "next/image"

const configurations = [
  {
    id: "2bhk",
    title: "2 BHK Apartments",
    description:
      "Upgrade from Compact to Comfort! Experience 100% Vastu-compliant living with enhanced ventilation for a healthier, happier home.",
    carpetArea: "1136 - 1322 sq ft",
    sba: "1511 - 1721 sq ft",
    towers: "Available in all 5 towers",
    image: "/placeholder.svg?height=400&width=600",
  },
  {
    id: "3bhk",
    title: "3 BHK Apartments",
    description:
      "Spacious 3BHK units designed for families seeking luxury and comfort. Every detail is crafted with care, using premium materials and thoughtful design.",
    carpetArea: "1511 - 1721 sq ft",
    sba: "1692 - 1758 sq ft",
    towers: "Available in Towers A, B, C, D & E",
    image: "/placeholder.svg?height=400&width=600",
  },
]

export default function UnitConfiguration() {
  const [activeTab, setActiveTab] = useState("2bhk")

  return (
    <section className="py-16">
      <div className="container mx-auto px-4">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold mb-6">Unit Configuration</h2>
          <p className="text-gray-600 max-w-2xl mx-auto">
            Choose from thoughtfully designed 2BHK and 3BHK apartments across 5 towers, each crafted for refined living.
          </p>
        </div>

        {/* Tab Navigation */}
        <div className="flex justify-center mb-8 border-b">
          {configurations.map((config) => (
            <button
              key={config.id}
              onClick={() => setActiveTab(config.id)}
              className={`px-8 py-3 mx-2 mb-2 rounded-t-lg transition-colors font-semibold ${
                activeTab === config.id
                  ? "bg-blue-600 text-white border-b-2 border-blue-600"
                  : "text-gray-600 hover:text-blue-600"
              }`}
            >
              {config.title}
            </button>
          ))}
        </div>

        {/* Tab Content */}
        {configurations.map((config) => (
          <div key={config.id} className={`${activeTab === config.id ? "block" : "hidden"}`}>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
              <div>
                <h3 className="text-2xl font-bold mb-6">{config.title}</h3>
                <p className="text-gray-600 leading-relaxed mb-6">{config.description}</p>

                <div className="space-y-4">
                  <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg">
                    <span className="font-medium">Carpet Area:</span>
                    <span className="text-blue-600 font-semibold">{config.carpetArea}</span>
                  </div>
                  <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg">
                    <span className="font-medium">Super Built-up Area:</span>
                    <span className="text-blue-600 font-semibold">{config.sba}</span>
                  </div>
                  <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg">
                    <span className="font-medium">Availability:</span>
                    <span className="text-green-600 font-semibold">{config.towers}</span>
                  </div>
                </div>
              </div>
              <div>
                <Image
                  src={config.image || "/placeholder.svg"}
                  alt={config.title}
                  width={600}
                  height={400}
                  className="rounded-lg shadow-md"
                />
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
