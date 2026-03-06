"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import ModernContactModal from "./modern-contact-modal"

export default function ContactModal() {
  const [isModalOpen, setIsModalOpen] = useState(false)

  return (
    <>
      {/* CTA Section */}
      <section className="py-16 relative">
        <div
          className="absolute inset-0 bg-cover bg-center bg-fixed"
          style={{
            backgroundImage: "url(/placeholder.svg?height=400&width=1200&query=luxury residential building at sunset)",
          }}
        >
          <div className="absolute inset-0 bg-black bg-opacity-60" />
        </div>

        <div className="relative container mx-auto px-4 text-center text-white">
          <p className="text-lg mb-4">Free cab facility for you and your family.</p>
          <h2 className="text-3xl md:text-4xl font-bold mb-8">Book your site visit now.</h2>
          <div className="space-y-4 md:space-y-0 md:space-x-4 md:flex md:justify-center">
            <a href="tel:+917338628777" className="inline-block">
              <Button
                size="lg"
                variant="outline"
                className="text-white border-white hover:bg-white hover:text-black bg-transparent"
              >
                Call: +91 7338628777
              </Button>
            </a>
            <Button size="lg" onClick={() => setIsModalOpen(true)} className="bg-blue-600 hover:bg-blue-700">
              Schedule Site Visit
            </Button>
          </div>
        </div>
      </section>

      <ModernContactModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        title="Schedule Your Site Visit"
        subtitle="Book your free site visit with complimentary cab facility"
        source="site_visit"
      />
    </>
  )
}
