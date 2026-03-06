"use client"

import { useEffect } from "react"
import { CheckCircle, Home, ArrowLeft } from 'lucide-react'
import Link from "next/link"

export default function ThankYou() {
  // Track page view in Google Tag Manager and GA4 when component mounts
  useEffect(() => {
    // Add a small delay to ensure tracking scripts are loaded
    const timer = setTimeout(() => {
      if (typeof window !== 'undefined') {
        const pagePath = window.location.pathname + window.location.search
        console.log('Tracking Thank You page view:', pagePath)

        // Method 1: Push virtual pageview to GTM dataLayer
        if (window.dataLayer) {
          window.dataLayer.push({
            event: 'virtualPageview',
            page: {
              path: pagePath,
              title: 'Thank You Page',
              url: window.location.href
            }
          })
          console.log('GTM virtualPageview pushed')
        }

        // Method 2: Direct GA4 config tracking (most reliable for SPAs)
        if (window.gtag) {
          window.gtag('config', 'G-W6C28DJ6M3', {
            page_title: 'Thank You Page',
            page_path: pagePath,
            page_location: window.location.href
          })
          console.log('GA4 config page view sent')
        }

        // Method 3: Send explicit page_view event to GA4
        if (window.gtag) {
          window.gtag('event', 'page_view', {
            page_title: 'Thank You Page',
            page_path: pagePath,
            page_location: window.location.href,
            send_to: 'G-W6C28DJ6M3'
          })
          console.log('GA4 page_view event sent')
        }

        // Method 4: Track form conversion
        if (window.gtag) {
          window.gtag('event', 'conversion', {
            send_to: 'AW-17340305414/form_submission',
            event_category: 'Form',
            event_label: 'Thank You Page Visit'
          })
          console.log('Conversion event sent')
        }
      }
    }, 1000)

    return () => clearTimeout(timer)
  }, [])

  return (
    <div className="min-h-screen bg-gradient-to-br from-orange-50 to-white flex items-center justify-center px-4 py-8">
      <div className="max-w-2xl w-full">
        {/* Success Card */}
        <div className="bg-white rounded-2xl shadow-2xl overflow-hidden">
          {/* Success Icon */}
          <div className="bg-gradient-to-r from-[#E67E22] to-[#c96b05] px-6 py-12 text-center">
            <CheckCircle className="h-24 w-24 text-white mx-auto mb-4 animate-bounce" />
            <h1 className="text-4xl font-bold text-white mb-2">Thank You!</h1>
            <p className="text-orange-100 text-lg">Your request has been submitted successfully</p>
          </div>

          {/* Content */}
          <div className="px-6 py-8 text-center">
            <h2 className="text-2xl font-bold text-gray-900 mb-4">
              We've Received Your Information
            </h2>
            <p className="text-gray-600 mb-6">
              Our team will get in touch with you shortly to assist with your requirements.
            </p>

            {/* Access Granted Section */}
            <div className="bg-orange-50 border border-orange-200 rounded-lg p-6 mb-8">
              <h3 className="text-lg font-semibold text-[#E67E22] mb-3">
                🎉 Full Access Granted!
              </h3>
              <p className="text-[#c96b05] text-sm mb-4">
                You now have complete access to all project information:
              </p>
              <ul className="text-left space-y-2 text-[#E67E22] text-sm">
                <li className="flex items-center">
                  <span className="mr-2">✅</span>
                  <span>Detailed floor plans for all blocks</span>
                </li>
                <li className="flex items-center">
                  <span className="mr-2">✅</span>
                  <span>Location details and connectivity information</span>
                </li>
                <li className="flex items-center">
                  <span className="mr-2">✅</span>
                  <span>Project brochure download</span>
                </li>
                <li className="flex items-center">
                  <span className="mr-2">✅</span>
                  <span>Exclusive pricing and offers</span>
                </li>
              </ul>
            </div>

            {/* What's Next */}
            <div className="bg-orange-50 border border-orange-200 rounded-lg p-6 mb-8">
              <h3 className="text-lg font-semibold text-[#E67E22] mb-3">
                What Happens Next?
              </h3>
              <div className="space-y-3 text-left text-sm text-[#c96b05]">
                <div className="flex items-start">
                  <span className="font-bold mr-2">1.</span>
                  <span>Our sales team will contact you within 24 hours</span>
                </div>
                <div className="flex items-start">
                  <span className="font-bold mr-2">2.</span>
                  <span>We'll schedule a convenient time for your site visit</span>
                </div>
                <div className="flex items-start">
                  <span className="font-bold mr-2">3.</span>
                  <span>Get personalized guidance and exclusive offers</span>
                </div>
              </div>
            </div>

            {/* Contact Information */}
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-6 mb-8">
              <h3 className="text-lg font-semibold text-gray-800 mb-3">
                Need Immediate Assistance?
              </h3>
              <div className="space-y-2">
                <p className="text-gray-700">
                  <span className="font-medium">Call us:</span>{' '}
                  <a href="tel:+917338628777" className="text-[#E67E22] hover:text-[#c96b05] font-semibold">
                    +91 73386 28777
                  </a>
                </p>
                <p className="text-gray-700">
                  <span className="font-medium">WhatsApp:</span>{' '}
                  <a
                    href="https://wa.me/917338628777?text=Hi, I'm interested in Anahata project"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-[#E67E22] hover:text-[#c96b05] font-semibold"
                  >
                    Chat with us
                  </a>
                </p>
              </div>
            </div>

            {/* Action Buttons */}
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Link
                href="/"
                className="inline-flex items-center justify-center bg-gradient-to-r from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04] text-white px-8 py-3 rounded-lg font-semibold transition-all transform hover:scale-105"
              >
                <Home className="mr-2" size={20} />
                Back to Home
              </Link>
              <Link
                href="/#floor_plan"
                className="inline-flex items-center justify-center bg-white border-2 border-[#E67E22] text-[#E67E22] hover:bg-orange-50 px-8 py-3 rounded-lg font-semibold transition-all"
              >
                View Floor Plans
              </Link>
            </div>
          </div>
        </div>

        {/* Footer Note */}
        <div className="mt-6 text-center text-gray-600 text-sm">
          <p>Thank you for choosing Anahata - Realm of Living</p>
          <p className="mt-1">Your dream home awaits at Soukya Road, Whitefield, Bengaluru</p>
        </div>
      </div>
    </div>
  )
}
