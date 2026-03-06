"use client"

import { useState, useEffect, Suspense } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import { User, Phone, Mail, MessageSquare, ArrowLeft, Download, MapPin, FileText } from 'lucide-react'
import { Button } from "@/components/ui/button"
import { trackGoogleAdsConversion, trackBrochureDownload } from '@/lib/google-ads-tracking'
import Link from "next/link"

// Unified access management
const setUnifiedAccess = () => {
  if (typeof window !== "undefined") {
    console.log("Setting unified access...") // Debug log
    localStorage.setItem("userVerified", "true")
    localStorage.setItem("floorPlansAccess", "granted")
    localStorage.setItem("locationAccess", "granted")
    localStorage.setItem("brochureAccess", "granted")

    // Dispatch custom event to notify all components
    window.dispatchEvent(new CustomEvent("accessGranted"))
    console.log("Access granted event dispatched") // Debug log
  }
}

// Function to save lead data to Supabase (INSERT)
const saveLeadData = async (
  formData: { name: string; phone: string; email: string },
  source: string,
  status: string,
  apartmentType?: string,
) => {
  try {
    // Use window.location.origin for production compatibility
    const apiUrl = typeof window !== 'undefined'
      ? `${window.location.origin}/api/submit-lead`
      : '/api/submit-lead';
    const response = await fetch(apiUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: formData.name,
        phone: formData.phone,
        email: formData.email,
        source: source,
        status_of_save: status,
        action: "insert",
        preferred_type: apartmentType || null,
      }),
    })

    const result = await response.json()

    if (result.success) {
      console.log("Lead data saved successfully:", result.data)
    } else {
      console.error("Failed to save lead data:", result.error)
    }

    return result.success
  } catch (error) {
    console.error("Error saving lead data:", error)
    return false
  }
}

// Download brochure from public folder
const downloadBrochure = async () => {
  try {
    console.log("Downloading brochure...")

    const brochureUrl = "/Anahata-Brochure.pdf"

    // Create a temporary link and click it to trigger download
    const link = document.createElement("a")
    link.href = brochureUrl
    link.download = "Anahata-Brochure.pdf"
    link.target = "_blank"
    link.rel = "noopener noreferrer"
    link.style.display = "none"

    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)

    console.log("Brochure download initiated successfully")
    return true
  } catch (error) {
    console.error("Download error:", error)

    // Fallback: open in new tab
    try {
      window.open("/Anahata-Brochure.pdf", "_blank")
      return true
    } catch (fallbackError) {
      console.error("Fallback also failed:", fallbackError)
      alert("Download failed. Please try again or contact support.")
      return false
    }
  }
}

// Source configuration
const sourceConfig: Record<string, { title: string; subtitle: string; icon: React.ReactNode; color: string }> = {
  site_visit: {
    title: "Schedule Your Site Visit",
    subtitle: "Fill in your details and we'll get back to you shortly",
    icon: <User size={48} />,
    color: "blue"
  },
  brochure: {
    title: "Download Brochure",
    subtitle: "Get detailed information about Anahata project",
    icon: <Download size={48} />,
    color: "green"
  },
  floor_plans: {
    title: "Access Floor Plans",
    subtitle: "View detailed floor plans for all blocks",
    icon: <FileText size={48} />,
    color: "purple"
  },
  location: {
    title: "Access Location Details",
    subtitle: "View detailed location map and connectivity",
    icon: <MapPin size={48} />,
    color: "orange"
  },
  pricing_card_2_bhk: {
    title: "Get Best Price for 2 BHK",
    subtitle: "Interested in 2 BHK apartments? Let's connect!",
    icon: <User size={48} />,
    color: "blue"
  },
  pricing_card_3_bhk_2t: {
    title: "Get Best Price for 3 BHK 2T",
    subtitle: "Interested in 3 BHK 2T apartments? Let's connect!",
    icon: <User size={48} />,
    color: "blue"
  },
  pricing_card_3_bhk_3t: {
    title: "Get Best Price for 3 BHK 3T",
    subtitle: "Interested in 3 BHK 3T apartments? Let's connect!",
    icon: <User size={48} />,
    color: "blue"
  },
  gallery: {
    title: "Schedule Your Site Visit",
    subtitle: "Experience Anahata in person with our complimentary site visit",
    icon: <User size={48} />,
    color: "blue"
  }
}

function BookSiteVisitForm() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const source = searchParams.get('source') || 'site_visit'
  const apartmentType = searchParams.get('apartmentType') || ''

  const [formData, setFormData] = useState({ name: "", phone: "", email: "" })
  const [loading, setLoading] = useState(false)

  const config = sourceConfig[source] || sourceConfig.site_visit

  // Track page view in Google Tag Manager and GA4 when component mounts
  useEffect(() => {
    // Add a small delay to ensure tracking scripts are loaded
    const timer = setTimeout(() => {
      if (typeof window !== 'undefined') {
        const pagePath = window.location.pathname + window.location.search
        console.log('Tracking Booksitevisit page view:', pagePath)

        // Method 1: Push virtual pageview to GTM dataLayer
        if (window.dataLayer) {
          window.dataLayer.push({
            event: 'virtualPageview',
            page: {
              path: pagePath,
              title: 'Book Site Visit Form',
              url: window.location.href
            }
          })
          console.log('GTM virtualPageview pushed')
        }

        // Method 2: Direct GA4 config tracking (most reliable for SPAs)
        if (window.gtag) {
          window.gtag('config', 'G-W6C28DJ6M3', {
            page_title: 'Book Site Visit Form',
            page_path: pagePath,
            page_location: window.location.href
          })
          console.log('GA4 config page view sent')
        }

        // Method 3: Send explicit page_view event to GA4
        if (window.gtag) {
          window.gtag('event', 'page_view', {
            page_title: 'Book Site Visit Form',
            page_path: pagePath,
            page_location: window.location.href,
            send_to: 'G-W6C28DJ6M3'
          })
          console.log('GA4 page_view event sent')
        }
      }
    }, 1000)

    return () => clearTimeout(timer)
  }, [])

  // Check if user already has access and handle brochure download
  useEffect(() => {
    const checkAccess = async () => {
      if (source === 'brochure' && typeof window !== 'undefined') {
        const userVerified = localStorage.getItem("userVerified")

        if (userVerified === "true") {
          // User already has access, download directly and redirect
          console.log("User already verified, downloading brochure directly")
          await downloadBrochure()
          trackBrochureDownload()
          router.push('/thankyou')
        }
      }
    }

    checkAccess()
  }, [source, router])

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({ ...formData, [e.target.name]: e.target.value })
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)

    // Save lead data with apartment preference if applicable
    const leadSaved = await saveLeadData(formData, source, "saved", apartmentType)

    if (leadSaved) {
      console.log("Lead data saved successfully with preference:", apartmentType)

      // Track Google Ads conversion
      trackGoogleAdsConversion(source, formData)

      // If brochure download, trigger it and track
      if (source === 'brochure') {
        trackBrochureDownload()
        await downloadBrochure()
      }
    } else {
      console.warn("Failed to save lead data, but continuing")
    }

    // Set unified access for all restricted content
    setUnifiedAccess()

    setLoading(false)

    // Redirect to thank you page
    router.push('/thankyou')
  }

  const colorClasses = {
    blue: {
      gradient: "from-[#E67E22] to-[#c96b05]",
      button: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
      ring: "ring-[#E67E22]"
    },
    green: {
      gradient: "from-[#E67E22] to-[#c96b05]",
      button: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
      ring: "ring-[#E67E22]"
    },
    purple: {
      gradient: "from-[#E67E22] to-[#c96b05]",
      button: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
      ring: "ring-[#E67E22]"
    },
    orange: {
      gradient: "from-[#E67E22] to-[#c96b05]",
      button: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
      ring: "ring-[#E67E22]"
    }
  }

  const colors = colorClasses[config.color as keyof typeof colorClasses] || colorClasses.blue

  return (
    <div className="min-h-screen bg-gray-50 py-8 px-4">
      <div className="max-w-2xl mx-auto">
        {/* Back to Home Link */}
        <Link href="/" className="inline-flex items-center text-[#E67E22] hover:text-[#c96b05] mb-6 transition-colors">
          <ArrowLeft size={20} className="mr-2" />
          Back to Home
        </Link>

        {/* Form Card */}
        <div className="bg-white rounded-2xl shadow-2xl overflow-hidden">
          {/* Header */}
          <div className={`bg-gradient-to-r ${colors.gradient} px-6 py-8 text-white text-center`}>
            <div className="mx-auto mb-4">{config.icon}</div>
            <h1 className="text-3xl font-bold mb-2">{config.title}</h1>
            <p className="text-white/90">{config.subtitle}</p>
          </div>

          {/* Form */}
          <div className="px-6 py-8">
            {apartmentType && (
              <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
                <p className="text-blue-800 text-sm font-medium">
                  🏠 You're interested in: <span className="font-bold">{apartmentType}</span> apartments
                </p>
              </div>
            )}

            <form onSubmit={handleSubmit} className="space-y-6">
              {/* Name Input */}
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <User className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  type="text"
                  name="name"
                  placeholder="Enter your full name"
                  value={formData.name}
                  onChange={handleInputChange}
                  required
                  className={`w-full pl-10 pr-4 py-4 text-base border border-gray-300 rounded-lg focus:ring-2 focus:${colors.ring} focus:border-transparent transition-all`}
                  style={{ fontSize: '16px' }}
                />
              </div>

              {/* Phone Input */}
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <Phone className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  type="tel"
                  name="phone"
                  placeholder="Enter your phone number"
                  value={formData.phone}
                  onChange={handleInputChange}
                  required
                  className={`w-full pl-10 pr-4 py-4 text-base border border-gray-300 rounded-lg focus:ring-2 focus:${colors.ring} focus:border-transparent transition-all`}
                  style={{ fontSize: '16px' }}
                />
              </div>

              {/* Email Input */}
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <Mail className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  type="email"
                  name="email"
                  placeholder="Enter your email address"
                  value={formData.email}
                  onChange={handleInputChange}
                  required
                  className={`w-full pl-10 pr-4 py-4 text-base border border-gray-300 rounded-lg focus:ring-2 focus:${colors.ring} focus:border-transparent transition-all`}
                  style={{ fontSize: '16px' }}
                />
              </div>

              {/* Submit Button */}
              <Button
                type="submit"
                disabled={loading}
                className={`w-full bg-gradient-to-r ${colors.button} text-white py-4 rounded-lg font-semibold transition-all transform hover:scale-105 disabled:opacity-70 text-base`}
              >
                {loading ? (
                  <div className="flex items-center justify-center">
                    <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white mr-2"></div>
                    Processing...
                  </div>
                ) : (
                  <>
                    <MessageSquare className="mr-2 inline" size={18} />
                    Submit & Get Access
                  </>
                )}
              </Button>

              <p className="text-xs text-gray-500 text-center">
                🔒 Your information is secure and will be used only to send you project updates.
              </p>
            </form>
          </div>
        </div>

        {/* Additional Info */}
        <div className="mt-8 text-center text-gray-600 text-sm">
          <p>By submitting this form, you'll get instant access to:</p>
          <ul className="mt-4 space-y-2">
            <li>✅ Detailed floor plans for all blocks</li>
            <li>✅ Location details and connectivity information</li>
            <li>✅ Project brochure download</li>
            <li>✅ Exclusive pricing and offers</li>
          </ul>
        </div>
      </div>
    </div>
  )
}

export default function BookSiteVisit() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-gray-50 py-8 px-4 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading...</p>
        </div>
      </div>
    }>
      <BookSiteVisitForm />
    </Suspense>
  )
}
