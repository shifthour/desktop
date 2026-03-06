"use client"

import type React from "react"
import { useState } from "react"
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { X, MapPin, User, Phone, Mail, CheckCircle } from "lucide-react"
import { trackGoogleAdsConversion } from '@/lib/google-ads-tracking'

interface LocationAccessModalProps {
  isOpen: boolean
  onClose: () => void
  onSuccess: () => void
}

// Unified access management (sets access for all protected content)
const setUnifiedAccess = () => {
  if (typeof window !== "undefined") {
    console.log("Setting unified access for location...")
    localStorage.setItem("userVerified", "true")
    localStorage.setItem("floorPlansAccess", "granted")
    localStorage.setItem("locationAccess", "granted")
    localStorage.setItem("brochureAccess", "granted")
    
    // Dispatch custom event to notify all components
    window.dispatchEvent(new CustomEvent("accessGranted"))
    console.log("Access granted event dispatched")
  }
}

// Function to save lead data to Supabase
const saveLeadData = async (
  formData: { name: string; phone: string; email: string },
  source: string,
  status: string,
) => {
  try {
    const response = await fetch("/api/submit-lead", {
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
      }),
    })

    const result = await response.json()

    if (result.success) {
      console.log("Location lead data saved successfully:", result.data)
    } else {
      console.error("Failed to save location lead data:", result.error)
    }

    return result.success
  } catch (error) {
    console.error("Error saving location lead data:", error)
    return false
  }
}

export default function LocationAccessModal({ isOpen, onClose, onSuccess }: LocationAccessModalProps) {
  const [formData, setFormData] = useState({
    name: "",
    phone: "",
    email: "",
  })
  const [isSubmitted, setIsSubmitted] = useState(false)
  const [isLoading, setIsLoading] = useState(false)

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value,
    })
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)

    // Save lead data to Supabase
    const leadSaved = await saveLeadData(formData, "location", "saved")

    if (leadSaved) {
      console.log("Location lead saved successfully")
      
      // TRACK GOOGLE ADS CONVERSION for location access
      trackGoogleAdsConversion('location', formData)
    } else {
      console.warn("Failed to save location lead data, but continuing")
    }

    // Set unified access for all restricted content
    setUnifiedAccess()

    setIsLoading(false)
    setIsSubmitted(true)

    // Call success callback to unlock location map
    setTimeout(() => {
      onSuccess()
      setIsSubmitted(false)
      setFormData({ name: "", phone: "", email: "" })
      onClose()
    }, 3000)
  }

  const handleClose = () => {
    setIsSubmitted(false)
    setFormData({ name: "", phone: "", email: "" })
    onClose()
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="w-[94vw] max-w-xs mx-3 p-0 overflow-hidden bg-white rounded-2xl border-0 shadow-2xl">
        <DialogTitle className="sr-only">View Location Details</DialogTitle>
        {!isSubmitted ? (
          <>
            {/* Header */}
            <div className="relative bg-gradient-to-r from-[#E67E22] to-[#c96b05] px-6 py-8 text-white">
              <button
                onClick={handleClose}
                className="absolute top-4 right-4 text-white/80 hover:text-white transition-colors"
              >
                <X size={24} />
              </button>
              <div className="text-center">
                <MapPin className="mx-auto mb-4" size={48} />
                <h2 className="text-2xl font-bold mb-2">View Location Details</h2>
                <p className="text-orange-100 text-sm">Enter your details to access location & connectivity info</p>
              </div>
            </div>

            {/* Form */}
            <div className="px-4 py-6">
              <form onSubmit={handleSubmit} className="space-y-4">
                {/* Name Field */}
                <div className="relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <User className="h-5 w-5 text-gray-400" />
                  </div>
                  <input
                    type="text"
                    name="name"
                    value={formData.name}
                    onChange={handleInputChange}
                    placeholder="Enter your full name"
                    className="w-full pl-10 pr-4 py-4 text-base border border-gray-300 rounded-lg focus:ring-2 focus:ring-[#E67E22] focus:border-transparent transition-all touch-manipulation"
                    style={{ fontSize: '16px' }}
                    required
                  />
                </div>

                {/* Phone Field */}
                <div className="relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <Phone className="h-5 w-5 text-gray-400" />
                  </div>
                  <input
                    type="tel"
                    name="phone"
                    value={formData.phone}
                    onChange={handleInputChange}
                    placeholder="Enter your phone number"
                    pattern="[0-9]{10}"
                    title="Please enter a valid 10-digit phone number"
                    className="w-full pl-10 pr-4 py-4 text-base border border-gray-300 rounded-lg focus:ring-2 focus:ring-[#E67E22] focus:border-transparent transition-all touch-manipulation"
                    style={{ fontSize: '16px' }}
                    required
                  />
                </div>

                {/* Email Field */}
                <div className="relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <Mail className="h-5 w-5 text-gray-400" />
                  </div>
                  <input
                    type="email"
                    name="email"
                    value={formData.email}
                    onChange={handleInputChange}
                    placeholder="Enter your email address"
                    className="w-full pl-10 pr-4 py-4 text-base border border-gray-300 rounded-lg focus:ring-2 focus:ring-[#E67E22] focus:border-transparent transition-all touch-manipulation"
                    style={{ fontSize: '16px' }}
                    required
                  />
                </div>

                {/* Submit Button with tracking class */}
                <Button
                  id="tracking"
                  type="submit"
                  disabled={isLoading}
                  className="addtrack_ads_conversion w-full bg-gradient-to-r from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04] text-white py-4 min-h-[56px] text-sm leading-tight rounded-lg font-semibold transition-all transform hover:scale-105 disabled:transform-none disabled:opacity-70 touch-manipulation"
                >
                  {isLoading ? (
                    <div className="flex items-center justify-center">
                      <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white mr-2"></div>
                      Processing...
                    </div>
                  ) : (
                    <>
                      <MapPin className="mr-2" size={18} />
                      Submit & Get Access to Brochure & Floor Plan
                    </>
                  )}
                </Button>

                {/* Privacy Notice */}
                <p className="text-xs text-gray-500 text-center">
                  🔒 Your information is secure and will be used only to send you project updates.
                </p>
              </form>
            </div>
          </>
        ) : (
          /* Success State */
          <div className="px-6 py-12 text-center">
            <div className="mb-6">
              <CheckCircle className="h-16 w-16 text-[#E67E22] mx-auto mb-4" />
              <h3 className="text-2xl font-bold text-gray-900 mb-2">Successfully Submitted!</h3>
              <p className="text-gray-600">You can now view the location map and connectivity details.</p>
            </div>
            <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
              <p className="text-orange-800 text-sm font-medium">
                🎉 You now have full access to location details, floor plans, and all project information!
              </p>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}