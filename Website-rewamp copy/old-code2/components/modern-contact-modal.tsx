"use client"

import type React from "react"
import { useState } from "react"
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { X, Phone, Mail, User, CheckCircle, MessageSquare } from 'lucide-react'
import { trackGoogleAdsConversion } from '@/lib/google-ads-tracking'

interface ModernContactModalProps {
  isOpen: boolean
  onClose: () => void
  title?: string
  subtitle?: string
  onSuccess?: () => void
  source?: string // Track where the lead came from
  apartmentType?: string // Track apartment preference
}

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


export default function ModernContactModal({
  isOpen,
  onClose,
  title = "Get In Touch With Us",
  subtitle = "Fill in your details and we'll get back to you shortly",
  onSuccess,
  source = "contact_form",
  apartmentType,
}: ModernContactModalProps) {
  const [formData, setFormData] = useState({ name: "", phone: "", email: "" })
  const [step, setStep] = useState<"form" | "success">("form")
  const [loading, setLoading] = useState(false)

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({ ...formData, [e.target.name]: e.target.value })
  }


  const handleSubmit = async () => {
    setLoading(true)

    // Save lead data with apartment preference
    const leadSaved = await saveLeadData(formData, source, "saved", apartmentType)

    if (leadSaved) {
      console.log("Contact lead data saved successfully with preference:", apartmentType)
      
      // TRACK GOOGLE ADS CONVERSION - This is the key!
      trackGoogleAdsConversion(source, formData)
    } else {
      console.warn("Failed to save lead data, but continuing")
    }

    // Set unified access for all restricted content
    setUnifiedAccess()

    // Show success
    setStep("success")
    
    if (onSuccess) {
      onSuccess()
    }

    setTimeout(() => handleClose(), 3000)
    setLoading(false)
  }


  const handleClose = () => {
    setStep("form")
    setFormData({ name: "", phone: "", email: "" })
    onClose()
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="w-[90vw] sm:w-[400px] max-w-[360px] sm:max-w-md mx-auto p-0 overflow-hidden bg-white rounded-2xl border-0 shadow-2xl">
        {/* Hidden DialogTitle for accessibility */}
        <DialogTitle className="sr-only">{title}</DialogTitle>
        {/* STEP 1: FORM */}
        {step === "form" && (
          <>
            <Header title={title} subtitle={subtitle} icon={<User size={48} />} color="blue" onClose={handleClose} />
            <div className="px-3 py-4">
              {apartmentType && (
                <div className="mb-6 p-3 bg-orange-50 border border-orange-200 rounded-lg">
                  <p className="text-orange-800 text-sm font-medium">
                    🏠 You're interested in: <span className="font-bold">{apartmentType}</span> apartments
                  </p>
                </div>
              )}
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  handleSubmit()
                }}
                className="space-y-4"
              >
                <Input
                  icon={<User className="h-5 w-5 text-gray-400" />}
                  name="name"
                  placeholder="Enter your full name"
                  value={formData.name}
                  onChange={handleInputChange}
                  required
                />

                <Input
                  icon={<Phone className="h-5 w-5 text-gray-400" />}
                  name="phone"
                  placeholder="Enter your phone number"
                  value={formData.phone}
                  onChange={handleInputChange}
                  required
                />

                <Input
                  icon={<Mail className="h-5 w-5 text-gray-400" />}
                  name="email"
                  type="email"
                  placeholder="Enter your email address"
                  value={formData.email}
                  onChange={handleInputChange}
                  required
                />

                <ActionButton
                  loading={loading}
                  text="Submit & Get Access to Brochure & Floor Plan"
                  loadingText="Processing..."
                  color="blue"
                  icon={<MessageSquare className="mr-2" size={18} />}
                />
              </form>
            </div>
          </>
        )}


        {/* STEP 3: SUCCESS */}
        {step === "success" && (
          <div className="px-6 py-12 text-center">
            <div className="mb-6">
              <CheckCircle className="h-16 w-16 text-[#E67E22] mx-auto mb-4" />
              <h3 className="text-2xl font-bold text-gray-900 mb-2">Successfully Submitted!</h3>
              <p className="text-gray-600">Thank you for your interest. We'll contact you shortly.</p>
            </div>
            <div className="bg-green-50 border border-green-200 rounded-lg p-4">
              <p className="text-green-800 text-sm font-medium">
                🎉 You now have full access to floor plans, location details, and all project information!
              </p>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}

/* Helper Components */
import type { ReactNode } from "react"

function Header({
  title,
  subtitle,
  icon,
  color,
  onClose,
}: {
  title: string
  subtitle?: string
  icon: ReactNode
  color: "blue" | "green" | "purple"
  onClose: () => void
}) {
  const colors = {
    blue: "from-[#E67E22] to-[#c96b05]",
    green: "from-[#E67E22] to-[#c96b05]",
    purple: "from-[#E67E22] to-[#c96b05]",
  }[color]
  return (
    <div className={`relative bg-gradient-to-r ${colors} px-3 py-4 text-white`}>
      <button onClick={onClose} className="absolute top-4 right-4 text-white/80 hover:text-white transition-colors">
        <X size={24} />
      </button>
      <div className="text-center">
        <div className="mx-auto mb-4">{icon}</div>
        <h2 className="text-2xl font-bold mb-2">{title}</h2>
        {subtitle && <p className="text-orange-100 text-sm">{subtitle}</p>}
      </div>
    </div>
  )
}

function Input({
  icon,
  ...props
}: React.InputHTMLAttributes<HTMLInputElement> & {
  icon: ReactNode
}) {
  return (
    <div className="relative">
      <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">{icon}</div>
      <input
        {...props}
        className="w-full pl-10 pr-4 py-4 text-base border border-gray-300 rounded-lg focus:ring-2 focus:ring-[#E67E22] focus:border-transparent transition-all touch-manipulation"
        style={{ fontSize: '16px' }} // Prevents zoom on iOS
      />
    </div>
  )
}

function ActionButton({
  loading,
  text,
  loadingText,
  color,
  icon,
}: {
  loading: boolean
  text: string
  loadingText: string
  color: "blue" | "green" | "purple"
  icon: ReactNode
}) {
  const colors = {
    blue: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
    green: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
    purple: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
  }[color]
  return (
    <Button
      id="tracking"
      type="submit"
      disabled={loading}
      className={`addtrack_ads_conversion w-full bg-gradient-to-r ${colors} text-white py-3 px-2 min-h-[48px] text-xs sm:text-sm leading-tight rounded-lg font-semibold transition-all transform hover:scale-105 disabled:opacity-70 touch-manipulation whitespace-normal`}
    >
      {loading ? (
        <div className="flex items-center justify-center">
          <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white mr-2"></div>
          {loadingText}
        </div>
      ) : (
        <>
          {icon}
          {text}
        </>
      )}
    </Button>
  )
}
