"use client"

import type React from "react"
import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { X, Download, User, Phone, Mail, CheckCircle, MessageSquare } from 'lucide-react'
import { trackGoogleAdsConversion, trackBrochureDownload } from '@/lib/google-ads-tracking'

interface BrochureDownloadModalProps {
  isOpen: boolean
  onClose: () => void
}

// Unified access management
const setUnifiedAccess = () => {
  if (typeof window !== "undefined") {
    localStorage.setItem("userVerified", "true")
    localStorage.setItem("floorPlansAccess", "granted")
    localStorage.setItem("locationAccess", "granted")
    localStorage.setItem("brochureAccess", "granted")

    // Dispatch custom event to notify all components
    window.dispatchEvent(new CustomEvent("accessGranted"))
  }
}

// Function to save lead data to Supabase (INSERT)
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


// Check if user is already verified
const checkExistingAccess = () => {
  if (typeof window !== "undefined") {
    const userVerified = localStorage.getItem("userVerified")
    return userVerified === "true"
  }
  return false
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

export default function BrochureDownloadModal({ isOpen, onClose }: BrochureDownloadModalProps) {
  const [formData, setFormData] = useState({ name: "", phone: "", email: "" })
  const [step, setStep] = useState<"form" | "success">("form")
  const [loading, setLoading] = useState(false)

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) =>
    setFormData({ ...formData, [e.target.name]: e.target.value })


  const handleSubmit = async () => {
    setLoading(true)

    // Save or update lead data
    const leadSaved = await saveLeadData(formData, "brochure_download", "saved")

    if (leadSaved) {
      console.log("Brochure lead data saved successfully")
      
      // TRACK GOOGLE ADS CONVERSION for brochure download
      trackGoogleAdsConversion('brochure_download', formData)
      trackBrochureDownload()
    } else {
      console.warn("Failed to save lead data, but continuing with download")
    }

    // Set unified access for all restricted content
    setUnifiedAccess()

    // Trigger download from Supabase storage
    const downloadSuccess = await downloadBrochure()

    if (downloadSuccess) {
      setStep("success")
      setTimeout(() => handleClose(), 3000)
    }

    setLoading(false)
  }


  const handleClose = () => {
    setStep("form")
    setFormData({ name: "", phone: "", email: "" })
    onClose()
  }

  const handleInstantDownload = async () => {
    console.log("User already verified, downloading brochure directly")
    const downloadSuccess = await downloadBrochure()

    if (downloadSuccess) {
      setStep("success")
      setTimeout(handleClose, 2000)
    }
  }

  // Check access when modal opens
  useEffect(() => {
    if (isOpen) {
      const existingAccess = checkExistingAccess()

      if (existingAccess) {
        // User already verified from any form, directly download brochure
        handleInstantDownload()
      } else {
        // New user, show the form
        console.log("New user, showing form")
        setStep("form")
      }
    }
  }, [isOpen])

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="w-[94vw] max-w-xs mx-3 p-0 overflow-hidden bg-white rounded-2xl border-0 shadow-2xl">
        <DialogTitle className="sr-only">Download Brochure</DialogTitle>
        {/* STEP 1: FORM */}
        {step === "form" && (
          <>
            <Header title="Download Brochure" icon={<Download size={48} />} color="green" onClose={handleClose} />
            <div className="px-6 py-8">
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  handleSubmit()
                }}
                className="space-y-6"
              >
                <Input
                  icon={<User className="h-5 w-5 text-gray-400" />}
                  name="name"
                  placeholder="Enter your full name"
                  value={formData.name}
                  onChange={handleChange}
                  required
                />

                <Input
                  icon={<Phone className="h-5 w-5 text-gray-400" />}
                  name="phone"
                  placeholder="Enter your phone number"
                  value={formData.phone}
                  onChange={handleChange}
                  required
                />

                <Input
                  icon={<Mail className="h-5 w-5 text-gray-400" />}
                  name="email"
                  type="email"
                  placeholder="Enter your email address"
                  value={formData.email}
                  onChange={handleChange}
                  required
                />

                <ActionButton
                  loading={loading}
                  text="Download Brochure"
                  loadingText="Processing..."
                  color="green"
                  icon={<Download className="mr-2" size={18} />}
                />

                <p className="text-xs text-gray-500 text-center">
                  🔒 Your information is secure and will be used only to send you project updates.
                </p>
              </form>
            </div>
          </>
        )}


        {/* STEP 3: SUCCESS */}
        {step === "success" && (
          <div className="px-6 py-12 text-center">
            <CheckCircle className="h-16 w-16 text-[#E67E22] mx-auto mb-4" />
            <h3 className="text-2xl font-bold text-gray-900 mb-2">Download Started!</h3>
            <p className="text-gray-600 mb-4">Your brochure download has begun automatically.</p>
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
  color: "green" | "blue" | "orange"
  onClose: () => void
}) {
  const colors = {
    green: "from-[#E67E22] to-[#c96b05]",
    blue: "from-[#E67E22] to-[#c96b05]",
    orange: "from-[#E67E22] to-[#c96b05]",
  }[color]
  return (
    <div className={`relative bg-gradient-to-r ${colors} px-6 py-8 text-white`}>
      <button onClick={onClose} className="absolute top-4 right-4 text-white/80 hover:text-white">
        <X size={24} />
      </button>
      <div className="text-center">
        <div className="mx-auto mb-4">{icon}</div>
        <h2 className="text-2xl font-bold mb-2">{title}</h2>
        {subtitle && <p className={`${color}-100 text-sm`}>{subtitle}</p>}
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
        className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent transition-all"
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
  color: "green" | "blue" | "orange"
  icon: ReactNode
}) {
  const colors = {
    green: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
    blue: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
    orange: "from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04]",
  }[color]
  return (
    <Button
      id="tracking"
      type="submit"
      disabled={loading}
      className={`addtrack_ads_conversion w-full bg-gradient-to-r ${colors} text-white py-3 rounded-lg font-semibold transition-all transform hover:scale-105 disabled:opacity-70`}
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
