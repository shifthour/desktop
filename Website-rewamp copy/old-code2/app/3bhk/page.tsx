"use client"

import { useEffect } from "react"
import dynamic from 'next/dynamic'
import Header from "@/components/header"
import Breadcrumb from "@/components/breadcrumb"
import Hero from "@/components/hero"
import Pricing from "@/components/pricing"
import WhatsAppWidget from "@/components/whatsapp-widget"

// Lazy load below-the-fold components
const About = dynamic(() => import("@/components/about"))
const FloorPlans = dynamic(() => import("@/components/floor-plans"))
const Gallery = dynamic(() => import("@/components/gallery"))
const Amenities = dynamic(() => import("@/components/amenities"))
const Location = dynamic(() => import("@/components/location"))
const FAQ = dynamic(() => import("@/components/faq"))
const Footer = dynamic(() => import("@/components/footer"))
const MobileFooter = dynamic(() => import("@/components/mobile-footer"))

export default function ThreeBHK() {
  // Track page view in Google Tag Manager and GA4 when component mounts
  useEffect(() => {
    // Add a small delay to ensure tracking scripts are loaded
    const timer = setTimeout(() => {
      if (typeof window !== 'undefined') {
        const pagePath = window.location.pathname + window.location.search
        console.log('Tracking 3BHK page view:', pagePath)

        // Method 1: Push to GTM dataLayer
        if (window.dataLayer) {
          window.dataLayer.push({
            event: 'page_view',
            page_path: pagePath,
            page_title: '3BHK Apartments Page',
            page_location: window.location.href
          })
        }

        // Method 2: Direct GA4 tracking via gtag
        if (window.gtag) {
          window.gtag('event', 'page_view', {
            page_title: '3BHK Apartments Page',
            page_path: pagePath,
            page_location: window.location.href
          })
        }
      }
    }, 500)

    return () => clearTimeout(timer)
  }, [])

  return (
    <main className="min-h-screen">
      <Header />
      <Breadcrumb />
      <Hero filterType="3bhk" />
      <Pricing />
      <About filterType="3bhk" />
      <FloorPlans filterType="3bhk" />
      <Gallery filterType="3bhk" />
      <Amenities />
      <Location filterType="3bhk" />
      <FAQ filterType="3bhk" />
      <Footer />
      <MobileFooter />
      <WhatsAppWidget />
    </main>
  )
}
