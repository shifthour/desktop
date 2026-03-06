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
const ExploreMoreProjects = dynamic(() => import("@/components/explore-more-projects"))
const Footer = dynamic(() => import("@/components/footer"))
const MobileFooter = dynamic(() => import("@/components/mobile-footer"))

export default function Home() {
  return (
    <main className="min-h-screen">
      <Header />
      <Breadcrumb />
      <Hero />
      <Pricing />
      <About />
      <FloorPlans />
      <Gallery />
      <Amenities />
      <Location />
      <FAQ />
      <ExploreMoreProjects />
      <Footer />
      <MobileFooter />
      <WhatsAppWidget />
    </main>
  )
}
