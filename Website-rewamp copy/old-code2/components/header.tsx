"use client"

import { useState } from "react"
import Image from "next/image"
import { Button } from "@/components/ui/button"
import { Menu, X, Download, Phone } from 'lucide-react'
import Link from "next/link"
import { getImagePath } from "@/lib/utils-path"

export default function Header() {
  const [isMenuOpen, setIsMenuOpen] = useState(false)

  const navItems = [
    { href: "#home", label: "Home" },
    { href: "#about", label: "About" },
    { href: "#floor_plan", label: "Floor Plan" },
    { href: "#gallery", label: "Gallery" },
    { href: "#amenities", label: "Amenities" },
    { href: "#location", label: "Location" },
  ]

  return (
    <header className="fixed top-0 w-full bg-white shadow-md z-50">
      <nav className="container mx-auto px-4 py-2">
        <div className="flex items-center justify-between">
          <Link href="/" className="flex items-center gap-2 md:gap-3 cursor-pointer">
            <Image
              src={getImagePath("/logo.png")}
              alt="Anahata Logo"
              width={50}
              height={50}
              className="h-10 md:h-12 w-auto object-contain"
              priority
            />
            <Image
              src={getImagePath("/word-mark.png")}
              alt="Anahata - Realm of Living"
              width={120}
              height={50}
              className="h-8 md:h-10 w-auto object-contain"
              priority
            />
          </Link>

          {/* Desktop Navigation */}
          <div className="hidden lg:flex items-center space-x-8">
            {navItems.map((item) => (
              <a key={item.href} href={item.href} className="text-[#E67E22] hover:text-[#c96b05] transition-colors font-medium">
                {item.label}
              </a>
            ))}
            <Link href="/Booksitevisit?source=brochure">
              <Button
                id="tracking"
                className="addtrack_ads_conversion bg-[#E67E22] hover:bg-[#c96b05] animate-pulse"
              >
                <Download className="mr-2" size={16} />
                Download Brochure
              </Button>
            </Link>
          </div>

          {/* Phone Number */}
          <div className="hidden lg:block">
            <a
              href="tel:+917338628777"
              className="flex items-center space-x-2 bg-[#E67E22] hover:bg-[#c96b05] text-white px-4 py-2 rounded-lg transition-colors"
            >
              <Phone size={18} />
              <span className="font-semibold">7338628777</span>
            </a>
          </div>

          {/* Mobile Menu Button */}
          <button
            className="lg:hidden p-2 min-w-[44px] min-h-[44px] flex items-center justify-center -mr-2"
            onClick={() => setIsMenuOpen(!isMenuOpen)}
            aria-label={isMenuOpen ? "Close menu" : "Open menu"}
          >
            {isMenuOpen ? <X size={24} /> : <Menu size={24} />}
          </button>
        </div>

        {/* Mobile Navigation */}
        {isMenuOpen && (
          <div className="lg:hidden mt-4 pb-4">
            {navItems.map((item) => (
              <a
                key={item.href}
                href={item.href}
                className="block py-2 text-[#E67E22] hover:text-[#c96b05] font-medium"
                onClick={() => setIsMenuOpen(false)}
              >
                {item.label}
              </a>
            ))}
            <Link href="/Booksitevisit?source=brochure" className="w-full">
              <Button
                id="tracking"
                className="addtrack_ads_conversion w-full mt-4 bg-[#E67E22] hover:bg-[#c96b05] animate-pulse"
              >
                <Download className="mr-2" size={16} />
                Download Brochure
              </Button>
            </Link>
            <a
              href="tel:+917338628777"
              className="flex items-center justify-center space-x-2 w-full mt-2 bg-[#E67E22] hover:bg-[#c96b05] text-white px-4 py-2 rounded-lg transition-colors"
            >
              <Phone size={18} />
              <span className="font-semibold">Call: 7338628777</span>
            </a>
          </div>
        )}
      </nav>
    </header>
  )
}
