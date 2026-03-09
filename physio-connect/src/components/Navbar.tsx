"use client";

import Link from "next/link";
import Image from "next/image";
import { useState } from "react";

export default function Navbar() {
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <nav className="sticky top-0 z-50 bg-white/90 backdrop-blur-md border-b border-primary-100/50 shadow-sm">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-44">
          <Link href="/" className="flex items-center">
            <Image src="/logo.png" alt="On the Move Home Physio" width={874} height={667} className="h-40 w-auto" priority />
          </Link>

          <div className="hidden md:flex items-center gap-6">
            <Link href="/search" className="text-gray-600 hover:text-primary transition-colors font-medium relative group">
              Find a Physio
              <span className="absolute -bottom-1 left-0 w-0 h-0.5 bg-primary transition-all duration-300 group-hover:w-full rounded-full"></span>
            </Link>
            <a href="/#how-it-works" className="text-gray-600 hover:text-primary transition-colors font-medium relative group">
              How It Works
              <span className="absolute -bottom-1 left-0 w-0 h-0.5 bg-primary transition-all duration-300 group-hover:w-full rounded-full"></span>
            </a>
            <a href="/#areas" className="text-gray-600 hover:text-primary transition-colors font-medium relative group">
              Areas
              <span className="absolute -bottom-1 left-0 w-0 h-0.5 bg-primary transition-all duration-300 group-hover:w-full rounded-full"></span>
            </a>
            <a href="/#pricing" className="text-gray-600 hover:text-primary transition-colors font-medium relative group">
              Pricing
              <span className="absolute -bottom-1 left-0 w-0 h-0.5 bg-primary transition-all duration-300 group-hover:w-full rounded-full"></span>
            </a>
            <a href="/#for-physios" className="text-gray-600 hover:text-primary transition-colors font-medium relative group">
              For Physios
              <span className="absolute -bottom-1 left-0 w-0 h-0.5 bg-primary transition-all duration-300 group-hover:w-full rounded-full"></span>
            </a>
            <button className="btn-outline text-sm !py-2 !px-5">Log In</button>
            <button className="btn-coral text-sm !py-2 !px-5">Book Now</button>
          </div>

          <button
            className="md:hidden p-2 rounded-lg hover:bg-primary-light transition-colors"
            onClick={() => setMobileOpen(!mobileOpen)}
            aria-label="Toggle menu"
          >
            <svg className="w-6 h-6 text-navy" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              {mobileOpen ? (
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
              )}
            </svg>
          </button>
        </div>
      </div>

      {mobileOpen && (
        <div className="md:hidden border-t border-primary-100/50 bg-white/95 backdrop-blur-md">
          <div className="px-4 py-4 space-y-1">
            <Link href="/search" className="block py-3 px-3 text-navy font-medium rounded-xl hover:bg-primary-light transition-colors" onClick={() => setMobileOpen(false)}>
              Find a Physio
            </Link>
            <a href="/#how-it-works" className="block py-3 px-3 text-navy font-medium rounded-xl hover:bg-primary-light transition-colors" onClick={() => setMobileOpen(false)}>
              How It Works
            </a>
            <a href="/#areas" className="block py-3 px-3 text-navy font-medium rounded-xl hover:bg-primary-light transition-colors" onClick={() => setMobileOpen(false)}>
              Areas
            </a>
            <a href="/#pricing" className="block py-3 px-3 text-navy font-medium rounded-xl hover:bg-primary-light transition-colors" onClick={() => setMobileOpen(false)}>
              Pricing
            </a>
            <a href="/#for-physios" className="block py-3 px-3 text-navy font-medium rounded-xl hover:bg-primary-light transition-colors" onClick={() => setMobileOpen(false)}>
              For Physios
            </a>
            <hr className="my-3 border-gray-100" />
            <button className="w-full btn-outline text-sm !py-2.5">Log In</button>
            <button className="w-full btn-coral text-sm !py-2.5">Book Now</button>
          </div>
        </div>
      )}
    </nav>
  );
}
