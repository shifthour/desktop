'use client'

import { useState } from 'react'
import Image from 'next/image'

export default function Header() {
  const [menuOpen, setMenuOpen] = useState(false)

  return (
    <>
      <header
        className="fixed w-full top-0 z-50 transition-all duration-300"
        style={{ background: 'rgba(255, 255, 255, 0.95)', boxShadow: '0 2px 10px rgba(0,0,0,0.05)' }}
      >
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            {/* Menu Button */}
            <button
              className="order-1 flex items-center gap-3 hover:opacity-70 transition group py-2"
              onClick={() => setMenuOpen(true)}
            >
              <span className="font-light text-base md:text-lg uppercase font-montserrat" style={{ letterSpacing: '0.15em', color: '#333333' }}>
                MENU
              </span>
              <div className="flex flex-col gap-[4px] justify-center items-end">
                <span className="block w-[20px] h-[2px] transition-all" style={{ background: '#333333' }}></span>
                <span className="block w-[30px] h-[2px] transition-all" style={{ background: '#333333' }}></span>
                <span className="block w-[20px] h-[2px] transition-all" style={{ background: '#333333' }}></span>
              </div>
            </button>

            {/* Logo */}
            <div className="order-2 absolute left-1/2 transform -translate-x-1/2 md:static md:transform-none">
              <Image src="/images/ishtika-logo.png" alt="Ishtika Homes" width={120} height={40} className="h-8 md:h-10 w-auto" />
            </div>

            {/* Phone */}
            <a href="tel:+919876543210" className="order-3 flex items-center hover:opacity-70 transition">
              <i className="fas fa-phone mr-2" style={{ color: '#333333', fontSize: '16px' }}></i>
              <span className="font-light font-montserrat" style={{ color: '#333333', letterSpacing: '0.05em' }}>
                +91 98765 43210
              </span>
            </a>

            {/* Spacer for mobile */}
            <div className="order-3 w-16 md:hidden"></div>
          </div>
        </div>
      </header>

      {/* Sidebar Menu */}
      <div
        className={`fixed top-0 left-0 h-full w-80 bg-white shadow-2xl z-[1001] transition-all duration-300 ${
          menuOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="p-6 h-full overflow-y-auto">
          <div className="flex items-center justify-between mb-8">
            <h3 className="text-xl font-bold text-gray-900">Menu</h3>
            <button onClick={() => setMenuOpen(false)} className="text-gray-700 hover:text-gray-900">
              <i className="fas fa-times text-2xl"></i>
            </button>
          </div>

          <nav className="flex flex-col space-y-1">
            <a
              href="#about"
              className="text-lg font-light px-4 py-3 rounded-lg transition hover:opacity-70 font-montserrat"
              style={{ color: '#333333' }}
              onClick={() => setMenuOpen(false)}
            >
              About Us
            </a>
            <a
              href="#projects"
              className="text-lg font-light px-4 py-3 rounded-lg transition hover:opacity-70 font-montserrat"
              style={{ color: '#333333' }}
              onClick={() => setMenuOpen(false)}
            >
              Our Portfolio
            </a>
            <a
              href="#testimonials"
              className="text-lg font-light px-4 py-3 rounded-lg transition hover:opacity-70 font-montserrat"
              style={{ color: '#333333' }}
              onClick={() => setMenuOpen(false)}
            >
              Testimonials
            </a>
            <a
              href="#contact"
              className="text-lg font-light px-4 py-3 rounded-lg transition hover:opacity-70 font-montserrat"
              style={{ color: '#333333' }}
              onClick={() => setMenuOpen(false)}
            >
              Contact Us
            </a>

            <div className="border-t border-gray-200 my-4"></div>

            <a
              href="tel:+919876543210"
              className="text-lg font-light px-4 py-3 rounded-lg transition hover:opacity-70 font-montserrat"
              style={{ color: '#333333' }}
            >
              <i className="fas fa-phone mr-3"></i>+91 98765 43210
            </a>
          </nav>
        </div>
      </div>

      {/* Overlay */}
      {menuOpen && (
        <div
          className="fixed inset-0 bg-black bg-opacity-50 z-[1000]"
          onClick={() => setMenuOpen(false)}
        ></div>
      )}
    </>
  )
}
