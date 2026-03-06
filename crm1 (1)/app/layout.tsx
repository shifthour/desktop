import type React from "react"
import type { Metadata } from "next"
import { DM_Sans } from "next/font/google"
import "./globals.css"
import { Toaster } from "@/components/ui/toaster"
import { Toaster as Sonner } from "@/components/ui/sonner"

const dmSans = DM_Sans({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-dm-sans",
})

export const metadata: Metadata = {
  title: "LabGig CRM - Laboratory Equipment Distribution",
  description: "Modern CRM system for laboratory equipment distribution and services",
  generator: "v0.dev",
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={`${dmSans.variable} ${dmSans.className}`}>
        {children}
        <Toaster />
        <Sonner />
        <script 
          dangerouslySetInnerHTML={{
            __html: `
              // Hide development tools and floating widgets
              function hideBottomLeftElements() {
                if (typeof window !== 'undefined') {
                  // Look for elements in bottom-left area
                  const elements = document.querySelectorAll('*');
                  elements.forEach(el => {
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    
                    // Check if element is in bottom-left corner
                    if (style.position === 'fixed' && 
                        (style.bottom === '0px' || parseInt(style.bottom) < 100) &&
                        (style.left === '0px' || parseInt(style.left) < 100) &&
                        rect.width < 200 && rect.height < 200) {
                      
                      // Check if it contains debugging-related content
                      const text = el.textContent || el.title || el.className || '';
                      if (text.includes('preference') || text.includes('route') || 
                          text.includes('debug') || text.includes('developer') ||
                          el.querySelector('[title*="preference"]') ||
                          el.querySelector('[title*="route"]')) {
                        el.style.display = 'none';
                        el.style.visibility = 'hidden';
                        el.style.opacity = '0';
                        el.style.pointerEvents = 'none';
                      }
                    }
                  });
                }
              }
              
              // Run immediately and on DOM changes
              if (document.readyState === 'loading') {
                document.addEventListener('DOMContentLoaded', hideBottomLeftElements);
              } else {
                hideBottomLeftElements();
              }
              
              // Also run periodically to catch dynamically added elements
              setInterval(hideBottomLeftElements, 1000);
            `
          }}
        />
      </body>
    </html>
  )
}
