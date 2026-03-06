"use client"
import { getImagePath } from "@/lib/utils-path"

import { useEffect } from 'react'

export default function PerformanceMonitor() {
  useEffect(() => {
    // Web Vitals monitoring
    const reportWebVitals = () => {
      if ('PerformanceObserver' in window) {
        try {
          // Monitor LCP (Largest Contentful Paint)
          const lcpObserver = new PerformanceObserver((list) => {
            const entries = list.getEntries()
            const lcp = entries[entries.length - 1]
            console.log('LCP:', lcp.startTime)
            
            // Send to analytics if needed
            if (window.gtag) {
              window.gtag('event', 'web_vitals', {
                event_category: 'Web Vitals',
                event_label: 'LCP',
                value: Math.round(lcp.startTime)
              })
            }
          })
          lcpObserver.observe({ entryTypes: ['largest-contentful-paint'] })

          // Monitor FID (First Input Delay)
          const fidObserver = new PerformanceObserver((list) => {
            const entries = list.getEntries()
            entries.forEach((entry: any) => {
              console.log('FID:', entry.processingStart - entry.startTime)
              
              if (window.gtag) {
                window.gtag('event', 'web_vitals', {
                  event_category: 'Web Vitals',
                  event_label: 'FID',
                  value: Math.round(entry.processingStart - entry.startTime)
                })
              }
            })
          })
          fidObserver.observe({ entryTypes: ['first-input'] })

          // Monitor CLS (Cumulative Layout Shift)
          let clsValue = 0
          const clsObserver = new PerformanceObserver((list) => {
            const entries = list.getEntries()
            entries.forEach((entry: any) => {
              if (!entry.hadRecentInput) {
                clsValue += entry.value
                console.log('CLS:', clsValue)
                
                if (window.gtag) {
                  window.gtag('event', 'web_vitals', {
                    event_category: 'Web Vitals',
                    event_label: 'CLS',
                    value: Math.round(clsValue * 1000)
                  })
                }
              }
            })
          })
          clsObserver.observe({ entryTypes: ['layout-shift'] })

        } catch (error) {
          console.warn('Performance monitoring failed:', error)
        }
      }

      // Basic performance metrics
      if ('performance' in window) {
        window.addEventListener('load', () => {
          const perfData = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming
          
          const metrics = {
            dns: perfData.domainLookupEnd - perfData.domainLookupStart,
            tcp: perfData.connectEnd - perfData.connectStart,
            ttfb: perfData.responseStart - perfData.requestStart,
            download: perfData.responseEnd - perfData.responseStart,
            domReady: perfData.domContentLoadedEventEnd - perfData.domContentLoadedEventStart,
            windowLoad: perfData.loadEventEnd - perfData.loadEventStart,
            totalTime: perfData.loadEventEnd - perfData.fetchStart
          }
          
          console.log('Performance Metrics:', metrics)
          
          // Track to analytics
          if (window.gtag) {
            window.gtag('event', 'page_performance', {
              event_category: 'Performance',
              custom_parameter_1: Math.round(metrics.totalTime),
              custom_parameter_2: Math.round(metrics.ttfb),
              custom_parameter_3: Math.round(metrics.domReady)
            })
          }
        })
      }
    }

    // Start monitoring after component mounts
    reportWebVitals()

    // Preload critical resources
    const preloadCriticalResources = () => {
      // Preload hero video poster
      const link1 = document.createElement('link')
      link1.rel = 'preload'
      link1.href = getImagePath('/images/gallery/aerial-complex-view.png')
      link1.as = 'image'
      document.head.appendChild(link1)

      // Preload logo
      const link2 = document.createElement('link')
      link2.rel = 'preload'
      link2.href = 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/logo-vDyanSCJrja7OOADYzwx5iuyIr4DwD.png'
      link2.as = 'image'
      document.head.appendChild(link2)

      // Preconnect to external domains
      const preconnect1 = document.createElement('link')
      preconnect1.rel = 'preconnect'
      preconnect1.href = 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com'
      document.head.appendChild(preconnect1)

      const preconnect2 = document.createElement('link')
      preconnect2.rel = 'dns-prefetch'
      preconnect2.href = 'https://www.googletagmanager.com'
      document.head.appendChild(preconnect2)
    }

    preloadCriticalResources()

    // Clean up observers on unmount
    return () => {
      // Observers will be automatically cleaned up when the page unloads
    }
  }, [])

  return null // This component doesn't render anything
}

declare global {
  interface Window {
    gtag?: (command: string, targetId: string, config?: any) => void
  }
}