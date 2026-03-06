"use client"

import { ChevronRight } from "lucide-react"
import { useEffect, useState } from "react"

interface BreadcrumbItem {
  name: string
  href: string
  current?: boolean
}

const sectionMap: { [key: string]: string } = {
  "#home": "Home",
  "#about": "About",
  "#floor_plan": "Floor Plans",
  "#gallery": "Gallery",
  "#amenities": "Amenities",
  "#location": "Location",
  "#faq": "FAQ"
}

export default function Breadcrumb() {
  const [breadcrumbs, setBreadcrumbs] = useState<BreadcrumbItem[]>([
    { name: "Home", href: "#home", current: true }
  ])
  const [currentSection, setCurrentSection] = useState("#home")

  useEffect(() => {
    const handleScroll = () => {
      const sections = Object.keys(sectionMap)
      let current = "#home"
      
      for (const section of sections) {
        const element = document.querySelector(section)
        if (element) {
          const rect = element.getBoundingClientRect()
          if (rect.top <= 100 && rect.bottom >= 100) {
            current = section
            break
          }
        }
      }
      
      if (current !== currentSection) {
        setCurrentSection(current)
        updateBreadcrumbs(current)
      }
    }

    const handleHashChange = () => {
      const hash = window.location.hash || "#home"
      setCurrentSection(hash)
      updateBreadcrumbs(hash)
    }

    const updateBreadcrumbs = (section: string) => {
      const items: BreadcrumbItem[] = [
        { name: "Anahata", href: "#home", current: false }
      ]
      
      if (section !== "#home") {
        items.push({
          name: sectionMap[section] || "Page",
          href: section,
          current: true
        })
      } else {
        items[0].current = true
      }
      
      setBreadcrumbs(items)
    }

    window.addEventListener("scroll", handleScroll)
    window.addEventListener("hashchange", handleHashChange)
    
    // Initial check
    handleHashChange()
    
    return () => {
      window.removeEventListener("scroll", handleScroll)
      window.removeEventListener("hashchange", handleHashChange)
    }
  }, [currentSection])

  // Generate breadcrumb schema markup
  const breadcrumbSchema = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    "itemListElement": breadcrumbs.map((item, index) => ({
      "@type": "ListItem",
      "position": index + 1,
      "name": item.name,
      "item": `https://www.anahata-soukya.in${item.href}`
    }))
  }

  return (
    <>
      {currentSection !== "#home" && (
        <nav
          className="fixed top-14 left-0 right-0 z-40 hidden md:block"
          aria-label="Breadcrumb"
        >
          <div className="container mx-auto px-4">
            <ol className="flex items-center space-x-2 py-2 text-sm">
              {breadcrumbs.map((item, index) => (
                <li key={item.href} className="flex items-center">
                  {index > 0 && (
                    <ChevronRight className="h-4 w-4 text-gray-400 mx-2" aria-hidden="true" />
                  )}
                  {item.current ? (
                    <span className="text-gray-900 font-medium" aria-current="page">
                      {item.name}
                    </span>
                  ) : (
                    <a
                      href={item.href}
                      className="text-gray-500 hover:text-blue-600 transition-colors"
                    >
                      {item.name}
                    </a>
                  )}
                </li>
              ))}
            </ol>
          </div>
        </nav>
      )}

      {/* Breadcrumb Schema Markup */}
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumbSchema) }}
      />
    </>
  )
}