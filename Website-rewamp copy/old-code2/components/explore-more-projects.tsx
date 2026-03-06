"use client"

import { Button } from "@/components/ui/button"
import { ArrowRight, Building2 } from "lucide-react"

export default function ExploreMoreProjects() {
  return (
    <section className="py-16 bg-gradient-to-r from-[#E67E22] to-[#c96b05]">
      <div className="container mx-auto px-4">
        <div className="max-w-4xl mx-auto text-center">
          <div className="mb-6">
            <Building2 className="h-16 w-16 text-white mx-auto mb-4" />
            <h2 className="text-3xl md:text-4xl font-bold text-white mb-4">
              Discover More Premium Projects
            </h2>
            <p className="text-xl text-orange-100 mb-8">
              Explore our diverse portfolio of luxury residential developments across Karnataka
            </p>
          </div>

          <a href="/" className="inline-block">
            <Button
              size="lg"
              className="bg-white text-[#E67E22] hover:bg-orange-50 font-semibold text-lg px-8 py-6 rounded-lg shadow-xl hover:shadow-2xl transform hover:scale-105 transition-all duration-300"
            >
              Click Here to Explore More Projects
              <ArrowRight className="ml-2 h-5 w-5" />
            </Button>
          </a>

          <p className="mt-6 text-orange-100 text-sm">
            Visit Ishtika Homes to view all our ongoing and upcoming projects
          </p>
        </div>
      </div>
    </section>
  )
}
