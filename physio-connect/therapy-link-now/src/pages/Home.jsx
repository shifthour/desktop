import React from "react";
import { base44 } from "@/api/base44Client";
import { useQuery } from "@tanstack/react-query";
import HeroSection from "@/components/home/HeroSection";
import SpecializationGrid from "@/components/home/SpecializationGrid";
import HowItWorks from "@/components/home/HowItWorks";
import FeaturedPhysios from "@/components/home/FeaturedPhysios";

export default function Home() {
  const { data: physiotherapists, isLoading } = useQuery({
    queryKey: ["physios-featured"],
    queryFn: () => base44.entities.Physiotherapist.filter({ status: "approved" }, "-rating", 8),
    initialData: [],
  });

  return (
    <div className="min-h-screen bg-white">
      <HeroSection />
      <SpecializationGrid />
      <FeaturedPhysios physiotherapists={physiotherapists} isLoading={isLoading} />
      <HowItWorks />

      {/* CTA Section */}
      <section className="py-24 bg-gradient-to-br from-teal-600 to-emerald-700 relative overflow-hidden">
        <div className="absolute inset-0">
          <div className="absolute top-10 left-10 w-64 h-64 bg-white/5 rounded-full blur-2xl" />
          <div className="absolute bottom-10 right-10 w-96 h-96 bg-white/5 rounded-full blur-3xl" />
        </div>
        <div className="relative z-10 max-w-3xl mx-auto text-center px-4">
          <h2 className="text-3xl sm:text-4xl font-bold text-white tracking-tight">
            Are you a physiotherapist?
          </h2>
          <p className="mt-4 text-lg text-teal-100">
            Join our growing network of healthcare professionals. Manage your schedule, grow your practice, and help more patients.
          </p>
          <a href={`/PhysioRegistration`}>
            <button className="mt-8 bg-white text-teal-700 font-semibold px-8 py-4 rounded-xl hover:bg-teal-50 transition-colors shadow-lg">
              Register Your Practice
            </button>
          </a>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-gray-900 text-gray-400 py-16">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-8">
            <div>
              <h4 className="text-white font-bold text-lg mb-4">PhysioConnect</h4>
              <p className="text-sm leading-relaxed">
                Connecting patients with top physiotherapy professionals for better recovery outcomes.
              </p>
            </div>
            <div>
              <h5 className="text-white font-semibold mb-4">For Patients</h5>
              <ul className="space-y-2 text-sm">
                <li>Find a Physiotherapist</li>
                <li>How It Works</li>
                <li>Reviews</li>
              </ul>
            </div>
            <div>
              <h5 className="text-white font-semibold mb-4">For Professionals</h5>
              <ul className="space-y-2 text-sm">
                <li>Join Our Network</li>
                <li>Practice Dashboard</li>
                <li>Resources</li>
              </ul>
            </div>
            <div>
              <h5 className="text-white font-semibold mb-4">Support</h5>
              <ul className="space-y-2 text-sm">
                <li>Help Center</li>
                <li>Privacy Policy</li>
                <li>Terms of Service</li>
              </ul>
            </div>
          </div>
          <div className="mt-12 pt-8 border-t border-gray-800 text-center text-sm">
            © {new Date().getFullYear()} PhysioConnect. All rights reserved.
          </div>
        </div>
      </footer>
    </div>
  );
}