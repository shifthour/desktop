import React from 'react';
import { motion } from 'framer-motion';
import { Search, FileText, Calendar, Home, Key, CheckCircle } from 'lucide-react';

const steps = [
  {
    icon: Search,
    title: "Explore Projects",
    description: "Browse our portfolio of premium residential projects across Bangalore and select the one that fits your needs.",
  },
  {
    icon: FileText,
    title: "Schedule Site Visit",
    description: "Book a personalized site visit to experience the location, amenities, and quality of construction firsthand.",
  },
  {
    icon: Calendar,
    title: "Book Your Home",
    description: "Complete the booking process with transparent pricing, flexible payment plans, and comprehensive documentation.",
  },
  {
    icon: Home,
    title: "Construction Updates",
    description: "Receive regular updates on construction progress with complete transparency throughout the building phase.",
  },
  {
    icon: Key,
    title: "Handover",
    description: "Get your dream home with timely possession, complete legal documentation, and quality assurance.",
  },
  {
    icon: CheckCircle,
    title: "After-Sales Support",
    description: "Enjoy dedicated customer support even after possession for any maintenance or service requirements.",
  },
];

export default function ProcessSection() {
  return (
    <section className="py-8 md:py-12 px-4" style={{ backgroundColor: '#F9F9F9' }}>
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="text-center mb-8 md:mb-16"
        >
          <p className="text-xs md:text-sm tracking-[0.3em] uppercase mb-2 md:mb-4 font-semibold" style={{ color: '#FF8C00' }}>
            Our Process
          </p>
          <h2 className="text-2xl md:text-4xl font-light text-gray-800 mb-2 md:mb-4" style={{ fontFamily: 'Inter, Montserrat, sans-serif' }}>
            Your Journey to Your Dream Home
          </h2>
          <p className="text-sm md:text-base text-gray-600 max-w-2xl mx-auto" style={{ fontFamily: 'Inter, Montserrat, sans-serif' }}>
            A simple, transparent process from discovery to possession
          </p>
        </motion.div>

        {/* Mobile: Vertical list with line */}
        <div className="md:hidden relative pl-8">
          <div className="absolute left-4 top-0 bottom-0 w-0.5" style={{ backgroundColor: '#FF8C00' }} />
          
          {steps.map((step, index) => (
            <motion.div
              key={step.title}
              initial={{ opacity: 0, x: -20 }}
              whileInView={{ opacity: 1, x: 0 }}
              transition={{ duration: 0.5, delay: index * 0.1 }}
              viewport={{ once: true }}
              className="relative mb-4 last:mb-0"
            >
              <div className="bg-white border border-gray-200 rounded-[20px] p-4 shadow-sm relative">
                {/* Circular badge */}
                <div 
                  className="absolute -left-10 top-4 w-8 h-8 rounded-full flex items-center justify-center text-white text-sm font-bold z-10"
                  style={{ backgroundColor: '#FF8C00' }}
                >
                  {index + 1}
                </div>

                {/* Icon */}
                <div className="flex items-center mb-2">
                  <step.icon className="w-6 h-6" style={{ color: '#FF8C00' }} />
                </div>

                {/* Text left-aligned on mobile */}
                <h3 className="text-base font-semibold text-gray-800 mb-1 text-left" style={{ fontFamily: 'Inter, Montserrat, sans-serif' }}>
                  {step.title}
                </h3>

                <p className="text-gray-600 text-xs leading-relaxed text-left" style={{ fontFamily: 'Inter, Montserrat, sans-serif' }}>
                  {step.description}
                </p>
              </div>
            </motion.div>
          ))}
        </div>

        {/* Desktop: 3x2 Grid */}
        <div className="hidden md:grid md:grid-cols-3 gap-6">
          {steps.map((step, index) => (
            <motion.div
              key={step.title}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: index * 0.1 }}
              viewport={{ once: true }}
              className="bg-white border border-gray-200 rounded-[20px] p-8 shadow-sm hover:shadow-md transition-shadow relative"
            >
              {/* Circular badge at top left */}
              <div 
                className="absolute top-4 left-4 w-10 h-10 rounded-full flex items-center justify-center text-white font-bold"
                style={{ backgroundColor: '#FF8C00' }}
              >
                {index + 1}
              </div>

              {/* Icon centered */}
              <div className="flex justify-center mb-6 mt-8">
                <step.icon className="w-12 h-12" style={{ color: '#FF8C00' }} />
              </div>

              {/* Text center-aligned on desktop */}
              <h3 className="text-xl font-semibold text-gray-800 mb-3 text-center" style={{ fontFamily: 'Inter, Montserrat, sans-serif' }}>
                {step.title}
              </h3>

              <p className="text-gray-600 text-sm leading-relaxed text-center" style={{ fontFamily: 'Inter, Montserrat, sans-serif' }}>
                {step.description}
              </p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}