import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { Compass, Maximize, Clock, Award, Calendar, MapPin, ChevronLeft, ChevronRight, ArrowRight } from 'lucide-react';

const features = [
  {
    icon: Compass,
    title: "100% Vastu Compliant",
    description: "Every home designed with authentic Vastu principles for harmony, prosperity, and positive energy flow.",
  },
  {
    icon: Maximize,
    title: "Spacious Living",
    description: "Unlike others, we don't build compact homes. Enjoy generous room sizes and open layouts for comfortable living.",
  },
  {
    icon: Award,
    title: "12+ Years Experience",
    description: "Over a decade of excellence in real estate development, delivering quality homes across Bangalore.",
  },
  {
    icon: Clock,
    title: "Quality Construction",
    description: "Premium materials and superior craftsmanship ensure lasting value and durability in every project.",
  },
  {
    icon: Calendar,
    title: "Timely Delivery",
    description: "We honor our commitments with on-time project completion and transparent progress updates.",
  },
  {
    icon: MapPin,
    title: "Prime Locations",
    description: "Strategic locations with excellent connectivity, schools, hospitals, and shopping centers nearby.",
  },
];

export default function WhyUsSection() {
  const [currentIndex, setCurrentIndex] = useState(0);
  
  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentIndex((prev) => (prev + 1) % features.length);
    }, 3000);
    return () => clearInterval(interval);
  }, []);

  return (
    <section className="bg-white py-8 md:py-12 px-4">
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="text-center mb-8 md:mb-16"
        >
          <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">
            Why Ishtika Homes
          </p>
          <h2 className="text-3xl md:text-4xl font-light text-gray-800 mb-4">
            What Sets Us Apart
          </h2>
          <p className="text-gray-600 max-w-2xl mx-auto">
            Experience the difference with our commitment to excellence, quality, and customer satisfaction
          </p>
        </motion.div>

        {/* Mobile Slider */}
        <div className="md:hidden relative mb-6">
          <div className="overflow-hidden">
            <motion.div
              animate={{ x: `-${currentIndex * 100}%` }}
              transition={{ type: "spring", stiffness: 300, damping: 30 }}
              className="flex"
            >
              {features.map((feature) => (
                <div key={feature.title} className="w-full flex-shrink-0 px-2">
                  <div className="bg-white border border-gray-200 rounded-2xl p-6 shadow-md h-full">
                    <div className="w-14 h-14 bg-orange-100 rounded-xl flex items-center justify-center mb-4">
                      <feature.icon className="w-7 h-7 text-orange-500" />
                    </div>
                    <h3 className="text-lg font-medium text-gray-800 mb-2">
                      {feature.title}
                    </h3>
                    <p className="text-gray-600 text-sm leading-relaxed">
                      {feature.description}
                    </p>
                  </div>
                </div>
              ))}
            </motion.div>
          </div>
          <button
            onClick={() => setCurrentIndex(Math.max(0, currentIndex - 1))}
            disabled={currentIndex === 0}
            className="absolute left-0 top-1/2 -translate-y-1/2 bg-white/90 p-2 rounded-full shadow-lg disabled:opacity-50"
          >
            <ChevronLeft className="w-6 h-6" />
          </button>
          <button
            onClick={() => setCurrentIndex(Math.min(features.length - 1, currentIndex + 1))}
            disabled={currentIndex === features.length - 1}
            className="absolute right-0 top-1/2 -translate-y-1/2 bg-white/90 p-2 rounded-full shadow-lg disabled:opacity-50"
          >
            <ChevronRight className="w-6 h-6" />
          </button>
          <div className="flex justify-center gap-2 mt-4">
            {features.map((_, idx) => (
              <button
                key={idx}
                onClick={() => setCurrentIndex(idx)}
                className={`w-2 h-2 rounded-full transition-all ${
                  idx === currentIndex ? 'bg-orange-500 w-6' : 'bg-gray-300'
                }`}
              />
            ))}
          </div>
        </div>

        {/* Mobile CTA */}
        <div className="md:hidden mb-6">
          <Link to={createPageUrl("Contact")}>
            <Button 
              size="lg" 
              style={{ backgroundColor: '#FF8C00', color: 'white' }}
              className="w-full hover:opacity-90 text-base py-6"
            >
              Get in Touch
              <ArrowRight className="w-5 h-5 ml-2" />
            </Button>
          </Link>
        </div>

        {/* Desktop Grid */}
        <div className="hidden md:grid md:grid-cols-2 lg:grid-cols-3 gap-6">
          {features.map((feature, index) => (
            <motion.div
              key={feature.title}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: index * 0.1 }}
              viewport={{ once: true }}
              className="group bg-white hover:bg-gray-50 border border-gray-200 hover:border-orange-400 rounded-2xl p-8 transition-all duration-500 shadow-md hover:shadow-lg"
            >
              <div className="w-14 h-14 bg-orange-100 rounded-xl flex items-center justify-center mb-6 group-hover:bg-orange-200 transition-colors">
                <feature.icon className="w-7 h-7 text-orange-500" />
              </div>
              
              <h3 className="text-xl font-medium text-gray-800 mb-3">
                {feature.title}
              </h3>
              
              <p className="text-gray-600 text-sm leading-relaxed">
                {feature.description}
              </p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}