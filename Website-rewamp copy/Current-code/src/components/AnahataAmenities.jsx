import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { 
  Dumbbell, Waves, TentTree, Bike, Music, BookOpen, 
  Wind, TreeDeciduous, Users, Shield, Camera, Droplets,
  Zap, Wifi, Car, Baby, Heart, Coffee, Sun, Building,
  ChevronLeft, ChevronRight, ArrowRight
} from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';

const amenities = [
  { icon: Waves, name: "Swimming Pool" },
  { icon: Dumbbell, name: "Fully Equipped Gym" },
  { icon: Users, name: "Clubhouse" },
  { icon: Baby, name: "Children's Play Area" },
  { icon: TentTree, name: "Indoor Games" },
  { icon: Bike, name: "Cycling & Jogging Track" },
  { icon: TreeDeciduous, name: "Landscaped Gardens" },
  { icon: Shield, name: "24/7 Security" },
  { icon: Camera, name: "CCTV Surveillance" },
  { icon: Car, name: "Ample Parking Space" },
  { icon: Zap, name: "Power Backup" },
  { icon: Wifi, name: "High-Speed Internet" },
  { icon: Droplets, name: "Water Softener Plant" },
  { icon: Wind, name: "Rainwater Harvesting" },
  { icon: Sun, name: "Solar Power Backup" },
  { icon: Heart, name: "Yoga & Meditation Area" },
  { icon: Coffee, name: "Cafeteria" },
  { icon: Music, name: "Amphitheatre" },
  { icon: BookOpen, name: "Library & Reading Room" },
  { icon: Building, name: "Party Hall" },
];

export default function AnahataAmenities() {
  const [pageIndex, setPageIndex] = useState(0);
  const itemsPerPage = 6; // 3 rows x 2 columns
  const totalPages = Math.ceil(amenities.length / itemsPerPage);

  const getCurrentPageAmenities = () => {
    const start = pageIndex * itemsPerPage;
    return amenities.slice(start, start + itemsPerPage);
  };

  return (
    <section className="py-6 md:py-20 px-4 bg-white">
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-10 md:mb-16"
        >
          <h2 className="text-3xl md:text-4xl font-light text-gray-800 mb-4">
            50+ Premium Amenities
          </h2>
          <p className="text-gray-600">
            Experience luxury living with world-class facilities
          </p>
        </motion.div>

        {/* Mobile Slider - 3 rows x 2 columns */}
        <div className="md:hidden relative mb-6">
          <div className="overflow-hidden">
            <motion.div
              key={pageIndex}
              initial={{ opacity: 0, x: 50 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ duration: 0.3 }}
              className="grid grid-cols-2 gap-4"
            >
              {getCurrentPageAmenities().map((amenity, index) => (
                <motion.div
                  key={amenity.name}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: index * 0.05 }}
                  className="flex flex-col items-center text-center p-4 bg-orange-50 rounded-xl"
                >
                  <div className="w-12 h-12 bg-orange-100 rounded-full flex items-center justify-center mb-3">
                    <amenity.icon className="w-6 h-6 text-orange-500" />
                  </div>
                  <p className="text-sm text-gray-700 font-medium">{amenity.name}</p>
                </motion.div>
              ))}
            </motion.div>
          </div>

          {/* Navigation Buttons */}
          <button
            onClick={() => setPageIndex(Math.max(0, pageIndex - 1))}
            disabled={pageIndex === 0}
            className="absolute left-0 top-1/2 -translate-y-1/2 -translate-x-2 bg-white/90 p-2 rounded-full shadow-lg disabled:opacity-50 z-10"
          >
            <ChevronLeft className="w-6 h-6" />
          </button>
          <button
            onClick={() => setPageIndex(Math.min(totalPages - 1, pageIndex + 1))}
            disabled={pageIndex === totalPages - 1}
            className="absolute right-0 top-1/2 -translate-y-1/2 translate-x-2 bg-white/90 p-2 rounded-full shadow-lg disabled:opacity-50 z-10"
          >
            <ChevronRight className="w-6 h-6" />
          </button>

          {/* Pagination Dots */}
          <div className="flex justify-center gap-2 mt-6">
            {Array.from({ length: totalPages }).map((_, idx) => (
              <button
                key={idx}
                onClick={() => setPageIndex(idx)}
                className={`w-2 h-2 rounded-full transition-all ${
                  idx === pageIndex ? 'bg-orange-500 w-6' : 'bg-gray-300'
                }`}
              />
            ))}
          </div>
        </div>

        {/* Desktop Grid */}
        <div className="hidden md:grid md:grid-cols-4 lg:grid-cols-5 gap-6">
          {amenities.map((amenity, index) => (
            <motion.div
              key={amenity.name}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.03 }}
              viewport={{ once: true }}
              className="flex flex-col items-center text-center p-4 bg-orange-50 rounded-xl hover:shadow-md transition-all"
            >
              <div className="w-12 h-12 bg-orange-100 rounded-full flex items-center justify-center mb-3">
                <amenity.icon className="w-6 h-6 text-orange-500" />
              </div>
              <p className="text-sm text-gray-700 font-medium">{amenity.name}</p>
            </motion.div>
          ))}
        </div>

        {/* Mobile CTA */}
        <div className="md:hidden mt-6">
          <Link to={createPageUrl("AnahataBookSiteVisit")}>
            <Button 
              size="lg" 
              style={{ backgroundColor: '#FF8C00', color: 'white' }}
              className="w-full hover:opacity-90 text-base py-6"
            >
              Book a Site Visit
              <ArrowRight className="w-5 h-5 ml-2" />
            </Button>
          </Link>
        </div>
      </div>
    </section>
  );
}