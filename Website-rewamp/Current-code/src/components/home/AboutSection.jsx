import React from 'react';
import { motion } from 'framer-motion';
import { Button } from "@/components/ui/button";
import { ArrowRight, Home, Award, Users } from 'lucide-react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import EditableImage from '@/components/EditableImage';

export default function AboutSection() {
  return (
    <section className="bg-white py-8 md:py-12 px-4">
      <div className="max-w-7xl mx-auto">
        <div className="grid lg:grid-cols-2 gap-16 items-center">
          {/* Left content */}
          <motion.div
            initial={{ opacity: 0, x: -40 }}
            whileInView={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8 }}
            viewport={{ once: true }}
            className="order-2 lg:order-1"
          >
            <h2 className="text-2xl md:text-3xl tracking-[0.15em] uppercase text-orange-500 mb-6 font-semibold">
              About Ishtika Homes
            </h2>

            <p className="text-gray-600 leading-relaxed mb-8">
              At Ishtika Homes, we create thoughtfully designed living spaces that bring together 
              Vastu harmony, spacious planning, and modern comfort. Our projects in Karnataka are 
              built to offer affordable homes without compromising on quality or lifestyle. With 
              refined amenities, practical layouts, and a focus on peaceful living, we are committed 
              to delivering every project on time and ensuring long-lasting value for every family.
            </p>

            <div className="grid grid-cols-3 gap-4 mb-8">
              <div className="text-center p-4 bg-white rounded-xl shadow-md border border-gray-200">
                <Award className="w-6 h-6 text-orange-500 mx-auto mb-2" />
                <div className="text-2xl font-light text-gray-800">12+</div>
                <div className="text-xs text-gray-500 uppercase tracking-wider font-medium">Years</div>
              </div>
              <div className="text-center p-4 bg-white rounded-xl shadow-md border border-gray-200">
                <Home className="w-6 h-6 text-orange-500 mx-auto mb-2" />
                <div className="text-2xl font-light text-gray-800">15+</div>
                <div className="text-xs text-gray-500 uppercase tracking-wider font-medium">Projects</div>
              </div>
              <div className="text-center p-4 bg-white rounded-xl shadow-md border border-gray-200">
                <Users className="w-6 h-6 text-orange-500 mx-auto mb-2" />
                <div className="text-2xl font-light text-gray-800">2K+</div>
                <div className="text-xs text-gray-500 uppercase tracking-wider font-medium">Families</div>
              </div>
            </div>

            <Link to={createPageUrl('About')}>
              <Button
                size="lg"
                style={{ backgroundColor: '#FF8C00', color: 'white' }}
                className="hover:opacity-90 px-8"
              >
                Discover Our Story
                <ArrowRight className="w-4 h-4 ml-2" />
              </Button>
            </Link>
          </motion.div>

          {/* Right images */}
          <motion.div
            initial={{ opacity: 0, x: 40 }}
            whileInView={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8, delay: 0.2 }}
            viewport={{ once: true }}
            className="relative order-1 lg:order-2"
          >
            <div className="relative">
              <EditableImage
                imageKey="about-main-image"
                src="/images/anahata-birdeye.jpeg"
                alt="Anahata Premium Apartments by Ishtika Homes"
                className="w-full rounded-2xl object-contain"
              />
            </div>
          </motion.div>
        </div>
      </div>
    </section>
  );
}