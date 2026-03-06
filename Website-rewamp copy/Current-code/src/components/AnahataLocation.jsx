import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Building2, Train, GraduationCap, ShoppingCart, MapPin, ArrowRight } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';

const categories = [
  {
    id: 'it',
    name: 'IT Hub Proximity',
    icon: Building2,
    description: 'Close to major IT companies and tech parks in Whitefield',
    landmarks: [
      { name: 'Whitefield Railway Station', distance: '5.3 km' },
      { name: 'ITPL (IT Park)', distance: '6.1 km' }
    ]
  },
  {
    id: 'metro',
    name: 'Metro Connectivity',
    icon: Train,
    description: 'Easy access to Namma Metro Purple Line stations',
    landmarks: [
      { name: 'Hopeform Channasandra metro', distance: '5.4 km' },
      { name: 'Hopefarm Junction', distance: '5 km' }
    ]
  },
  {
    id: 'education',
    name: 'Educational Hubs',
    icon: GraduationCap,
    description: 'Renowned schools and colleges in the vicinity',
    landmarks: [
      { name: 'Bangalore International Academy', distance: '1.7 km' },
      { name: 'Delhi Public School Whitefield', distance: '4 km' }
    ]
  },
  {
    id: 'shopping',
    name: 'Shopping & Dining',
    icon: ShoppingCart,
    description: 'Hopefarm Channasandra metro, VR Mall, and fine dining options',
    landmarks: [
      { name: 'Phoenix Marketcity', distance: '9 km' },
      { name: 'VR Bengaluru', distance: '10 km' }
    ]
  }
];

export default function AnahataLocation() {
  const [activeCategory, setActiveCategory] = useState('it');
  const activeCategoryData = categories.find(c => c.id === activeCategory);

  return (
    <>
      {/* Location Advantage */}
      <section className="py-6 md:py-16 px-4 bg-gray-50">
        <div className="max-w-7xl mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-16"
          >
            <h2 className="text-3xl md:text-4xl font-bold text-gray-900 mb-4">
              Prime Location Advantage
            </h2>
            <p className="text-gray-600 flex items-center justify-center gap-2 text-lg">
              <MapPin className="w-5 h-5 text-orange-500" />
              Soukya Road, Whitefield, Bengaluru
            </p>
          </motion.div>

          <div className="grid md:grid-cols-2 gap-12 items-center mb-16">
            <motion.div
              initial={{ opacity: 0, x: -40 }}
              whileInView={{ opacity: 1, x: 0 }}
              viewport={{ once: true }}
            >
              <div className="space-y-4">
                <p className="text-gray-700 leading-relaxed text-lg">
                  Anahata is strategically located in Whitefield, one of Bangalore's most sought-after residential and IT hubs. The project offers excellent connectivity to major business districts, educational institutions, healthcare facilities, and entertainment zones.
                </p>
                <p className="text-gray-700 leading-relaxed text-lg">
                  With the upcoming metro connectivity and well-developed infrastructure, Whitefield continues to be a prime investment destination with strong appreciation potential.
                </p>
                
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
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: 40 }}
              whileInView={{ opacity: 1, x: 0 }}
              viewport={{ once: true }}
              className="h-96 bg-gray-200 rounded-xl overflow-hidden shadow-lg"
            >
              <iframe
                src="https://www.google.com/maps/embed?pb=!1m18!1m12!1m3!1d3887.7485!2d77.7497!3d12.9900!2m3!1f0!2f0!3f0!3m2!1i1024!2i768!4f13.1!3m3!1m2!1s0x0%3A0x0!2zMTLCsDU5JzI0LjAiTiA3N8KwNDQnNTkuMCJF!5e0!3m2!1sen!2sin!4v1234567890"
                width="100%"
                height="100%"
                style={{ border: 0 }}
                allowFullScreen
                loading="lazy"
                title="Anahata Location"
              />
            </motion.div>
          </div>
        </div>
      </section>

      {/* Nearby Landmarks with Category Tabs */}
      <section className="py-6 md:py-16 px-4 bg-white">
        <div className="max-w-6xl mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-12"
          >
            <h2 className="text-3xl md:text-4xl font-bold text-gray-900 mb-2">
              Nearby Landmarks
            </h2>
            <p className="text-gray-600 text-lg">
              Everything you need is just minutes away
            </p>
          </motion.div>

          {/* Category Tabs */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-12"
          >
            {categories.map((category, index) => (
              <motion.button
                key={category.id}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1 }}
                viewport={{ once: true }}
                onClick={() => setActiveCategory(category.id)}
                className={`group relative p-6 rounded-xl border-2 transition-all duration-300 ${
                  activeCategory === category.id
                    ? 'bg-orange-50 border-orange-500 shadow-lg scale-105'
                    : 'bg-white border-gray-200 hover:border-orange-300 hover:shadow-md hover:scale-102'
                }`}
              >
                <category.icon 
                  className={`w-10 h-10 md:w-12 md:h-12 mx-auto mb-3 transition-all duration-300 ${
                    activeCategory === category.id ? 'text-orange-500' : 'text-gray-400 group-hover:text-orange-400'
                  }`} 
                />
                <h3 className={`text-xs md:text-sm font-bold transition-colors ${
                  activeCategory === category.id ? 'text-orange-600' : 'text-gray-700'
                }`}>
                  {category.name}
                </h3>
                <p className="text-xs text-gray-500 mt-2 hidden md:block">
                  {category.description}
                </p>
              </motion.button>
            ))}
          </motion.div>

          {/* Landmarks List */}
          <motion.div
            key={activeCategory}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3 }}
            className="mb-8"
          >
            <h3 className="text-2xl font-bold text-center text-orange-600 mb-8">
              Nearby Landmarks & Distances
            </h3>
            <div className="grid md:grid-cols-2 gap-6 max-w-4xl mx-auto">
              {activeCategoryData.landmarks.map((landmark, index) => (
                <motion.div
                  key={landmark.name}
                  initial={{ opacity: 0, x: index % 2 === 0 ? -20 : 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: index * 0.15 }}
                  className="group bg-white rounded-xl p-6 shadow-md hover:shadow-xl transition-all duration-300 border border-gray-100 hover:border-orange-200 hover:scale-105"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-start gap-3 flex-1">
                      <MapPin className="w-5 h-5 text-orange-500 mt-1 flex-shrink-0" />
                      <div>
                        <h4 className="text-base font-semibold text-gray-900 group-hover:text-orange-600 transition-colors">
                          {landmark.name}
                        </h4>
                      </div>
                    </div>
                    <span className="px-4 py-2 bg-orange-100 text-orange-600 rounded-full text-sm font-bold whitespace-nowrap ml-2">
                      {landmark.distance}
                    </span>
                  </div>
                </motion.div>
              ))}
            </div>
          </motion.div>

          {/* CTA Button */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center"
          >
            <Link to={createPageUrl("AnahataBookSiteVisit")}>
              <Button 
                size="lg" 
                style={{ backgroundColor: '#FF8C00', color: 'white' }}
                className="hover:opacity-90 px-8 py-6"
              >
                Book a Site Visit
                <ArrowRight className="w-5 h-5 ml-2" />
              </Button>
            </Link>
          </motion.div>

        </div>
      </section>
    </>
  );
}