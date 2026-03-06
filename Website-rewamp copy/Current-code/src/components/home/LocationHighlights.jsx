import React from 'react';
import { motion } from 'framer-motion';
import { MapPin, TrendingUp, Building2, Briefcase, GraduationCap, Hospital, ShoppingBag, Train } from 'lucide-react';

const locations = [
  {
    name: "Whitefield",
    image: "https://www.ishtikahomes.com/assets/img/all-images/location-img1.png",
    description: "Bangalore's premier IT hub with world-class infrastructure and connectivity.",
    highlights: [
      { icon: Briefcase, text: "IT Parks & Tech Companies" },
      { icon: ShoppingBag, text: "Phoenix Marketcity & Malls" },
      { icon: Train, text: "Upcoming Metro Connectivity" },
    ],
    growth: "+35% appreciation in 5 years",
  },
  {
    name: "JP Nagar",
    image: "https://www.ishtikahomes.com/assets/img/all-images/location-img2.png",
    description: "Well-established residential area with excellent social infrastructure.",
    highlights: [
      { icon: GraduationCap, text: "Top Schools & Colleges" },
      { icon: Hospital, text: "Multi-specialty Hospitals" },
      { icon: ShoppingBag, text: "Shopping & Entertainment" },
    ],
    growth: "+28% appreciation in 5 years",
  },
  {
    name: "Hosapete",
    image: "https://www.ishtikahomes.com/assets/img/all-images/location-img3.png",
    description: "Emerging investment destination with industrial and heritage tourism potential.",
    highlights: [
      { icon: Building2, text: "Industrial Development" },
      { icon: MapPin, text: "Near Hampi UNESCO Site" },
      { icon: TrendingUp, text: "High Growth Potential" },
    ],
    growth: "+40% appreciation potential",
  },
];

export default function LocationHighlights() {
  return (
    <section className="bg-white py-24 px-4">
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">
            Prime Locations
          </p>
          <h2 className="text-3xl md:text-4xl font-light text-gray-800 mb-4">
            Strategic Locations Across Karnataka
          </h2>
          <p className="text-gray-600 max-w-2xl mx-auto">
            Carefully chosen locations offering excellent connectivity, infrastructure, and investment potential
          </p>
        </motion.div>

        <div className="grid md:grid-cols-3 gap-8">
          {locations.map((location, index) => (
            <motion.div
              key={location.name}
              initial={{ opacity: 0, y: 40 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: index * 0.15 }}
              viewport={{ once: true }}
              className="group"
            >
              <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden hover:shadow-xl transition-all duration-300">
                {/* Image */}
                <div className="relative aspect-[16/10] overflow-hidden">
                  <img
                    src={location.image}
                    alt={location.name}
                    className="w-full h-full object-cover transition-transform duration-700 group-hover:scale-110"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent" />
                  
                  {/* Growth badge */}
                  <div className="absolute top-4 right-4 bg-green-500 text-white px-3 py-1 rounded-full text-xs font-semibold flex items-center gap-1">
                    <TrendingUp className="w-3 h-3" />
                    {location.growth}
                  </div>

                  <div className="absolute bottom-4 left-4">
                    <h3 className="text-2xl font-semibold text-white flex items-center gap-2">
                      <MapPin className="w-5 h-5 text-orange-400" />
                      {location.name}
                    </h3>
                  </div>
                </div>

                {/* Content */}
                <div className="p-6">
                  <p className="text-gray-600 text-sm mb-6 leading-relaxed">
                    {location.description}
                  </p>

                  <div className="space-y-3">
                    {location.highlights.map((highlight, idx) => (
                      <div key={idx} className="flex items-center gap-3">
                        <div className="w-8 h-8 bg-orange-100 rounded-lg flex items-center justify-center flex-shrink-0">
                          <highlight.icon className="w-4 h-4 text-orange-500" />
                        </div>
                        <span className="text-sm text-gray-700">{highlight.text}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}