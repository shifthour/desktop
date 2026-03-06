import React from 'react';
import { motion } from 'framer-motion';

const stats = [
  { number: "12+", label: "Years Of Experience" },
  { number: "12+", label: "Established Projects" },
  { number: "5+", label: "Ongoing Projects" },
  { number: "2K+", label: "Happy Residents" },
];

export default function StatsSection() {
  return (
    <section className="bg-gradient-to-b from-white to-gray-50 py-10 md:py-16 px-4">
      <div className="max-w-6xl mx-auto">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
          {stats.map((stat, index) => (
            <motion.div
              key={stat.label}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: index * 0.1 }}
              viewport={{ once: true }}
              className="text-center"
            >
              <div className="text-4xl md:text-5xl font-light text-orange-500 mb-2">
                {stat.number}
              </div>
              <div className="text-gray-600 text-sm tracking-wider uppercase font-medium">
                {stat.label}
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}