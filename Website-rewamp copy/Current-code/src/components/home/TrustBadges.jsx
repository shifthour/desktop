import React from 'react';
import { motion } from 'framer-motion';

const badges = [
  { 
    name: "Architecture Quality", 
    image: "https://qtrypzzcjebvfcihiynt.supabase.co/storage/v1/object/public/base44-prod/public/697a530d2bb6906969ab4953/b16ba4e88_Gemini_Generated_Image_rzvzxmrzvzxmrzvz.png" 
  },
  { 
    name: "RERA Approved", 
    image: "https://qtrypzzcjebvfcihiynt.supabase.co/storage/v1/object/public/base44-prod/public/697a530d2bb6906969ab4953/60c96deb3_Gemini_Generated_Image_bk3krbk3krbk3krb.png" 
  },
  { 
    name: "Trusted Bank Partner", 
    image: "https://qtrypzzcjebvfcihiynt.supabase.co/storage/v1/object/public/base44-prod/public/697a530d2bb6906969ab4953/48a0c8a92_Gemini_Generated_Image_33x27z33x27z33x2.png" 
  },
];

export default function TrustBadges() {
  return (
    <section className="py-8 px-4 bg-white">
      <div className="max-w-7xl mx-auto">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-8 md:gap-12 items-center justify-items-center">
          {badges.map((badge, index) => (
            <motion.div
              key={badge.name}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.1, duration: 0.5 }}
              viewport={{ once: true }}
              className="w-full max-w-xs"
            >
              <img 
                src={badge.image} 
                alt={badge.name}
                className="w-full h-auto object-contain"
              />
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}