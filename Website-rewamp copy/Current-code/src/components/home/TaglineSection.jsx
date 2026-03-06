import React from 'react';
import { motion } from 'framer-motion';

export default function TaglineSection() {
  return (
    <section className="relative w-full overflow-hidden" style={{ height: '65vh', minHeight: '400px' }}>
      {/* Parallax Background Image */}
      <div
        className="absolute inset-0 bg-fixed bg-cover bg-center"
        style={{
          backgroundImage: 'url(/images/anahata-entrance.jpg)',
          backgroundAttachment: 'fixed',
        }}
      />

      {/* Subtle dark gradient overlay — heavier at bottom for depth */}
      <div className="absolute inset-0 bg-gradient-to-t from-black/50 via-black/20 to-black/10" />

      {/* Minimal text overlay at bottom-left */}
      <div className="absolute inset-0 flex items-end">
        <div className="px-6 md:px-16 pb-10 md:pb-14 max-w-3xl">
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8 }}
            viewport={{ once: true }}
            className="text-white/90 text-sm md:text-base tracking-[0.25em] uppercase font-medium mb-2"
          >
            Crafting Spaces That Inspire
          </motion.p>
          <motion.h2
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8, delay: 0.15 }}
            viewport={{ once: true }}
            className="text-3xl md:text-5xl font-light text-white leading-tight"
          >
            Where Vision Meets{' '}
            <span className="text-orange-400 font-normal">Home</span>
          </motion.h2>
        </div>
      </div>
    </section>
  );
}
