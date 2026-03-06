import React from 'react';
import { motion } from 'framer-motion';

export default function HeroSection() {
  return (
    <section className="relative min-h-screen bg-white overflow-hidden">
      {/* Video Background */}
      <div className="absolute inset-0 w-full h-full overflow-hidden">
        <iframe
          className="absolute pointer-events-none"
          src="https://www.youtube.com/embed/_UE-muzzbz4?autoplay=1&mute=1&loop=1&playlist=_UE-muzzbz4&controls=0&showinfo=0&modestbranding=1&rel=0"
          title="Ishtika Homes Video"
          allow="autoplay; encrypted-media"
          style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            width: 'max(100vw, 177.77vh)',
            height: 'max(100vh, 56.25vw)',
            pointerEvents: 'none',
            border: 'none',
          }}
        />
        {/* Dark overlay for better text readability */}
        <div className="absolute inset-0 bg-black/40" />
      </div>

      {/* Main content - Mobile only */}
      <div className="md:hidden absolute inset-0 flex items-center justify-center z-10">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 1 }}
          className="text-center px-4"
        >
          <h1 className="font-light italic text-6xl text-white tracking-wide mb-2" style={{ fontFamily: "'Playfair Display', serif" }}>
            ishtika
          </h1>
          <p className="text-orange-400 text-xl tracking-[0.4em] uppercase font-light">
            HOMES
          </p>
        </motion.div>
      </div>

      {/* Scroll indicator - positioned at bottom */}
      <motion.div
        className="absolute bottom-10 left-1/2 -translate-x-1/2 z-10"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 1.2 }}
      >
        <motion.div
          animate={{ y: [0, 10, 0] }}
          transition={{ duration: 2, repeat: Infinity }}
          className="w-6 h-10 border-2 border-white/80 rounded-full flex justify-center pt-2"
        >
          <div className="w-1 h-2 bg-orange-400 rounded-full" />
        </motion.div>
      </motion.div>
    </section>
  );
}