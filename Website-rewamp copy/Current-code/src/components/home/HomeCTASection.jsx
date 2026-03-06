import React from 'react';
import { motion } from 'framer-motion';
import { Button } from "@/components/ui/button";
import { ArrowRight } from 'lucide-react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import EditableBackgroundImage from '@/components/EditableBackgroundImage';

export default function HomeCTASection() {
  return (
    <section className="py-0 px-0">
      <div className="max-w-full mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="relative overflow-hidden min-h-[350px] md:min-h-[500px] flex items-center justify-start"
        >
          {/* Background Image */}
          <EditableBackgroundImage
            imageKey="home-cta-background"
            src="https://www.ishtikahomes.com/assets/img/aerial-view-bg.jpg"
            className="absolute inset-0 bg-cover bg-center"
          >
            <div className="absolute inset-0 bg-black/60" />
          </EditableBackgroundImage>

          {/* Content */}
          <div className="relative px-8 md:px-16 py-16 w-full text-center">
            <h2 className="text-3xl md:text-4xl font-light text-white mb-8 leading-tight">
              Ready to Find Your Dream Home?
            </h2>
            <p className="text-white/90 text-lg mb-8 max-w-2xl mx-auto">
              Explore our projects and discover Vastu-aligned homes designed for modern living
            </p>
            <Link to={createPageUrl('Projects')}>
              <Button size="lg" className="bg-white text-gray-800 hover:bg-gray-100 px-8 group">
                View Projects
                <ArrowRight className="w-5 h-5 ml-2 group-hover:translate-x-1 transition-transform" />
              </Button>
            </Link>
          </div>
        </motion.div>
      </div>
    </section>
  );
}