import React from 'react';
import Navbar from '@/components/Navbar';
import HeroSection from '@/components/home/HeroSection';
import StatsSection from '@/components/home/StatsSection';

import FeaturedProjects from '@/components/home/FeaturedProjects';
import AboutSection from '@/components/home/AboutSection';
import ProcessSection from '@/components/home/ProcessSection';
import HomeCTASection from '@/components/home/HomeCTASection';

import WhyUsSection from '@/components/home/WhyUsSection';
import LocationHighlights from '@/components/home/LocationHighlights';
import TestimonialsSection from '@/components/home/TestimonialsSection';
import CTASection from '@/components/home/CTASection';
import BlogSection from '@/components/home/BlogSection';
import FAQSection from '@/components/home/FAQSection';
import Footer from '@/components/home/Footer';
import WhatsAppButton from '@/components/WhatsAppButton';

export default function Home() {
  return (
    <div className="bg-white min-h-screen">
      <WhatsAppButton />
      <Navbar />
      <HeroSection />
      <StatsSection />
      <AboutSection />
      <FeaturedProjects />
      <ProcessSection />
      <HomeCTASection />
      <WhyUsSection />
      <TestimonialsSection />
      <FAQSection />
      <CTASection />
      <Footer />
    </div>
  );
}