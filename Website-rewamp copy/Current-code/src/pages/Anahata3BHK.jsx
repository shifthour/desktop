import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Home, CheckCircle, ArrowRight, MapPin, Trees, Building2, ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import AnahataAmenities from '@/components/AnahataAmenities';
import AnahataLocation from '@/components/AnahataLocation';
import AnahataFAQ from '@/components/AnahataFAQ';
import Footer from '@/components/home/Footer';
import EditableImage from '@/components/EditableImage';
import EditableBackgroundImage from '@/components/EditableBackgroundImage';

const configurations = [
  {
    type: "3 BHK 2T",
    price: "₹1.15 Cr*",
    size: "1511 - 1591 sq ft",
  },
  {
    type: "3 BHK 3T",
    price: "₹1.3 Cr*",
    size: "1670 - 1758 sq ft",
  },
];

const features = [
  "Spacious Living & Dining Area",
  "3 Large Bedrooms with Wardrobes",
  "2 or 3 Modern Bathrooms",
  "Modular Kitchen with Utility",
  "Multiple Balconies",
  "Vastu Compliant Design",
  "Premium Vitrified Tiles",
  "High-Quality Fittings",
  "Ample Natural Light & Ventilation",
  "Perfect for Growing Families",
];

const highlights = [
  { icon: Trees, number: "5 Acres", label: "Project Area" },
  { icon: Home, number: "440", label: "Families" },
  { icon: Building2, number: "5", label: "Towers" },
  { icon: Trees, number: "80%", label: "Open Space" },
  { icon: Home, number: "50+", label: "Amenities" },
  { icon: Building2, number: "20,000 sq ft", label: "Club House" },
];

const galleryImages = [
  { url: "https://www.ishtikahomes.com/anahata/images/gallery/amphitheater-night.png", title: "Amphitheater" },
  { url: "https://www.ishtikahomes.com/anahata/images/gallery/sports-facilities-day.png", title: "Sports Facilities" },
  { url: "https://www.ishtikahomes.com/anahata/images/gallery/landscaped-pathway.png", title: "Gardens" },
  { url: "https://www.ishtikahomes.com/anahata/images/gallery/tennis-courts.png", title: "Courts" },
  { url: "https://www.ishtikahomes.com/anahata/images/gallery/kids-play-area.png", title: "Play Area" },
  { url: "https://www.ishtikahomes.com/anahata/images/gallery/outdoor-clubhouse.png", title: "Clubhouse" },
];

export default function Anahata3BHK() {
  const [galleryIndex, setGalleryIndex] = useState(0);

  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  return (
    <div className="bg-white min-h-screen pt-24">
      <Navbar />
      <WhatsAppButton message="Hi, I'm interested in 3BHK apartments at Anahata. Can you please provide more details?" />

      {/* Hero */}
      <section className="relative h-[80vh] md:h-[80vh] overflow-hidden">
        <div className="absolute inset-0 w-full h-full">
          <iframe
            className="absolute pointer-events-none"
            src="https://www.youtube.com/embed/_UE-muzzbz4?autoplay=1&mute=1&loop=1&playlist=_UE-muzzbz4&controls=0&showinfo=0&rel=0&modestbranding=1"
            title="Anahata Project"
            allow="autoplay; encrypted-media"
            style={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              width: '177.77vh',
              height: '56.25vw',
              minWidth: '100%',
              minHeight: '100%',
            }}
          />
        </div>
        <div className="absolute inset-0 bg-black/50" />
        <div className="relative h-full flex flex-col items-center justify-between text-center px-4 py-12 md:py-0 md:px-6 md:justify-center">
          <div className="flex-shrink-0 mt-8 md:mt-0">
            <motion.h1
              initial={{ opacity: 0, y: 30 }}
              animate={{ opacity: 1, y: 0 }}
              className="text-3xl sm:text-4xl md:text-6xl font-light text-white mb-2 md:mb-4"
            >
              3 BHK Apartments at Anahata
            </motion.h1>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="flex items-center justify-center gap-1.5 text-orange-400 mb-6"
            >
              <MapPin className="w-3.5 h-3.5 md:w-5 md:h-5" />
              <span className="text-xs md:text-base">Soukya Road, Whitefield, Bengaluru</span>
            </motion.div>
          </div>

          {/* CTA Button */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.5 }}
            className="mb-4 md:mb-6"
          >
            <Link to={createPageUrl("AnahataBookSiteVisit")}>
              <Button 
                size="lg" 
                style={{ backgroundColor: '#FF8C00', color: 'white' }}
                className="hover:opacity-90 px-8 py-6 text-base"
              >
                Book a Site Visit
                <ArrowRight className="w-5 h-5 ml-2" />
              </Button>
            </Link>
          </motion.div>
        </div>
      </section>

      {/* Quick Info */}
      <section className="py-8 md:py-16 px-4 bg-gray-50">
        <div className="max-w-5xl mx-auto">
          <div className="grid grid-cols-2 md:grid-cols-2 gap-4 md:gap-6">
            {configurations.map((config, index) => (
              <motion.div
                key={config.type}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.1 + index * 0.1 }}
                className="bg-white p-6 sm:p-8 rounded-xl shadow-md text-center"
              >
                <Home className="w-8 h-8 text-orange-500 mx-auto mb-3" />
                <h3 className="text-2xl font-semibold text-gray-800 mb-2">{config.type}</h3>
                <div className="text-3xl font-light text-orange-500 mb-2">{config.price}</div>
                <div className="text-sm text-gray-600">{config.size}</div>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* About */}
      <section className="min-h-screen md:min-h-0 py-6 md:py-20 px-4 sm:px-6 flex items-center">
        <div className="max-w-6xl mx-auto grid md:grid-cols-2 gap-8 md:gap-12 items-center w-full">
          <motion.div
            initial={{ opacity: 0, x: -40 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
          >
            <EditableImage
              imageKey="anahata-3bhk-living-room"
              src="https://qtrypzzcjebvfcihiynt.supabase.co/storage/v1/object/public/base44-prod/public/697a530d2bb6906969ab4953/83428c180_BirdEye_v1.jpg"
              alt="Aerial view of Anahata"
              className="rounded-2xl shadow-xl w-full"
            />
          </motion.div>
          <motion.div
            initial={{ opacity: 0, x: 40 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
          >
            <h2 className="text-3xl sm:text-4xl font-light text-gray-800 mb-6">About 3BHK at Anahata</h2>
            <p className="text-gray-600 text-base sm:text-lg leading-relaxed mb-4">
              Our spacious 3BHK apartments at Anahata are designed for growing families who value comfort and luxury. Available in two configurations - 3BHK 2T (1511-1591 sq ft) and 3BHK 3T (1670-1758 sq ft) - these apartments offer generous living spaces with premium finishes.
            </p>
            <p className="text-gray-600 text-base sm:text-lg leading-relaxed mb-6">
              Each apartment features three well-appointed bedrooms, multiple bathrooms, a large living-dining area, modern kitchen, and multiple balconies. The Vastu-compliant design ensures harmony and positive energy throughout your home.
            </p>
            <Link to={createPageUrl("AnahataBookSiteVisit")}>
              <Button 
                size="lg" 
                style={{ backgroundColor: '#FF8C00', color: 'white' }}
                className="hover:opacity-90 w-full sm:w-auto"
              >
                Book a Site Visit
                <ArrowRight className="w-5 h-5 ml-2" />
              </Button>
            </Link>
          </motion.div>
        </div>
      </section>

      {/* Highlights */}
      <section className="py-5 md:py-20 px-4 sm:px-6 bg-gray-50">
        <div className="max-w-6xl mx-auto w-full">
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 sm:gap-4 md:gap-6 mb-6 md:mb-0">
            {highlights.map((item, index) => (
              <motion.div
                key={item.label}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1 }}
                viewport={{ once: true }}
                className="text-center bg-white p-4 sm:p-6 rounded-xl shadow-md"
              >
                <item.icon className="w-6 h-6 sm:w-8 sm:h-8 text-orange-500 mx-auto mb-2 sm:mb-3" />
                <div className="text-lg sm:text-2xl font-light text-gray-800 mb-1">{item.number}</div>
                <div className="text-[10px] sm:text-xs text-gray-600 uppercase tracking-wider">{item.label}</div>
              </motion.div>
            ))}
          </div>
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

      {/* Floor Plan */}
      <section className="pt-2 pb-6 md:py-20 px-4 sm:px-6">
        <div className="max-w-5xl mx-auto w-full">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-6 md:mb-12"
          >
            <h2 className="text-3xl sm:text-4xl font-light text-gray-800 mb-4">3BHK Floor Plan</h2>
            <p className="text-gray-600 text-base sm:text-lg">Spacious layouts designed for comfort</p>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            whileInView={{ opacity: 1, scale: 1 }}
            viewport={{ once: true }}
            className="bg-gray-50 p-6 sm:p-8 rounded-2xl"
          >
            <EditableImage
              imageKey="anahata-3bhk-floor-plan"
              src="https://www.ishtikahomes.com/anahata/images/floor-plans/block-b.png"
              alt="3BHK Floor Plan"
              className="w-full rounded-xl"
            />
          </motion.div>

          {/* Mobile CTA */}
          <div className="sm:hidden mt-6">
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

      {/* Features */}
      <section className="pt-2 pb-6 md:py-20 px-4 sm:px-6 bg-gray-50">
        <div className="max-w-5xl mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-6 md:mb-12"
          >
            <h2 className="text-3xl sm:text-4xl font-light text-gray-800 mb-4">Key Features</h2>
          </motion.div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6 md:mb-0">
            {features.map((feature, index) => (
              <motion.div
                key={feature}
                initial={{ opacity: 0, x: -20 }}
                whileInView={{ opacity: 1, x: 0 }}
                transition={{ delay: index * 0.05 }}
                viewport={{ once: true }}
                className="flex items-center gap-3 bg-white p-4 rounded-xl shadow-sm"
              >
                <CheckCircle className="w-5 h-5 text-green-500 flex-shrink-0" />
                <span className="text-gray-700">{feature}</span>
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

      {/* Gallery */}
      <section className="py-5 md:py-20 px-4 sm:px-6">
        <div className="max-w-7xl mx-auto w-full">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-10 sm:mb-16"
          >
            <h2 className="text-3xl sm:text-4xl md:text-5xl font-light text-gray-800 mb-4 sm:mb-6 px-2">
              Project Gallery
            </h2>
            <p className="text-gray-600 text-base sm:text-lg px-4">
              Explore the stunning visuals of Anahata
            </p>
          </motion.div>

          {/* Mobile Slider */}
          <div className="sm:hidden relative mb-6">
            <div className="overflow-hidden">
              <motion.div
                animate={{ x: `-${galleryIndex * 100}%` }}
                transition={{ type: "spring", stiffness: 300, damping: 30 }}
                className="flex"
              >
                {galleryImages.map((image, index) => (
                  <div key={index} className="w-full flex-shrink-0 px-2">
                    <div className="relative w-full h-56 overflow-hidden rounded-xl shadow-md">
                      <EditableImage
                        imageKey={`anahata-3bhk-gallery-${index}`}
                        src={image.url}
                        alt={image.title}
                        className="w-full h-full object-cover"
                      />
                      <div className="absolute inset-0 bg-gradient-to-t from-black/70 to-transparent">
                        <div className="absolute bottom-4 left-4 right-4">
                          <p className="text-white font-medium text-sm">{image.title}</p>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </motion.div>
            </div>
            <button
              onClick={() => setGalleryIndex(Math.max(0, galleryIndex - 1))}
              disabled={galleryIndex === 0}
              className="absolute left-0 top-1/2 -translate-y-1/2 bg-white/90 p-2 rounded-full shadow-lg disabled:opacity-50"
            >
              <ChevronLeft className="w-6 h-6" />
            </button>
            <button
              onClick={() => setGalleryIndex(Math.min(galleryImages.length - 1, galleryIndex + 1))}
              disabled={galleryIndex === galleryImages.length - 1}
              className="absolute right-0 top-1/2 -translate-y-1/2 bg-white/90 p-2 rounded-full shadow-lg disabled:opacity-50"
            >
              <ChevronRight className="w-6 h-6" />
            </button>
            <div className="flex justify-center gap-2 mt-4">
              {galleryImages.map((_, idx) => (
                <button
                  key={idx}
                  onClick={() => setGalleryIndex(idx)}
                  className={`w-2 h-2 rounded-full transition-all ${
                    idx === galleryIndex ? 'bg-orange-500 w-6' : 'bg-gray-300'
                  }`}
                />
              ))}
            </div>
          </div>

          {/* Desktop Grid */}
          <div className="hidden sm:grid sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
            {galleryImages.map((image, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, scale: 0.9 }}
                whileInView={{ opacity: 1, scale: 1 }}
                transition={{ delay: index * 0.1 }}
                viewport={{ once: true }}
                className="group relative aspect-[4/3] overflow-hidden rounded-xl shadow-md hover:shadow-xl transition-all"
              >
                <EditableImage
                  imageKey={`anahata-3bhk-gallery-${index}`}
                  src={image.url}
                  alt={image.title}
                  className="w-full h-full object-cover transition-transform duration-500 group-hover:scale-110"
                />
                <div className="absolute inset-0 bg-gradient-to-t from-black/70 to-transparent opacity-0 group-hover:opacity-100 transition-opacity">
                  <div className="absolute bottom-4 left-4">
                    <p className="text-white font-medium">{image.title}</p>
                  </div>
                </div>
              </motion.div>
            ))}
          </div>

          {/* Mobile CTA */}
          <div className="sm:hidden mt-6">
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

      {/* Amenities */}
      <AnahataAmenities />

      {/* Location */}
      <AnahataLocation />

      {/* FAQ */}
      <AnahataFAQ />

      {/* CTA */}
      <section className="py-0 px-0 mt-6 md:mt-0">
        <div className="max-w-full mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="relative overflow-hidden min-h-[350px] md:min-h-[500px] flex items-center justify-center"
          >
            <EditableBackgroundImage
              imageKey="anahata-3bhk-cta-background"
              src="https://www.ishtikahomes.com/assets/img/aerial-view-bg.jpg"
              className="absolute inset-0 bg-cover bg-center"
            >
              <div className="absolute inset-0 bg-black/60" />
            </EditableBackgroundImage>

            <div className="relative px-8 md:px-16 py-16 w-full text-center">
              <h2 className="text-3xl md:text-4xl font-light text-white mb-8 leading-tight">
                Interested in this 3BHK Apartment?
              </h2>
              <p className="text-white/90 text-lg mb-8 max-w-2xl mx-auto">
                Schedule a site visit to experience Anahata firsthand
              </p>
              <Link to={createPageUrl("AnahataBookSiteVisit")}>
                <Button size="lg" className="bg-white text-gray-800 hover:bg-gray-100 px-8 group">
                  Book Site Visit
                  <ArrowRight className="w-5 h-5 ml-2 group-hover:translate-x-1 transition-transform" />
                </Button>
              </Link>
            </div>
          </motion.div>
        </div>
      </section>

      {/* RERA & Disclaimer */}
      <section className="bg-gray-900 text-white py-8 px-4">
        <div className="max-w-4xl mx-auto text-center space-y-4">
          <div className="space-y-2">
            <p className="text-sm">
              <span className="font-medium text-orange-400">Agent RERA No :</span>{' '}
              PRM/KA/RERA/1251/446/AG/250617/005841
            </p>
            <p className="text-sm">
              <span className="font-medium text-orange-400">Project RERA No:</span>{' '}
              PRM/KA/RERA/1250/304/PR/290425/007702, PRM/KA/RERA/1250/304/PR/050725/007898
            </p>
          </div>
          <div>
            <p className="text-xs text-gray-400 leading-relaxed">
              <span className="font-medium text-gray-300">Disclaimer -</span> The content provided on this website is for informational purposes only and does not constitute an offer to avail any service. The prices mentioned are subject to change without prior notice, and the availability of properties mentioned is not guaranteed.
            </p>
          </div>
        </div>
      </section>

      <Footer />
    </div>
  );
}