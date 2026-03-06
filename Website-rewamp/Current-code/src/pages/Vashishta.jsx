import React, { useState, useEffect } from 'react';
import { Helmet } from 'react-helmet-async';
import { motion } from 'framer-motion';
import { MapPin, Home, Building2, Trees, ArrowRight, ChevronLeft, ChevronRight, CheckCircle, Phone } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';

const highlights = [
  { icon: Building2, number: "1.25 Lakh", label: "Sqft Total Area" },
  { icon: Building2, number: "2", label: "Lifts" },
  { icon: CheckCircle, number: "100%", label: "Vaastu" },
  { icon: Trees, number: "EV", label: "Charging" },
];

const salientFeatures = [
  "No Common Walls",
  "General Spatial Floor Plan",
  "100% Vaastu Compliant",
  "Close To Nice Road",
  "Broad & Wider Balconies",
  "Prime Location",
  "100% Natural Light & Ventilation",
  "Close to Proposed Gottigere Metro Station",
  "Adjacent to BG Road & Kanakapura Road Connecting Corridor",
  "Two Levels of Basement with Spacious Car Parking",
];

const amenities = [
  "Swimming Pool", "Yoga Hall", "Indoor Badminton Court", "Kids Play Area",
  "Gymnasium", "Outdoor Sitting Spaces", "Party Hall", "Landscaped Garden",
  "Convenience Store", "Vertical Green Wall", "Electric Charging Points for EV",
];

const galleryImages = [
  { url: "/assets/img/all-images/vashishta-img1.png", title: "Vashishta Exterior" },
  { url: "/assets/img/all-images/vashishta-img2.png", title: "Vashishta View" },
  { url: "/assets/img/all-images/vashishta-img3.png", title: "Vashishta Amenities" },
  { url: "/assets/img/all-images/vashishta-img4.png", title: "Vashishta Interior" },
  { url: "/assets/img/all-images/vashishta-img5.png", title: "Vashishta Landscape" },
  { url: "/assets/img/all-images/vashishta-img6.png", title: "Vashishta Facilities" },
];

const specifications = [
  { title: "Structure", desc: "RCC framed structure" },
  { title: "Windows", desc: "UPVC sliding windows with mosquito mesh" },
  { title: "Painting", desc: "Premium emulsion paint (internal); premium paint (exterior)" },
  { title: "Plumbing & Sanitary", desc: "Hindware/Cera or equivalent" },
  { title: "Security", desc: "CCTV surveillance" },
  { title: "Lifts", desc: "2 lifts of adequate capacity" },
  { title: "Power Backup", desc: "Generator for lift, common areas; 0.5kVA to each flat" },
  { title: "Electrical", desc: "Fire resistant PVC insulated copper wires; Havells/Salzer/VGuard switches" },
  { title: "Flooring", desc: "Superior quality vitrified tiles; anti-skid ceramic/vitrified tiles for toilets" },
  { title: "Doors", desc: "Teak wood frame (main); hardwood frame (internal)" },
  { title: "Kitchen", desc: "Polished granite platform, stainless steel sink, 2ft wall dado" },
];

export default function Vashishta() {
  const [galleryIndex, setGalleryIndex] = useState(0);

  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  return (
    <div className="bg-white min-h-screen pt-24">
      <Helmet>
        <title>Vashishta by Ishtika Homes | Premium Apartments in JP Nagar, Bangalore</title>
        <meta name="description" content="Vashishta by Ishtika Homes - Completed premium 2BHK & 3BHK apartments in JP Nagar, Bangalore. 1.25 Lakh sqft, 100% Vaastu compliant, EV charging, close to Nice Road and Gottigere Metro Station." />
        <link rel="canonical" href="https://www.ishtikahomes.com/vashishta" />
        <meta property="og:title" content="Vashishta by Ishtika Homes | Premium Apartments in JP Nagar, Bangalore" />
        <meta property="og:description" content="Completed premium 2BHK & 3BHK apartments in JP Nagar, Bangalore. 1.25 Lakh sqft, 100% Vaastu compliant with modern amenities." />
        <meta property="og:url" content="https://www.ishtikahomes.com/vashishta" />
        <meta property="og:type" content="website" />
        <meta name="twitter:title" content="Vashishta by Ishtika Homes | Premium Apartments in JP Nagar" />
        <meta name="twitter:description" content="Completed premium 2BHK & 3BHK apartments in JP Nagar, Bangalore. 1.25 Lakh sqft, 100% Vaastu compliant." />
      </Helmet>
      <Navbar />
      <WhatsAppButton message="Hi, I'm interested in Vashishta project. Can you please provide more details?" />

      {/* Hero Section */}
      <section className="relative h-[60vh] md:h-[70vh] overflow-hidden">
        <div className="absolute inset-0">
          <img
            src="/assets/img/all-images/vashishta-img1.png"
            alt="Vashishta Project"
            className="w-full h-full object-cover"
          />
        </div>
        <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-black/40 to-black/20" />
        <div className="relative h-full flex flex-col items-center justify-center text-center px-4">
          <motion.h1
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            className="text-4xl md:text-6xl font-light text-white mb-4"
          >
            Vashishta
          </motion.h1>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-lg md:text-xl text-white/90 mb-2"
          >
            Premium Apartments
          </motion.p>
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 }}
            className="flex items-center gap-2 text-orange-400 mb-6"
          >
            <MapPin className="w-5 h-5" />
            <span>JP Nagar, Bengaluru</span>
          </motion.div>
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4 }}
          >
            <Link to={createPageUrl("Contact")}>
              <Button size="lg" style={{ backgroundColor: '#FF8C00', color: 'white' }} className="hover:opacity-90 px-8 py-6 text-base">
                Enquire Now
                <ArrowRight className="w-5 h-5 ml-2" />
              </Button>
            </Link>
          </motion.div>
        </div>
      </section>

      {/* Highlights */}
      <section className="py-8 px-4 bg-gray-50">
        <div className="max-w-6xl mx-auto">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {highlights.map((item, index) => (
              <motion.div
                key={item.label}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1 }}
                viewport={{ once: true }}
                className="text-center bg-white p-6 rounded-2xl shadow-md"
              >
                <item.icon className="w-8 h-8 text-orange-500 mx-auto mb-3" />
                <div className="text-2xl font-light text-gray-800">{item.number}</div>
                <div className="text-sm text-gray-500">{item.label}</div>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* About */}
      <section className="py-12 md:py-20 px-4">
        <div className="max-w-6xl mx-auto grid md:grid-cols-2 gap-12 items-center">
          <motion.div initial={{ opacity: 0, x: -40 }} whileInView={{ opacity: 1, x: 0 }} viewport={{ once: true }}>
            <img src="/assets/img/all-images/vashishta-img2.png" alt="Vashishta Project" className="rounded-2xl shadow-xl w-full" />
          </motion.div>
          <motion.div initial={{ opacity: 0, x: 40 }} whileInView={{ opacity: 1, x: 0 }} viewport={{ once: true }}>
            <h2 className="text-3xl font-semibold text-gray-900 mb-6">About Vashishta</h2>
            <div className="space-y-4 text-gray-600 leading-relaxed">
              <p>
                Vashishta, located in South Bengaluru's Anjanapura BDA layout, offers premium apartments amidst greenery. With excellent connectivity to schools, shopping, hospitals, metro stations, and the Kanakapura-Bannerghatta corridor, it combines nature, modern infrastructure, and affordability for a blissful living experience.
              </p>
            </div>
          </motion.div>
        </div>
      </section>

      {/* Salient Features */}
      <section className="py-12 md:py-20 px-4 bg-gray-50">
        <div className="max-w-6xl mx-auto">
          <motion.div initial={{ opacity: 0, y: 30 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="text-center mb-12">
            <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">Why Choose Vashishta</p>
            <h2 className="text-3xl md:text-4xl font-light text-gray-800">Salient Features</h2>
          </motion.div>
          <div className="grid md:grid-cols-2 gap-4 max-w-4xl mx-auto">
            {salientFeatures.map((feature, index) => (
              <motion.div key={feature} initial={{ opacity: 0, x: index % 2 === 0 ? -20 : 20 }} whileInView={{ opacity: 1, x: 0 }} transition={{ delay: index * 0.05 }} viewport={{ once: true }} className="flex items-center gap-3 bg-white p-4 rounded-xl shadow-sm">
                <CheckCircle className="w-5 h-5 text-orange-500 flex-shrink-0" />
                <span className="text-gray-700">{feature}</span>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* Amenities */}
      <section className="py-12 md:py-20 px-4">
        <div className="max-w-6xl mx-auto">
          <motion.div initial={{ opacity: 0, y: 30 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="text-center mb-12">
            <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">Premium Living</p>
            <h2 className="text-3xl md:text-4xl font-light text-gray-800">Amenities</h2>
          </motion.div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {amenities.map((amenity, index) => (
              <motion.div key={amenity} initial={{ opacity: 0, scale: 0.9 }} whileInView={{ opacity: 1, scale: 1 }} transition={{ delay: index * 0.05 }} viewport={{ once: true }} className="bg-orange-50 p-5 rounded-xl text-center">
                <p className="text-gray-800 font-medium text-sm">{amenity}</p>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* Gallery */}
      <section className="py-12 md:py-20 px-4 bg-gray-50">
        <div className="max-w-6xl mx-auto">
          <motion.div initial={{ opacity: 0, y: 30 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="text-center mb-12">
            <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">Visual Tour</p>
            <h2 className="text-3xl md:text-4xl font-light text-gray-800">Project Gallery</h2>
          </motion.div>
          <div className="relative">
            <div className="aspect-[16/9] rounded-2xl overflow-hidden shadow-xl">
              <img src={galleryImages[galleryIndex].url} alt={galleryImages[galleryIndex].title} className="w-full h-full object-cover" />
            </div>
            <div className="absolute inset-0 flex items-center justify-between px-4">
              <Button variant="ghost" size="icon" onClick={() => setGalleryIndex((p) => (p - 1 + galleryImages.length) % galleryImages.length)} className="bg-white/80 hover:bg-white rounded-full shadow-lg">
                <ChevronLeft className="w-6 h-6" />
              </Button>
              <Button variant="ghost" size="icon" onClick={() => setGalleryIndex((p) => (p + 1) % galleryImages.length)} className="bg-white/80 hover:bg-white rounded-full shadow-lg">
                <ChevronRight className="w-6 h-6" />
              </Button>
            </div>
            <p className="text-center mt-4 text-gray-600">{galleryImages[galleryIndex].title}</p>
          </div>
          <div className="grid grid-cols-3 md:grid-cols-6 gap-2 mt-6">
            {galleryImages.map((img, i) => (
              <button key={i} onClick={() => setGalleryIndex(i)} className={`aspect-square rounded-lg overflow-hidden border-2 transition-all ${i === galleryIndex ? 'border-orange-500 shadow-lg' : 'border-transparent opacity-70 hover:opacity-100'}`}>
                <img src={img.url} alt={img.title} className="w-full h-full object-cover" />
              </button>
            ))}
          </div>
        </div>
      </section>

      {/* Specifications */}
      <section className="py-12 md:py-20 px-4">
        <div className="max-w-6xl mx-auto">
          <motion.div initial={{ opacity: 0, y: 30 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="text-center mb-12">
            <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">Quality Standards</p>
            <h2 className="text-3xl md:text-4xl font-light text-gray-800">Specifications</h2>
          </motion.div>
          <div className="grid md:grid-cols-2 gap-4 max-w-4xl mx-auto">
            {specifications.map((spec, index) => (
              <motion.div key={spec.title} initial={{ opacity: 0, y: 20 }} whileInView={{ opacity: 1, y: 0 }} transition={{ delay: index * 0.03 }} viewport={{ once: true }} className="bg-gray-50 p-4 rounded-xl">
                <h4 className="font-semibold text-gray-800 text-sm mb-1">{spec.title}</h4>
                <p className="text-gray-600 text-sm">{spec.desc}</p>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* Location */}
      <section className="py-12 md:py-20 px-4 bg-gray-50">
        <div className="max-w-6xl mx-auto">
          <motion.div initial={{ opacity: 0, y: 30 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="text-center mb-12">
            <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">Strategically Located</p>
            <h2 className="text-3xl md:text-4xl font-light text-gray-800">Location</h2>
          </motion.div>
          <div className="bg-white p-6 rounded-2xl shadow-md">
            <div className="flex items-start gap-3 mb-4">
              <MapPin className="w-5 h-5 text-orange-500 flex-shrink-0 mt-1" />
              <p className="text-gray-700">Anjanapura BDA Layout, JP Nagar 9th Phase, Bengaluru - 560 083</p>
            </div>
            <div className="grid md:grid-cols-2 gap-4 mt-6">
              <div className="space-y-2 text-sm text-gray-600">
                <p className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-orange-500" /> Close to Nice Road</p>
                <p className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-orange-500" /> Close to Proposed Gottigere Metro Station</p>
                <p className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-orange-500" /> Adjacent to BG Road & Kanakapura Road</p>
              </div>
              <div className="space-y-2 text-sm text-gray-600">
                <p className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-orange-500" /> Schools & Hospitals Nearby</p>
                <p className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-orange-500" /> Shopping & Entertainment</p>
                <p className="flex items-center gap-2"><CheckCircle className="w-4 h-4 text-orange-500" /> Two Levels of Basement Parking</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-12 md:py-20 px-4">
        <div className="max-w-4xl mx-auto text-center">
          <motion.div initial={{ opacity: 0, y: 30 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} className="bg-gradient-to-br from-orange-500 to-orange-600 rounded-3xl p-8 md:p-12 text-white shadow-2xl">
            <h2 className="text-3xl md:text-4xl font-light mb-4">Interested in Vashishta?</h2>
            <p className="text-lg mb-8 text-orange-50">Connect with our team for pricing, floor plans, and site visit scheduling.</p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Link to={createPageUrl("Contact")}>
                <Button size="lg" className="bg-white text-orange-600 hover:bg-orange-50 border-0 w-full sm:w-auto">
                  Enquire Now <ArrowRight className="w-5 h-5 ml-2" />
                </Button>
              </Link>
              <a href="tel:+917338628777">
                <Button size="lg" className="bg-white text-orange-600 hover:bg-orange-50 border-0 w-full sm:w-auto">
                  <Phone className="w-5 h-5 mr-2" /> Call: +91 7338628777
                </Button>
              </a>
            </div>
          </motion.div>
        </div>
      </section>

      <Footer />
    </div>
  );
}
