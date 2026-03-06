import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Helmet } from 'react-helmet-async';
import { motion } from 'framer-motion';
import { MapPin, Home, Calendar, Trees, Building2, ArrowRight, ChevronLeft, ChevronRight, Volume2, VolumeX } from 'lucide-react';
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
import EditableText from '@/components/EditableText';

const configurations = [
  {
    type: "2 BHK",
    price: "₹89 Lakhs*",
    size: "1164 - 1279 sq ft",
    link: "Anahata2BHK"
  },
  {
    type: "3 BHK 2T",
    price: "₹1.15 Cr*",
    size: "1511 - 1591 sq ft",
    link: "Anahata3BHK"
  },
];

const allConfigurations = [
  {
    type: "2 BHK",
    price: "₹89 Lakhs*",
    size: "1164 - 1279 sq ft",
    link: "Anahata2BHK"
  },
  {
    type: "3 BHK 2T",
    price: "₹1.15 Cr*",
    size: "1511 - 1591 sq ft",
    link: "Anahata3BHK"
  },
  {
    type: "3 BHK 3T",
    price: "₹1.3 Cr*",
    size: "1670 - 1758 sq ft",
    link: "Anahata3BHK"
  },
];

const AutoSlider = ({ items }) => {
  const [currentIndex, setCurrentIndex] = useState(0);
  
  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentIndex((prev) => (prev + 1) % items.length);
    }, 3000);
    return () => clearInterval(interval);
  }, [items.length]);
  
  return (
    <div className="relative overflow-hidden">
      <motion.div
        animate={{ x: `-${currentIndex * 100}%` }}
        transition={{ type: "spring", stiffness: 300, damping: 30 }}
        className="flex"
      >
        {items.map((config, index) => (
          <div key={config.type} className="w-full flex-shrink-0 px-2">
            <Link to={createPageUrl(config.link)}>
              <div className="bg-white p-4 rounded-xl shadow-md hover:shadow-xl transition-all text-center border-2 border-orange-100 h-full">
                <h3 className="text-lg font-semibold text-gray-800 mb-2">{config.type}</h3>
                <p className="text-2xl font-light text-orange-500 mb-1">{config.price}</p>
                <p className="text-gray-600 text-sm">{config.size}</p>
              </div>
            </Link>
          </div>
        ))}
      </motion.div>
    </div>
  );
};

const highlights = [
  { icon: Trees, number: "5 Acres", label: "Project Area" },
  { icon: Home, number: "440", label: "Families" },
  { icon: Building2, number: "5", label: "Towers" },
  { icon: Trees, number: "80%", label: "Open Space" },
  { icon: Home, number: "50+", label: "Amenities" },
  { icon: Building2, number: "20,000 sq ft", label: "Club House" },
];

const floorPlans = [
  { name: "Master Plan", image: "/anahata/images/floor-plans/master-plan.png", desc: "The complete project layout includes all five towers, open spaces, and amenities" },
  { name: "Block A", image: "/anahata/images/floor-plans/block-a.png", desc: "Floor plans featuring premium 2BHK and 3BHK apartment layouts" },
  { name: "Block B", image: "/anahata/images/floor-plans/block-b.png", desc: "Thoughtfully designed apartments with modern floor plans and function" },
  { name: "Block C", image: "/anahata/images/floor-plans/block-c.png", desc: "Generous living spaces and optimal floor plans for your dream home" },
  { name: "Block D", image: "/anahata/images/floor-plans/block-d.png", desc: "Elegant design made for modern living in Bangalore" },
  { name: "Block E", image: "/anahata/images/floor-plans/block-e.png", desc: "Private apartments that offer space and comfort in perfect balance" },
];

const galleryImages = [
  { url: "/anahata/images/gallery/amphitheater-night.png", title: "Amphitheater & Evening Ambiance" },
  { url: "/anahata/images/gallery/sports-facilities-day.png", title: "Sports Facilities & Recreation" },
  { url: "/anahata/images/gallery/landscaped-pathway.png", title: "Landscaped Walkways & Gardens" },
  { url: "/anahata/images/gallery/aerial-complex-view.png", title: "Aerial View of Complete Complex" },
  { url: "/anahata/images/gallery/tennis-courts.png", title: "Tennis & Badminton Courts" },
  { url: "/anahata/images/gallery/kids-play-area.png", title: "Kids Play Area" },
];

export default function Anahata() {
  const [floorPlanIndex, setFloorPlanIndex] = useState(0);
  const [galleryIndex, setGalleryIndex] = useState(0);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [isMuted, setIsMuted] = useState(true);
  const playerRef = useRef(null);
  const iframeRef = useRef(null);

  useEffect(() => {
    window.scrollTo(0, 0);
    const interval = setInterval(() => {
      setCurrentIndex((prev) => (prev + 1) % configurations.length);
    }, 3000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    const initPlayer = () => {
      if (window.YT && window.YT.Player && iframeRef.current) {
        playerRef.current = new window.YT.Player(iframeRef.current, {
          events: {
            onReady: (event) => {
              event.target.mute();
              event.target.playVideo();
            },
          },
        });
      }
    };

    if (!window.YT) {
      const tag = document.createElement('script');
      tag.src = 'https://www.youtube.com/iframe_api';
      const firstScriptTag = document.getElementsByTagName('script')[0];
      firstScriptTag.parentNode.insertBefore(tag, firstScriptTag);
      window.onYouTubeIframeAPIReady = initPlayer;
    } else {
      initPlayer();
    }
  }, []);

  const toggleMute = useCallback(() => {
    if (playerRef.current) {
      if (isMuted) {
        playerRef.current.unMute();
      } else {
        playerRef.current.mute();
      }
      setIsMuted(!isMuted);
    }
  }, [isMuted]);

  return (
    <div className="bg-white min-h-screen pt-24">
      <Helmet>
        <title>Anahata by Ishtika Homes | Premium 2BHK & 3BHK Apartments in Whitefield, Bangalore</title>
        <meta name="description" content="Anahata by Ishtika Homes - Premium 2BHK & 3BHK Vaastu-compliant apartments in Whitefield, Bangalore. 5 acres, 440 families, 5 towers, 80% open space, 50+ amenities. Starting at ₹89 Lakhs." />
        <meta name="keywords" content="Anahata, Ishtika Homes, 2BHK apartments Whitefield, 3BHK apartments Whitefield, luxury apartments Bangalore, Vaastu compliant homes Whitefield, gated community Whitefield, Soukya Road apartments, premium apartments Bangalore" />
        <link rel="canonical" href="https://www.ishtikahomes.com/anahata" />
        <meta property="og:title" content="Anahata by Ishtika Homes | Premium Apartments in Whitefield, Bangalore" />
        <meta property="og:description" content="Premium 2BHK & 3BHK Vaastu-compliant apartments in Whitefield, Bangalore. 5 acres, 440 families, 80% open space, 50+ amenities. Starting at ₹89 Lakhs." />
        <meta property="og:url" content="https://www.ishtikahomes.com/anahata" />
        <meta property="og:type" content="website" />
        <meta property="og:image" content="https://www.ishtikahomes.com/assets/img/aerial-view-bg.jpg" />
        <meta name="twitter:title" content="Anahata by Ishtika Homes | Premium Apartments in Whitefield, Bangalore" />
        <meta name="twitter:description" content="Premium 2BHK & 3BHK Vaastu-compliant apartments in Whitefield, Bangalore. 5 acres, 440 families, 80% open space, 50+ amenities." />
      </Helmet>
      <Navbar />
      <WhatsAppButton message="Hi, I'm interested in Anahata project. Can you please provide more details?" />

      {/* Hero Video Section */}
      <section className="relative h-[80vh] overflow-hidden">
        <div className="absolute inset-0 w-full h-full overflow-hidden">
          <iframe
            ref={iframeRef}
            className="absolute"
            id="anahata-yt-player"
            src="https://www.youtube.com/embed/_UE-muzzbz4?autoplay=1&mute=1&loop=1&playlist=_UE-muzzbz4&controls=0&showinfo=0&rel=0&modestbranding=1&enablejsapi=1&origin=https://www.ishtikahomes.com"
            title="Anahata Project"
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
        </div>
        <div className="absolute inset-0 bg-black/50" />

        {/* Mute/Unmute Button - Top Left */}
        <button
          onClick={toggleMute}
          className="absolute top-6 left-6 z-10 w-10 h-10 md:w-12 md:h-12 rounded-full bg-white/20 backdrop-blur-sm border border-white/30 flex items-center justify-center text-white hover:bg-white/30 transition-all duration-300"
          aria-label={isMuted ? "Unmute video" : "Mute video"}
        >
          {isMuted ? <VolumeX className="w-4 h-4 md:w-5 md:h-5" /> : <Volume2 className="w-4 h-4 md:w-5 md:h-5" />}
        </button>
        <div className="relative h-full flex flex-col items-center justify-between text-center px-4 py-12 md:py-0 md:px-6 md:justify-center">
          <div className="flex-shrink-0 mt-8 md:mt-0">
            <motion.div
              initial={{ opacity: 0, y: 30 }}
              animate={{ opacity: 1, y: 0 }}
            >
              <EditableText
                textKey="anahata-hero-title"
                defaultContent="Anahata"
                as="h1"
                className="text-3xl sm:text-4xl md:text-6xl font-light text-white mb-2 md:mb-4"
              />
            </motion.div>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
            >
              <EditableText
                textKey="anahata-hero-subtitle"
                defaultContent="2BHK & 3BHK Luxury Gated Community Apartments"
                as="p"
                className="text-sm sm:text-lg md:text-xl text-white/90 mb-1 md:mb-2"
              />
            </motion.div>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="flex items-center justify-center gap-1.5 text-orange-400 mb-2 md:mb-6"
            >
              <MapPin className="w-3.5 h-3.5 md:w-5 md:h-5" />
              <EditableText
                textKey="anahata-hero-location"
                defaultContent="@ Soukya Road, Whitefield, Bengaluru"
                as="span"
                className="text-xs md:text-base"
              />
            </motion.div>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.4 }}
            >
              <EditableText
                textKey="anahata-hero-offer"
                defaultContent="No Pre-EMI till possession (March 2028)"
                as="p"
                className="text-white text-xs md:text-sm md:mb-8"
              />
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

          {/* Configurations inside Hero - Desktop only */}
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.6 }}
            className="hidden md:block max-w-4xl mx-auto w-full"
          >
            <div className="grid grid-cols-3 gap-4">
              {allConfigurations.map((config, index) => (
                <motion.div
                  key={config.type}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.7 + index * 0.1 }}
                >
                  <Link to={createPageUrl(config.link)}>
                    <div className="backdrop-blur-md bg-white/10 p-6 rounded-xl shadow-lg hover:shadow-2xl transition-all text-center cursor-pointer border-2 border-white/30 hover:border-white h-full">
                      <h3 className="text-xl font-semibold text-white mb-2">{config.type}</h3>
                      <p className="text-2xl font-light text-white mb-1">{config.price}</p>
                      <p className="text-white/90 text-sm">{config.size}</p>
                    </div>
                  </Link>
                </motion.div>
              ))}
            </div>
          </motion.div>
        </div>
      </section>

      {/* Configurations Grid - Mobile only (2 columns) */}
      <section className="md:hidden py-4 px-4 bg-gray-50">
        <div className="max-w-4xl mx-auto">
          <div className="grid grid-cols-2 gap-3">
            {configurations.map((config) => (
              <Link key={config.type} to={createPageUrl(config.link)}>
                <div className="bg-white p-4 rounded-xl shadow-md hover:shadow-xl transition-all text-center border-2 border-orange-100 h-full">
                  <h3 className="text-base font-semibold text-gray-800 mb-2">{config.type}</h3>
                  <p className="text-xl font-light text-orange-500 mb-1">{config.price}</p>
                  <p className="text-gray-600 text-xs">{config.size}</p>
                </div>
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* About Section */}
      <section className="min-h-screen md:min-h-0 py-6 md:py-16 px-4 sm:px-6 flex items-center">
        <div className="max-w-6xl mx-auto grid md:grid-cols-2 gap-8 md:gap-12 items-center w-full">
          <motion.div
            initial={{ opacity: 0, x: -40 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
          >
            <EditableImage
              imageKey="anahata-about-aerial"
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
            <EditableText
              textKey="anahata-about-title"
              defaultContent="About Anahata"
              as="h2"
              className="text-3xl sm:text-4xl font-semibold text-gray-900 mb-6 sm:mb-8"
            />
            <EditableText
              textKey="anahata-about-para1"
              defaultContent="Ishtika's Anahata Towers offers thoughtfully configured 2BHK in Whitefield, and spacious 3 BHK in Whitefield, Bangalore, all within a beautifully landscaped 5-acre gated community that fits perfectly into the modern urban lifestyle. Offering over 80% of its land as open green space, the community consists of five carefully planned residential towers which house 440 exclusive families, creating an environment that promotes community living in one of the fastest-growing neighborhoods of Bangalore."
              as="p"
              multiline
              className="text-gray-700 text-base sm:text-lg leading-relaxed mb-4 sm:mb-6"
            />
            <EditableText
              textKey="anahata-about-para2"
              defaultContent="Anahata Towers is located near Soukya Road, which allows easy access to major business districts – ITPL, Marathahalli, and Outer Ring Road. The combination of contemporary layouts, quality amenities, and abundant green space make residents enjoy an unprecedented green living experience rarely found in urban Bangalore."
              as="p"
              multiline
              className="text-gray-700 text-base sm:text-lg leading-relaxed mb-6 sm:mb-8"
            />
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
      <section className="py-5 md:py-16 px-4 sm:px-6 bg-gray-50">
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

      {/* Floor Plans */}
      <section className="min-h-screen md:min-h-0 py-6 md:py-16 px-4 sm:px-6 flex items-center">
        <div className="max-w-7xl mx-auto w-full">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-10 sm:mb-16"
          >
            <h2 className="text-3xl sm:text-4xl md:text-5xl font-semibold text-gray-900 mb-4 sm:mb-6 px-2">
              2BHK & 3BHK Block & Floor Plans
            </h2>
            <p className="text-gray-700 text-base sm:text-lg px-4">
              Each apartment is thoughtfully designed with optimal space utilization
            </p>
          </motion.div>

          {/* Mobile Slider */}
          <div className="sm:hidden relative mb-6">
            <div className="overflow-hidden">
              <motion.div
                animate={{ x: `-${floorPlanIndex * 100}%` }}
                transition={{ type: "spring", stiffness: 300, damping: 30 }}
                className="flex"
              >
                {floorPlans.map((plan) => (
                  <div key={plan.name} className="w-full flex-shrink-0 px-2">
                    <div className="bg-white border border-gray-200 rounded-xl overflow-hidden shadow-md">
                      <div className="w-full h-64 overflow-hidden bg-gray-50">
                        <EditableImage
                          imageKey={`anahata-floor-plan-${plan.name.toLowerCase().replace(' ', '-')}`}
                          src={plan.image}
                          alt={plan.name}
                          className="w-full h-64 object-cover"
                        />
                      </div>
                      <div className="p-4">
                        <h3 className="text-xl font-semibold text-gray-900 mb-2">{plan.name}</h3>
                        <p className="text-sm text-gray-700 leading-relaxed">{plan.desc}</p>
                      </div>
                    </div>
                  </div>
                ))}
              </motion.div>
            </div>
            <button
              onClick={() => setFloorPlanIndex(Math.max(0, floorPlanIndex - 1))}
              disabled={floorPlanIndex === 0}
              className="absolute left-0 top-1/2 -translate-y-1/2 bg-white/90 p-2 rounded-full shadow-lg disabled:opacity-50"
            >
              <ChevronLeft className="w-6 h-6" />
            </button>
            <button
              onClick={() => setFloorPlanIndex(Math.min(floorPlans.length - 1, floorPlanIndex + 1))}
              disabled={floorPlanIndex === floorPlans.length - 1}
              className="absolute right-0 top-1/2 -translate-y-1/2 bg-white/90 p-2 rounded-full shadow-lg disabled:opacity-50"
            >
              <ChevronRight className="w-6 h-6" />
            </button>
            <div className="flex justify-center gap-2 mt-4">
              {floorPlans.map((_, idx) => (
                <button
                  key={idx}
                  onClick={() => setFloorPlanIndex(idx)}
                  className={`w-2 h-2 rounded-full transition-all ${
                    idx === floorPlanIndex ? 'bg-orange-500 w-6' : 'bg-gray-300'
                  }`}
                />
              ))}
            </div>
          </div>

          {/* Desktop Grid */}
          <div className="hidden sm:grid sm:grid-cols-2 lg:grid-cols-3 gap-6 sm:gap-8">
            {floorPlans.map((plan, index) => (
              <motion.div
                key={plan.name}
                initial={{ opacity: 0, y: 30 }}
                whileInView={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1 }}
                viewport={{ once: true }}
                className="bg-white border border-gray-200 rounded-xl overflow-hidden shadow-md hover:shadow-xl transition-all cursor-pointer"
              >
                <div className="w-full h-64 sm:h-80 overflow-hidden bg-gray-50">
                  <EditableImage
                    imageKey={`anahata-floor-plan-${plan.name.toLowerCase().replace(' ', '-')}`}
                    src={plan.image}
                    alt={plan.name}
                    className="w-full h-64 sm:h-80 object-cover"
                  />
                </div>
                <div className="p-4 sm:p-6">
                  <h3 className="text-xl sm:text-2xl font-semibold text-gray-900 mb-2 sm:mb-3">{plan.name}</h3>
                  <p className="text-sm sm:text-base text-gray-700 leading-relaxed">{plan.desc}</p>
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

      {/* Gallery */}
      <section className="py-5 md:py-16 px-4 sm:px-6 bg-gray-50">
        <div className="max-w-7xl mx-auto w-full">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-10 sm:mb-16"
          >
            <h2 className="text-3xl sm:text-4xl md:text-5xl font-semibold text-gray-900 mb-4 sm:mb-6 px-2">
              Project Gallery
            </h2>
            <p className="text-gray-700 text-base sm:text-lg mt-2 px-4">
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
                        imageKey={`anahata-gallery-${index}`}
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
                className="group relative w-full h-56 sm:h-64 overflow-hidden rounded-xl shadow-md hover:shadow-xl transition-all"
              >
                <EditableImage
                  imageKey={`anahata-gallery-${index}`}
                  src={image.url}
                  alt={image.title}
                  className="w-full h-full object-cover transition-transform duration-500 group-hover:scale-110"
                />
                <div className="absolute inset-0 bg-gradient-to-t from-black/70 to-transparent opacity-0 group-hover:opacity-100 transition-opacity">
                  <div className="absolute bottom-4 left-4 right-4">
                    <p className="text-white font-medium text-xs sm:text-sm">{image.title}</p>
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
            className="relative overflow-hidden min-h-[350px] md:min-h-[500px] flex items-center justify-start"
          >
            {/* Background Image */}
            <EditableBackgroundImage
              imageKey="anahata-cta-background"
              src="/assets/img/aerial-view-bg.jpg"
              className="absolute inset-0 bg-cover bg-center"
            >
              <div className="absolute inset-0 bg-black/60" />
            </EditableBackgroundImage>

            {/* Content */}
            <div className="relative px-8 md:px-16 py-16 w-full text-center">
              <h2 className="text-3xl md:text-4xl font-light text-white mb-8 leading-tight">
                Ready to Make Anahata Your Home?
              </h2>
              <p className="text-white/90 text-lg mb-8 max-w-2xl mx-auto">
                Schedule a visit and experience the lifestyle you deserve
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