import React, { useEffect } from 'react';
import { motion } from 'framer-motion';
import { Target, CheckCircle, ArrowRight, Building2, Users, MapPin, Home, Award, Rocket, Star } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';
import EditableImage from '@/components/EditableImage';

const milestones = [
  { year: '2014', title: 'Founded Ishtika Homes', icon: Rocket },
  { year: '2015', title: 'Launched Advaitha', icon: Home },
  { year: '2017', title: 'Delivered White Pearl', icon: Award },
  { year: '2019', title: '400K+ Sq. Ft. Delivered', icon: MapPin },
  { year: '2022', title: 'Anahata & Krishna Launch', icon: Building2 },
  { year: '2024', title: '17.72 Lakh Sq. Ft. Delivered', icon: Star },
  { year: '2026', title: '3 Upcoming Projects in Pipeline', icon: Rocket },
];

export default function About() {
  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  return (
    <div className="bg-white min-h-screen pt-24">
      <Navbar />
      <WhatsAppButton />

      {/* Hero Section with Stats */}
      <section className="py-12 md:py-20 px-4 bg-gradient-to-b from-orange-50 to-white">
        <div className="max-w-4xl mx-auto text-center">
          <motion.h1
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            className="text-4xl md:text-6xl font-light text-gray-800 mb-4"
          >
            Building Dreams, <span className="text-orange-500">One At A Time!</span>
          </motion.h1>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-lg text-gray-600 mb-10"
          >
            Vaastu Compliant Natural Sustainable Residences
          </motion.p>

          <div className="grid grid-cols-3 gap-4 md:gap-8 max-w-2xl mx-auto">
            {[
              { number: "12+", label: "Years Of Experience" },
              { number: "12+", label: "Established Projects" },
              { number: "5+", label: "Ongoing Projects" },
            ].map((stat, index) => (
              <motion.div
                key={stat.label}
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.3 + index * 0.1 }}
                className="text-center bg-white p-4 md:p-6 rounded-2xl shadow-md"
              >
                <div className="text-3xl md:text-5xl font-light text-orange-500 mb-1">{stat.number}</div>
                <div className="text-xs md:text-sm text-gray-600 uppercase tracking-wider">{stat.label}</div>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* Main Story */}
      <section className="py-6 md:py-20 px-4">
        <div className="max-w-6xl mx-auto grid md:grid-cols-2 gap-12 items-center">
          <motion.div
            initial={{ opacity: 0, x: -40 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
          >
            <EditableImage
              imageKey="about-page-main"
              src="/images/anahata-side-elevation.jpg"
              alt="Vastu-compliant residential design"
              className="rounded-2xl shadow-xl w-full"
            />
          </motion.div>
          <motion.div
            initial={{ opacity: 0, x: 40 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
          >
            <h2 className="text-3xl font-light text-gray-800 mb-6">
              About Ishtika Homes
            </h2>
            <div className="space-y-4 text-gray-600 leading-relaxed">
              <p>
                The name "Ishtika" is derived from Sanskrit, meaning "brick" – the fundamental building block that represents strength, stability, and the foundation of every great structure. This ancient word perfectly encapsulates our philosophy: to build not just houses, but homes that stand the test of time.
              </p>
              <p>
                Founded over 12 years ago with a vision to transform Bangalore's residential landscape, Ishtika Homes has grown from a passionate dream into a trusted name in real estate development. Our journey began with a simple yet powerful belief – that every family deserves a home that combines modern amenities with traditional values, quality craftsmanship with affordability, and contemporary design with Vaastu compliance.
              </p>
              <p>
                Over the years, we've delivered approximately 17.72 lakh square feet of premium residential space, creating homes for hundreds of families across Bangalore. Each project is a testament to our unwavering commitment to quality, sustainability, and customer satisfaction. From completed landmarks like Advaitha and White Pearl to our ongoing developments like Anahata and Krishna, every Ishtika project tells a story of dedication, integrity, and excellence.
              </p>
            </div>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              className="mt-6 md:hidden"
            >
              <Link to={createPageUrl("Contact")}>
                <Button 
                  size="lg" 
                  style={{ backgroundColor: '#FF8C00', color: 'white' }}
                  className="w-full hover:opacity-90 text-base py-6"
                >
                  Get in Touch
                  <ArrowRight className="w-5 h-5 ml-2" />
                </Button>
              </Link>
            </motion.div>
          </motion.div>
        </div>
      </section>

      {/* Our Journey Timeline */}
      <section className="py-6 md:py-14 px-4 bg-gray-50">
        <div className="max-w-4xl mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-10"
          >
            <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-3 font-semibold">
              Our Journey
            </p>
            <h2 className="text-3xl md:text-4xl font-light text-gray-800">
              A Decade of Building Trust
            </h2>
          </motion.div>

          {/* Timeline */}
          <div className="relative">
            {/* Vertical line - desktop center */}
            <div className="hidden md:block absolute left-1/2 top-0 bottom-0 w-0.5 bg-orange-200 -translate-x-1/2" />
            {/* Vertical line - mobile left */}
            <div className="md:hidden absolute left-5 top-0 bottom-0 w-0.5 bg-orange-200" />

            {milestones.map((milestone, index) => {
              const Icon = milestone.icon;
              const isLeft = index % 2 === 0;

              return (
                <motion.div
                  key={milestone.year}
                  initial={{ opacity: 0, y: 30 }}
                  whileInView={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.5, delay: index * 0.08 }}
                  viewport={{ once: true }}
                  className={`relative flex items-center mb-6 last:mb-0 md:mb-8 ${
                    isLeft ? 'md:flex-row' : 'md:flex-row-reverse'
                  }`}
                >
                  {/* Mobile layout */}
                  <div className="md:hidden flex items-center gap-3 w-full">
                    <div className="relative z-10 flex-shrink-0 w-10 h-10 rounded-full bg-gradient-to-br from-orange-500 to-orange-600 flex items-center justify-center shadow-md">
                      <Icon className="w-4 h-4 text-white" />
                    </div>
                    <div className="flex-1 bg-white rounded-lg px-4 py-3 shadow-sm border border-gray-100">
                      <span className="text-xs font-bold text-orange-500">{milestone.year}</span>
                      <h3 className="text-sm font-semibold text-gray-800">{milestone.title}</h3>
                    </div>
                  </div>

                  {/* Desktop layout - content card */}
                  <div className={`hidden md:block md:w-5/12 ${isLeft ? 'md:pr-8 md:text-right' : 'md:pl-8 md:text-left'}`}>
                    <div className="bg-white rounded-lg px-5 py-3 shadow-sm border border-gray-100 hover:shadow-md transition-shadow inline-block">
                      <span className="text-xs font-bold text-orange-500">{milestone.year}</span>
                      <h3 className="text-sm font-semibold text-gray-800">{milestone.title}</h3>
                    </div>
                  </div>

                  {/* Center icon (desktop) */}
                  <div className="hidden md:flex absolute left-1/2 -translate-x-1/2 z-10 w-10 h-10 rounded-full bg-gradient-to-br from-orange-500 to-orange-600 items-center justify-center shadow-md border-3 border-white">
                    <Icon className="w-4 h-4 text-white" />
                  </div>

                  {/* Spacer */}
                  <div className="hidden md:block md:w-5/12" />
                </motion.div>
              );
            })}
          </div>
        </div>
      </section>

      {/* Our Founders Section */}
      <section className="py-6 md:py-20 px-4 bg-gray-50">
        <div className="max-w-4xl mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-12"
          >
            <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">
              The Visionaries
            </p>
            <h2 className="text-3xl md:text-4xl font-light text-gray-800 mb-4">
              Our Founders
            </h2>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="bg-white rounded-3xl p-8 md:p-12 shadow-lg border border-gray-100"
          >
            <div className="text-center mb-8">
              <div className="inline-flex items-center gap-3 mb-4">
                <Users className="w-6 h-6 text-orange-500" />
                <h3 className="text-2xl md:text-3xl font-semibold text-gray-800">
                  Mr. Chiranjeevi Reddy & Mr. Vijay Reddy
                </h3>
              </div>
              <p className="text-sm uppercase tracking-widest text-orange-500 font-semibold">
                Co-Founders
              </p>
              <div className="w-20 h-1 bg-gradient-to-r from-orange-400 to-orange-600 mx-auto mt-4 rounded-full" />
            </div>

            <div className="space-y-5 text-gray-600 leading-relaxed text-base md:text-lg">
              <p>
                In <strong className="text-gray-800">2014</strong>, two visionaries, <strong className="text-orange-500">Mr. Chiranjeevi Reddy and Mr. Vijay Reddy</strong>, embarked on a remarkable journey with a shared dream – to transform the residential landscape of Bangalore. With a deep-rooted passion for real estate and an unwavering commitment to quality, they founded <strong className="text-orange-500">Ishtika Homes</strong> with the aim of creating living spaces that blend traditional values with modern luxury.
              </p>
              <p>
                Their vision was clear: to build homes that are not just structures of brick and mortar, but sanctuaries where families can thrive, grow, and create lasting memories. With a focus on <strong className="text-orange-500">Vastu compliance, sustainable practices, and affordable luxury</strong>, they set out to redefine what quality housing means in Bangalore's competitive real estate market.
              </p>
              <p>
                Over the past decade, their dedication has transformed Ishtika Homes from a modest venture into a trusted name in real estate development. Their leadership has been instrumental in delivering over <strong className="text-orange-500">17.72 lakh square feet</strong> of premium residential space, touching the lives of hundreds of families across Bangalore.
              </p>

              <div className="bg-orange-50 border-l-4 border-orange-500 rounded-r-xl p-6 my-8">
                <h4 className="text-lg font-bold text-gray-800 mb-3 flex items-center gap-2">
                  <Target className="w-5 h-5 text-orange-500" />
                  Their Aim
                </h4>
                <p className="text-gray-600 leading-relaxed mb-0">
                  To establish Ishtika Homes as one of Bangalore's most trusted real estate developers, recognized for exceptional craftsmanship, ethical practices, and customer-centric approach. They envision every project as an opportunity to exceed expectations and set new benchmarks in quality, sustainability, and innovation.
                </p>
              </div>

              <p>
                Supporting Mr. Chiranjeevi Reddy and Mr. Vijay Reddy is a dynamic team of <strong className="text-orange-500">young, enthusiastic professionals</strong> who share their passion for real estate excellence. This energetic team brings fresh perspectives, innovative ideas, and relentless dedication to every project. Together, they form a cohesive unit that combines the wisdom of experienced leadership with the vigor of youth, all working towards a common goal – building homes that stand the test of time and creating communities where life truly flourishes.
              </p>
            </div>
          </motion.div>
        </div>
      </section>

      {/* Vision & Mission */}
      <section className="py-6 md:py-20 px-4 bg-white">
        <div className="max-w-6xl mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-16"
          >
            <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">
              Our Vision & Mission
            </p>
            <h2 className="text-3xl md:text-4xl font-light text-gray-800">
              Building Tomorrow's Homes, Today
            </h2>
          </motion.div>

          <div className="grid md:grid-cols-2 gap-12">
            <motion.div
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              className="bg-white p-8 rounded-2xl shadow-md"
            >
              <Target className="w-12 h-12 text-orange-500 mb-4" />
              <h3 className="text-2xl font-semibold text-gray-800 mb-4">Our Vision</h3>
              <p className="text-gray-600 leading-relaxed">
                To be one of Bangalore's most trusted real estate developers, recognized for creating Vastu-compliant, spacious homes that seamlessly blend traditional wisdom with contemporary luxury. We envision a future where every family enjoys premium living experiences at affordable prices, setting new benchmarks in quality, sustainability, and customer satisfaction.
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              viewport={{ once: true }}
              className="bg-white p-8 rounded-2xl shadow-md"
            >
              <CheckCircle className="w-12 h-12 text-orange-500 mb-4" />
              <h3 className="text-2xl font-semibold text-gray-800 mb-4">Our Mission</h3>
              <p className="text-gray-600 leading-relaxed">
                To deliver cost-effective, eco-friendly, and Vastu-aligned residential spaces with exceptional amenities and superior craftsmanship. We are committed to providing spacious, luxurious homes that respect traditional values while embracing modern technology, ensuring every project reflects our promise of quality, integrity, and excellence in real estate development.
              </p>
            </motion.div>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-12 md:py-20 px-4">
        <div className="max-w-4xl mx-auto text-center">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="bg-gradient-to-br from-orange-500 to-orange-600 rounded-3xl p-8 md:p-12 text-white shadow-2xl"
          >
            <h2 className="text-3xl md:text-4xl font-light mb-4">
              Ready to Find Your Dream Home?
            </h2>
            <p className="text-lg mb-8 text-orange-50">
              Connect with our team to explore our projects and discover the perfect home for your family.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Link to={createPageUrl("Projects")}>
                <Button 
                  size="lg" 
                  variant="outline"
                  className="bg-white text-orange-600 hover:bg-orange-50 border-0 w-full sm:w-auto"
                >
                  View Projects
                </Button>
              </Link>
              <Link to={createPageUrl("Contact")}>
                <Button 
                  size="lg" 
                  className="bg-gray-900 hover:bg-gray-800 text-white w-full sm:w-auto"
                >
                  Contact Us
                  <ArrowRight className="w-5 h-5 ml-2" />
                </Button>
              </Link>
            </div>
          </motion.div>
        </div>
      </section>

      <Footer />
    </div>
  );
}