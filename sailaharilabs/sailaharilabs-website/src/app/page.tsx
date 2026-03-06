'use client';

import { motion, useInView } from 'framer-motion';
import { useRef } from 'react';
import Link from 'next/link';
import Image from 'next/image';

// Stats Data
const stats = [
  { value: '20+', label: 'Years Of Experience' },
  { value: '350+', label: 'Happy Customers' },
  { value: '98%', label: 'Customer Satisfaction' },
];

// Services Data
const services = [
  {
    number: '01',
    title: 'CUSTOM INSTRUMENTATION',
    description: 'Bespoke lab instruments tailored to your specific needs.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-M1416R.png?resize=600%2C600&ssl=1',
  },
  {
    number: '02',
    title: 'CALIBRATION SERVICES',
    description: 'Expert calibration services for accurate results.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-EzScope-101.png?resize=600%2C600&ssl=1',
  },
  {
    number: '03',
    title: 'TECHNICAL SUPPORT',
    description: 'Ongoing technical support for all products.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Electrophoresis-Power-Supply.png?resize=600%2C600&ssl=1',
  },
];

// Advantages Data
const advantages = [
  {
    title: 'Tailored Solutions',
    description: 'We provide customized laboratory instruments perfectly suited to meet the unique needs of each industry and client.',
  },
  {
    title: 'Expert Support',
    description: 'Our team of experts offers dedicated support to ensure you maximize the benefits of our products and achieve your goals.',
  },
  {
    title: 'Quality Assurance',
    description: 'We guarantee the highest quality standards in all our instruments to ensure reliability and precision for your research.',
  },
];


// Industries Data
const industries = [
  { name: 'R & D', image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/reseach.png?ssl=1', description: 'Research & Development' },
  { name: 'Healthcare', image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/health.png?ssl=1', description: 'Hospitals & Medical' },
  { name: 'Pharma', image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/pharma.png?ssl=1', description: 'Pharma / Bio Pharma' },
  { name: 'Bio Industrial', image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/bio-industry.jpg?ssl=1', description: 'Bio Industrial' },
  { name: 'Food & Beverage', image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/food.jpg?ssl=1', description: 'Quality Testing' },
  { name: 'Environmental', image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/envir.png?ssl=1', description: 'Environmental Testing' },
  { name: 'Bio Agriculture', image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/agri.png?ssl=1', description: 'Agriculture & Biotech' },
  { name: 'Forensic', image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/for.png?ssl=1', description: 'Forensic Sciences' },
];

// International Partners with logos
const partners = [
  { name: 'Alliance Bio Expertise', logo: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/08/9.jpg?ssl=1' },
  { name: 'Affinite Instruments', logo: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/08/10.jpg?ssl=1' },
  { name: 'Blue-Ray Biotech', logo: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/08/8.jpg?ssl=1' },
  { name: 'Orflo', logo: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/11/Orflo-logo-1.png?ssl=1' },
  { name: 'RWD Life Sciences', logo: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/08/4.jpg?ssl=1' },
  { name: 'Shenhua Science', logo: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/08/1.jpg?ssl=1' },
  { name: 'JH Bio Innovations', logo: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/08/6.jpg?ssl=1' },
];

// Certifications
const certifications = [
  { name: 'ISO 9001:2015', description: 'Quality Management' },
  { name: 'ISO 13485', description: 'Medical Devices' },
  { name: 'CE Certified', description: 'European Standards' },
  { name: 'BIS Certified', description: 'Bureau of Indian Standards' },
];

export default function HomePage() {
  return (
    <div className="overflow-hidden">
      {/* Hero Section */}
      <HeroSection />

      {/* Stats Section */}
      <StatsSection />

      {/* Trusted By / Clients Section */}
      <ClientsSection />

      {/* Who We Are Section */}
      <WhoWeAreSection />

      {/* Industries We Serve Section */}
      <IndustriesSection />

      {/* Services Section */}
      <ServicesSection />

      {/* Advantages Section */}
      <AdvantagesSection />

      {/* Certifications Section */}
      <CertificationsSection />
    </div>
  );
}

// Hero Section Component
function HeroSection() {
  return (
    <section className="relative min-h-screen flex items-center pt-24 pb-12 spotlight">
      <div className="max-w-7xl mx-auto px-6 lg:px-8 w-full">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          {/* Left Content */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8, ease: 'easeOut' }}
          >
            <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-8 rounded-full" />
            <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-white leading-tight mb-6">
              PRECISION LABORATORY{' '}
              <span className="gradient-text">INSTRUMENTS</span>{' '}
              FOR RESEARCH
            </h1>
            <p className="text-gray-400 text-lg mb-8 max-w-lg leading-relaxed">
              Discover high-quality laboratory instruments that cater to research,
              healthcare, and industry-specific applications for maximum reliability and precision.
            </p>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.4, duration: 0.6 }}
            >
              <Link href="/products">
                <button className="btn-primary">
                  Explore Products
                </button>
              </Link>
            </motion.div>
          </motion.div>

          {/* Right Images */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8, ease: 'easeOut', delay: 0.2 }}
            className="relative"
          >
            <div className="grid grid-cols-2 gap-4">
              <motion.div
                whileHover={{ scale: 1.02 }}
                className="relative h-64 rounded-2xl overflow-hidden glass-card"
              >
                <Image
                  src="https://images.unsplash.com/photo-1582719471384-894fbb16e074?w=600"
                  alt="Laboratory equipment"
                  fill
                  className="object-cover"
                />
              </motion.div>
              <motion.div
                whileHover={{ scale: 1.02 }}
                className="relative h-64 rounded-2xl overflow-hidden glass-card mt-8"
              >
                <Image
                  src="https://images.unsplash.com/photo-1576086213369-97a306d36557?w=600"
                  alt="Lab research"
                  fill
                  className="object-cover"
                />
              </motion.div>
            </div>

            {/* Floating decoration */}
            <div className="absolute -top-10 -right-10 w-40 h-40 bg-gradient-to-br from-[#f97066]/20 to-transparent rounded-full blur-3xl" />
            <div className="absolute -bottom-10 -left-10 w-32 h-32 bg-gradient-to-br from-[#fb923c]/20 to-transparent rounded-full blur-3xl" />
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Stats Section Component
function StatsSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-16 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="glass rounded-3xl p-8 lg:p-12">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 lg:gap-12">
            {stats.map((stat, index) => (
              <motion.div
                key={stat.label}
                initial={{ opacity: 0, y: 30 }}
                animate={isInView ? { opacity: 1, y: 0 } : {}}
                transition={{ duration: 0.6, delay: index * 0.2 }}
                className="text-center"
              >
                <div className="text-5xl lg:text-6xl font-bold number-accent mb-2">
                  {stat.value}
                </div>
                <div className="text-gray-400 text-sm uppercase tracking-wider">
                  {stat.label}
                </div>
              </motion.div>
            ))}
            <motion.div
              initial={{ opacity: 0, y: 30 }}
              animate={isInView ? { opacity: 1, y: 0 } : {}}
              transition={{ duration: 0.6, delay: 0.6 }}
              className="md:col-span-3 lg:col-span-3 text-center mt-4"
            >
              <p className="text-gray-300 max-w-2xl mx-auto">
                At Sailahari Labxcite, we are committed to excellence in laboratory solutions.
              </p>
            </motion.div>
          </div>
        </div>
      </div>
    </section>
  );
}

// Who We Are Section Component
function WhoWeAreSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-20 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          {/* Image */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8 }}
            className="relative"
          >
            <div className="relative h-[500px] rounded-3xl overflow-hidden">
              <Image
                src="https://images.unsplash.com/photo-1532187863486-abf9dbad1b69?w=800"
                alt="Laboratory microscope"
                fill
                className="object-cover"
              />
              <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0f] via-transparent to-transparent" />
            </div>

          </motion.div>

          {/* Content */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8, delay: 0.2 }}
          >
            <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-8 rounded-full" />
            <h2 className="text-3xl lg:text-4xl font-bold text-white mb-6">
              WHO WE ARE AT SAILAHARI{' '}
              <span className="gradient-text">LABXCITE</span>
            </h2>
            <p className="text-gray-400 leading-relaxed mb-8">
              Sailahari Labxcite specializes in providing cutting-edge laboratory
              instruments tailored for diverse sectors, empowering research, healthcare,
              and industry with innovative solutions.
            </p>
            <Link href="/about">
              <motion.span
                whileHover={{ x: 5 }}
                className="inline-flex items-center gap-2 text-[#f97066] font-medium cursor-pointer"
              >
                Read More
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                </svg>
              </motion.span>
            </Link>
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Services Section Component
function ServicesSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-20 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.6 }}
          className="text-center mb-16"
        >
          <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mx-auto mb-8 rounded-full" />
          <h2 className="text-3xl lg:text-4xl font-bold text-white">
            COMPREHENSIVE LABORATORY{' '}
            <span className="gradient-text">SERVICES OFFERED</span>
          </h2>
        </motion.div>

        <div className="space-y-8">
          {services.map((service, index) => (
            <motion.div
              key={service.number}
              initial={{ opacity: 0, y: 30 }}
              animate={isInView ? { opacity: 1, y: 0 } : {}}
              transition={{ duration: 0.6, delay: index * 0.2 }}
              className="glass-card rounded-2xl p-6 lg:p-8"
            >
              <div className="grid lg:grid-cols-12 gap-6 items-center">
                <div className="lg:col-span-1">
                  <span className="text-4xl font-bold number-accent">{service.number}.</span>
                </div>
                <div className="lg:col-span-5">
                  <h3 className="text-xl lg:text-2xl font-bold text-white mb-2">
                    {service.title}
                  </h3>
                  <p className="text-gray-400">{service.description}</p>
                </div>
                <div className="lg:col-span-2 text-center">
                  <Link href="/services">
                    <motion.span
                      whileHover={{ x: 5 }}
                      className="inline-flex items-center gap-2 text-[#f97066] font-medium cursor-pointer"
                    >
                      Read More
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                      </svg>
                    </motion.span>
                  </Link>
                </div>
                <div className="lg:col-span-4">
                  <div className="relative h-48 rounded-xl overflow-hidden">
                    <Image
                      src={service.image}
                      alt={service.title}
                      fill
                      className="object-cover"
                    />
                  </div>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}

// Advantages Section Component
function AdvantagesSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-20 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          {/* Content */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8 }}
          >
            <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-8 rounded-full" />
            <h2 className="text-3xl lg:text-4xl font-bold text-white mb-10">
              OUR UNIQUE{' '}
              <span className="gradient-text">ADVANTAGES</span>
            </h2>

            <div className="space-y-8">
              {advantages.map((advantage, index) => (
                <motion.div
                  key={advantage.title}
                  initial={{ opacity: 0, x: -30 }}
                  animate={isInView ? { opacity: 1, x: 0 } : {}}
                  transition={{ duration: 0.6, delay: index * 0.2 }}
                >
                  <h3 className="text-xl font-semibold text-white mb-2">
                    {advantage.title}
                  </h3>
                  <p className="text-gray-400 leading-relaxed">
                    {advantage.description}
                  </p>
                </motion.div>
              ))}
            </div>
          </motion.div>

          {/* Image */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8, delay: 0.2 }}
            className="relative"
          >
            <div className="relative h-[500px] rounded-3xl overflow-hidden glass-card">
              <Image
                src="https://images.unsplash.com/photo-1581091226825-a6a2a5aee158?w=800"
                alt="Laboratory equipment"
                fill
                className="object-cover"
              />
            </div>
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Partners Section Component
function ClientsSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-16 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.6 }}
          className="text-center mb-10"
        >
          <p className="text-gray-400 text-sm uppercase tracking-wider mb-2">Trusted By</p>
          <h3 className="text-xl font-semibold text-white">International Partners</h3>
        </motion.div>

        <motion.div
          initial={{ opacity: 0 }}
          animate={isInView ? { opacity: 1 } : {}}
          transition={{ duration: 0.8, delay: 0.2 }}
          className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-7 gap-4"
        >
          {partners.map((partner, index) => (
            <motion.div
              key={partner.name}
              initial={{ opacity: 0, y: 20 }}
              animate={isInView ? { opacity: 1, y: 0 } : {}}
              transition={{ duration: 0.4, delay: index * 0.1 }}
              whileHover={{ scale: 1.05 }}
              className="glass rounded-xl p-4 flex items-center justify-center hover:bg-white/10 transition-all duration-300 group"
            >
              <div className="relative w-full h-16 sm:h-20">
                <Image
                  src={partner.logo}
                  alt={partner.name}
                  fill
                  className="object-contain filter grayscale group-hover:grayscale-0 transition-all duration-300"
                />
              </div>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}

// Industries Section Component
function IndustriesSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-20 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.6 }}
          className="text-center mb-12"
        >
          <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mx-auto mb-8 rounded-full" />
          <h2 className="text-3xl lg:text-4xl font-bold text-white">
            INDUSTRIES{' '}
            <span className="gradient-text">WE SERVE</span>
          </h2>
          <p className="text-gray-400 mt-4 max-w-2xl mx-auto">
            Our laboratory instruments power research and quality control across diverse sectors
          </p>
        </motion.div>

        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-4 gap-4 sm:gap-6">
          {industries.map((industry, index) => (
            <motion.div
              key={industry.name}
              initial={{ opacity: 0, y: 30 }}
              animate={isInView ? { opacity: 1, y: 0 } : {}}
              transition={{ duration: 0.5, delay: index * 0.1 }}
              whileHover={{ y: -5, scale: 1.02 }}
              className="glass-card rounded-2xl overflow-hidden group cursor-pointer"
            >
              <div className="relative h-32 sm:h-40 overflow-hidden">
                <Image
                  src={industry.image}
                  alt={industry.name}
                  fill
                  className="object-cover transition-transform duration-500 group-hover:scale-110"
                />
                <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0f] via-[#0a0a0f]/40 to-transparent" />
              </div>
              <div className="p-3 sm:p-4 text-center">
                <h3 className="text-white font-semibold text-sm sm:text-base mb-1 group-hover:text-[#f97066] transition-colors">
                  {industry.name}
                </h3>
                <p className="text-gray-500 text-[10px] sm:text-xs">{industry.description}</p>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}

// Certifications Section Component
function CertificationsSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-16 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.6 }}
          className="glass rounded-3xl p-8 lg:p-12"
        >
          <div className="text-center mb-10">
            <h3 className="text-2xl font-bold text-white mb-2">
              Quality <span className="gradient-text">Certifications</span>
            </h3>
            <p className="text-gray-400 text-sm">Our commitment to excellence is backed by industry certifications</p>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            {certifications.map((cert, index) => (
              <motion.div
                key={cert.name}
                initial={{ opacity: 0, scale: 0.9 }}
                animate={isInView ? { opacity: 1, scale: 1 } : {}}
                transition={{ duration: 0.4, delay: index * 0.1 }}
                className="text-center"
              >
                <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-[#f97066] to-[#fb923c] flex items-center justify-center mx-auto mb-3 shadow-lg shadow-[#f97066]/20">
                  <svg className="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4M7.835 4.697a3.42 3.42 0 001.946-.806 3.42 3.42 0 014.438 0 3.42 3.42 0 001.946.806 3.42 3.42 0 013.138 3.138 3.42 3.42 0 00.806 1.946 3.42 3.42 0 010 4.438 3.42 3.42 0 00-.806 1.946 3.42 3.42 0 01-3.138 3.138 3.42 3.42 0 00-1.946.806 3.42 3.42 0 01-4.438 0 3.42 3.42 0 00-1.946-.806 3.42 3.42 0 01-3.138-3.138 3.42 3.42 0 00-.806-1.946 3.42 3.42 0 010-4.438 3.42 3.42 0 00.806-1.946 3.42 3.42 0 013.138-3.138z" />
                  </svg>
                </div>
                <h4 className="text-white font-semibold text-sm">{cert.name}</h4>
                <p className="text-gray-500 text-xs">{cert.description}</p>
              </motion.div>
            ))}
          </div>
        </motion.div>
      </div>
    </section>
  );
}

