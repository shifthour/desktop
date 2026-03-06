'use client';

import { motion, useInView } from 'framer-motion';
import { useRef, useState } from 'react';
import Image from 'next/image';

// Accordion Data
const accordionItems = [
  {
    title: 'Our Mission: Empowering Scientific Advancement',
    content: 'To provide innovative laboratory solutions that enable scientific breakthroughs and enhance research capabilities within life sciences, healthcare, and various industrial sectors.',
  },
  {
    title: 'Our Vision: Shaping the Future of Labs',
    content: 'To be the global leader in laboratory instrumentation, driving innovation and excellence in scientific research and industrial applications worldwide.',
  },
  {
    title: 'Our Core Values: Excellence and Integrity',
    content: 'We are committed to delivering excellence in every product and service, maintaining the highest standards of integrity, and fostering long-term partnerships with our clients.',
  },
  {
    title: 'Awards and Recognition for Excellence',
    content: 'Our commitment to quality and innovation has been recognized with multiple industry awards and certifications, establishing us as a trusted name in laboratory solutions.',
  },
];

export default function AboutPage() {
  return (
    <div className="overflow-hidden">
      {/* Hero Section */}
      <HeroSection />

      {/* Innovation Section */}
      <InnovationSection />

      {/* Who We Are Section */}
      <WhoWeAreSection />

      {/* Journey Section */}
      <JourneySection />
    </div>
  );
}

// Hero Section
function HeroSection() {
  return (
    <section className="relative pt-32 pb-20 spotlight">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          {/* Content */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8 }}
          >
            <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-8 rounded-full" />
            <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-white leading-tight mb-6">
              DISCOVER SAILAHARI{' '}
              <span className="gradient-text">LABXCITE&apos;S</span>{' '}
              EXPERTISE
            </h1>
            <p className="text-gray-400 text-lg leading-relaxed">
              Learn about our journey, mission, and commitment to providing cutting-edge
              laboratory solutions for research and industry.
            </p>
          </motion.div>

          {/* Image */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8, delay: 0.2 }}
            className="relative"
          >
            <div className="relative h-[400px] rounded-3xl overflow-hidden glass-card">
              <Image
                src="https://images.unsplash.com/photo-1532187863486-abf9dbad1b69?w=800"
                alt="Laboratory research"
                fill
                className="object-cover"
              />
            </div>
            {/* Decorative elements */}
            <div className="absolute -top-10 -right-10 w-40 h-40 bg-gradient-to-br from-[#f97066]/20 to-transparent rounded-full blur-3xl" />
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Innovation Section
function InnovationSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-20 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        {/* Hero Image with 3D effect */}
        <motion.div
          initial={{ opacity: 0, y: 50 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.8 }}
          className="relative h-[400px] rounded-3xl overflow-hidden mb-16"
        >
          <Image
            src="https://images.unsplash.com/photo-1579684385127-1ef15d508118?w=1200"
            alt="Laboratory innovation"
            fill
            className="object-cover"
          />
          <div className="absolute inset-0 bg-gradient-to-r from-[#0a0a0f]/80 via-transparent to-[#0a0a0f]/80" />

          {/* Floating pills decoration */}
          <motion.div
            animate={{ y: [0, -15, 0] }}
            transition={{ duration: 4, repeat: Infinity, ease: 'easeInOut' }}
            className="absolute top-10 right-20 w-16 h-16 rounded-full bg-white/10 backdrop-blur-sm"
          />
          <motion.div
            animate={{ y: [0, 15, 0] }}
            transition={{ duration: 5, repeat: Infinity, ease: 'easeInOut' }}
            className="absolute bottom-20 left-32 w-12 h-12 rounded-full bg-white/10 backdrop-blur-sm"
          />
        </motion.div>

        <div className="grid lg:grid-cols-2 gap-12 items-start">
          {/* Left Content */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8, delay: 0.2 }}
          >
            <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-8 rounded-full" />
            <h2 className="text-3xl lg:text-4xl font-bold text-white mb-6">
              COMMITTED TO INNOVATION IN{' '}
              <span className="gradient-text">LABORATORY SOLUTIONS</span>
            </h2>
          </motion.div>

          {/* Right Content */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8, delay: 0.4 }}
          >
            <p className="text-xl text-gray-300 mb-4 font-medium">
              SAILAHARI LABXCITE PROVIDES ADVANCED LABORATORY INSTRUMENTS
              TAILORED FOR VARIOUS INDUSTRIES, ENHANCING PRECISION AND EFFICIENCY
              IN RESEARCH AND DEVELOPMENT.
            </p>
            <p className="text-gray-400 leading-relaxed">
              With a proven track record, Sailahari Labxcite has successfully delivered
              cutting-edge solutions that empower clients in life sciences, healthcare,
              and industry, driving innovation and excellence in their projects.
            </p>
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Who We Are Section with Accordion
function WhoWeAreSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });
  const [openIndex, setOpenIndex] = useState<number | null>(0);

  return (
    <section ref={ref} className="py-20 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12 items-start">
          {/* Image */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8 }}
            className="relative"
          >
            <div className="relative h-[500px] rounded-3xl overflow-hidden">
              <Image
                src="https://images.unsplash.com/photo-1582719471384-894fbb16e074?w=800"
                alt="Laboratory equipment"
                fill
                className="object-cover"
              />
              <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0f] via-transparent to-transparent" />
            </div>

            {/* Info Card */}
            <motion.div
              initial={{ opacity: 0, y: 30 }}
              animate={isInView ? { opacity: 1, y: 0 } : {}}
              transition={{ duration: 0.6, delay: 0.4 }}
              className="absolute bottom-0 left-0 right-0 p-8"
            >
              <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-4 rounded-full" />
              <h3 className="text-2xl font-bold text-white mb-3">
                WHO WE ARE AT SAILAHARI LABXCITE
              </h3>
              <p className="text-gray-300 text-sm leading-relaxed">
                Our commitment to quality and innovation has helped clients achieve
                outstanding results, significantly enhancing their research capabilities and
                operational efficiency.
              </p>
            </motion.div>
          </motion.div>

          {/* Accordion */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8, delay: 0.2 }}
            className="space-y-4"
          >
            {accordionItems.map((item, index) => (
              <motion.div
                key={item.title}
                initial={{ opacity: 0, y: 20 }}
                animate={isInView ? { opacity: 1, y: 0 } : {}}
                transition={{ duration: 0.5, delay: 0.3 + index * 0.1 }}
                className="glass-card rounded-2xl overflow-hidden"
              >
                <button
                  onClick={() => setOpenIndex(openIndex === index ? null : index)}
                  className="w-full flex items-center justify-between p-6 text-left"
                >
                  <span className="text-white font-medium pr-4">{item.title}</span>
                  <motion.div
                    animate={{ rotate: openIndex === index ? 180 : 0 }}
                    transition={{ duration: 0.3 }}
                    className="flex-shrink-0"
                  >
                    <svg className="w-5 h-5 text-[#f97066]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </motion.div>
                </button>
                <motion.div
                  initial={false}
                  animate={{
                    height: openIndex === index ? 'auto' : 0,
                    opacity: openIndex === index ? 1 : 0,
                  }}
                  transition={{ duration: 0.3 }}
                  className="overflow-hidden"
                >
                  <p className="px-6 pb-6 text-gray-400 leading-relaxed">
                    {item.content}
                  </p>
                </motion.div>
              </motion.div>
            ))}
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Journey Section
function JourneySection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-20 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          {/* Left Content */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8 }}
          >
            <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-8 rounded-full" />
            <h2 className="text-3xl lg:text-4xl font-bold text-white mb-6">
              OUR JOURNEY: FROM INCEPTION TO{' '}
              <span className="gradient-text">INNOVATION</span>
            </h2>
            <p className="text-gray-400 leading-relaxed mb-6">
              Founded in 2000, Sailahari Labxcite has rapidly evolved into a trusted provider
              of laboratory instruments, meeting the growing needs of diverse industries.
            </p>

            {/* Image placeholder */}
            <div className="relative h-[250px] rounded-2xl overflow-hidden glass-card">
              <Image
                src="https://images.unsplash.com/photo-1507413245164-6160d8298b31?w=600"
                alt="Laboratory microscope"
                fill
                className="object-cover"
              />
            </div>
          </motion.div>

          {/* Right Content */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8, delay: 0.2 }}
          >
            <h3 className="text-2xl font-bold text-white mb-4">
              A COMPREHENSIVE EVOLUTION: SHAPING{' '}
              <span className="gradient-text">LABORATORY TECHNOLOGY</span>
            </h3>
            <p className="text-gray-400 leading-relaxed mb-8">
              Over the years, Sailahari Labxcite has continuously expanded its product offerings,
              investing in research and development to ensure cutting-edge technology is always available to
              our clients, positioning us at the forefront of the laboratory solutions market.
            </p>

            {/* Stats */}
            <div className="grid grid-cols-2 gap-6">
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={isInView ? { opacity: 1, y: 0 } : {}}
                transition={{ duration: 0.5, delay: 0.4 }}
                className="glass-card rounded-2xl p-6 text-center"
              >
                <div className="text-4xl font-bold number-accent mb-2">2000</div>
                <div className="text-gray-400 text-sm">Founded</div>
              </motion.div>
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={isInView ? { opacity: 1, y: 0 } : {}}
                transition={{ duration: 0.5, delay: 0.5 }}
                className="glass-card rounded-2xl p-6 text-center"
              >
                <div className="text-4xl font-bold number-accent mb-2">50+</div>
                <div className="text-gray-400 text-sm">Products</div>
              </motion.div>
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={isInView ? { opacity: 1, y: 0 } : {}}
                transition={{ duration: 0.5, delay: 0.6 }}
                className="glass-card rounded-2xl p-6 text-center"
              >
                <div className="text-4xl font-bold number-accent mb-2">15+</div>
                <div className="text-gray-400 text-sm">Team Members</div>
              </motion.div>
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={isInView ? { opacity: 1, y: 0 } : {}}
                transition={{ duration: 0.5, delay: 0.7 }}
                className="glass-card rounded-2xl p-6 text-center"
              >
                <div className="text-4xl font-bold number-accent mb-2">10+</div>
                <div className="text-gray-400 text-sm">Industries Served</div>
              </motion.div>
            </div>
          </motion.div>
        </div>
      </div>
    </section>
  );
}
