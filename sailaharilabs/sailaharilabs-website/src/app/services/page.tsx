'use client';

import { motion, useInView } from 'framer-motion';
import { useRef } from 'react';
import Image from 'next/image';

// Services Data
const services = [
  {
    number: '01',
    title: 'CUSTOM INSTRUMENTATION',
    description: 'We specialize in designing and manufacturing custom laboratory instruments that meet the unique requirements of your research and development projects. Our team collaborates with you to create instruments that not only enhance efficiency but also improve accuracy in your experiments.',
    image: 'https://images.unsplash.com/photo-1582719471384-894fbb16e074?w=800',
  },
  {
    number: '02',
    title: 'CALIBRATION SERVICES',
    description: 'Our calibration services ensure that your laboratory instruments operate at peak performance. We adhere to industry standards and protocols, providing precise calibration that gives you confidence in your results and helps maintain compliance with regulatory requirements.',
    image: 'https://images.unsplash.com/photo-1576086213369-97a306d36557?w=800',
  },
  {
    number: '03',
    title: 'TECHNICAL SUPPORT',
    description: 'Our dedicated technical support team is always available to assist you with any queries or challenges you may face. From troubleshooting to maintenance, we ensure your laboratory instruments remain in optimal condition, allowing you to focus on your important research activities.',
    image: 'https://images.unsplash.com/photo-1581091226825-a6a2a5aee158?w=800',
  },
  {
    number: '04',
    title: 'TRAINING WORKSHOPS',
    description: 'We offer comprehensive training workshops to help your team master the use of our laboratory instruments. Our expert instructors provide hands-on training, ensuring your personnel can operate equipment efficiently and effectively. Continuous education leads to better results and maximizes the potential of your laboratory.',
    image: 'https://images.unsplash.com/photo-1532187863486-abf9dbad1b69?w=800',
  },
];

// Process Steps
const processSteps = [
  {
    title: 'Initial Consultation',
    description: 'We begin with a detailed consultation to understand your laboratory needs and objectives.',
  },
  {
    title: 'Custom Design Phase',
    description: 'Our team designs tailored solutions to meet your specific requirements, leveraging the latest technology.',
  },
  {
    title: 'Manufacturing & Testing',
    description: 'After design approval, we manufacture the equipment and conduct rigorous testing for quality assurance.',
  },
  {
    title: 'Delivery & Training',
    description: 'Once the instruments are ready, we deliver them to your facility and provide thorough training to ensure proficiency.',
  },
];

export default function ServicesPage() {
  return (
    <div className="overflow-hidden">
      {/* Hero Section */}
      <HeroSection />

      {/* Services List Section */}
      <ServicesListSection />

      {/* Process Section */}
      <ProcessSection />
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
              COMPREHENSIVE{' '}
              <span className="gradient-text">LABORATORY SOLUTIONS</span>{' '}
              TAILORED FOR YOU
            </h1>
            <p className="text-gray-400 text-lg leading-relaxed">
              Explore our wide range of laboratory services designed to meet
              the unique needs of your research and development projects.
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
                src="https://images.unsplash.com/photo-1579684385127-1ef15d508118?w=800"
                alt="Laboratory solutions"
                fill
                className="object-cover"
              />
            </div>
            {/* Decorative elements */}
            <div className="absolute -top-10 -right-10 w-40 h-40 bg-gradient-to-br from-[#f97066]/20 to-transparent rounded-full blur-3xl" />
            <div className="absolute -bottom-10 -left-10 w-32 h-32 bg-gradient-to-br from-[#fb923c]/20 to-transparent rounded-full blur-3xl" />
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Services List Section
function ServicesListSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-20 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="space-y-20">
          {services.map((service, index) => (
            <motion.div
              key={service.number}
              initial={{ opacity: 0, y: 50 }}
              animate={isInView ? { opacity: 1, y: 0 } : {}}
              transition={{ duration: 0.8, delay: index * 0.2 }}
              className={`grid lg:grid-cols-2 gap-12 items-center ${
                index % 2 === 1 ? 'lg:grid-flow-dense' : ''
              }`}
            >
              {/* Image */}
              <motion.div
                whileHover={{ scale: 1.02 }}
                transition={{ duration: 0.3 }}
                className={`relative ${index % 2 === 1 ? 'lg:col-start-2' : ''}`}
              >
                <div className="relative h-[350px] lg:h-[400px] rounded-3xl overflow-hidden">
                  <Image
                    src={service.image}
                    alt={service.title}
                    fill
                    className="object-cover"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0f]/60 via-transparent to-transparent" />
                </div>

                {/* Number Badge */}
                <div className="absolute -top-6 -left-6 lg:-top-8 lg:-left-8">
                  <div className="w-16 h-16 lg:w-20 lg:h-20 rounded-2xl bg-gradient-to-br from-[#f97066] to-[#fb923c] flex items-center justify-center shadow-lg shadow-[#f97066]/30">
                    <span className="text-2xl lg:text-3xl font-bold text-white">{service.number}</span>
                  </div>
                </div>
              </motion.div>

              {/* Content */}
              <div className={index % 2 === 1 ? 'lg:col-start-1 lg:row-start-1' : ''}>
                <div className="w-12 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-6 rounded-full" />
                <h2 className="text-2xl lg:text-3xl font-bold text-white mb-4">
                  {service.title}
                </h2>
                <p className="text-gray-400 leading-relaxed mb-6">
                  {service.description}
                </p>
                <motion.button
                  whileHover={{ x: 5 }}
                  className="inline-flex items-center gap-2 text-[#f97066] font-medium"
                >
                  Learn More
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                  </svg>
                </motion.button>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}

// Process Section
function ProcessSection() {
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
            OUR STEP-BY-STEP PROCESS FOR{' '}
            <span className="gradient-text">YOUR LAB NEEDS</span>
          </h2>
        </motion.div>

        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
          {processSteps.map((step, index) => (
            <motion.div
              key={step.title}
              initial={{ opacity: 0, y: 30 }}
              animate={isInView ? { opacity: 1, y: 0 } : {}}
              transition={{ duration: 0.6, delay: index * 0.15 }}
              whileHover={{ y: -8 }}
              className="glass-card rounded-2xl p-6 lg:p-8 relative group"
            >
              {/* Step Number */}
              <div className="absolute -top-4 left-6">
                <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-[#f97066] to-[#fb923c] flex items-center justify-center text-white text-sm font-bold shadow-lg shadow-[#f97066]/30">
                  {index + 1}
                </div>
              </div>

              {/* Content */}
              <div className="pt-4">
                <h3 className="text-lg font-semibold text-white mb-3">
                  {step.title}
                </h3>
                <p className="text-gray-400 text-sm leading-relaxed">
                  {step.description}
                </p>
              </div>

              {/* Arrow connector (not for last item) */}
              {index < processSteps.length - 1 && (
                <div className="hidden lg:block absolute top-1/2 -right-3 transform -translate-y-1/2">
                  <svg className="w-6 h-6 text-[#f97066]/30" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M9 5l7 7-7 7" />
                  </svg>
                </div>
              )}

              {/* Hover glow effect */}
              <div className="absolute inset-0 rounded-2xl bg-gradient-to-br from-[#f97066]/0 to-[#fb923c]/0 group-hover:from-[#f97066]/5 group-hover:to-[#fb923c]/5 transition-all duration-300" />
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}
