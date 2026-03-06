'use client';

import { motion, useInView } from 'framer-motion';
import { useRef, useState } from 'react';
import Link from 'next/link';

// FAQ Data
const faqCategories = [
  {
    category: 'General',
    questions: [
      {
        question: 'What types of laboratory instruments does Sailahari Labxcite offer?',
        answer: 'We offer a comprehensive range of laboratory instruments including analytical instruments (spectrophotometers, centrifuges), measuring equipment (pH meters, analytical balances), testing devices (incubators, ovens), and lab consumables (pipettes, glassware). We also specialize in custom instrumentation tailored to your specific research needs.',
      },
      {
        question: 'How long has Sailahari Labxcite been in business?',
        answer: 'Sailahari Labxcite was founded in 2000, and we have over 20 years of experience in providing high-quality laboratory instruments and services to research institutions, healthcare facilities, and industries across India.',
      },
      {
        question: 'Do you serve customers outside Bangalore?',
        answer: 'Yes, while our headquarters is in Bangalore, we serve customers across India. We provide nationwide delivery and can arrange for installation and training at your location.',
      },
    ],
  },
  {
    category: 'Products & Services',
    questions: [
      {
        question: 'Do you provide custom laboratory instruments?',
        answer: 'Yes, custom instrumentation is one of our core services. Our team works closely with you to understand your specific requirements and designs instruments that meet your unique research and development needs. Contact us for a consultation.',
      },
      {
        question: 'What calibration services do you offer?',
        answer: 'We provide comprehensive calibration services that adhere to industry standards and protocols. Our calibration ensures your instruments operate at peak performance, giving you confidence in your results and helping maintain compliance with regulatory requirements.',
      },
      {
        question: 'Do you provide training for the instruments?',
        answer: 'Yes, we offer comprehensive training workshops to help your team master the use of our laboratory instruments. Our expert instructors provide hands-on training, ensuring your personnel can operate equipment efficiently and effectively.',
      },
    ],
  },
  {
    category: 'Orders & Delivery',
    questions: [
      {
        question: 'How can I request a quote for laboratory instruments?',
        answer: 'You can request a quote by visiting our Products page and clicking "Request Quote" on any product, or by contacting us directly through our Contact page, WhatsApp, or email at SAILATHA.LABXCITE@GMAIL.COM.',
      },
      {
        question: 'What is the typical delivery time for instruments?',
        answer: 'Delivery times vary depending on the product and your location. Standard products are typically delivered within 7-14 business days. Custom instruments may take longer depending on complexity. We will provide you with an estimated delivery time when you place your order.',
      },
      {
        question: 'Do you offer installation services?',
        answer: 'Yes, we provide professional installation services for all our laboratory instruments. Our technicians will ensure proper setup, calibration, and testing of your equipment at your facility.',
      },
    ],
  },
  {
    category: 'Support & Warranty',
    questions: [
      {
        question: 'What warranty do you provide on instruments?',
        answer: 'We provide comprehensive warranty coverage on all our instruments. The warranty period varies by product, typically ranging from 1 to 3 years. Please check the specific product details or contact us for warranty information.',
      },
      {
        question: 'How can I get technical support?',
        answer: 'Our dedicated technical support team is always available to assist you. You can reach us via phone (+91 8971855369), email, WhatsApp, or through the contact form on our website. We offer troubleshooting, maintenance guidance, and on-site support when needed.',
      },
      {
        question: 'Do you provide maintenance services?',
        answer: 'Yes, we offer Annual Maintenance Contracts (AMC) for all our instruments. Our maintenance services include regular inspections, preventive maintenance, calibration, and priority support to ensure your equipment remains in optimal condition.',
      },
    ],
  },
];

export default function FAQPage() {
  return (
    <div className="overflow-hidden">
      {/* Hero Section */}
      <HeroSection />

      {/* FAQ Section */}
      <FAQSection />

      {/* Still Have Questions CTA */}
      <CTASection />
    </div>
  );
}

// Hero Section
function HeroSection() {
  return (
    <section className="relative pt-32 pb-16 spotlight">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          className="text-center max-w-3xl mx-auto"
        >
          <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mx-auto mb-8 rounded-full" />
          <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-white leading-tight mb-6">
            FREQUENTLY ASKED{' '}
            <span className="gradient-text">QUESTIONS</span>
          </h1>
          <p className="text-gray-400 text-lg leading-relaxed">
            Find answers to common questions about our laboratory instruments,
            services, orders, and support.
          </p>
        </motion.div>
      </div>
    </section>
  );
}

// FAQ Section
function FAQSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });
  const [openItems, setOpenItems] = useState<{ [key: string]: boolean }>({});

  const toggleItem = (key: string) => {
    setOpenItems((prev) => ({
      ...prev,
      [key]: !prev[key],
    }));
  };

  return (
    <section ref={ref} className="py-16 relative">
      <div className="max-w-4xl mx-auto px-6 lg:px-8">
        {faqCategories.map((category, categoryIndex) => (
          <motion.div
            key={category.category}
            initial={{ opacity: 0, y: 30 }}
            animate={isInView ? { opacity: 1, y: 0 } : {}}
            transition={{ duration: 0.6, delay: categoryIndex * 0.1 }}
            className="mb-12"
          >
            <h2 className="text-2xl font-bold text-white mb-6 flex items-center gap-3">
              <span className="w-8 h-8 rounded-lg bg-gradient-to-br from-[#f97066] to-[#fb923c] flex items-center justify-center text-white text-sm">
                {categoryIndex + 1}
              </span>
              {category.category}
            </h2>

            <div className="space-y-4">
              {category.questions.map((item, questionIndex) => {
                const key = `${categoryIndex}-${questionIndex}`;
                const isOpen = openItems[key];

                return (
                  <motion.div
                    key={key}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ delay: questionIndex * 0.1 }}
                    className="glass-card rounded-2xl overflow-hidden"
                  >
                    <button
                      onClick={() => toggleItem(key)}
                      className="w-full flex items-center justify-between p-6 text-left"
                    >
                      <span className="text-white font-medium pr-4">
                        {item.question}
                      </span>
                      <motion.div
                        animate={{ rotate: isOpen ? 180 : 0 }}
                        transition={{ duration: 0.3 }}
                        className="flex-shrink-0"
                      >
                        <svg
                          className="w-5 h-5 text-[#f97066]"
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                        >
                          <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={2}
                            d="M19 9l-7 7-7-7"
                          />
                        </svg>
                      </motion.div>
                    </button>
                    <motion.div
                      initial={false}
                      animate={{
                        height: isOpen ? 'auto' : 0,
                        opacity: isOpen ? 1 : 0,
                      }}
                      transition={{ duration: 0.3 }}
                      className="overflow-hidden"
                    >
                      <p className="px-6 pb-6 text-gray-400 leading-relaxed">
                        {item.answer}
                      </p>
                    </motion.div>
                  </motion.div>
                );
              })}
            </div>
          </motion.div>
        ))}
      </div>
    </section>
  );
}

// CTA Section
function CTASection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-16 relative">
      <div className="max-w-4xl mx-auto px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.6 }}
          className="glass-card rounded-3xl p-8 lg:p-12 text-center"
        >
          <div className="w-16 h-16 rounded-full bg-gradient-to-br from-[#f97066] to-[#fb923c] flex items-center justify-center mx-auto mb-6">
            <svg
              className="w-8 h-8 text-white"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
          </div>
          <h3 className="text-2xl lg:text-3xl font-bold text-white mb-4">
            Still Have Questions?
          </h3>
          <p className="text-gray-400 mb-8 max-w-lg mx-auto">
            Can&apos;t find the answer you&apos;re looking for? Our team is here to help.
            Reach out to us and we&apos;ll get back to you as soon as possible.
          </p>
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <Link href="/contact">
              <motion.button
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                className="btn-primary"
              >
                Contact Us
              </motion.button>
            </Link>
            <a href="https://wa.me/918971855369" target="_blank" rel="noopener noreferrer">
              <motion.button
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                className="btn-secondary flex items-center gap-2"
              >
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z" />
                </svg>
                WhatsApp
              </motion.button>
            </a>
          </div>
        </motion.div>
      </div>
    </section>
  );
}
