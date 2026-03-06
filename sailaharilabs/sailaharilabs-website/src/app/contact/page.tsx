'use client';

import { motion, useInView } from 'framer-motion';
import { useRef, useState } from 'react';
import Image from 'next/image';

// Contact Info
const contactInfo = [
  {
    title: 'Phone',
    value: '+918971855369',
    href: 'tel:+918971855369',
    icon: PhoneIcon,
  },
  {
    title: 'Email',
    value: 'SAILATHA.LABXCITE@GMAIL.COM',
    href: 'mailto:SAILATHA.LABXCITE@GMAIL.COM',
    icon: EmailIcon,
  },
];

// Social Links
const socialLinks = [
  { name: 'Facebook', icon: 'fb', href: '#' },
  { name: 'Twitter', icon: 'tw', href: '#' },
  { name: 'Instagram', icon: 'ig', href: '#' },
  { name: 'YouTube', icon: 'yt', href: '#' },
  { name: 'LinkedIn', icon: 'li', href: '#' },
];

export default function ContactPage() {
  return (
    <div className="overflow-hidden">
      {/* Hero Section */}
      <HeroSection />

      {/* Contact Info Cards */}
      <ContactInfoSection />

      {/* Contact Form & Map Section */}
      <ContactFormSection />
    </div>
  );
}

// Hero Section
function HeroSection() {
  return (
    <section className="relative pt-32 pb-16 spotlight">
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
              GET IN TOUCH WITH{' '}
              <span className="gradient-text">SAILAHARI LABXCITE</span>
            </h1>
            <p className="text-gray-400 text-lg leading-relaxed">
              We&apos;re here to answer your questions and provide support for your
              laboratory needs. Reach out to us today.
            </p>
          </motion.div>

          {/* Image */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8, delay: 0.2 }}
            className="relative"
          >
            <div className="relative h-[350px] rounded-3xl overflow-hidden glass-card">
              <Image
                src="https://images.unsplash.com/photo-1581092918056-0c4c3acd3789?w=800"
                alt="Contact us"
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

// Contact Info Section
function ContactInfoSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-12 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
          {/* Phone Card */}
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={isInView ? { opacity: 1, y: 0 } : {}}
            transition={{ duration: 0.6 }}
            className="glass-card rounded-2xl p-6"
          >
            <div className="text-gray-500 text-sm mb-2">Phone</div>
            <a
              href="tel:+918971855369"
              className="text-xl font-bold text-white hover:text-[#f97066] transition-colors"
            >
              +918971855369
            </a>
          </motion.div>

          {/* Email Card */}
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={isInView ? { opacity: 1, y: 0 } : {}}
            transition={{ duration: 0.6, delay: 0.1 }}
            className="glass-card rounded-2xl p-6"
          >
            <div className="text-gray-500 text-sm mb-2">Email</div>
            <a
              href="mailto:SAILATHA.LABXCITE@GMAIL.COM"
              className="text-lg font-bold text-white hover:text-[#f97066] transition-colors break-all"
            >
              SAILATHA.LABXCITE@GMAIL.COM
            </a>
          </motion.div>

          {/* Address Card */}
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={isInView ? { opacity: 1, y: 0 } : {}}
            transition={{ duration: 0.6, delay: 0.2 }}
            className="glass-card rounded-2xl p-6"
          >
            <div className="text-gray-500 text-sm mb-2">Address</div>
            <p className="text-white font-medium leading-relaxed">
              NO.131, 4TH CROSS, 9TH MAIN,<br />
              1ST STAGE, 2ND PHASE, GOKUL<br />
              EXTENSION, YESWANTHPUR,<br />
              BANGALORE – 560054
            </p>
          </motion.div>

          {/* Social Media Card */}
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={isInView ? { opacity: 1, y: 0 } : {}}
            transition={{ duration: 0.6, delay: 0.3 }}
            className="glass-card rounded-2xl p-6"
          >
            <div className="text-gray-500 text-sm mb-3">Social Media</div>
            <div className="flex items-center gap-3">
              {socialLinks.map((social) => (
                <a
                  key={social.name}
                  href={social.href}
                  className="w-9 h-9 rounded-lg bg-white/5 flex items-center justify-center text-gray-400 hover:bg-[#f97066]/20 hover:text-[#f97066] transition-all duration-300"
                  aria-label={social.name}
                >
                  <SocialIcon type={social.icon} />
                </a>
              ))}
            </div>
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Contact Form Section
function ContactFormSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });
  const [formData, setFormData] = useState({
    firstName: '',
    lastName: '',
    email: '',
    message: '',
  });
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    setFormData({ ...formData, [e.target.name]: e.target.value });
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);

    // Simulate form submission
    setTimeout(() => {
      setIsSubmitting(false);
      setSubmitted(true);
      setFormData({ firstName: '', lastName: '', email: '', message: '' });
    }, 1500);
  };

  return (
    <section ref={ref} className="py-16 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12">
          {/* Form */}
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8 }}
          >
            <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-8 rounded-full" />
            <h2 className="text-3xl lg:text-4xl font-bold text-white mb-4">
              REACH OUT FOR{' '}
              <span className="gradient-text">INQUIRIES OR ASSISTANCE</span>
            </h2>
            <p className="text-gray-400 mb-8">
              We are here to answer your questions and provide support for your
              laboratory needs.
            </p>

            {submitted ? (
              <motion.div
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                className="glass-card rounded-2xl p-8 text-center"
              >
                <div className="w-16 h-16 rounded-full bg-gradient-to-br from-[#f97066] to-[#fb923c] flex items-center justify-center mx-auto mb-4">
                  <svg className="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                </div>
                <h3 className="text-2xl font-bold text-white mb-2">Thank You!</h3>
                <p className="text-gray-400 mb-6">Your message has been sent successfully. We&apos;ll get back to you soon.</p>
                <button
                  onClick={() => setSubmitted(false)}
                  className="btn-primary text-sm"
                >
                  Send Another Message
                </button>
              </motion.div>
            ) : (
              <form onSubmit={handleSubmit} className="space-y-6">
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div>
                    <input
                      type="text"
                      name="firstName"
                      placeholder="First Name"
                      required
                      value={formData.firstName}
                      onChange={handleChange}
                      className="input-modern"
                    />
                  </div>
                  <div>
                    <input
                      type="text"
                      name="lastName"
                      placeholder="Last Name"
                      required
                      value={formData.lastName}
                      onChange={handleChange}
                      className="input-modern"
                    />
                  </div>
                </div>

                <div>
                  <input
                    type="email"
                    name="email"
                    placeholder="Email Address"
                    required
                    value={formData.email}
                    onChange={handleChange}
                    className="input-modern"
                  />
                </div>

                <div>
                  <textarea
                    name="message"
                    placeholder="Type your message"
                    rows={5}
                    required
                    value={formData.message}
                    onChange={handleChange}
                    className="input-modern resize-none"
                  />
                </div>

                <motion.button
                  type="submit"
                  disabled={isSubmitting}
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  className="btn-primary w-full sm:w-auto disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isSubmitting ? (
                    <span className="flex items-center gap-2">
                      <svg className="animate-spin w-5 h-5" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                      </svg>
                      Sending...
                    </span>
                  ) : (
                    'Submit'
                  )}
                </motion.button>
              </form>
            )}
          </motion.div>

          {/* Map */}
          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={isInView ? { opacity: 1, x: 0 } : {}}
            transition={{ duration: 0.8, delay: 0.2 }}
            className="relative"
          >
            <div className="glass-card rounded-2xl overflow-hidden h-full min-h-[500px]">
              {/* Map iframe */}
              <iframe
                src="https://www.google.com/maps/embed?pb=!1m18!1m12!1m3!1d3887.4567123456789!2d77.5456789!3d13.0234567!2m3!1f0!2f0!3f0!3m2!1i1024!2i768!4f13.1!3m3!1m2!1s0x0%3A0x0!2zMTPCsDAxJzI0LjQiTiA3N8KwMzInNDQuNCJF!5e0!3m2!1sen!2sin!4v1234567890"
                width="100%"
                height="100%"
                style={{ border: 0, minHeight: '500px' }}
                allowFullScreen
                loading="lazy"
                referrerPolicy="no-referrer-when-downgrade"
                className="grayscale opacity-80 hover:grayscale-0 hover:opacity-100 transition-all duration-500"
              />

              {/* Location Card Overlay */}
              <div className="absolute top-4 left-4 glass rounded-xl p-4 max-w-xs">
                <h4 className="text-white font-semibold text-sm mb-1">C-13-1, 4th Cross Rd</h4>
                <p className="text-gray-400 text-xs leading-relaxed">
                  C-13-1, 4th Cross Rd, Kamla Nehru Extension, Yeswanthpur, Bengaluru, Karnataka 560022
                </p>
                <a
                  href="https://maps.google.com"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-[#f97066] text-xs mt-2 hover:underline"
                >
                  View larger map
                  <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                  </svg>
                </a>
              </div>
            </div>
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Icon Components
function PhoneIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z" />
    </svg>
  );
}

function EmailIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
    </svg>
  );
}

function SocialIcon({ type }: { type: string }) {
  switch (type) {
    case 'fb':
      return (
        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
          <path d="M18.77,7.46H14.5v-1.9c0-.9.6-1.1,1-1.1h3V.5h-4.33C10.24.5,9.5,3.44,9.5,5.32v2.15h-3v4h3v12h5v-12h3.85l.42-4Z" />
        </svg>
      );
    case 'tw':
      return (
        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
          <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z" />
        </svg>
      );
    case 'ig':
      return (
        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
          <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z" />
        </svg>
      );
    case 'yt':
      return (
        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
          <path d="M23.498 6.186a3.016 3.016 0 0 0-2.122-2.136C19.505 3.545 12 3.545 12 3.545s-7.505 0-9.377.505A3.017 3.017 0 0 0 .502 6.186C0 8.07 0 12 0 12s0 3.93.502 5.814a3.016 3.016 0 0 0 2.122 2.136c1.871.505 9.376.505 9.376.505s7.505 0 9.377-.505a3.015 3.015 0 0 0 2.122-2.136C24 15.93 24 12 24 12s0-3.93-.502-5.814zM9.545 15.568V8.432L15.818 12l-6.273 3.568z" />
        </svg>
      );
    case 'li':
      return (
        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
          <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z" />
        </svg>
      );
    default:
      return null;
  }
}
