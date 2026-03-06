import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { User, Phone, Mail, Send, UserCircle2 } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useNavigate, Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';
import { toast } from 'sonner';
import { submitLead } from '@/lib/supabase';

export default function AnahataBookSiteVisit() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    phone: '',
  });
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);

    try {
      localStorage.setItem('brochureAccess', 'granted');

      const result = await submitLead({
        name: formData.name,
        email: formData.email,
        phone: formData.phone,
        source: 'site_visit',
        project_name: 'Anahata',
      });

      if (!result.success) {
        // Fallback to mailto if Supabase fails
        const subject = encodeURIComponent(`Site Visit Request - Anahata | ${formData.name}`);
        const body = encodeURIComponent(
          `Site Visit Booking Request\n\nProject: Anahata\nName: ${formData.name}\nPhone: ${formData.phone}\nEmail: ${formData.email}\nDate: ${new Date().toISOString().split('T')[0]}`
        );
        window.open(`mailto:info@ishtikahomes.com?subject=${subject}&body=${body}`, '_self');
      }

      navigate(createPageUrl('AnahataThankYou'));
    } catch (error) {
      toast.error("Failed to submit. Please try again.");
      setIsSubmitting(false);
    }
  };

  return (
    <div className="bg-gradient-to-b from-orange-50 via-white to-orange-50 min-h-screen pt-20">
      <Navbar />
      <WhatsAppButton message="Hi, I'm interested in booking a site visit for Anahata. Can you help me?" />

      {/* Form Section */}
      <section className="py-6 px-4 pb-20">
        <div className="max-w-xl mx-auto">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="bg-white rounded-3xl shadow-2xl overflow-hidden"
          >
            {/* Header */}
            <div className="bg-gradient-to-r from-orange-500 to-orange-600 p-6 md:p-8 text-center">
              <UserCircle2 className="w-12 h-12 text-white mx-auto mb-3 opacity-90" />
              <h1 className="text-2xl md:text-3xl font-light text-white mb-2">
                Schedule Your Site Visit
              </h1>
              <p className="text-orange-100 text-sm">
                Fill in your details and we'll get back to you shortly
              </p>
            </div>

            {/* Form */}
            <form onSubmit={handleSubmit} className="p-6 md:p-8 space-y-4">
              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2 flex items-center">
                  <User className="w-4 h-4 mr-2 text-orange-500" />
                  Enter your full name
                </label>
                <Input
                  required
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  placeholder="Your full name"
                  className="h-11 text-base border-gray-300 focus:border-orange-500 focus:ring-orange-500"
                />
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2 flex items-center">
                  <Phone className="w-4 h-4 mr-2 text-orange-500" />
                  Enter your phone number
                </label>
                <Input
                  required
                  type="tel"
                  value={formData.phone}
                  onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
                  placeholder="+91 98765 43210"
                  className="h-11 text-base border-gray-300 focus:border-orange-500 focus:ring-orange-500"
                />
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2 flex items-center">
                  <Mail className="w-4 h-4 mr-2 text-orange-500" />
                  Enter your email address
                </label>
                <Input
                  required
                  type="email"
                  value={formData.email}
                  onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                  placeholder="rahul.sharma@gmail.com"
                  className="h-11 text-base border-gray-300 focus:border-orange-500 focus:ring-orange-500"
                />
              </div>

              <Button
                type="submit"
                size="lg"
                disabled={isSubmitting}
                className="w-full h-12 bg-gradient-to-r from-orange-500 to-orange-600 hover:from-orange-600 hover:to-orange-700 text-white text-base font-semibold shadow-lg"
              >
                {isSubmitting ? (
                  <>Submitting...</>
                ) : (
                  <>
                    <Send className="w-5 h-5 mr-2" />
                    Submit & Get Access
                  </>
                )}
              </Button>

              <div className="flex items-center justify-center gap-1 text-xs text-gray-500">
                <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M5 9V7a5 5 0 0110 0v2a2 2 0 012 2v5a2 2 0 01-2 2H5a2 2 0 01-2-2v-5a2 2 0 012-2zm8-2v2H7V7a3 3 0 016 0z" clipRule="evenodd" />
                </svg>
                <span>Your information is secure</span>
              </div>
            </form>

            {/* Benefits Footer */}
            <div className="bg-gray-50 px-6 md:px-8 py-4 border-t border-gray-200">
              <p className="text-xs font-semibold text-gray-700 mb-2 text-center">
                By submitting this form, you'll get instant access to:
              </p>
              <div className="space-y-1.5">
                {[
                  'Detailed floor plans for all blocks',
                  'Location details and connectivity information',
                  'Exclusive pricing and payment plans'
                ].map((benefit, index) => (
                  <div key={index} className="flex items-center gap-2 text-xs text-gray-600">
                    <svg className="w-3.5 h-3.5 text-green-500 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                    </svg>
                    <span>{benefit}</span>
                  </div>
                ))}
              </div>
            </div>
          </motion.div>

          {/* Back Link */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.4 }}
            className="text-center mt-8"
          >
            <Link 
              to={createPageUrl('Anahata')} 
              className="text-gray-600 hover:text-orange-500 transition-colors text-sm inline-flex items-center gap-2"
            >
              ← Back to Anahata
            </Link>
          </motion.div>
        </div>
      </section>

      <Footer />
    </div>
  );
}