import React from 'react';
import { motion } from 'framer-motion';
import { CheckCircle, Home, ArrowRight, Phone } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';

export default function AnahataThankYou() {
  return (
    <div className="bg-white min-h-screen pt-24">
      <Navbar />
      <WhatsAppButton message="Hi, I just submitted a site visit request for Anahata. I have some questions." />

      <section className="py-20 px-4 flex items-center justify-center min-h-[80vh]">
        <div className="max-w-2xl mx-auto text-center">
          <motion.div
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ duration: 0.5 }}
          >
            <div className="w-24 h-24 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-8">
              <CheckCircle className="w-14 h-14 text-green-500" />
            </div>

            <h1 className="text-4xl md:text-5xl font-light text-gray-800 mb-6">
              Thank You!
            </h1>

            <p className="text-xl text-gray-600 mb-4">
              Your site visit request has been received successfully.
            </p>

            <p className="text-gray-600 mb-12 leading-relaxed">
              Our team will contact you within 24 hours to confirm your visit details. 
              We're excited to show you your future home at Anahata!
            </p>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="bg-orange-50 border-2 border-orange-200 rounded-2xl p-8 mb-8"
            >
              <h3 className="text-lg font-semibold text-gray-800 mb-4">
                What happens next?
              </h3>
              <ul className="text-left space-y-3 text-gray-600">
                <li className="flex items-start gap-3">
                  <CheckCircle className="w-5 h-5 text-orange-500 mt-0.5 flex-shrink-0" />
                  <span>Our sales team will call you to confirm the date and time</span>
                </li>
                <li className="flex items-start gap-3">
                  <CheckCircle className="w-5 h-5 text-orange-500 mt-0.5 flex-shrink-0" />
                  <span>You'll receive a confirmation email with visit details</span>
                </li>
                <li className="flex items-start gap-3">
                  <CheckCircle className="w-5 h-5 text-orange-500 mt-0.5 flex-shrink-0" />
                  <span>A property expert will give you a complete tour of Anahata</span>
                </li>
                <li className="flex items-start gap-3">
                  <CheckCircle className="w-5 h-5 text-orange-500 mt-0.5 flex-shrink-0" />
                  <span>All your questions will be answered by our team</span>
                </li>
              </ul>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.5 }}
              className="flex flex-col sm:flex-row gap-4 justify-center"
            >
              <Link to={createPageUrl("Home")}>
                <Button size="lg" variant="outline" className="border-orange-500 text-orange-500 hover:bg-orange-50 rounded-full px-8">
                  <Home className="w-5 h-5 mr-2" />
                  Back to Home
                </Button>
              </Link>
              <Link to={createPageUrl("Anahata")}>
                <Button size="lg" className="bg-orange-500 hover:bg-orange-600 text-white rounded-full px-8">
                  View Anahata Details
                  <ArrowRight className="w-5 h-5 ml-2" />
                </Button>
              </Link>
            </motion.div>

            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.7 }}
              className="mt-12 pt-8 border-t border-gray-200"
            >
              <p className="text-gray-600 text-sm mb-2">Need immediate assistance?</p>
              <a href="tel:+917338628777" className="text-orange-500 font-semibold text-lg hover:text-orange-600 flex items-center justify-center gap-2">
                <Phone className="w-5 h-5" />
                +91 7338628777
              </a>
            </motion.div>
          </motion.div>
        </div>
      </section>

      <Footer />
    </div>
  );
}