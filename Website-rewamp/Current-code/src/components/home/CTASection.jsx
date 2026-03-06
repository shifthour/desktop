import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Button } from "@/components/ui/button";
import { Phone, Mail, Calendar, Download } from 'lucide-react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import EditableBackgroundImage from '@/components/EditableBackgroundImage';
import BrochureDownloadModal from '@/components/BrochureDownloadModal';

export default function CTASection() {
  const [isBrochureModalOpen, setIsBrochureModalOpen] = useState(false);
  return (
    <>
    <EditableBackgroundImage
      imageKey="cta-background"
      src=""
      className="relative pt-12 md:pt-16 pb-12 md:pb-16 px-4 overflow-hidden bg-gradient-to-br from-orange-500 to-orange-600"
      style={{ backgroundSize: 'cover', backgroundPosition: 'center' }}
    >
      {/* Pattern overlay */}
      <div className="absolute inset-0 opacity-10 pointer-events-none">
        <div className="absolute inset-0" style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='1'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")`,
        }} />
      </div>

      <div className="relative max-w-5xl mx-auto text-center">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
        >
          <h2 className="text-3xl md:text-5xl font-light text-white mb-6 leading-tight">
            Ready to Find Your Dream Home?
          </h2>
          
          <p className="text-white/90 text-lg mb-12 max-w-2xl mx-auto">
            Take the first step towards owning a beautiful, Vastu-compliant home in one of Bangalore's premium locations.
          </p>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 max-w-4xl mx-auto">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.1 }}
              viewport={{ once: true }}
            >
              <Button 
                size="lg"
                variant="outline"
                className="w-full border-2 border-white bg-transparent hover:bg-white text-white hover:text-orange-500 transition-colors group"
                asChild
              >
                <Link to={createPageUrl('AnahataBookSiteVisit')}>
                  <Calendar className="w-5 h-5 mr-2 text-white group-hover:text-orange-500 transition-colors" />
                  Book Site Visit
                </Link>
              </Button>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.2 }}
              viewport={{ once: true }}
            >
              <Button
                size="lg"
                variant="outline"
                className="w-full border-2 border-white bg-transparent hover:bg-white text-white hover:text-orange-500 transition-colors group"
                onClick={() => setIsBrochureModalOpen(true)}
              >
                <Download className="w-5 h-5 mr-2 text-white group-hover:text-orange-500 transition-colors" />
                Get Brochure
              </Button>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.3 }}
              viewport={{ once: true }}
            >
              <Button 
                size="lg"
                variant="outline"
                className="w-full border-2 border-white bg-transparent hover:bg-white text-white hover:text-orange-500 transition-colors group"
                asChild
              >
                <a href="tel:+917338628777" className="flex items-center justify-center">
                  <Phone className="w-5 h-5 mr-2 text-white group-hover:text-orange-500 transition-colors" />
                  Call Now
                </a>
              </Button>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.4 }}
              viewport={{ once: true }}
            >
              <Button 
                size="lg"
                variant="outline"
                className="w-full border-2 border-white bg-transparent hover:bg-white text-white hover:text-orange-500 transition-colors group"
                asChild
              >
                <a href="mailto:info@ishtikahomes.com" className="flex items-center justify-center">
                  <Mail className="w-5 h-5 mr-2 text-white group-hover:text-orange-500 transition-colors" />
                  Email Us
                </a>
              </Button>
            </motion.div>
          </div>

          <motion.div
            initial={{ opacity: 0 }}
            whileInView={{ opacity: 1 }}
            transition={{ duration: 0.8, delay: 0.6 }}
            viewport={{ once: true }}
            className="mt-12 pt-8 border-t border-white/30"
          >
            <p className="text-white/80 text-sm">
              Our team is available Monday to Sunday 9:00 AM - 9:00 PM
            </p>
            <p className="text-white font-semibold text-lg mt-2">
              +91 7338628777
            </p>
          </motion.div>
        </motion.div>
      </div>
    </EditableBackgroundImage>

    {/* Brochure Download Modal */}
    <BrochureDownloadModal
      isOpen={isBrochureModalOpen}
      onClose={() => setIsBrochureModalOpen(false)}
    />
    </>
  );
}