import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Menu, X, Phone } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link, useNavigate } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import BrochureDownloadModal from '@/components/BrochureDownloadModal';

const navLinks = [
  { name: 'Home', path: 'Home' },
  { name: 'About', path: 'About' },
  { name: 'Projects', path: 'Projects' },
  { name: 'Gallery', path: 'Gallery' },
  { name: 'Blog', path: 'Blog' },
  { name: 'Contact', path: 'Contact' },
];

export default function Navbar() {
  const [isScrolled, setIsScrolled] = useState(false);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const [isBrochureModalOpen, setIsBrochureModalOpen] = useState(false);
  const navigate = useNavigate();

  const handleLogoClick = (e) => {
    e.preventDefault();
    navigate(createPageUrl('Home'));
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  useEffect(() => {
    const handleScroll = () => {
      setIsScrolled(window.scrollY > 50);
    };
    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  return (
    <>
      <motion.nav
        initial={{ y: -100 }}
        animate={{ y: 0 }}
        transition={{ duration: 0.6 }}
        className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 ${
          isScrolled 
            ? 'bg-white/95 backdrop-blur-md border-b border-gray-200 shadow-sm' 
            : 'bg-white/90 backdrop-blur-sm'
        }`}
      >
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center justify-between">
          {/* Logo with Tagline */}
          <a href="/" onClick={handleLogoClick} className="flex flex-col cursor-pointer">
            <img
              src="/assets/img/logo/logo1.png"
              alt="Ishtika Homes"
              className="h-16 w-auto"
            />
            <p className="text-[10px] text-gray-600 italic -mt-1">Homes That Breathe</p>
          </a>

          {/* Desktop Navigation */}
          <div className="hidden lg:flex items-center gap-8">
            {navLinks.map((link) => (
              <Link
                key={link.name}
                to={createPageUrl(link.path)}
                className="text-gray-700 hover:text-orange-500 text-sm tracking-wide transition-colors font-medium"
              >
                {link.name}
              </Link>
            ))}
          </div>

          {/* Right side actions */}
          <div className="flex items-center gap-4">
            <div className="hidden md:flex">
              <Button
                variant="outline"
                size="sm"
                style={{ borderColor: '#FF8C00', color: '#FF8C00' }}
                className="hover:bg-orange-50"
                onClick={() => setIsBrochureModalOpen(true)}
              >
                Download Brochure
              </Button>
            </div>

            <a
              href="tel:+917338628777"
              className="flex items-center gap-2 text-orange-500 hover:text-orange-600 transition-colors font-medium"
            >
              <Phone className="w-4 h-4" />
              <span className="text-sm">+91 7338628777</span>
            </a>

            <Button
              variant="ghost"
              size="icon"
              className="lg:hidden text-gray-700"
              onClick={() => setIsMobileMenuOpen(true)}
            >
              <Menu className="w-6 h-6" />
            </Button>
          </div>
        </div>

      </motion.nav>

      {/* Mobile Menu */}
      <AnimatePresence>
        {isMobileMenuOpen && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 bg-white z-50 lg:hidden"
          >
            <div className="p-4 flex justify-between items-center border-b border-gray-200">
              <a href="/" onClick={(e) => { handleLogoClick(e); setIsMobileMenuOpen(false); }} className="flex items-center cursor-pointer">
                <img
                  src="/assets/img/logo/logo1.png"
                  alt="Ishtika Homes"
                  className="h-12 w-auto"
                />
              </a>
              <Button
                variant="ghost"
                size="icon"
                className="text-gray-700"
                onClick={() => setIsMobileMenuOpen(false)}
              >
                <X className="w-6 h-6" />
              </Button>
            </div>

            <div className="p-8 space-y-6">
              {navLinks.map((link, index) => (
                <motion.div
                  key={link.name}
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: index * 0.1 }}
                >
                  <Link
                    to={createPageUrl(link.path)}
                    className="block text-2xl text-gray-800 hover:text-orange-500 transition-colors font-medium"
                    onClick={() => setIsMobileMenuOpen(false)}
                  >
                    {link.name}
                  </Link>
                </motion.div>
              ))}

              <motion.a
                href="tel:+917338628777"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.6 }}
                className="flex items-center gap-2 text-orange-500 mt-8 pt-8 border-t border-gray-200"
              >
                <Phone className="w-5 h-5" />
                <span>+91 7338628777</span>
              </motion.a>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Brochure Download Modal */}
      <BrochureDownloadModal
        isOpen={isBrochureModalOpen}
        onClose={() => setIsBrochureModalOpen(false)}
      />
    </>
  );
}