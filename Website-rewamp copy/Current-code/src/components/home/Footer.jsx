import React from 'react';
import { Phone, Mail, MapPin, Facebook, Instagram, Linkedin, Youtube, Calendar } from 'lucide-react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { Button } from "@/components/ui/button";

export default function Footer() {
  return (
    <footer className="bg-gray-900 border-t border-gray-800">
      <div className="max-w-7xl mx-auto px-4 py-16">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8 md:gap-12">
          {/* Brand */}
          <div className="col-span-2 md:col-span-1">
            <h3 className="text-3xl font-light italic text-white mb-2">ishtika</h3>
            <p className="text-orange-400 text-sm tracking-wider mb-4">HOMES</p>
            <p className="text-white/50 text-sm leading-relaxed">
              Homes That Breathe — Creating thoughtfully designed living spaces with Vastu harmony and modern comfort.
            </p>
          </div>

          {/* Quick Links */}
          <div>
            <h4 className="text-white font-medium mb-6">Quick Links</h4>
            <ul className="space-y-3">
              {['Home', 'About', 'Projects', 'Gallery', 'Blog', 'Contact'].map((link) => (
                <li key={link}>
                  <Link to={createPageUrl(link)} className="text-white/50 hover:text-orange-400 text-sm transition-colors">
                    {link}
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Projects */}
          <div>
            <h4 className="text-white font-medium mb-6">Our Projects</h4>
            <ul className="space-y-3">
              {[
                { name: 'Anahata - Whitefield', page: 'Anahata' },
                { name: 'Vashishta - JP Nagar', page: 'Vashishta' },
                { name: 'Krishna - Hosapete', page: 'Krishna' },
                { name: 'Naadam', page: 'Naadam' },
                { name: 'Vyasa', page: 'Vyasa' },
                { name: 'Agastya', page: 'Agastya' },
              ].map((project) => (
                <li key={project.name}>
                  <Link to={createPageUrl(project.page)} className="text-white/50 hover:text-orange-400 text-sm transition-colors">
                    {project.name}
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Contact */}
          <div className="col-span-2 md:col-span-1">
            <h4 className="text-white font-medium mb-6">Contact Us</h4>
            <ul className="space-y-4">
              <li className="flex items-start gap-3">
                <Phone className="w-4 h-4 text-orange-400 mt-1" />
                <span className="text-white/50 text-sm">+91 7338628777</span>
              </li>
              <li className="flex items-start gap-3">
                <Mail className="w-4 h-4 text-orange-400 mt-1" />
                <span className="text-white/50 text-sm">info@ishtikahomes.com</span>
              </li>
              <li className="flex items-start gap-3">
                <MapPin className="w-4 h-4 text-orange-400 mt-1" />
                <span className="text-white/50 text-sm">Bangalore, Karnataka, India</span>
              </li>
            </ul>

            <div className="flex gap-4 mt-4 md:mt-6">
              <a href="https://www.instagram.com/ishtika.homes/" target="_blank" rel="noopener noreferrer" className="w-10 h-10 bg-zinc-900 rounded-full flex items-center justify-center text-white/50 hover:bg-orange-400 hover:text-black transition-all">
                <Instagram className="w-4 h-4" />
              </a>
              <a href="https://www.facebook.com/theishtikahomes" target="_blank" rel="noopener noreferrer" className="w-10 h-10 bg-zinc-900 rounded-full flex items-center justify-center text-white/50 hover:bg-orange-400 hover:text-black transition-all">
                <Facebook className="w-4 h-4" />
              </a>
              <a href="https://www.linkedin.com/company/ishtika-homes" target="_blank" rel="noopener noreferrer" className="w-10 h-10 bg-zinc-900 rounded-full flex items-center justify-center text-white/50 hover:bg-orange-400 hover:text-black transition-all">
                <Linkedin className="w-4 h-4" />
              </a>
              <a href="https://www.youtube.com/@IshtikaHomes" target="_blank" rel="noopener noreferrer" className="w-10 h-10 bg-zinc-900 rounded-full flex items-center justify-center text-white/50 hover:bg-orange-400 hover:text-black transition-all">
                <Youtube className="w-4 h-4" />
              </a>
            </div>
          </div>
        </div>

        <div className="border-t border-zinc-800 mt-8 md:mt-12 pt-8 flex flex-col md:flex-row justify-between items-center gap-4">
          <p className="text-white/40 text-sm">
            © 2024 Ishtika Homes. All rights reserved.
          </p>
          <div className="flex flex-wrap items-center justify-center gap-4 md:gap-6">
            <a href="#" className="text-white/40 hover:text-orange-400 text-sm transition-colors">
              Privacy Policy
            </a>
            <a href="#" className="text-white/40 hover:text-orange-400 text-sm transition-colors">
              Terms of Service
            </a>
          </div>
        </div>
      </div>

      {/* Fixed CTA Buttons - Mobile Only */}
      <div className="md:hidden fixed bottom-0 left-0 right-0 z-50 bg-white border-t border-gray-200 p-3 flex gap-2">
        <Link to={createPageUrl("AnahataBookSiteVisit")} className="flex-1">
          <Button
            size="lg"
            style={{ backgroundColor: '#FF8C00', color: 'white' }}
            className="w-full py-3 text-xs font-medium"
          >
            <Calendar className="w-4 h-4 mr-1" />
            Book Site Visit
          </Button>
        </Link>
        <a href="tel:+917338628777" className="flex-1">
          <Button
            size="lg"
            variant="outline"
            style={{ borderColor: '#FF8C00', color: '#FF8C00' }}
            className="w-full py-3 text-xs font-medium"
          >
            <Phone className="w-4 h-4 mr-1" />
            Call Us Now
          </Button>
        </a>
      </div>
    </footer>
  );
}
