import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from './utils';
import { motion, AnimatePresence } from 'framer-motion';
import { Menu, X } from 'lucide-react';

const navLinks = [
    { label: "Home", href: "/", isPage: true },
    { label: "About", href: "/About", isPage: true },
    { label: "Products", href: "/Products", isPage: true },
    { label: "Contact", href: "/Contact", isPage: true }
];

export default function Layout({ children }) {
    const [isScrolled, setIsScrolled] = useState(false);
    const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
    useEffect(() => {
        const handleScroll = () => {
            setIsScrolled(window.scrollY > 50);
        };
        window.addEventListener('scroll', handleScroll);
        return () => window.removeEventListener('scroll', handleScroll);
    }, []);

    return (
        <div className="min-h-screen bg-slate-950">
            {/* Navigation */}
            <nav className={`fixed top-0 left-0 right-0 z-50 transition-all duration-500 ${
                isScrolled 
                    ? 'bg-slate-950/90 backdrop-blur-xl border-b border-white/5' 
                    : 'bg-transparent'
            }`}>
                <div className="max-w-7xl mx-auto px-6">
                    <div className="flex items-center justify-between h-20">
                        {/* Logo */}
                        <Link to={createPageUrl('Home')} className="flex items-center gap-3">
                            <img 
                                src="https://qtrypzzcjebvfcihiynt.supabase.co/storage/v1/object/public/base44-prod/public/user_6862c0ee98b2f361bc4f4e97/58da6e379_logo.png"
                                alt="Stoptions"
                                className="w-10 h-10 object-contain"
                            />
                            <span className="text-xl font-bold text-white">Stoptions</span>
                        </Link>

                        {/* Desktop Nav */}
                        <div className="hidden md:flex items-center gap-8">
                            {navLinks.map((link, idx) => (
                                link.isPage ? (
                                    <Link
                                        key={idx}
                                        to={link.href}
                                        className="text-gray-300 hover:text-white transition-colors text-sm font-medium"
                                    >
                                        {link.label}
                                    </Link>
                                ) : (
                                    <a
                                        key={idx}
                                        href={link.href}
                                        className="text-gray-300 hover:text-white transition-colors text-sm font-medium"
                                    >
                                        {link.label}
                                    </a>
                                )
                            ))}
                        </div>

                        <div className="hidden md:block" />

                        {/* Mobile Menu Button */}
                        <button
                            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                            className="md:hidden text-white p-2"
                        >
                            {mobileMenuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
                        </button>
                    </div>
                </div>

                {/* Mobile Menu */}
                <AnimatePresence>
                    {mobileMenuOpen && (
                        <motion.div
                            initial={{ opacity: 0, height: 0 }}
                            animate={{ opacity: 1, height: 'auto' }}
                            exit={{ opacity: 0, height: 0 }}
                            className="md:hidden bg-slate-950/95 backdrop-blur-xl border-b border-white/5"
                        >
                            <div className="px-6 py-6 space-y-4">
                                {navLinks.map((link, idx) => (
                                    link.isPage ? (
                                        <Link
                                            key={idx}
                                            to={link.href}
                                            onClick={() => setMobileMenuOpen(false)}
                                            className="block text-gray-300 hover:text-white transition-colors py-2 text-lg"
                                        >
                                            {link.label}
                                        </Link>
                                    ) : (
                                        <a
                                            key={idx}
                                            href={link.href}
                                            onClick={() => setMobileMenuOpen(false)}
                                            className="block text-gray-300 hover:text-white transition-colors py-2 text-lg"
                                        >
                                            {link.label}
                                        </a>
                                    )
                                ))}
                            </div>
                        </motion.div>
                    )}
                </AnimatePresence>
            </nav>

            {/* Page Content */}
            <main>
                {children}
            </main>
        </div>
    );
}