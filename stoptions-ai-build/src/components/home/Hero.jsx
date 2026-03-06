import React from 'react';
import { motion } from 'framer-motion';
import { ArrowRight, Sparkles } from 'lucide-react';
import { Link } from 'react-router-dom';
import { Button } from "@/components/ui/button";

export default function Hero() {
    return (
        <section className="relative min-h-screen flex items-center justify-center overflow-hidden bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
            {/* Animated background elements */}
            <div className="absolute inset-0 overflow-hidden">
                <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-blue-500/10 rounded-full blur-3xl animate-pulse" />
                <div className="absolute bottom-1/4 right-1/4 w-80 h-80 bg-green-500/10 rounded-full blur-3xl animate-pulse delay-1000" />
                <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-indigo-500/5 rounded-full blur-3xl" />
            </div>

            {/* Grid pattern overlay */}
            <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiMyMDI5M2EiIGZpbGwtb3BhY2l0eT0iMC40Ij48cGF0aCBkPSJNMzYgMzRoLTJ2LTRoMnY0em0wLTZoLTJ2LTRoMnY0em0wLTZoLTJWMThoMnY0em0wLTZoLTJ2LTRoMnY0eiIvPjwvZz48L2c+PC9zdmc+')] opacity-20" />

            <div className="relative z-10 max-w-7xl mx-auto px-6 py-20">
                <div className="max-w-4xl mx-auto">
                    {/* Content */}
                    <motion.div
                        initial={{ opacity: 0, y: 30 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.8 }}
                        className="text-center"
                    >
                        <motion.div
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: 0.2 }}
                            className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-white/5 border border-white/10 backdrop-blur-sm mb-8"
                        >
                            <Sparkles className="w-4 h-4 text-green-400" />
                            <span className="text-sm text-gray-300">AI-Powered Solutions</span>
                        </motion.div>

                        <h1 className="text-5xl md:text-6xl lg:text-7xl font-bold text-white leading-tight mb-6">
                            Building the
                            <span className="block bg-gradient-to-r from-blue-400 via-green-400 to-blue-500 bg-clip-text text-transparent">
                                Future with AI
                            </span>
                        </h1>

                        <p className="text-lg md:text-xl text-gray-400 mb-10 max-w-xl mx-auto lg:mx-0 leading-relaxed">
                            We craft intelligent software solutions that maximize impact with minimal resources. 
                            Smart technology, lean execution, exceptional results.
                        </p>

                        <div className="flex flex-col sm:flex-row gap-4 justify-center">
                            <Link to="/Contact">
                                <Button
                                    size="lg"
                                    className="bg-gradient-to-r from-blue-600 to-blue-500 hover:from-blue-500 hover:to-blue-400 text-white px-8 py-6 text-lg rounded-xl shadow-lg shadow-blue-500/25 transition-all duration-300 hover:shadow-blue-500/40 hover:-translate-y-0.5"
                                >
                                    Start Your Project
                                    <ArrowRight className="w-5 h-5 ml-2" />
                                </Button>
                            </Link>
                            <Link to="/Products">
                                <Button
                                    variant="outline"
                                    size="lg"
                                    className="border-green-400/50 text-green-400 hover:bg-green-400/10 hover:border-green-400 px-8 py-6 text-lg rounded-xl backdrop-blur-sm"
                                >
                                    View Our Work
                                </Button>
                            </Link>
                        </div>

                        {/* Stats */}
                        <motion.div
                            initial={{ opacity: 0, y: 30 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: 0.6 }}
                            className="grid grid-cols-3 gap-8 mt-16 pt-10 border-t border-white/10 max-w-2xl mx-auto"
                        >
                            {[
                                { value: "50+", label: "Projects Delivered" },
                                { value: "95%", label: "Client Satisfaction" },
                                { value: "24/7", label: "Support Available" }
                            ].map((stat, idx) => (
                                <div key={idx} className="text-center">
                                    <div className="text-3xl md:text-4xl font-bold text-white mb-1">{stat.value}</div>
                                    <div className="text-sm text-gray-500">{stat.label}</div>
                                </div>
                            ))}
                        </motion.div>

                        {/* Trusted By - Scrolling Logos */}
                        <motion.div
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            transition={{ delay: 0.9 }}
                            className="mt-14"
                        >
                            <p className="text-sm text-gray-500 uppercase tracking-widest mb-6">Trusted By</p>
                            <div className="relative overflow-hidden max-w-3xl mx-auto">
                                {/* Fade edges */}
                                <div className="absolute left-0 top-0 bottom-0 w-16 bg-gradient-to-r from-slate-950 to-transparent z-10" />
                                <div className="absolute right-0 top-0 bottom-0 w-16 bg-gradient-to-l from-slate-950 to-transparent z-10" />

                                <div className="flex animate-scroll-logos">
                                    {[...Array(2)].map((_, setIdx) => (
                                        <div key={setIdx} className="flex items-center gap-12 shrink-0 px-6">
                                            {[
                                                { src: "/logos/client-safestorage.png", alt: "Safe Storage" },
                                                { src: "/logos/client-flatrix.png", alt: "Flatrix" },
                                                { src: "/logos/client-shifthour.png", alt: "Shift Hour" },
                                                { src: "/logos/client-lavos.png", alt: "Lavos" },
                                                { src: "/logos/client-pg.png", alt: "PG" },
                                                { src: "/logos/client-labgig.png", alt: "Labgigs" },
                                            ].map((logo, idx) => (
                                                <div key={idx} className="bg-white rounded-xl px-6 py-3 flex items-center justify-center shrink-0">
                                                    <img
                                                        src={logo.src}
                                                        alt={logo.alt}
                                                        className="h-8 md:h-10 w-auto object-contain"
                                                    />
                                                </div>
                                            ))}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </motion.div>
                    </motion.div>
                </div>
            </div>

            {/* Scroll indicator */}
            <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 1.5 }}
                className="absolute bottom-8 left-1/2 -translate-x-1/2"
            >
                <motion.div
                    animate={{ y: [0, 8, 0] }}
                    transition={{ duration: 1.5, repeat: Infinity }}
                    className="w-6 h-10 rounded-full border-2 border-white/20 flex items-start justify-center p-2"
                >
                    <div className="w-1.5 h-2.5 bg-white/50 rounded-full" />
                </motion.div>
            </motion.div>
        </section>
    );
}