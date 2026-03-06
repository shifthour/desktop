import React from 'react';
import { motion } from 'framer-motion';
import { ArrowRight, Sparkles } from 'lucide-react';
import { Link } from 'react-router-dom';
import { Button } from "@/components/ui/button";

export default function CTA() {
    return (
        <section className="py-24 md:py-32 bg-white dark:bg-slate-950 relative overflow-hidden">
            {/* Background */}
            <div className="absolute inset-0">
                <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[400px] bg-gradient-to-r from-blue-500/10 via-green-500/10 to-blue-500/10 rounded-full blur-3xl" />
            </div>

            <div className="max-w-4xl mx-auto px-6 relative z-10">
                <motion.div
                    initial={{ opacity: 0, y: 40 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.7 }}
                    className="text-center"
                >
                    <motion.div
                        initial={{ opacity: 0, scale: 0.9 }}
                        whileInView={{ opacity: 1, scale: 1 }}
                        viewport={{ once: true }}
                        transition={{ delay: 0.2 }}
                        className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-green-50 dark:bg-green-500/10 border border-green-100 dark:border-green-500/20 mb-8"
                    >
                        <Sparkles className="w-4 h-4 text-green-500" />
                        <span className="text-sm text-green-600 dark:text-green-400 font-medium">Ready to start?</span>
                    </motion.div>

                    <h2 className="text-4xl md:text-5xl lg:text-6xl font-bold text-slate-900 dark:text-white leading-tight mb-6">
                        Let's Build
                        <span className="block text-transparent bg-clip-text bg-gradient-to-r from-green-500 to-blue-500">
                            Something Great
                        </span>
                    </h2>

                    <p className="text-lg md:text-xl text-slate-600 dark:text-gray-400 mb-10 max-w-2xl mx-auto leading-relaxed">
                        Have a project in mind? We'd love to hear about it.
                        Tell us what you need and let's explore how we can help.
                    </p>

                    <div className="flex flex-col sm:flex-row gap-4 justify-center">
                        <Link to="/Contact">
                            <Button
                                size="lg"
                                className="bg-gradient-to-r from-blue-600 to-blue-500 hover:from-blue-500 hover:to-blue-400 text-white px-8 py-6 text-lg rounded-xl shadow-lg shadow-blue-500/25 transition-all duration-300 hover:shadow-blue-500/40 hover:-translate-y-0.5"
                            >
                                Contact Us
                                <ArrowRight className="w-5 h-5 ml-2" />
                            </Button>
                        </Link>
                        <Link to="/Products">
                            <Button
                                variant="outline"
                                size="lg"
                                className="border-slate-200 dark:border-white/10 text-slate-700 dark:text-gray-300 hover:bg-slate-50 dark:hover:bg-white/5 px-8 py-6 text-lg rounded-xl"
                            >
                                Explore Products
                            </Button>
                        </Link>
                    </div>
                </motion.div>
            </div>
        </section>
    );
}
