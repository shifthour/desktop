import React from 'react';
import { motion } from 'framer-motion';
import { Target, Lightbulb, Users, TrendingUp } from 'lucide-react';

const values = [
    {
        icon: Target,
        title: "Lean & Efficient",
        description: "Maximum results with minimal resources"
    },
    {
        icon: Lightbulb,
        title: "Innovation First",
        description: "AI-driven solutions for modern challenges"
    },
    {
        icon: Users,
        title: "Client Focused",
        description: "Your success is our priority"
    },
    {
        icon: TrendingUp,
        title: "Scalable Solutions",
        description: "Built to grow with your business"
    }
];

export default function About() {
    return (
        <section className="py-24 md:py-32 bg-slate-950 relative overflow-hidden" id="about">
            {/* Background elements */}
            <div className="absolute inset-0">
                <div className="absolute top-1/4 left-1/3 w-72 h-72 bg-blue-500/10 rounded-full blur-3xl" />
                <div className="absolute bottom-1/3 right-1/4 w-64 h-64 bg-green-500/10 rounded-full blur-3xl" />
            </div>

            <div className="max-w-7xl mx-auto px-6 relative z-10">
                <div className="grid lg:grid-cols-2 gap-16 items-center">
                    {/* Left - Content */}
                    <motion.div
                        initial={{ opacity: 0, x: -30 }}
                        whileInView={{ opacity: 1, x: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                    >
                        <span className="inline-block px-4 py-2 bg-white/5 border border-white/10 text-green-400 rounded-full text-sm font-medium mb-6">
                            About Stoptions
                        </span>
                        
                        <h2 className="text-4xl md:text-5xl font-bold text-white mb-6 leading-tight">
                            Doing More
                            <span className="block text-transparent bg-clip-text bg-gradient-to-r from-green-400 to-blue-400">
                                With Less
                            </span>
                        </h2>
                        
                        <p className="text-lg text-gray-400 mb-6 leading-relaxed">
                            At Stoptions, we believe in the power of smart technology. As a lean team of 
                            AI and software experts, we punch above our weight—delivering enterprise-grade 
                            solutions without the enterprise-level costs.
                        </p>
                        
                        <p className="text-gray-400 mb-10 leading-relaxed">
                            Our approach is simple: leverage artificial intelligence to amplify human 
                            creativity, automate the mundane, and focus resources where they matter most. 
                            The result? Faster delivery, smarter solutions, and better outcomes for our clients.
                        </p>

                        {/* Values Grid */}
                        <div className="grid grid-cols-2 gap-6">
                            {values.map((value, idx) => (
                                <motion.div
                                    key={idx}
                                    initial={{ opacity: 0, y: 20 }}
                                    whileInView={{ opacity: 1, y: 0 }}
                                    viewport={{ once: true }}
                                    transition={{ delay: idx * 0.1 }}
                                    className="group"
                                >
                                    <div className="flex items-start gap-4">
                                        <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-blue-500/20 to-green-500/20 flex items-center justify-center shrink-0 group-hover:scale-110 transition-transform">
                                            <value.icon className="w-5 h-5 text-green-400" />
                                        </div>
                                        <div>
                                            <h4 className="text-white font-semibold mb-1">{value.title}</h4>
                                            <p className="text-sm text-gray-500">{value.description}</p>
                                        </div>
                                    </div>
                                </motion.div>
                            ))}
                        </div>
                    </motion.div>

                    {/* Right - Visual */}
                    <motion.div
                        initial={{ opacity: 0, x: 30 }}
                        whileInView={{ opacity: 1, x: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6, delay: 0.2 }}
                        className="relative"
                    >
                        <div className="relative">
                            {/* Main image */}
                            <div className="relative rounded-3xl overflow-hidden shadow-2xl shadow-blue-500/10">
                                <img 
                                    src="https://images.unsplash.com/photo-1552664730-d307ca884978?w=800&q=80"
                                    alt="Team collaboration"
                                    className="w-full h-96 object-cover"
                                />
                                <div className="absolute inset-0 bg-gradient-to-t from-slate-950/80 via-transparent to-transparent" />
                            </div>

                            {/* Floating stat card */}
                            <motion.div
                                initial={{ opacity: 0, scale: 0.8 }}
                                whileInView={{ opacity: 1, scale: 1 }}
                                viewport={{ once: true }}
                                transition={{ delay: 0.5 }}
                                className="absolute -bottom-8 -left-8 bg-white rounded-2xl p-6 shadow-xl"
                            >
                                <div className="flex items-center gap-4">
                                    <div className="w-14 h-14 bg-gradient-to-br from-green-500 to-green-600 rounded-xl flex items-center justify-center">
                                        <TrendingUp className="w-7 h-7 text-white" />
                                    </div>
                                    <div>
                                        <div className="text-3xl font-bold text-slate-900">3x</div>
                                        <div className="text-sm text-slate-500">Faster Delivery</div>
                                    </div>
                                </div>
                            </motion.div>

                            {/* Floating badge */}
                            <motion.div
                                initial={{ opacity: 0, y: -20 }}
                                whileInView={{ opacity: 1, y: 0 }}
                                viewport={{ once: true }}
                                transition={{ delay: 0.6 }}
                                className="absolute -top-4 -right-4 bg-gradient-to-r from-blue-500 to-blue-600 text-white px-5 py-3 rounded-full shadow-lg shadow-blue-500/30 text-sm font-medium"
                            >
                                AI-Powered Team
                            </motion.div>
                        </div>
                    </motion.div>
                </div>
            </div>
        </section>
    );
}