import React from 'react';
import { motion } from 'framer-motion';
import { Target, Lightbulb, Users, TrendingUp, Zap, Heart, ArrowRight, Code2, Brain, Rocket } from 'lucide-react';
import { Link } from 'react-router-dom';
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import Footer from '../components/home/Footer';

const values = [
    {
        icon: Target,
        title: "Lean & Efficient",
        description: "We maximize results with minimal resources. No bloated teams, no wasted cycles — just focused execution that delivers real outcomes.",
        gradient: "from-blue-500 to-blue-600"
    },
    {
        icon: Lightbulb,
        title: "Innovation First",
        description: "We stay on the cutting edge of AI and software development, bringing the latest tools and techniques to every project we take on.",
        gradient: "from-green-500 to-emerald-600"
    },
    {
        icon: Heart,
        title: "Client Focused",
        description: "Your success is our success. We build long-term partnerships, not one-off projects. Every decision is made with your goals in mind.",
        gradient: "from-purple-500 to-purple-600"
    },
    {
        icon: Zap,
        title: "Scalable Solutions",
        description: "Everything we build is designed to grow with your business. From MVP to enterprise scale, our architecture evolves with you.",
        gradient: "from-orange-500 to-red-600"
    }
];

const stats = [
    { value: "50+", label: "Projects Delivered" },
    { value: "3x", label: "Faster Than Traditional Teams" },
    { value: "95%", label: "Client Satisfaction" },
    { value: "24/7", label: "Support Available" }
];

const team = [
    {
        role: "AI & ML Engineers",
        icon: Brain,
        description: "Experts in TensorFlow, PyTorch, and LLM integration who turn complex data into intelligent products.",
        gradient: "from-purple-500 to-indigo-600"
    },
    {
        role: "Full-Stack Developers",
        icon: Code2,
        description: "Senior engineers building scalable applications with React, Node.js, Python, and cloud-native architectures.",
        gradient: "from-blue-500 to-cyan-600"
    },
    {
        role: "Product Strategists",
        icon: Rocket,
        description: "Strategic thinkers who bridge the gap between business goals and technical solutions.",
        gradient: "from-green-500 to-emerald-600"
    }
];

const milestones = [
    { year: "2022", title: "Founded", description: "Started with a mission to make AI accessible to businesses of all sizes." },
    { year: "2023", title: "First 20 Projects", description: "Delivered AI solutions across healthcare, fintech, and e-commerce." },
    { year: "2024", title: "Product Launch", description: "Launched our own suite of AI-powered products alongside client work." },
    { year: "2025", title: "Scaling Up", description: "Expanded our team and capabilities while keeping our lean philosophy." }
];

export default function About() {
    return (
        <div>
            {/* Hero */}
            <section className="relative pt-32 pb-20 bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 overflow-hidden">
                <div className="absolute inset-0">
                    <div className="absolute top-1/4 left-1/3 w-96 h-96 bg-green-500/10 rounded-full blur-3xl animate-pulse" />
                    <div className="absolute bottom-1/3 right-1/4 w-80 h-80 bg-blue-500/10 rounded-full blur-3xl animate-pulse delay-1000" />
                </div>

                <div className="max-w-7xl mx-auto px-6 relative z-10">
                    <div className="grid lg:grid-cols-2 gap-16 items-center">
                        <motion.div
                            initial={{ opacity: 0, x: -30 }}
                            animate={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.8 }}
                        >
                            <span className="inline-block px-4 py-2 bg-green-500/10 border border-green-500/20 text-green-400 rounded-full text-sm font-medium mb-6">
                                About Stoptions
                            </span>

                            <h1 className="text-5xl md:text-6xl font-bold text-white leading-tight mb-6">
                                Doing More
                                <span className="block bg-gradient-to-r from-green-400 to-blue-400 bg-clip-text text-transparent">
                                    With Less
                                </span>
                            </h1>

                            <p className="text-lg text-gray-400 mb-6 leading-relaxed">
                                At Stoptions, we believe in the power of smart technology. As a lean team of
                                AI and software experts, we punch above our weight — delivering enterprise-grade
                                solutions without the enterprise-level costs.
                            </p>

                            <p className="text-gray-400 leading-relaxed">
                                Our approach is simple: leverage artificial intelligence to amplify human
                                creativity, automate the mundane, and focus resources where they matter most.
                                The result? Faster delivery, smarter solutions, and better outcomes for our clients.
                            </p>
                        </motion.div>

                        <motion.div
                            initial={{ opacity: 0, x: 30 }}
                            animate={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.8, delay: 0.2 }}
                            className="relative"
                        >
                            <div className="relative rounded-3xl overflow-hidden shadow-2xl shadow-blue-500/10">
                                <img
                                    src="https://images.unsplash.com/photo-1552664730-d307ca884978?w=800&q=80"
                                    alt="Team collaboration"
                                    className="w-full h-96 object-cover"
                                />
                                <div className="absolute inset-0 bg-gradient-to-t from-slate-950/80 via-transparent to-transparent" />
                            </div>

                            <motion.div
                                initial={{ opacity: 0, scale: 0.8 }}
                                animate={{ opacity: 1, scale: 1 }}
                                transition={{ delay: 0.6 }}
                                className="absolute -bottom-6 -left-6 bg-white rounded-2xl p-5 shadow-xl"
                            >
                                <div className="flex items-center gap-3">
                                    <div className="w-12 h-12 bg-gradient-to-br from-green-500 to-green-600 rounded-xl flex items-center justify-center">
                                        <TrendingUp className="w-6 h-6 text-white" />
                                    </div>
                                    <div>
                                        <div className="text-2xl font-bold text-slate-900">3x</div>
                                        <div className="text-sm text-slate-500">Faster Delivery</div>
                                    </div>
                                </div>
                            </motion.div>

                            <motion.div
                                initial={{ opacity: 0, y: -20 }}
                                animate={{ opacity: 1, y: 0 }}
                                transition={{ delay: 0.7 }}
                                className="absolute -top-4 -right-4 bg-gradient-to-r from-blue-500 to-blue-600 text-white px-5 py-3 rounded-full shadow-lg shadow-blue-500/30 text-sm font-medium"
                            >
                                AI-Powered Team
                            </motion.div>
                        </motion.div>
                    </div>
                </div>
            </section>

            {/* Stats */}
            <section className="py-16 bg-slate-950 border-y border-white/5">
                <div className="max-w-7xl mx-auto px-6">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
                        {stats.map((stat, idx) => (
                            <motion.div
                                key={idx}
                                initial={{ opacity: 0, y: 20 }}
                                whileInView={{ opacity: 1, y: 0 }}
                                viewport={{ once: true }}
                                transition={{ delay: idx * 0.1 }}
                                className="text-center"
                            >
                                <div className="text-3xl md:text-4xl font-bold text-white mb-1">{stat.value}</div>
                                <div className="text-sm text-gray-500">{stat.label}</div>
                            </motion.div>
                        ))}
                    </div>
                </div>
            </section>

            {/* Values */}
            <section className="py-24 bg-slate-950 relative overflow-hidden">
                <div className="absolute inset-0">
                    <div className="absolute top-1/3 right-1/4 w-80 h-80 bg-blue-500/5 rounded-full blur-3xl" />
                </div>

                <div className="max-w-7xl mx-auto px-6 relative z-10">
                    <motion.div
                        initial={{ opacity: 0, y: 30 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                        className="text-center mb-16"
                    >
                        <span className="inline-flex items-center gap-2 px-4 py-2 bg-white/5 border border-white/10 text-blue-400 rounded-full text-sm font-medium mb-4">
                            Our Values
                        </span>
                        <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">
                            What Drives
                            <span className="block text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-green-400">
                                Everything We Do
                            </span>
                        </h2>
                    </motion.div>

                    <div className="grid md:grid-cols-2 gap-6">
                        {values.map((value, idx) => (
                            <motion.div
                                key={idx}
                                initial={{ opacity: 0, y: 30 }}
                                whileInView={{ opacity: 1, y: 0 }}
                                viewport={{ once: true }}
                                transition={{ duration: 0.5, delay: idx * 0.1 }}
                            >
                                <Card className="p-8 bg-white/5 border-white/10 hover:border-white/20 hover:bg-white/[0.08] transition-all duration-300 group h-full">
                                    <div className="flex items-start gap-5">
                                        <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${value.gradient} flex items-center justify-center shadow-lg shrink-0 group-hover:scale-110 transition-transform`}>
                                            <value.icon className="w-6 h-6 text-white" />
                                        </div>
                                        <div>
                                            <h3 className="text-xl font-semibold text-white mb-2">{value.title}</h3>
                                            <p className="text-gray-400 leading-relaxed">{value.description}</p>
                                        </div>
                                    </div>
                                </Card>
                            </motion.div>
                        ))}
                    </div>
                </div>
            </section>

            {/* Team */}
            <section className="py-24 bg-gradient-to-br from-slate-900 to-slate-950 relative overflow-hidden">
                <div className="absolute inset-0">
                    <div className="absolute bottom-1/4 left-1/3 w-72 h-72 bg-green-500/5 rounded-full blur-3xl" />
                </div>

                <div className="max-w-7xl mx-auto px-6 relative z-10">
                    <motion.div
                        initial={{ opacity: 0, y: 30 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                        className="text-center mb-16"
                    >
                        <span className="inline-flex items-center gap-2 px-4 py-2 bg-white/5 border border-white/10 text-green-400 rounded-full text-sm font-medium mb-4">
                            <Users className="w-4 h-4" />
                            Our Team
                        </span>
                        <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">
                            Small Team,
                            <span className="block text-transparent bg-clip-text bg-gradient-to-r from-green-400 to-blue-400">
                                Big Impact
                            </span>
                        </h2>
                        <p className="text-lg text-gray-400 max-w-2xl mx-auto">
                            We're a focused team of senior specialists. By leveraging AI in our own workflow,
                            we deliver what traditionally requires much larger teams.
                        </p>
                    </motion.div>

                    <div className="grid md:grid-cols-3 gap-6">
                        {team.map((member, idx) => (
                            <motion.div
                                key={idx}
                                initial={{ opacity: 0, y: 30 }}
                                whileInView={{ opacity: 1, y: 0 }}
                                viewport={{ once: true }}
                                transition={{ duration: 0.5, delay: idx * 0.15 }}
                            >
                                <Card className="p-8 bg-white/5 border-white/10 hover:border-white/20 hover:bg-white/[0.08] transition-all duration-300 group text-center h-full">
                                    <div className={`w-16 h-16 rounded-2xl bg-gradient-to-br ${member.gradient} flex items-center justify-center shadow-lg mx-auto mb-6 group-hover:scale-110 transition-transform`}>
                                        <member.icon className="w-8 h-8 text-white" />
                                    </div>
                                    <h3 className="text-xl font-semibold text-white mb-3">{member.role}</h3>
                                    <p className="text-gray-400 leading-relaxed text-sm">{member.description}</p>
                                </Card>
                            </motion.div>
                        ))}
                    </div>
                </div>
            </section>

            {/* Milestones */}
            <section className="py-24 bg-slate-950 relative overflow-hidden">
                <div className="absolute inset-0">
                    <div className="absolute top-1/2 right-1/3 w-80 h-80 bg-blue-500/5 rounded-full blur-3xl" />
                </div>

                <div className="max-w-4xl mx-auto px-6 relative z-10">
                    <motion.div
                        initial={{ opacity: 0, y: 30 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                        className="text-center mb-16"
                    >
                        <span className="inline-flex items-center gap-2 px-4 py-2 bg-white/5 border border-white/10 text-blue-400 rounded-full text-sm font-medium mb-4">
                            Our Journey
                        </span>
                        <h2 className="text-4xl md:text-5xl font-bold text-white">
                            Milestones
                        </h2>
                    </motion.div>

                    <div className="space-y-0">
                        {milestones.map((milestone, idx) => (
                            <motion.div
                                key={idx}
                                initial={{ opacity: 0, x: idx % 2 === 0 ? -20 : 20 }}
                                whileInView={{ opacity: 1, x: 0 }}
                                viewport={{ once: true }}
                                transition={{ duration: 0.5, delay: idx * 0.1 }}
                                className="flex gap-6 group"
                            >
                                {/* Timeline line */}
                                <div className="flex flex-col items-center">
                                    <div className="w-12 h-12 rounded-full bg-gradient-to-br from-blue-500/20 to-green-500/20 border border-white/10 flex items-center justify-center shrink-0 group-hover:scale-110 transition-transform">
                                        <span className="text-xs font-bold text-blue-400">{milestone.year}</span>
                                    </div>
                                    {idx < milestones.length - 1 && (
                                        <div className="w-px flex-1 bg-gradient-to-b from-white/10 to-transparent min-h-[40px]" />
                                    )}
                                </div>

                                <div className="pb-10">
                                    <h3 className="text-lg font-semibold text-white mb-1">{milestone.title}</h3>
                                    <p className="text-gray-400 text-sm leading-relaxed">{milestone.description}</p>
                                </div>
                            </motion.div>
                        ))}
                    </div>
                </div>
            </section>

            {/* CTA */}
            <section className="py-24 bg-gradient-to-br from-slate-900 to-slate-950 relative overflow-hidden">
                <div className="absolute inset-0">
                    <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-green-500/5 rounded-full blur-3xl" />
                </div>
                <div className="max-w-3xl mx-auto px-6 text-center relative z-10">
                    <motion.div
                        initial={{ opacity: 0, y: 30 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                    >
                        <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">
                            Want to work with us?
                        </h2>
                        <p className="text-lg text-gray-400 mb-10">
                            We're always looking for exciting projects and great people to work with.
                            Let's start a conversation.
                        </p>
                        <Link to="/Contact">
                            <Button
                                size="lg"
                                className="bg-gradient-to-r from-blue-600 to-blue-500 hover:from-blue-500 hover:to-blue-400 text-white px-8 py-6 text-lg rounded-xl shadow-lg shadow-blue-500/25 transition-all duration-300 hover:shadow-blue-500/40 hover:-translate-y-0.5"
                            >
                                Get in Touch
                                <ArrowRight className="w-5 h-5 ml-2" />
                            </Button>
                        </Link>
                    </motion.div>
                </div>
            </section>

            <Footer />
        </div>
    );
}
