import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Mail, MapPin, Phone, ArrowRight, CheckCircle, MessageSquare, Clock, FileText } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Card } from "@/components/ui/card";
import Footer from '../components/home/Footer';

const contactInfo = [
    {
        icon: Mail,
        title: "Email Us",
        value: "hello@stoptions.com",
        description: "We typically respond within a few hours",
        gradient: "from-blue-500 to-blue-600"
    },
    {
        icon: Phone,
        title: "Call Us",
        value: "+91 9742596739",
        description: "Mon-Fri, 9am-6pm IST",
        gradient: "from-green-500 to-green-600"
    },
    {
        icon: MapPin,
        title: "Location",
        value: "Remote-First Company",
        description: "Working with clients worldwide",
        gradient: "from-purple-500 to-purple-600"
    }
];

const processSteps = [
    {
        icon: MessageSquare,
        step: "01",
        title: "Discovery Call",
        description: "We learn about your project, goals, and requirements in a 30-minute call."
    },
    {
        icon: FileText,
        step: "02",
        title: "Proposal & Plan",
        description: "We deliver a detailed proposal with scope, timeline, and transparent pricing."
    },
    {
        icon: Clock,
        step: "03",
        title: "Kick Off",
        description: "Once aligned, we hit the ground running with sprint-based delivery."
    }
];

const projectTypes = [
    "AI / Machine Learning",
    "Web Application",
    "Mobile App",
    "Automation",
    "Data Analytics",
    "MVP / Prototype",
    "Consulting",
    "Other"
];

export default function Contact() {
    const [formData, setFormData] = useState({
        name: '',
        email: '',
        projectType: '',
        budget: '',
        message: ''
    });
    const [submitted, setSubmitted] = useState(false);

    const handleSubmit = (e) => {
        e.preventDefault();
        setSubmitted(true);
        setTimeout(() => {
            setSubmitted(false);
            setFormData({ name: '', email: '', projectType: '', budget: '', message: '' });
        }, 4000);
    };

    return (
        <div>
            {/* Hero */}
            <section className="relative pt-32 pb-16 bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 overflow-hidden">
                <div className="absolute inset-0">
                    <div className="absolute top-1/3 left-1/4 w-96 h-96 bg-green-500/10 rounded-full blur-3xl animate-pulse" />
                    <div className="absolute bottom-1/4 right-1/3 w-80 h-80 bg-blue-500/10 rounded-full blur-3xl animate-pulse delay-1000" />
                </div>

                <div className="max-w-7xl mx-auto px-6 relative z-10 text-center">
                    <motion.div
                        initial={{ opacity: 0, y: 30 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.8 }}
                    >
                        <span className="inline-block px-4 py-2 bg-green-500/10 border border-green-500/20 text-green-400 rounded-full text-sm font-medium mb-6">
                            Get in Touch
                        </span>
                        <h1 className="text-5xl md:text-6xl font-bold text-white leading-tight mb-6">
                            Let's Build
                            <span className="block bg-gradient-to-r from-green-400 to-blue-400 bg-clip-text text-transparent">
                                Something Great
                            </span>
                        </h1>
                        <p className="text-lg md:text-xl text-gray-400 max-w-2xl mx-auto leading-relaxed">
                            Have a project in mind? Tell us about it and we'll get back to you
                            within 24 hours with a plan to make it happen.
                        </p>
                    </motion.div>
                </div>
            </section>

            {/* Contact Info Cards */}
            <section className="py-16 bg-slate-950">
                <div className="max-w-7xl mx-auto px-6">
                    <div className="grid md:grid-cols-3 gap-6">
                        {contactInfo.map((info, idx) => (
                            <motion.div
                                key={idx}
                                initial={{ opacity: 0, y: 20 }}
                                animate={{ opacity: 1, y: 0 }}
                                transition={{ delay: 0.2 + idx * 0.1 }}
                            >
                                <Card className="p-6 bg-white/5 border-white/10 hover:border-white/20 hover:bg-white/[0.08] transition-all duration-300 group">
                                    <div className="flex items-start gap-4">
                                        <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${info.gradient} flex items-center justify-center shadow-lg group-hover:scale-110 transition-transform shrink-0`}>
                                            <info.icon className="w-6 h-6 text-white" />
                                        </div>
                                        <div>
                                            <div className="text-sm text-gray-500 mb-1">{info.title}</div>
                                            <div className="font-semibold text-white mb-1">{info.value}</div>
                                            <div className="text-sm text-gray-500">{info.description}</div>
                                        </div>
                                    </div>
                                </Card>
                            </motion.div>
                        ))}
                    </div>
                </div>
            </section>

            {/* Form + Process */}
            <section className="py-16 bg-slate-950 relative overflow-hidden">
                <div className="absolute top-0 left-0 w-80 h-80 bg-blue-500/5 rounded-full blur-3xl" />
                <div className="absolute bottom-0 right-0 w-96 h-96 bg-green-500/5 rounded-full blur-3xl" />

                <div className="max-w-7xl mx-auto px-6 relative z-10">
                    <div className="grid lg:grid-cols-5 gap-16">
                        {/* Contact Form */}
                        <motion.div
                            initial={{ opacity: 0, x: -30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            viewport={{ once: true }}
                            transition={{ duration: 0.6 }}
                            className="lg:col-span-3"
                        >
                            <h2 className="text-2xl font-bold text-white mb-2">Tell us about your project</h2>
                            <p className="text-gray-400 mb-8">Fill out the form below and we'll be in touch shortly.</p>

                            <Card className="p-8 md:p-10 bg-white/5 border-white/10">
                                {submitted ? (
                                    <motion.div
                                        initial={{ opacity: 0, scale: 0.9 }}
                                        animate={{ opacity: 1, scale: 1 }}
                                        className="text-center py-16"
                                    >
                                        <div className="w-16 h-16 bg-green-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
                                            <CheckCircle className="w-8 h-8 text-green-400" />
                                        </div>
                                        <h3 className="text-2xl font-bold text-white mb-2">Thank You!</h3>
                                        <p className="text-gray-400">We'll get back to you within 24 hours.</p>
                                    </motion.div>
                                ) : (
                                    <form onSubmit={handleSubmit} className="space-y-6">
                                        <div className="grid sm:grid-cols-2 gap-6">
                                            <div>
                                                <label className="block text-sm font-medium text-gray-300 mb-2">
                                                    Your Name
                                                </label>
                                                <Input
                                                    placeholder="Rahul Sharma"
                                                    value={formData.name}
                                                    onChange={(e) => setFormData({...formData, name: e.target.value})}
                                                    className="h-12 bg-white/5 border-white/10 text-white placeholder:text-gray-500 focus:border-blue-500 focus:ring-blue-500/20 rounded-xl"
                                                    required
                                                />
                                            </div>
                                            <div>
                                                <label className="block text-sm font-medium text-gray-300 mb-2">
                                                    Email Address
                                                </label>
                                                <Input
                                                    type="email"
                                                    placeholder="rahul@example.com"
                                                    value={formData.email}
                                                    onChange={(e) => setFormData({...formData, email: e.target.value})}
                                                    className="h-12 bg-white/5 border-white/10 text-white placeholder:text-gray-500 focus:border-blue-500 focus:ring-blue-500/20 rounded-xl"
                                                    required
                                                />
                                            </div>
                                        </div>

                                        <div className="grid sm:grid-cols-2 gap-6">
                                            <div>
                                                <label className="block text-sm font-medium text-gray-300 mb-2">
                                                    Project Type
                                                </label>
                                                <select
                                                    value={formData.projectType}
                                                    onChange={(e) => setFormData({...formData, projectType: e.target.value})}
                                                    className="w-full h-12 px-3 bg-white/5 border border-white/10 text-white rounded-xl focus:border-blue-500 focus:ring-1 focus:ring-blue-500/20 outline-none appearance-none cursor-pointer"
                                                    required
                                                >
                                                    <option value="" disabled className="bg-slate-900 text-gray-400">Select a type...</option>
                                                    {projectTypes.map((type, idx) => (
                                                        <option key={idx} value={type} className="bg-slate-900 text-white">{type}</option>
                                                    ))}
                                                </select>
                                            </div>
                                            <div>
                                                <label className="block text-sm font-medium text-gray-300 mb-2">
                                                    Estimated Budget
                                                </label>
                                                <select
                                                    value={formData.budget}
                                                    onChange={(e) => setFormData({...formData, budget: e.target.value})}
                                                    className="w-full h-12 px-3 bg-white/5 border border-white/10 text-white rounded-xl focus:border-blue-500 focus:ring-1 focus:ring-blue-500/20 outline-none appearance-none cursor-pointer"
                                                >
                                                    <option value="" disabled className="bg-slate-900 text-gray-400">Select a range...</option>
                                                    <option value="< ₹3L" className="bg-slate-900 text-white">Under ₹3,00,000</option>
                                                    <option value="₹3L-₹10L" className="bg-slate-900 text-white">₹3,00,000 - ₹10,00,000</option>
                                                    <option value="₹10L-₹30L" className="bg-slate-900 text-white">₹10,00,000 - ₹30,00,000</option>
                                                    <option value="₹30L+" className="bg-slate-900 text-white">₹30,00,000+</option>
                                                    <option value="Not sure" className="bg-slate-900 text-white">Not sure yet</option>
                                                </select>
                                            </div>
                                        </div>

                                        <div>
                                            <label className="block text-sm font-medium text-gray-300 mb-2">
                                                Tell us about your project
                                            </label>
                                            <Textarea
                                                placeholder="What are you looking to build? What problem does it solve? Any timeline in mind?"
                                                value={formData.message}
                                                onChange={(e) => setFormData({...formData, message: e.target.value})}
                                                className="min-h-[150px] bg-white/5 border-white/10 text-white placeholder:text-gray-500 focus:border-blue-500 focus:ring-blue-500/20 rounded-xl resize-none"
                                                required
                                            />
                                        </div>

                                        <Button
                                            type="submit"
                                            size="lg"
                                            className="w-full h-14 bg-gradient-to-r from-blue-600 to-blue-500 hover:from-blue-500 hover:to-blue-400 text-white text-lg rounded-xl shadow-lg shadow-blue-500/25 transition-all duration-300 hover:shadow-blue-500/40 group"
                                        >
                                            Send Message
                                            <ArrowRight className="w-5 h-5 ml-2 group-hover:translate-x-1 transition-transform" />
                                        </Button>
                                    </form>
                                )}
                            </Card>
                        </motion.div>

                        {/* What Happens Next */}
                        <motion.div
                            initial={{ opacity: 0, x: 30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            viewport={{ once: true }}
                            transition={{ duration: 0.6, delay: 0.2 }}
                            className="lg:col-span-2"
                        >
                            <h2 className="text-2xl font-bold text-white mb-2">What happens next?</h2>
                            <p className="text-gray-400 mb-8">Our simple 3-step process to get your project started.</p>

                            <div className="space-y-8">
                                {processSteps.map((step, idx) => (
                                    <motion.div
                                        key={idx}
                                        initial={{ opacity: 0, y: 20 }}
                                        whileInView={{ opacity: 1, y: 0 }}
                                        viewport={{ once: true }}
                                        transition={{ delay: 0.3 + idx * 0.15 }}
                                        className="flex gap-5 group"
                                    >
                                        <div className="relative">
                                            <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-blue-500/20 to-green-500/20 border border-white/10 flex items-center justify-center group-hover:scale-110 transition-transform">
                                                <step.icon className="w-5 h-5 text-blue-400" />
                                            </div>
                                            {idx < processSteps.length - 1 && (
                                                <div className="absolute top-14 left-1/2 -translate-x-1/2 w-px h-8 bg-gradient-to-b from-white/10 to-transparent" />
                                            )}
                                        </div>
                                        <div className="pt-1">
                                            <div className="text-xs text-blue-400 font-mono mb-1">Step {step.step}</div>
                                            <h3 className="text-lg font-semibold text-white mb-1">{step.title}</h3>
                                            <p className="text-gray-400 text-sm leading-relaxed">{step.description}</p>
                                        </div>
                                    </motion.div>
                                ))}
                            </div>

                            {/* Social proof */}
                            <motion.div
                                initial={{ opacity: 0, y: 20 }}
                                whileInView={{ opacity: 1, y: 0 }}
                                viewport={{ once: true }}
                                transition={{ delay: 0.6 }}
                                className="mt-12 pt-8 border-t border-white/10"
                            >
                                <div className="bg-white/5 border border-white/10 rounded-2xl p-6">
                                    <div className="flex items-center gap-3 mb-3">
                                        <div className="flex -space-x-2">
                                            <div className="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-blue-600 border-2 border-slate-950" />
                                            <div className="w-8 h-8 rounded-full bg-gradient-to-br from-green-500 to-green-600 border-2 border-slate-950" />
                                            <div className="w-8 h-8 rounded-full bg-gradient-to-br from-purple-500 to-purple-600 border-2 border-slate-950" />
                                        </div>
                                        <div className="text-sm text-gray-400">Trusted by 50+ companies</div>
                                    </div>
                                    <p className="text-sm text-gray-500 leading-relaxed">
                                        "Stoptions delivered our AI dashboard in half the time we expected.
                                        Exceptional team and communication throughout."
                                    </p>
                                    <p className="text-sm text-white font-medium mt-2">— Happy Client</p>
                                </div>
                            </motion.div>
                        </motion.div>
                    </div>
                </div>
            </section>

            <Footer />
        </div>
    );
}
