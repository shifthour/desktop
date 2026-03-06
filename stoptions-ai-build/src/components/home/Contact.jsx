import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Send, Mail, MapPin, Phone, ArrowRight, CheckCircle } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Card } from "@/components/ui/card";

const contactInfo = [
    {
        icon: Mail,
        title: "Email Us",
        value: "hello@stoptions.com",
        gradient: "from-blue-500 to-blue-600"
    },
    {
        icon: Phone,
        title: "Call Us",
        value: "+1 (555) 123-4567",
        gradient: "from-green-500 to-green-600"
    },
    {
        icon: MapPin,
        title: "Location",
        value: "Remote-First Company",
        gradient: "from-purple-500 to-purple-600"
    }
];

export default function Contact() {
    const [formData, setFormData] = useState({
        name: '',
        email: '',
        message: ''
    });
    const [submitted, setSubmitted] = useState(false);

    const handleSubmit = (e) => {
        e.preventDefault();
        setSubmitted(true);
        setTimeout(() => {
            setSubmitted(false);
            setFormData({ name: '', email: '', message: '' });
        }, 3000);
    };

    return (
        <section className="py-24 md:py-32 bg-white dark:bg-slate-950 relative overflow-hidden" id="contact">
            {/* Background */}
            <div className="absolute bottom-0 left-0 w-full h-1/2 bg-gradient-to-t from-slate-50 dark:from-slate-900 to-transparent" />
            <div className="absolute top-40 left-0 w-80 h-80 bg-blue-50 dark:bg-blue-500/10 rounded-full blur-3xl -z-10" />
            <div className="absolute bottom-20 right-0 w-96 h-96 bg-green-50 dark:bg-green-500/10 rounded-full blur-3xl -z-10" />

            <div className="max-w-7xl mx-auto px-6 relative z-10">
                <motion.div
                    initial={{ opacity: 0, y: 50 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.7 }}
                    className="text-center mb-16"
                >
                    <span className="inline-block px-4 py-2 bg-green-50 dark:bg-green-500/10 text-green-600 dark:text-green-400 rounded-full text-sm font-medium mb-4">
                        Get in Touch
                    </span>
                    <h2 className="text-4xl md:text-5xl font-bold text-slate-900 dark:text-white mb-6">
                        Let's Build
                        <span className="block text-transparent bg-clip-text bg-gradient-to-r from-green-500 to-blue-500">
                            Something Great
                        </span>
                    </h2>
                    <p className="text-lg text-slate-600 dark:text-gray-400 max-w-2xl mx-auto">
                        Have a project in mind? We'd love to hear about it.
                        Drop us a message and let's explore how we can help.
                    </p>
                </motion.div>

                <div className="grid lg:grid-cols-5 gap-12">
                    {/* Contact Info */}
                    <motion.div
                        initial={{ opacity: 0, y: 40 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                        className="lg:col-span-2 space-y-6"
                    >
                        {contactInfo.map((info, idx) => (
                            <Card
                                key={idx}
                                className="p-6 border-slate-100 dark:border-white/10 dark:bg-white/5 hover:border-slate-200 dark:hover:border-white/20 hover:shadow-lg dark:hover:shadow-none transition-all duration-300 group"
                            >
                                <div className="flex items-center gap-4">
                                    <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${info.gradient} flex items-center justify-center shadow-lg group-hover:scale-110 transition-transform`}>
                                        <info.icon className="w-6 h-6 text-white" />
                                    </div>
                                    <div>
                                        <div className="text-sm text-slate-500 dark:text-gray-500 mb-1">{info.title}</div>
                                        <div className="font-semibold text-slate-900 dark:text-white">{info.value}</div>
                                    </div>
                                </div>
                            </Card>
                        ))}

                        {/* Social proof */}
                        <div className="pt-8 border-t border-slate-100 dark:border-white/10">
                            <p className="text-sm text-slate-500 dark:text-gray-500 mb-4">Trusted by innovative companies</p>
                            <div className="flex items-center gap-6 opacity-50">
                                <div className="h-8 w-20 bg-slate-200 dark:bg-white/10 rounded" />
                                <div className="h-8 w-24 bg-slate-200 dark:bg-white/10 rounded" />
                                <div className="h-8 w-16 bg-slate-200 dark:bg-white/10 rounded" />
                            </div>
                        </div>
                    </motion.div>

                    {/* Contact Form */}
                    <motion.div
                        initial={{ opacity: 0, y: 40 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6, delay: 0.2 }}
                        className="lg:col-span-3"
                    >
                        <Card className="p-8 md:p-10 border-slate-100 dark:border-white/10 dark:bg-white/5 shadow-xl shadow-slate-100/50 dark:shadow-none">
                            {submitted ? (
                                <motion.div
                                    initial={{ opacity: 0, scale: 0.9 }}
                                    animate={{ opacity: 1, scale: 1 }}
                                    className="text-center py-12"
                                >
                                    <div className="w-16 h-16 bg-green-100 dark:bg-green-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
                                        <CheckCircle className="w-8 h-8 text-green-600 dark:text-green-400" />
                                    </div>
                                    <h3 className="text-2xl font-bold text-slate-900 dark:text-white mb-2">Thank You!</h3>
                                    <p className="text-slate-600 dark:text-gray-400">We'll get back to you within 24 hours.</p>
                                </motion.div>
                            ) : (
                                <form onSubmit={handleSubmit} className="space-y-6">
                                    <div className="grid sm:grid-cols-2 gap-6">
                                        <div>
                                            <label className="block text-sm font-medium text-slate-700 dark:text-gray-300 mb-2">
                                                Your Name
                                            </label>
                                            <Input
                                                placeholder="John Doe"
                                                value={formData.name}
                                                onChange={(e) => setFormData({...formData, name: e.target.value})}
                                                className="h-12 border-slate-200 dark:border-white/10 dark:bg-white/5 dark:text-white dark:placeholder:text-gray-500 focus:border-blue-500 focus:ring-blue-500/20 rounded-xl"
                                                required
                                            />
                                        </div>
                                        <div>
                                            <label className="block text-sm font-medium text-slate-700 dark:text-gray-300 mb-2">
                                                Email Address
                                            </label>
                                            <Input
                                                type="email"
                                                placeholder="john@example.com"
                                                value={formData.email}
                                                onChange={(e) => setFormData({...formData, email: e.target.value})}
                                                className="h-12 border-slate-200 dark:border-white/10 dark:bg-white/5 dark:text-white dark:placeholder:text-gray-500 focus:border-blue-500 focus:ring-blue-500/20 rounded-xl"
                                                required
                                            />
                                        </div>
                                    </div>
                                    <div>
                                        <label className="block text-sm font-medium text-slate-700 dark:text-gray-300 mb-2">
                                            Tell us about your project
                                        </label>
                                        <Textarea
                                            placeholder="I'm looking for help with..."
                                            value={formData.message}
                                            onChange={(e) => setFormData({...formData, message: e.target.value})}
                                            className="min-h-[150px] border-slate-200 dark:border-white/10 dark:bg-white/5 dark:text-white dark:placeholder:text-gray-500 focus:border-blue-500 focus:ring-blue-500/20 rounded-xl resize-none"
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
                </div>
            </div>
        </section>
    );
}