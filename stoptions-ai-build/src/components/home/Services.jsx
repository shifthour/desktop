import React from 'react';
import { motion } from 'framer-motion';
import { Brain, Code2, Cpu, Rocket, BarChart3, Shield } from 'lucide-react';
import { Card } from "@/components/ui/card";

const services = [
    {
        icon: Brain,
        title: "AI Solutions",
        description: "Custom AI models and machine learning solutions tailored to your business needs.",
        gradient: "from-purple-500 to-indigo-600"
    },
    {
        icon: Code2,
        title: "Software Development",
        description: "End-to-end software products built with modern technologies and best practices.",
        gradient: "from-blue-500 to-cyan-600"
    },
    {
        icon: Cpu,
        title: "Automation",
        description: "Streamline operations with intelligent automation that saves time and resources.",
        gradient: "from-green-500 to-emerald-600"
    },
    {
        icon: Rocket,
        title: "MVP Development",
        description: "Rapid prototyping and MVP development to validate your ideas quickly.",
        gradient: "from-orange-500 to-red-600"
    },
    {
        icon: BarChart3,
        title: "Data Analytics",
        description: "Transform your data into actionable insights with advanced analytics.",
        gradient: "from-pink-500 to-rose-600"
    },
    {
        icon: Shield,
        title: "Tech Consulting",
        description: "Strategic guidance to help you make the right technology decisions.",
        gradient: "from-slate-500 to-slate-700"
    }
];

export default function Services() {
    return (
        <section className="py-24 md:py-32 bg-white dark:bg-slate-950 relative overflow-hidden" id="services">
            {/* Background decoration */}
            <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-blue-500 via-green-500 to-blue-500" />
            <div className="absolute top-40 right-0 w-96 h-96 bg-blue-50 dark:bg-blue-500/10 rounded-full blur-3xl -z-10" />
            <div className="absolute bottom-20 left-0 w-80 h-80 bg-green-50 dark:bg-green-500/10 rounded-full blur-3xl -z-10" />

            <div className="max-w-7xl mx-auto px-6">
                <motion.div
                    initial={{ opacity: 0, y: 30 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.6 }}
                    className="text-center mb-16"
                >
                    <span className="inline-block px-4 py-2 bg-blue-50 dark:bg-blue-500/10 text-blue-600 dark:text-blue-400 rounded-full text-sm font-medium mb-4">
                        What We Do
                    </span>
                    <h2 className="text-4xl md:text-5xl font-bold text-slate-900 dark:text-white mb-6">
                        Services That Drive
                        <span className="block text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-green-500">
                            Innovation
                        </span>
                    </h2>
                    <p className="text-lg text-slate-600 dark:text-gray-400 max-w-2xl mx-auto">
                        We leverage cutting-edge AI and software expertise to deliver solutions
                        that maximize your impact while minimizing resource investment.
                    </p>
                </motion.div>

                <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
                    {services.map((service, idx) => (
                        <motion.div
                            key={idx}
                            initial={{ opacity: 0, scale: 0.9 }}
                            whileInView={{ opacity: 1, scale: 1 }}
                            viewport={{ once: true }}
                            transition={{ duration: 0.5, delay: idx * 0.1 }}
                        >
                            <Card className="group relative p-8 h-full bg-white dark:bg-white/5 border border-slate-100 dark:border-white/10 hover:border-slate-200 dark:hover:border-white/20 rounded-2xl transition-all duration-500 hover:shadow-xl hover:shadow-slate-100/50 dark:hover:shadow-none hover:-translate-y-1 overflow-hidden">
                                {/* Hover gradient */}
                                <div className={`absolute inset-0 bg-gradient-to-br ${service.gradient} opacity-0 group-hover:opacity-[0.03] dark:group-hover:opacity-[0.08] transition-opacity duration-500`} />

                                <div className={`w-14 h-14 rounded-xl bg-gradient-to-br ${service.gradient} flex items-center justify-center mb-6 shadow-lg transform group-hover:scale-110 transition-transform duration-300`}>
                                    <service.icon className="w-7 h-7 text-white" />
                                </div>

                                <h3 className="text-xl font-semibold text-slate-900 dark:text-white mb-3 group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
                                    {service.title}
                                </h3>

                                <p className="text-slate-600 dark:text-gray-400 leading-relaxed">
                                    {service.description}
                                </p>
                            </Card>
                        </motion.div>
                    ))}
                </div>
            </div>
        </section>
    );
}