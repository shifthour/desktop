import React from 'react';
import { motion } from 'framer-motion';
import { ExternalLink, Sparkles } from 'lucide-react';
import { Badge } from "@/components/ui/badge";

const projects = [
    {
        title: "AI Analytics Dashboard",
        description: "Real-time business intelligence powered by machine learning algorithms.",
        image: "https://images.unsplash.com/photo-1551288049-bebda4e38f71?w=800&q=80",
        tags: ["AI/ML", "Analytics", "Dashboard"],
        gradient: "from-blue-600 to-indigo-600"
    },
    {
        title: "Smart Inventory System",
        description: "Predictive inventory management reducing waste by 40%.",
        image: "https://images.unsplash.com/photo-1586528116311-ad8dd3c8310d?w=800&q=80",
        tags: ["Automation", "Prediction", "SaaS"],
        gradient: "from-green-600 to-emerald-600"
    },
    {
        title: "Customer Service Bot",
        description: "AI-powered support handling 80% of queries automatically.",
        image: "https://images.unsplash.com/photo-1531746790731-6c087fecd65a?w=800&q=80",
        tags: ["Chatbot", "NLP", "AI"],
        gradient: "from-purple-600 to-pink-600"
    }
];

export default function Portfolio() {
    return (
        <section className="py-24 md:py-32 bg-slate-50 dark:bg-slate-900 relative overflow-hidden" id="work">
            {/* Background */}
            <div className="absolute top-0 right-0 w-96 h-96 bg-blue-100 dark:bg-blue-500/10 rounded-full blur-3xl opacity-50 -z-10" />

            <div className="max-w-7xl mx-auto px-6">
                <motion.div
                    initial={{ opacity: 0, y: 30 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.6 }}
                    className="text-center mb-16"
                >
                    <span className="inline-flex items-center gap-2 px-4 py-2 bg-white dark:bg-white/5 text-slate-700 dark:text-gray-300 rounded-full text-sm font-medium mb-4 shadow-sm dark:shadow-none dark:border dark:border-white/10">
                        <Sparkles className="w-4 h-4 text-blue-500" />
                        Our Work
                    </span>
                    <h2 className="text-4xl md:text-5xl font-bold text-slate-900 dark:text-white mb-6">
                        Projects That
                        <span className="block text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-green-500">
                            Make an Impact
                        </span>
                    </h2>
                    <p className="text-lg text-slate-600 dark:text-gray-400 max-w-2xl mx-auto">
                        From startups to enterprises, we've helped businesses transform
                        their operations with intelligent software solutions.
                    </p>
                </motion.div>

                <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
                    {projects.map((project, idx) => (
                        <motion.div
                            key={idx}
                            initial={{ opacity: 0, x: idx % 2 === 0 ? -30 : 30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            viewport={{ once: true }}
                            transition={{ duration: 0.5, delay: idx * 0.15 }}
                            className="group"
                        >
                            <div className="bg-white dark:bg-white/5 rounded-2xl overflow-hidden shadow-sm dark:shadow-none dark:border dark:border-white/10 hover:shadow-xl dark:hover:border-white/20 transition-all duration-500 hover:-translate-y-2">
                                {/* Image */}
                                <div className="relative h-56 overflow-hidden">
                                    <img
                                        src={project.image}
                                        alt={project.title}
                                        className="w-full h-full object-cover transform group-hover:scale-110 transition-transform duration-700"
                                    />
                                    <div className={`absolute inset-0 bg-gradient-to-t ${project.gradient} opacity-0 group-hover:opacity-60 transition-opacity duration-500`} />

                                    {/* View button */}
                                    <div className="absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                                        <div className="w-12 h-12 bg-white rounded-full flex items-center justify-center shadow-lg transform scale-0 group-hover:scale-100 transition-transform duration-300 cursor-pointer hover:bg-slate-50">
                                            <ExternalLink className="w-5 h-5 text-slate-700" />
                                        </div>
                                    </div>
                                </div>

                                {/* Content */}
                                <div className="p-6">
                                    <h3 className="text-xl font-semibold text-slate-900 dark:text-white mb-2 group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
                                        {project.title}
                                    </h3>
                                    <p className="text-slate-600 dark:text-gray-400 mb-4 text-sm leading-relaxed">
                                        {project.description}
                                    </p>
                                    <div className="flex flex-wrap gap-2">
                                        {project.tags.map((tag, tagIdx) => (
                                            <Badge
                                                key={tagIdx}
                                                variant="secondary"
                                                className="bg-slate-100 dark:bg-white/5 text-slate-600 dark:text-gray-300 hover:bg-slate-200 dark:hover:bg-white/10 dark:border dark:border-white/10 transition-colors"
                                            >
                                                {tag}
                                            </Badge>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        </motion.div>
                    ))}
                </div>
            </div>
        </section>
    );
}