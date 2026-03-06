import React from 'react';
import { motion } from 'framer-motion';
import { Code2 } from 'lucide-react';

const technologies = [
    {
        category: "AI & Machine Learning",
        items: ["TensorFlow", "PyTorch", "OpenAI", "Hugging Face", "LangChain", "Claude/Anthropic"],
        gradient: "from-purple-500 to-indigo-500"
    },
    {
        category: "Frontend",
        items: ["React", "Next.js", "TypeScript", "Tailwind CSS"],
        gradient: "from-blue-500 to-cyan-500"
    },
    {
        category: "Backend",
        items: ["Node.js", "Python", "FastAPI", "PostgreSQL", "MongoDB", "Supabase"],
        gradient: "from-green-500 to-emerald-500"
    },
    {
        category: "Cloud & DevOps",
        items: ["AWS", "Google Cloud", "Docker", "Kubernetes", "CI/CD"],
        gradient: "from-orange-500 to-red-500"
    }
];

export default function TechStack() {
    return (
        <section className="py-24 md:py-32 bg-gradient-to-br from-slate-900 via-slate-950 to-slate-900 relative overflow-hidden">
            {/* Background elements */}
            <div className="absolute inset-0">
                <div className="absolute top-20 left-1/4 w-96 h-96 bg-blue-500/5 rounded-full blur-3xl" />
                <div className="absolute bottom-20 right-1/4 w-80 h-80 bg-green-500/5 rounded-full blur-3xl" />
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
                        <Code2 className="w-4 h-4" />
                        Technology Stack
                    </span>
                    <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">
                        Powered by
                        <span className="block text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-green-400">
                            Modern Technology
                        </span>
                    </h2>
                    <p className="text-lg text-gray-400 max-w-2xl mx-auto">
                        We leverage the latest and most powerful technologies to build 
                        robust, scalable, and intelligent solutions.
                    </p>
                </motion.div>

                <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
                    {technologies.map((tech, idx) => (
                        <motion.div
                            key={idx}
                            initial={{ opacity: 0, rotate: -3, y: 20 }}
                            whileInView={{ opacity: 1, rotate: 0, y: 0 }}
                            viewport={{ once: true }}
                            transition={{ duration: 0.5, delay: idx * 0.1 }}
                            className="group"
                        >
                            <div className="bg-white/5 backdrop-blur-sm border border-white/10 rounded-2xl p-6 h-full hover:bg-white/10 hover:border-white/20 transition-all duration-300 hover:-translate-y-1">
                                <div className={`inline-flex items-center justify-center w-12 h-12 rounded-xl bg-gradient-to-br ${tech.gradient} mb-4 shadow-lg`}>
                                    <Code2 className="w-6 h-6 text-white" />
                                </div>
                                
                                <h3 className="text-lg font-semibold text-white mb-4">
                                    {tech.category}
                                </h3>
                                
                                <div className="flex flex-wrap gap-2">
                                    {tech.items.map((item, itemIdx) => (
                                        <span 
                                            key={itemIdx}
                                            className="px-3 py-1 bg-white/5 border border-white/10 rounded-full text-xs text-gray-300 hover:bg-white/10 hover:text-white transition-all"
                                        >
                                            {item}
                                        </span>
                                    ))}
                                </div>
                            </div>
                        </motion.div>
                    ))}
                </div>

                {/* Additional badges */}
                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    viewport={{ once: true }}
                    transition={{ delay: 0.6 }}
                    className="mt-16 text-center"
                >
                    <p className="text-gray-500 text-sm mb-4">And many more cutting-edge tools</p>
                    <div className="flex flex-wrap justify-center gap-3">
                        {["REST API", "GraphQL", "WebSockets", "Microservices", "Serverless"].map((tech, idx) => (
                            <span 
                                key={idx}
                                className="px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-sm text-gray-400 hover:bg-white/10 hover:text-white transition-all"
                            >
                                {tech}
                            </span>
                        ))}
                    </div>
                </motion.div>
            </div>
        </section>
    );
}