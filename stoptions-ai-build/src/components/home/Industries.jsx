import React from 'react';
import { motion } from 'framer-motion';
import { Building2, Home, ShoppingCart, Truck, Plane, Coins, Package, Users, Sparkles } from 'lucide-react';
import { Link } from 'react-router-dom';

const industries = [
    {
        icon: Building2,
        name: "Healthcare & Lab Equipment",
        description: "CRM and inventory systems for laboratory equipment companies managing contracts and maintenance.",
        gradient: "from-blue-500 to-indigo-600"
    },
    {
        icon: Home,
        name: "Real Estate",
        description: "Lead management, property listings, deal tracking, and commission platforms for agencies.",
        gradient: "from-green-500 to-emerald-600"
    },
    {
        icon: ShoppingCart,
        name: "Retail & E-Commerce",
        description: "Multi-location inventory management, order systems, and sales forecasting for retail chains.",
        gradient: "from-orange-500 to-red-600"
    },
    {
        icon: Truck,
        name: "Logistics & Supply Chain",
        description: "Fleet management, route optimization, driver coordination, and delivery tracking platforms.",
        gradient: "from-cyan-500 to-blue-600"
    },
    {
        icon: Plane,
        name: "Travel & Transportation",
        description: "Passenger communication systems, trip management, and real-time scheduling updates.",
        gradient: "from-purple-500 to-pink-600"
    },
    {
        icon: Coins,
        name: "Fintech & SaaS",
        description: "Multi-tenant platforms, usage metering, billing systems, and scalable backend infrastructure.",
        gradient: "from-amber-500 to-orange-600"
    },
    {
        icon: Package,
        name: "Warehousing & Storage",
        description: "Analytics platforms, ETL pipelines, data warehousing, and self-service reporting dashboards.",
        gradient: "from-rose-500 to-red-600"
    },
    {
        icon: Users,
        name: "Staffing & HR",
        description: "Job marketplace apps, shift management, time tracking, and payment processing platforms.",
        gradient: "from-teal-500 to-green-600"
    }
];

export default function Industries() {
    return (
        <section className="py-24 md:py-32 bg-slate-50 dark:bg-slate-900 relative overflow-hidden">
            <div className="absolute top-0 right-0 w-96 h-96 bg-blue-100 dark:bg-blue-500/10 rounded-full blur-3xl opacity-50 -z-10" />
            <div className="absolute bottom-0 left-0 w-80 h-80 bg-green-100 dark:bg-green-500/10 rounded-full blur-3xl opacity-50 -z-10" />

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
                        Industries
                    </span>
                    <h2 className="text-4xl md:text-5xl font-bold text-slate-900 dark:text-white mb-6">
                        Industries We
                        <span className="block text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-green-500">
                            Support & Serve
                        </span>
                    </h2>
                    <p className="text-lg text-slate-600 dark:text-gray-400 max-w-2xl mx-auto">
                        We've built and shipped products across a wide range of industries,
                        solving real problems with tailored technology.
                    </p>
                </motion.div>

                <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
                    {industries.map((industry, idx) => (
                        <motion.div
                            key={idx}
                            initial={{ opacity: 0, scale: 0.9 }}
                            whileInView={{ opacity: 1, scale: 1 }}
                            viewport={{ once: true }}
                            transition={{ duration: 0.4, delay: idx * 0.07 }}
                        >
                            <div className="group bg-white dark:bg-white/5 border border-slate-100 dark:border-white/10 rounded-2xl p-6 h-full hover:border-slate-200 dark:hover:border-white/20 hover:shadow-lg dark:hover:shadow-none transition-all duration-300 hover:-translate-y-1">
                                <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${industry.gradient} flex items-center justify-center mb-4 shadow-lg group-hover:scale-110 transition-transform duration-300`}>
                                    <industry.icon className="w-6 h-6 text-white" />
                                </div>
                                <h3 className="text-lg font-semibold text-slate-900 dark:text-white mb-2 group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
                                    {industry.name}
                                </h3>
                                <p className="text-sm text-slate-600 dark:text-gray-400 leading-relaxed">
                                    {industry.description}
                                </p>
                            </div>
                        </motion.div>
                    ))}
                </div>

                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    viewport={{ once: true }}
                    transition={{ delay: 0.5 }}
                    className="text-center mt-12"
                >
                    <Link
                        to="/Products"
                        className="inline-flex items-center gap-2 text-blue-600 dark:text-blue-400 hover:text-blue-700 dark:hover:text-blue-300 font-medium transition-colors"
                    >
                        See our projects
                        <span className="group-hover:translate-x-1 transition-transform">&rarr;</span>
                    </Link>
                </motion.div>
            </div>
        </section>
    );
}
