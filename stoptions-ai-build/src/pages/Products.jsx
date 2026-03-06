import React from 'react';
import { motion } from 'framer-motion';
import { ArrowRight, Globe, Smartphone, Server, Rocket, Check, Sparkles, Users, MapPin, ShoppingCart, BarChart3, MessageSquare, Clock, Package, Building2, Truck, FileText } from 'lucide-react';
import { Link } from 'react-router-dom';
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import Footer from '../components/home/Footer';

const categories = [
    {
        title: "Web Applications",
        icon: Globe,
        gradient: "from-blue-600 to-indigo-600",
        projects: [
            {
                name: "Labgigs CRM",
                client: "Labgigs",
                status: "Live",
                description: "Laboratory equipment company managing customer relationships, equipment inventory, service contracts, and maintenance schedules — all unified in one platform.",
                features: [
                    "Customer & contact management",
                    "Equipment inventory tracking",
                    "Service contract management",
                    "Maintenance scheduling & history",
                    "Sales pipeline & quotations",
                    "Reporting & analytics dashboard"
                ],
                tech: ["React", "TypeScript", "Node.js", "Supabase", "PostgreSQL", "AWS"],
                gradient: "from-blue-600 to-indigo-600",
                bgGlow: "bg-blue-500/10"
            },
            {
                name: "Flatrix — Real Estate CRM",
                client: "Flatrix",
                status: "Live",
                description: "End-to-end real estate CRM replacing spreadsheets and disconnected tools — capturing leads, managing listings, tracking deals, and scheduling site visits.",
                features: [
                    "Property listing management",
                    "Lead capture & tracking",
                    "Sales pipeline & deal tracking",
                    "Site visit scheduling",
                    "Commission tracking",
                    "Reports & analytics dashboard"
                ],
                tech: ["Next.js", "TypeScript", "Node.js", "Supabase", "PostgreSQL", "AWS"],
                gradient: "from-green-600 to-emerald-600",
                bgGlow: "bg-green-500/10"
            },
            {
                name: "Inventory & Order Management",
                client: "Ashoka Enterprises",
                status: "Live — 25 locations",
                description: "Multi-location inventory system for a retail chain, eliminating stockouts and overstock situations across 25+ stores with predictive forecasting.",
                features: [
                    "Multi-location inventory tracking",
                    "Automated purchase order generation",
                    "Barcode scanning & management",
                    "Sales forecasting",
                    "Supplier management"
                ],
                tech: ["Vue.js", "Java", "Spring Boot", "Supabase", "MySQL", "AWS"],
                gradient: "from-orange-600 to-red-600",
                bgGlow: "bg-orange-500/10"
            },
            {
                name: "Client Self-Service Portal",
                client: "Confidential",
                status: "Live",
                description: "Customer portal that reduced support ticket volume by giving clients full visibility into accounts, billing, service requests, and a self-service knowledge base.",
                features: [
                    "Account overview & billing",
                    "Service request submission",
                    "Ticket tracking & history",
                    "Document repository",
                    "Knowledge base"
                ],
                tech: ["React", "Node.js", "Supabase", "MongoDB", "Elasticsearch", "Azure"],
                gradient: "from-purple-600 to-pink-600",
                bgGlow: "bg-purple-500/10"
            }
        ]
    },
    {
        title: "Mobile Applications",
        icon: Smartphone,
        gradient: "from-green-600 to-emerald-600",
        projects: [
            {
                name: "Shift Hour",
                client: "Shift Hour",
                status: "Live",
                description: "Two-sided marketplace connecting job seekers with flexible hourly shift work and employers looking for reliable short-term workers. Separate apps for both sides.",
                features: [
                    "Browse & apply for shifts by location",
                    "Post shifts with hourly rates",
                    "Schedule & calendar management",
                    "Earnings & payment tracking",
                    "Time tracking & attendance",
                    "Ratings & reviews"
                ],
                tech: ["React Native", "Node.js", "Supabase", "PostgreSQL", "Redis", "AWS"],
                gradient: "from-green-600 to-emerald-600",
                bgGlow: "bg-green-500/10"
            },
            {
                name: "Mythri — Passenger Communication",
                client: "Mythri",
                status: "Live",
                description: "Real-time communication platform enabling drivers and staff to broadcast trip updates, delays, and announcements to booked passengers instantly.",
                features: [
                    "Broadcast messages to passengers",
                    "Trip-specific announcements",
                    "Real-time delay & schedule updates",
                    "Message templates & quick replies",
                    "Delivery & read receipts"
                ],
                tech: ["Flutter", "Node.js", "Supabase", "PostgreSQL", "Firebase", "AWS"],
                gradient: "from-cyan-600 to-blue-600",
                bgGlow: "bg-cyan-500/10"
            }
        ]
    },
    {
        title: "Backend Platforms & Data Systems",
        icon: Server,
        gradient: "from-purple-600 to-pink-600",
        projects: [
            {
                name: "Coinex — Multi-tenant SaaS Platform",
                client: "Coinex",
                status: "Live — 100+ tenants",
                description: "Scalable multi-tenant backend that onboards new customers with fully isolated data while maintaining a single codebase. Built for rapid tenant provisioning.",
                features: [
                    "Tenant provisioning & management",
                    "Schema isolation per tenant",
                    "Usage metering & billing",
                    "Admin console for operations",
                    "API gateway & rate limiting"
                ],
                tech: ["Node.js", "Supabase", "PostgreSQL", "Redis", "Kubernetes", "AWS"],
                gradient: "from-purple-600 to-pink-600",
                bgGlow: "bg-purple-500/10"
            },
            {
                name: "Store My Goods — Analytics Platform",
                client: "Store My Goods",
                status: "Live",
                description: "Unified analytics platform replacing days of manual report preparation with automated ETL pipelines, a data warehouse, and self-service dashboards.",
                features: [
                    "ETL pipelines from 10+ sources",
                    "Data warehouse with star schema",
                    "Self-service dashboards",
                    "Scheduled report generation",
                    "Data quality monitoring"
                ],
                tech: ["Python", "Apache Airflow", "Snowflake", "dbt", "Metabase", "AWS"],
                gradient: "from-orange-600 to-amber-600",
                bgGlow: "bg-orange-500/10"
            }
        ]
    },
    {
        title: "In Development",
        icon: Rocket,
        gradient: "from-orange-600 to-red-600",
        projects: [
            {
                name: "Safe Storage — Logistics Platform",
                client: "Safe Storage",
                status: "In Development",
                description: "End-to-end logistics management platform replacing disconnected tools with fleet tracking, route optimization, driver coordination, and customer delivery tracking.",
                features: [
                    "Fleet management & tracking",
                    "Route optimization",
                    "Driver mobile app",
                    "Customer delivery tracking",
                    "Dispatch console"
                ],
                tech: ["React", "React Native", "Go", "Supabase", "PostgreSQL", "GCP"],
                gradient: "from-slate-600 to-slate-700",
                bgGlow: "bg-slate-500/10"
            },
            {
                name: "Safe Storage — Vendor Management",
                client: "Safe Storage",
                status: "MVP Phase",
                description: "Procurement tool replacing spreadsheets and email chains with a centralized vendor registry, contract management, and performance tracking system.",
                features: [
                    "Vendor registry & profiles",
                    "Contract management",
                    "Performance scorecards",
                    "Document storage"
                ],
                tech: ["Next.js", "Node.js", "Supabase", "PostgreSQL", "AWS"],
                gradient: "from-slate-600 to-slate-700",
                bgGlow: "bg-slate-500/10"
            }
        ]
    }
];

export default function Products() {
    return (
        <div>
            {/* Hero */}
            <section className="relative pt-32 pb-20 bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 overflow-hidden">
                <div className="absolute inset-0">
                    <div className="absolute top-1/3 left-1/4 w-96 h-96 bg-blue-500/10 rounded-full blur-3xl animate-pulse" />
                    <div className="absolute bottom-1/4 right-1/3 w-80 h-80 bg-green-500/10 rounded-full blur-3xl animate-pulse delay-1000" />
                </div>

                <div className="max-w-7xl mx-auto px-6 relative z-10 text-center">
                    <motion.div
                        initial={{ opacity: 0, y: 30 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.8 }}
                    >
                        <span className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-white/5 border border-white/10 backdrop-blur-sm mb-8">
                            <Sparkles className="w-4 h-4 text-green-400" />
                            <span className="text-sm text-gray-300">Our Work</span>
                        </span>

                        <h1 className="text-5xl md:text-6xl lg:text-7xl font-bold text-white leading-tight mb-6">
                            Products &
                            <span className="block bg-gradient-to-r from-blue-400 via-green-400 to-blue-500 bg-clip-text text-transparent">
                                Platforms We've Built
                            </span>
                        </h1>

                        <p className="text-lg md:text-xl text-gray-400 mb-10 max-w-2xl mx-auto leading-relaxed">
                            From CRMs to multi-tenant SaaS platforms, here's a look at the real products
                            we've designed, built, and shipped for our clients.
                        </p>
                    </motion.div>
                </div>
            </section>

            {/* Categories */}
            {categories.map((category, catIdx) => (
                <section key={catIdx} className="py-20 bg-slate-950 relative">
                    {catIdx > 0 && <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/10 to-transparent" />}

                    <div className="max-w-7xl mx-auto px-6">
                        {/* Category Header */}
                        <motion.div
                            initial={{ opacity: 0, y: 20 }}
                            whileInView={{ opacity: 1, y: 0 }}
                            viewport={{ once: true }}
                            transition={{ duration: 0.5 }}
                            className="flex items-center gap-4 mb-12"
                        >
                            <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${category.gradient} flex items-center justify-center shadow-lg`}>
                                <category.icon className="w-6 h-6 text-white" />
                            </div>
                            <h2 className="text-3xl md:text-4xl font-bold text-white">{category.title}</h2>
                        </motion.div>

                        {/* Projects */}
                        <div className="space-y-16">
                            {category.projects.map((project, idx) => (
                                <motion.div
                                    key={idx}
                                    initial={{ opacity: 0, y: 40 }}
                                    whileInView={{ opacity: 1, y: 0 }}
                                    viewport={{ once: true }}
                                    transition={{ duration: 0.6 }}
                                    className={`grid lg:grid-cols-2 gap-12 items-start`}
                                >
                                    {/* Info Side */}
                                    <div className={idx % 2 === 1 ? 'lg:order-2' : ''}>
                                        <div className="flex items-center gap-3 mb-4">
                                            <h3 className="text-2xl md:text-3xl font-bold text-white">
                                                {project.name}
                                            </h3>
                                        </div>

                                        <div className="flex items-center gap-3 mb-5">
                                            <Badge className="bg-white/5 border border-white/10 text-gray-300">
                                                {project.client}
                                            </Badge>
                                            <Badge className={`${project.status.includes('Live') ? 'bg-green-500/10 border-green-500/20 text-green-400' : 'bg-orange-500/10 border-orange-500/20 text-orange-400'}`}>
                                                {project.status}
                                            </Badge>
                                        </div>

                                        <p className="text-gray-400 leading-relaxed mb-6">
                                            {project.description}
                                        </p>

                                        <div className="flex flex-wrap gap-2">
                                            {project.tech.map((t, tIdx) => (
                                                <span
                                                    key={tIdx}
                                                    className="px-3 py-1 bg-white/5 border border-white/10 rounded-full text-xs text-gray-400 hover:bg-white/10 hover:text-white transition-all"
                                                >
                                                    {t}
                                                </span>
                                            ))}
                                        </div>
                                    </div>

                                    {/* Features Card */}
                                    <div className={idx % 2 === 1 ? 'lg:order-1' : ''}>
                                        <div className="relative bg-white/5 backdrop-blur-sm border border-white/10 rounded-2xl p-8 hover:bg-white/[0.08] transition-all duration-300">
                                            <div className={`absolute -top-4 -right-4 w-32 h-32 ${project.bgGlow} rounded-full blur-2xl`} />
                                            <h4 className="text-lg font-semibold text-white mb-6">Core Features</h4>
                                            <ul className="space-y-4">
                                                {project.features.map((feature, fIdx) => (
                                                    <li key={fIdx} className="flex items-start gap-3">
                                                        <div className={`w-6 h-6 rounded-full bg-gradient-to-br ${project.gradient} flex items-center justify-center shrink-0 mt-0.5`}>
                                                            <Check className="w-3.5 h-3.5 text-white" />
                                                        </div>
                                                        <span className="text-gray-300">{feature}</span>
                                                    </li>
                                                ))}
                                            </ul>
                                        </div>
                                    </div>
                                </motion.div>
                            ))}
                        </div>
                    </div>
                </section>
            ))}

            {/* CTA */}
            <section className="py-24 bg-gradient-to-br from-slate-900 to-slate-950 relative overflow-hidden">
                <div className="absolute inset-0">
                    <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-blue-500/5 rounded-full blur-3xl" />
                </div>
                <div className="max-w-3xl mx-auto px-6 text-center relative z-10">
                    <motion.div
                        initial={{ opacity: 0, y: 30 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                    >
                        <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">
                            Have a similar project in mind?
                        </h2>
                        <p className="text-lg text-gray-400 mb-10">
                            Whether it's a CRM, a mobile app, or a data platform —
                            we'd love to hear what you're building.
                        </p>
                        <Link to="/Contact">
                            <Button
                                size="lg"
                                className="bg-gradient-to-r from-blue-600 to-blue-500 hover:from-blue-500 hover:to-blue-400 text-white px-8 py-6 text-lg rounded-xl shadow-lg shadow-blue-500/25 transition-all duration-300 hover:shadow-blue-500/40 hover:-translate-y-0.5"
                            >
                                Start a Conversation
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
