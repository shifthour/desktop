import React from 'react';
import { Github, Linkedin, Twitter } from 'lucide-react';

const socialLinks = [
    { icon: Twitter, href: "#", label: "Twitter" },
    { icon: Linkedin, href: "#", label: "LinkedIn" },
    { icon: Github, href: "#", label: "GitHub" }
];

const footerLinks = [
    {
        title: "Company",
        links: ["About", "Services", "Work", "Contact"]
    },
    {
        title: "Services",
        links: ["AI Solutions", "Software Dev", "Consulting", "Automation"]
    },
    {
        title: "Legal",
        links: ["Privacy", "Terms", "Cookies"]
    }
];

export default function Footer() {
    return (
        <footer className="bg-slate-950 pt-20 pb-8">
            <div className="max-w-7xl mx-auto px-6">
                <div className="grid md:grid-cols-2 lg:grid-cols-5 gap-12 pb-12 border-b border-white/10">
                    {/* Brand */}
                    <div className="lg:col-span-2">
                        <div className="flex items-center gap-3 mb-6">
                            <img 
                                src="https://qtrypzzcjebvfcihiynt.supabase.co/storage/v1/object/public/base44-prod/public/user_6862c0ee98b2f361bc4f4e97/58da6e379_logo.png"
                                alt="Stoptions"
                                className="w-12 h-12 object-contain"
                            />
                            <span className="text-2xl font-bold text-white">Stoptions</span>
                        </div>
                        <p className="text-gray-400 mb-6 max-w-sm leading-relaxed">
                            Building intelligent software solutions that maximize impact 
                            with minimal resources. AI-powered, human-driven.
                        </p>
                        <div className="flex items-center gap-4">
                            {socialLinks.map((social, idx) => (
                                <a
                                    key={idx}
                                    href={social.href}
                                    aria-label={social.label}
                                    className="w-10 h-10 rounded-lg bg-white/5 border border-white/10 flex items-center justify-center text-gray-400 hover:text-white hover:bg-white/10 hover:border-white/20 transition-all duration-300"
                                >
                                    <social.icon className="w-5 h-5" />
                                </a>
                            ))}
                        </div>
                    </div>

                    {/* Links */}
                    {footerLinks.map((group, idx) => (
                        <div key={idx}>
                            <h4 className="text-white font-semibold mb-4">{group.title}</h4>
                            <ul className="space-y-3">
                                {group.links.map((link, linkIdx) => (
                                    <li key={linkIdx}>
                                        <a 
                                            href={`#${link.toLowerCase()}`}
                                            className="text-gray-400 hover:text-white transition-colors text-sm"
                                        >
                                            {link}
                                        </a>
                                    </li>
                                ))}
                            </ul>
                        </div>
                    ))}
                </div>

                {/* Bottom */}
                <div className="pt-8 flex flex-col md:flex-row justify-between items-center gap-4">
                    <p className="text-gray-500 text-sm">
                        © {new Date().getFullYear()} Stoptions. All rights reserved.
                    </p>
                    <p className="text-gray-600 text-sm">
                        Built with AI, crafted with care
                    </p>
                </div>
            </div>
        </footer>
    );
}