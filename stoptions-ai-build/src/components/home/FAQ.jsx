import React from 'react';
import { motion } from 'framer-motion';
import { HelpCircle } from 'lucide-react';
import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
} from "@/components/ui/accordion";

const faqs = [
    {
        question: "What types of AI solutions do you build?",
        answer: "We build custom AI solutions including machine learning models, natural language processing systems, computer vision applications, predictive analytics platforms, and AI-powered automation tools. Each solution is tailored to your specific business needs and data."
    },
    {
        question: "How long does a typical project take?",
        answer: "Project timelines vary based on complexity. An MVP or proof of concept typically takes 4-6 weeks. A full-featured product usually takes 2-4 months. We work in agile sprints so you see progress every two weeks and can adjust priorities along the way."
    },
    {
        question: "What does your pricing look like?",
        answer: "We offer flexible engagement models — fixed-price for well-defined projects, and time-and-materials for evolving scopes. As a lean, AI-augmented team, our rates are significantly lower than traditional agencies while delivering the same enterprise-grade quality. Contact us for a detailed quote."
    },
    {
        question: "How large is your team?",
        answer: "We're a lean team of senior engineers and AI specialists. By leveraging AI tools in our own workflow, a small team of experts can deliver what traditionally requires much larger teams. This means lower overhead for you and faster, more focused execution."
    },
    {
        question: "What technologies do you work with?",
        answer: "Our core stack includes Python, React, Node.js, and major cloud platforms (AWS, GCP). For AI/ML we work with TensorFlow, PyTorch, OpenAI, LangChain, and Hugging Face. We choose the best tool for each project rather than forcing a one-size-fits-all approach."
    },
    {
        question: "Do you provide ongoing support after launch?",
        answer: "Absolutely. We offer maintenance and support packages that include monitoring, bug fixes, performance optimization, and feature updates. We believe in long-term partnerships, not just one-off projects."
    },
    {
        question: "Can you work with our existing systems?",
        answer: "Yes. We specialize in integrating AI capabilities into existing software ecosystems. Whether it's adding intelligence to your current platform, building microservices that plug into your architecture, or migrating legacy systems — we adapt to your environment."
    }
];

export default function FAQ() {
    return (
        <section className="py-24 md:py-32 bg-slate-950 relative overflow-hidden" id="faq">
            {/* Background elements */}
            <div className="absolute inset-0">
                <div className="absolute top-1/3 right-1/4 w-80 h-80 bg-blue-500/5 rounded-full blur-3xl" />
                <div className="absolute bottom-1/4 left-1/3 w-72 h-72 bg-green-500/5 rounded-full blur-3xl" />
            </div>

            <div className="max-w-4xl mx-auto px-6 relative z-10">
                <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    whileInView={{ opacity: 1, scale: 1 }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.6 }}
                    className="text-center mb-16"
                >
                    <span className="inline-flex items-center gap-2 px-4 py-2 bg-white/5 border border-white/10 text-blue-400 rounded-full text-sm font-medium mb-4">
                        <HelpCircle className="w-4 h-4" />
                        FAQ
                    </span>
                    <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">
                        Common
                        <span className="block text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-green-400">
                            Questions
                        </span>
                    </h2>
                    <p className="text-lg text-gray-400 max-w-2xl mx-auto">
                        Everything you need to know about working with us.
                    </p>
                </motion.div>

                <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    whileInView={{ opacity: 1, scale: 1 }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.5, delay: 0.2 }}
                >
                    <Accordion type="single" collapsible className="space-y-4">
                        {faqs.map((faq, idx) => (
                            <AccordionItem
                                key={idx}
                                value={`item-${idx}`}
                                className="bg-white/5 border border-white/10 rounded-xl px-6 data-[state=open]:bg-white/[0.08] transition-colors"
                            >
                                <AccordionTrigger className="text-white hover:no-underline text-left text-base py-5 [&>svg]:text-gray-400">
                                    {faq.question}
                                </AccordionTrigger>
                                <AccordionContent className="text-gray-400 leading-relaxed pb-5">
                                    {faq.answer}
                                </AccordionContent>
                            </AccordionItem>
                        ))}
                    </Accordion>
                </motion.div>
            </div>
        </section>
    );
}
