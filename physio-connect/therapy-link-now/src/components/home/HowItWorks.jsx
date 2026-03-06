import React from "react";
import { motion } from "framer-motion";
import { Search, CalendarCheck, UserCheck } from "lucide-react";

const steps = [
  {
    icon: Search,
    title: "Search & Filter",
    description: "Browse our verified physiotherapists by specialization, location, availability, and price.",
    number: "01",
  },
  {
    icon: CalendarCheck,
    title: "Book Instantly",
    description: "Select your preferred date and time slot. Get instant confirmation with no waiting.",
    number: "02",
  },
  {
    icon: UserCheck,
    title: "Get Treatment",
    description: "Visit the clinic or get treated at home. Rate your experience and track your recovery.",
    number: "03",
  },
];

export default function HowItWorks() {
  return (
    <section className="py-24 bg-gradient-to-b from-gray-50 to-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <h2 className="text-3xl sm:text-4xl font-bold text-gray-900 tracking-tight">
            How it works
          </h2>
          <p className="mt-4 text-lg text-gray-400">
            Three simple steps to your recovery journey
          </p>
        </motion.div>

        <div className="grid md:grid-cols-3 gap-8 lg:gap-12">
          {steps.map((step, i) => (
            <motion.div
              key={step.number}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.15 }}
              className="relative text-center group"
            >
              {i < steps.length - 1 && (
                <div className="hidden md:block absolute top-16 left-[60%] w-[80%] h-[2px] bg-gradient-to-r from-teal-200 to-transparent" />
              )}
              <div className="relative inline-flex mb-6">
                <div className="w-20 h-20 rounded-2xl bg-white shadow-lg shadow-gray-100 flex items-center justify-center group-hover:shadow-xl group-hover:shadow-teal-100/50 transition-all duration-300 border border-gray-50">
                  <step.icon className="w-8 h-8 text-teal-600" />
                </div>
                <span className="absolute -top-2 -right-2 w-7 h-7 rounded-full bg-teal-600 text-white text-xs font-bold flex items-center justify-center">
                  {step.number}
                </span>
              </div>
              <h3 className="text-xl font-bold text-gray-900 mb-3">{step.title}</h3>
              <p className="text-gray-400 leading-relaxed max-w-xs mx-auto">{step.description}</p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}