import React from "react";
import { Button } from "@/components/ui/button";
import { Search, ArrowRight, Shield, Clock, Star } from "lucide-react";
import { Link } from "react-router-dom";
import { createPageUrl } from "../../utils";
import { motion } from "framer-motion";

export default function HeroSection() {
  return (
    <section className="relative overflow-hidden bg-gradient-to-br from-teal-50 via-white to-emerald-50 min-h-[90vh] flex items-center">
      {/* Decorative blobs */}
      <div className="absolute top-20 right-10 w-72 h-72 bg-teal-100/40 rounded-full blur-3xl" />
      <div className="absolute bottom-10 left-10 w-96 h-96 bg-emerald-100/30 rounded-full blur-3xl" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-teal-50/50 rounded-full blur-3xl" />

      <div className="relative z-10 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-20 w-full">
        <div className="grid lg:grid-cols-2 gap-16 items-center">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8, ease: "easeOut" }}
          >
            <div className="inline-flex items-center gap-2 bg-teal-100/60 text-teal-700 px-4 py-2 rounded-full text-sm font-medium mb-6 backdrop-blur-sm">
              <Shield className="w-4 h-4" />
              Verified & Licensed Professionals
            </div>

            <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-gray-900 leading-tight tracking-tight">
              Your recovery,{" "}
              <span className="text-transparent bg-clip-text bg-gradient-to-r from-teal-600 to-emerald-600">
                reimagined
              </span>
            </h1>

            <p className="mt-6 text-lg sm:text-xl text-gray-500 leading-relaxed max-w-lg">
              Connect with top physiotherapists near you. Book clinic or home visits in seconds with real-time availability.
            </p>

            <div className="mt-10 flex flex-col sm:flex-row gap-4">
              <Link to={createPageUrl("Search")}>
                <Button size="lg" className="bg-teal-600 hover:bg-teal-700 text-white rounded-xl px-8 h-14 text-base shadow-lg shadow-teal-600/20 transition-all hover:shadow-xl hover:shadow-teal-600/30">
                  <Search className="w-5 h-5 mr-2" />
                  Find a Physiotherapist
                </Button>
              </Link>
              <Link to={createPageUrl("PhysioRegistration")}>
                <Button size="lg" variant="outline" className="rounded-xl px-8 h-14 text-base border-gray-200 hover:bg-gray-50">
                  Join as Professional
                  <ArrowRight className="w-4 h-4 ml-2" />
                </Button>
              </Link>
            </div>

            <div className="mt-12 flex items-center gap-8">
              {[
                { icon: Star, label: "4.9 Avg Rating", sublabel: "500+ reviews" },
                { icon: Shield, label: "100% Verified", sublabel: "Licensed pros" },
                { icon: Clock, label: "24hr Booking", sublabel: "Instant confirm" },
              ].map((item, i) => (
                <div key={i} className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-xl bg-teal-50 flex items-center justify-center">
                    <item.icon className="w-5 h-5 text-teal-600" />
                  </div>
                  <div>
                    <p className="text-sm font-semibold text-gray-900">{item.label}</p>
                    <p className="text-xs text-gray-400">{item.sublabel}</p>
                  </div>
                </div>
              ))}
            </div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ duration: 0.8, delay: 0.2, ease: "easeOut" }}
            className="hidden lg:block"
          >
            <div className="relative">
              <div className="absolute inset-0 bg-gradient-to-br from-teal-200 to-emerald-200 rounded-3xl transform rotate-3 scale-105 opacity-20" />
              <img
                src="https://images.unsplash.com/photo-1576091160550-2173dba999ef?w=700&h=800&fit=crop&q=80"
                alt="Physiotherapy session"
                className="relative rounded-3xl shadow-2xl w-full h-[550px] object-cover"
              />
              {/* Floating card */}
              <motion.div
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.6, duration: 0.5 }}
                className="absolute -left-8 bottom-20 bg-white/90 backdrop-blur-md rounded-2xl p-5 shadow-xl border border-white/50"
              >
                <div className="flex items-center gap-3">
                  <div className="w-12 h-12 rounded-full bg-emerald-100 flex items-center justify-center">
                    <Star className="w-6 h-6 text-emerald-600 fill-emerald-600" />
                  </div>
                  <div>
                    <p className="font-semibold text-gray-900">200+ Specialists</p>
                    <p className="text-sm text-gray-400">Ready to help you</p>
                  </div>
                </div>
              </motion.div>
            </div>
          </motion.div>
        </div>
      </div>
    </section>
  );
}