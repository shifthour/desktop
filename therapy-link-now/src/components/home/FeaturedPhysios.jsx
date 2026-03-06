import React from "react";
import { Link } from "react-router-dom";
import { createPageUrl } from "../../utils";
import { Button } from "@/components/ui/button";
import { ArrowRight } from "lucide-react";
import { motion } from "framer-motion";
import PhysioCard from "@/components/shared/PhysioCard";

export default function FeaturedPhysios({ physiotherapists, isLoading }) {
  const source = physiotherapists || [];
  const featured = source
    .filter(p => p.status === "approved")
    .sort((a, b) => (b.rating || 0) - (a.rating || 0))
    .slice(0, 4);

  if (isLoading) {
    return (
      <section className="py-24 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <div className="h-8 w-64 bg-gray-100 rounded-lg mx-auto mb-4 animate-pulse" />
            <div className="h-5 w-96 bg-gray-50 rounded-lg mx-auto animate-pulse" />
          </div>
          <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-6">
            {[1,2,3,4].map(i => (
              <div key={i} className="h-80 bg-gray-50 rounded-2xl animate-pulse" />
            ))}
          </div>
        </div>
      </section>
    );
  }

  if (featured.length === 0) return null;

  return (
    <section className="py-24 bg-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="flex items-end justify-between mb-16"
        >
          <div>
            <h2 className="text-3xl sm:text-4xl font-bold text-gray-900 tracking-tight">
              Top-rated specialists
            </h2>
            <p className="mt-4 text-lg text-gray-400">
              Trusted by hundreds of patients for exceptional care
            </p>
          </div>
          <Link to={createPageUrl("Search")} className="hidden sm:block">
            <Button variant="ghost" className="text-teal-600 hover:text-teal-700 hover:bg-teal-50">
              View all
              <ArrowRight className="w-4 h-4 ml-2" />
            </Button>
          </Link>
        </motion.div>

        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-6">
          {featured.map((physio, i) => (
            <motion.div
              key={physio.id}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.1 }}
            >
              <PhysioCard physio={physio} />
            </motion.div>
          ))}
        </div>

        <div className="mt-10 text-center sm:hidden">
          <Link to={createPageUrl("Search")}>
            <Button variant="outline" className="rounded-xl">
              View All Physiotherapists
              <ArrowRight className="w-4 h-4 ml-2" />
            </Button>
          </Link>
        </div>
      </div>
    </section>
  );
}