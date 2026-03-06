import React from "react";
import { Link } from "react-router-dom";
import { createPageUrl } from "../../utils";
import { motion } from "framer-motion";
import { Activity, Brain, Bone, Baby, Heart, Users, Dumbbell, Wind } from "lucide-react";

const specializations = [
  { name: "Sports Injury", icon: Dumbbell, color: "from-orange-400 to-red-400", bg: "bg-orange-50" },
  { name: "Neurological", icon: Brain, color: "from-purple-400 to-indigo-400", bg: "bg-purple-50" },
  { name: "Orthopedic", icon: Bone, color: "from-blue-400 to-cyan-400", bg: "bg-blue-50" },
  { name: "Pediatric", icon: Baby, color: "from-pink-400 to-rose-400", bg: "bg-pink-50" },
  { name: "Geriatric", icon: Users, color: "from-amber-400 to-yellow-500", bg: "bg-amber-50" },
  { name: "Cardiopulmonary", icon: Heart, color: "from-red-400 to-pink-400", bg: "bg-red-50" },
  { name: "Women's Health", icon: Activity, color: "from-teal-400 to-emerald-400", bg: "bg-teal-50" },
  { name: "Respiratory", icon: Wind, color: "from-sky-400 to-blue-400", bg: "bg-sky-50" },
];

export default function SpecializationGrid() {
  return (
    <section className="py-24 bg-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <h2 className="text-3xl sm:text-4xl font-bold text-gray-900 tracking-tight">
            Find care by specialization
          </h2>
          <p className="mt-4 text-lg text-gray-400 max-w-2xl mx-auto">
            Our network covers every area of physiotherapy, from sports injuries to neurological rehabilitation.
          </p>
        </motion.div>

        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4 sm:gap-6">
          {specializations.map((spec, i) => (
            <motion.div
              key={spec.name}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.05 }}
            >
              <Link
                to={createPageUrl("Search") + `?specialization=${encodeURIComponent(spec.name)}`}
                className={`group block ${spec.bg} rounded-2xl p-6 sm:p-8 transition-all duration-300 hover:shadow-lg hover:shadow-gray-200/50 hover:-translate-y-1 border border-transparent hover:border-gray-100`}
              >
                <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${spec.color} flex items-center justify-center mb-4 group-hover:scale-110 transition-transform duration-300`}>
                  <spec.icon className="w-6 h-6 text-white" />
                </div>
                <h3 className="font-semibold text-gray-900 text-sm sm:text-base">{spec.name}</h3>
              </Link>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}