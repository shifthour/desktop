import React, { useState, useMemo, useEffect } from "react";
import { base44 } from "@/api/base44Client";
import { useQuery } from "@tanstack/react-query";
import SearchFilters from "@/components/search/SearchFilters";
import PhysioCard from "@/components/shared/PhysioCard";
import { Loader2 } from "lucide-react";
import { motion } from "framer-motion";

export default function Search() {
  const urlParams = new URLSearchParams(window.location.search);
  const initialSpec = urlParams.get("specialization") || "all";

  const [filters, setFilters] = useState({
    search: "",
    specialization: initialSpec,
    visitType: "all",
    maxPrice: 200,
    sortBy: "rating",
  });

  const { data: physios, isLoading } = useQuery({
    queryKey: ["physios-search"],
    queryFn: () => base44.entities.Physiotherapist.filter({ status: "approved" }),
    initialData: [],
  });

  const filtered = useMemo(() => {
    let list = [...physios];

    if (filters.search) {
      const q = filters.search.toLowerCase();
      list = list.filter(p =>
        p.full_name?.toLowerCase().includes(q) ||
        p.city?.toLowerCase().includes(q) ||
        (p.specializations || []).some(s => s.toLowerCase().includes(q))
      );
    }

    if (filters.specialization !== "all") {
      list = list.filter(p => (p.specializations || []).includes(filters.specialization));
    }

    if (filters.visitType !== "all") {
      list = list.filter(p => p.visit_type === filters.visitType || p.visit_type === "both");
    }

    if (filters.maxPrice < 200) {
      list = list.filter(p => (p.consultation_fee || 0) <= filters.maxPrice);
    }

    switch (filters.sortBy) {
      case "rating":
        list.sort((a, b) => (b.rating || 0) - (a.rating || 0));
        break;
      case "price_low":
        list.sort((a, b) => (a.consultation_fee || 0) - (b.consultation_fee || 0));
        break;
      case "price_high":
        list.sort((a, b) => (b.consultation_fee || 0) - (a.consultation_fee || 0));
        break;
      case "experience":
        list.sort((a, b) => (b.experience_years || 0) - (a.experience_years || 0));
        break;
    }

    return list;
  }, [physios, filters]);

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <h1 className="text-3xl font-bold text-gray-900">Find Your Physiotherapist</h1>
          <p className="mt-2 text-gray-400">
            {filtered.length} specialist{filtered.length !== 1 ? "s" : ""} available
          </p>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="lg:grid lg:grid-cols-[280px_1fr] lg:gap-8">
          <div className="mb-6 lg:mb-0">
            <div className="lg:hidden">
              <SearchFilters filters={filters} onFilterChange={setFilters} />
            </div>
            <div className="hidden lg:block lg:sticky lg:top-8">
              <SearchFilters filters={filters} onFilterChange={setFilters} />
            </div>
          </div>

          <div>
            <div className="lg:hidden mb-6">
              {/* Search bar shown on mobile via SearchFilters */}
            </div>

            {isLoading ? (
              <div className="flex items-center justify-center py-20">
                <Loader2 className="w-8 h-8 text-teal-600 animate-spin" />
              </div>
            ) : filtered.length === 0 ? (
              <div className="text-center py-20">
                <div className="w-20 h-20 rounded-full bg-gray-100 flex items-center justify-center mx-auto mb-4">
                  <span className="text-3xl">🔍</span>
                </div>
                <h3 className="text-xl font-semibold text-gray-900 mb-2">No results found</h3>
                <p className="text-gray-400">Try adjusting your filters or search terms</p>
              </div>
            ) : (
              <div className="grid sm:grid-cols-2 xl:grid-cols-3 gap-6">
                {filtered.map((physio, i) => (
                  <motion.div
                    key={physio.id}
                    initial={{ opacity: 0, y: 15 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: i * 0.05 }}
                  >
                    <PhysioCard physio={physio} />
                  </motion.div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}