import React, { useState, useMemo } from "react";
import { api as base44 } from "@/api/apiClient";
import { useQuery } from "@tanstack/react-query";
import SearchFilters from "@/components/search/SearchFilters";
import PhysioCard from "@/components/shared/PhysioCard";
import { Loader2 } from "lucide-react";
import { motion } from "framer-motion";


export default function Search() {
  const urlParams = new URLSearchParams(window.location.search);
  const initialSpec = urlParams.get("specialization") || "";

  const [filters, setFilters] = useState({
    search: "",
    specializations: initialSpec ? [initialSpec] : [],
    location: "all",
    visitType: "all",
    maxPrice: 500,
    minRating: 0,
    gender: "all",
    availability: "all",
    sortBy: "rating",
  });

  const { data: apiPhysios, isLoading } = useQuery({
    queryKey: ["physios-search"],
    queryFn: () => base44.entities.Physiotherapist.filter({ status: "approved" }),
    initialData: [],
  });

  const physios = apiPhysios;

  const filtered = useMemo(() => {
    let list = [...physios];

    // Text search
    if (filters.search) {
      const q = filters.search.toLowerCase();
      list = list.filter(
        (p) =>
          p.full_name?.toLowerCase().includes(q) ||
          p.city?.toLowerCase().includes(q) ||
          (p.specializations || []).some((s) => s.toLowerCase().includes(q))
      );
    }

    // Specializations (multi-select checkboxes)
    if (filters.specializations && filters.specializations.length > 0) {
      list = list.filter((p) =>
        filters.specializations.some((s) => (p.specializations || []).includes(s))
      );
    }

    // Location
    if (filters.location && filters.location !== "all") {
      list = list.filter(
        (p) => p.city?.toLowerCase() === filters.location.toLowerCase()
      );
    }

    // Visit type
    if (filters.visitType !== "all") {
      list = list.filter(
        (p) => p.visit_type === filters.visitType || p.visit_type === "both"
      );
    }

    // Max price
    if (filters.maxPrice < 500) {
      list = list.filter((p) => (p.consultation_fee || 0) <= filters.maxPrice);
    }

    // Minimum rating
    if (filters.minRating > 0) {
      list = list.filter((p) => (p.rating || 0) >= filters.minRating);
    }

    // Gender
    if (filters.gender !== "all") {
      list = list.filter(
        (p) => p.gender?.toLowerCase() === filters.gender.toLowerCase()
      );
    }

    // Sort
    switch (filters.sortBy) {
      case "rating":
        list.sort((a, b) => (b.rating || 0) - (a.rating || 0));
        break;
      case "price_low":
        list.sort(
          (a, b) => (a.consultation_fee || 0) - (b.consultation_fee || 0)
        );
        break;
      case "price_high":
        list.sort(
          (a, b) => (b.consultation_fee || 0) - (a.consultation_fee || 0)
        );
        break;
      case "experience":
        list.sort(
          (a, b) => (b.experience_years || 0) - (a.experience_years || 0)
        );
        break;
    }

    return list;
  }, [physios, filters]);

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <h1 className="text-3xl font-bold text-gray-900">
            Find Your Physiotherapist
          </h1>
          <p className="mt-2 text-gray-400">
            {filtered.length} specialist{filtered.length !== 1 ? "s" : ""}{" "}
            available
          </p>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="lg:grid lg:grid-cols-[280px_1fr] lg:gap-8">
          {/* Sidebar / mobile filter */}
          <div className="mb-6 lg:mb-0">
            <div className="lg:hidden">
              <SearchFilters
                filters={filters}
                onFilterChange={setFilters}
                resultCount={filtered.length}
                physios={physios}
              />
            </div>
            <div className="hidden lg:block lg:sticky lg:top-24">
              <SearchFilters
                filters={filters}
                onFilterChange={setFilters}
                resultCount={filtered.length}
                physios={physios}
              />
            </div>
          </div>

          {/* Results */}
          <div>
            {isLoading ? (
              <div className="flex items-center justify-center py-20">
                <Loader2 className="w-8 h-8 text-teal-600 animate-spin" />
              </div>
            ) : filtered.length === 0 ? (
              <div className="text-center py-20">
                <div className="w-20 h-20 rounded-full bg-gray-100 flex items-center justify-center mx-auto mb-4">
                  <span className="text-3xl">🔍</span>
                </div>
                <h3 className="text-xl font-semibold text-gray-900 mb-2">
                  No results found
                </h3>
                <p className="text-gray-400">
                  Try adjusting your filters or search terms
                </p>
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
