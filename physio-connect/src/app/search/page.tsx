"use client";

import { useState, useEffect, useMemo } from "react";
import { Physiotherapist } from "@/lib/types";
import { fetchPhysios } from "@/lib/api-client";
import { SpecializationItem, fetchSpecializations } from "@/lib/api-client";
import ProviderCard from "@/components/ProviderCard";

type VisitType = "clinic" | "home" | "online";
type SortOption = "relevance" | "rating" | "price-low" | "price-high" | "experience";

export default function SearchPage() {
  const [allPhysios, setAllPhysios] = useState<Physiotherapist[]>([]);
  const [specializations, setSpecializations] = useState<SpecializationItem[]>([]);
  const [loading, setLoading] = useState(true);

  const [selectedSpecs, setSelectedSpecs] = useState<string[]>([]);
  const [visitType, setVisitType] = useState<VisitType | "any">("any");
  const [priceRange, setPriceRange] = useState<[number, number]>([0, 200]);
  const [minRating, setMinRating] = useState(0);
  const [gender, setGender] = useState<"any" | "male" | "female">("any");
  const [sortBy, setSortBy] = useState<SortOption>("relevance");
  const [showMobileFilters, setShowMobileFilters] = useState(false);
  const [searchLocation, setSearchLocation] = useState("London");

  useEffect(() => {
    Promise.all([fetchPhysios(), fetchSpecializations()])
      .then(([physios, specs]) => {
        setAllPhysios(physios);
        setSpecializations(specs);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const filtered = useMemo(() => {
    let result = allPhysios.filter((p) => {
      if (selectedSpecs.length > 0 && !selectedSpecs.some((s) => p.specializations.includes(s))) return false;
      if (visitType !== "any" && !p.visitTypes.includes(visitType)) return false;
      const minPrice = Math.min(...p.services.map((s) => s.price));
      if (minPrice < priceRange[0] || minPrice > priceRange[1]) return false;
      if (p.rating < minRating) return false;
      if (gender !== "any" && p.gender !== gender) return false;
      return true;
    });

    switch (sortBy) {
      case "rating":
        result.sort((a, b) => b.rating - a.rating);
        break;
      case "price-low":
        result.sort((a, b) => Math.min(...a.services.map((s) => s.price)) - Math.min(...b.services.map((s) => s.price)));
        break;
      case "price-high":
        result.sort((a, b) => Math.min(...b.services.map((s) => s.price)) - Math.min(...a.services.map((s) => s.price)));
        break;
      case "experience":
        result.sort((a, b) => b.experience - a.experience);
        break;
    }

    return result;
  }, [allPhysios, selectedSpecs, visitType, priceRange, minRating, gender, sortBy]);

  function toggleSpec(spec: string) {
    setSelectedSpecs((prev) =>
      prev.includes(spec) ? prev.filter((s) => s !== spec) : [...prev, spec]
    );
  }

  function clearAll() {
    setSelectedSpecs([]);
    setVisitType("any");
    setPriceRange([0, 3000]);
    setMinRating(0);
    setGender("any");
  }

  const activeFilterCount = selectedSpecs.length + (visitType !== "any" ? 1 : 0) + (minRating > 0 ? 1 : 0) + (gender !== "any" ? 1 : 0);

  const FilterPanel = () => (
    <div className="space-y-6">
      {/* Specialization */}
      <div>
        <h3 className="font-semibold text-gray-900 mb-3">Specialisation</h3>
        <div className="space-y-2">
          {specializations.map((spec) => (
            <label key={spec.name} className="flex items-center gap-2 cursor-pointer group">
              <input
                type="checkbox"
                checked={selectedSpecs.includes(spec.name)}
                onChange={() => toggleSpec(spec.name)}
                className="w-4 h-4 rounded border-gray-300 text-primary focus:ring-primary"
              />
              <span className="text-sm text-gray-700 group-hover:text-primary transition-colors">
                {spec.icon} {spec.name}
              </span>
              <span className="text-xs text-gray-400 ml-auto">({spec.count})</span>
            </label>
          ))}
        </div>
      </div>

      {/* Visit Type */}
      <div>
        <h3 className="font-semibold text-gray-900 mb-3">Visit Type</h3>
        <div className="space-y-2">
          {(["any", "clinic", "home", "online"] as const).map((type) => (
            <label key={type} className="flex items-center gap-2 cursor-pointer">
              <input
                type="radio"
                name="visitType"
                checked={visitType === type}
                onChange={() => setVisitType(type)}
                className="w-4 h-4 border-gray-300 text-primary focus:ring-primary"
              />
              <span className="text-sm text-gray-700 capitalize">
                {type === "any" ? "Any" : type === "clinic" ? "🏥 Clinic" : type === "home" ? "🏠 Home Visit" : "💻 Online"}
              </span>
            </label>
          ))}
        </div>
      </div>

      {/* Price Range */}
      <div>
        <h3 className="font-semibold text-gray-900 mb-3">Price Range</h3>
        <div className="px-1">
          <input
            type="range"
            min={0}
            max={200}
            step={10}
            value={priceRange[1]}
            onChange={(e) => setPriceRange([priceRange[0], parseInt(e.target.value)])}
            className="w-full accent-primary"
          />
          <div className="flex justify-between text-sm text-gray-500">
            <span>£{priceRange[0]}</span>
            <span>£{priceRange[1]}</span>
          </div>
        </div>
      </div>

      {/* Rating */}
      <div>
        <h3 className="font-semibold text-gray-900 mb-3">Minimum Rating</h3>
        <div className="space-y-2">
          {[0, 4, 4.5].map((r) => (
            <label key={r} className="flex items-center gap-2 cursor-pointer">
              <input
                type="radio"
                name="rating"
                checked={minRating === r}
                onChange={() => setMinRating(r)}
                className="w-4 h-4 border-gray-300 text-primary focus:ring-primary"
              />
              <span className="text-sm text-gray-700">
                {r === 0 ? "Any" : `★ ${r}+`}
              </span>
            </label>
          ))}
        </div>
      </div>

      {/* Gender */}
      <div>
        <h3 className="font-semibold text-gray-900 mb-3">Gender</h3>
        <div className="space-y-2">
          {(["any", "male", "female"] as const).map((g) => (
            <label key={g} className="flex items-center gap-2 cursor-pointer">
              <input
                type="radio"
                name="gender"
                checked={gender === g}
                onChange={() => setGender(g)}
                className="w-4 h-4 border-gray-300 text-primary focus:ring-primary"
              />
              <span className="text-sm text-gray-700 capitalize">{g}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Clear All */}
      <button onClick={clearAll} className="text-sm text-primary hover:underline font-medium">
        Clear All Filters
      </button>
    </div>
  );

  if (loading) {
    return (
      <div className="min-h-screen bg-cream flex items-center justify-center">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-primary border-t-transparent rounded-full animate-spin mx-auto mb-4" />
          <p className="text-gray-500">Loading physiotherapists...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-cream">
      {/* Search Bar */}
      <div className="sticky top-16 z-40 bg-white/90 backdrop-blur-md border-b border-primary-100/50 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3">
          <div className="flex gap-3 items-center">
            <select className="input-field max-w-xs" defaultValue="" aria-label="Specialization">
              <option value="">All Specialisations</option>
              {specializations.map((s) => (
                <option key={s.name} value={s.name}>{s.name}</option>
              ))}
            </select>
            <input
              type="text"
              placeholder="📍 Location"
              className="input-field max-w-xs"
              value={searchLocation}
              onChange={(e) => setSearchLocation(e.target.value)}
            />
            <button className="btn-primary whitespace-nowrap hidden sm:block">Search</button>

            {/* Mobile filter toggle */}
            <button
              onClick={() => setShowMobileFilters(true)}
              className="lg:hidden btn-outline relative !py-2 !px-3"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
              </svg>
              {activeFilterCount > 0 && (
                <span className="absolute -top-1 -right-1 w-5 h-5 bg-primary text-white text-xs rounded-full flex items-center justify-center">
                  {activeFilterCount}
                </span>
              )}
            </button>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <div className="flex gap-8">
          {/* Desktop Sidebar */}
          <aside className="hidden lg:block w-64 flex-shrink-0">
            <div className="sticky top-36 card">
              <h2 className="font-bold text-lg text-gray-900 mb-4">Filters</h2>
              <FilterPanel />
            </div>
          </aside>

          {/* Results */}
          <div className="flex-1 min-w-0">
            <div className="flex items-center justify-between mb-4">
              <p className="text-gray-600">
                Showing <span className="font-semibold text-gray-900">{filtered.length}</span> physiotherapists
              </p>
              <select
                value={sortBy}
                onChange={(e) => setSortBy(e.target.value as SortOption)}
                className="input-field !w-auto text-sm"
                aria-label="Sort by"
              >
                <option value="relevance">Sort: Relevance</option>
                <option value="rating">Highest Rated</option>
                <option value="price-low">Price: Low to High</option>
                <option value="price-high">Price: High to Low</option>
                <option value="experience">Most Experienced</option>
              </select>
            </div>

            {filtered.length === 0 ? (
              <div className="card text-center py-16">
                <div className="text-5xl mb-4">🔍</div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">No results found</h3>
                <p className="text-gray-500 mb-6">Try adjusting your filters to see more physiotherapists.</p>
                <button onClick={clearAll} className="btn-primary">Clear All Filters</button>
              </div>
            ) : (
              <div className="space-y-4">
                {filtered.map((physio) => (
                  <ProviderCard key={physio.id} physio={physio} />
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Mobile Filter Bottom Sheet */}
      {showMobileFilters && (
        <div className="fixed inset-0 z-50 lg:hidden">
          <div className="absolute inset-0 bg-black/50" onClick={() => setShowMobileFilters(false)} />
          <div className="absolute bottom-0 left-0 right-0 bg-white rounded-t-2xl max-h-[80vh] overflow-y-auto">
            <div className="sticky top-0 bg-white border-b border-gray-100 px-4 py-3 flex items-center justify-between">
              <h2 className="font-bold text-lg">Filters</h2>
              <button onClick={() => setShowMobileFilters(false)} className="p-2" aria-label="Close filters">
                <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <div className="p-4">
              <FilterPanel />
            </div>
            <div className="sticky bottom-0 bg-white border-t border-gray-100 p-4">
              <button
                onClick={() => setShowMobileFilters(false)}
                className="btn-primary w-full"
              >
                Show {filtered.length} Results
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
