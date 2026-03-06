import React, { useState, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Search, SlidersHorizontal, X, Star, MapPin } from "lucide-react";

const SPECIALIZATIONS = [
  { name: "Sports Injury", icon: "🏃" },
  { name: "Neurological", icon: "🧠" },
  { name: "Orthopedic", icon: "🦴" },
  { name: "Pediatric", icon: "👶" },
  { name: "Geriatric", icon: "🧓" },
  { name: "Cardiopulmonary", icon: "❤️" },
  { name: "Women's Health", icon: "🌸" },
  { name: "Respiratory", icon: "🫁" },
  { name: "Post-Surgical", icon: "🏥" },
];

export default function SearchFilters({ filters, onFilterChange, resultCount, physios = [] }) {
  const [mobileOpen, setMobileOpen] = useState(false);

  const updateFilter = (key, value) => {
    onFilterChange({ ...filters, [key]: value });
  };

  const toggleSpecialization = (spec) => {
    const current = filters.specializations || [];
    const updated = current.includes(spec)
      ? current.filter((s) => s !== spec)
      : [...current, spec];
    updateFilter("specializations", updated);
  };

  // Extract unique cities from physio data
  const cities = useMemo(() => {
    const citySet = new Set();
    physios.forEach((p) => {
      if (p.city) citySet.add(p.city);
    });
    return [...citySet].sort();
  }, [physios]);

  const clearFilters = () => {
    onFilterChange({
      search: "",
      specializations: [],
      location: "all",
      visitType: "all",
      maxPrice: 500,
      minRating: 0,
      gender: "all",
      availability: "all",
      sortBy: "rating",
    });
  };

  const activeCount = [
    (filters.specializations || []).length > 0,
    filters.location && filters.location !== "all",
    filters.visitType !== "all",
    filters.maxPrice < 500,
    filters.minRating > 0,
    filters.gender !== "all",
    filters.availability !== "all",
  ].filter(Boolean).length;

  const FilterContent = () => (
    <div className="space-y-6">
      {/* Specializations — Checkboxes */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Specialization</h4>
        <div className="space-y-2.5">
          {SPECIALIZATIONS.map((spec) => {
            const checked = (filters.specializations || []).includes(spec.name);
            return (
              <label
                key={spec.name}
                className="flex items-center gap-2.5 cursor-pointer group"
              >
                <Checkbox
                  checked={checked}
                  onCheckedChange={() => toggleSpecialization(spec.name)}
                  className="data-[state=checked]:bg-teal-600 data-[state=checked]:border-teal-600"
                />
                <span className="text-sm text-gray-600 group-hover:text-gray-900 transition-colors">
                  {spec.icon} {spec.name}
                </span>
              </label>
            );
          })}
        </div>
      </div>

      {/* Location */}
      {cities.length > 0 && (
        <div>
          <h4 className="text-sm font-semibold text-gray-900 mb-3">Location</h4>
          <Select
            value={filters.location || "all"}
            onValueChange={(v) => updateFilter("location", v)}
          >
            <SelectTrigger className="rounded-xl border-gray-200">
              <div className="flex items-center gap-2">
                <MapPin className="w-4 h-4 text-gray-400" />
                <SelectValue />
              </div>
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Locations</SelectItem>
              {cities.map((city) => (
                <SelectItem key={city} value={city}>{city}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {/* Visit Type — Radio */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Visit Type</h4>
        <RadioGroup
          value={filters.visitType || "all"}
          onValueChange={(v) => updateFilter("visitType", v)}
          className="space-y-2.5"
        >
          {[
            { value: "all", label: "Any" },
            { value: "clinic", label: "🏥 Clinic Only" },
            { value: "home_visit", label: "🏠 Home Visit Only" },
            { value: "both", label: "🏥🏠 Both Available" },
          ].map((opt) => (
            <label key={opt.value} className="flex items-center gap-2.5 cursor-pointer">
              <RadioGroupItem
                value={opt.value}
                className="border-gray-300 text-teal-600 data-[state=checked]:border-teal-600"
              />
              <span className="text-sm text-gray-600">{opt.label}</span>
            </label>
          ))}
        </RadioGroup>
      </div>

      {/* Price Range — Slider */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">
          Max Price:{" "}
          <span className="text-teal-600 font-bold">£{filters.maxPrice || 500}</span>
        </h4>
        <Slider
          value={[filters.maxPrice || 500]}
          onValueChange={([v]) => updateFilter("maxPrice", v)}
          max={500}
          min={20}
          step={5}
          className="py-2 [&_[data-radix-slider-range]]:bg-teal-600 [&_[data-radix-slider-thumb]]:border-teal-600"
        />
        <div className="flex justify-between text-xs text-gray-400 mt-1">
          <span>£20</span>
          <span>£500</span>
        </div>
      </div>

      {/* Availability */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Availability</h4>
        <RadioGroup
          value={filters.availability || "all"}
          onValueChange={(v) => updateFilter("availability", v)}
          className="space-y-2.5"
        >
          {[
            { value: "all", label: "Any Time" },
            { value: "today", label: "Today" },
            { value: "this_week", label: "This Week" },
            { value: "next_week", label: "Next Week" },
          ].map((opt) => (
            <label key={opt.value} className="flex items-center gap-2.5 cursor-pointer">
              <RadioGroupItem
                value={opt.value}
                className="border-gray-300 text-teal-600 data-[state=checked]:border-teal-600"
              />
              <span className="text-sm text-gray-600">{opt.label}</span>
            </label>
          ))}
        </RadioGroup>
      </div>

      {/* Rating — Radio */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Minimum Rating</h4>
        <RadioGroup
          value={String(filters.minRating || 0)}
          onValueChange={(v) => updateFilter("minRating", parseFloat(v))}
          className="space-y-2.5"
        >
          {[
            { value: "0", label: "Any Rating" },
            { value: "4", label: "★ 4.0 & above" },
            { value: "4.5", label: "★ 4.5 & above" },
          ].map((opt) => (
            <label key={opt.value} className="flex items-center gap-2.5 cursor-pointer">
              <RadioGroupItem
                value={opt.value}
                className="border-gray-300 text-teal-600 data-[state=checked]:border-teal-600"
              />
              <span className="text-sm text-gray-600">{opt.label}</span>
            </label>
          ))}
        </RadioGroup>
      </div>

      {/* Gender — Radio */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Gender Preference</h4>
        <RadioGroup
          value={filters.gender || "all"}
          onValueChange={(v) => updateFilter("gender", v)}
          className="space-y-2.5"
        >
          {[
            { value: "all", label: "Any" },
            { value: "male", label: "Male" },
            { value: "female", label: "Female" },
          ].map((opt) => (
            <label key={opt.value} className="flex items-center gap-2.5 cursor-pointer">
              <RadioGroupItem
                value={opt.value}
                className="border-gray-300 text-teal-600 data-[state=checked]:border-teal-600"
              />
              <span className="text-sm text-gray-600">{opt.label}</span>
            </label>
          ))}
        </RadioGroup>
      </div>

      {/* Sort By */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Sort By</h4>
        <Select
          value={filters.sortBy || "rating"}
          onValueChange={(v) => updateFilter("sortBy", v)}
        >
          <SelectTrigger className="rounded-xl border-gray-200">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="rating">Highest Rated</SelectItem>
            <SelectItem value="price_low">Price: Low to High</SelectItem>
            <SelectItem value="price_high">Price: High to Low</SelectItem>
            <SelectItem value="experience">Most Experienced</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Clear All */}
      {activeCount > 0 && (
        <Button
          variant="ghost"
          onClick={clearFilters}
          className="w-full text-gray-500 hover:text-gray-700"
        >
          <X className="w-4 h-4 mr-2" />
          Clear all filters
        </Button>
      )}
    </div>
  );

  return (
    <>
      {/* Search Bar + Mobile Filter Trigger */}
      <div className="flex items-center gap-3 mb-6">
        <div className="relative flex-1">
          <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-300" />
          <Input
            placeholder="Search by name, specialization, or city..."
            value={filters.search || ""}
            onChange={(e) => updateFilter("search", e.target.value)}
            className="pl-12 h-14 rounded-xl border-gray-200 text-base focus:border-teal-300 focus:ring-teal-100"
          />
        </div>

        {/* Mobile filter trigger */}
        <Sheet open={mobileOpen} onOpenChange={setMobileOpen}>
          <SheetTrigger asChild>
            <Button
              variant="outline"
              className="lg:hidden h-14 px-4 rounded-xl border-gray-200 relative"
            >
              <SlidersHorizontal className="w-5 h-5" />
              {activeCount > 0 && (
                <span className="absolute -top-1.5 -right-1.5 w-5 h-5 bg-teal-600 text-white text-xs rounded-full flex items-center justify-center font-semibold">
                  {activeCount}
                </span>
              )}
            </Button>
          </SheetTrigger>
          <SheetContent className="w-[320px] sm:w-[380px] overflow-y-auto">
            <SheetHeader>
              <SheetTitle className="flex items-center justify-between">
                <span>Filters</span>
                {activeCount > 0 && (
                  <Badge className="bg-teal-50 text-teal-700 border-0">
                    {activeCount} active
                  </Badge>
                )}
              </SheetTitle>
            </SheetHeader>
            <div className="mt-6 pb-24">
              <FilterContent />
            </div>
            {/* Sticky bottom apply button in mobile sheet */}
            <div className="sticky bottom-0 left-0 right-0 bg-white border-t border-gray-100 p-4 -mx-6 -mb-6">
              <Button
                onClick={() => setMobileOpen(false)}
                className="w-full bg-teal-600 hover:bg-teal-700 rounded-xl h-12 text-base"
              >
                Show {resultCount ?? 0} result{resultCount !== 1 ? "s" : ""}
              </Button>
            </div>
          </SheetContent>
        </Sheet>
      </div>

      {/* Desktop Sidebar Filters */}
      <div className="hidden lg:block bg-white rounded-2xl border border-gray-100 p-6 shadow-sm">
        <div className="flex items-center justify-between mb-6">
          <h3 className="font-bold text-gray-900 text-base">Filters</h3>
          {activeCount > 0 && (
            <Badge className="bg-teal-50 text-teal-700 border-0 text-xs">
              {activeCount} active
            </Badge>
          )}
        </div>
        <FilterContent />
      </div>
    </>
  );
}
