import React, { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { Badge } from "@/components/ui/badge";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Search, SlidersHorizontal, X } from "lucide-react";

const SPECIALIZATIONS = [
  "Sports Injury", "Neurological", "Orthopedic", "Pediatric",
  "Geriatric", "Cardiopulmonary", "Women's Health", "Respiratory",
];

export default function SearchFilters({ filters, onFilterChange }) {
  const [mobileOpen, setMobileOpen] = useState(false);

  const updateFilter = (key, value) => {
    onFilterChange({ ...filters, [key]: value });
  };

  const clearFilters = () => {
    onFilterChange({
      search: "",
      specialization: "all",
      visitType: "all",
      maxPrice: 500,
      sortBy: "rating",
    });
  };

  const activeCount = [
    filters.specialization !== "all",
    filters.visitType !== "all",
    filters.maxPrice < 500,
  ].filter(Boolean).length;

  const FilterContent = () => (
    <div className="space-y-6">
      <div>
        <label className="text-sm font-medium text-gray-700 mb-2 block">Specialization</label>
        <Select value={filters.specialization || "all"} onValueChange={v => updateFilter("specialization", v)}>
          <SelectTrigger className="rounded-xl border-gray-200">
            <SelectValue placeholder="All specializations" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Specializations</SelectItem>
            {SPECIALIZATIONS.map(s => (
              <SelectItem key={s} value={s}>{s}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div>
        <label className="text-sm font-medium text-gray-700 mb-2 block">Visit Type</label>
        <Select value={filters.visitType || "all"} onValueChange={v => updateFilter("visitType", v)}>
          <SelectTrigger className="rounded-xl border-gray-200">
            <SelectValue placeholder="All types" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Types</SelectItem>
            <SelectItem value="clinic">Clinic Only</SelectItem>
            <SelectItem value="home_visit">Home Visit Only</SelectItem>
            <SelectItem value="both">Both Available</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div>
        <label className="text-sm font-medium text-gray-700 mb-2 block">
          Max Price: <span className="text-teal-600 font-bold">£{filters.maxPrice || 200}</span>
        </label>
        <Slider
          value={[filters.maxPrice || 200]}
          onValueChange={([v]) => updateFilter("maxPrice", v)}
          max={200}
          min={20}
          step={5}
          className="py-2"
        />
        <div className="flex justify-between text-xs text-gray-400 mt-1">
          <span>£20</span>
          <span>£200</span>
        </div>
      </div>

      <div>
        <label className="text-sm font-medium text-gray-700 mb-2 block">Sort By</label>
        <Select value={filters.sortBy || "rating"} onValueChange={v => updateFilter("sortBy", v)}>
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

      {activeCount > 0 && (
        <Button variant="ghost" onClick={clearFilters} className="w-full text-gray-500 hover:text-gray-700">
          <X className="w-4 h-4 mr-2" />
          Clear all filters
        </Button>
      )}
    </div>
  );

  return (
    <>
      {/* Desktop Search Bar */}
      <div className="flex items-center gap-4 mb-6">
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
            <Button variant="outline" className="lg:hidden h-14 rounded-xl border-gray-200 relative">
              <SlidersHorizontal className="w-5 h-5" />
              {activeCount > 0 && (
                <span className="absolute -top-1 -right-1 w-5 h-5 bg-teal-600 text-white text-xs rounded-full flex items-center justify-center">
                  {activeCount}
                </span>
              )}
            </Button>
          </SheetTrigger>
          <SheetContent className="w-80">
            <SheetHeader>
              <SheetTitle>Filters</SheetTitle>
            </SheetHeader>
            <div className="mt-6">
              <FilterContent />
            </div>
          </SheetContent>
        </Sheet>
      </div>

      {/* Desktop Sidebar Filters */}
      <div className="hidden lg:block bg-white rounded-2xl border border-gray-100 p-6">
        <div className="flex items-center justify-between mb-6">
          <h3 className="font-bold text-gray-900">Filters</h3>
          {activeCount > 0 && (
            <Badge className="bg-teal-50 text-teal-700 border-0">{activeCount} active</Badge>
          )}
        </div>
        <FilterContent />
      </div>
    </>
  );
}