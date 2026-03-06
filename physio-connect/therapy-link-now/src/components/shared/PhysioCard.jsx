import React from "react";
import { Link } from "react-router-dom";
import { createPageUrl } from "../../utils";
import { Badge } from "@/components/ui/badge";
import { Star, MapPin, Clock, Home, Building2 } from "lucide-react";

export default function PhysioCard({ physio }) {
  const visitTypeIcon = physio.visit_type === "home_visit" ? Home : 
                         physio.visit_type === "both" ? Home : Building2;
  const visitTypeLabel = physio.visit_type === "home_visit" ? "Home Visit" : 
                          physio.visit_type === "both" ? "Clinic & Home" : "Clinic";

  return (
    <Link
      to={createPageUrl("PhysioProfile") + `?id=${physio.id}`}
      className="group block bg-white rounded-2xl border border-gray-100 overflow-hidden transition-all duration-300 hover:shadow-xl hover:shadow-gray-100/80 hover:-translate-y-1 hover:border-gray-200"
    >
      <div className="relative h-52 overflow-hidden bg-gradient-to-br from-teal-50 to-emerald-50">
        {physio.photo_url ? (
          <img
            src={physio.photo_url}
            alt={physio.full_name}
            className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center">
            <div className="w-20 h-20 rounded-full bg-teal-100 flex items-center justify-center">
              <span className="text-2xl font-bold text-teal-600">
                {physio.full_name?.charAt(0)?.toUpperCase()}
              </span>
            </div>
          </div>
        )}
        {physio.rating > 0 && (
          <div className="absolute top-3 right-3 bg-white/90 backdrop-blur-sm rounded-lg px-2.5 py-1 flex items-center gap-1 shadow-sm">
            <Star className="w-3.5 h-3.5 text-amber-400 fill-amber-400" />
            <span className="text-sm font-semibold text-gray-900">{physio.rating?.toFixed(1)}</span>
          </div>
        )}
      </div>

      <div className="p-5">
        <h3 className="font-bold text-gray-900 text-lg group-hover:text-teal-600 transition-colors">
          {physio.full_name}
        </h3>

        {physio.city && (
          <div className="flex items-center gap-1.5 mt-1.5 text-gray-400 text-sm">
            <MapPin className="w-3.5 h-3.5" />
            {physio.city}
          </div>
        )}

        <div className="flex flex-wrap gap-1.5 mt-3">
          {(physio.specializations || []).slice(0, 2).map(spec => (
            <Badge key={spec} variant="secondary" className="bg-teal-50 text-teal-700 border-0 text-xs font-medium rounded-lg">
              {spec}
            </Badge>
          ))}
          {(physio.specializations || []).length > 2 && (
            <Badge variant="secondary" className="bg-gray-50 text-gray-500 border-0 text-xs rounded-lg">
              +{physio.specializations.length - 2}
            </Badge>
          )}
        </div>

        <div className="flex items-center justify-between mt-4 pt-4 border-t border-gray-50">
          <div className="flex items-center gap-1.5 text-sm text-gray-400">
            {React.createElement(visitTypeIcon, { className: "w-3.5 h-3.5" })}
            {visitTypeLabel}
          </div>
          <span className="font-bold text-teal-600 text-lg">
            £{physio.consultation_fee}
          </span>
        </div>
      </div>
    </Link>
  );
}