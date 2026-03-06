import React, { useState } from "react";
import { api as base44 } from "@/api/apiClient";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { createPageUrl } from "../utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  MapPin, Clock, Star, GraduationCap, Briefcase, Home,
  Building2, Phone, Mail, CalendarCheck, Languages, ArrowLeft, Loader2
} from "lucide-react";
import StarRating from "@/components/shared/StarRating";
import ReviewCard from "@/components/shared/ReviewCard";
import { motion } from "framer-motion";

export default function PhysioProfile() {
  const urlParams = new URLSearchParams(window.location.search);
  const id = urlParams.get("id");

  const { data: physios, isLoading: physioLoading } = useQuery({
    queryKey: ["physio", id],
    queryFn: () => base44.entities.Physiotherapist.filter({ id }),
    enabled: !!id,
  });

  const physio = physios?.[0];

  const { data: reviews = [] } = useQuery({
    queryKey: ["reviews", id],
    queryFn: () => base44.entities.Review.filter({ physio_id: id }, "-created_date"),
    enabled: !!id,
    initialData: [],
  });

  if (physioLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <Loader2 className="w-8 h-8 text-teal-600 animate-spin" />
      </div>
    );
  }

  if (!physio) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-gray-900 mb-2">Profile not found</h2>
          <Link to={createPageUrl("Search")}>
            <Button variant="outline" className="mt-4">Back to Search</Button>
          </Link>
        </div>
      </div>
    );
  }

  const visitTypeLabel = physio.visit_type === "home_visit" ? "Home Visit" :
                          physio.visit_type === "both" ? "Clinic & Home Visit" : "Clinic Visit";

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Hero */}
      <div className="bg-white border-b border-gray-100">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 py-6">
          <Link to={createPageUrl("Search")} className="inline-flex items-center gap-2 text-sm text-gray-400 hover:text-gray-600 mb-6 transition-colors">
            <ArrowLeft className="w-4 h-4" />
            Back to search
          </Link>

          <div className="flex flex-col sm:flex-row gap-6 sm:gap-8">
            <motion.div
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              className="w-32 h-32 sm:w-40 sm:h-40 rounded-2xl overflow-hidden bg-gradient-to-br from-teal-50 to-emerald-50 flex-shrink-0"
            >
              {physio.photo_url ? (
                <img src={physio.photo_url} alt={physio.full_name} className="w-full h-full object-cover" />
              ) : (
                <div className="w-full h-full flex items-center justify-center">
                  <span className="text-4xl font-bold text-teal-600">
                    {physio.full_name?.charAt(0)?.toUpperCase()}
                  </span>
                </div>
              )}
            </motion.div>

            <div className="flex-1">
              <h1 className="text-2xl sm:text-3xl font-bold text-gray-900">{physio.full_name}</h1>

              <div className="flex flex-wrap items-center gap-3 mt-3">
                {physio.rating > 0 && (
                  <div className="flex items-center gap-1.5">
                    <StarRating rating={physio.rating} size="sm" />
                    <span className="text-sm font-semibold text-gray-700">{physio.rating?.toFixed(1)}</span>
                    <span className="text-sm text-gray-400">({physio.review_count || reviews.length} reviews)</span>
                  </div>
                )}
                {physio.city && (
                  <div className="flex items-center gap-1 text-sm text-gray-400">
                    <MapPin className="w-4 h-4" />
                    {physio.city}
                  </div>
                )}
              </div>

              <div className="flex flex-wrap gap-2 mt-4">
                {(physio.specializations || []).map(s => (
                  <Badge key={s} className="bg-teal-50 text-teal-700 border-0 rounded-lg">{s}</Badge>
                ))}
              </div>

              <div className="flex flex-wrap items-center gap-6 mt-5">
                <div className="flex items-center gap-2 text-sm text-gray-500">
                  <Briefcase className="w-4 h-4 text-gray-400" />
                  {physio.experience_years || 0} years experience
                </div>
                <div className="flex items-center gap-2 text-sm text-gray-500">
                  {physio.visit_type === "home_visit" || physio.visit_type === "both"
                    ? <Home className="w-4 h-4 text-gray-400" />
                    : <Building2 className="w-4 h-4 text-gray-400" />
                  }
                  {visitTypeLabel}
                </div>
              </div>

              <div className="flex items-center gap-4 mt-6">
                <div className="text-2xl font-bold text-teal-600">£{physio.consultation_fee}<span className="text-sm font-normal text-gray-400">/session</span></div>
                <Link to={createPageUrl("BookAppointment") + `?physio_id=${physio.id}`}>
                  <Button size="lg" className="bg-teal-600 hover:bg-teal-700 rounded-xl px-8 shadow-lg shadow-teal-600/20">
                    <CalendarCheck className="w-5 h-5 mr-2" />
                    Book Appointment
                  </Button>
                </Link>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-8">
        <Tabs defaultValue="about" className="space-y-6">
          <TabsList className="bg-white border border-gray-100 rounded-xl p-1">
            <TabsTrigger value="about" className="rounded-lg">About</TabsTrigger>
            <TabsTrigger value="reviews" className="rounded-lg">Reviews ({reviews.length})</TabsTrigger>
            <TabsTrigger value="schedule" className="rounded-lg">Schedule</TabsTrigger>
          </TabsList>

          <TabsContent value="about" className="space-y-6">
            {physio.bio && (
              <div className="bg-white rounded-2xl border border-gray-100 p-6">
                <h3 className="font-bold text-gray-900 mb-3">About</h3>
                <p className="text-gray-600 leading-relaxed">{physio.bio}</p>
              </div>
            )}

            <div className="grid sm:grid-cols-2 gap-6">
              {(physio.qualifications || []).length > 0 && (
                <div className="bg-white rounded-2xl border border-gray-100 p-6">
                  <h3 className="font-bold text-gray-900 mb-4 flex items-center gap-2">
                    <GraduationCap className="w-5 h-5 text-teal-600" />
                    Qualifications
                  </h3>
                  <ul className="space-y-2">
                    {physio.qualifications.map((q, i) => (
                      <li key={i} className="flex items-start gap-2 text-sm text-gray-600">
                        <span className="w-1.5 h-1.5 rounded-full bg-teal-400 mt-2 flex-shrink-0" />
                        {q}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <div className="bg-white rounded-2xl border border-gray-100 p-6 space-y-4">
                <h3 className="font-bold text-gray-900 flex items-center gap-2">
                  <Clock className="w-5 h-5 text-teal-600" />
                  Practice Details
                </h3>
                {physio.clinic_name && (
                  <div className="text-sm">
                    <span className="text-gray-400">Clinic:</span>
                    <span className="ml-2 text-gray-700 font-medium">{physio.clinic_name}</span>
                  </div>
                )}
                {physio.clinic_address && (
                  <div className="text-sm">
                    <span className="text-gray-400">Address:</span>
                    <span className="ml-2 text-gray-700">{physio.clinic_address}</span>
                  </div>
                )}
                {physio.session_duration && (
                  <div className="text-sm">
                    <span className="text-gray-400">Session:</span>
                    <span className="ml-2 text-gray-700">{physio.session_duration} minutes</span>
                  </div>
                )}
                {(physio.languages || []).length > 0 && (
                  <div className="text-sm flex items-center gap-2">
                    <Languages className="w-4 h-4 text-gray-400" />
                    {physio.languages.join(", ")}
                  </div>
                )}
                {physio.phone && (
                  <div className="text-sm flex items-center gap-2">
                    <Phone className="w-4 h-4 text-gray-400" />
                    {physio.phone}
                  </div>
                )}
                {physio.email && (
                  <div className="text-sm flex items-center gap-2">
                    <Mail className="w-4 h-4 text-gray-400" />
                    {physio.email}
                  </div>
                )}
              </div>
            </div>
          </TabsContent>

          <TabsContent value="reviews">
            {reviews.length === 0 ? (
              <div className="text-center py-12 bg-white rounded-2xl border border-gray-100">
                <Star className="w-10 h-10 text-gray-200 mx-auto mb-3" />
                <p className="text-gray-400">No reviews yet</p>
              </div>
            ) : (
              <div className="space-y-4">
                {reviews.map(r => (
                  <ReviewCard key={r.id} review={r} />
                ))}
              </div>
            )}
          </TabsContent>

          <TabsContent value="schedule">
            <div className="bg-white rounded-2xl border border-gray-100 p-6">
              <h3 className="font-bold text-gray-900 mb-4">Available Schedule</h3>
              {(physio.available_days || []).length > 0 ? (
                <div className="space-y-3">
                  <div className="flex flex-wrap gap-2">
                    {physio.available_days.map(d => (
                      <Badge key={d} variant="secondary" className="bg-teal-50 text-teal-700 border-0 rounded-lg px-4 py-2">{d}</Badge>
                    ))}
                  </div>
                  <p className="text-sm text-gray-500 mt-3">
                    Working hours: {physio.working_hours_start || "09:00"} – {physio.working_hours_end || "17:00"}
                  </p>
                  <Link to={createPageUrl("BookAppointment") + `?physio_id=${physio.id}`}>
                    <Button className="mt-4 bg-teal-600 hover:bg-teal-700 rounded-xl">
                      <CalendarCheck className="w-4 h-4 mr-2" />
                      Book Now
                    </Button>
                  </Link>
                </div>
              ) : (
                <p className="text-gray-400">Schedule information not yet available</p>
              )}
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}