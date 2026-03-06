import React, { useState } from "react";
import { api as base44 } from "@/api/apiClient";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/lib/AuthContext";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  CalendarCheck, DollarSign, Users, Clock, CheckCircle2,
  XCircle, Loader2, AlertCircle, Star, MapPin, Briefcase
} from "lucide-react";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import WeeklySchedule from "@/components/booking/WeeklySchedule";

export default function PhysioDashboard() {
  const { user, isAuthenticated } = useAuth();
  const queryClient = useQueryClient();
  const [aptFilter, setAptFilter] = useState("all");

  // Fetch physio profile linked to this user
  const { data: physioList = [], isLoading: profileLoading } = useQuery({
    queryKey: ["my-physio-profile", user?.id],
    queryFn: () => base44.entities.Physiotherapist.filter({ user_id: user?.id }),
    enabled: !!user?.id,
  });

  const physio = physioList[0] || null;

  // Fetch appointments for this physio
  const { data: appointments = [], isLoading: aptsLoading } = useQuery({
    queryKey: ["physio-appointments", physio?.id],
    queryFn: () => base44.entities.Appointment.filter({ physio_id: physio?.id }),
    enabled: !!physio?.id,
    staleTime: 0,
    refetchOnMount: "always",
  });

  const updateAppointment = useMutation({
    mutationFn: ({ id, data }) => base44.entities.Appointment.update(id, data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["physio-appointments"] }),
  });

  if (!isAuthenticated || !user) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
        <div className="bg-white rounded-2xl border border-gray-100 p-8 text-center max-w-md">
          <AlertCircle className="w-12 h-12 text-amber-500 mx-auto mb-4" />
          <h2 className="text-xl font-bold text-gray-900 mb-2">Login Required</h2>
          <p className="text-gray-400 mb-6">Please sign in with your physio account to access your dashboard.</p>
          <a href="/Login">
            <Button className="bg-teal-600 hover:bg-teal-700 rounded-xl px-8 h-12">Sign In</Button>
          </a>
        </div>
      </div>
    );
  }

  if (profileLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <Loader2 className="w-8 h-8 text-teal-600 animate-spin" />
      </div>
    );
  }

  if (!physio) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
        <div className="bg-white rounded-2xl border border-gray-100 p-8 text-center max-w-md">
          <AlertCircle className="w-12 h-12 text-gray-300 mx-auto mb-4" />
          <h2 className="text-xl font-bold text-gray-900 mb-2">No Physio Profile Found</h2>
          <p className="text-gray-400 mb-6">Your account isn't linked to a physiotherapist profile.</p>
          <a href="/">
            <Button variant="outline" className="rounded-xl px-8 h-12">Back to Home</Button>
          </a>
        </div>
      </div>
    );
  }

  // Stats
  const confirmed = appointments.filter(a => a.status === "confirmed");
  const completed = appointments.filter(a => a.status === "completed");
  const cancelled = appointments.filter(a => a.status === "cancelled");
  const noShow = appointments.filter(a => a.status === "no_show");
  const totalRevenue = [...confirmed, ...completed].reduce((s, a) => s + (a.consultation_fee || 0), 0);

  const today = new Date().toISOString().split("T")[0];
  const upcoming = appointments.filter(a => a.appointment_date >= today && (a.status === "confirmed"));

  const stats = [
    { title: "Total Bookings", value: appointments.length, icon: CalendarCheck, color: "text-blue-600", bg: "bg-blue-50" },
    { title: "Completed", value: completed.length, icon: CheckCircle2, color: "text-emerald-600", bg: "bg-emerald-50" },
    { title: "Upcoming", value: upcoming.length, icon: Clock, color: "text-amber-600", bg: "bg-amber-50" },
    { title: "Revenue", value: `£${totalRevenue.toLocaleString()}`, icon: DollarSign, color: "text-teal-600", bg: "bg-teal-50" },
  ];

  const filteredApts = aptFilter === "all" ? appointments : appointments.filter(a => a.status === aptFilter);

  const statusColors = {
    confirmed: "bg-blue-50 text-blue-700",
    completed: "bg-emerald-50 text-emerald-700",
    cancelled: "bg-red-50 text-red-700",
    no_show: "bg-gray-100 text-gray-700",
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b border-gray-100">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 py-8">
          <div className="flex flex-col sm:flex-row sm:items-center gap-4">
            <div className="w-16 h-16 rounded-2xl overflow-hidden bg-teal-50 flex-shrink-0">
              {physio.photo_url ? (
                <img src={physio.photo_url} alt="" className="w-full h-full object-cover" />
              ) : (
                <div className="w-full h-full flex items-center justify-center text-teal-600 font-bold text-xl">
                  {physio.full_name?.charAt(0)}
                </div>
              )}
            </div>
            <div className="flex-1">
              <div className="flex items-center gap-3">
                <h1 className="text-2xl font-bold text-gray-900">{physio.full_name}</h1>
                <Badge className={cn("border-0 rounded-lg",
                  physio.status === "approved" ? "bg-emerald-50 text-emerald-700" :
                  physio.status === "pending" ? "bg-amber-50 text-amber-700" :
                  "bg-red-50 text-red-700"
                )}>
                  {physio.status}
                </Badge>
              </div>
              <div className="flex flex-wrap items-center gap-4 mt-1 text-sm text-gray-400">
                {physio.city && (
                  <span className="flex items-center gap-1"><MapPin className="w-3.5 h-3.5" /> {physio.city}</span>
                )}
                <span className="flex items-center gap-1"><Briefcase className="w-3.5 h-3.5" /> {physio.experience_years} yrs exp</span>
                <span className="flex items-center gap-1"><Star className="w-3.5 h-3.5" /> {physio.rating} ({physio.review_count} reviews)</span>
                <span className="flex items-center gap-1"><DollarSign className="w-3.5 h-3.5" /> £{physio.consultation_fee}/session</span>
              </div>
            </div>
          </div>
          {physio.status === "pending" && (
            <div className="mt-4 bg-amber-50 border border-amber-200 rounded-xl p-4 text-sm text-amber-700">
              <AlertCircle className="w-4 h-4 inline mr-2" />
              Your profile is under review. You'll be visible to patients once approved by admin.
            </div>
          )}
        </div>
      </div>

      <div className="max-w-6xl mx-auto px-4 sm:px-6 py-8">
        {/* Stats */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          {stats.map(s => (
            <Card key={s.title} className="border-gray-100">
              <CardContent className="p-5">
                <div className="flex items-start justify-between">
                  <div>
                    <p className="text-sm text-gray-400 font-medium">{s.title}</p>
                    <p className="text-2xl font-bold text-gray-900 mt-1">{s.value}</p>
                  </div>
                  <div className={cn("w-10 h-10 rounded-xl flex items-center justify-center", s.bg)}>
                    <s.icon className={cn("w-5 h-5", s.color)} />
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Weekly Schedule */}
        {physio.status === "approved" && <WeeklySchedule physio={physio} />}

        {/* Appointments */}
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-bold text-gray-900">My Bookings</h2>
          <Select value={aptFilter} onValueChange={setAptFilter}>
            <SelectTrigger className="w-40 rounded-xl"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Status</SelectItem>
              <SelectItem value="confirmed">Confirmed</SelectItem>
              <SelectItem value="completed">Completed</SelectItem>
              <SelectItem value="cancelled">Cancelled</SelectItem>
              <SelectItem value="no_show">No Show</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {aptsLoading ? (
          <div className="flex justify-center py-12"><Loader2 className="w-6 h-6 text-teal-600 animate-spin" /></div>
        ) : filteredApts.length === 0 ? (
          <div className="bg-white rounded-2xl border border-gray-100 p-12 text-center">
            <CalendarCheck className="w-12 h-12 text-gray-200 mx-auto mb-3" />
            <p className="text-gray-400">No bookings found</p>
          </div>
        ) : (
          <div className="space-y-3">
            {filteredApts
              .sort((a, b) => (b.appointment_date || "").localeCompare(a.appointment_date || ""))
              .map(a => (
              <div key={a.id} className="bg-white rounded-2xl border border-gray-100 p-5 flex flex-col sm:flex-row sm:items-center gap-4">
                <div className="flex-1 min-w-0">
                  <h3 className="font-semibold text-gray-900">{a.patient_name}</h3>
                  <p className="text-sm text-gray-400 mt-0.5">
                    {a.appointment_date ? format(new Date(a.appointment_date + "T00:00:00"), "MMM d, yyyy") : ""} · {a.time_slot} · {a.visit_type === "home_visit" ? "Home Visit" : "Clinic"}
                  </p>
                  {a.patient_phone && <p className="text-sm text-gray-400">{a.patient_phone}</p>}
                  {a.notes && <p className="text-sm text-gray-400 mt-1 italic">"{a.notes}"</p>}
                </div>
                <div className="flex items-center gap-3">
                  <span className="font-bold text-teal-600">£{a.consultation_fee}</span>
                  <Badge className={cn("border-0 rounded-lg", statusColors[a.status] || "bg-gray-100 text-gray-600")}>
                    {a.status}
                  </Badge>
                  {a.status === "confirmed" && (
                    <div className="flex gap-1">
                      <Button
                        size="sm"
                        onClick={() => updateAppointment.mutate({ id: a.id, data: { status: "completed" } })}
                        className="bg-emerald-600 hover:bg-emerald-700 rounded-lg text-xs"
                      >
                        <CheckCircle2 className="w-3.5 h-3.5 mr-1" /> Done
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => updateAppointment.mutate({ id: a.id, data: { status: "no_show" } })}
                        className="rounded-lg text-xs text-gray-600"
                      >
                        No Show
                      </Button>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
