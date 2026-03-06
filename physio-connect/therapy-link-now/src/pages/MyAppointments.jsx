import React, { useState } from "react";
import { base44 } from "@/api/base44Client";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  CalendarCheck, Clock, MapPin, User, XCircle, Loader2, Calendar
} from "lucide-react";
import { format } from "date-fns";
import { cn } from "@/lib/utils";

const statusColors = {
  confirmed: "bg-emerald-50 text-emerald-700 border-emerald-200",
  completed: "bg-blue-50 text-blue-700 border-blue-200",
  cancelled: "bg-red-50 text-red-700 border-red-200",
  no_show: "bg-gray-50 text-gray-700 border-gray-200",
};

export default function MyAppointments() {
  const queryClient = useQueryClient();

  const { data: appointments = [], isLoading } = useQuery({
    queryKey: ["my-appointments"],
    queryFn: () => base44.entities.Appointment.list("-appointment_date"),
    initialData: [],
  });

  const cancelMutation = useMutation({
    mutationFn: (id) => base44.entities.Appointment.update(id, { status: "cancelled" }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["my-appointments"] }),
  });

  const upcoming = appointments.filter(a => a.status === "confirmed" && a.appointment_date >= format(new Date(), "yyyy-MM-dd"));
  const past = appointments.filter(a => a.status !== "confirmed" || a.appointment_date < format(new Date(), "yyyy-MM-dd"));

  const AppointmentCard = ({ apt }) => (
    <div className="bg-white rounded-2xl border border-gray-100 p-5 sm:p-6 hover:shadow-sm transition-shadow">
      <div className="flex items-start justify-between">
        <div className="flex items-start gap-4">
          <div className="w-12 h-12 rounded-xl bg-teal-50 flex items-center justify-center flex-shrink-0">
            <CalendarCheck className="w-6 h-6 text-teal-600" />
          </div>
          <div>
            <h3 className="font-bold text-gray-900">{apt.physio_name}</h3>
            <div className="flex flex-wrap items-center gap-3 mt-2 text-sm text-gray-400">
              <span className="flex items-center gap-1">
                <Calendar className="w-3.5 h-3.5" />
                {apt.appointment_date ? format(new Date(apt.appointment_date + "T00:00:00"), "MMM d, yyyy") : ""}
              </span>
              <span className="flex items-center gap-1">
                <Clock className="w-3.5 h-3.5" />
                {apt.time_slot}
              </span>
            </div>
            <div className="flex items-center gap-2 mt-2 text-sm text-gray-400">
              <MapPin className="w-3.5 h-3.5" />
              {apt.visit_type === "home_visit" ? "Home Visit" : "Clinic Visit"}
            </div>
          </div>
        </div>
        <div className="flex flex-col items-end gap-2">
          <Badge variant="outline" className={cn("border rounded-lg", statusColors[apt.status] || statusColors.confirmed)}>
            {apt.status?.replace("_", " ")}
          </Badge>
          <span className="font-bold text-teal-600">£{apt.consultation_fee}</span>
        </div>
      </div>

      {apt.status === "confirmed" && apt.appointment_date >= format(new Date(), "yyyy-MM-dd") && (
        <div className="mt-4 pt-4 border-t border-gray-50 flex justify-end">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => cancelMutation.mutate(apt.id)}
            disabled={cancelMutation.isPending}
            className="text-red-500 hover:text-red-700 hover:bg-red-50"
          >
            <XCircle className="w-4 h-4 mr-1" />
            Cancel
          </Button>
        </div>
      )}
    </div>
  );

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b border-gray-100">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 py-8">
          <h1 className="text-3xl font-bold text-gray-900">My Appointments</h1>
          <p className="mt-2 text-gray-400">{appointments.length} total appointment{appointments.length !== 1 ? "s" : ""}</p>
        </div>
      </div>

      <div className="max-w-4xl mx-auto px-4 sm:px-6 py-8">
        {isLoading ? (
          <div className="flex items-center justify-center py-20">
            <Loader2 className="w-8 h-8 text-teal-600 animate-spin" />
          </div>
        ) : appointments.length === 0 ? (
          <div className="text-center py-20 bg-white rounded-2xl border border-gray-100">
            <Calendar className="w-12 h-12 text-gray-200 mx-auto mb-4" />
            <h3 className="text-xl font-semibold text-gray-900 mb-2">No appointments yet</h3>
            <p className="text-gray-400 mb-6">Find a physiotherapist and book your first session</p>
            <a href="/Search">
              <Button className="bg-teal-600 hover:bg-teal-700 rounded-xl">Find a Physiotherapist</Button>
            </a>
          </div>
        ) : (
          <Tabs defaultValue="upcoming">
            <TabsList className="bg-white border border-gray-100 rounded-xl p-1 mb-6">
              <TabsTrigger value="upcoming" className="rounded-lg">Upcoming ({upcoming.length})</TabsTrigger>
              <TabsTrigger value="past" className="rounded-lg">Past ({past.length})</TabsTrigger>
            </TabsList>

            <TabsContent value="upcoming" className="space-y-4">
              {upcoming.length === 0 ? (
                <div className="text-center py-12 bg-white rounded-2xl border border-gray-100">
                  <p className="text-gray-400">No upcoming appointments</p>
                </div>
              ) : upcoming.map(a => <AppointmentCard key={a.id} apt={a} />)}
            </TabsContent>

            <TabsContent value="past" className="space-y-4">
              {past.length === 0 ? (
                <div className="text-center py-12 bg-white rounded-2xl border border-gray-100">
                  <p className="text-gray-400">No past appointments</p>
                </div>
              ) : past.map(a => <AppointmentCard key={a.id} apt={a} />)}
            </TabsContent>
          </Tabs>
        )}
      </div>
    </div>
  );
}