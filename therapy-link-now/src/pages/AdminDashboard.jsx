import React, { useState } from "react";
import { api as base44 } from "@/api/apiClient";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Users, CalendarCheck, DollarSign, TrendingUp, Check, X, Eye,
  Loader2, Search, Ban, ShieldCheck, Clock, Trash2
} from "lucide-react";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import { Link } from "react-router-dom";
import { createPageUrl } from "../utils";

export default function AdminDashboard() {
  const queryClient = useQueryClient();
  const [physioFilter, setPhysioFilter] = useState("pending");
  const [aptFilter, setAptFilter] = useState("confirmed");

  const { data: physios = [], isLoading: physiosLoading } = useQuery({
    queryKey: ["admin-physios"],
    queryFn: () => base44.entities.Physiotherapist.list(),
    staleTime: 0,
    refetchOnMount: "always",
  });

  const { data: appointments = [], isLoading: aptsLoading } = useQuery({
    queryKey: ["admin-appointments"],
    queryFn: () => base44.entities.Appointment.list("-appointment_date"),
    staleTime: 0,
    refetchOnMount: "always",
  });

  const [actionId, setActionId] = useState(null);

  const updatePhysio = useMutation({
    mutationFn: ({ id, data }) => { setActionId(id); return base44.entities.Physiotherapist.update(id, data); },
    onSettled: () => { setActionId(null); queryClient.invalidateQueries({ queryKey: ["admin-physios"] }); },
  });

  const deletePhysio = useMutation({
    mutationFn: (id) => base44.entities.Physiotherapist.delete(id),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["admin-physios"] }),
  });

  const updateAppointment = useMutation({
    mutationFn: ({ id, data }) => base44.entities.Appointment.update(id, data),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["admin-appointments"] }),
  });

  const stats = [
    { title: "Total Physios", value: physios.filter(p => p.status === "approved").length, icon: Users, color: "text-teal-600", bg: "bg-teal-50" },
    { title: "Pending Approval", value: physios.filter(p => p.status === "pending").length, icon: Clock, color: "text-amber-600", bg: "bg-amber-50" },
    { title: "Total Bookings", value: appointments.length, icon: CalendarCheck, color: "text-blue-600", bg: "bg-blue-50" },
    { title: "Revenue", value: `£${appointments.filter(a => a.status === "confirmed" || a.status === "completed").reduce((s, a) => s + (a.consultation_fee || 0), 0).toLocaleString()}`, icon: DollarSign, color: "text-emerald-600", bg: "bg-emerald-50" },
  ];

  const filteredPhysios = physios.filter(p => physioFilter === "all" ? true : p.status === physioFilter);
  const filteredApts = appointments.filter(a => aptFilter === "all" ? true : a.status === aptFilter);

  const statusColors = {
    pending: "bg-amber-50 text-amber-700",
    approved: "bg-emerald-50 text-emerald-700",
    rejected: "bg-red-50 text-red-700",
    suspended: "bg-gray-100 text-gray-700",
  };

  const isInitialLoading = physiosLoading && aptsLoading;

  if (isInitialLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex flex-col items-center justify-center gap-3">
        <Loader2 className="w-8 h-8 text-teal-600 animate-spin" />
        <p className="text-gray-500 text-sm">Loading dashboard…</p>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
          <h1 className="text-3xl font-bold text-gray-900">Admin Dashboard</h1>
          <p className="mt-2 text-gray-400">Manage physiotherapists, bookings, and analytics</p>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
        {/* Stats */}
        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          {stats.map(s => (
            <Card key={s.title} className="border-gray-100">
              <CardContent className="p-6">
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

        <Tabs defaultValue="physios">
          <TabsList className="bg-white border border-gray-100 rounded-xl p-1 mb-6">
            <TabsTrigger value="physios" className="rounded-lg">Physiotherapists</TabsTrigger>
            <TabsTrigger value="bookings" className="rounded-lg">Bookings</TabsTrigger>
          </TabsList>

          <TabsContent value="physios">
            <div className="flex items-center gap-3 mb-6">
              <Select value={physioFilter} onValueChange={setPhysioFilter}>
                <SelectTrigger className="w-44 rounded-xl"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Status</SelectItem>
                  <SelectItem value="pending">Pending</SelectItem>
                  <SelectItem value="approved">Approved</SelectItem>
                  <SelectItem value="rejected">Rejected</SelectItem>
                  <SelectItem value="suspended">Suspended</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {physiosLoading ? (
              <div className="flex justify-center py-12"><Loader2 className="w-6 h-6 text-teal-600 animate-spin" /></div>
            ) : (
              <div className="space-y-3">
                {filteredPhysios.map(p => (
                  <div key={p.id} className="bg-white rounded-2xl border border-gray-100 p-5 flex flex-col sm:flex-row sm:items-center gap-4">
                    <div className="flex items-center gap-4 flex-1">
                      <div className="w-12 h-12 rounded-xl overflow-hidden bg-teal-50 flex-shrink-0">
                        {p.photo_url ? (
                          <img src={p.photo_url} alt="" className="w-full h-full object-cover" />
                        ) : (
                          <div className="w-full h-full flex items-center justify-center text-teal-600 font-bold">{p.full_name?.charAt(0)}</div>
                        )}
                      </div>
                      <div className="flex-1 min-w-0">
                        <h3 className="font-semibold text-gray-900 truncate">{p.full_name}</h3>
                        <p className="text-sm text-gray-400 truncate">{(p.specializations || []).join(", ")}</p>
                        <p className="text-sm text-gray-400">{p.city} · £{p.consultation_fee}/session</p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge className={cn("border-0 rounded-lg", statusColors[p.status])}>{p.status}</Badge>
                      <Link to={createPageUrl("PhysioProfile") + `?id=${p.id}`}>
                        <Button variant="ghost" size="icon"><Eye className="w-4 h-4" /></Button>
                      </Link>
                      {actionId === p.id && updatePhysio.isPending ? (
                        <div className="flex items-center gap-2 text-sm text-gray-500">
                          <Loader2 className="w-4 h-4 animate-spin" /> Updating…
                        </div>
                      ) : (
                        <>
                          {p.status === "pending" && (
                            <>
                              <Button size="sm" disabled={updatePhysio.isPending} onClick={() => updatePhysio.mutate({ id: p.id, data: { status: "approved" } })} className="bg-emerald-600 hover:bg-emerald-700 rounded-lg">
                                <Check className="w-4 h-4 mr-1" /> Approve
                              </Button>
                              <Button size="sm" variant="outline" disabled={updatePhysio.isPending} onClick={() => updatePhysio.mutate({ id: p.id, data: { status: "rejected" } })} className="rounded-lg text-red-600 border-red-200 hover:bg-red-50">
                                <X className="w-4 h-4 mr-1" /> Reject
                              </Button>
                            </>
                          )}
                          {p.status === "approved" && (
                            <Button size="sm" variant="outline" disabled={updatePhysio.isPending} onClick={() => updatePhysio.mutate({ id: p.id, data: { status: "suspended" } })} className="rounded-lg text-gray-600">
                              <Ban className="w-4 h-4 mr-1" /> Suspend
                            </Button>
                          )}
                          {(p.status === "rejected" || p.status === "suspended") && (
                            <Button size="sm" disabled={updatePhysio.isPending} onClick={() => updatePhysio.mutate({ id: p.id, data: { status: "approved" } })} className="bg-teal-600 hover:bg-teal-700 rounded-lg">
                              <ShieldCheck className="w-4 h-4 mr-1" /> Approve
                            </Button>
                          )}
                        </>
                      )}
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => {
                          if (window.confirm(`Delete ${p.full_name}? This will also remove their appointments, reviews, and account. This cannot be undone.`)) {
                            deletePhysio.mutate(p.id);
                          }
                        }}
                        disabled={deletePhysio.isPending}
                        className="rounded-lg text-red-600 border-red-200 hover:bg-red-50"
                      >
                        {deletePhysio.isPending && deletePhysio.variables === p.id ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
                      </Button>
                    </div>
                  </div>
                ))}
                {filteredPhysios.length === 0 && (
                  <div className="text-center py-12 text-gray-400">No physiotherapists found</div>
                )}
              </div>
            )}
          </TabsContent>

          <TabsContent value="bookings">
            <div className="flex items-center gap-3 mb-6">
              <Select value={aptFilter} onValueChange={setAptFilter}>
                <SelectTrigger className="w-44 rounded-xl"><SelectValue /></SelectTrigger>
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
            ) : (
              <div className="space-y-3">
                {filteredApts.map(a => (
                  <div key={a.id} className="bg-white rounded-2xl border border-gray-100 p-5 flex flex-col sm:flex-row sm:items-center gap-4">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <h3 className="font-semibold text-gray-900">{a.patient_name}</h3>
                        <span className="text-gray-300">→</span>
                        <span className="text-gray-600">{a.physio_name}</span>
                      </div>
                      <p className="text-sm text-gray-400 mt-1">
                        {a.appointment_date ? format(new Date(a.appointment_date + "T00:00:00"), "MMM d, yyyy") : ""} · {a.time_slot} · {a.visit_type === "home_visit" ? "Home" : "Clinic"}
                      </p>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="font-bold text-teal-600">£{a.consultation_fee}</span>
                      <Select value={a.status} onValueChange={v => updateAppointment.mutate({ id: a.id, data: { status: v } })}>
                        <SelectTrigger className="w-36 rounded-lg"><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="confirmed">Confirmed</SelectItem>
                          <SelectItem value="completed">Completed</SelectItem>
                          <SelectItem value="cancelled">Cancelled</SelectItem>
                          <SelectItem value="no_show">No Show</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                ))}
                {filteredApts.length === 0 && (
                  <div className="text-center py-12 text-gray-400">No bookings found</div>
                )}
              </div>
            )}
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}