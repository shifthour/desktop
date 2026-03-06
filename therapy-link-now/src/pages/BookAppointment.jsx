import React, { useState, useEffect } from "react";
import { api as base44 } from "@/api/apiClient";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { createPageUrl } from "../utils";
import { useAuth } from "@/lib/AuthContext";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  ArrowLeft, ArrowRight, Check, CalendarCheck, User, FileText,
  Loader2, Star, MapPin, Home, Building2, CheckCircle2
} from "lucide-react";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import TimeSlotPicker from "@/components/booking/TimeSlotPicker";

const STEPS = [
  { id: 1, label: "Date & Time", icon: CalendarCheck },
  { id: 2, label: "Your Details", icon: User },
  { id: 3, label: "Confirm", icon: FileText },
];

export default function BookAppointment() {
  const urlParams = new URLSearchParams(window.location.search);
  const physioId = urlParams.get("physio_id");

  const navigate = useNavigate();
  const { user, isAuthenticated, isLoadingAuth } = useAuth();

  const [step, setStep] = useState(1);
  const [selectedDate, setSelectedDate] = useState(null);
  const [selectedTime, setSelectedTime] = useState("");
  const [visitType, setVisitType] = useState("clinic");
  const [formData, setFormData] = useState({
    patient_name: "",
    patient_email: "",
    patient_phone: "",
    notes: "",
    patient_address: "",
  });
  const [booked, setBooked] = useState(false);
  const queryClient = useQueryClient();

  // Pre-fill patient details from logged-in user
  useEffect(() => {
    if (user && !formData.patient_name) {
      setFormData(prev => ({
        ...prev,
        patient_name: user.full_name || prev.patient_name,
        patient_email: user.email || prev.patient_email,
        patient_phone: user.phone || prev.patient_phone,
      }));
    }
  }, [user]);

  const handleContinueToDetails = () => {
    if (!isAuthenticated) {
      // Redirect to login with return URL
      const currentUrl = window.location.pathname + window.location.search;
      navigate(`/Login?returnTo=${encodeURIComponent(currentUrl)}`);
      return;
    }
    setStep(2);
  };

  const { data: physios } = useQuery({
    queryKey: ["physio-book", physioId],
    queryFn: () => base44.entities.Physiotherapist.filter({ id: physioId }),
    enabled: !!physioId,
  });
  const physio = physios?.[0];

  const { data: appointments = [] } = useQuery({
    queryKey: ["appointments", physioId],
    queryFn: () => base44.entities.Appointment.filter({ physio_id: physioId }),
    enabled: !!physioId,
    initialData: [],
  });

  const { data: blockedSlots = [] } = useQuery({
    queryKey: ["blocked", physioId],
    queryFn: () => base44.entities.BlockedSlot.filter({ physio_id: physioId }),
    enabled: !!physioId,
    initialData: [],
  });

  const { data: availabilityOverrides = [] } = useQuery({
    queryKey: ["availability-overrides", physioId],
    queryFn: () => base44.entities.AvailabilityOverride.filter({ physio_id: physioId }),
    enabled: !!physioId,
    initialData: [],
  });

  const bookMutation = useMutation({
    mutationFn: (data) => base44.entities.Appointment.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["appointments"] });
      setBooked(true);
    },
  });

  const handleBook = () => {
    bookMutation.mutate({
      physio_id: physioId,
      physio_name: physio.full_name,
      appointment_date: format(selectedDate, "yyyy-MM-dd"),
      time_slot: selectedTime,
      visit_type: visitType,
      consultation_fee: physio.consultation_fee,
      ...formData,
    });
  };

  if (!physio) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <Loader2 className="w-8 h-8 text-teal-600 animate-spin" />
      </div>
    );
  }

  if (booked) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          className="bg-white rounded-3xl border border-gray-100 shadow-xl p-8 sm:p-12 max-w-lg w-full text-center"
        >
          <div className="w-20 h-20 rounded-full bg-emerald-100 flex items-center justify-center mx-auto mb-6">
            <CheckCircle2 className="w-10 h-10 text-emerald-600" />
          </div>
          <h2 className="text-2xl font-bold text-gray-900 mb-2">Booking Confirmed!</h2>
          <p className="text-gray-400 mb-6">Your appointment has been successfully booked</p>

          <div className="bg-gray-50 rounded-2xl p-6 text-left space-y-3 mb-8">
            <div className="flex justify-between text-sm">
              <span className="text-gray-400">Physiotherapist</span>
              <span className="font-semibold text-gray-900">{physio.full_name}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-gray-400">Date</span>
              <span className="font-semibold text-gray-900">{format(selectedDate, "EEEE, MMM d, yyyy")}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-gray-400">Time</span>
              <span className="font-semibold text-gray-900">{selectedTime}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-gray-400">Visit Type</span>
              <span className="font-semibold text-gray-900">{visitType === "home_visit" ? "Home Visit" : "Clinic"}</span>
            </div>
            <div className="flex justify-between text-sm border-t border-gray-200 pt-3">
              <span className="text-gray-400">Fee</span>
              <span className="font-bold text-teal-600 text-lg">£{physio.consultation_fee}</span>
            </div>
          </div>

          <div className="flex flex-col gap-3">
            <Link to={createPageUrl("Home")}>
              <Button className="w-full bg-teal-600 hover:bg-teal-700 rounded-xl h-12">
                Back to Home
              </Button>
            </Link>
            <Link to={createPageUrl("MyAppointments")}>
              <Button variant="outline" className="w-full rounded-xl h-12">
                View My Appointments
              </Button>
            </Link>
          </div>
        </motion.div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-100">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 py-6">
          <Link to={createPageUrl("PhysioProfile") + `?id=${physioId}`} className="inline-flex items-center gap-2 text-sm text-gray-400 hover:text-gray-600 mb-4 transition-colors">
            <ArrowLeft className="w-4 h-4" />
            Back to profile
          </Link>

          <div className="flex items-center gap-4">
            <div className="w-14 h-14 rounded-xl overflow-hidden bg-teal-50 flex-shrink-0">
              {physio.photo_url ? (
                <img src={physio.photo_url} alt="" className="w-full h-full object-cover" />
              ) : (
                <div className="w-full h-full flex items-center justify-center">
                  <span className="text-lg font-bold text-teal-600">{physio.full_name?.charAt(0)}</span>
                </div>
              )}
            </div>
            <div>
              <h1 className="text-xl font-bold text-gray-900">Book with {physio.full_name}</h1>
              <p className="text-sm text-gray-400">£{physio.consultation_fee} per session · {physio.session_duration || 45} min</p>
            </div>
          </div>
        </div>
      </div>

      {/* Stepper */}
      <div className="max-w-4xl mx-auto px-4 sm:px-6 py-6">
        <div className="flex items-center justify-center gap-2 mb-8">
          {STEPS.map((s, i) => (
            <React.Fragment key={s.id}>
              <div className={cn(
                "flex items-center gap-2 px-4 py-2 rounded-full text-sm font-medium transition-all",
                step >= s.id ? "bg-teal-600 text-white" : "bg-white text-gray-400 border border-gray-200"
              )}>
                {step > s.id ? (
                  <Check className="w-4 h-4" />
                ) : (
                  <s.icon className="w-4 h-4" />
                )}
                <span className="hidden sm:inline">{s.label}</span>
              </div>
              {i < STEPS.length - 1 && (
                <div className={cn("w-8 h-0.5", step > s.id ? "bg-teal-600" : "bg-gray-200")} />
              )}
            </React.Fragment>
          ))}
        </div>

        <AnimatePresence mode="wait">
          {step === 1 && (
            <motion.div key="step1" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }}>
              <div className="grid lg:grid-cols-[1fr_320px] gap-8">
                <div>
                  <TimeSlotPicker
                    physio={physio}
                    appointments={appointments}
                    blockedSlots={blockedSlots}
                    availabilityOverrides={availabilityOverrides}
                    selectedDate={selectedDate}
                    selectedTime={selectedTime}
                    onDateChange={(d) => { setSelectedDate(d); setSelectedTime(""); }}
                    onTimeChange={setSelectedTime}
                  />
                </div>
                <div className="bg-white rounded-2xl border border-gray-100 p-6 h-fit sticky top-8">
                  <h3 className="font-bold text-gray-900 mb-4">Visit Type</h3>
                  <div className="space-y-2">
                    {(physio.visit_type === "clinic" || physio.visit_type === "both") && (
                      <button
                        onClick={() => setVisitType("clinic")}
                        className={cn(
                          "w-full flex items-center gap-3 p-4 rounded-xl border transition-all",
                          visitType === "clinic" ? "border-teal-600 bg-teal-50" : "border-gray-200 hover:border-gray-300"
                        )}
                      >
                        <Building2 className={cn("w-5 h-5", visitType === "clinic" ? "text-teal-600" : "text-gray-400")} />
                        <div className="text-left">
                          <p className="font-medium text-gray-900 text-sm">Clinic Visit</p>
                          <p className="text-xs text-gray-400">{physio.clinic_name || "At the clinic"}</p>
                        </div>
                      </button>
                    )}
                    {(physio.visit_type === "home_visit" || physio.visit_type === "both") && (
                      <button
                        onClick={() => setVisitType("home_visit")}
                        className={cn(
                          "w-full flex items-center gap-3 p-4 rounded-xl border transition-all",
                          visitType === "home_visit" ? "border-teal-600 bg-teal-50" : "border-gray-200 hover:border-gray-300"
                        )}
                      >
                        <Home className={cn("w-5 h-5", visitType === "home_visit" ? "text-teal-600" : "text-gray-400")} />
                        <div className="text-left">
                          <p className="font-medium text-gray-900 text-sm">Home Visit</p>
                          <p className="text-xs text-gray-400">Therapist comes to you</p>
                        </div>
                      </button>
                    )}
                  </div>

                  <Button
                    disabled={!selectedDate || !selectedTime}
                    onClick={handleContinueToDetails}
                    className="w-full mt-6 bg-teal-600 hover:bg-teal-700 rounded-xl h-12"
                  >
                    {isAuthenticated ? "Continue" : "Sign In to Continue"}
                    <ArrowRight className="w-4 h-4 ml-2" />
                  </Button>
                </div>
              </div>
            </motion.div>
          )}

          {step === 2 && (
            <motion.div key="step2" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="max-w-xl mx-auto">
              <div className="bg-white rounded-2xl border border-gray-100 p-6 sm:p-8">
                <h3 className="text-xl font-bold text-gray-900 mb-6">Your Details</h3>
                <div className="space-y-5">
                  <div>
                    <Label className="text-sm text-gray-700">Full Name *</Label>
                    <Input
                      value={formData.patient_name}
                      onChange={e => setFormData({...formData, patient_name: e.target.value})}
                      placeholder="John Doe"
                      className="mt-1.5 rounded-xl h-12"
                    />
                  </div>
                  <div>
                    <Label className="text-sm text-gray-700">Email *</Label>
                    <Input
                      type="email"
                      value={formData.patient_email}
                      onChange={e => setFormData({...formData, patient_email: e.target.value})}
                      placeholder="john@example.com"
                      className="mt-1.5 rounded-xl h-12"
                    />
                  </div>
                  <div>
                    <Label className="text-sm text-gray-700">Phone Number</Label>
                    <Input
                      value={formData.patient_phone}
                      onChange={e => setFormData({...formData, patient_phone: e.target.value})}
                      placeholder="+1 (555) 000-0000"
                      className="mt-1.5 rounded-xl h-12"
                    />
                  </div>
                  {visitType === "home_visit" && (
                    <div>
                      <Label className="text-sm text-gray-700">Home Address *</Label>
                      <Input
                        value={formData.patient_address}
                        onChange={e => setFormData({...formData, patient_address: e.target.value})}
                        placeholder="123 Main St, City"
                        className="mt-1.5 rounded-xl h-12"
                      />
                    </div>
                  )}
                  <div>
                    <Label className="text-sm text-gray-700">Notes (optional)</Label>
                    <Textarea
                      value={formData.notes}
                      onChange={e => setFormData({...formData, notes: e.target.value})}
                      placeholder="Describe your condition or reason for visit..."
                      className="mt-1.5 rounded-xl min-h-[100px]"
                    />
                  </div>
                </div>

                <div className="flex gap-3 mt-8">
                  <Button variant="outline" onClick={() => setStep(1)} className="rounded-xl h-12 flex-1">
                    <ArrowLeft className="w-4 h-4 mr-2" />
                    Back
                  </Button>
                  <Button
                    disabled={!formData.patient_name || !formData.patient_email || (visitType === "home_visit" && !formData.patient_address)}
                    onClick={() => setStep(3)}
                    className="bg-teal-600 hover:bg-teal-700 rounded-xl h-12 flex-1"
                  >
                    Review Booking
                    <ArrowRight className="w-4 h-4 ml-2" />
                  </Button>
                </div>
              </div>
            </motion.div>
          )}

          {step === 3 && (
            <motion.div key="step3" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="max-w-xl mx-auto">
              <div className="bg-white rounded-2xl border border-gray-100 p-6 sm:p-8">
                <h3 className="text-xl font-bold text-gray-900 mb-6">Review & Confirm</h3>

                <div className="space-y-4">
                  <div className="bg-gray-50 rounded-xl p-5 space-y-3">
                    <h4 className="font-semibold text-gray-700 text-sm uppercase tracking-wider">Appointment</h4>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-400">Physiotherapist</span>
                      <span className="font-medium text-gray-900">{physio.full_name}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-400">Date</span>
                      <span className="font-medium text-gray-900">{selectedDate && format(selectedDate, "EEEE, MMM d, yyyy")}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-400">Time</span>
                      <span className="font-medium text-gray-900">{selectedTime}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-400">Type</span>
                      <span className="font-medium text-gray-900">{visitType === "home_visit" ? "Home Visit" : "Clinic"}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-400">Duration</span>
                      <span className="font-medium text-gray-900">{physio.session_duration || 45} minutes</span>
                    </div>
                  </div>

                  <div className="bg-gray-50 rounded-xl p-5 space-y-3">
                    <h4 className="font-semibold text-gray-700 text-sm uppercase tracking-wider">Patient</h4>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-400">Name</span>
                      <span className="font-medium text-gray-900">{formData.patient_name}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-400">Email</span>
                      <span className="font-medium text-gray-900">{formData.patient_email}</span>
                    </div>
                    {formData.patient_phone && (
                      <div className="flex justify-between text-sm">
                        <span className="text-gray-400">Phone</span>
                        <span className="font-medium text-gray-900">{formData.patient_phone}</span>
                      </div>
                    )}
                  </div>

                  <div className="flex justify-between items-center p-5 bg-teal-50 rounded-xl">
                    <span className="font-semibold text-gray-700">Total Fee</span>
                    <span className="text-2xl font-bold text-teal-600">£{physio.consultation_fee}</span>
                  </div>
                </div>

                <div className="flex gap-3 mt-8">
                  <Button variant="outline" onClick={() => setStep(2)} className="rounded-xl h-12 flex-1">
                    <ArrowLeft className="w-4 h-4 mr-2" />
                    Back
                  </Button>
                  <Button
                    onClick={handleBook}
                    disabled={bookMutation.isPending}
                    className="bg-teal-600 hover:bg-teal-700 rounded-xl h-12 flex-1"
                  >
                    {bookMutation.isPending ? (
                      <Loader2 className="w-4 h-4 animate-spin mr-2" />
                    ) : (
                      <Check className="w-4 h-4 mr-2" />
                    )}
                    Confirm Booking
                  </Button>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}