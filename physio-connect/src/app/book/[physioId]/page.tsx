"use client";

import { use, useState, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Physiotherapist } from "@/lib/types";
import { fetchPhysioById } from "@/lib/api-client";
import StepIndicator from "@/components/StepIndicator";
import SlotPicker from "@/components/SlotPicker";
import Link from "next/link";

const steps = [
  { label: "Service" },
  { label: "Schedule" },
  { label: "Details" },
  { label: "Confirm" },
];

const avatarColors = [
  "bg-blue-500", "bg-emerald-500", "bg-violet-500", "bg-amber-500", "bg-rose-500", "bg-teal-500",
];

export default function BookingPage({ params }: { params: Promise<{ physioId: string }> }) {
  const { physioId } = use(params);
  const searchParams = useSearchParams();
  const router = useRouter();
  const [physio, setPhysio] = useState<Physiotherapist | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchPhysioById(physioId)
      .then(setPhysio)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [physioId]);

  const [currentStep, setCurrentStep] = useState(0);
  const [selectedServiceId, setSelectedServiceId] = useState<string | null>(null);
  const [selectedDate, setSelectedDate] = useState<string | null>(searchParams.get("date"));
  const [selectedTime, setSelectedTime] = useState<string | null>(searchParams.get("time"));
  const [formData, setFormData] = useState({
    fullName: "",
    phone: "",
    email: "",
    reason: "",
    agreeTerms: false,
  });
  const [paymentMethod, setPaymentMethod] = useState("card");

  if (loading) {
    return (
      <div className="min-h-screen bg-cream flex items-center justify-center">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-primary border-t-transparent rounded-full animate-spin mx-auto mb-4" />
          <p className="text-gray-500">Loading booking details...</p>
        </div>
      </div>
    );
  }

  if (!physio) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-gray-900 mb-2">Provider Not Found</h1>
          <Link href="/search" className="btn-primary">Find a Physiotherapist</Link>
        </div>
      </div>
    );
  }

  const selectedService = physio.services.find((s) => s.id === selectedServiceId);
  const platformFee = 50;
  const total = selectedService ? selectedService.price + platformFee : 0;
  const colorIndex = parseInt(physio.id) % avatarColors.length;

  function formatDate(dateStr: string) {
    return new Date(dateStr + "T00:00:00").toLocaleDateString("en-IN", {
      weekday: "long",
      day: "numeric",
      month: "short",
      year: "numeric",
    });
  }

  function getEndTime(startTime: string, durationMin: number) {
    const [h, m] = startTime.split(":").map(Number);
    const totalMin = h * 60 + m + durationMin;
    const endH = Math.floor(totalMin / 60);
    const endM = totalMin % 60;
    return `${String(endH).padStart(2, "0")}:${String(endM).padStart(2, "0")}`;
  }

  function handleConfirmBooking() {
    const bookingId = `PHC-${new Date().toISOString().split("T")[0].replace(/-/g, "")}-${Math.floor(1000 + Math.random() * 9000)}`;
    router.push(`/booking/confirmed/${bookingId}?physio=${physio!.name}&service=${selectedService?.name}&date=${selectedDate}&time=${selectedTime}&fee=${total}`);
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-2xl mx-auto px-4 sm:px-6 py-8">
        {/* Progress */}
        <div className="mb-8">
          <StepIndicator steps={steps} currentStep={currentStep} />
        </div>

        {/* Provider Mini Header */}
        <div className="flex items-center gap-3 mb-6 pb-4 border-b border-gray-200">
          <div className={`w-10 h-10 rounded-full ${avatarColors[colorIndex]} flex items-center justify-center`}>
            <span className="text-white font-bold text-sm">
              {physio.name.replace(/^Dr\.\s*/, "").split(" ").map(n => n[0]).join("").slice(0, 2)}
            </span>
          </div>
          <div>
            <p className="font-semibold text-gray-900">Booking with {physio.name}</p>
            <p className="text-sm text-gray-500">{physio.clinicName}, {physio.location.area}</p>
          </div>
        </div>

        {/* Step 1: Select Service */}
        {currentStep === 0 && (
          <div>
            <h2 className="text-xl font-bold text-gray-900 mb-4">Select Consultation Type</h2>
            <div className="space-y-3">
              {physio.services.map((service) => (
                <button
                  key={service.id}
                  onClick={() => setSelectedServiceId(service.id)}
                  className={`w-full text-left p-4 rounded-xl border-2 transition-all ${
                    selectedServiceId === service.id
                      ? "border-primary bg-primary-light"
                      : "border-gray-200 hover:border-gray-300 bg-white"
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className={`w-5 h-5 rounded-full border-2 flex items-center justify-center ${
                        selectedServiceId === service.id ? "border-primary" : "border-gray-300"
                      }`}>
                        {selectedServiceId === service.id && (
                          <div className="w-3 h-3 rounded-full bg-primary" />
                        )}
                      </div>
                      <div>
                        <div className="font-semibold text-gray-900">{service.name}</div>
                        <div className="text-sm text-gray-500 mt-0.5">{service.description}</div>
                      </div>
                    </div>
                    <div className="text-right flex-shrink-0 ml-4">
                      <div className="font-bold text-gray-900">₹{service.price}</div>
                      <div className="text-xs text-gray-500">{service.duration} min</div>
                    </div>
                  </div>
                </button>
              ))}
            </div>

            <div className="mt-8 flex justify-end">
              <button
                onClick={() => selectedServiceId && setCurrentStep(1)}
                disabled={!selectedServiceId}
                className={`py-3 px-8 rounded-lg font-semibold transition-colors ${
                  selectedServiceId
                    ? "bg-primary text-white hover:bg-primary-dark"
                    : "bg-gray-200 text-gray-400 cursor-not-allowed"
                }`}
              >
                Next: Pick a Time &rarr;
              </button>
            </div>
          </div>
        )}

        {/* Step 2: Pick Date & Time */}
        {currentStep === 1 && (
          <div>
            <h2 className="text-xl font-bold text-gray-900 mb-4">Select Date & Time</h2>
            <div className="card">
              <SlotPicker
                availability={physio.availability}
                selectedDate={selectedDate}
                selectedTime={selectedTime}
                onSelectDate={(d) => { setSelectedDate(d); setSelectedTime(null); }}
                onSelectTime={setSelectedTime}
              />
            </div>

            <div className="mt-8 flex justify-between">
              <button onClick={() => setCurrentStep(0)} className="btn-outline">
                &larr; Back
              </button>
              <button
                onClick={() => selectedDate && selectedTime && setCurrentStep(2)}
                disabled={!selectedDate || !selectedTime}
                className={`py-3 px-8 rounded-lg font-semibold transition-colors ${
                  selectedDate && selectedTime
                    ? "bg-primary text-white hover:bg-primary-dark"
                    : "bg-gray-200 text-gray-400 cursor-not-allowed"
                }`}
              >
                Next: Your Details &rarr;
              </button>
            </div>
          </div>
        )}

        {/* Step 3: Patient Details */}
        {currentStep === 2 && (
          <div>
            <h2 className="text-xl font-bold text-gray-900 mb-4">Your Details</h2>
            <div className="card space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Full Name *</label>
                <input
                  type="text"
                  className="input-field"
                  value={formData.fullName}
                  onChange={(e) => setFormData({ ...formData, fullName: e.target.value })}
                  placeholder="Enter your full name"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Phone *</label>
                <input
                  type="tel"
                  className="input-field"
                  value={formData.phone}
                  onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
                  placeholder="+91 98765 43210"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Email *</label>
                <input
                  type="email"
                  className="input-field"
                  value={formData.email}
                  onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                  placeholder="your@email.com"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Reason for Visit (optional)</label>
                <textarea
                  className="input-field"
                  rows={3}
                  value={formData.reason}
                  onChange={(e) => setFormData({ ...formData, reason: e.target.value })}
                  placeholder="Briefly describe your condition or reason for visit..."
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Upload Reports (optional)</label>
                <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center">
                  <svg className="w-8 h-8 mx-auto text-gray-400 mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                  </svg>
                  <p className="text-sm text-gray-500">Drop files here or click to upload</p>
                  <p className="text-xs text-gray-400 mt-1">PDF, JPG (max 10MB)</p>
                </div>
              </div>
              <label className="flex items-start gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={formData.agreeTerms}
                  onChange={(e) => setFormData({ ...formData, agreeTerms: e.target.checked })}
                  className="w-4 h-4 rounded border-gray-300 text-primary focus:ring-primary mt-0.5"
                />
                <span className="text-sm text-gray-600">
                  I agree to the <a href="#" className="text-primary underline">Terms of Service</a> and <a href="#" className="text-primary underline">Privacy Policy</a>
                </span>
              </label>
            </div>

            <div className="mt-8 flex justify-between">
              <button onClick={() => setCurrentStep(1)} className="btn-outline">
                &larr; Back
              </button>
              <button
                onClick={() => {
                  if (formData.fullName && formData.phone && formData.email && formData.agreeTerms) {
                    setCurrentStep(3);
                  }
                }}
                disabled={!formData.fullName || !formData.phone || !formData.email || !formData.agreeTerms}
                className={`py-3 px-8 rounded-lg font-semibold transition-colors ${
                  formData.fullName && formData.phone && formData.email && formData.agreeTerms
                    ? "bg-primary text-white hover:bg-primary-dark"
                    : "bg-gray-200 text-gray-400 cursor-not-allowed"
                }`}
              >
                Review & Confirm &rarr;
              </button>
            </div>
          </div>
        )}

        {/* Step 4: Confirm & Pay */}
        {currentStep === 3 && selectedService && selectedDate && selectedTime && (
          <div>
            <h2 className="text-xl font-bold text-gray-900 mb-4">Review Your Booking</h2>

            <div className="card mb-4">
              <h3 className="font-semibold text-gray-900 mb-3">Booking Summary</h3>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-500">Physiotherapist</span>
                  <span className="font-medium">{physio.name}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Service</span>
                  <span className="font-medium">{selectedService.name}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Date</span>
                  <span className="font-medium">{formatDate(selectedDate)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Time</span>
                  <span className="font-medium">
                    {selectedTime} — {getEndTime(selectedTime, selectedService.duration)}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Location</span>
                  <span className="font-medium">{physio.clinicName}, {physio.location.area}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Patient</span>
                  <span className="font-medium">{formData.fullName}</span>
                </div>

                <hr className="my-3" />

                <div className="flex justify-between">
                  <span className="text-gray-500">Consultation Fee</span>
                  <span>₹{selectedService.price}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Platform Fee</span>
                  <span>₹{platformFee}</span>
                </div>
                <hr />
                <div className="flex justify-between text-base font-bold">
                  <span>Total</span>
                  <span>₹{total}</span>
                </div>
              </div>

              <button
                onClick={() => setCurrentStep(0)}
                className="text-sm text-primary hover:underline mt-3"
              >
                Edit Booking
              </button>
            </div>

            <div className="card mb-4">
              <h3 className="font-semibold text-gray-900 mb-2">Cancellation Policy</h3>
              <p className="text-sm text-gray-600">
                Free cancellation up to 4 hours before the appointment. After that, a ₹200 cancellation fee applies.
              </p>
            </div>

            <div className="card mb-6">
              <h3 className="font-semibold text-gray-900 mb-3">Payment Method</h3>
              <div className="space-y-2">
                {[
                  { id: "upi", label: "UPI (GPay, PhonePe, Paytm)" },
                  { id: "card", label: "Credit / Debit Card" },
                  { id: "netbanking", label: "Net Banking" },
                  { id: "clinic", label: "Pay at Clinic" },
                ].map((method) => (
                  <label key={method.id} className="flex items-center gap-3 p-3 rounded-lg border border-gray-200 cursor-pointer hover:border-primary transition-colors">
                    <input
                      type="radio"
                      name="payment"
                      value={method.id}
                      checked={paymentMethod === method.id}
                      onChange={(e) => setPaymentMethod(e.target.value)}
                      className="w-4 h-4 border-gray-300 text-primary focus:ring-primary"
                    />
                    <span className="text-sm font-medium text-gray-700">{method.label}</span>
                  </label>
                ))}
              </div>
            </div>

            <div className="flex justify-between">
              <button onClick={() => setCurrentStep(2)} className="btn-outline">
                &larr; Back
              </button>
              <button onClick={handleConfirmBooking} className="btn-accent text-lg">
                Pay ₹{total} & Book
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
