"use client";

import { use, useState } from "react";
import Link from "next/link";
import { getPhysioBySlug, getReviewsByPhysioId } from "@/lib/data";
import { notFound } from "next/navigation";
import SlotPicker from "@/components/SlotPicker";
import ReviewCard from "@/components/ReviewCard";

const avatarColors = [
  "bg-blue-500", "bg-emerald-500", "bg-violet-500", "bg-amber-500", "bg-rose-500", "bg-teal-500",
];

function getInitials(name: string) {
  return name.replace(/^Dr\.\s*/, "").split(" ").map(n => n[0]).join("").slice(0, 2).toUpperCase();
}

export default function PhysioProfilePage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = use(params);
  const physio = getPhysioBySlug(slug);
  const [activeTab, setActiveTab] = useState<"about" | "services" | "reviews" | "location">("about");
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [selectedTime, setSelectedTime] = useState<string | null>(null);

  if (!physio) return notFound();

  const reviews = getReviewsByPhysioId(physio.id);
  const colorIndex = parseInt(physio.id) % avatarColors.length;

  const tabs = [
    { key: "about" as const, label: "About" },
    { key: "services" as const, label: "Services" },
    { key: "reviews" as const, label: `Reviews (${reviews.length})` },
    { key: "location" as const, label: "Location" },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Breadcrumb */}
      <div className="bg-white border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3">
          <nav className="text-sm text-gray-500">
            <Link href="/" className="hover:text-primary">Home</Link>
            <span className="mx-2">/</span>
            <Link href="/search" className="hover:text-primary">{physio.specializations[0]}</Link>
            <span className="mx-2">/</span>
            <span className="text-gray-900">{physio.name}</span>
          </nav>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <div className="flex flex-col lg:flex-row gap-6">
          {/* Main Content */}
          <div className="flex-1 min-w-0">
            {/* Profile Header */}
            <div className="card mb-6">
              <div className="flex flex-col sm:flex-row gap-4 sm:gap-6">
                <div className={`w-24 h-24 sm:w-28 sm:h-28 rounded-full ${avatarColors[colorIndex]} flex items-center justify-center flex-shrink-0 mx-auto sm:mx-0`}>
                  <span className="text-white font-bold text-2xl sm:text-3xl">{getInitials(physio.name)}</span>
                </div>
                <div className="flex-1 text-center sm:text-left">
                  <div className="flex flex-col sm:flex-row sm:items-center gap-1 sm:gap-2">
                    <h1 className="text-2xl font-bold text-gray-900">{physio.name}</h1>
                    {physio.verified && (
                      <span className="inline-flex items-center justify-center sm:justify-start text-primary text-sm font-medium">
                        <svg className="w-5 h-5 mr-0.5" fill="currentColor" viewBox="0 0 20 20">
                          <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                        </svg>
                        Verified
                      </span>
                    )}
                  </div>
                  <p className="text-gray-600 mt-1">{physio.specializations.join(" · ")}</p>
                  <div className="flex flex-wrap items-center justify-center sm:justify-start gap-3 mt-2 text-sm text-gray-500">
                    <span className="flex items-center gap-1">
                      <svg className="w-4 h-4 text-amber-400" fill="currentColor" viewBox="0 0 20 20">
                        <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                      </svg>
                      {physio.rating} ({physio.reviewCount} reviews)
                    </span>
                    <span>{physio.experience} years experience</span>
                    <span>{physio.totalSessions}+ sessions</span>
                  </div>
                  <div className="flex flex-wrap items-center justify-center sm:justify-start gap-2 mt-2 text-sm text-gray-500">
                    <span>📍 {physio.clinicName}, {physio.location.area}</span>
                  </div>
                  <div className="flex flex-wrap items-center justify-center sm:justify-start gap-2 mt-3">
                    {physio.visitTypes.includes("clinic") && (
                      <span className="chip bg-blue-50 text-blue-700 text-sm">🏥 Clinic Visit</span>
                    )}
                    {physio.visitTypes.includes("home") && (
                      <span className="chip bg-green-50 text-green-700 text-sm">🏠 Home Visit</span>
                    )}
                    {physio.visitTypes.includes("online") && (
                      <span className="chip bg-purple-50 text-purple-700 text-sm">💻 Online</span>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Tab Navigation */}
            <div className="border-b border-gray-200 mb-6 overflow-x-auto">
              <div className="flex gap-0">
                {tabs.map((tab) => (
                  <button
                    key={tab.key}
                    onClick={() => setActiveTab(tab.key)}
                    className={`px-4 sm:px-6 py-3 text-sm font-medium whitespace-nowrap border-b-2 transition-colors ${
                      activeTab === tab.key
                        ? "border-primary text-primary"
                        : "border-transparent text-gray-500 hover:text-gray-700"
                    }`}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Tab Content */}
            {activeTab === "about" && (
              <div className="space-y-6">
                <div className="card">
                  <h2 className="text-lg font-bold text-gray-900 mb-3">Qualifications</h2>
                  <ul className="space-y-2">
                    {physio.qualifications.map((q, i) => (
                      <li key={i} className="flex items-start gap-2 text-gray-700">
                        <svg className="w-5 h-5 text-accent flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                          <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                        </svg>
                        {q}
                      </li>
                    ))}
                  </ul>
                </div>

                <div className="card">
                  <h2 className="text-lg font-bold text-gray-900 mb-3">Specializations</h2>
                  <div className="flex flex-wrap gap-2">
                    {physio.specializations.map((s) => (
                      <span key={s} className="chip bg-primary-light text-primary">{s}</span>
                    ))}
                  </div>
                </div>

                <div className="card">
                  <h2 className="text-lg font-bold text-gray-900 mb-3">About Me</h2>
                  <p className="text-gray-700 leading-relaxed">{physio.bio}</p>
                </div>
              </div>
            )}

            {activeTab === "services" && (
              <div className="card">
                <h2 className="text-lg font-bold text-gray-900 mb-4">Consultation Types & Pricing</h2>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-gray-200">
                        <th className="text-left py-3 pr-4 text-sm font-semibold text-gray-600">Service</th>
                        <th className="text-left py-3 px-4 text-sm font-semibold text-gray-600">Duration</th>
                        <th className="text-right py-3 pl-4 text-sm font-semibold text-gray-600">Fee</th>
                      </tr>
                    </thead>
                    <tbody>
                      {physio.services.map((service) => (
                        <tr key={service.id} className="border-b border-gray-50 last:border-0">
                          <td className="py-4 pr-4">
                            <div className="font-medium text-gray-900">{service.name}</div>
                            <div className="text-sm text-gray-500 mt-0.5">{service.description}</div>
                          </td>
                          <td className="py-4 px-4 text-gray-600">{service.duration} min</td>
                          <td className="py-4 pl-4 text-right font-bold text-gray-900">₹{service.price}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {activeTab === "reviews" && (
              <div className="space-y-4">
                <div className="card">
                  <div className="flex items-center gap-4">
                    <div className="text-center">
                      <div className="text-4xl font-bold text-gray-900">{physio.rating}</div>
                      <div className="flex gap-0.5 mt-1">
                        {[...Array(5)].map((_, i) => (
                          <svg key={i} className={`w-5 h-5 ${i < Math.round(physio.rating) ? "text-amber-400" : "text-gray-200"}`} fill="currentColor" viewBox="0 0 20 20">
                            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                          </svg>
                        ))}
                      </div>
                      <div className="text-sm text-gray-500 mt-1">{physio.reviewCount} reviews</div>
                    </div>
                  </div>
                </div>
                {reviews.map((review) => (
                  <ReviewCard key={review.id} review={review} />
                ))}
              </div>
            )}

            {activeTab === "location" && (
              <div className="card">
                <h2 className="text-lg font-bold text-gray-900 mb-3">{physio.clinicName}</h2>
                <div className="bg-gray-100 rounded-lg h-64 flex items-center justify-center mb-4">
                  <div className="text-center text-gray-500">
                    <svg className="w-12 h-12 mx-auto mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
                    </svg>
                    <p>Map View</p>
                    <p className="text-sm">📍 {physio.location.area}, {physio.location.city}</p>
                  </div>
                </div>
                <button className="btn-outline w-full sm:w-auto">Get Directions</button>
              </div>
            )}
          </div>

          {/* Booking Sidebar (Desktop) / Bottom Bar (Mobile) */}
          <aside className="hidden lg:block w-80 flex-shrink-0">
            <div className="sticky top-20 card">
              <div className="flex items-center justify-between mb-4">
                <span className="text-2xl font-bold text-gray-900">₹{physio.services[0].price}</span>
                <span className="text-gray-500 text-sm">/ {physio.services[0].name.toLowerCase()}</span>
              </div>

              <h3 className="font-semibold text-gray-900 mb-3">Select Date & Time</h3>
              <SlotPicker
                availability={physio.availability}
                selectedDate={selectedDate}
                selectedTime={selectedTime}
                onSelectDate={(d) => { setSelectedDate(d); setSelectedTime(null); }}
                onSelectTime={setSelectedTime}
              />

              <Link
                href={selectedDate && selectedTime ? `/book/${physio.id}?date=${selectedDate}&time=${selectedTime}` : `#`}
                onClick={(e) => { if (!selectedDate || !selectedTime) e.preventDefault(); }}
                className={`block w-full text-center mt-4 py-3 rounded-lg font-semibold transition-colors ${
                  selectedDate && selectedTime
                    ? "bg-accent text-white hover:bg-green-700"
                    : "bg-gray-200 text-gray-400 cursor-not-allowed"
                }`}
              >
                {selectedDate && selectedTime ? "Continue to Booking" : "Select a time slot"}
              </Link>
            </div>
          </aside>
        </div>
      </div>

      {/* Mobile Sticky Bottom Bar */}
      <div className="fixed bottom-0 left-0 right-0 bg-white border-t border-gray-200 p-4 lg:hidden z-40">
        <div className="flex items-center justify-between">
          <div>
            <span className="text-xl font-bold text-gray-900">₹{Math.min(...physio.services.map(s => s.price))}</span>
            <span className="text-gray-500 text-sm"> / session</span>
          </div>
          <Link
            href={`/book/${physio.id}`}
            className="btn-accent"
          >
            Book Now
          </Link>
        </div>
      </div>
    </div>
  );
}
