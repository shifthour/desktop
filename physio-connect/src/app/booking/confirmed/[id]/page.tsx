"use client";

import { use } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";

export default function BookingConfirmedPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const searchParams = useSearchParams();

  const physioName = searchParams.get("physio") || "Dr. Ananya Sharma";
  const serviceName = searchParams.get("service") || "Follow-up Session";
  const date = searchParams.get("date") || "2026-02-09";
  const time = searchParams.get("time") || "16:00";
  const fee = searchParams.get("fee") || "850";

  const formattedDate = new Date(date + "T00:00:00").toLocaleDateString("en-IN", {
    weekday: "long",
    day: "numeric",
    month: "short",
    year: "numeric",
  });

  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
      <div className="max-w-lg w-full">
        <div className="card text-center">
          {/* Success Animation */}
          <div className="w-20 h-20 bg-accent/10 rounded-full flex items-center justify-center mx-auto mb-4">
            <div className="w-14 h-14 bg-accent rounded-full flex items-center justify-center">
              <svg className="w-8 h-8 text-white" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
              </svg>
            </div>
          </div>

          <h1 className="text-2xl font-bold text-gray-900 mb-1">Booking Confirmed!</h1>
          <p className="text-gray-500 mb-6">Booking ID: #{id}</p>

          {/* Booking Details Card */}
          <div className="bg-gray-50 rounded-xl p-4 text-left mb-6">
            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-500">Physiotherapist</span>
                <span className="font-medium text-gray-900">{physioName}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Service</span>
                <span className="font-medium text-gray-900">{serviceName}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Date</span>
                <span className="font-medium text-gray-900">{formattedDate}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Time</span>
                <span className="font-medium text-gray-900">{time}</span>
              </div>
              <hr />
              <div className="flex justify-between font-bold">
                <span>Total Paid</span>
                <span>₹{fee}</span>
              </div>
            </div>
          </div>

          <div className="bg-blue-50 rounded-lg p-3 text-left mb-6">
            <p className="text-sm text-blue-800">
              Confirmation sent to your email. An SMS reminder will be sent 2 hours before your appointment.
            </p>
          </div>

          {/* Action Buttons */}
          <div className="flex gap-3 mb-6">
            <button className="btn-outline flex-1 !py-2.5 text-sm flex items-center justify-center gap-1.5">
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
              </svg>
              Add to Calendar
            </button>
            <button className="btn-outline flex-1 !py-2.5 text-sm flex items-center justify-center gap-1.5">
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
              </svg>
              Get Directions
            </button>
          </div>

          {/* What to Bring */}
          <div className="text-left border-t border-gray-100 pt-4">
            <h3 className="font-semibold text-gray-900 text-sm mb-2">What to Bring</h3>
            <ul className="text-sm text-gray-600 space-y-1">
              <li className="flex items-center gap-2">
                <span className="w-1.5 h-1.5 rounded-full bg-gray-400"></span>
                Previous medical reports / X-rays
              </li>
              <li className="flex items-center gap-2">
                <span className="w-1.5 h-1.5 rounded-full bg-gray-400"></span>
                Comfortable clothing
              </li>
              <li className="flex items-center gap-2">
                <span className="w-1.5 h-1.5 rounded-full bg-gray-400"></span>
                List of current medications
              </li>
            </ul>
          </div>

          <div className="flex gap-3 mt-6">
            <Link href="/" className="btn-outline flex-1">
              Back to Home
            </Link>
            <Link href="/search" className="btn-primary flex-1">
              Book Another
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
