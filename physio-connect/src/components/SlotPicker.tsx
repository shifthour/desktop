"use client";

import { useState } from "react";
import { DayAvailability } from "@/lib/types";

interface SlotPickerProps {
  availability: DayAvailability[];
  selectedDate: string | null;
  selectedTime: string | null;
  onSelectDate: (date: string) => void;
  onSelectTime: (time: string) => void;
}

export default function SlotPicker({
  availability,
  selectedDate,
  selectedTime,
  onSelectDate,
  onSelectTime,
}: SlotPickerProps) {
  const [startIdx, setStartIdx] = useState(0);
  const visibleDays = 5;

  const visibleAvailability = availability.slice(startIdx, startIdx + visibleDays);

  const currentDay = availability.find((d) => d.date === selectedDate);
  const morningSlots = currentDay?.slots.filter((s) => parseInt(s.time) < 12) ?? [];
  const afternoonSlots = currentDay?.slots.filter((s) => parseInt(s.time) >= 12 && parseInt(s.time) < 17) ?? [];
  const eveningSlots = currentDay?.slots.filter((s) => parseInt(s.time) >= 17) ?? [];

  function formatDate(dateStr: string) {
    const d = new Date(dateStr + "T00:00:00");
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const diff = Math.floor((d.getTime() - today.getTime()) / 86400000);
    const weekday = d.toLocaleDateString("en-IN", { weekday: "short" });
    const day = d.getDate();
    if (diff === 0) return { label: "Today", day };
    if (diff === 1) return { label: "Tmrw", day };
    return { label: weekday, day };
  }

  function hasAvailableSlots(day: DayAvailability) {
    return day.slots.some((s) => s.available);
  }

  return (
    <div>
      <div className="flex items-center gap-1 mb-4">
        <button
          onClick={() => setStartIdx(Math.max(0, startIdx - 1))}
          disabled={startIdx === 0}
          className="p-1.5 rounded-lg hover:bg-gray-100 disabled:opacity-30 transition-colors"
          aria-label="Previous dates"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
          </svg>
        </button>

        <div className="flex gap-1.5 flex-1 overflow-hidden">
          {visibleAvailability.map((day) => {
            const { label, day: dayNum } = formatDate(day.date);
            const isSelected = day.date === selectedDate;
            const hasSlots = hasAvailableSlots(day);

            return (
              <button
                key={day.date}
                onClick={() => hasSlots && onSelectDate(day.date)}
                disabled={!hasSlots}
                className={`flex-1 py-2 px-1 rounded-lg text-center transition-all ${
                  isSelected
                    ? "bg-primary text-white shadow-md"
                    : hasSlots
                    ? "bg-gray-50 hover:bg-primary-light text-gray-700"
                    : "bg-gray-50 text-gray-300 cursor-not-allowed"
                }`}
              >
                <div className="text-xs font-medium">{label}</div>
                <div className="text-lg font-bold">{dayNum}</div>
                {hasSlots && (
                  <div className={`w-1.5 h-1.5 rounded-full mx-auto mt-0.5 ${isSelected ? "bg-white" : "bg-accent"}`} />
                )}
              </button>
            );
          })}
        </div>

        <button
          onClick={() => setStartIdx(Math.min(availability.length - visibleDays, startIdx + 1))}
          disabled={startIdx >= availability.length - visibleDays}
          className="p-1.5 rounded-lg hover:bg-gray-100 disabled:opacity-30 transition-colors"
          aria-label="Next dates"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
        </button>
      </div>

      {selectedDate && currentDay && (
        <div className="space-y-4">
          {morningSlots.length > 0 && (
            <div>
              <h4 className="text-sm font-medium text-gray-500 mb-2">Morning</h4>
              <div className="flex flex-wrap gap-2">
                {morningSlots.map((slot) => (
                  <button
                    key={slot.time}
                    onClick={() => slot.available && onSelectTime(slot.time)}
                    disabled={!slot.available}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                      selectedTime === slot.time
                        ? "bg-primary text-white shadow-md"
                        : slot.available
                        ? "border border-gray-200 hover:border-primary hover:text-primary"
                        : "bg-gray-50 text-gray-300 line-through cursor-not-allowed"
                    }`}
                  >
                    {slot.time}
                  </button>
                ))}
              </div>
            </div>
          )}
          {afternoonSlots.length > 0 && (
            <div>
              <h4 className="text-sm font-medium text-gray-500 mb-2">Afternoon</h4>
              <div className="flex flex-wrap gap-2">
                {afternoonSlots.map((slot) => (
                  <button
                    key={slot.time}
                    onClick={() => slot.available && onSelectTime(slot.time)}
                    disabled={!slot.available}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                      selectedTime === slot.time
                        ? "bg-primary text-white shadow-md"
                        : slot.available
                        ? "border border-gray-200 hover:border-primary hover:text-primary"
                        : "bg-gray-50 text-gray-300 line-through cursor-not-allowed"
                    }`}
                  >
                    {slot.time}
                  </button>
                ))}
              </div>
            </div>
          )}
          {eveningSlots.length > 0 && (
            <div>
              <h4 className="text-sm font-medium text-gray-500 mb-2">Evening</h4>
              <div className="flex flex-wrap gap-2">
                {eveningSlots.map((slot) => (
                  <button
                    key={slot.time}
                    onClick={() => slot.available && onSelectTime(slot.time)}
                    disabled={!slot.available}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                      selectedTime === slot.time
                        ? "bg-primary text-white shadow-md"
                        : slot.available
                        ? "border border-gray-200 hover:border-primary hover:text-primary"
                        : "bg-gray-50 text-gray-300 line-through cursor-not-allowed"
                    }`}
                  >
                    {slot.time}
                  </button>
                ))}
              </div>
            </div>
          )}

          <p className="text-xs text-gray-400 mt-2 flex items-center gap-1">
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            Slots update in real time. Selected slots are held for 10 minutes.
          </p>
        </div>
      )}
    </div>
  );
}
