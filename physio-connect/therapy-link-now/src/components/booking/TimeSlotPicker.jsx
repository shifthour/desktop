import React, { useState, useMemo } from "react";
import { Calendar } from "@/components/ui/calendar";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Clock, ChevronLeft, ChevronRight } from "lucide-react";
import { format, addDays, isBefore, startOfDay, isToday } from "date-fns";
import { cn } from "@/lib/utils";
import { motion, AnimatePresence } from "framer-motion";

export default function TimeSlotPicker({
  physio,
  appointments = [],
  blockedSlots = [],
  selectedDate,
  selectedTime,
  onDateChange,
  onTimeChange,
}) {
  const generateTimeSlots = (date) => {
    if (!date || !physio) return [];

    const dayName = format(date, "EEEE");
    if (!(physio.available_days || []).includes(dayName)) return [];

    const start = physio.working_hours_start || "09:00";
    const end = physio.working_hours_end || "17:00";
    const duration = physio.session_duration || 45;

    const [startH, startM] = start.split(":").map(Number);
    const [endH, endM] = end.split(":").map(Number);

    const slots = [];
    let currentMin = startH * 60 + startM;
    const endMin = endH * 60 + endM;

    while (currentMin + duration <= endMin) {
      const h = Math.floor(currentMin / 60);
      const m = currentMin % 60;
      const slotStart = `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
      const eH = Math.floor((currentMin + duration) / 60);
      const eM = (currentMin + duration) % 60;
      const slotEnd = `${String(eH).padStart(2, "0")}:${String(eM).padStart(2, "0")}`;
      slots.push(`${slotStart} - ${slotEnd}`);
      currentMin += duration;
    }

    return slots;
  };

  const dateStr = selectedDate ? format(selectedDate, "yyyy-MM-dd") : null;

  const bookedSlots = useMemo(() => {
    if (!dateStr) return new Set();
    return new Set(
      appointments
        .filter(a => a.appointment_date === dateStr && a.status !== "cancelled")
        .map(a => a.time_slot)
    );
  }, [appointments, dateStr]);

  const blockedSlotSet = useMemo(() => {
    if (!dateStr) return new Set();
    return new Set(
      blockedSlots
        .filter(b => b.date === dateStr)
        .map(b => b.time_slot)
    );
  }, [blockedSlots, dateStr]);

  const timeSlots = selectedDate ? generateTimeSlots(selectedDate) : [];

  const isSlotAvailable = (slot) => {
    return !bookedSlots.has(slot) && !blockedSlotSet.has(slot);
  };

  const isSlotPast = (slot) => {
    if (!selectedDate || !isToday(selectedDate)) return false;
    const [timeStr] = slot.split(" - ");
    const [h, m] = timeStr.split(":").map(Number);
    const now = new Date();
    return h < now.getHours() || (h === now.getHours() && m <= now.getMinutes());
  };

  const disabledDays = (date) => {
    if (isBefore(date, startOfDay(new Date()))) return true;
    const dayName = format(date, "EEEE");
    return !(physio?.available_days || []).includes(dayName);
  };

  const availableCount = timeSlots.filter(s => isSlotAvailable(s) && !isSlotPast(s)).length;

  return (
    <div className="space-y-8">
      <div className="bg-white rounded-2xl border border-gray-100 p-6">
        <h3 className="font-bold text-gray-900 mb-4 flex items-center gap-2">
          <Clock className="w-5 h-5 text-teal-600" />
          Select Date & Time
        </h3>

        <Calendar
          mode="single"
          selected={selectedDate}
          onSelect={onDateChange}
          disabled={disabledDays}
          fromDate={new Date()}
          toDate={addDays(new Date(), 60)}
          className="rounded-xl border-0 mx-auto"
        />
      </div>

      <AnimatePresence mode="wait">
        {selectedDate && (
          <motion.div
            key={dateStr}
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="bg-white rounded-2xl border border-gray-100 p-6"
          >
            <div className="flex items-center justify-between mb-4">
              <h4 className="font-semibold text-gray-900">
                {format(selectedDate, "EEEE, MMM d")}
              </h4>
              <Badge variant="secondary" className={cn(
                "rounded-lg",
                availableCount > 0 ? "bg-emerald-50 text-emerald-700" : "bg-red-50 text-red-700"
              )}>
                {availableCount} available
              </Badge>
            </div>

            {timeSlots.length === 0 ? (
              <p className="text-gray-400 text-sm text-center py-8">
                No slots available on this day
              </p>
            ) : (
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
                {timeSlots.map(slot => {
                  const available = isSlotAvailable(slot) && !isSlotPast(slot);
                  const isSelected = selectedTime === slot;

                  return (
                    <button
                      key={slot}
                      disabled={!available}
                      onClick={() => onTimeChange(slot)}
                      className={cn(
                        "px-3 py-3 rounded-xl text-sm font-medium transition-all duration-200 border",
                        isSelected
                          ? "bg-teal-600 text-white border-teal-600 shadow-lg shadow-teal-600/20"
                          : available
                            ? "bg-white border-gray-200 text-gray-700 hover:border-teal-300 hover:bg-teal-50"
                            : "bg-gray-50 border-gray-100 text-gray-300 cursor-not-allowed line-through"
                      )}
                    >
                      {slot.split(" - ")[0]}
                    </button>
                  );
                })}
              </div>
            )}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}