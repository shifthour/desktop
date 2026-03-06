import React, { useState, useMemo, useEffect, useRef } from "react";
import { api as base44 } from "@/api/apiClient";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { CalendarDays, Save, Loader2, CheckCircle2, AlertCircle } from "lucide-react";
import { format, addDays, startOfDay } from "date-fns";
import { cn } from "@/lib/utils";

// Generate time options from 06:00 to 22:00 in 30-min intervals
const TIME_OPTIONS = [];
for (let h = 6; h <= 22; h++) {
  for (let m = 0; m < 60; m += 30) {
    if (h === 22 && m > 0) break;
    TIME_OPTIONS.push(`${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`);
  }
}

export default function WeeklySchedule({ physio }) {
  const queryClient = useQueryClient();
  const [saved, setSaved] = useState(false);
  const [saveError, setSaveError] = useState("");

  // Next 7 days starting from today
  const days = useMemo(() => {
    const today = startOfDay(new Date());
    return Array.from({ length: 7 }, (_, i) => {
      const date = addDays(today, i);
      return {
        date,
        dateStr: format(date, "yyyy-MM-dd"),
        dayName: format(date, "EEEE"),
        label: format(date, "EEE, MMM d"),
      };
    });
  }, []);

  const fromDate = days[0].dateStr;
  const toDate = days[6].dateStr;

  // Fetch existing overrides for this week
  const { data: overrides = [], isLoading } = useQuery({
    queryKey: ["availability-overrides", physio.id, fromDate, toDate],
    queryFn: () =>
      base44.entities.AvailabilityOverride.filter({
        physio_id: physio.id,
        from_date: fromDate,
        to_date: toDate,
      }),
    enabled: !!physio.id,
  });

  // Build schedule state: each day has { is_available, start_time, end_time }
  const [schedule, setSchedule] = useState({});
  const initializedFor = useRef(null);

  // Initialize schedule from overrides + physio defaults — only once per physio
  useEffect(() => {
    if (!overrides || initializedFor.current === physio.id) return;

    const availableDays = physio.available_days || [];
    const defaultStart = physio.working_hours_start || "09:00";
    const defaultEnd = physio.working_hours_end || "17:00";
    const overrideMap = {};
    overrides.forEach((o) => {
      overrideMap[o.date] = o;
    });

    const initial = {};
    days.forEach((d) => {
      const override = overrideMap[d.dateStr];
      if (override) {
        initial[d.dateStr] = {
          is_available: !!override.is_available,
          start_time: override.start_time || defaultStart,
          end_time: override.end_time || defaultEnd,
        };
      } else {
        const isDefault = availableDays.includes(d.dayName);
        initial[d.dateStr] = {
          is_available: isDefault,
          start_time: defaultStart,
          end_time: defaultEnd,
        };
      }
    });
    setSchedule(initial);
    initializedFor.current = physio.id;
  }, [overrides, physio, days]);

  const toggleDay = (dateStr) => {
    setSchedule((prev) => ({
      ...prev,
      [dateStr]: { ...prev[dateStr], is_available: !prev[dateStr]?.is_available },
    }));
    setSaved(false);
  };

  const updateTime = (dateStr, field, value) => {
    setSchedule((prev) => ({
      ...prev,
      [dateStr]: { ...prev[dateStr], [field]: value },
    }));
    setSaved(false);
  };

  // Save all 7 days (upsert each)
  const saveMutation = useMutation({
    mutationFn: async () => {
      const promises = days.map((d) => {
        const entry = schedule[d.dateStr];
        if (!entry) return Promise.resolve();
        return base44.entities.AvailabilityOverride.create({
          physio_id: physio.id,
          date: d.dateStr,
          is_available: entry.is_available ? 1 : 0,
          start_time: entry.start_time,
          end_time: entry.end_time,
        });
      });
      return Promise.all(promises);
    },
    onSuccess: () => {
      // Allow re-initialization from fresh server data after save
      initializedFor.current = null;
      queryClient.invalidateQueries({ queryKey: ["availability-overrides"] });
      setSaveError("");
      setSaved(true);
      setTimeout(() => setSaved(false), 5000);
    },
    onError: (err) => {
      setSaveError(err.message || "Failed to save schedule. Please try again.");
    },
  });

  if (isLoading) {
    return (
      <Card className="border-gray-100 mb-8">
        <CardContent className="p-6 flex justify-center">
          <Loader2 className="w-6 h-6 text-teal-600 animate-spin" />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="border-gray-100 mb-8">
      <CardContent className="p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-bold text-gray-900 flex items-center gap-2">
            <CalendarDays className="w-5 h-5 text-teal-600" />
            Weekly Schedule
          </h2>
          <Button
            onClick={() => { setSaveError(""); setSaved(false); saveMutation.mutate(); }}
            disabled={saveMutation.isPending}
            className="bg-teal-600 hover:bg-teal-700 rounded-xl h-10 px-5"
          >
            {saveMutation.isPending ? (
              <Loader2 className="w-4 h-4 animate-spin mr-2" />
            ) : saved ? (
              <CheckCircle2 className="w-4 h-4 mr-2" />
            ) : (
              <Save className="w-4 h-4 mr-2" />
            )}
            {saved ? "Saved!" : "Save Schedule"}
          </Button>
        </div>

        <p className="text-sm text-gray-400 mb-4">
          Manage your availability for the next 7 days. Toggle days on/off and adjust working hours.
        </p>

        {saved && (
          <div className="mb-4 p-3 bg-emerald-50 border border-emerald-200 rounded-xl text-sm text-emerald-700 flex items-center gap-2">
            <CheckCircle2 className="w-4 h-4 flex-shrink-0" />
            Schedule saved successfully! Changes are now visible to patients.
          </div>
        )}

        {saveError && (
          <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700 flex items-center gap-2">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            {saveError}
          </div>
        )}

        <div className="space-y-3">
          {days.map((d) => {
            const entry = schedule[d.dateStr] || {};
            const isOn = entry.is_available;

            return (
              <div
                key={d.dateStr}
                className={cn(
                  "flex flex-col sm:flex-row sm:items-center gap-3 p-4 rounded-xl border transition-all",
                  isOn ? "border-gray-200 bg-white" : "border-gray-100 bg-gray-50"
                )}
              >
                {/* Day label + toggle */}
                <div className="flex items-center gap-3 sm:w-48 flex-shrink-0">
                  <button
                    onClick={() => toggleDay(d.dateStr)}
                    className={cn(
                      "w-12 h-7 rounded-full relative transition-colors flex-shrink-0",
                      isOn ? "bg-teal-600" : "bg-gray-300"
                    )}
                  >
                    <span
                      className={cn(
                        "absolute top-0.5 w-6 h-6 bg-white rounded-full shadow transition-transform",
                        isOn ? "translate-x-5" : "translate-x-0.5"
                      )}
                    />
                  </button>
                  <div>
                    <p className={cn("font-medium text-sm", isOn ? "text-gray-900" : "text-gray-400")}>
                      {d.label}
                    </p>
                    <p className="text-xs text-gray-400">{d.dayName}</p>
                  </div>
                </div>

                {/* Time selects */}
                {isOn ? (
                  <div className="flex items-center gap-2 flex-1">
                    <Select
                      value={entry.start_time || "09:00"}
                      onValueChange={(v) => updateTime(d.dateStr, "start_time", v)}
                    >
                      <SelectTrigger className="w-28 rounded-lg h-9 text-sm">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {TIME_OPTIONS.map((t) => (
                          <SelectItem key={t} value={t}>
                            {t}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <span className="text-gray-400 text-sm">to</span>
                    <Select
                      value={entry.end_time || "17:00"}
                      onValueChange={(v) => updateTime(d.dateStr, "end_time", v)}
                    >
                      <SelectTrigger className="w-28 rounded-lg h-9 text-sm">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {TIME_OPTIONS.map((t) => (
                          <SelectItem key={t} value={t}>
                            {t}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                ) : (
                  <p className="text-sm text-gray-400 italic">Day off</p>
                )}
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
