"use client";

import { useState, useEffect } from "react";
import { adminStats } from "@/lib/data";
import { Physiotherapist, Booking } from "@/lib/types";
import { fetchPhysios } from "@/lib/api-client";
import Link from "next/link";

type AdminTab = "dashboard" | "providers" | "bookings" | "slots";

const statusColors: Record<string, string> = {
  confirmed: "bg-accent/10 text-accent",
  pending: "bg-warning/10 text-warning",
  cancelled: "bg-error/10 text-error",
  completed: "bg-blue-50 text-blue-700",
};

const pendingProviders = [
  { id: "p1", name: "Dr. Raj Kumar", specialization: "Sports Injury", applied: "2 days ago", experience: 5 },
  { id: "p2", name: "Dr. Meera Nair", specialization: "Pediatric", applied: "3 days ago", experience: 6 },
  { id: "p3", name: "Dr. Sanjay Reddy", specialization: "Orthopedic", applied: "4 days ago", experience: 3 },
  { id: "p4", name: "Dr. Fatima Khan", specialization: "Women's Health", applied: "5 days ago", experience: 8 },
];

export default function AdminPage() {
  const [activeTab, setActiveTab] = useState<AdminTab>("dashboard");
  const [providerStatuses, setProviderStatuses] = useState<Record<string, string>>({});
  const [blockedSlots, setBlockedSlots] = useState<string[]>([]);
  const [physiotherapists, setPhysiotherapists] = useState<Physiotherapist[]>([]);
  const [sampleBookings] = useState<Booking[]>([]);

  useEffect(() => {
    fetchPhysios()
      .then(setPhysiotherapists)
      .catch(console.error);
  }, []);

  const tabs = [
    { key: "dashboard" as const, label: "Dashboard", icon: "📊" },
    { key: "providers" as const, label: "Providers", icon: "👨‍⚕️" },
    { key: "bookings" as const, label: "Bookings", icon: "📅" },
    { key: "slots" as const, label: "Time Slots", icon: "🕐" },
  ];

  function handleApprove(id: string) {
    setProviderStatuses((prev) => ({ ...prev, [id]: "approved" }));
  }

  function handleReject(id: string) {
    setProviderStatuses((prev) => ({ ...prev, [id]: "rejected" }));
  }

  function toggleBlockSlot(slotKey: string) {
    setBlockedSlots((prev) =>
      prev.includes(slotKey) ? prev.filter((s) => s !== slotKey) : [...prev, slotKey]
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Admin Header */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-14">
            <div className="flex items-center gap-2">
              <span className="text-sm font-bold text-gray-500 bg-gray-100 px-2 py-0.5 rounded">ADMIN</span>
              <h1 className="font-bold text-gray-900">PhysioConnect Admin</h1>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-sm text-gray-500">admin@physioconnect.in</span>
            </div>
          </div>
          {/* Tab Nav */}
          <div className="flex gap-0 overflow-x-auto">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={`px-4 py-3 text-sm font-medium whitespace-nowrap border-b-2 transition-colors ${
                  activeTab === tab.key
                    ? "border-primary text-primary"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                <span className="mr-1.5">{tab.icon}</span>
                {tab.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {/* Dashboard Tab */}
        {activeTab === "dashboard" && (
          <div className="space-y-6">
            {/* Metric Cards */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="card">
                <div className="text-sm text-gray-500 mb-1">Total Providers</div>
                <div className="text-3xl font-bold text-gray-900">{adminStats.totalProviders}</div>
                <div className="text-sm text-accent mt-1">+{adminStats.newProviders} new this month</div>
              </div>
              <div className="card">
                <div className="text-sm text-gray-500 mb-1">Bookings This Month</div>
                <div className="text-3xl font-bold text-gray-900">{adminStats.bookingsThisMonth.toLocaleString()}</div>
                <div className="text-sm text-accent mt-1">+{adminStats.bookingsGrowth}% vs last month</div>
              </div>
              <div className="card">
                <div className="text-sm text-gray-500 mb-1">Revenue This Month</div>
                <div className="text-3xl font-bold text-gray-900">₹{(adminStats.revenueThisMonth / 100000).toFixed(1)}L</div>
                <div className="text-sm text-accent mt-1">+{adminStats.revenueGrowth}% vs last month</div>
              </div>
              <div className="card border-warning/30 bg-warning-light">
                <div className="text-sm text-gray-500 mb-1">Pending Approvals</div>
                <div className="text-3xl font-bold text-warning">{adminStats.pendingApprovals}</div>
                <button onClick={() => setActiveTab("providers")} className="text-sm text-warning font-medium underline mt-1">
                  Review now &rarr;
                </button>
              </div>
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Bookings Chart Placeholder */}
              <div className="card">
                <h3 className="font-bold text-gray-900 mb-4">Bookings (Last 30 Days)</h3>
                <div className="h-48 bg-gray-50 rounded-lg flex items-center justify-center">
                  <div className="text-center">
                    <div className="flex items-end justify-center gap-1 mb-2">
                      {[35, 42, 38, 55, 48, 62, 58, 71, 65, 80, 75, 85].map((h, i) => (
                        <div
                          key={i}
                          className="w-5 bg-primary/70 rounded-t"
                          style={{ height: `${h * 1.5}px` }}
                        />
                      ))}
                    </div>
                    <p className="text-xs text-gray-400">Trending upward +18%</p>
                  </div>
                </div>
              </div>

              {/* Specialization Breakdown */}
              <div className="card">
                <h3 className="font-bold text-gray-900 mb-4">Top Specializations</h3>
                <div className="space-y-3">
                  {[
                    { name: "Orthopedic", pct: 34, color: "bg-blue-500" },
                    { name: "Sports Injury", pct: 28, color: "bg-emerald-500" },
                    { name: "Neurological", pct: 18, color: "bg-violet-500" },
                    { name: "Pediatric", pct: 12, color: "bg-amber-500" },
                    { name: "Other", pct: 8, color: "bg-gray-400" },
                  ].map((item) => (
                    <div key={item.name}>
                      <div className="flex justify-between text-sm mb-1">
                        <span className="font-medium text-gray-700">{item.name}</span>
                        <span className="text-gray-500">{item.pct}%</span>
                      </div>
                      <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                        <div className={`h-full ${item.color} rounded-full`} style={{ width: `${item.pct}%` }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Recent Bookings Quick View */}
            <div className="card">
              <div className="flex items-center justify-between mb-4">
                <h3 className="font-bold text-gray-900">Recent Bookings</h3>
                <button onClick={() => setActiveTab("bookings")} className="text-sm text-primary font-medium hover:underline">
                  View All
                </button>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 font-semibold text-gray-600">ID</th>
                      <th className="text-left py-2 font-semibold text-gray-600">Patient</th>
                      <th className="text-left py-2 font-semibold text-gray-600">Physio</th>
                      <th className="text-left py-2 font-semibold text-gray-600">Date/Time</th>
                      <th className="text-left py-2 font-semibold text-gray-600">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sampleBookings.slice(0, 5).map((b) => (
                      <tr key={b.id} className="border-b border-gray-50">
                        <td className="py-3 font-mono text-xs text-gray-500">#{b.id.slice(-4)}</td>
                        <td className="py-3">{b.patientName}</td>
                        <td className="py-3">{b.physioName}</td>
                        <td className="py-3 text-gray-500">{b.date} {b.time}</td>
                        <td className="py-3">
                          <span className={`chip text-xs ${statusColors[b.status]}`}>
                            {b.status}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* Providers Tab */}
        {activeTab === "providers" && (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-bold text-gray-900">Provider Approvals</h2>
              <span className="chip bg-warning-light text-warning font-semibold">
                {pendingProviders.filter((p) => !providerStatuses[p.id]).length} pending
              </span>
            </div>

            <div className="space-y-4">
              {pendingProviders.map((provider) => {
                const status = providerStatuses[provider.id];
                return (
                  <div key={provider.id} className={`card ${status ? "opacity-60" : ""}`}>
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                      <div className="flex items-center gap-4">
                        <div className="w-12 h-12 rounded-full bg-gray-200 flex items-center justify-center font-bold text-gray-600">
                          {provider.name.replace(/^Dr\.\s*/, "")[0]}
                        </div>
                        <div>
                          <h3 className="font-semibold text-gray-900">{provider.name}</h3>
                          <p className="text-sm text-gray-500">
                            {provider.specialization} &middot; {provider.experience} yrs exp &middot; Applied {provider.applied}
                          </p>
                        </div>
                      </div>
                      <div className="flex gap-2 sm:flex-shrink-0">
                        {status ? (
                          <span className={`chip ${status === "approved" ? "bg-accent/10 text-accent" : "bg-error/10 text-error"} font-semibold`}>
                            {status === "approved" ? "Approved" : "Rejected"}
                          </span>
                        ) : (
                          <>
                            <button className="btn-outline !py-1.5 !px-3 text-sm">
                              Review
                            </button>
                            <button
                              onClick={() => handleReject(provider.id)}
                              className="text-sm py-1.5 px-3 rounded-lg border-2 border-error/30 text-error hover:bg-error-light transition-colors font-semibold"
                            >
                              Reject
                            </button>
                            <button
                              onClick={() => handleApprove(provider.id)}
                              className="btn-accent !py-1.5 !px-3 text-sm"
                            >
                              Approve
                            </button>
                          </>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* All Providers */}
            <div className="card">
              <h3 className="font-bold text-gray-900 mb-4">Active Providers ({physiotherapists.length})</h3>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 font-semibold text-gray-600">Provider</th>
                      <th className="text-left py-2 font-semibold text-gray-600">Specialization</th>
                      <th className="text-left py-2 font-semibold text-gray-600">Rating</th>
                      <th className="text-left py-2 font-semibold text-gray-600">Sessions</th>
                      <th className="text-left py-2 font-semibold text-gray-600">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {physiotherapists.map((p) => (
                      <tr key={p.id} className="border-b border-gray-50">
                        <td className="py-3">
                          <Link href={`/physio/${p.slug}`} className="font-medium text-primary hover:underline">
                            {p.name}
                          </Link>
                        </td>
                        <td className="py-3 text-gray-600">{p.specializations[0]}</td>
                        <td className="py-3 text-gray-600">★ {p.rating}</td>
                        <td className="py-3 text-gray-600">{p.totalSessions}</td>
                        <td className="py-3">
                          <span className="chip bg-accent/10 text-accent text-xs font-semibold">Active</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* Bookings Tab */}
        {activeTab === "bookings" && (
          <div>
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-xl font-bold text-gray-900">All Bookings</h2>
              <div className="flex gap-2">
                <select className="input-field !w-auto text-sm" aria-label="Filter by status">
                  <option value="">All Statuses</option>
                  <option value="confirmed">Confirmed</option>
                  <option value="pending">Pending</option>
                  <option value="completed">Completed</option>
                  <option value="cancelled">Cancelled</option>
                </select>
              </div>
            </div>

            <div className="card overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-3 font-semibold text-gray-600">Booking ID</th>
                    <th className="text-left py-3 font-semibold text-gray-600">Patient</th>
                    <th className="text-left py-3 font-semibold text-gray-600">Physiotherapist</th>
                    <th className="text-left py-3 font-semibold text-gray-600">Service</th>
                    <th className="text-left py-3 font-semibold text-gray-600">Date & Time</th>
                    <th className="text-left py-3 font-semibold text-gray-600">Fee</th>
                    <th className="text-left py-3 font-semibold text-gray-600">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {sampleBookings.map((booking) => (
                    <tr key={booking.id} className="border-b border-gray-50 hover:bg-gray-50">
                      <td className="py-3 font-mono text-xs text-gray-500">#{booking.id}</td>
                      <td className="py-3">
                        <div className="font-medium">{booking.patientName}</div>
                        <div className="text-xs text-gray-400">{booking.patientEmail}</div>
                      </td>
                      <td className="py-3">{booking.physioName}</td>
                      <td className="py-3 text-gray-600">{booking.serviceName}</td>
                      <td className="py-3 text-gray-600">{booking.date} at {booking.time}</td>
                      <td className="py-3 font-medium">₹{booking.fee + booking.platformFee}</td>
                      <td className="py-3">
                        <span className={`chip text-xs font-semibold ${statusColors[booking.status]}`}>
                          {booking.status}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Time Slots Tab */}
        {activeTab === "slots" && (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-bold text-gray-900">Manage Time Slots</h2>
              <p className="text-sm text-gray-500">Click a slot to block/unblock it</p>
            </div>

            <div className="card">
              <h3 className="font-bold text-gray-900 mb-4">
                Dr. Ananya Sharma — {new Date().toLocaleDateString("en-IN", { weekday: "long", month: "long", day: "numeric" })}
              </h3>
              <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 gap-2">
                {(physiotherapists[0]?.availability[0]?.slots || []).map((slot) => {
                  const slotKey = `1-${physiotherapists[0]?.availability[0]?.date || ""}-${slot.time}`;
                  const isBlocked = blockedSlots.includes(slotKey);
                  return (
                    <button
                      key={slot.time}
                      onClick={() => toggleBlockSlot(slotKey)}
                      className={`p-3 rounded-lg text-sm font-medium transition-all border ${
                        isBlocked
                          ? "bg-error-light border-error/30 text-error"
                          : slot.available
                          ? "bg-white border-gray-200 text-gray-700 hover:border-primary"
                          : "bg-gray-50 border-gray-100 text-gray-400"
                      }`}
                    >
                      <div>{slot.time}</div>
                      <div className="text-xs mt-1">
                        {isBlocked ? "🔒 Blocked" : slot.available ? "Open" : "Booked"}
                      </div>
                    </button>
                  );
                })}
              </div>

              <div className="mt-4 flex items-center gap-4 text-xs text-gray-500">
                <span className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded border border-gray-200 bg-white inline-block"></span> Open
                </span>
                <span className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded bg-gray-50 border border-gray-100 inline-block"></span> Booked
                </span>
                <span className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded bg-error-light border border-error/30 inline-block"></span> Blocked by Admin
                </span>
              </div>
            </div>

            {blockedSlots.length > 0 && (
              <div className="card bg-error-light border-error/20">
                <h3 className="font-semibold text-error mb-2">Blocked Slots ({blockedSlots.length})</h3>
                <div className="flex flex-wrap gap-2">
                  {blockedSlots.map((s) => {
                    const parts = s.split("-");
                    const time = parts[parts.length - 1];
                    return (
                      <span key={s} className="chip bg-white text-error text-xs border border-error/20">
                        🔒 {time}
                        <button onClick={() => toggleBlockSlot(s)} className="ml-1 hover:text-red-800" aria-label={`Unblock ${time}`}>×</button>
                      </span>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
