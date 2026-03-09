"use client";

import { useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import Image from "next/image";

interface Stats {
  totalProviders: number;
  pendingApplications: number;
  totalBookings: number;
  approvedApplications: number;
  rejectedApplications: number;
  recentApplications: {
    id: string;
    firstName: string;
    lastName: string;
    email: string;
    status: string;
    createdAt: string;
  }[];
}

const statusColors: Record<string, string> = {
  pending: "bg-amber-50 text-amber-700 border-amber-200",
  approved: "bg-emerald-50 text-emerald-700 border-emerald-200",
  rejected: "bg-red-50 text-red-700 border-red-200",
  needs_info: "bg-blue-50 text-blue-700 border-blue-200",
};

const statusLabels: Record<string, string> = {
  pending: "Pending",
  approved: "Approved",
  rejected: "Rejected",
  needs_info: "Needs Info",
};

export default function AdminDashboard() {
  const router = useRouter();
  const [stats, setStats] = useState<Stats | null>(null);
  const [loading, setLoading] = useState(true);
  const [adminName, setAdminName] = useState("");

  const loadData = useCallback(async () => {
    try {
      const [statsRes, sessionRes] = await Promise.all([
        fetch("/api/admin/stats"),
        fetch("/api/admin/session"),
      ]);

      if (!statsRes.ok || !sessionRes.ok) {
        router.push("/admin/login");
        return;
      }

      const statsData = await statsRes.json();
      const sessionData = await sessionRes.json();

      setStats(statsData);
      setAdminName(sessionData.displayName || sessionData.username);
    } catch {
      router.push("/admin/login");
    } finally {
      setLoading(false);
    }
  }, [router]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const handleLogout = async () => {
    await fetch("/api/admin/logout", { method: "POST" });
    router.push("/admin/login");
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="flex items-center gap-3 text-gray-500">
          <svg className="w-6 h-6 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
          </svg>
          Loading dashboard...
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center gap-3">
              <Image src="/logo.png" alt="On the Move Home Physio" width={140} height={40} />
              <span className="text-xs font-bold text-white bg-navy px-2 py-0.5 rounded-md">ADMIN</span>
            </div>
            <div className="flex items-center gap-4">
              <Link
                href="/admin/applications"
                className="text-sm font-medium text-gray-600 hover:text-primary transition-colors"
              >
                Applications
              </Link>
              <div className="h-5 w-px bg-gray-200" />
              <span className="text-sm text-gray-500">Hi, {adminName}</span>
              <button
                onClick={handleLogout}
                className="text-sm text-gray-500 hover:text-red-600 transition-colors flex items-center gap-1"
              >
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 9V5.25A2.25 2.25 0 0013.5 3h-6a2.25 2.25 0 00-2.25 2.25v13.5A2.25 2.25 0 007.5 21h6a2.25 2.25 0 002.25-2.25V15m3 0l3-3m0 0l-3-3m3 3H9" />
                </svg>
                Logout
              </button>
            </div>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Page Title */}
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-1">Overview of your On the Move Home Physio platform</p>
        </div>

        {/* Stat Cards */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5 mb-8">
          <StatCard
            label="Active Providers"
            value={stats?.totalProviders || 0}
            icon={
              <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M15 19.128a9.38 9.38 0 002.625.372 9.337 9.337 0 004.121-.952 4.125 4.125 0 00-7.533-2.493M15 19.128v-.003c0-1.113-.285-2.16-.786-3.07M15 19.128v.106A12.318 12.318 0 018.624 21c-2.331 0-4.512-.645-6.374-1.766l-.001-.109a6.375 6.375 0 0111.964-3.07M12 6.375a3.375 3.375 0 11-6.75 0 3.375 3.375 0 016.75 0zm8.25 2.25a2.625 2.625 0 11-5.25 0 2.625 2.625 0 015.25 0z" />
              </svg>
            }
            color="primary"
          />
          <StatCard
            label="Pending Applications"
            value={stats?.pendingApplications || 0}
            icon={
              <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            }
            color="amber"
            highlight={!!stats?.pendingApplications}
          />
          <StatCard
            label="Total Bookings"
            value={stats?.totalBookings || 0}
            icon={
              <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6.75 3v2.25M17.25 3v2.25M3 18.75V7.5a2.25 2.25 0 012.25-2.25h13.5A2.25 2.25 0 0121 7.5v11.25m-18 0A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75m-18 0v-7.5A2.25 2.25 0 015.25 9h13.5A2.25 2.25 0 0121 11.25v7.5" />
              </svg>
            }
            color="blue"
          />
          <StatCard
            label="Approved Applications"
            value={stats?.approvedApplications || 0}
            icon={
              <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            }
            color="emerald"
          />
        </div>

        {/* Two-column layout */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Recent Applications */}
          <div className="lg:col-span-2 bg-white rounded-2xl border border-gray-100 shadow-sm">
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
              <h2 className="font-bold text-gray-900">Recent Applications</h2>
              <Link href="/admin/applications" className="text-sm text-primary font-medium hover:underline">
                View All →
              </Link>
            </div>
            <div className="divide-y divide-gray-50">
              {stats?.recentApplications && stats.recentApplications.length > 0 ? (
                stats.recentApplications.map((app) => (
                  <Link
                    key={app.id}
                    href={`/admin/applications/${app.id}`}
                    className="flex items-center justify-between px-6 py-4 hover:bg-gray-50 transition-colors"
                  >
                    <div className="flex items-center gap-3">
                      <div className="w-10 h-10 rounded-full bg-primary-light text-primary font-bold text-sm flex items-center justify-center">
                        {app.firstName[0]}{app.lastName[0]}
                      </div>
                      <div>
                        <p className="font-medium text-gray-900 text-sm">{app.firstName} {app.lastName}</p>
                        <p className="text-xs text-gray-500">{app.email}</p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className={`text-xs font-medium px-2.5 py-1 rounded-full border ${statusColors[app.status] || statusColors.pending}`}>
                        {statusLabels[app.status] || app.status}
                      </span>
                      <span className="text-xs text-gray-400">
                        {new Date(app.createdAt).toLocaleDateString("en-GB", { day: "numeric", month: "short" })}
                      </span>
                    </div>
                  </Link>
                ))
              ) : (
                <div className="px-6 py-12 text-center">
                  <svg className="w-12 h-12 text-gray-300 mx-auto mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                  </svg>
                  <p className="text-sm text-gray-500">No applications yet</p>
                  <p className="text-xs text-gray-400 mt-1">Applications submitted via the Join page will appear here</p>
                </div>
              )}
            </div>
          </div>

          {/* Quick Actions / Summary */}
          <div className="space-y-6">
            {/* Quick Actions */}
            <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-6">
              <h2 className="font-bold text-gray-900 mb-4">Quick Actions</h2>
              <div className="space-y-3">
                <Link
                  href="/admin/applications?status=pending"
                  className="flex items-center gap-3 p-3 rounded-xl hover:bg-amber-50 transition-colors group"
                >
                  <div className="w-10 h-10 rounded-xl bg-amber-100 text-amber-600 flex items-center justify-center flex-shrink-0 group-hover:bg-amber-200 transition-colors">
                    <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-gray-900">Review Pending</p>
                    <p className="text-xs text-gray-500">{stats?.pendingApplications || 0} awaiting review</p>
                  </div>
                </Link>
                <Link
                  href="/admin/applications"
                  className="flex items-center gap-3 p-3 rounded-xl hover:bg-primary-50 transition-colors group"
                >
                  <div className="w-10 h-10 rounded-xl bg-primary-light text-primary flex items-center justify-center flex-shrink-0 group-hover:bg-primary-100 transition-colors">
                    <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 12h16.5m-16.5 3.75h16.5M3.75 19.5h16.5M5.625 4.5h12.75a1.875 1.875 0 010 3.75H5.625a1.875 1.875 0 010-3.75z" />
                    </svg>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-gray-900">All Applications</p>
                    <p className="text-xs text-gray-500">View and manage all submissions</p>
                  </div>
                </Link>
                <Link
                  href="/"
                  className="flex items-center gap-3 p-3 rounded-xl hover:bg-gray-50 transition-colors group"
                >
                  <div className="w-10 h-10 rounded-xl bg-gray-100 text-gray-600 flex items-center justify-center flex-shrink-0 group-hover:bg-gray-200 transition-colors">
                    <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 6H5.25A2.25 2.25 0 003 8.25v10.5A2.25 2.25 0 005.25 21h10.5A2.25 2.25 0 0018 18.75V10.5m-10.5 6L21 3m0 0h-5.25M21 3v5.25" />
                    </svg>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-gray-900">View Website</p>
                    <p className="text-xs text-gray-500">Open the public-facing site</p>
                  </div>
                </Link>
              </div>
            </div>

            {/* Application Status Summary */}
            <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-6">
              <h2 className="font-bold text-gray-900 mb-4">Application Summary</h2>
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-gray-600">Pending</span>
                  <span className="text-sm font-bold text-amber-600">{stats?.pendingApplications || 0}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-gray-600">Approved</span>
                  <span className="text-sm font-bold text-emerald-600">{stats?.approvedApplications || 0}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-gray-600">Rejected</span>
                  <span className="text-sm font-bold text-red-600">{stats?.rejectedApplications || 0}</span>
                </div>
                <div className="border-t border-gray-100 pt-3 flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-900">Total</span>
                  <span className="text-sm font-bold text-gray-900">
                    {(stats?.pendingApplications || 0) + (stats?.approvedApplications || 0) + (stats?.rejectedApplications || 0)}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Stat Card Component ──────────────────────────── */
function StatCard({
  label,
  value,
  icon,
  color,
  highlight,
}: {
  label: string;
  value: number;
  icon: React.ReactNode;
  color: string;
  highlight?: boolean;
}) {
  const colorMap: Record<string, string> = {
    primary: "bg-primary-light text-primary",
    amber: "bg-amber-100 text-amber-600",
    blue: "bg-blue-100 text-blue-600",
    emerald: "bg-emerald-100 text-emerald-600",
  };

  return (
    <div className={`bg-white rounded-2xl border shadow-sm p-5 ${highlight ? "border-amber-200 ring-2 ring-amber-100" : "border-gray-100"}`}>
      <div className="flex items-center justify-between mb-3">
        <div className={`w-11 h-11 rounded-xl flex items-center justify-center ${colorMap[color] || colorMap.primary}`}>
          {icon}
        </div>
        {highlight && (
          <span className="w-2.5 h-2.5 rounded-full bg-amber-400 animate-pulse" />
        )}
      </div>
      <p className="text-2xl font-bold text-gray-900">{value}</p>
      <p className="text-sm text-gray-500 mt-0.5">{label}</p>
    </div>
  );
}
