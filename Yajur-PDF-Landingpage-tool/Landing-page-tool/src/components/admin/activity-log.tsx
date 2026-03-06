"use client";

import { useState, useEffect } from "react";
import type { ActivityLogEntry } from "@/lib/types";

const actionLabels: Record<string, { label: string; color: string }> = {
  user_login: { label: "Login", color: "text-blue-400" },
  user_created: { label: "User Created", color: "text-green-400" },
  user_updated: { label: "User Updated", color: "text-yellow-400" },
  user_deactivated: { label: "User Deactivated", color: "text-red-400" },
  project_created: { label: "Project Created", color: "text-green-400" },
  project_updated: { label: "Project Updated", color: "text-yellow-400" },
  project_generated: { label: "Page Generated", color: "text-brand-purple-light" },
  project_deployed: { label: "Deployed", color: "text-green-400" },
  project_deleted: { label: "Project Deleted", color: "text-red-400" },
  pdf_uploaded: { label: "PDF Uploaded", color: "text-blue-400" },
};

export function ActivityLog() {
  const [activities, setActivities] = useState<ActivityLogEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchActivity = async () => {
      try {
        const res = await fetch("/api/admin/activity?limit=50");
        if (!res.ok) throw new Error("Failed to fetch");
        const data = await res.json();
        setActivities(data.activities);
      } catch {
        console.error("Failed to load activity log");
      } finally {
        setLoading(false);
      }
    };
    fetchActivity();
  }, []);

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-brand-purple border-t-transparent" />
      </div>
    );
  }

  return (
    <div>
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-white">Activity Log</h3>
        <p className="text-sm text-gray-500 mt-1">
          Recent actions across the platform
        </p>
      </div>

      <div className="glass-card rounded-2xl overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="border-b border-dark-border/50">
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Time
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                User
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Action
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Resource
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Details
              </th>
            </tr>
          </thead>
          <tbody>
            {activities.map((entry) => {
              const actionConfig = actionLabels[entry.action] || {
                label: entry.action,
                color: "text-gray-400",
              };
              const userProfile = entry.user_profile as
                | { full_name?: string; email?: string }
                | undefined;

              return (
                <tr
                  key={entry.id}
                  className="border-b border-dark-border/30 hover:bg-dark-hover/50 transition-colors"
                >
                  <td className="px-5 py-3 text-xs text-gray-500 whitespace-nowrap">
                    {new Date(entry.created_at).toLocaleString()}
                  </td>
                  <td className="px-5 py-3 text-sm text-gray-300">
                    {userProfile?.full_name || userProfile?.email || "Unknown"}
                  </td>
                  <td className="px-5 py-3">
                    <span
                      className={`text-xs font-semibold ${actionConfig.color}`}
                    >
                      {actionConfig.label}
                    </span>
                  </td>
                  <td className="px-5 py-3 text-xs text-gray-500">
                    {entry.resource_type && (
                      <span className="capitalize">
                        {entry.resource_type}
                      </span>
                    )}
                  </td>
                  <td className="px-5 py-3 text-xs text-gray-600 max-w-[200px] truncate">
                    {entry.details && Object.keys(entry.details).length > 0
                      ? JSON.stringify(entry.details)
                      : "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {activities.length === 0 && (
          <div className="py-12 text-center text-sm text-gray-500">
            No activity recorded yet
          </div>
        )}
      </div>
    </div>
  );
}
