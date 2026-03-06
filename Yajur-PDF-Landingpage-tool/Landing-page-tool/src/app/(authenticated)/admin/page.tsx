"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { UserManagement } from "@/components/admin/user-management";
import { AllProjectsList } from "@/components/admin/all-projects-list";
import { ActivityLog } from "@/components/admin/activity-log";

type Tab = "users" | "projects" | "activity";

export default function AdminPage() {
  const router = useRouter();
  const [activeTab, setActiveTab] = useState<Tab>("users");
  const [isAdmin, setIsAdmin] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const checkAdmin = async () => {
      try {
        // Use our admin users API to check if current user is admin
        // If this returns 403, user is not admin; if 401, not logged in
        const res = await fetch("/api/admin/users");
        if (res.status === 401) {
          router.push("/login");
          return;
        }
        if (res.status === 403 || !res.ok) {
          router.push("/");
          return;
        }
        setIsAdmin(true);
      } catch {
        router.push("/");
      } finally {
        setLoading(false);
      }
    };

    checkAdmin();
  }, [router]);

  if (loading) {
    return (
      <div className="flex justify-center py-20">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-brand-purple border-t-transparent" />
      </div>
    );
  }

  if (!isAdmin) return null;

  const tabs: { key: Tab; label: string; icon: React.ReactNode }[] = [
    {
      key: "users",
      label: "Users",
      icon: (
        <svg
          className="h-4 w-4"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197m13.5-9a2.5 2.5 0 11-5 0 2.5 2.5 0 015 0z"
          />
        </svg>
      ),
    },
    {
      key: "projects",
      label: "All Projects",
      icon: (
        <svg
          className="h-4 w-4"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
          />
        </svg>
      ),
    },
    {
      key: "activity",
      label: "Activity Log",
      icon: (
        <svg
          className="h-4 w-4"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01"
          />
        </svg>
      ),
    },
  ];

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-white">
          Admin <span className="gradient-text">Panel</span>
        </h1>
        <p className="mt-2 text-sm text-gray-500">
          Manage users, view all projects, and monitor activity
        </p>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-1 mb-8 p-1 glass-card rounded-xl w-fit">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeTab === tab.key
                ? "gradient-brand text-white shadow-lg shadow-brand-purple/20"
                : "text-gray-400 hover:text-white hover:bg-dark-hover"
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {activeTab === "users" && <UserManagement />}
      {activeTab === "projects" && <AllProjectsList />}
      {activeTab === "activity" && <ActivityLog />}
    </div>
  );
}
