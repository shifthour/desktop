"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Users,
  FolderKanban,
  Building2,
  UserCircle,
  AlertCircle,
  Package,
  CheckCircle2,
  Clock
} from "lucide-react";

const stats = [
  {
    title: "Active Studies",
    value: "0",
    icon: FolderKanban,
    color: "text-blue-600",
    bgColor: "bg-blue-100",
  },
  {
    title: "Total Sites",
    value: "0",
    icon: Building2,
    color: "text-green-600",
    bgColor: "bg-green-100",
  },
  {
    title: "Active Users",
    value: "1",
    icon: Users,
    color: "text-purple-600",
    bgColor: "bg-purple-100",
  },
  {
    title: "Total Subjects",
    value: "0",
    icon: UserCircle,
    color: "text-orange-600",
    bgColor: "bg-orange-100",
  },
  {
    title: "Open Queries",
    value: "0",
    icon: AlertCircle,
    color: "text-red-600",
    bgColor: "bg-red-100",
  },
  {
    title: "Pending SDV",
    value: "0",
    icon: Clock,
    color: "text-yellow-600",
    bgColor: "bg-yellow-100",
  },
  {
    title: "Data Locked",
    value: "0",
    icon: CheckCircle2,
    color: "text-teal-600",
    bgColor: "bg-teal-100",
  },
  {
    title: "IP Kits Available",
    value: "0",
    icon: Package,
    color: "text-indigo-600",
    bgColor: "bg-indigo-100",
  },
];

export default function DashboardPage() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Dashboard</h2>
        <p className="text-gray-500">Welcome to DataTrial EDC/IWRS System</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map((stat) => (
          <Card key={stat.title}>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-gray-500">
                {stat.title}
              </CardTitle>
              <div className={`p-2 rounded-full ${stat.bgColor}`}>
                <stat.icon className={`h-4 w-4 ${stat.color}`} />
              </div>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Quick Actions</CardTitle>
          </CardHeader>
          <CardContent className="grid grid-cols-2 gap-3">
            <a
              href="/studies/new"
              className="flex items-center space-x-3 p-3 rounded-lg border border-gray-200 hover:bg-gray-50 transition-colors"
            >
              <FolderKanban className="h-5 w-5 text-blue-600" />
              <span className="text-sm font-medium">Create Study</span>
            </a>
            <a
              href="/users"
              className="flex items-center space-x-3 p-3 rounded-lg border border-gray-200 hover:bg-gray-50 transition-colors"
            >
              <Users className="h-5 w-5 text-purple-600" />
              <span className="text-sm font-medium">Manage Users</span>
            </a>
            <a
              href="/subjects"
              className="flex items-center space-x-3 p-3 rounded-lg border border-gray-200 hover:bg-gray-50 transition-colors"
            >
              <UserCircle className="h-5 w-5 text-orange-600" />
              <span className="text-sm font-medium">View Subjects</span>
            </a>
            <a
              href="/queries"
              className="flex items-center space-x-3 p-3 rounded-lg border border-gray-200 hover:bg-gray-50 transition-colors"
            >
              <AlertCircle className="h-5 w-5 text-red-600" />
              <span className="text-sm font-medium">Manage Queries</span>
            </a>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Recent Activity</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <p className="text-sm text-gray-500 text-center py-8">
                No recent activity to display.
              </p>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* System Info */}
      <Card>
        <CardHeader>
          <CardTitle>System Information</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <p className="text-gray-500">Version</p>
              <p className="font-medium">1.0.0</p>
            </div>
            <div>
              <p className="text-gray-500">Compliance</p>
              <p className="font-medium">21 CFR Part 11</p>
            </div>
            <div>
              <p className="text-gray-500">Session Timeout</p>
              <p className="font-medium">30 minutes</p>
            </div>
            <div>
              <p className="text-gray-500">Database</p>
              <p className="font-medium">SQLite (Dev)</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
