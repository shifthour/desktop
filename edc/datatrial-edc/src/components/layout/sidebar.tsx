"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import {
  LayoutDashboard,
  FolderKanban,
  Building2,
  Users,
  FileText,
  UserCircle,
  Package,
  Truck,
  ClipboardList,
  AlertCircle,
  FileBarChart,
  History,
  Settings,
  ChevronDown,
} from "lucide-react";
import { useState } from "react";

interface NavItem {
  title: string;
  href?: string;
  icon: React.ElementType;
  children?: { title: string; href: string }[];
}

const navItems: NavItem[] = [
  {
    title: "Dashboard",
    href: "/dashboard",
    icon: LayoutDashboard,
  },
  {
    title: "Study Management",
    icon: FolderKanban,
    children: [
      { title: "Studies", href: "/studies" },
      { title: "Create Study", href: "/studies/new" },
    ],
  },
  {
    title: "Site Management",
    icon: Building2,
    children: [
      { title: "Sites", href: "/sites" },
      { title: "Site Allocation", href: "/sites/allocation" },
    ],
  },
  {
    title: "User Management",
    icon: Users,
    children: [
      { title: "Users", href: "/users" },
      { title: "Roles", href: "/users/roles" },
      { title: "Study Assignments", href: "/users/assignments" },
    ],
  },
  {
    title: "CRF Builder",
    icon: FileText,
    children: [
      { title: "Forms", href: "/crf/forms" },
      { title: "Visits", href: "/crf/visits" },
      { title: "LOV Lists", href: "/crf/lov" },
      { title: "Edit Checks", href: "/crf/edit-checks" },
    ],
  },
  {
    title: "Subject Management",
    icon: UserCircle,
    children: [
      { title: "Subjects", href: "/subjects" },
      { title: "Data Entry", href: "/subjects/data-entry" },
      { title: "Randomization", href: "/subjects/randomization" },
    ],
  },
  {
    title: "IWRS",
    icon: Package,
    children: [
      { title: "Kit Definitions", href: "/iwrs/kits" },
      { title: "IP Inventory", href: "/iwrs/inventory" },
      { title: "Kit Requests", href: "/iwrs/requests" },
    ],
  },
  {
    title: "Shipments",
    icon: Truck,
    children: [
      { title: "All Shipments", href: "/shipments" },
      { title: "Create Shipment", href: "/shipments/new" },
    ],
  },
  {
    title: "Data Review",
    icon: ClipboardList,
    children: [
      { title: "SDV Review", href: "/review/sdv" },
      { title: "DM Review", href: "/review/dm" },
      { title: "QA Review", href: "/review/qa" },
      { title: "Data Lock", href: "/review/lock" },
    ],
  },
  {
    title: "Query Management",
    href: "/queries",
    icon: AlertCircle,
  },
  {
    title: "Reports",
    icon: FileBarChart,
    children: [
      { title: "Subject Status", href: "/reports/subject-status" },
      { title: "Visit Status", href: "/reports/visit-status" },
      { title: "Query Report", href: "/reports/queries" },
      { title: "IP Report", href: "/reports/ip" },
      { title: "Data Export", href: "/reports/export" },
    ],
  },
  {
    title: "Audit Trail",
    href: "/audit",
    icon: History,
  },
  {
    title: "Settings",
    href: "/settings",
    icon: Settings,
  },
];

export function Sidebar() {
  const pathname = usePathname();
  const [expandedItems, setExpandedItems] = useState<string[]>(["Study Management"]);

  const toggleExpand = (title: string) => {
    setExpandedItems((prev) =>
      prev.includes(title) ? prev.filter((t) => t !== title) : [...prev, title]
    );
  };

  return (
    <aside className="fixed left-0 top-0 z-40 h-screen w-64 bg-slate-900 text-white">
      <div className="flex h-16 items-center border-b border-slate-700 px-6">
        <Link href="/dashboard" className="flex items-center space-x-2">
          <div className="h-8 w-8 rounded bg-blue-500 flex items-center justify-center">
            <FileText className="h-5 w-5" />
          </div>
          <span className="text-lg font-semibold">DataTrial EDC</span>
        </Link>
      </div>

      <nav className="h-[calc(100vh-4rem)] overflow-y-auto p-4">
        <ul className="space-y-1">
          {navItems.map((item) => (
            <li key={item.title}>
              {item.href && !item.children ? (
                <Link
                  href={item.href}
                  className={cn(
                    "flex items-center space-x-3 rounded-md px-3 py-2 text-sm transition-colors",
                    pathname === item.href
                      ? "bg-blue-600 text-white"
                      : "text-slate-300 hover:bg-slate-800 hover:text-white"
                  )}
                >
                  <item.icon className="h-5 w-5" />
                  <span>{item.title}</span>
                </Link>
              ) : (
                <div>
                  <button
                    onClick={() => toggleExpand(item.title)}
                    className={cn(
                      "flex w-full items-center justify-between rounded-md px-3 py-2 text-sm transition-colors",
                      item.children?.some((child) => pathname === child.href)
                        ? "bg-slate-800 text-white"
                        : "text-slate-300 hover:bg-slate-800 hover:text-white"
                    )}
                  >
                    <div className="flex items-center space-x-3">
                      <item.icon className="h-5 w-5" />
                      <span>{item.title}</span>
                    </div>
                    <ChevronDown
                      className={cn(
                        "h-4 w-4 transition-transform",
                        expandedItems.includes(item.title) && "rotate-180"
                      )}
                    />
                  </button>
                  {expandedItems.includes(item.title) && item.children && (
                    <ul className="mt-1 space-y-1 pl-10">
                      {item.children.map((child) => (
                        <li key={child.href}>
                          <Link
                            href={child.href}
                            className={cn(
                              "block rounded-md px-3 py-1.5 text-sm transition-colors",
                              pathname === child.href
                                ? "bg-blue-600 text-white"
                                : "text-slate-400 hover:bg-slate-800 hover:text-white"
                            )}
                          >
                            {child.title}
                          </Link>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              )}
            </li>
          ))}
        </ul>
      </nav>
    </aside>
  );
}
