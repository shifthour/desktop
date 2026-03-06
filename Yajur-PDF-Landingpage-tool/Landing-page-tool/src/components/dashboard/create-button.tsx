"use client";

import Link from "next/link";

export function CreateButton() {
  return (
    <Link
      href="/project/new"
      className="group relative flex flex-col items-center justify-center rounded-2xl border-2 border-dashed border-dark-border p-8 transition-all duration-300 hover:border-brand-purple/40 hover:bg-dark-card/50 min-h-[200px]"
    >
      <div className="flex h-16 w-16 items-center justify-center rounded-2xl gradient-brand text-white text-2xl transition-transform group-hover:scale-110 group-hover:shadow-lg group-hover:shadow-brand-purple/25">
        +
      </div>
      <span className="mt-4 text-sm font-semibold text-gray-400 group-hover:text-white transition-colors">
        Create New Project
      </span>
      <span className="mt-1 text-xs text-gray-600">
        Upload a pitch deck PDF
      </span>
    </Link>
  );
}
