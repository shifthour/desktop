"use client";

import Link from "next/link";

interface ToolCardProps {
  title: string;
  description: string;
  icon: "pdf" | "drive";
  href: string;
  available?: boolean;
  comingSoon?: boolean;
}

function PdfIcon() {
  return (
    <svg
      className="h-8 w-8"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.5}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z"
      />
    </svg>
  );
}

function DriveIcon() {
  return (
    <svg
      className="h-8 w-8"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.5}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M2.25 12.75V12A2.25 2.25 0 014.5 9.75h15A2.25 2.25 0 0121.75 12v.75m-8.69-6.44l-2.12-2.12a1.5 1.5 0 00-1.061-.44H4.5A2.25 2.25 0 002.25 6v12a2.25 2.25 0 002.25 2.25h15A2.25 2.25 0 0021.75 18V9a2.25 2.25 0 00-2.25-2.25h-5.379a1.5 1.5 0 01-1.06-.44z"
      />
    </svg>
  );
}

export function ToolCard({
  title,
  description,
  icon,
  href,
  available = true,
  comingSoon = false,
}: ToolCardProps) {
  const IconComponent = icon === "pdf" ? PdfIcon : DriveIcon;

  if (!available) {
    return (
      <div className="group relative flex flex-col rounded-2xl border border-dark-border/50 bg-dark-card/30 p-6 min-h-[220px] opacity-60 cursor-not-allowed">
        {comingSoon && (
          <span className="absolute top-4 right-4 inline-flex items-center rounded-full bg-brand-orange/10 border border-brand-orange/20 px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-brand-orange">
            Coming Soon
          </span>
        )}
        <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-dark-hover/50 text-gray-500 mb-4">
          <IconComponent />
        </div>
        <h3 className="text-base font-semibold text-gray-400 mb-2">{title}</h3>
        <p className="text-sm text-gray-600 leading-relaxed">{description}</p>
      </div>
    );
  }

  return (
    <Link
      href={href}
      className="group relative flex flex-col rounded-2xl border border-dark-border/50 bg-dark-card/50 p-6 min-h-[220px] transition-all duration-300 hover:border-brand-purple/40 hover:bg-dark-card/80 hover:shadow-xl hover:shadow-brand-purple/5"
    >
      <div className="flex h-14 w-14 items-center justify-center rounded-xl gradient-brand text-white mb-4 transition-transform group-hover:scale-110 group-hover:shadow-lg group-hover:shadow-brand-purple/25">
        <IconComponent />
      </div>
      <h3 className="text-base font-semibold text-white mb-2 group-hover:gradient-text transition-colors">
        {title}
      </h3>
      <p className="text-sm text-gray-500 leading-relaxed mb-4">{description}</p>
      <div className="mt-auto flex items-center text-sm font-medium text-brand-purple-light group-hover:text-white transition-colors">
        Get started
        <svg
          className="ml-1.5 h-4 w-4 transition-transform group-hover:translate-x-1"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M13 7l5 5m0 0l-5 5m5-5H6"
          />
        </svg>
      </div>
    </Link>
  );
}
