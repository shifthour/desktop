"use client";

import { type ResearchMode } from "@/lib/types";

interface ResearchModeSelectorProps {
  value: ResearchMode;
  onChange: (mode: ResearchMode) => void;
}

export function ResearchModeSelector({
  value,
  onChange,
}: ResearchModeSelectorProps) {
  const isPdfOnly = value === "pdf_only";
  const isDeepResearch = value === "deep_research";

  const handleToggle = (selected: ResearchMode) => {
    // Selecting one automatically deselects the other
    onChange(selected);
  };

  return (
    <div>
      <label className="block text-sm font-semibold text-gray-300 mb-1.5">
        Data Source
      </label>
      <p className="text-xs text-gray-500 mb-4">
        Choose how the landing page content should be generated
      </p>

      <div className="space-y-3">
        {/* PDF Data Only Toggle */}
        <button
          type="button"
          onClick={() => handleToggle("pdf_only")}
          className={`w-full flex items-center gap-4 rounded-xl border-2 px-4 py-3.5 text-left transition-all duration-200 ${
            isPdfOnly
              ? "border-brand-purple/60 bg-brand-purple/8"
              : "border-dark-border bg-dark-bg/30 hover:border-gray-600"
          }`}
        >
          {/* Toggle Switch */}
          <div
            className={`relative flex-shrink-0 h-6 w-11 rounded-full transition-colors duration-200 ${
              isPdfOnly ? "bg-brand-purple" : "bg-gray-700"
            }`}
          >
            <div
              className={`absolute top-0.5 h-5 w-5 rounded-full bg-white shadow-md transition-transform duration-200 ${
                isPdfOnly ? "translate-x-[22px]" : "translate-x-0.5"
              }`}
            />
          </div>

          {/* Icon */}
          <div
            className={`flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-lg transition-colors ${
              isPdfOnly
                ? "bg-brand-purple/20 text-brand-purple"
                : "bg-dark-card text-gray-600 border border-dark-border"
            }`}
          >
            <svg
              className="h-4.5 w-4.5"
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
          </div>

          {/* Text */}
          <div className="flex-1 min-w-0">
            <span
              className={`text-sm font-semibold block ${
                isPdfOnly ? "text-white" : "text-gray-400"
              }`}
            >
              PDF Data Only
            </span>
            <span className="text-[11px] text-gray-500 block mt-0.5">
              Uses only PDF content. No assumptions, no external data.
            </span>
          </div>

          {/* Active badge */}
          {isPdfOnly && (
            <span className="flex-shrink-0 text-[10px] font-bold uppercase tracking-wider text-brand-purple bg-brand-purple/15 px-2 py-0.5 rounded-full">
              Active
            </span>
          )}
        </button>

        {/* Deep Research Toggle */}
        <button
          type="button"
          onClick={() => handleToggle("deep_research")}
          className={`w-full flex items-center gap-4 rounded-xl border-2 px-4 py-3.5 text-left transition-all duration-200 ${
            isDeepResearch
              ? "border-brand-orange/60 bg-brand-orange/8"
              : "border-dark-border bg-dark-bg/30 hover:border-gray-600"
          }`}
        >
          {/* Toggle Switch */}
          <div
            className={`relative flex-shrink-0 h-6 w-11 rounded-full transition-colors duration-200 ${
              isDeepResearch ? "bg-brand-orange" : "bg-gray-700"
            }`}
          >
            <div
              className={`absolute top-0.5 h-5 w-5 rounded-full bg-white shadow-md transition-transform duration-200 ${
                isDeepResearch ? "translate-x-[22px]" : "translate-x-0.5"
              }`}
            />
          </div>

          {/* Icon */}
          <div
            className={`flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-lg transition-colors ${
              isDeepResearch
                ? "bg-brand-orange/20 text-brand-orange"
                : "bg-dark-card text-gray-600 border border-dark-border"
            }`}
          >
            <svg
              className="h-4.5 w-4.5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={1.5}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M12 21a9.004 9.004 0 008.716-6.747M12 21a9.004 9.004 0 01-8.716-6.747M12 21c2.485 0 4.5-4.03 4.5-9S14.485 3 12 3m0 18c-2.485 0-4.5-4.03-4.5-9S9.515 3 12 3m0 0a8.997 8.997 0 017.843 4.582M12 3a8.997 8.997 0 00-7.843 4.582m15.686 0A11.953 11.953 0 0112 10.5c-2.998 0-5.74-1.1-7.843-2.918m15.686 0A8.959 8.959 0 0121 12c0 .778-.099 1.533-.284 2.253m0 0A17.919 17.919 0 0112 16.5c-3.162 0-6.133-.815-8.716-2.247m0 0A9.015 9.015 0 013 12c0-1.605.42-3.113 1.157-4.418"
              />
            </svg>
          </div>

          {/* Text */}
          <div className="flex-1 min-w-0">
            <span
              className={`text-sm font-semibold block ${
                isDeepResearch ? "text-white" : "text-gray-400"
              }`}
            >
              Deep Research
            </span>
            <span className="text-[11px] text-gray-500 block mt-0.5">
              PDF data enriched with web research from Google.
            </span>
          </div>

          {/* Active badge */}
          {isDeepResearch && (
            <span className="flex-shrink-0 text-[10px] font-bold uppercase tracking-wider text-brand-orange bg-brand-orange/15 px-2 py-0.5 rounded-full">
              Active
            </span>
          )}
        </button>
      </div>
    </div>
  );
}
