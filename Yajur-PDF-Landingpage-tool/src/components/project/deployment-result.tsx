"use client";

import Link from "next/link";
import { Button } from "@/components/ui/button";

interface DeploymentResultProps {
  vercelUrl: string;
  githubRepoUrl: string;
}

export function DeploymentResult({
  vercelUrl,
  githubRepoUrl,
}: DeploymentResultProps) {
  return (
    <div className="flex flex-col items-center py-12">
      {/* Success icon */}
      <div className="relative">
        <div className="absolute -inset-4 rounded-full bg-green-500/10 animate-ping" />
        <div className="relative flex h-16 w-16 items-center justify-center rounded-full bg-green-500/20 border border-green-500/30">
          <svg className="h-8 w-8 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
          </svg>
        </div>
      </div>

      <h3 className="mt-6 text-2xl font-bold text-white">
        Successfully Deployed!
      </h3>
      <p className="mt-2 text-sm text-gray-400">
        Your investor landing page is now live
      </p>

      {/* Vercel URL */}
      <div className="mt-8 w-full max-w-md">
        <div className="rounded-xl border border-green-500/20 bg-green-500/5 p-4">
          <label className="text-[10px] font-semibold uppercase tracking-wider text-green-400">
            Live URL
          </label>
          <div className="mt-2 flex items-center gap-2">
            <a
              href={vercelUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="flex-1 truncate text-sm font-mono text-green-300 hover:underline"
            >
              {vercelUrl}
            </a>
            <button
              onClick={() => navigator.clipboard.writeText(vercelUrl)}
              className="shrink-0 rounded-lg bg-green-500/10 p-2 text-green-400 hover:bg-green-500/20 transition-colors"
              title="Copy URL"
            >
              <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
              </svg>
            </button>
          </div>
        </div>
      </div>

      {/* GitHub URL */}
      <div className="mt-4 w-full max-w-md">
        <div className="rounded-xl border border-dark-border bg-dark-card/50 p-4">
          <label className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">
            GitHub Repository
          </label>
          <div className="mt-2">
            <a
              href={githubRepoUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="truncate text-xs font-mono text-gray-400 hover:text-brand-purple transition-colors hover:underline"
            >
              {githubRepoUrl}
            </a>
          </div>
        </div>
      </div>

      {/* Action buttons */}
      <div className="mt-8 flex items-center gap-4">
        <a href={vercelUrl} target="_blank" rel="noopener noreferrer">
          <Button variant="primary" size="lg">
            <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
            </svg>
            Open Landing Page
          </Button>
        </a>
        <Link href="/">
          <Button variant="secondary" size="lg">
            Back to Dashboard
          </Button>
        </Link>
      </div>
    </div>
  );
}
