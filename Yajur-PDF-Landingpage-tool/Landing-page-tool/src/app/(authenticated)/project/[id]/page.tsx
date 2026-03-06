"use client";

import { useState, useEffect, use } from "react";
import Link from "next/link";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ProcessingAnimation } from "@/components/project/processing-animation";
import type { Project } from "@/lib/types";

// Safely parse API responses — handles non-JSON error pages (e.g. Vercel timeouts)
async function safeJson(
  res: Response
): Promise<{ ok: boolean; data: Record<string, unknown>; error?: string }> {
  const text = await res.text();
  try {
    const data = JSON.parse(text);
    if (!res.ok) {
      return {
        ok: false,
        data,
        error: data.error || `Request failed (${res.status})`,
      };
    }
    return { ok: true, data };
  } catch {
    return {
      ok: false,
      data: {},
      error:
        res.status === 504
          ? "Request timed out. Please try again."
          : `Server error (${res.status}): ${text.substring(0, 150)}`,
    };
  }
}

export default function ProjectDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const [project, setProject] = useState<Project | null>(null);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState(false);
  const [processingStage, setProcessingStage] = useState<
    "generating" | "deploying"
  >("generating");
  const [error, setError] = useState("");
  const [editInstructions, setEditInstructions] = useState("");

  const fetchProject = async () => {
    try {
      const res = await fetch(`/api/projects/${id}`);
      if (!res.ok) throw new Error("Failed to load project");
      const data = await res.json();
      setProject(data);
    } catch {
      setError("Failed to load project");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchProject();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  const handleEdit = async () => {
    if (!project) return;
    if (!editInstructions.trim()) {
      setError("Please describe what changes you want to make");
      return;
    }

    setError("");
    setProcessing(true);
    setProcessingStage("generating");

    try {
      // Save the edit instructions as the prompt
      await fetch(`/api/projects/${id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: editInstructions }),
      });

      // Generate with edit mode — sends existing HTML + instructions to Claude
      const genRes = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          projectId: id,
          editMode: true,
          editInstructions: editInstructions,
        }),
      });
      const genResult = await safeJson(genRes);
      if (!genResult.ok) {
        throw new Error(genResult.error || "Edit failed");
      }

      // Deploy the updated page
      setProcessingStage("deploying");
      const deployRes = await fetch("/api/deploy", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ projectId: id }),
      });
      const deployResult = await safeJson(deployRes);
      if (!deployResult.ok) {
        throw new Error(deployResult.error || "Deployment failed");
      }

      // Refresh project data
      const res = await fetch(`/api/projects/${id}`);
      const refreshResult = await safeJson(res);
      if (refreshResult.ok) {
        setProject(refreshResult.data as unknown as Project);
        setEditInstructions("");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to edit");
      fetchProject();
    } finally {
      setProcessing(false);
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center py-20">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-brand-purple border-t-transparent" />
      </div>
    );
  }

  if (!project) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-8 text-center">
        <p className="text-gray-400">Project not found</p>
        <Link href="/" className="mt-4 inline-block">
          <Button variant="secondary">Back to Dashboard</Button>
        </Link>
      </div>
    );
  }

  if (processing) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-8">
        <Card>
          <ProcessingAnimation stage={processingStage} />
        </Card>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-4xl px-6 py-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-8">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-2xl font-bold text-white">{project.name}</h1>
            <Badge status={project.status} />
          </div>
          <p className="mt-1 text-sm text-gray-500">
            {project.pdf_original_name}
          </p>
        </div>
        <Link href="/projects">
          <Button variant="secondary" size="sm">
            ← Projects
          </Button>
        </Link>
      </div>

      {/* Deployed URL */}
      {project.vercel_url && (
        <Card className="mb-6">
          <div className="flex items-center justify-between">
            <div>
              <label className="text-[10px] font-semibold uppercase tracking-wider text-green-400">
                Live URL
              </label>
              <a
                href={project.vercel_url}
                target="_blank"
                rel="noopener noreferrer"
                className="mt-1 block text-sm font-mono text-green-300 hover:underline"
              >
                {project.vercel_url}
              </a>
            </div>
            <a
              href={project.vercel_url}
              target="_blank"
              rel="noopener noreferrer"
            >
              <Button variant="primary" size="sm">
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
                    d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
                  />
                </svg>
                Open
              </Button>
            </a>
          </div>
        </Card>
      )}

      {/* Error */}
      {(error || project.error_message) && (
        <div className="mb-6 rounded-xl border border-red-500/20 bg-red-500/5 px-4 py-3 text-sm text-red-400">
          {error || project.error_message}
        </div>
      )}

      {/* Edit Instructions */}
      <Card className="mb-6">
        <h3 className="text-base font-semibold text-white mb-2">
          Edit Landing Page
        </h3>
        <p className="text-xs text-gray-500 mb-4">
          Describe the changes you want. The existing page will be modified —
          not regenerated from scratch.
        </p>

        <textarea
          value={editInstructions}
          onChange={(e) => setEditInstructions(e.target.value)}
          rows={6}
          placeholder={`Examples:\n• "Change the hero section background to dark blue"\n• "Add a team section with founder details"\n• "Make the CTA button text say 'Get Started'"\n• "Fix the chart colors to match our brand orange"\n• "Add more data from the PDF to the market section"`}
          className="w-full resize-y rounded-xl border border-dark-border bg-dark-bg/50 px-4 py-3 text-sm text-gray-200 placeholder-gray-600 transition-all focus:border-brand-purple/50 focus:outline-none focus:ring-1 focus:ring-brand-purple/30 font-mono leading-relaxed"
        />

        <div className="mt-4 flex items-center justify-between">
          <span className="text-[10px] text-gray-600">
            {editInstructions.length} characters
          </span>
          <Button variant="primary" onClick={handleEdit}>
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
                d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
              />
            </svg>
            Apply Changes & Deploy
          </Button>
        </div>
      </Card>

      {/* Preview */}
      {project.generated_html && (
        <Card>
          <h3 className="text-base font-semibold text-white mb-4">
            Current Preview
          </h3>
          <div className="rounded-xl border border-dark-border overflow-hidden">
            <iframe
              srcDoc={project.generated_html}
              className="w-full h-[600px] bg-white"
              sandbox="allow-scripts allow-same-origin"
              title="Landing page preview"
            />
          </div>
        </Card>
      )}
    </div>
  );
}
