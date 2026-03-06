"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Header } from "@/components/ui/header";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { ProgressSteps } from "@/components/ui/progress-steps";
import { PdfUploader } from "@/components/project/pdf-uploader";
import { PromptEditor } from "@/components/project/prompt-editor";
import { ProcessingAnimation } from "@/components/project/processing-animation";
import { DeploymentResult } from "@/components/project/deployment-result";

const steps = [
  { label: "Upload", icon: "1" },
  { label: "Configure", icon: "2" },
  { label: "Generate", icon: "3" },
  { label: "Deploy", icon: "4" },
];

export default function NewProjectPage() {
  const router = useRouter();
  const [currentStep, setCurrentStep] = useState(0);

  // Form state
  const [pdfPath, setPdfPath] = useState("");
  const [pdfOriginalName, setPdfOriginalName] = useState("");
  const [prompt, setPrompt] = useState("");
  const [projectName, setProjectName] = useState("");
  const [projectId, setProjectId] = useState("");

  // Processing state
  const [generating, setGenerating] = useState(false);
  const [deploying, setDeploying] = useState(false);
  const [generatedHtml, setGeneratedHtml] = useState("");
  const [error, setError] = useState<string | null>(null);

  // Result state
  const [vercelUrl, setVercelUrl] = useState("");
  const [githubRepoUrl, setGithubRepoUrl] = useState("");

  const handleUploadComplete = (data: {
    pdfPath: string;
    pdfOriginalName: string;
    defaultPrompt: string;
  }) => {
    setPdfPath(data.pdfPath);
    setPdfOriginalName(data.pdfOriginalName);
    setPrompt(data.defaultPrompt);
    setCurrentStep(1);
  };

  const handleGenerate = async () => {
    if (!projectName.trim()) {
      setError("Please enter a project name");
      return;
    }
    setError(null);
    setGenerating(true);
    setCurrentStep(2);

    try {
      // Create the project first
      const createRes = await fetch("/api/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: projectName,
          prompt,
          pdfPath,
          pdfOriginalName,
        }),
      });

      if (!createRes.ok) {
        const data = await createRes.json();
        throw new Error(data.error || "Failed to create project");
      }

      const project = await createRes.json();
      setProjectId(project.id);

      // Generate the landing page
      const genRes = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ projectId: project.id }),
      });

      if (!genRes.ok) {
        const data = await genRes.json();
        throw new Error(data.error || "Generation failed");
      }

      const genData = await genRes.json();
      setGeneratedHtml(genData.html);
      setGenerating(false);
      setCurrentStep(3);
    } catch (err) {
      setGenerating(false);
      setError(err instanceof Error ? err.message : "An error occurred");
      setCurrentStep(1);
    }
  };

  const handleDeploy = async () => {
    setError(null);
    setDeploying(true);

    try {
      const res = await fetch("/api/deploy", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ projectId }),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Deployment failed");
      }

      const data = await res.json();
      setVercelUrl(data.vercelUrl);
      setGithubRepoUrl(data.githubRepoUrl);
      setDeploying(false);
      setCurrentStep(4);
    } catch (err) {
      setDeploying(false);
      setError(err instanceof Error ? err.message : "Deployment failed");
    }
  };

  return (
    <div className="min-h-screen">
      <Header />

      <main className="mx-auto max-w-3xl px-6 py-10">
        {/* Page title */}
        <div className="mb-8 text-center">
          <h1 className="text-3xl font-bold text-white">
            Create New Project
          </h1>
          <p className="mt-2 text-sm text-gray-500">
            Upload a pitch deck and generate an investor landing page
          </p>
        </div>

        {/* Progress steps */}
        <div className="mb-10">
          <ProgressSteps steps={steps} currentStep={currentStep} />
        </div>

        {/* Step content */}
        {/* Step 0: Upload PDF */}
        {currentStep === 0 && (
          <Card className="animate-in fade-in">
            <h2 className="mb-6 text-lg font-semibold text-white">
              Upload Pitch Deck
            </h2>
            <PdfUploader onUploadComplete={handleUploadComplete} />
          </Card>
        )}

        {/* Step 1: Configure */}
        {currentStep === 1 && (
          <div className="space-y-6 animate-in fade-in">
            <Card>
              <h2 className="mb-6 text-lg font-semibold text-white">
                Configure Project
              </h2>

              {/* Project name */}
              <div className="mb-6">
                <label className="mb-2 block text-sm font-semibold text-gray-300">
                  Project Name
                </label>
                <input
                  type="text"
                  value={projectName}
                  onChange={(e) => setProjectName(e.target.value)}
                  placeholder="e.g., TechStartup Series A"
                  className="w-full rounded-xl border border-dark-border bg-dark-bg/50 px-4 py-3 text-sm text-white placeholder-gray-600 transition-all focus:border-brand-purple/50 focus:outline-none focus:ring-1 focus:ring-brand-purple/30"
                />
                <p className="mt-1.5 text-[11px] text-gray-600">
                  This will be used as the project identifier and Vercel deployment name
                </p>
              </div>

              {/* Prompt editor */}
              <PromptEditor value={prompt} onChange={setPrompt} />

              {/* Uploaded file info */}
              <div className="mt-6 flex items-center gap-2 text-xs text-gray-500">
                <svg className="h-4 w-4 text-brand-purple" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                <span>{pdfOriginalName}</span>
              </div>
            </Card>

            {error && (
              <div className="rounded-xl border border-red-500/20 bg-red-500/5 p-4">
                <p className="text-sm text-red-400">{error}</p>
              </div>
            )}

            <div className="flex items-center justify-between">
              <Button variant="ghost" onClick={() => setCurrentStep(0)}>
                ← Back
              </Button>
              <Button variant="primary" size="lg" onClick={handleGenerate}>
                Generate Landing Page
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </Button>
            </div>
          </div>
        )}

        {/* Step 2: Generating */}
        {currentStep === 2 && generating && (
          <Card>
            <ProcessingAnimation stage="generating" />
          </Card>
        )}

        {/* Step 3: Preview & Deploy */}
        {currentStep === 3 && !deploying && (
          <div className="space-y-6 animate-in fade-in">
            <Card>
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-semibold text-white">
                  Preview Landing Page
                </h2>
                <span className="text-[10px] font-semibold uppercase tracking-wider text-green-400 bg-green-500/10 rounded-full px-3 py-1 border border-green-500/20">
                  Generated
                </span>
              </div>
              <div className="relative overflow-hidden rounded-xl border border-dark-border bg-white">
                <iframe
                  srcDoc={generatedHtml}
                  className="h-[500px] w-full"
                  title="Landing Page Preview"
                  sandbox="allow-scripts"
                />
              </div>
            </Card>

            {error && (
              <div className="rounded-xl border border-red-500/20 bg-red-500/5 p-4">
                <p className="text-sm text-red-400">{error}</p>
              </div>
            )}

            <div className="flex items-center justify-between">
              <Button
                variant="ghost"
                onClick={() => {
                  setCurrentStep(1);
                  setError(null);
                }}
              >
                ← Edit Prompt
              </Button>
              <Button variant="primary" size="lg" onClick={handleDeploy}>
                Deploy to Vercel
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                </svg>
              </Button>
            </div>
          </div>
        )}

        {/* Deploying state */}
        {deploying && (
          <Card>
            <ProcessingAnimation stage="deploying" />
          </Card>
        )}

        {/* Step 4: Success */}
        {currentStep === 4 && vercelUrl && (
          <Card>
            <DeploymentResult
              vercelUrl={vercelUrl}
              githubRepoUrl={githubRepoUrl}
            />
          </Card>
        )}
      </main>
    </div>
  );
}
