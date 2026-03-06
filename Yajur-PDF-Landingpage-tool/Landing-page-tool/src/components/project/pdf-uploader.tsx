"use client";

import { useState, useRef } from "react";

interface PdfUploaderProps {
  onUploadComplete: (data: {
    pdf_storage_path: string;
    pdf_original_name: string;
    defaultPrompt: string;
  }) => void;
}

export function PdfUploader({ onUploadComplete }: PdfUploaderProps) {
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploaded, setUploaded] = useState(false);
  const [fileName, setFileName] = useState("");
  const [error, setError] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFile = async (file: File) => {
    if (!file.name.endsWith(".pdf")) {
      setError("Only PDF files are accepted");
      return;
    }

    if (file.size > 32 * 1024 * 1024) {
      setError("File must be under 32MB");
      return;
    }

    setUploading(true);
    setError("");
    setFileName(file.name);

    try {
      const formData = new FormData();
      formData.append("pdf", file);

      const res = await fetch("/api/upload", {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Upload failed");
      }

      const data = await res.json();
      setUploaded(true);
      onUploadComplete(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed");
      setFileName("");
    } finally {
      setUploading(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) handleFile(file);
  };

  if (uploaded) {
    return (
      <div className="flex flex-col items-center justify-center rounded-2xl border-2 border-green-500/30 bg-green-500/5 p-12">
        <div className="flex h-14 w-14 items-center justify-center rounded-full bg-green-500/20">
          <svg
            className="h-7 w-7 text-green-400"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M5 13l4 4L19 7"
            />
          </svg>
        </div>
        <p className="mt-4 text-sm font-semibold text-green-400">
          PDF Uploaded Successfully
        </p>
        <p className="mt-1 text-xs text-gray-500">{fileName}</p>
      </div>
    );
  }

  return (
    <div
      onDragOver={(e) => {
        e.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      onClick={() => fileInputRef.current?.click()}
      className={`flex flex-col items-center justify-center rounded-2xl border-2 border-dashed p-12 cursor-pointer transition-all duration-300 ${
        dragging
          ? "border-brand-purple bg-brand-purple/5"
          : "border-dark-border hover:border-brand-purple/40 hover:bg-dark-card/50"
      } ${uploading ? "pointer-events-none opacity-60" : ""}`}
    >
      <input
        ref={fileInputRef}
        type="file"
        accept=".pdf"
        onChange={handleInputChange}
        className="hidden"
      />

      {uploading ? (
        <>
          <div className="h-10 w-10 animate-spin rounded-full border-2 border-brand-purple border-t-transparent" />
          <p className="mt-4 text-sm text-gray-400">Uploading...</p>
        </>
      ) : (
        <>
          <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-dark-card border border-dark-border">
            <svg
              className="h-7 w-7 text-gray-500"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
              />
            </svg>
          </div>
          <p className="mt-4 text-sm font-semibold text-gray-300">
            Drop your pitch deck PDF here
          </p>
          <p className="mt-1 text-xs text-gray-600">
            or click to browse (max 32MB)
          </p>
        </>
      )}

      {error && (
        <div className="mt-4 flex items-center gap-2 text-sm text-red-400">
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
              d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          {error}
        </div>
      )}
    </div>
  );
}
