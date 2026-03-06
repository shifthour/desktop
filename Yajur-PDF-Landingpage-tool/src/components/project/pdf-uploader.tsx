"use client";

import { useState, useRef, useCallback } from "react";

interface PdfUploaderProps {
  onUploadComplete: (data: {
    pdfPath: string;
    pdfOriginalName: string;
    defaultPrompt: string;
  }) => void;
}

export function PdfUploader({ onUploadComplete }: PdfUploaderProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadedFile, setUploadedFile] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleFile = useCallback(
    async (file: File) => {
      if (!file.name.toLowerCase().endsWith(".pdf")) {
        setError("Please upload a PDF file");
        return;
      }
      if (file.size > 32 * 1024 * 1024) {
        setError("File must be under 32MB");
        return;
      }

      setError(null);
      setUploading(true);

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
        setUploadedFile(file.name);
        onUploadComplete(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Upload failed");
      } finally {
        setUploading(false);
      }
    },
    [onUploadComplete]
  );

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);
      const file = e.dataTransfer.files[0];
      if (file) handleFile(file);
    },
    [handleFile]
  );

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);

  if (uploadedFile) {
    return (
      <div className="flex items-center gap-4 rounded-2xl border border-green-500/20 bg-green-500/5 p-6">
        <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-green-500/10">
          <svg className="h-6 w-6 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
          </svg>
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold text-green-400">PDF Uploaded</p>
          <p className="mt-0.5 truncate text-xs text-gray-400">{uploadedFile}</p>
        </div>
        <button
          onClick={() => {
            setUploadedFile(null);
            if (inputRef.current) inputRef.current.value = "";
          }}
          className="text-xs text-gray-500 hover:text-white transition-colors"
        >
          Change
        </button>
      </div>
    );
  }

  return (
    <div>
      <div
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onClick={() => inputRef.current?.click()}
        className={`relative flex cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed p-12 transition-all duration-300 ${
          isDragging
            ? "border-brand-purple bg-brand-purple/5 shadow-lg shadow-brand-purple/10"
            : "border-dark-border hover:border-brand-purple/40 hover:bg-dark-card/30"
        } ${uploading ? "pointer-events-none opacity-60" : ""}`}
      >
        {uploading ? (
          <>
            <div className="h-12 w-12 rounded-full border-2 border-brand-purple border-t-transparent animate-spin" />
            <p className="mt-4 text-sm font-medium text-gray-300">
              Uploading...
            </p>
          </>
        ) : (
          <>
            <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-dark-card border border-dark-border">
              <svg className="h-8 w-8 text-brand-purple" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
              </svg>
            </div>
            <p className="mt-4 text-sm font-semibold text-gray-200">
              {isDragging ? "Drop your PDF here" : "Drag & drop your pitch deck PDF"}
            </p>
            <p className="mt-1 text-xs text-gray-500">
              or click to browse — PDF files up to 32MB
            </p>
          </>
        )}
        <input
          ref={inputRef}
          type="file"
          accept=".pdf"
          className="hidden"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) handleFile(file);
          }}
        />
      </div>
      {error && (
        <p className="mt-3 text-sm text-red-400 flex items-center gap-2">
          <svg className="h-4 w-4 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          {error}
        </p>
      )}
    </div>
  );
}
