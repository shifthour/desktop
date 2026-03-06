"use client";

import { useState, useEffect } from "react";
import Image from "next/image";

const messages = [
  "Analyzing your pitch deck...",
  "Extracting key metrics and data...",
  "Identifying company highlights...",
  "Designing page structure...",
  "Crafting hero section...",
  "Building interactive animations...",
  "Generating responsive layout...",
  "Adding finishing touches...",
];

interface ProcessingAnimationProps {
  stage: "generating" | "deploying";
}

export function ProcessingAnimation({ stage }: ProcessingAnimationProps) {
  const [messageIndex, setMessageIndex] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      setMessageIndex((prev) => (prev + 1) % messages.length);
    }, 3000);
    return () => clearInterval(interval);
  }, []);

  const deployMessages = [
    "Creating GitHub repository...",
    "Pushing landing page code...",
    "Deploying to Vercel...",
    "Configuring production build...",
    "Almost there...",
  ];

  const currentMessages = stage === "generating" ? messages : deployMessages;
  const currentMsg = currentMessages[messageIndex % currentMessages.length];

  return (
    <div className="flex flex-col items-center justify-center py-16">
      {/* Animated logo */}
      <div className="relative">
        {/* Outer glow ring */}
        <div className="absolute -inset-8 rounded-full animate-pulse-glow opacity-50" />

        {/* Spinning ring */}
        <div className="absolute -inset-4 rounded-full border-2 border-transparent border-t-brand-purple border-r-brand-orange animate-spin-slow" />

        {/* Logo */}
        <div className="relative h-20 w-20 rounded-2xl overflow-hidden">
          <Image
            src="/logo.png"
            alt="Processing"
            fill
            className="object-contain animate-float"
          />
        </div>
      </div>

      {/* Status text */}
      <div className="mt-10 text-center">
        <h3 className="text-xl font-semibold gradient-text">
          {stage === "generating"
            ? "Generating Landing Page"
            : "Deploying to Web"}
        </h3>
        <p
          className="mt-3 text-sm text-gray-400 transition-opacity duration-500"
          key={currentMsg}
        >
          {currentMsg}
        </p>
      </div>

      {/* Progress bar */}
      <div className="mt-8 h-1 w-64 overflow-hidden rounded-full bg-dark-card">
        <div className="h-full gradient-brand animate-shimmer rounded-full" style={{ width: "100%" }} />
      </div>

      <p className="mt-6 text-[11px] text-gray-600">
        This may take up to 2 minutes
      </p>
    </div>
  );
}
