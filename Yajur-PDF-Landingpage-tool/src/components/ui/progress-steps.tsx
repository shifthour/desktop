"use client";

interface Step {
  label: string;
  icon: string;
}

interface ProgressStepsProps {
  steps: Step[];
  currentStep: number;
}

export function ProgressSteps({ steps, currentStep }: ProgressStepsProps) {
  return (
    <div className="flex items-center justify-center gap-2">
      {steps.map((step, index) => {
        const isActive = index === currentStep;
        const isComplete = index < currentStep;

        return (
          <div key={step.label} className="flex items-center gap-2">
            <div className="flex items-center gap-2">
              <div
                className={`flex h-8 w-8 items-center justify-center rounded-full text-sm font-semibold transition-all duration-300 ${
                  isActive
                    ? "gradient-brand text-white shadow-lg shadow-brand-purple/30"
                    : isComplete
                      ? "bg-green-500/20 text-green-400 border border-green-500/30"
                      : "bg-dark-card text-gray-500 border border-dark-border"
                }`}
              >
                {isComplete ? (
                  <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                ) : (
                  step.icon
                )}
              </div>
              <span
                className={`text-xs font-medium transition-colors ${
                  isActive ? "text-white" : isComplete ? "text-green-400" : "text-gray-500"
                }`}
              >
                {step.label}
              </span>
            </div>
            {index < steps.length - 1 && (
              <div
                className={`h-px w-8 transition-colors ${
                  isComplete ? "bg-green-500/30" : "bg-dark-border"
                }`}
              />
            )}
          </div>
        );
      })}
    </div>
  );
}
