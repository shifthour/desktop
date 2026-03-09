"use client";

import Link from "next/link";
import Image from "next/image";
import { useState, useEffect } from "react";

/* ─── Types ──────────────────────────────────────────────────── */
type Answer = "yes" | "no" | null;

/* ─── Eligibility Questions ──────────────────────────────────── */
const questions = [
  {
    number: 1,
    question: "Are you currently HCPC registered?",
    subtitle:
      "Health and Care Professions Council registration is mandatory for all physiotherapists practising in the UK.",
    icon: (
      <svg className="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75m-3-7.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285z" />
      </svg>
    ),
  },
  {
    number: 2,
    question: "Are you a member of your relevant professional body?",
    subtitle:
      "Examples: CSP for Physiotherapists, RCOT for OTs, RCSLT for SLTs, BPS for Psychologists.",
    icon: (
      <svg className="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M15 9h3.75M15 12h3.75M15 15h3.75M4.5 19.5h15a2.25 2.25 0 002.25-2.25V6.75A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25v10.5A2.25 2.25 0 004.5 19.5zm6-10.125a1.875 1.875 0 11-3.75 0 1.875 1.875 0 013.75 0zm1.294 6.336a6.721 6.721 0 01-3.17.789 6.721 6.721 0 01-3.168-.789 3.376 3.376 0 016.338 0z" />
      </svg>
    ),
  },
  {
    number: 3,
    question:
      "Do you have at least 2 years of UK work experience in rehabilitation?",
    subtitle:
      "Including neurology, elderly care, community rehab, musculoskeletal, or similar clinical areas.",
    icon: (
      <svg className="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 14.15v4.25c0 1.094-.787 2.036-1.872 2.18-2.087.277-4.216.42-6.378.42s-4.291-.143-6.378-.42c-1.085-.144-1.872-1.086-1.872-2.18v-4.25m16.5 0a2.18 2.18 0 00.75-1.661V8.706c0-1.081-.768-2.015-1.837-2.175a48.114 48.114 0 00-3.413-.387m4.5 8.006c-.194.165-.42.295-.673.38A23.978 23.978 0 0112 15.75c-2.648 0-5.195-.429-7.577-1.22a2.016 2.016 0 01-.673-.38m0 0A2.18 2.18 0 013 12.489V8.706c0-1.081.768-2.015 1.837-2.175a48.111 48.111 0 013.413-.387m7.5 0V5.25A2.25 2.25 0 0013.5 3h-3a2.25 2.25 0 00-2.25 2.25v.894m7.5 0a48.667 48.667 0 00-7.5 0" />
      </svg>
    ),
  },
  {
    number: 4,
    question: "Do you have the right to work in the United Kingdom?",
    subtitle:
      "You must be legally entitled to work in the UK. We may ask for proof during the verification process.",
    icon: (
      <svg className="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M12 21a9.004 9.004 0 008.716-6.747M12 21a9.004 9.004 0 01-8.716-6.747M12 21c2.485 0 4.5-4.03 4.5-9S14.485 3 12 3m0 18c-2.485 0-4.5-4.03-4.5-9S9.515 3 12 3m0 0a8.997 8.997 0 017.843 4.582M12 3a8.997 8.997 0 00-7.843 4.582m15.686 0A11.953 11.953 0 0112 10.5c-2.998 0-5.74-1.1-7.843-2.918m15.686 0A8.959 8.959 0 0121 12c0 .778-.099 1.533-.284 2.253m0 0A17.919 17.919 0 0112 16.5c-3.162 0-6.133-.815-8.716-2.247m0 0A9.015 9.015 0 013 12c0-1.605.42-3.113 1.157-4.418" />
      </svg>
    ),
  },
];

/* ─── Main Page ──────────────────────────────────────────────── */
export default function JoinPage() {
  const [currentStep, setCurrentStep] = useState(0); // 0-3 = questions, 4 = congrats, 5 = form
  const [answers, setAnswers] = useState<Answer[]>([null, null, null, null]);
  const [showIneligible, setShowIneligible] = useState(false);
  const [animating, setAnimating] = useState(false);

  const progress = ((currentStep) / 4) * 100;

  const handleAnswer = (answer: Answer) => {
    if (animating) return;

    const newAnswers = [...answers];
    newAnswers[currentStep] = answer;
    setAnswers(newAnswers);

    if (answer === "no") {
      // Show ineligible immediately
      setTimeout(() => setShowIneligible(true), 300);
      return;
    }

    // answer === "yes" → animate to next
    setAnimating(true);
    setTimeout(() => {
      if (currentStep < 3) {
        setCurrentStep(currentStep + 1);
      } else {
        // All 4 answered Yes → show congrats
        setCurrentStep(4);
      }
      setAnimating(false);
    }, 600);
  };

  const handleStartOver = () => {
    setAnswers([null, null, null, null]);
    setCurrentStep(0);
    setShowIneligible(false);
  };

  const handleGoToForm = () => {
    setCurrentStep(5);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  /* ── Render: Registration Form ──────────────────────────────── */
  if (currentStep === 5) {
    return <RegistrationForm onBack={() => setCurrentStep(4)} />;
  }

  /* ── Render: Congratulations Screen ─────────────────────────── */
  if (currentStep === 4) {
    return (
      <div className="min-h-screen bg-gradient-to-b from-cream to-white">
        {/* Hero Banner */}
        <div className="relative bg-navy overflow-hidden">
          <div className="absolute inset-0">
            <Image src="/pics/overhead-dumbbell.jpg" alt="Physiotherapy" fill className="object-cover opacity-30" />
            <div className="absolute inset-0 bg-gradient-to-r from-navy via-navy/90 to-navy/80" />
          </div>
          <div className="relative max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-16 sm:py-20">
            <Link href="/" className="inline-flex items-center gap-2 text-white/70 hover:text-white transition-colors mb-8 group">
              <svg className="w-4 h-4 transition-transform group-hover:-translate-x-1" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
              </svg>
              Back to Home
            </Link>
            <div className="inline-flex items-center gap-2 bg-primary/20 border border-primary/30 rounded-full px-4 py-1.5 mb-5">
              <span className="w-2 h-2 bg-primary rounded-full animate-pulse" />
              <span className="text-primary-200 text-sm font-medium">Eligibility Confirmed</span>
            </div>
            <h1 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white leading-tight">
              Primary Checks <span className="text-primary-300">Passed!</span>
            </h1>
          </div>
        </div>

        {/* Congrats Content */}
        <div className="max-w-2xl mx-auto px-4 sm:px-6 lg:px-8 -mt-4 relative z-10">
          <div className="bg-white rounded-3xl border border-gray-100 shadow-xl p-8 sm:p-12 text-center animate-slide-up">
            {/* Success icon */}
            <div className="relative mx-auto w-24 h-24 mb-6">
              <div className="absolute inset-0 bg-primary/10 rounded-full animate-ping" style={{ animationDuration: '2s' }} />
              <div className="relative w-24 h-24 bg-gradient-to-br from-primary to-primary-dark rounded-full flex items-center justify-center shadow-lg shadow-primary/25">
                <svg className="w-12 h-12 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                </svg>
              </div>
            </div>

            <h2 className="text-2xl sm:text-3xl font-bold text-navy mb-3">
              Congratulations! 🎉
            </h2>
            <p className="text-gray-500 text-lg leading-relaxed mb-6">
              You have passed all primary eligibility checks. You&apos;re one step away from joining our network of trusted physiotherapists.
            </p>

            {/* Checklist recap */}
            <div className="bg-primary-50 rounded-2xl p-6 mb-8 text-left">
              <h3 className="text-sm font-semibold text-primary-dark uppercase tracking-wider mb-4">Your Results</h3>
              <div className="space-y-3">
                {questions.map((q, i) => (
                  <div key={i} className="flex items-center gap-3">
                    <div className="w-6 h-6 rounded-full bg-primary flex items-center justify-center flex-shrink-0">
                      <svg className="w-3.5 h-3.5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                      </svg>
                    </div>
                    <span className="text-sm text-navy font-medium">{q.question}</span>
                  </div>
                ))}
              </div>
            </div>

            {/* Next step info */}
            <div className="bg-cream rounded-2xl p-6 mb-8 text-left">
              <h3 className="font-semibold text-navy mb-2 flex items-center gap-2">
                <svg className="w-5 h-5 text-coral" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
                </svg>
                Next Step: Complete Your Registration
              </h3>
              <p className="text-sm text-gray-500 leading-relaxed">
                Fill in your personal details, professional qualifications, work preferences, and agree to our terms. This usually takes about 5-10 minutes.
              </p>
            </div>

            <button
              onClick={handleGoToForm}
              className="bg-gradient-to-r from-coral to-coral-dark text-white px-10 py-4 rounded-full font-semibold text-lg shadow-lg hover:shadow-xl hover:-translate-y-0.5 transition-all duration-300"
            >
              <span className="flex items-center gap-2">
                Proceed to Registration Form
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
                </svg>
              </span>
            </button>
          </div>
        </div>

        <div className="h-20" />
      </div>
    );
  }

  /* ── Render: Eligibility Screening (step-by-step) ───────────── */
  const q = questions[currentStep];

  return (
    <div className="min-h-screen bg-gradient-to-b from-cream to-white">
      {/* Hero Banner */}
      <div className="relative bg-navy overflow-hidden">
        <div className="absolute inset-0">
          <Image src="/pics/overhead-dumbbell.jpg" alt="Physiotherapy" fill className="object-cover opacity-30" />
          <div className="absolute inset-0 bg-gradient-to-r from-navy via-navy/90 to-navy/80" />
        </div>
        <div className="relative max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-16 sm:py-20">
          <Link href="/" className="inline-flex items-center gap-2 text-white/70 hover:text-white transition-colors mb-8 group">
            <svg className="w-4 h-4 transition-transform group-hover:-translate-x-1" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
            </svg>
            Back to Home
          </Link>
          <div className="inline-flex items-center gap-2 bg-primary/20 border border-primary/30 rounded-full px-4 py-1.5 mb-5">
            <span className="w-2 h-2 bg-primary rounded-full animate-pulse" />
            <span className="text-primary-200 text-sm font-medium">Join Our Network</span>
          </div>
          <h1 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white leading-tight">
            Join On the Move Home Physio as a <br />
            <span className="text-primary-300">Registered Practitioner</span>
          </h1>
          <p className="mt-4 text-gray-400 text-lg max-w-2xl leading-relaxed">
            Before we begin your application, we need to verify you meet our minimum eligibility criteria. This takes less than a minute.
          </p>
        </div>
      </div>

      {/* Progress Bar */}
      <div className="sticky top-16 z-40 bg-white/90 backdrop-blur-md border-b border-gray-100 shadow-sm">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-3">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-navy">Eligibility Check</span>
            <span className="text-sm text-gray-500">Question {currentStep + 1} of 4</span>
          </div>
          <div className="w-full h-2 bg-gray-100 rounded-full overflow-hidden">
            <div
              className="h-full bg-gradient-to-r from-primary to-primary-400 rounded-full transition-all duration-500 ease-out"
              style={{ width: `${progress}%` }}
            />
          </div>
          {/* Step dots */}
          <div className="flex items-center justify-between mt-3">
            {questions.map((_, i) => (
              <div key={i} className="flex items-center gap-1.5">
                <div
                  className={`
                    w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold transition-all duration-300
                    ${i < currentStep
                      ? "bg-primary text-white scale-90"
                      : i === currentStep
                      ? "bg-primary text-white ring-4 ring-primary/20 scale-110"
                      : "bg-gray-100 text-gray-400"
                    }
                  `}
                >
                  {i < currentStep ? (
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                    </svg>
                  ) : (
                    i + 1
                  )}
                </div>
                {i < 3 && (
                  <div className={`hidden sm:block w-16 md:w-28 lg:w-40 h-0.5 rounded-full transition-colors duration-300 ${i < currentStep ? "bg-primary" : "bg-gray-200"}`} />
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Current Question */}
      {!showIneligible ? (
        <div className="max-w-2xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
          <div
            key={currentStep}
            className="animate-slide-up"
          >
            {/* Question card */}
            <div className="bg-white rounded-2xl border-2 border-gray-100 shadow-lg p-6 sm:p-8">
              {/* Icon */}
              <div className="w-12 h-12 rounded-xl bg-primary-light text-primary flex items-center justify-center mb-4">
                {q.icon}
              </div>

              {/* Question number */}
              <div className="text-xs font-semibold text-primary uppercase tracking-wider mb-2">
                Question {q.number} of 4
              </div>

              {/* Question */}
              <h2 className="text-xl sm:text-2xl font-bold text-navy leading-snug mb-2">
                {q.question}
              </h2>

              {/* Subtitle */}
              <p className="text-gray-500 leading-relaxed mb-8">
                {q.subtitle}
              </p>

              {/* Answer buttons */}
              <div className="flex gap-3">
                <button
                  type="button"
                  onClick={() => handleAnswer("yes")}
                  disabled={animating}
                  className={`
                    flex items-center gap-2.5 px-8 py-3.5 rounded-xl border-2 font-semibold text-base transition-all duration-300
                    ${answers[currentStep] === "yes"
                      ? "border-primary bg-primary text-white shadow-md shadow-primary/20"
                      : "border-gray-200 bg-white text-gray-700 hover:border-primary/50 hover:bg-primary-50"
                    }
                    ${animating ? "pointer-events-none" : "cursor-pointer"}
                  `}
                >
                  <span className={`w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0 transition-all duration-200 ${
                    answers[currentStep] === "yes" ? "border-white bg-white/20" : "border-gray-300"
                  }`}>
                    {answers[currentStep] === "yes" && <span className="w-2 h-2 rounded-full bg-white" />}
                  </span>
                  Yes
                </button>

                <button
                  type="button"
                  onClick={() => handleAnswer("no")}
                  disabled={animating}
                  className={`
                    flex items-center gap-2.5 px-8 py-3.5 rounded-xl border-2 font-semibold text-base transition-all duration-300
                    ${answers[currentStep] === "no"
                      ? "border-coral bg-coral text-white shadow-md shadow-coral/20"
                      : "border-gray-200 bg-white text-gray-700 hover:border-coral/50 hover:bg-coral-50"
                    }
                    ${animating ? "pointer-events-none" : "cursor-pointer"}
                  `}
                >
                  <span className={`w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0 transition-all duration-200 ${
                    answers[currentStep] === "no" ? "border-white bg-white/20" : "border-gray-300"
                  }`}>
                    {answers[currentStep] === "no" && <span className="w-2 h-2 rounded-full bg-white" />}
                  </span>
                  No
                </button>
              </div>
            </div>

            {/* Previous answers summary (shown below current question) */}
            {currentStep > 0 && (
              <div className="mt-6 space-y-2">
                <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 px-1">Previous Answers</p>
                {questions.slice(0, currentStep).map((prevQ, i) => (
                  <div key={i} className="flex items-center gap-3 bg-white/80 backdrop-blur-sm rounded-xl px-4 py-3 border border-gray-100">
                    <div className="w-6 h-6 rounded-full bg-primary flex items-center justify-center flex-shrink-0">
                      <svg className="w-3.5 h-3.5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                      </svg>
                    </div>
                    <span className="text-sm text-gray-600">{prevQ.question}</span>
                    <span className="ml-auto text-xs font-semibold text-primary bg-primary-light rounded-full px-2.5 py-0.5">Yes</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      ) : (
        /* ── Ineligible Screen ───────────────────────────────────── */
        <div className="max-w-2xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
          <div className="animate-slide-up">
            <div className="bg-white rounded-3xl border-2 border-coral/20 shadow-lg p-8 sm:p-12 text-center">
              {/* Failed icon */}
              <div className="w-20 h-20 bg-coral-light rounded-full flex items-center justify-center mx-auto mb-6">
                <svg className="w-10 h-10 text-coral" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                </svg>
              </div>

              <h2 className="text-2xl sm:text-3xl font-bold text-navy mb-3">
                Unfortunately, you don&apos;t meet our eligibility criteria
              </h2>
              <p className="text-gray-500 leading-relaxed mb-6">
                To join On the Move Home Physio, practitioners must answer &quot;Yes&quot; to all eligibility questions. This ensures the safety and quality of care for our patients.
              </p>

              {/* Which question failed */}
              <div className="bg-coral-light rounded-2xl p-6 mb-6 text-left">
                <h3 className="text-sm font-semibold text-coral-dark uppercase tracking-wider mb-3">Failed Check</h3>
                <div className="flex items-center gap-3">
                  <div className="w-6 h-6 rounded-full bg-coral flex items-center justify-center flex-shrink-0">
                    <svg className="w-3.5 h-3.5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </div>
                  <span className="text-sm text-coral-dark font-medium">{questions[currentStep].question}</span>
                </div>
              </div>

              <div className="bg-cream rounded-2xl p-6 mb-8 text-left">
                <p className="font-semibold text-navy mb-2">You may still be eligible if:</p>
                <ul className="list-disc list-inside space-y-1.5 text-sm text-gray-600">
                  <li>You are in the process of obtaining your HCPC registration</li>
                  <li>You are applying for CSP or equivalent membership</li>
                  <li>You have equivalent overseas experience being verified</li>
                </ul>
              </div>

              <div className="flex flex-col sm:flex-row justify-center gap-3">
                <a href="mailto:support@onthemovehomephysio.co.uk" className="btn-outline !text-sm">
                  Contact Us for Help
                </a>
                <button onClick={handleStartOver} className="btn-coral !text-sm !bg-gray-100 !text-gray-700 !shadow-none hover:!bg-gray-200">
                  Start Over
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   REGISTRATION FORM — shown when all eligibility answers = Yes
   ═══════════════════════════════════════════════════════════════ */
function RegistrationForm({ onBack }: { onBack: () => void }) {
  const [profilePhoto, setProfilePhoto] = useState<string | null>(null);
  const [profilePhotoName, setProfilePhotoName] = useState<string>("");
  const [profilePhotoFile, setProfilePhotoFile] = useState<File | null>(null);

  // Resume upload
  const [resumeName, setResumeName] = useState<string>("");
  const [resumeFile, setResumeFile] = useState<File | null>(null);

  // Mandatory ID proof upload
  const [idProofName, setIdProofName] = useState<string>("");
  const [idProofPreview, setIdProofPreview] = useState<string | null>(null);
  const [idProofFile, setIdProofFile] = useState<File | null>(null);

  // Eligibility to work
  const [eligibilityType, setEligibilityType] = useState<"" | "visa" | "uk_citizen">("");
  const [visaDocName, setVisaDocName] = useState<string>("");
  const [visaDocPreview, setVisaDocPreview] = useState<string | null>(null);
  const [visaDocFile, setVisaDocFile] = useState<File | null>(null);
  const [passportPage1, setPassportPage1] = useState<string | null>(null);
  const [passportPage1Name, setPassportPage1Name] = useState("");
  const [passportPage1File, setPassportPage1File] = useState<File | null>(null);
  const [passportPage2, setPassportPage2] = useState<string | null>(null);
  const [passportPage2Name, setPassportPage2Name] = useState("");
  const [passportPage2File, setPassportPage2File] = useState<File | null>(null);

  // Submission state
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");

  // Weekly schedule: each day maps to an array of selected time slots
  const [weeklySchedule, setWeeklySchedule] = useState<Record<string, string[]>>({
    Monday: [],
    Tuesday: [],
    Wednesday: [],
    Thursday: [],
    Friday: [],
    Saturday: [],
    Sunday: [],
  });

  const [form, setForm] = useState({
    // Personal
    title: "",
    firstName: "",
    lastName: "",
    email: "",
    phone: "",
    dateOfBirth: "",
    gender: "",

    // Professional
    hcpcNumber: "",
    professionalBody: "",
    membershipNumber: "",
    yearsExperience: "",
    specialisations: [] as string[],

    // Work Preferences
    serviceRadius: "",
    homeVisit: false,
    online: false,

    // About
    bio: "",
    qualifications: "",

    // Consent
    agreeTerms: false,
    agreePrivacy: false,
    agreeDBS: false,
    agreeRightToWork: false,
  });

  const [formSubmitted, setFormSubmitted] = useState(false);

  const specialisationOptions = [
    "Sports Injury",
    "Neurological",
    "Orthopaedic",
    "Paediatric",
    "Geriatric",
    "Cardiopulmonary",
    "Women's Health",
    "Post-Surgical",
    "Musculoskeletal",
    "Mental Health",
    "Occupational Health",
    "Chronic Pain",
  ];

  const weekDays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  const timeSlots = [
    "08:00 – 10:00",
    "10:00 – 12:00",
    "12:00 – 14:00",
    "14:00 – 16:00",
    "16:00 – 18:00",
    "18:00 – 20:00",
  ];

  const toggleTimeSlot = (day: string, slot: string) => {
    setWeeklySchedule((prev) => ({
      ...prev,
      [day]: prev[day].includes(slot)
        ? prev[day].filter((s) => s !== slot)
        : [...prev[day], slot],
    }));
  };

  const toggleAllDay = (day: string) => {
    setWeeklySchedule((prev) => ({
      ...prev,
      [day]: prev[day].length === timeSlots.length ? [] : [...timeSlots],
    }));
  };

  const hasAnySchedule = Object.values(weeklySchedule).some((slots) => slots.length > 0);

  const toggleSpecialisation = (spec: string) => {
    setForm((prev) => ({
      ...prev,
      specialisations: prev.specialisations.includes(spec)
        ? prev.specialisations.filter((s) => s !== spec)
        : [...prev.specialisations, spec],
    }));
  };

  const updateField = (field: string, value: string | boolean) => {
    setForm((prev) => ({ ...prev, [field]: value }));
  };

  const handlePhotoUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (!file.type.startsWith("image/")) return;
    setProfilePhotoName(file.name);
    setProfilePhotoFile(file);
    const reader = new FileReader();
    reader.onloadend = () => {
      setProfilePhoto(reader.result as string);
    };
    reader.readAsDataURL(file);
  };

  const removePhoto = () => {
    setProfilePhoto(null);
    setProfilePhotoName("");
    setProfilePhotoFile(null);
  };

  const handleResumeUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setResumeName(file.name);
    setResumeFile(file);
  };

  const handleIdProofUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setIdProofName(file.name);
    setIdProofFile(file);
    if (file.type.startsWith("image/")) {
      const reader = new FileReader();
      reader.onloadend = () => setIdProofPreview(reader.result as string);
      reader.readAsDataURL(file);
    } else {
      setIdProofPreview(null);
    }
  };

  const handleVisaDocUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setVisaDocName(file.name);
    setVisaDocFile(file);
    if (file.type.startsWith("image/")) {
      const reader = new FileReader();
      reader.onloadend = () => setVisaDocPreview(reader.result as string);
      reader.readAsDataURL(file);
    } else {
      setVisaDocPreview(null);
    }
  };

  const handlePassportUpload = (page: 1 | 2) => (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (page === 1) setPassportPage1File(file);
    else setPassportPage2File(file);
    const reader = new FileReader();
    reader.onloadend = () => {
      if (page === 1) {
        setPassportPage1(reader.result as string);
        setPassportPage1Name(file.name);
      } else {
        setPassportPage2(reader.result as string);
        setPassportPage2Name(file.name);
      }
    };
    reader.readAsDataURL(file);
  };

  // Eligibility validation
  const isEligibilityValid =
    eligibilityType === "visa" ? !!visaDocName :
    eligibilityType === "uk_citizen" ? !!passportPage1 :
    false;

  const isFormValid =
    profilePhoto &&
    resumeName &&
    idProofName &&
    form.title &&
    form.gender &&
    form.firstName &&
    form.lastName &&
    form.email &&
    form.phone &&
    form.hcpcNumber &&
    form.professionalBody &&
    form.membershipNumber &&
    form.yearsExperience &&
    form.serviceRadius &&
    form.specialisations.length > 0 &&
    hasAnySchedule &&
    isEligibilityValid &&
    form.agreeTerms &&
    form.agreePrivacy &&
    form.agreeDBS &&
    form.agreeRightToWork;

  // Helper to upload a file to Supabase Storage
  const uploadFile = async (file: File, applicationId: string, fileType: string): Promise<string | null> => {
    const fd = new FormData();
    fd.append("file", file);
    fd.append("applicationId", applicationId);
    fd.append("fileType", fileType);
    const res = await fetch("/api/applications/upload", { method: "POST", body: fd });
    if (!res.ok) return null;
    const data = await res.json();
    return data.path || null;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!isFormValid || submitting) return;

    setSubmitting(true);
    setSubmitError("");

    try {
      // Generate a temporary application ID for file storage paths
      const tempId = `app_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;

      // Upload files in parallel
      const uploads = await Promise.all([
        profilePhotoFile ? uploadFile(profilePhotoFile, tempId, "profile-photo") : null,
        resumeFile ? uploadFile(resumeFile, tempId, "resume") : null,
        idProofFile ? uploadFile(idProofFile, tempId, "id-proof") : null,
        visaDocFile ? uploadFile(visaDocFile, tempId, "visa-doc") : null,
        passportPage1File ? uploadFile(passportPage1File, tempId, "passport-page1") : null,
        passportPage2File ? uploadFile(passportPage2File, tempId, "passport-page2") : null,
      ]);

      const [profilePhotoUrl, resumeUrl, idProofUrl, visaDocUrl, passportPage1Url, passportPage2Url] = uploads;

      // Submit application data
      const res = await fetch("/api/applications", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: form.title,
          gender: form.gender,
          firstName: form.firstName,
          lastName: form.lastName,
          email: form.email,
          phone: form.phone,
          dateOfBirth: form.dateOfBirth,
          profilePhotoUrl,
          resumeUrl,
          idProofUrl,
          hcpcNumber: form.hcpcNumber,
          professionalBody: form.professionalBody,
          membershipNumber: form.membershipNumber,
          yearsExperience: form.yearsExperience,
          qualifications: form.qualifications,
          specialisations: form.specialisations,
          serviceRadius: form.serviceRadius,
          homeVisit: form.homeVisit,
          online: form.online,
          weeklySchedule,
          bio: form.bio,
          eligibilityType,
          visaDocUrl,
          passportPage1Url,
          passportPage2Url,
          agreeTerms: form.agreeTerms,
          agreePrivacy: form.agreePrivacy,
          agreeDBS: form.agreeDBS,
          agreeRightToWork: form.agreeRightToWork,
        }),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Submission failed");
      }

      setFormSubmitted(true);
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : "Something went wrong. Please try again.");
    } finally {
      setSubmitting(false);
    }
  };

  /* ── Success State ──────────────────────────────────────────── */
  if (formSubmitted) {
    return (
      <div className="min-h-screen bg-gradient-to-b from-cream to-white flex items-center justify-center px-4">
        <div className="max-w-lg mx-auto text-center animate-slide-up">
          <div className="w-20 h-20 bg-primary-light rounded-full flex items-center justify-center mx-auto mb-6">
            <svg className="w-10 h-10 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12c0 1.268-.63 2.39-1.593 3.068a3.745 3.745 0 01-1.043 3.296 3.745 3.745 0 01-3.296 1.043A3.745 3.745 0 0112 21c-1.268 0-2.39-.63-3.068-1.593a3.746 3.746 0 01-3.296-1.043 3.745 3.745 0 01-1.043-3.296A3.745 3.745 0 013 12c0-1.268.63-2.39 1.593-3.068a3.745 3.745 0 011.043-3.296 3.746 3.746 0 013.296-1.043A3.746 3.746 0 0112 3c1.268 0 2.39.63 3.068 1.593a3.746 3.746 0 013.296 1.043 3.746 3.746 0 011.043 3.296A3.745 3.745 0 0121 12z" />
            </svg>
          </div>
          <h1 className="text-3xl sm:text-4xl font-bold text-navy mb-4">
            Application Submitted!
          </h1>
          <p className="text-gray-500 text-lg leading-relaxed mb-3">
            Thank you for applying to join On the Move Home Physio, <strong className="text-navy">{form.firstName}</strong>.
          </p>
          <p className="text-gray-500 leading-relaxed mb-8">
            Our team will review your application within <strong className="text-navy">2-3 working days</strong>.
            We&apos;ll send a confirmation email to <strong className="text-primary">{form.email}</strong> with next steps,
            including DBS verification and credential checks.
          </p>
          <div className="bg-white rounded-2xl border border-gray-100 p-6 mb-8 text-left">
            <h3 className="font-semibold text-navy mb-3">What happens next?</h3>
            <div className="space-y-3">
              {[
                { step: "1", text: "Application review by our team" },
                { step: "2", text: "HCPC registration & DBS verification" },
                { step: "3", text: "Profile setup & onboarding call" },
                { step: "4", text: "Go live and start receiving bookings!" },
              ].map((item) => (
                <div key={item.step} className="flex items-center gap-3">
                  <span className="w-7 h-7 rounded-full bg-primary-light text-primary text-sm font-bold flex items-center justify-center flex-shrink-0">
                    {item.step}
                  </span>
                  <span className="text-gray-600 text-sm">{item.text}</span>
                </div>
              ))}
            </div>
          </div>
          <Link href="/" className="btn-primary !text-base">
            Back to Home
          </Link>
        </div>
      </div>
    );
  }

  /* ── Registration Form ──────────────────────────────────────── */
  return (
    <div className="min-h-screen bg-gradient-to-b from-cream to-white">
      {/* Header */}
      <div className="relative bg-navy overflow-hidden">
        <div className="absolute inset-0">
          <Image src="/pics/home-visit-stretch.jpg" alt="Physiotherapy home visit" fill className="object-cover opacity-20" />
          <div className="absolute inset-0 bg-gradient-to-r from-navy via-navy/95 to-navy/85" />
        </div>
        <div className="relative max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-12 sm:py-16">
          <button onClick={onBack} className="inline-flex items-center gap-2 text-white/70 hover:text-white transition-colors mb-6 group">
            <svg className="w-4 h-4 transition-transform group-hover:-translate-x-1" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
            </svg>
            Back to Eligibility Check
          </button>
          <div className="flex items-center gap-3 mb-4">
            <div className="w-10 h-10 bg-primary/20 border border-primary/30 rounded-xl flex items-center justify-center">
              <svg className="w-5 h-5 text-primary-300" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
              </svg>
            </div>
            <span className="text-primary-300 font-medium">Eligibility Verified</span>
          </div>
          <h1 className="text-3xl sm:text-4xl font-bold text-white leading-tight">
            Complete Your Registration
          </h1>
          <p className="mt-3 text-gray-400 text-lg max-w-2xl">
            Fill in your details below to apply as a On the Move Home Physio practitioner. Fields marked with <span className="text-coral">*</span> are required.
          </p>
        </div>
      </div>

      {/* Form */}
      <form onSubmit={handleSubmit} className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
        <div className="space-y-8">

          {/* ── Section: Personal Information ──────────────────── */}
          <FormSection number="01" title="Personal Information" description="Your basic contact details and profile photo">
            {/* Profile Photo Upload */}
            <div className="mb-8">
              <label className="block text-sm font-medium text-navy mb-3">
                Profile Photo <span className="text-coral">*</span>
                <span className="text-gray-400 font-normal ml-2 text-xs">This will be visible to patients on your profile</span>
              </label>
              <div className="flex items-start gap-6">
                {/* Photo preview */}
                <div className="relative flex-shrink-0">
                  {profilePhoto ? (
                    <div className="relative group">
                      <div className="w-28 h-28 rounded-2xl overflow-hidden border-2 border-primary/30 shadow-md">
                        <img src={profilePhoto} alt="Profile preview" className="w-full h-full object-cover" />
                      </div>
                      <button
                        type="button"
                        onClick={removePhoto}
                        className="absolute -top-2 -right-2 w-7 h-7 bg-coral text-white rounded-full flex items-center justify-center shadow-md hover:bg-coral-dark transition-colors opacity-0 group-hover:opacity-100"
                      >
                        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  ) : (
                    <div className="w-28 h-28 rounded-2xl border-2 border-dashed border-gray-300 bg-gray-50 flex flex-col items-center justify-center text-gray-400">
                      <svg className="w-8 h-8 mb-1" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 6a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0zM4.501 20.118a7.5 7.5 0 0114.998 0A17.933 17.933 0 0112 21.75c-2.676 0-5.216-.584-7.499-1.632z" />
                      </svg>
                      <span className="text-xs">No photo</span>
                    </div>
                  )}
                </div>

                {/* Upload area */}
                <div className="flex-1">
                  <label className="block cursor-pointer">
                    <input
                      type="file"
                      accept="image/jpeg,image/png,image/webp"
                      onChange={handlePhotoUpload}
                      className="sr-only"
                    />
                    <div className="border-2 border-dashed border-gray-200 rounded-xl p-5 text-center hover:border-primary/50 hover:bg-primary-50/30 transition-all duration-200">
                      <div className="w-10 h-10 bg-primary-light rounded-xl flex items-center justify-center mx-auto mb-3">
                        <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
                        </svg>
                      </div>
                      <p className="text-sm font-medium text-navy">
                        {profilePhoto ? "Change photo" : "Upload your photo"}
                      </p>
                      <p className="text-xs text-gray-400 mt-1">JPG, PNG or WebP. Max 5MB.</p>
                    </div>
                  </label>
                  {profilePhotoName && (
                    <p className="mt-2 text-xs text-primary font-medium flex items-center gap-1.5">
                      <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                      </svg>
                      {profilePhotoName}
                    </p>
                  )}
                  <p className="mt-2 text-xs text-gray-400">
                    A clear, professional headshot helps patients feel comfortable booking with you.
                  </p>
                </div>
              </div>
            </div>

            <div className="grid sm:grid-cols-2 gap-5">
              <div>
                <label className="block text-sm font-medium text-navy mb-1.5">
                  Title <span className="text-coral">*</span>
                </label>
                <select value={form.title} onChange={(e) => updateField("title", e.target.value)} className="input-field">
                  <option value="">Select title...</option>
                  <option value="Mr">Mr</option>
                  <option value="Mrs">Mrs</option>
                  <option value="Ms">Ms</option>
                  <option value="Miss">Miss</option>
                  <option value="Dr">Dr</option>
                  <option value="Prof">Prof</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-navy mb-1.5">
                  Gender <span className="text-coral">*</span>
                </label>
                <select value={form.gender} onChange={(e) => updateField("gender", e.target.value)} className="input-field">
                  <option value="">Select gender...</option>
                  <option value="Male">Male</option>
                  <option value="Female">Female</option>
                  <option value="Other">Other</option>
                  <option value="Prefer not to say">Prefer not to say</option>
                </select>
              </div>
              <InputField label="First Name" required value={form.firstName} onChange={(v) => updateField("firstName", v)} placeholder="e.g. Sarah" />
              <InputField label="Last Name" required value={form.lastName} onChange={(v) => updateField("lastName", v)} placeholder="e.g. Thompson" />
              <InputField label="Email Address" required type="email" value={form.email} onChange={(v) => updateField("email", v)} placeholder="sarah@example.co.uk" />
              <InputField label="Phone Number" required type="tel" value={form.phone} onChange={(v) => updateField("phone", v)} placeholder="07700 900000" />
              <InputField label="Date of Birth" required type="date" value={form.dateOfBirth} onChange={(v) => updateField("dateOfBirth", v)} />
            </div>

            {/* Resume Upload */}
            <div className="mt-8">
              <label className="block text-sm font-medium text-navy mb-3">
                Upload Resume / CV <span className="text-coral">*</span>
                <span className="text-gray-400 font-normal ml-2 text-xs">PDF, DOC or DOCX. Max 10MB.</span>
              </label>
              <label className="block cursor-pointer">
                <input
                  type="file"
                  accept=".pdf,.doc,.docx,application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                  onChange={handleResumeUpload}
                  className="sr-only"
                />
                <div className="border-2 border-dashed border-gray-200 rounded-xl p-5 text-center hover:border-primary/50 hover:bg-primary-50/30 transition-all duration-200">
                  <div className="w-10 h-10 bg-primary-light rounded-xl flex items-center justify-center mx-auto mb-3">
                    <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                    </svg>
                  </div>
                  <p className="text-sm font-medium text-navy">
                    {resumeName ? "Change file" : "Upload your CV / Resume"}
                  </p>
                  <p className="text-xs text-gray-400 mt-1">Click to browse or drag and drop</p>
                </div>
              </label>
              {resumeName && (
                <p className="mt-2 text-xs text-primary font-medium flex items-center gap-1.5">
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                  </svg>
                  {resumeName}
                </p>
              )}
            </div>

            {/* ID Proof Upload */}
            <div className="mt-8">
              <label className="block text-sm font-medium text-navy mb-3">
                Government-Issued Photo ID <span className="text-coral">*</span>
                <span className="text-gray-400 font-normal ml-2 text-xs">Passport, Driving Licence, or National ID Card</span>
              </label>
              <label className="block cursor-pointer">
                <input
                  type="file"
                  accept="image/jpeg,image/png,image/webp,application/pdf"
                  onChange={handleIdProofUpload}
                  className="sr-only"
                />
                <div className={`border-2 border-dashed rounded-xl p-5 text-center transition-all duration-200 ${idProofName ? "border-primary/40 bg-primary-50/30" : "border-gray-200 hover:border-primary/50 hover:bg-primary-50/30"}`}>
                  {idProofName ? (
                    <div className="flex flex-col items-center gap-2">
                      {idProofPreview && (
                        <div className="w-20 h-14 rounded-lg overflow-hidden border border-primary/20 mb-1">
                          <img src={idProofPreview} alt="ID preview" className="w-full h-full object-cover" />
                        </div>
                      )}
                      <div className="flex items-center gap-2">
                        <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                        </svg>
                        <span className="text-sm text-primary font-medium">{idProofName}</span>
                      </div>
                      <p className="text-xs text-gray-400">Click to replace</p>
                    </div>
                  ) : (
                    <>
                      <div className="w-10 h-10 bg-primary-light rounded-xl flex items-center justify-center mx-auto mb-3">
                        <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M15 9h3.75M15 12h3.75M15 15h3.75M4.5 19.5h15a2.25 2.25 0 002.25-2.25V6.75A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25v10.5A2.25 2.25 0 004.5 19.5zm6-10.125a1.875 1.875 0 11-3.75 0 1.875 1.875 0 013.75 0zm1.294 6.336a6.721 6.721 0 01-3.17.789 6.721 6.721 0 01-3.168-.789 3.376 3.376 0 016.338 0z" />
                        </svg>
                      </div>
                      <p className="text-sm font-medium text-navy">Upload your photo ID</p>
                      <p className="text-xs text-gray-400 mt-1">JPG, PNG, WebP or PDF. Max 5MB.</p>
                    </>
                  )}
                </div>
              </label>
              <p className="mt-2 text-xs text-gray-400">
                This is used to verify your identity. Accepted: valid passport, UK driving licence, or national identity card.
              </p>
            </div>
          </FormSection>

          {/* ── Section: Professional Details ─────────────────── */}
          <FormSection number="02" title="Professional Details" description="Your registration and qualifications">
            <div className="grid sm:grid-cols-2 gap-5">
              <InputField label="HCPC Registration Number" required value={form.hcpcNumber} onChange={(v) => updateField("hcpcNumber", v)} placeholder="PH12345" />
              <div>
                <label className="block text-sm font-medium text-navy mb-1.5">
                  Professional Body <span className="text-coral">*</span>
                </label>
                <select value={form.professionalBody} onChange={(e) => updateField("professionalBody", e.target.value)} className="input-field">
                  <option value="">Select...</option>
                  <option value="CSP">CSP — Chartered Society of Physiotherapy</option>
                  <option value="RCOT">RCOT — Royal College of Occupational Therapists</option>
                  <option value="RCSLT">RCSLT — Royal College of Speech and Language Therapists</option>
                  <option value="BPS">BPS — British Psychological Society</option>
                  <option value="Other">Other</option>
                </select>
              </div>
              <InputField label="Membership Number" required value={form.membershipNumber} onChange={(v) => updateField("membershipNumber", v)} placeholder="e.g. 123456" />
              <div>
                <label className="block text-sm font-medium text-navy mb-1.5">
                  Years of UK Experience <span className="text-coral">*</span>
                </label>
                <select value={form.yearsExperience} onChange={(e) => updateField("yearsExperience", e.target.value)} className="input-field">
                  <option value="">Select...</option>
                  <option value="2-5">2–5 years</option>
                  <option value="5-10">5–10 years</option>
                  <option value="10-15">10–15 years</option>
                  <option value="15+">15+ years</option>
                </select>
              </div>
            </div>

            <div className="mt-5">
              <label className="block text-sm font-medium text-navy mb-1.5">Qualifications &amp; Certifications</label>
              <textarea value={form.qualifications} onChange={(e) => updateField("qualifications", e.target.value)} rows={3} className="input-field resize-none" placeholder="e.g. BSc Physiotherapy (King's College London), MSc Sports Medicine, APPI Pilates Level 3..." />
            </div>

            <div className="mt-5">
              <label className="block text-sm font-medium text-navy mb-2.5">
                Specialisations <span className="text-coral">*</span>
                <span className="text-gray-400 font-normal ml-2 text-xs">Select all that apply</span>
              </label>
              <div className="flex flex-wrap gap-2.5">
                {specialisationOptions.map((spec) => (
                  <button key={spec} type="button" onClick={() => toggleSpecialisation(spec)}
                    className={`px-4 py-2 rounded-full text-sm font-medium border-2 transition-all duration-200 ${form.specialisations.includes(spec) ? "border-primary bg-primary text-white shadow-md" : "border-gray-200 bg-white text-gray-600 hover:border-primary/40 hover:text-primary"}`}
                  >
                    {spec}
                  </button>
                ))}
              </div>
            </div>
          </FormSection>

          {/* ── Section: Work Preferences ─────────────────────── */}
          <FormSection number="03" title="Work Preferences" description="How and when you'd like to work">
            <div className="grid sm:grid-cols-2 gap-5">
              <div>
                <label className="block text-sm font-medium text-navy mb-1.5">Service Radius <span className="text-coral">*</span></label>
                <select value={form.serviceRadius} onChange={(e) => updateField("serviceRadius", e.target.value)} className="input-field">
                  <option value="">Select travel distance...</option>
                  <option value="5">Up to 5 miles</option>
                  <option value="10">Up to 10 miles</option>
                  <option value="15">Up to 15 miles</option>
                  <option value="20">Up to 20 miles</option>
                  <option value="25+">25+ miles</option>
                </select>
              </div>
            </div>

            <div className="mt-5">
              <label className="block text-sm font-medium text-navy mb-2.5">
                Service Types <span className="text-gray-400 font-normal ml-2 text-xs">Select all that apply</span>
              </label>
              <div className="flex flex-wrap gap-3">
                {[
                  { key: "homeVisit", label: "🏠 Home Visits", field: "homeVisit" as const },
                  { key: "online", label: "💻 Online Sessions", field: "online" as const },
                ].map((item) => (
                  <button key={item.key} type="button" onClick={() => updateField(item.field, !form[item.field])}
                    className={`px-5 py-3 rounded-xl text-sm font-medium border-2 transition-all duration-200 ${form[item.field] ? "border-primary bg-primary-light text-primary-dark" : "border-gray-200 bg-white text-gray-600 hover:border-primary/40"}`}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Weekly Availability Schedule */}
            <div className="mt-6">
              <label className="block text-sm font-medium text-navy mb-1">
                Weekly Availability <span className="text-coral">*</span>
              </label>
              <p className="text-xs text-gray-400 mb-4">Click on time slots to set your preferred working hours for each day of the week.</p>

              {/* Desktop: full grid */}
              <div className="hidden md:block overflow-x-auto">
                <div className="min-w-[700px]">
                  {/* Header row */}
                  <div className="grid grid-cols-[120px_repeat(6,1fr)] gap-1 mb-1">
                    <div /> {/* empty corner */}
                    {timeSlots.map((slot) => (
                      <div key={slot} className="text-center text-[11px] font-medium text-gray-500 py-1.5">
                        {slot}
                      </div>
                    ))}
                  </div>

                  {/* Day rows */}
                  {weekDays.map((day) => {
                    const allSelected = weeklySchedule[day].length === timeSlots.length;
                    return (
                      <div key={day} className="grid grid-cols-[120px_repeat(6,1fr)] gap-1 mb-1">
                        <button
                          type="button"
                          onClick={() => toggleAllDay(day)}
                          className={`text-left text-sm font-semibold py-2.5 px-3 rounded-lg transition-all duration-200 ${
                            allSelected
                              ? "bg-primary text-white"
                              : weeklySchedule[day].length > 0
                              ? "bg-primary-light text-primary-dark"
                              : "text-navy hover:bg-gray-50"
                          }`}
                        >
                          {day.slice(0, 3)}
                          {weeklySchedule[day].length > 0 && (
                            <span className={`ml-1.5 text-[10px] font-normal ${allSelected ? "text-white/70" : "text-primary/60"}`}>
                              ({weeklySchedule[day].length})
                            </span>
                          )}
                        </button>
                        {timeSlots.map((slot) => {
                          const isSelected = weeklySchedule[day].includes(slot);
                          return (
                            <button
                              key={`${day}-${slot}`}
                              type="button"
                              onClick={() => toggleTimeSlot(day, slot)}
                              className={`py-2.5 rounded-lg text-xs font-medium border transition-all duration-150 ${
                                isSelected
                                  ? "bg-primary border-primary text-white shadow-sm"
                                  : "bg-gray-50 border-gray-200 text-gray-400 hover:border-primary/40 hover:bg-primary-50 hover:text-primary"
                              }`}
                            >
                              {isSelected ? "✓" : "–"}
                            </button>
                          );
                        })}
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Mobile: collapsible day sections */}
              <div className="md:hidden space-y-2">
                {weekDays.map((day) => {
                  const daySlots = weeklySchedule[day];
                  const allSelected = daySlots.length === timeSlots.length;
                  return (
                    <div key={day} className="border border-gray-200 rounded-xl overflow-hidden">
                      <button
                        type="button"
                        onClick={() => toggleAllDay(day)}
                        className={`w-full flex items-center justify-between px-4 py-3 text-sm font-semibold transition-all duration-200 ${
                          daySlots.length > 0 ? "bg-primary-light text-primary-dark" : "bg-gray-50 text-navy"
                        }`}
                      >
                        <span className="flex items-center gap-2">
                          {day}
                          {daySlots.length > 0 && (
                            <span className="text-[10px] bg-primary/10 text-primary rounded-full px-2 py-0.5">
                              {daySlots.length} slot{daySlots.length > 1 ? "s" : ""}
                            </span>
                          )}
                        </span>
                        <span className="text-xs text-gray-400">{allSelected ? "Clear all" : "Select all"}</span>
                      </button>
                      <div className="grid grid-cols-2 gap-1.5 p-3 bg-white">
                        {timeSlots.map((slot) => {
                          const isSelected = daySlots.includes(slot);
                          return (
                            <button
                              key={`${day}-${slot}-mobile`}
                              type="button"
                              onClick={() => toggleTimeSlot(day, slot)}
                              className={`py-2 px-3 rounded-lg text-xs font-medium border transition-all duration-150 ${
                                isSelected
                                  ? "bg-primary border-primary text-white"
                                  : "bg-gray-50 border-gray-200 text-gray-500 hover:border-primary/40"
                              }`}
                            >
                              {slot}
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
              </div>

              {/* Summary */}
              {hasAnySchedule && (
                <div className="mt-3 p-3 bg-primary-50 rounded-xl">
                  <p className="text-xs font-medium text-primary-dark">
                    📅 {Object.entries(weeklySchedule).filter(([, slots]) => slots.length > 0).length} day{Object.entries(weeklySchedule).filter(([, slots]) => slots.length > 0).length > 1 ? "s" : ""} selected
                    {" · "}
                    {Object.values(weeklySchedule).reduce((sum, slots) => sum + slots.length, 0)} total time slot{Object.values(weeklySchedule).reduce((sum, slots) => sum + slots.length, 0) > 1 ? "s" : ""}
                  </p>
                </div>
              )}
            </div>
          </FormSection>

          {/* ── Section: About You ────────────────────────────── */}
          <FormSection number="04" title="About You" description="Tell patients why they should choose you">
            <div>
              <label className="block text-sm font-medium text-navy mb-1.5">Professional Bio</label>
              <textarea value={form.bio} onChange={(e) => updateField("bio", e.target.value)} rows={5} className="input-field resize-none" placeholder="Write a short bio about your experience, approach to treatment, and what patients can expect. This will be visible on your public profile..." />
              <p className="mt-1.5 text-xs text-gray-400">{form.bio.length}/500 characters</p>
            </div>
          </FormSection>

          {/* ── Section: Right to Work / Eligibility ─────────── */}
          <FormSection number="05" title="Right to Work" description="Prove your eligibility to work in the United Kingdom">
            <div>
              <label className="block text-sm font-medium text-navy mb-1.5">
                Eligibility to Work <span className="text-coral">*</span>
              </label>
              <select
                value={eligibilityType}
                onChange={(e) => {
                  setEligibilityType(e.target.value as "" | "visa" | "uk_citizen");
                  setVisaDocName("");
                  setVisaDocPreview(null);
                  setPassportPage1(null);
                  setPassportPage1Name("");
                  setPassportPage2(null);
                  setPassportPage2Name("");
                }}
                className="input-field"
              >
                <option value="">Select your eligibility type...</option>
                <option value="visa">Visa</option>
                <option value="uk_citizen">UK Citizen</option>
              </select>
            </div>

            {/* Conditional: Visa → Document Upload */}
            {eligibilityType === "visa" && (
              <div className="mt-5 animate-slide-up">
                <div className="bg-primary-50 border border-primary-100 rounded-xl p-4 mb-4">
                  <p className="text-sm text-primary-dark flex items-center gap-2">
                    <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    Please upload a copy of your valid UK visa document.
                  </p>
                </div>
                <label className="block text-sm font-medium text-navy mb-2">
                  Visa Document <span className="text-coral">*</span>
                </label>
                <label className="block cursor-pointer">
                  <input
                    type="file"
                    accept="image/jpeg,image/png,image/webp,application/pdf"
                    onChange={handleVisaDocUpload}
                    className="sr-only"
                  />
                  <div className={`border-2 border-dashed rounded-xl p-5 text-center transition-all duration-200 ${visaDocName ? "border-primary/40 bg-primary-50/30" : "border-gray-200 hover:border-primary/50 hover:bg-primary-50/30"}`}>
                    {visaDocName ? (
                      <div className="flex items-center gap-2 justify-center">
                        <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                        </svg>
                        <span className="text-sm text-primary font-medium truncate">{visaDocName}</span>
                      </div>
                    ) : (
                      <>
                        <div className="w-10 h-10 bg-primary-light rounded-xl flex items-center justify-center mx-auto mb-3">
                          <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                            <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
                          </svg>
                        </div>
                        <p className="text-sm font-medium text-navy">Upload your visa document</p>
                        <p className="text-xs text-gray-400 mt-1">JPG, PNG, WebP or PDF. Max 5MB.</p>
                      </>
                    )}
                  </div>
                </label>
              </div>
            )}

            {/* Conditional: UK Citizen → Passport Upload (2 pages) */}
            {eligibilityType === "uk_citizen" && (
              <div className="mt-5 animate-slide-up">
                <div className="bg-primary-50 border border-primary-100 rounded-xl p-4 mb-4">
                  <p className="text-sm text-primary-dark flex items-center gap-2">
                    <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    Please upload the particulars pages of your UK passport (up to 2 pages).
                  </p>
                </div>

                <div className="grid sm:grid-cols-2 gap-4">
                  {/* Passport Page 1 */}
                  <div>
                    <label className="block text-sm font-medium text-navy mb-2">
                      Passport Page 1 <span className="text-coral">*</span>
                    </label>
                    <label className="block cursor-pointer">
                      <input
                        type="file"
                        accept="image/jpeg,image/png,image/webp,application/pdf"
                        onChange={handlePassportUpload(1)}
                        className="sr-only"
                      />
                      <div className={`border-2 border-dashed rounded-xl p-4 text-center transition-all duration-200 ${passportPage1 ? "border-primary/40 bg-primary-50/30" : "border-gray-200 hover:border-primary/50 hover:bg-primary-50/30"}`}>
                        {passportPage1 ? (
                          <div className="flex items-center gap-2 justify-center">
                            <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                            </svg>
                            <span className="text-sm text-primary font-medium truncate">{passportPage1Name}</span>
                          </div>
                        ) : (
                          <>
                            <svg className="w-8 h-8 text-gray-400 mx-auto mb-1" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                              <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
                            </svg>
                            <p className="text-xs text-gray-500">Upload page 1</p>
                          </>
                        )}
                      </div>
                    </label>
                  </div>

                  {/* Passport Page 2 */}
                  <div>
                    <label className="block text-sm font-medium text-navy mb-2">
                      Passport Page 2 <span className="text-gray-400 font-normal text-xs">(optional)</span>
                    </label>
                    <label className="block cursor-pointer">
                      <input
                        type="file"
                        accept="image/jpeg,image/png,image/webp,application/pdf"
                        onChange={handlePassportUpload(2)}
                        className="sr-only"
                      />
                      <div className={`border-2 border-dashed rounded-xl p-4 text-center transition-all duration-200 ${passportPage2 ? "border-primary/40 bg-primary-50/30" : "border-gray-200 hover:border-primary/50 hover:bg-primary-50/30"}`}>
                        {passportPage2 ? (
                          <div className="flex items-center gap-2 justify-center">
                            <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                            </svg>
                            <span className="text-sm text-primary font-medium truncate">{passportPage2Name}</span>
                          </div>
                        ) : (
                          <>
                            <svg className="w-8 h-8 text-gray-400 mx-auto mb-1" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                              <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
                            </svg>
                            <p className="text-xs text-gray-500">Upload page 2</p>
                          </>
                        )}
                      </div>
                    </label>
                  </div>
                </div>
                <p className="mt-2 text-xs text-gray-400">Accepted formats: JPG, PNG, WebP or PDF. Max 5MB per file.</p>
              </div>
            )}
          </FormSection>

          {/* ── Section: Agreements ───────────────────────────── */}
          <FormSection number="06" title="Agreements &amp; Consent" description="Please review and agree to the following">
            <div className="space-y-4">
              <CheckboxField
                checked={form.agreeTerms}
                onChange={(v) => updateField("agreeTerms", v)}
                label={<>I agree to the On the Move Home Physio <span className="text-primary underline cursor-pointer">Terms of Service</span> and <span className="text-primary underline cursor-pointer">Practitioner Agreement</span> <span className="text-coral">*</span></>}
              />
              <CheckboxField
                checked={form.agreePrivacy}
                onChange={(v) => updateField("agreePrivacy", v)}
                label={<>I consent to the processing of my data as described in the <span className="text-primary underline cursor-pointer">Privacy Policy</span> <span className="text-coral">*</span></>}
              />
              <CheckboxField
                checked={form.agreeDBS}
                onChange={(v) => updateField("agreeDBS", v)}
                label={<>I understand that a DBS (Disclosure and Barring Service) check will be required and I consent to this process <span className="text-coral">*</span></>}
              />
              <CheckboxField
                checked={form.agreeRightToWork}
                onChange={(v) => updateField("agreeRightToWork", v)}
                label={<>I understand and agree that my data will be used to check the right to work <span className="text-coral">*</span></>}
              />
            </div>
          </FormSection>
        </div>

        {/* Submit Error */}
        {submitError && (
          <div className="mt-6 bg-red-50 border border-red-200 rounded-xl p-4 text-sm text-red-700 flex items-center gap-2">
            <svg className="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.732-.833-2.5 0L4.268 16.5c-.77.833.192 2.5 1.732 2.5z" />
            </svg>
            {submitError}
          </div>
        )}

        {/* Submit */}
        <div className="mt-12 flex flex-col items-center pb-8">
          <button type="submit" disabled={!isFormValid || submitting}
            className={`relative px-12 py-4 rounded-full font-semibold text-lg transition-all duration-300 shadow-lg ${isFormValid && !submitting ? "bg-gradient-to-r from-coral to-coral-dark text-white hover:shadow-xl hover:-translate-y-0.5 cursor-pointer" : "bg-gray-200 text-gray-400 cursor-not-allowed shadow-none"}`}
          >
            {submitting ? (
              <span className="flex items-center gap-2">
                <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                Submitting...
              </span>
            ) : (
              <span className="flex items-center gap-2">
                Submit Application
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 12L3.269 3.126A59.768 59.768 0 0121.485 12 59.77 59.77 0 013.27 20.876L5.999 12zm0 0h7.5" />
                </svg>
              </span>
            )}
          </button>
          <p className="mt-3 text-sm text-gray-400">
            {submitting ? "Uploading files and submitting your application..." : "Your application will be reviewed within 2-3 working days"}
          </p>
        </div>
      </form>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   REUSABLE FORM COMPONENTS
   ═══════════════════════════════════════════════════════════════ */

function FormSection({ number, title, description, children }: { number: string; title: string; description: string; children: React.ReactNode }) {
  return (
    <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-6 sm:p-8">
      <div className="flex items-start gap-4 mb-6">
        <span className="flex-shrink-0 w-10 h-10 rounded-xl bg-primary-light text-primary font-bold text-sm flex items-center justify-center">{number}</span>
        <div>
          <h2 className="text-xl font-bold text-navy">{title}</h2>
          <p className="text-sm text-gray-500 mt-0.5">{description}</p>
        </div>
      </div>
      {children}
    </div>
  );
}

function InputField({ label, required, type = "text", value, onChange, placeholder }: { label: string; required?: boolean; type?: string; value: string; onChange: (v: string) => void; placeholder?: string }) {
  return (
    <div>
      <label className="block text-sm font-medium text-navy mb-1.5">
        {label} {required && <span className="text-coral">*</span>}
      </label>
      <input type={type} value={value} onChange={(e) => onChange(e.target.value)} placeholder={placeholder} className="input-field" />
    </div>
  );
}

function CheckboxField({ checked, onChange, label }: { checked: boolean; onChange: (v: boolean) => void; label: React.ReactNode }) {
  return (
    <label className="flex items-start gap-3 cursor-pointer group">
      <div className="relative flex-shrink-0 mt-0.5">
        <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} className="sr-only" />
        <div className={`w-5 h-5 rounded-md border-2 flex items-center justify-center transition-all duration-200 ${checked ? "bg-primary border-primary" : "border-gray-300 group-hover:border-primary/50"}`}>
          {checked && (
            <svg className="w-3.5 h-3.5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
            </svg>
          )}
        </div>
      </div>
      <span className="text-sm text-gray-600 leading-relaxed">{label}</span>
    </label>
  );
}
