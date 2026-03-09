"use client";

import { useState, useEffect, useCallback } from "react";
import { useRouter, useParams } from "next/navigation";
import Link from "next/link";
import Image from "next/image";

interface ApplicationData {
  id: string;
  title: string | null;
  gender: string | null;
  firstName: string;
  lastName: string;
  email: string;
  phone: string;
  dateOfBirth: string | null;
  profilePhotoUrl: string | null;
  resumeUrl: string | null;
  idProofUrl: string | null;
  hcpcNumber: string;
  professionalBody: string;
  membershipNumber: string;
  yearsExperience: string;
  qualifications: string | null;
  specialisations: string[];
  serviceRadius: string | null;
  homeVisit: boolean;
  online: boolean;
  weeklySchedule: Record<string, string[]>;
  bio: string | null;
  eligibilityType: string | null;
  visaDocUrl: string | null;
  passportPage1Url: string | null;
  passportPage2Url: string | null;
  agreeTerms: boolean;
  agreePrivacy: boolean;
  agreeDBS: boolean;
  agreeRightToWork: boolean;
  status: string;
  reviewNotes: string | null;
  reviewedAt: string | null;
  createdAt: string;
}

const statusColors: Record<string, string> = {
  pending: "bg-amber-50 text-amber-700 border-amber-200",
  approved: "bg-emerald-50 text-emerald-700 border-emerald-200",
  rejected: "bg-red-50 text-red-700 border-red-200",
  needs_info: "bg-blue-50 text-blue-700 border-blue-200",
};

const statusLabels: Record<string, string> = {
  pending: "Pending Review",
  approved: "Approved",
  rejected: "Rejected",
  needs_info: "Additional Info Requested",
};

export default function ApplicationDetailPage() {
  const router = useRouter();
  const params = useParams();
  const applicationId = params.id as string;

  const [application, setApplication] = useState<ApplicationData | null>(null);
  const [signedUrls, setSignedUrls] = useState<Record<string, string | null>>({});
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState("");
  const [reviewNotes, setReviewNotes] = useState("");
  const [showRejectModal, setShowRejectModal] = useState(false);
  const [showNeedsInfoModal, setShowNeedsInfoModal] = useState(false);
  const [actionSuccess, setActionSuccess] = useState("");

  const loadApplication = useCallback(async () => {
    try {
      const res = await fetch(`/api/admin/applications/${applicationId}`);
      if (!res.ok) {
        router.push("/admin/applications");
        return;
      }
      const data = await res.json();
      setApplication(data.application);
      setSignedUrls(data.signedUrls);
    } catch {
      router.push("/admin/applications");
    } finally {
      setLoading(false);
    }
  }, [applicationId, router]);

  useEffect(() => {
    loadApplication();
  }, [loadApplication]);

  const handleApprove = async () => {
    if (!confirm("Are you sure you want to approve this application? This will create a new physiotherapist profile.")) return;
    setActionLoading("approve");
    try {
      const res = await fetch(`/api/admin/applications/${applicationId}/approve`, { method: "POST" });
      if (res.ok) {
        const data = await res.json();
        setActionSuccess(`Approved! Physio profile created (slug: ${data.slug})`);
        await loadApplication();
      } else {
        const data = await res.json();
        alert(data.error || "Approval failed");
      }
    } catch {
      alert("Approval failed. Please try again.");
    } finally {
      setActionLoading("");
    }
  };

  const handleReject = async () => {
    setActionLoading("reject");
    try {
      const res = await fetch(`/api/admin/applications/${applicationId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "rejected", reviewNotes }),
      });
      if (res.ok) {
        setActionSuccess("Application rejected.");
        setShowRejectModal(false);
        setReviewNotes("");
        await loadApplication();
      } else {
        const data = await res.json();
        alert(data.error || "Failed to reject");
      }
    } catch {
      alert("Failed to reject. Please try again.");
    } finally {
      setActionLoading("");
    }
  };

  const handleNeedsInfo = async () => {
    if (!reviewNotes.trim()) {
      alert("Please provide details about what information is needed.");
      return;
    }
    setActionLoading("needs_info");
    try {
      const res = await fetch(`/api/admin/applications/${applicationId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "needs_info", reviewNotes }),
      });
      if (res.ok) {
        setActionSuccess("Additional information requested.");
        setShowNeedsInfoModal(false);
        setReviewNotes("");
        await loadApplication();
      } else {
        const data = await res.json();
        alert(data.error || "Failed to update");
      }
    } catch {
      alert("Failed to update. Please try again.");
    } finally {
      setActionLoading("");
    }
  };

  const handleLogout = async () => {
    await fetch("/api/admin/logout", { method: "POST" });
    router.push("/admin/login");
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="flex items-center gap-3 text-gray-500">
          <svg className="w-6 h-6 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
          </svg>
          Loading application...
        </div>
      </div>
    );
  }

  if (!application) return null;

  const weekDays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center gap-3">
              <Link href="/admin">
                <Image src="/logo.png" alt="PhysioConnect" width={140} height={40} />
              </Link>
              <span className="text-xs font-bold text-white bg-navy px-2 py-0.5 rounded-md">ADMIN</span>
            </div>
            <div className="flex items-center gap-4">
              <Link href="/admin" className="text-sm font-medium text-gray-600 hover:text-primary transition-colors">Dashboard</Link>
              <Link href="/admin/applications" className="text-sm font-medium text-gray-600 hover:text-primary transition-colors">Applications</Link>
              <div className="h-5 w-px bg-gray-200" />
              <button onClick={handleLogout} className="text-sm text-gray-500 hover:text-red-600 transition-colors">Logout</button>
            </div>
          </div>
        </div>
      </header>

      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Back + Title */}
        <div className="mb-6">
          <Link href="/admin/applications" className="text-sm text-gray-500 hover:text-gray-700 flex items-center gap-1 mb-3">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
            </svg>
            Back to Applications
          </Link>
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div className="flex items-center gap-4">
              {signedUrls.profilePhotoUrl ? (
                <img
                  src={signedUrls.profilePhotoUrl}
                  alt={`${application.firstName} ${application.lastName}`}
                  className="w-16 h-16 rounded-2xl object-cover border-2 border-gray-100"
                />
              ) : (
                <div className="w-16 h-16 rounded-2xl bg-primary-light text-primary font-bold text-xl flex items-center justify-center">
                  {application.firstName[0]}{application.lastName[0]}
                </div>
              )}
              <div>
                <h1 className="text-2xl font-bold text-gray-900">
                  {application.title ? `${application.title} ` : ''}{application.firstName} {application.lastName}
                </h1>
                <p className="text-sm text-gray-500">
                  Applied {new Date(application.createdAt).toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric" })}
                </p>
              </div>
            </div>
            <span className={`text-sm font-medium px-4 py-2 rounded-full border ${statusColors[application.status] || statusColors.pending}`}>
              {statusLabels[application.status] || application.status}
            </span>
          </div>
        </div>

        {/* Success message */}
        {actionSuccess && (
          <div className="mb-6 bg-emerald-50 border border-emerald-200 rounded-xl p-4 text-sm text-emerald-700 flex items-center gap-2">
            <svg className="w-5 h-5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            {actionSuccess}
          </div>
        )}

        {/* Review notes if any */}
        {application.reviewNotes && (
          <div className="mb-6 bg-blue-50 border border-blue-200 rounded-xl p-4">
            <h3 className="text-sm font-semibold text-blue-800 mb-1">Review Notes</h3>
            <p className="text-sm text-blue-700">{application.reviewNotes}</p>
            {application.reviewedAt && (
              <p className="text-xs text-blue-500 mt-2">
                Updated {new Date(application.reviewedAt).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" })}
              </p>
            )}
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Main Content */}
          <div className="lg:col-span-2 space-y-6">
            {/* Personal Information */}
            <DetailSection title="Personal Information">
              <div className="grid sm:grid-cols-2 gap-4">
                <DetailField label="Title" value={application.title || "Not provided"} />
                <DetailField label="Gender" value={application.gender || "Not provided"} />
                <DetailField label="Full Name" value={`${application.title ? application.title + ' ' : ''}${application.firstName} ${application.lastName}`} />
                <DetailField label="Email" value={application.email} />
                <DetailField label="Phone" value={application.phone} />
                <DetailField label="Date of Birth" value={application.dateOfBirth || "Not provided"} />
              </div>
            </DetailSection>

            {/* Professional Details */}
            <DetailSection title="Professional Details">
              <div className="grid sm:grid-cols-2 gap-4">
                <DetailField label="HCPC Number" value={application.hcpcNumber} />
                <DetailField label="Professional Body" value={application.professionalBody} />
                <DetailField label="Membership Number" value={application.membershipNumber} />
                <DetailField label="Years of Experience" value={application.yearsExperience} />
              </div>
              {application.qualifications && (
                <div className="mt-4">
                  <DetailField label="Qualifications" value={application.qualifications} />
                </div>
              )}
              {application.specialisations && application.specialisations.length > 0 && (
                <div className="mt-4">
                  <p className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Specialisations</p>
                  <div className="flex flex-wrap gap-2">
                    {application.specialisations.map((s) => (
                      <span key={s} className="text-sm bg-primary-light text-primary px-3 py-1 rounded-full font-medium">{s}</span>
                    ))}
                  </div>
                </div>
              )}
            </DetailSection>

            {/* Work Preferences */}
            <DetailSection title="Work Preferences">
              <div className="grid sm:grid-cols-2 gap-4 mb-4">
                <DetailField label="Service Radius" value={application.serviceRadius ? `${application.serviceRadius} miles` : "Not specified"} />
                <div>
                  <p className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1">Service Types</p>
                  <div className="flex gap-2">
                    {application.homeVisit && (
                      <span className="text-sm bg-emerald-50 text-emerald-700 px-3 py-1 rounded-full font-medium border border-emerald-200">Home Visits</span>
                    )}
                    {application.online && (
                      <span className="text-sm bg-blue-50 text-blue-700 px-3 py-1 rounded-full font-medium border border-blue-200">Online</span>
                    )}
                    {!application.homeVisit && !application.online && (
                      <span className="text-sm text-gray-400">None selected</span>
                    )}
                  </div>
                </div>
              </div>

              {/* Weekly Schedule */}
              {application.weeklySchedule && Object.keys(application.weeklySchedule).length > 0 && (
                <div>
                  <p className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Weekly Availability</p>
                  <div className="space-y-2">
                    {weekDays.map((day) => {
                      const slots = application.weeklySchedule[day] || [];
                      if (slots.length === 0) return null;
                      return (
                        <div key={day} className="flex items-start gap-3">
                          <span className="text-sm font-medium text-gray-700 w-24 flex-shrink-0">{day}</span>
                          <div className="flex flex-wrap gap-1.5">
                            {slots.map((slot) => (
                              <span key={slot} className="text-xs bg-gray-100 text-gray-600 px-2 py-1 rounded-md">{slot}</span>
                            ))}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </DetailSection>

            {/* Bio */}
            {application.bio && (
              <DetailSection title="Professional Bio">
                <p className="text-sm text-gray-700 leading-relaxed whitespace-pre-wrap">{application.bio}</p>
              </DetailSection>
            )}

            {/* Right to Work */}
            <DetailSection title="Right to Work">
              <DetailField label="Eligibility Type" value={application.eligibilityType === "visa" ? "Visa Holder" : application.eligibilityType === "uk_citizen" ? "UK Citizen" : "Not specified"} />
            </DetailSection>

            {/* Consent Flags */}
            <DetailSection title="Agreements">
              <div className="space-y-2">
                <ConsentRow label="Terms of Service" agreed={application.agreeTerms} />
                <ConsentRow label="Privacy Policy" agreed={application.agreePrivacy} />
                <ConsentRow label="DBS Check Consent" agreed={application.agreeDBS} />
                <ConsentRow label="Right to Work Check" agreed={application.agreeRightToWork} />
              </div>
            </DetailSection>
          </div>

          {/* Sidebar */}
          <div className="space-y-6">
            {/* Action Buttons */}
            {application.status === "pending" && (
              <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-6">
                <h3 className="font-bold text-gray-900 mb-4">Actions</h3>
                <div className="space-y-3">
                  <button
                    onClick={handleApprove}
                    disabled={!!actionLoading}
                    className="w-full py-3 px-4 rounded-xl bg-emerald-600 text-white font-semibold hover:bg-emerald-700 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
                  >
                    {actionLoading === "approve" ? (
                      <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" /></svg>
                    ) : (
                      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" /></svg>
                    )}
                    Approve Application
                  </button>
                  <button
                    onClick={() => setShowNeedsInfoModal(true)}
                    disabled={!!actionLoading}
                    className="w-full py-3 px-4 rounded-xl bg-blue-50 text-blue-700 font-semibold border border-blue-200 hover:bg-blue-100 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
                  >
                    <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><path strokeLinecap="round" strokeLinejoin="round" d="M9.879 7.519c1.171-1.025 3.071-1.025 4.242 0 1.172 1.025 1.172 2.687 0 3.712-.203.179-.43.326-.67.442-.745.361-1.45.999-1.45 1.827v.75M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9 5.25h.008v.008H12v-.008z" /></svg>
                    Request More Info
                  </button>
                  <button
                    onClick={() => setShowRejectModal(true)}
                    disabled={!!actionLoading}
                    className="w-full py-3 px-4 rounded-xl bg-red-50 text-red-700 font-semibold border border-red-200 hover:bg-red-100 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
                  >
                    <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" /></svg>
                    Reject Application
                  </button>
                </div>
              </div>
            )}

            {/* Uploaded Documents */}
            <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-6">
              <h3 className="font-bold text-gray-900 mb-4">Documents</h3>
              <div className="space-y-3">
                <FileLink label="Profile Photo" url={signedUrls.profilePhotoUrl} isImage />
                <FileLink label="Resume / CV" url={signedUrls.resumeUrl} />
                <FileLink label="Government ID" url={signedUrls.idProofUrl} isImage />
                {application.eligibilityType === "visa" && (
                  <FileLink label="Visa Document" url={signedUrls.visaDocUrl} />
                )}
                {application.eligibilityType === "uk_citizen" && (
                  <>
                    <FileLink label="Passport Page 1" url={signedUrls.passportPage1Url} isImage />
                    <FileLink label="Passport Page 2" url={signedUrls.passportPage2Url} isImage />
                  </>
                )}
              </div>
            </div>

            {/* Application Meta */}
            <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-6">
              <h3 className="font-bold text-gray-900 mb-4">Application Info</h3>
              <div className="space-y-3 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-500">Application ID</span>
                  <span className="text-gray-700 font-mono text-xs">{application.id.slice(0, 8)}...</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Submitted</span>
                  <span className="text-gray-700">{new Date(application.createdAt).toLocaleDateString("en-GB")}</span>
                </div>
                {application.reviewedAt && (
                  <div className="flex justify-between">
                    <span className="text-gray-500">Last Reviewed</span>
                    <span className="text-gray-700">{new Date(application.reviewedAt).toLocaleDateString("en-GB")}</span>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Reject Modal */}
      {showRejectModal && (
        <Modal title="Reject Application" onClose={() => setShowRejectModal(false)}>
          <p className="text-sm text-gray-600 mb-4">
            Are you sure you want to reject the application from <strong>{application.firstName} {application.lastName}</strong>?
          </p>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-1.5">Reason (optional)</label>
            <textarea
              value={reviewNotes}
              onChange={(e) => setReviewNotes(e.target.value)}
              rows={3}
              className="w-full px-3 py-2 rounded-xl border-2 border-gray-200 focus:border-red-400 focus:ring-0 outline-none text-sm"
              placeholder="Provide a reason for rejection..."
            />
          </div>
          <div className="flex gap-3 justify-end">
            <button onClick={() => setShowRejectModal(false)} className="px-4 py-2 rounded-xl text-sm font-medium text-gray-600 hover:bg-gray-100 transition-colors">
              Cancel
            </button>
            <button
              onClick={handleReject}
              disabled={actionLoading === "reject"}
              className="px-6 py-2 rounded-xl text-sm font-semibold bg-red-600 text-white hover:bg-red-700 transition-colors disabled:opacity-50"
            >
              {actionLoading === "reject" ? "Rejecting..." : "Confirm Reject"}
            </button>
          </div>
        </Modal>
      )}

      {/* Needs Info Modal */}
      {showNeedsInfoModal && (
        <Modal title="Request Additional Information" onClose={() => setShowNeedsInfoModal(false)}>
          <p className="text-sm text-gray-600 mb-4">
            What additional information do you need from <strong>{application.firstName} {application.lastName}</strong>?
          </p>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-1.5">Details <span className="text-red-500">*</span></label>
            <textarea
              value={reviewNotes}
              onChange={(e) => setReviewNotes(e.target.value)}
              rows={4}
              className="w-full px-3 py-2 rounded-xl border-2 border-gray-200 focus:border-blue-400 focus:ring-0 outline-none text-sm"
              placeholder="e.g. Please provide a clearer copy of your ID document..."
            />
          </div>
          <div className="flex gap-3 justify-end">
            <button onClick={() => setShowNeedsInfoModal(false)} className="px-4 py-2 rounded-xl text-sm font-medium text-gray-600 hover:bg-gray-100 transition-colors">
              Cancel
            </button>
            <button
              onClick={handleNeedsInfo}
              disabled={actionLoading === "needs_info" || !reviewNotes.trim()}
              className="px-6 py-2 rounded-xl text-sm font-semibold bg-blue-600 text-white hover:bg-blue-700 transition-colors disabled:opacity-50"
            >
              {actionLoading === "needs_info" ? "Sending..." : "Send Request"}
            </button>
          </div>
        </Modal>
      )}
    </div>
  );
}

/* ── Sub-components ──────────────────────────────── */

function DetailSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-6">
      <h2 className="text-sm font-bold text-gray-900 uppercase tracking-wider mb-4">{title}</h2>
      {children}
    </div>
  );
}

function DetailField({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-0.5">{label}</p>
      <p className="text-sm text-gray-900">{value}</p>
    </div>
  );
}

function ConsentRow({ label, agreed }: { label: string; agreed: boolean }) {
  return (
    <div className="flex items-center gap-2">
      {agreed ? (
        <svg className="w-5 h-5 text-emerald-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      ) : (
        <svg className="w-5 h-5 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 9.75l4.5 4.5m0-4.5l-4.5 4.5M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      )}
      <span className="text-sm text-gray-700">{label}</span>
    </div>
  );
}

function FileLink({ label, url, isImage }: { label: string; url: string | null | undefined; isImage?: boolean }) {
  if (!url) {
    return (
      <div className="flex items-center gap-3 p-3 rounded-xl bg-gray-50">
        <div className="w-8 h-8 rounded-lg bg-gray-200 text-gray-400 flex items-center justify-center flex-shrink-0">
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636" />
          </svg>
        </div>
        <div>
          <p className="text-sm font-medium text-gray-400">{label}</p>
          <p className="text-xs text-gray-400">Not uploaded</p>
        </div>
      </div>
    );
  }

  return (
    <a href={url} target="_blank" rel="noopener noreferrer" className="flex items-center gap-3 p-3 rounded-xl hover:bg-primary-50 transition-colors group">
      {isImage ? (
        <div className="w-10 h-10 rounded-lg overflow-hidden border border-gray-200 flex-shrink-0">
          <img src={url} alt={label} className="w-full h-full object-cover" />
        </div>
      ) : (
        <div className="w-10 h-10 rounded-lg bg-primary-light text-primary flex items-center justify-center flex-shrink-0">
          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
          </svg>
        </div>
      )}
      <div>
        <p className="text-sm font-medium text-gray-900 group-hover:text-primary transition-colors">{label}</p>
        <p className="text-xs text-gray-500">Click to view</p>
      </div>
      <svg className="w-4 h-4 text-gray-400 ml-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 6H5.25A2.25 2.25 0 003 8.25v10.5A2.25 2.25 0 005.25 21h10.5A2.25 2.25 0 0018 18.75V10.5m-10.5 6L21 3m0 0h-5.25M21 3v5.25" />
      </svg>
    </a>
  );
}

function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: React.ReactNode }) {
  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-white rounded-2xl shadow-2xl border border-gray-100 p-6 w-full max-w-md animate-slide-up">
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-bold text-gray-900">{title}</h3>
          <button onClick={onClose} className="w-8 h-8 rounded-full hover:bg-gray-100 flex items-center justify-center transition-colors">
            <svg className="w-5 h-5 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}
