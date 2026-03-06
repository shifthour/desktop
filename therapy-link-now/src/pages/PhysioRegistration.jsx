import React, { useState } from "react";
import { api as base44 } from "@/api/apiClient";
import { useMutation } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import {
  ArrowLeft, ArrowRight, Check, Upload, Loader2, X, CheckCircle2, FileText
} from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { cn } from "@/lib/utils";

const SPECIALIZATIONS = [
  "Sports Injury", "Neurological", "Orthopedic", "Pediatric",
  "Geriatric", "Cardiopulmonary", "Women's Health", "Respiratory",
];

const DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

export default function PhysioRegistration() {
  const [step, setStep] = useState(1);
  const [submitted, setSubmitted] = useState(false);
  const [error, setError] = useState("");
  const [uploading, setUploading] = useState(false);
  const [uploadingCert, setUploadingCert] = useState(-1);
  const [form, setForm] = useState({
    full_name: "", email: "", phone: "", password: "", bio: "",
    specializations: [], qualifications: [""], certificate_urls: [{ name: "", url: "" }],
    experience_years: "", consultation_fee: "",
    visit_type: "clinic", clinic_name: "", clinic_address: "", city: "",
    available_days: [], working_hours_start: "09:00", working_hours_end: "17:00",
    session_duration: 45, languages: ["English"], photo_url: "",
  });

  const updateForm = (key, value) => setForm(prev => ({ ...prev, [key]: value }));

  const toggleSpec = (spec) => {
    const current = form.specializations;
    updateForm("specializations",
      current.includes(spec) ? current.filter(s => s !== spec) : [...current, spec]
    );
  };

  const toggleDay = (day) => {
    const current = form.available_days;
    updateForm("available_days",
      current.includes(day) ? current.filter(d => d !== day) : [...current, day]
    );
  };

  const handlePhotoUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    const { file_url } = await base44.integrations.Core.UploadFile({ file });
    updateForm("photo_url", file_url);
    setUploading(false);
  };

  const addQualification = () => updateForm("qualifications", [...form.qualifications, ""]);
  const removeQualification = (i) => updateForm("qualifications", form.qualifications.filter((_, idx) => idx !== i));
  const updateQualification = (i, val) => {
    const updated = [...form.qualifications];
    updated[i] = val;
    updateForm("qualifications", updated);
  };

  const addCertificate = () => updateForm("certificate_urls", [...form.certificate_urls, { name: "", url: "" }]);
  const removeCertificate = (i) => updateForm("certificate_urls", form.certificate_urls.filter((_, idx) => idx !== i));
  const handleCertUpload = async (i, e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploadingCert(i);
    try {
      const { file_url } = await base44.integrations.Core.UploadFile({ file });
      const updated = [...form.certificate_urls];
      updated[i] = { name: file.name, url: file_url };
      updateForm("certificate_urls", updated);
    } catch (err) {
      console.error("Certificate upload failed:", err);
    }
    setUploadingCert(-1);
  };

  const registerMutation = useMutation({
    mutationFn: (data) => base44.entities.Physiotherapist.create(data),
    onSuccess: (result) => {
      // Auto-login if token returned (physio account created)
      if (result.token) {
        base44.auth.setToken(result.token);
      }
      setSubmitted(true);
    },
    onError: (err) => {
      setError(err.message || "Registration failed. Please try again.");
    },
  });

  const handleSubmit = () => {
    setError("");
    registerMutation.mutate({
      ...form,
      experience_years: Number(form.experience_years),
      consultation_fee: Number(form.consultation_fee),
      session_duration: Number(form.session_duration),
      qualifications: form.qualifications.filter(q => q.trim()),
      certificate_urls: form.certificate_urls.filter(c => c.url).map(c => ({ name: c.name, url: c.url })),
      status: "pending",
    });
  };

  if (submitted) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
        <motion.div initial={{ opacity: 0, scale: 0.9 }} animate={{ opacity: 1, scale: 1 }} className="bg-white rounded-3xl border border-gray-100 shadow-xl p-8 sm:p-12 max-w-lg w-full text-center">
          <div className="w-20 h-20 rounded-full bg-emerald-100 flex items-center justify-center mx-auto mb-6">
            <CheckCircle2 className="w-10 h-10 text-emerald-600" />
          </div>
          <h2 className="text-2xl font-bold text-gray-900 mb-2">Registration Submitted!</h2>
          <p className="text-gray-400 mb-8">Your profile is under review. We'll notify you once it's approved.</p>
          <a href="/">
            <Button className="bg-teal-600 hover:bg-teal-700 rounded-xl px-8 h-12">Back to Home</Button>
          </a>
        </motion.div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 py-12 px-4">
      <div className="max-w-2xl mx-auto">
        <div className="text-center mb-10">
          <h1 className="text-3xl font-bold text-gray-900">Join PhysioConnect</h1>
          <p className="text-gray-400 mt-2">Register as a physiotherapy professional</p>
        </div>

        {/* Steps */}
        <div className="flex items-center justify-center gap-2 mb-10">
          {["Personal", "Practice", "Schedule"].map((s, i) => (
            <React.Fragment key={s}>
              <div className={cn("flex items-center gap-2 px-4 py-2 rounded-full text-sm font-medium transition-all",
                step > i + 1 ? "bg-teal-600 text-white" :
                step === i + 1 ? "bg-teal-600 text-white" : "bg-white text-gray-400 border border-gray-200"
              )}>
                {step > i + 1 ? <Check className="w-4 h-4" /> : <span>{i + 1}</span>}
                <span className="hidden sm:inline">{s}</span>
              </div>
              {i < 2 && <div className={cn("w-8 h-0.5", step > i + 1 ? "bg-teal-600" : "bg-gray-200")} />}
            </React.Fragment>
          ))}
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700 flex items-start gap-2">
            <X className="w-4 h-4 mt-0.5 flex-shrink-0" />
            <span>{error}</span>
          </div>
        )}

        <AnimatePresence mode="wait">
          {step === 1 && (
            <motion.div key="s1" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="bg-white rounded-2xl border border-gray-100 p-6 sm:p-8 space-y-5">
              <h3 className="text-lg font-bold text-gray-900">Personal Information</h3>

              <div className="flex items-center gap-4">
                <div className="relative w-20 h-20 rounded-xl overflow-hidden bg-gray-100 flex-shrink-0">
                  {form.photo_url ? (
                    <img src={form.photo_url} alt="Profile" className="w-full h-full object-cover" />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center text-gray-300">
                      {uploading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Upload className="w-5 h-5" />}
                    </div>
                  )}
                </div>
                <div>
                  <Label className="text-sm text-gray-700">Profile Photo</Label>
                  <Input type="file" accept="image/*" onChange={handlePhotoUpload} className="mt-1 text-sm" />
                </div>
              </div>

              <div className="grid sm:grid-cols-2 gap-4">
                <div>
                  <Label className="text-sm text-gray-700">Full Name *</Label>
                  <Input value={form.full_name} onChange={e => updateForm("full_name", e.target.value)} className="mt-1 rounded-xl h-12" />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">Email *</Label>
                  <Input type="email" value={form.email} onChange={e => updateForm("email", e.target.value)} className="mt-1 rounded-xl h-12" />
                </div>
              </div>

              <div className="grid sm:grid-cols-2 gap-4">
                <div>
                  <Label className="text-sm text-gray-700">Phone</Label>
                  <Input value={form.phone} onChange={e => updateForm("phone", e.target.value)} className="mt-1 rounded-xl h-12" />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">Password *</Label>
                  <Input type="password" value={form.password} onChange={e => updateForm("password", e.target.value)} className="mt-1 rounded-xl h-12" placeholder="For your physio login" />
                </div>
              </div>

              <div>
                <Label className="text-sm text-gray-700">Bio</Label>
                <Textarea value={form.bio} onChange={e => updateForm("bio", e.target.value)} className="mt-1 rounded-xl min-h-[100px]" placeholder="Tell patients about yourself..." />
              </div>

              <div>
                <Label className="text-sm text-gray-700 mb-2 block">Specializations *</Label>
                <div className="flex flex-wrap gap-2">
                  {SPECIALIZATIONS.map(spec => (
                    <button
                      key={spec}
                      type="button"
                      onClick={() => toggleSpec(spec)}
                      className={cn("px-4 py-2 rounded-xl text-sm font-medium border transition-all",
                        form.specializations.includes(spec)
                          ? "bg-teal-600 text-white border-teal-600"
                          : "bg-white text-gray-600 border-gray-200 hover:border-teal-300"
                      )}
                    >
                      {spec}
                    </button>
                  ))}
                </div>
              </div>

              <div>
                <Label className="text-sm text-gray-700 mb-2 block">Qualifications</Label>
                {form.qualifications.map((q, i) => (
                  <div key={i} className="flex gap-2 mb-2">
                    <Input value={q} onChange={e => updateQualification(i, e.target.value)} placeholder="e.g., BPT, MPT, DPT" className="rounded-xl h-12" />
                    {form.qualifications.length > 1 && (
                      <Button variant="ghost" size="icon" onClick={() => removeQualification(i)} className="flex-shrink-0">
                        <X className="w-4 h-4" />
                      </Button>
                    )}
                  </div>
                ))}
                <Button variant="outline" size="sm" onClick={addQualification} className="rounded-lg text-xs">+ Add More</Button>
              </div>

              <div>
                <Label className="text-sm text-gray-700 mb-2 block">Qualification Certificates</Label>
                <p className="text-xs text-gray-400 mb-3">Upload scanned copies or photos of your certificates (PDF, JPG, PNG)</p>
                {form.certificate_urls.map((cert, i) => (
                  <div key={i} className="flex items-center gap-2 mb-2">
                    {cert.url ? (
                      <div className="flex items-center gap-2 flex-1 bg-teal-50 border border-teal-200 rounded-xl px-4 py-3">
                        <FileText className="w-4 h-4 text-teal-600 flex-shrink-0" />
                        <span className="text-sm text-teal-700 truncate flex-1">{cert.name}</span>
                        <CheckCircle2 className="w-4 h-4 text-teal-600 flex-shrink-0" />
                      </div>
                    ) : (
                      <label className="flex items-center gap-2 flex-1 bg-gray-50 border border-gray-200 border-dashed rounded-xl px-4 py-3 cursor-pointer hover:border-teal-300 transition-colors">
                        {uploadingCert === i ? (
                          <Loader2 className="w-4 h-4 text-teal-600 animate-spin flex-shrink-0" />
                        ) : (
                          <Upload className="w-4 h-4 text-gray-400 flex-shrink-0" />
                        )}
                        <span className="text-sm text-gray-400">
                          {uploadingCert === i ? "Uploading..." : "Choose file"}
                        </span>
                        <input
                          type="file"
                          accept=".pdf,.jpg,.jpeg,.png"
                          onChange={(e) => handleCertUpload(i, e)}
                          className="hidden"
                          disabled={uploadingCert >= 0}
                        />
                      </label>
                    )}
                    {form.certificate_urls.length > 1 && (
                      <Button variant="ghost" size="icon" onClick={() => removeCertificate(i)} className="flex-shrink-0">
                        <X className="w-4 h-4" />
                      </Button>
                    )}
                  </div>
                ))}
                <Button variant="outline" size="sm" onClick={addCertificate} className="rounded-lg text-xs">+ Add More</Button>
              </div>

              <div>
                <Label className="text-sm text-gray-700">Years of Experience</Label>
                <Input type="number" value={form.experience_years} onChange={e => updateForm("experience_years", e.target.value)} className="mt-1 rounded-xl h-12 w-32" />
              </div>

              <Button disabled={!form.full_name || !form.email || !form.password || form.specializations.length === 0} onClick={() => setStep(2)} className="w-full bg-teal-600 hover:bg-teal-700 rounded-xl h-12">
                Continue <ArrowRight className="w-4 h-4 ml-2" />
              </Button>
            </motion.div>
          )}

          {step === 2 && (
            <motion.div key="s2" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="bg-white rounded-2xl border border-gray-100 p-6 sm:p-8 space-y-5">
              <h3 className="text-lg font-bold text-gray-900">Practice Details</h3>

              <div>
                <Label className="text-sm text-gray-700">Consultation Fee (GBP) *</Label>
                <Input type="number" value={form.consultation_fee} onChange={e => updateForm("consultation_fee", e.target.value)} placeholder="e.g., 65" className="mt-1 rounded-xl h-12 w-40" />
              </div>

              <div>
                <Label className="text-sm text-gray-700">Visit Type *</Label>
                <Select value={form.visit_type} onValueChange={v => updateForm("visit_type", v)}>
                  <SelectTrigger className="mt-1 rounded-xl h-12"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="clinic">Clinic Only</SelectItem>
                    <SelectItem value="home_visit">Home Visit Only</SelectItem>
                    <SelectItem value="both">Both</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="grid sm:grid-cols-2 gap-4">
                <div>
                  <Label className="text-sm text-gray-700">Clinic Name</Label>
                  <Input value={form.clinic_name} onChange={e => updateForm("clinic_name", e.target.value)} className="mt-1 rounded-xl h-12" />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">City</Label>
                  <Input value={form.city} onChange={e => updateForm("city", e.target.value)} className="mt-1 rounded-xl h-12" />
                </div>
              </div>

              <div>
                <Label className="text-sm text-gray-700">Clinic Address</Label>
                <Input value={form.clinic_address} onChange={e => updateForm("clinic_address", e.target.value)} className="mt-1 rounded-xl h-12" />
              </div>

              <div>
                <Label className="text-sm text-gray-700">Session Duration (minutes)</Label>
                <Select value={String(form.session_duration)} onValueChange={v => updateForm("session_duration", Number(v))}>
                  <SelectTrigger className="mt-1 rounded-xl h-12 w-40"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="30">30 min</SelectItem>
                    <SelectItem value="45">45 min</SelectItem>
                    <SelectItem value="60">60 min</SelectItem>
                    <SelectItem value="90">90 min</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="flex gap-3">
                <Button variant="outline" onClick={() => setStep(1)} className="rounded-xl h-12 flex-1">
                  <ArrowLeft className="w-4 h-4 mr-2" /> Back
                </Button>
                <Button disabled={!form.consultation_fee} onClick={() => setStep(3)} className="bg-teal-600 hover:bg-teal-700 rounded-xl h-12 flex-1">
                  Continue <ArrowRight className="w-4 h-4 ml-2" />
                </Button>
              </div>
            </motion.div>
          )}

          {step === 3 && (
            <motion.div key="s3" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="bg-white rounded-2xl border border-gray-100 p-6 sm:p-8 space-y-5">
              <h3 className="text-lg font-bold text-gray-900">Schedule & Availability</h3>

              <div>
                <Label className="text-sm text-gray-700 mb-3 block">Available Days *</Label>
                <div className="flex flex-wrap gap-2">
                  {DAYS.map(day => (
                    <button
                      key={day}
                      type="button"
                      onClick={() => toggleDay(day)}
                      className={cn("px-4 py-2 rounded-xl text-sm font-medium border transition-all",
                        form.available_days.includes(day)
                          ? "bg-teal-600 text-white border-teal-600"
                          : "bg-white text-gray-600 border-gray-200 hover:border-teal-300"
                      )}
                    >
                      {day.slice(0, 3)}
                    </button>
                  ))}
                </div>
              </div>

              <div className="grid sm:grid-cols-2 gap-4">
                <div>
                  <Label className="text-sm text-gray-700">Start Time</Label>
                  <Input type="time" value={form.working_hours_start} onChange={e => updateForm("working_hours_start", e.target.value)} className="mt-1 rounded-xl h-12" />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">End Time</Label>
                  <Input type="time" value={form.working_hours_end} onChange={e => updateForm("working_hours_end", e.target.value)} className="mt-1 rounded-xl h-12" />
                </div>
              </div>

              <div className="flex gap-3">
                <Button variant="outline" onClick={() => setStep(2)} className="rounded-xl h-12 flex-1">
                  <ArrowLeft className="w-4 h-4 mr-2" /> Back
                </Button>
                <Button onClick={handleSubmit} disabled={form.available_days.length === 0 || registerMutation.isPending} className="bg-teal-600 hover:bg-teal-700 rounded-xl h-12 flex-1">
                  {registerMutation.isPending ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Check className="w-4 h-4 mr-2" />}
                  Submit Registration
                </Button>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}