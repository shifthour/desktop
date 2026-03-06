import React, { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { X, Download, User, Phone, Mail, CheckCircle } from 'lucide-react';
import { submitLead } from '@/lib/supabase';

const downloadBrochure = () => {
  try {
    const link = document.createElement('a');
    link.href = '/Ishtika-Anahata-Brochure.pdf';
    link.download = 'Ishtika-Anahata-Brochure.pdf';
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.style.display = 'none';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    return true;
  } catch (error) {
    console.error('Download error:', error);
    try {
      window.open('/Ishtika-Anahata-Brochure.pdf', '_blank');
      return true;
    } catch (fallbackError) {
      console.error('Fallback also failed:', fallbackError);
      return false;
    }
  }
};

const checkExistingAccess = () => {
  if (typeof window !== 'undefined') {
    return localStorage.getItem('brochureAccess') === 'granted';
  }
  return false;
};

const setAccess = () => {
  if (typeof window !== 'undefined') {
    localStorage.setItem('brochureAccess', 'granted');
  }
};

export default function BrochureDownloadModal({ isOpen, onClose }) {
  const [formData, setFormData] = useState({ name: '', phone: '', email: '' });
  const [step, setStep] = useState('form');
  const [loading, setLoading] = useState(false);

  const handleChange = (e) => {
    setFormData({ ...formData, [e.target.name]: e.target.value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);

    // Save lead to Supabase
    await submitLead({
      name: formData.name,
      email: formData.email,
      phone: formData.phone,
      source: 'brochure_download',
      project_name: 'Anahata',
    });

    // Set access so next time they can download directly
    setAccess();

    // Trigger download
    downloadBrochure();

    setStep('success');
    setTimeout(() => handleClose(), 3000);
    setLoading(false);
  };

  const handleClose = () => {
    setStep('form');
    setFormData({ name: '', phone: '', email: '' });
    onClose();
  };

  useEffect(() => {
    if (isOpen) {
      if (checkExistingAccess()) {
        downloadBrochure();
        setStep('success');
        setTimeout(() => handleClose(), 2000);
      } else {
        setStep('form');
      }
    }
  }, [isOpen]);

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="w-[94vw] max-w-md mx-auto p-0 overflow-hidden bg-white rounded-2xl border-0 shadow-2xl">
        <DialogTitle className="sr-only">Download Brochure</DialogTitle>

        {/* FORM STEP */}
        {step === 'form' && (
          <>
            {/* Header */}
            <div className="relative bg-gradient-to-r from-[#E67E22] to-[#c96b05] px-6 py-8 text-white">
              <button
                onClick={handleClose}
                className="absolute top-4 right-4 text-white/80 hover:text-white"
              >
                <X size={24} />
              </button>
              <div className="text-center">
                <Download size={48} className="mx-auto mb-4" />
                <h2 className="text-2xl font-bold mb-2">Download Brochure</h2>
                <p className="text-white/80 text-sm">Fill in your details to get the Anahata brochure</p>
              </div>
            </div>

            {/* Form */}
            <div className="px-6 py-8">
              <form onSubmit={handleSubmit} className="space-y-5">
                {/* Name */}
                <div className="relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <User className="h-5 w-5 text-gray-400" />
                  </div>
                  <input
                    name="name"
                    placeholder="Enter your full name"
                    value={formData.name}
                    onChange={handleChange}
                    required
                    className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-orange-500 focus:border-transparent transition-all outline-none"
                  />
                </div>

                {/* Phone */}
                <div className="relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <Phone className="h-5 w-5 text-gray-400" />
                  </div>
                  <input
                    name="phone"
                    placeholder="Enter your phone number"
                    value={formData.phone}
                    onChange={handleChange}
                    required
                    className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-orange-500 focus:border-transparent transition-all outline-none"
                  />
                </div>

                {/* Email */}
                <div className="relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <Mail className="h-5 w-5 text-gray-400" />
                  </div>
                  <input
                    name="email"
                    type="email"
                    placeholder="Enter your email address"
                    value={formData.email}
                    onChange={handleChange}
                    required
                    className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-orange-500 focus:border-transparent transition-all outline-none"
                  />
                </div>

                {/* Submit Button */}
                <Button
                  type="submit"
                  disabled={loading}
                  className="w-full bg-gradient-to-r from-[#E67E22] to-[#c96b05] hover:from-[#c96b05] hover:to-[#b05c04] text-white py-3 rounded-lg font-semibold transition-all transform hover:scale-[1.02] disabled:opacity-70"
                >
                  {loading ? (
                    <div className="flex items-center justify-center">
                      <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white mr-2"></div>
                      Processing...
                    </div>
                  ) : (
                    <>
                      <Download className="mr-2" size={18} />
                      Download Brochure
                    </>
                  )}
                </Button>

                <p className="text-xs text-gray-500 text-center">
                  Your information is secure and will be used only to send you project updates.
                </p>
              </form>
            </div>
          </>
        )}

        {/* SUCCESS STEP */}
        {step === 'success' && (
          <div className="px-6 py-12 text-center">
            <CheckCircle className="h-16 w-16 text-[#E67E22] mx-auto mb-4" />
            <h3 className="text-2xl font-bold text-gray-900 mb-2">Download Started!</h3>
            <p className="text-gray-600 mb-4">Your brochure download has begun automatically.</p>
            <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
              <p className="text-orange-800 text-sm font-medium">
                Thank you for your interest in Anahata by Ishtika Homes!
              </p>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
