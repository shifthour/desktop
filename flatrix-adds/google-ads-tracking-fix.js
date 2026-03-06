// GOOGLE ADS CONVERSION TRACKING FIX FOR ANAHATA
// Add this to your modern-contact-modal.tsx file

// 1. Add this helper function at the top of the file (after imports)
const trackGoogleAdsConversion = () => {
  if (typeof window !== 'undefined' && window.gtag) {
    // IMPORTANT: Replace YOUR_CONVERSION_LABEL with actual label from Google Ads
    window.gtag('event', 'conversion', {
      'send_to': 'AW-17340305414/YOUR_CONVERSION_LABEL', // ← GET THIS FROM GOOGLE ADS
      'value': 89.0, // Lead value in lakhs
      'currency': 'INR',
      'transaction_id': Date.now().toString() // Unique ID to prevent duplicates
    });
    
    console.log('Google Ads conversion tracked');
  } else {
    console.error('Google gtag not found - conversion not tracked!');
  }
};

// 2. Update your handleSubmit function (line 88 in modern-contact-modal.tsx)
const handleSubmit = async () => {
  setLoading(true)

  // Save lead data
  const leadSaved = await saveLeadData(formData, source, "saved")

  if (leadSaved) {
    console.log("Contact lead data saved successfully")
    
    // Track Google Ads Conversion
    trackGoogleAdsConversion();
    
    // Also track as a Google Analytics event
    if (window.gtag) {
      window.gtag('event', 'generate_lead', {
        'event_category': 'engagement',
        'event_label': source,
        'value': formData.phone
      });
    }
  } else {
    console.warn("Failed to save lead data, but continuing")
  }

  // Set unified access for all restricted content
  setUnifiedAccess()

  // Show success
  setStep("success")
  
  if (onSuccess) {
    onSuccess()
  }

  setTimeout(() => handleClose(), 3000)
  setLoading(false)
}

// 3. Add phone click tracking (add to header.tsx)
const trackPhoneClick = () => {
  if (window.gtag) {
    window.gtag('event', 'click_to_call', {
      'event_category': 'engagement',
      'event_label': 'header_phone_button'
    });
  }
};

// Update phone button in header.tsx:
<a 
  href="tel:+917338628777" 
  onClick={trackPhoneClick}
  className="inline-block"
>
  <Button variant="ghost" size="sm">
    <Phone className="mr-2 h-4 w-4" />
    +91 7338628777
  </Button>
</a>

// 4. Add WhatsApp tracking (in whatsapp-widget.tsx)
const trackWhatsAppClick = () => {
  if (window.gtag) {
    window.gtag('event', 'whatsapp_click', {
      'event_category': 'engagement',
      'event_label': 'floating_widget'
    });
  }
};