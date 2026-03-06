// Google Ads Conversion Tracking for Anahata
// This file handles all conversion tracking across the site

declare global {
  interface Window {
    gtag?: (command: string, targetId: string, config?: any) => void;
    dataLayer?: any[];
  }
}

/**
 * Main conversion tracking function
 * Call this after ANY form submission that saves to Supabase
 * 
 * @param formSource - Where the form was submitted from (brochure, floor_plan, location, site_visit, contact)
 * @param formData - Optional form data for enhanced tracking
 */
export const trackGoogleAdsConversion = (
  formSource: string,
  formData?: { name?: string; phone?: string; email?: string }
) => {
  // Check if we're in the browser
  if (typeof window === 'undefined') {
    console.log('Server-side render - skipping conversion tracking');
    return false;
  }

  // Check if gtag is available
  if (!window.gtag) {
    console.error('Google gtag not available - conversion not tracked');
    return false;
  }

  try {
    // IMPORTANT: You need to get this conversion label from Google Ads
    // Go to: Google Ads → Tools → Conversions → Create/View conversion action
    // The format will be: AW-17340305414/XXXXXXXXXXXXXX
    
    const CONVERSION_ID = 'AW-17340305414'; // Your Google Ads account ID
    const CONVERSION_LABEL = 'L4ByCJ6c-pgbEIagwMxA'; // ← Your actual Google Ads conversion label
    
    // Track the main conversion event for Google Ads
    window.gtag('event', 'conversion', {
      'send_to': `${CONVERSION_ID}/${CONVERSION_LABEL}`,
      'value': 89.0, // Starting price in lakhs
      'currency': 'INR',
      'transaction_id': `${Date.now()}_${formSource}`, // Unique ID to prevent duplicates
    });

    // Also track as an enhanced conversion with user data (if available)
    if (formData?.phone || formData?.email) {
      window.gtag('event', 'conversion', {
        'send_to': `${CONVERSION_ID}/${CONVERSION_LABEL}`,
        'value': 89.0,
        'currency': 'INR',
        'transaction_id': `${Date.now()}_${formSource}_enhanced`,
        'user_data': {
          'phone_number': formData.phone || '',
          'email': formData.email || '',
          'address': {
            'first_name': formData.name?.split(' ')[0] || '',
            'last_name': formData.name?.split(' ').slice(1).join(' ') || '',
            'city': 'Bangalore',
            'region': 'Karnataka',
            'country': 'IN'
          }
        }
      });
    }

    // Track as Google Analytics 4 event for additional insights
    window.gtag('event', 'generate_lead', {
      'event_category': 'Lead Generation',
      'event_label': formSource,
      'value': formData?.phone || '',
      'currency': 'INR',
      'lead_source': formSource,
      'campaign': 'Anahata'
    });

    // Track specific form submissions
    window.gtag('event', `form_submit_${formSource}`, {
      'event_category': 'Form Submission',
      'event_label': formSource,
      'value': 1
    });

    console.log(`✅ Google Ads conversion tracked successfully for: ${formSource}`);
    return true;

  } catch (error) {
    console.error('Error tracking conversion:', error);
    return false;
  }
};

/**
 * Track phone click events
 */
export const trackPhoneClick = (location: string = 'unknown') => {
  if (typeof window !== 'undefined' && window.gtag) {
    window.gtag('event', 'click_to_call', {
      'event_category': 'Phone Engagement',
      'event_label': location,
      'value': '+917338628777'
    });
    console.log(`📞 Phone click tracked from: ${location}`);
  }
};

/**
 * Track WhatsApp click events
 */
export const trackWhatsAppClick = (location: string = 'unknown') => {
  if (typeof window !== 'undefined' && window.gtag) {
    window.gtag('event', 'whatsapp_click', {
      'event_category': 'WhatsApp Engagement',
      'event_label': location,
      'value': 1
    });
    console.log(`💬 WhatsApp click tracked from: ${location}`);
  }
};

/**
 * Track brochure download
 */
export const trackBrochureDownload = () => {
  if (typeof window !== 'undefined' && window.gtag) {
    window.gtag('event', 'download', {
      'event_category': 'Downloads',
      'event_label': 'Brochure',
      'value': 1
    });
    console.log(`📄 Brochure download tracked`);
  }
};

/**
 * Initialize enhanced conversion tracking
 * Call this once when the app loads
 */
export const initializeConversionTracking = () => {
  if (typeof window !== 'undefined' && window.gtag) {
    // Enable enhanced conversions for Google Ads
    window.gtag('config', 'AW-17340305414', {
      'allow_enhanced_conversions': true
    });
    
    // Initialize Google Analytics 4 with enhanced measurement
    window.gtag('config', 'G-W6C28DJ6M3', {
      'send_page_view': true,
      'page_title': document.title,
      'page_location': window.location.href
    });
    
    console.log('🎯 Google Ads conversion tracking initialized');
    console.log('📊 Google Analytics 4 tracking initialized');
  }
};

/**
 * Test function to verify tracking is working
 */
export const testConversionTracking = () => {
  if (typeof window !== 'undefined' && window.gtag) {
    console.log('🧪 Testing conversion tracking...');
    console.log('gtag available:', !!window.gtag);
    console.log('dataLayer available:', !!window.dataLayer);
    
    // Send a test conversion
    trackGoogleAdsConversion('test_form', {
      name: 'Test User',
      phone: '9999999999',
      email: 'test@example.com'
    });
    
    alert('Test conversion sent! Check Google Ads in 5-10 minutes.');
  } else {
    alert('Google tracking not available. Check your implementation.');
  }
};