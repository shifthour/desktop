# Complete Google Ads Conversion Tracking Setup for Anahata

## 🎯 STEP 1: Get Your Conversion Label from Google Ads

1. Go to [Google Ads](https://ads.google.com)
2. Click **Tools & Settings** → **Measurement** → **Conversions**
3. Click **+ New conversion action**
4. Select **Website**
5. Configure as follows:
   - **Category**: Submit lead form
   - **Conversion name**: Anahata Lead Form Submission
   - **Value**: Use different values for each conversion → Enter 89
   - **Count**: Every
   - **Click-through conversion window**: 30 days
   - **View-through conversion window**: 1 day
   - **Attribution model**: Data-driven
6. Click **Create and continue**
7. Select **Use Google Tag Manager** or **Install the tag yourself**
8. Copy the conversion ID and label. It will look like:
   ```
   Conversion ID: AW-17340305414
   Conversion label: abcDEF123GHI
   ```

## 🔧 STEP 2: Update the Tracking Code

Open the file: `/Users/safestorage/Desktop/anhata-landing/lib/google-ads-tracking.ts`

Find this line (around line 30):
```typescript
const CONVERSION_LABEL = 'YOUR_CONVERSION_LABEL'; // ← YOU MUST REPLACE THIS
```

Replace it with your actual conversion label:
```typescript
const CONVERSION_LABEL = 'abcDEF123GHI'; // Your actual label from Google Ads
```

## ✅ STEP 3: Files Already Updated

The following files have been updated with conversion tracking:

1. ✅ **modern-contact-modal.tsx** - Site visit & general contact forms
2. ✅ **brochure-download-modal.tsx** - Brochure download form
3. ⚠️ **floor-plans-access-modal.tsx** - Needs Supabase integration
4. ⚠️ **location-access-modal.tsx** - Needs Supabase integration

## 🔨 STEP 4: Fix Remaining Forms

The floor-plans and location modals are NOT saving to Supabase. They need to be updated.

### Update floor-plans-access-modal.tsx:

Replace the handleSubmit function with:

```typescript
import { trackGoogleAdsConversion } from '@/lib/google-ads-tracking'

// Add this function for saving to Supabase
const saveLeadData = async (
  formData: { name: string; phone: string; email: string },
  source: string
) => {
  try {
    const response = await fetch("/api/submit-lead", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: formData.name,
        phone: formData.phone,
        email: formData.email,
        source: source,
        status_of_save: "saved",
        action: "insert",
      }),
    })
    return (await response.json()).success
  } catch (error) {
    console.error("Error saving lead:", error)
    return false
  }
}

const handleSubmit = async (e: React.FormEvent) => {
  e.preventDefault()
  setIsLoading(true)

  // Save to Supabase
  const saved = await saveLeadData(formData, "floor_plans")
  
  if (saved) {
    // Track conversion
    trackGoogleAdsConversion('floor_plans', formData)
  }

  setIsLoading(false)
  setIsSubmitted(true)
  
  setTimeout(() => {
    onSuccess()
    setIsSubmitted(false)
    setFormData({ name: "", phone: "", email: "" })
    onClose()
  }, 2000)
}
```

### Update location-access-modal.tsx:

Same changes as above, but use `source: "location"` instead.

## 📊 STEP 5: Initialize Tracking on App Load

Add this to your `/Users/safestorage/Desktop/anhata-landing/app/layout.tsx`:

```typescript
import { initializeConversionTracking } from '@/lib/google-ads-tracking'

// In the RootLayout component, add:
useEffect(() => {
  initializeConversionTracking()
}, [])
```

## 🧪 STEP 6: Test Your Implementation

1. **Install Google Tag Assistant** Chrome extension
2. **Open your website** with Tag Assistant enabled
3. **Submit a test form** with test data:
   - Name: Test User
   - Phone: 9999999999
   - Email: test@example.com
4. **Check Tag Assistant** - You should see:
   - ✅ Google Ads Conversion (AW-17340305414)
   - ✅ Google Analytics 4 (if configured)
5. **Wait 5-10 minutes** and check Google Ads:
   - Go to Campaigns → Your campaign
   - Check if conversion shows up

## 📈 STEP 7: Monitor Performance

After implementation, monitor these metrics:

### Expected Improvements (Within 24-48 hours):
- **Conversion Rate**: From 0.03% → 1-3%
- **Cost Per Conversion**: From ₹9,344 → ₹500-1,500
- **Quality Score**: Will improve as Google sees conversions

### Google Ads Dashboard Checks:
1. **Tools → Conversions**: Should show your new conversion action
2. **Campaigns → Columns → Modify columns**: Add "Conversions" column
3. **Keywords**: Quality scores should start improving

## 🚨 IMPORTANT NOTES

1. **Conversion Label is CRITICAL** - Without the correct label, tracking won't work
2. **Test with real phone numbers** - Google may validate phone formats
3. **Use unique transaction IDs** - Prevents duplicate conversions
4. **Enhanced conversions** - We're sending user data for better matching

## 🎯 Quick Checklist

- [ ] Get conversion label from Google Ads
- [ ] Update `CONVERSION_LABEL` in google-ads-tracking.ts
- [ ] Update floor-plans-access-modal.tsx
- [ ] Update location-access-modal.tsx
- [ ] Test with Tag Assistant
- [ ] Verify in Google Ads after 10 minutes
- [ ] Monitor for 24 hours

## 💡 Troubleshooting

If conversions aren't showing:

1. **Check Console** - Look for errors in browser console
2. **Verify gtag exists** - Type `window.gtag` in console
3. **Check Network tab** - Look for requests to `www.googleadservices.com`
4. **Test function** - Run `testConversionTracking()` in console
5. **Check ad blockers** - Disable them during testing

## 📞 Support

If you need help:
1. Check Google Ads Help: https://support.google.com/google-ads
2. Use Tag Assistant for debugging
3. Check conversion status in Google Ads after 3 hours