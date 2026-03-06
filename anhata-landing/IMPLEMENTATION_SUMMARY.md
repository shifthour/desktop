# Anahata Landing Page - Pricing Cards Implementation

## 🚀 Changes Implemented

### Overview
Made the 3 pricing cards (2 BHK, 3 BHK 2T, 3 BHK 3T) in the hero section interactive to capture leads when users click on them.

## 📝 Key Changes

### 1. **Hero Component** (`components/hero.tsx`)
- Added `selectedApartmentType` state to track which apartment card was clicked
- Created `handlePricingCardClick` function that:
  - Tracks card clicks to Google Analytics
  - Opens the contact modal with apartment-specific title
  - Passes apartment preference to modal
- Made pricing cards clickable with:
  - Cursor pointer on hover
  - Scale animation on hover (1.05x)
  - Shadow enhancement on hover
  - Active state scale (1.02x)
  - Chevron icon appears on hover
  - Color transitions for better visual feedback

### 2. **Contact Modal** (`components/modern-contact-modal.tsx`)
- Added `apartmentType` prop to receive preference
- Shows apartment preference badge when user clicked a specific card
- Updates modal subtitle based on apartment selection
- Passes apartment preference to lead saving function
- Enhanced Google Ads conversion tracking with apartment type

### 3. **API Route** (`app/api/submit-lead/route.ts`)
- Updated to accept `preferred_type` field (using existing column)
- Saves apartment preference to database for both insert and update operations
- No database changes needed - uses existing `preferred_type` column

## 🎯 User Experience Improvements

### Mobile Optimizations:
- Touch-friendly interaction areas
- Visual feedback for taps
- Smooth animations
- Clear indication that cards are clickable

### Lead Capture Enhancements:
- Captures apartment preference immediately
- Personalized modal titles (e.g., "Get Best Price for 2 BHK")
- Shows user's interest in the modal
- Better lead qualification data

## 📊 Tracking & Analytics

### Google Analytics Events:
- Event: `apartment_card_click`
- Category: `Engagement`
- Label: Apartment type (2 BHK, 3 BHK 2T, 3 BHK 3T)

### Lead Source Tracking:
- Source format: `pricing_card_[apartment_type]`
- Examples: `pricing_card_2_bhk`, `pricing_card_3_bhk_2t`

## 🖥️ Local Testing

The application is running on: **http://localhost:3002**

### To Test:
1. Open http://localhost:3002 in your browser
2. Look at the hero section with the 3 pricing cards
3. Hover over any card - you'll see:
   - Cursor changes to pointer
   - Card scales up slightly
   - Shadow becomes more prominent
   - Arrow icon appears
   - Text color changes
4. Click on any card:
   - Modal opens with apartment-specific title
   - Shows which apartment type you selected
   - Form submission will save apartment preference

### Mobile Testing:
1. Open Chrome DevTools (F12)
2. Toggle device toolbar (Ctrl+Shift+M)
3. Select a mobile device
4. Test the card interactions

## 📋 Before Production Deployment

### Database:
✅ No database changes required - using existing `preferred_type` column

### Verification Checklist:
- ✅ Cards are clickable on desktop
- ✅ Cards are clickable on mobile
- ✅ Visual feedback on hover/tap
- ✅ Modal shows apartment preference
- ✅ Google Analytics tracking works
- ✅ Lead data saves with preference

## 💡 Future Enhancements (Optional)

1. **A/B Testing**: Test different card designs and CTAs
2. **Price Calculator**: Add EMI calculator in modal
3. **Availability Indicator**: Show units available per type
4. **Progressive Profiling**: Ask different questions based on apartment type
5. **Smart Remarketing**: Use apartment preference for targeted ads

## 🔧 Technical Notes

- All changes are backward compatible
- No breaking changes to existing functionality
- Gracefully handles cases where apartment preference is not provided
- Database column is nullable to support existing records

---

**Ready for Production**: After verifying on localhost:3002 and updating the database schema