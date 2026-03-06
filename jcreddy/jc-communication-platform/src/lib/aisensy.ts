/**
 * AISensy WhatsApp Integration
 * Sends WhatsApp messages via AISensy API
 */

const AISENSY_API_URL = 'https://backend.aisensy.com/campaign/t1/api/v2'
const AISENSY_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NmJhMzNmOTgwODBiMDIwNTRiYWJkMyIsIm5hbWUiOiJNeXRocmkgVG91ciBhbmQgVHJhdmVscyIsImFwcE5hbWUiOiJBaVNlbnN5IiwiY2xpZW50SWQiOiI2NzZiYTMzZjk4MDgwYjAyMDU0YmFiY2QiLCJhY3RpdmVQbGFuIjoiRlJFRV9GT1JFVkVSIiwiaWF0IjoxNzM1MTA3MzkxfQ.wdrr3xoPy1zRU90__wHx-pMzoe6nAmH-MdTHy9mrS4k'

// PRODUCTION MODE - No test phone hardcoding

export interface BookingConfirmationData {
  passengerName: string
  pnr: string
  origin: string
  destination: string
  travelDate: string
  boardingTime?: string
  seatNumber?: string
  boardingPoint?: string
  fare?: number
  coachNumber?: string
}

export interface AiSensyResponse {
  success: boolean
  message?: string
  error?: string
  data?: any
}

/**
 * Send booking confirmation WhatsApp message via AISensy
 * Uses the 'booking_confirmation' campaign template
 */
export async function sendBookingConfirmation(
  bookingData: BookingConfirmationData,
  phoneNumber: string // Required - actual passenger phone number
): Promise<AiSensyResponse> {
  try {
    // Format phone number with country code
    let destination = phoneNumber.replace(/[^0-9]/g, '')
    if (destination.length === 10) {
      destination = '91' + destination
    }

    // Format the date nicely
    const formattedDate = bookingData.travelDate
      ? formatDate(bookingData.travelDate)
      : 'N/A'

    // Format fare with Indian Rupee symbol
    const formattedFare = bookingData.fare
      ? `₹${bookingData.fare.toLocaleString('en-IN')}`
      : 'N/A'

    // Template parameters - these will be substituted in the AISensy template
    // The order matters and must match the template variables {{1}}, {{2}}, etc.
    const templateParams = [
      bookingData.passengerName || 'Valued Customer',           // {{1}} - Passenger name
      bookingData.pnr || 'N/A',                                  // {{2}} - PNR number
      bookingData.origin || 'N/A',                               // {{3}} - Origin city
      bookingData.destination || 'N/A',                          // {{4}} - Destination city
      formattedDate,                                             // {{5}} - Travel date
      bookingData.boardingTime || 'N/A',                         // {{6}} - Boarding time
      bookingData.seatNumber || 'N/A',                           // {{7}} - Seat number
      bookingData.boardingPoint || 'N/A',                        // {{8}} - Boarding point
      formattedFare,                                             // {{9}} - Fare amount
    ]

    const payload = {
      apiKey: AISENSY_API_KEY,
      campaignName: 'booking_confirmtion_testing', // Text-only template (no image header)
      destination: destination,
      userName: bookingData.passengerName || 'Customer',
      templateParams: templateParams,
    }

    console.log('[AISensy] Sending booking confirmation:', {
      destination,
      pnr: bookingData.pnr,
      templateParams,
    })

    const response = await fetch(AISENSY_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    })

    const data = await response.json()

    if (response.ok) {
      console.log('[AISensy] Message sent successfully:', data)
      return {
        success: true,
        message: 'WhatsApp message sent successfully',
        data,
      }
    } else {
      console.error('[AISensy] Failed to send message:', data)
      return {
        success: false,
        error: data.message || data.error || 'Failed to send message',
        data,
      }
    }
  } catch (error: any) {
    console.error('[AISensy] Error sending message:', error)
    return {
      success: false,
      error: error.message || 'Unknown error occurred',
    }
  }
}

/**
 * Format date string to readable format
 * Input: YYYY-MM-DD
 * Output: DD Mon YYYY (e.g., 31 Dec 2024)
 */
function formatDate(dateStr: string): string {
  try {
    const date = new Date(dateStr)
    if (isNaN(date.getTime())) return dateStr

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    const day = date.getDate()
    const month = months[date.getMonth()]
    const year = date.getFullYear()

    return `${day} ${month} ${year}`
  } catch {
    return dateStr
  }
}

/**
 * Format time string from HH:MM:SS to HH:MM AM/PM
 */
export function formatTime(timeStr: string | null): string {
  if (!timeStr) return 'N/A'

  try {
    const [hours, minutes] = timeStr.split(':').map(Number)
    const period = hours >= 12 ? 'PM' : 'AM'
    const displayHours = hours % 12 || 12

    return `${displayHours}:${minutes.toString().padStart(2, '0')} ${period}`
  } catch {
    return timeStr
  }
}

/**
 * Generic WhatsApp message sender via AISensy
 * Can be used with any campaign/template
 */
export async function sendWhatsAppMessage(
  campaignName: string,
  phoneNumber: string,
  templateParams: string[],
  userName?: string
): Promise<AiSensyResponse> {
  try {
    // Format phone number - ensure it starts with country code
    let destination = phoneNumber.replace(/[^0-9]/g, '') // Remove non-digits
    if (destination.length === 10) {
      destination = '91' + destination // Add India country code
    }
    if (!destination.startsWith('91')) {
      destination = '91' + destination
    }

    const payload = {
      apiKey: AISENSY_API_KEY,
      campaignName: campaignName,
      destination: destination,
      userName: userName || 'Customer',
      templateParams: templateParams,
    }

    console.log('[AISensy] Sending WhatsApp message:', {
      campaignName,
      destination,
      templateParams,
    })

    const response = await fetch(AISENSY_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    })

    const data = await response.json()

    if (response.ok && (data.success === 'true' || data.success === true)) {
      console.log('[AISensy] Message sent successfully:', data)
      return {
        success: true,
        message: 'WhatsApp message sent successfully',
        data,
      }
    } else {
      console.error('[AISensy] Failed to send message:', data)
      return {
        success: false,
        error: data.message || data.error || 'Failed to send message',
        data,
      }
    }
  } catch (error: any) {
    console.error('[AISensy] Error sending message:', error)
    return {
      success: false,
      error: error.message || 'Unknown error occurred',
    }
  }
}


/**
 * Send WhatsApp message with optional media attachment
 * Supports video, image, or document attachments
 */
export async function sendWhatsAppMessageWithMedia(
  campaignName: string,
  phoneNumber: string,
  templateParams: string[],
  userName?: string,
  mediaUrl?: string
): Promise<AiSensyResponse> {
  try {
    // Format phone number - ensure it starts with country code
    let destination = phoneNumber.replace(/[^0-9]/g, '') // Remove non-digits
    if (destination.length === 10) {
      destination = '91' + destination // Add India country code
    }
    if (!destination.startsWith('91')) {
      destination = '91' + destination
    }

    const payload: any = {
      apiKey: AISENSY_API_KEY,
      campaignName: campaignName,
      destination: destination,
      userName: userName || 'Customer',
      templateParams: templateParams,
    }

    // Add media if provided
    if (mediaUrl) {
      payload.media = {
        url: mediaUrl,
        filename: 'video.mp4', // AISensy uses this for the file
      }
    }

    console.log('[AISensy] Sending WhatsApp message with media:', {
      campaignName,
      destination,
      templateParams,
      hasMedia: !!mediaUrl,
    })

    const response = await fetch(AISENSY_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    })

    const data = await response.json()

    if (response.ok && (data.success === 'true' || data.success === true)) {
      console.log('[AISensy] Message sent successfully:', data)
      return {
        success: true,
        message: 'WhatsApp message sent successfully',
        data,
      }
    } else {
      console.error('[AISensy] Failed to send message:', data)
      return {
        success: false,
        error: data.message || data.error || 'Failed to send message',
        data,
      }
    }
  } catch (error: any) {
    console.error('[AISensy] Error sending message:', error)
    return {
      success: false,
      error: error.message || 'Unknown error occurred',
    }
  }
}
