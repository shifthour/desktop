import { NextRequest, NextResponse } from 'next/server'
import { sendBookingConfirmation, formatTime } from '@/lib/aisensy'

/**
 * Test endpoint to send a WhatsApp booking confirmation
 * POST /api/test/whatsapp
 *
 * Requires phone number in request body
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()

    // Phone number is required
    if (!body.phoneNumber) {
      return NextResponse.json(
        { error: 'phoneNumber is required' },
        { status: 400 }
      )
    }

    // Use provided data or defaults for testing
    const testData = {
      passengerName: body.passengerName || 'Test Passenger',
      pnr: body.pnr || 'MTT-TEST-123456',
      origin: body.origin || 'Hyderabad',
      destination: body.destination || 'Bangalore',
      travelDate: body.travelDate || new Date().toISOString().split('T')[0],
      boardingTime: body.boardingTime || '10:30 PM',
      seatNumber: body.seatNumber || '12A',
      boardingPoint: body.boardingPoint || 'Ameerpet',
      fare: body.fare || 850,
      coachNumber: body.coachNumber || 'AP-09-AB-1234',
    }

    console.log('[Test WhatsApp] Sending test message with data:', testData)

    const result = await sendBookingConfirmation(testData, body.phoneNumber)

    return NextResponse.json({
      message: 'WhatsApp test completed',
      testData,
      result,
    })
  } catch (error: any) {
    console.error('[Test WhatsApp] Error:', error)
    return NextResponse.json(
      { error: error.message || 'Failed to send test message' },
      { status: 500 }
    )
  }
}

/**
 * GET endpoint to check WhatsApp integration status
 */
export async function GET() {
  return NextResponse.json({
    status: 'ready',
    endpoint: '/api/test/whatsapp',
    description: 'Test WhatsApp booking confirmation integration',
    usage: {
      method: 'POST',
      body: {
        phoneNumber: 'REQUIRED - phone number to send message to',
        passengerName: 'optional - defaults to Test Passenger',
        pnr: 'optional - defaults to MTT-TEST-123456',
        origin: 'optional - defaults to Hyderabad',
        destination: 'optional - defaults to Bangalore',
        travelDate: 'optional - defaults to today',
        boardingTime: 'optional - defaults to 10:30 PM',
        seatNumber: 'optional - defaults to 12A',
        boardingPoint: 'optional - defaults to Ameerpet',
        fare: 'optional - defaults to 850',
      },
    },
  })
}
