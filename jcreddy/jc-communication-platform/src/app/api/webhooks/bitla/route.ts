import { NextRequest, NextResponse } from 'next/server'

/**
 * Deprecated endpoint - not in use
 * All webhook data should be sent to /api/webhooks/bitla/events
 */
export async function POST(request: NextRequest) {
  // Do not process or store any data - just acknowledge
  return NextResponse.json({
    status: 'deprecated',
    message: 'This endpoint is deprecated. Please use /api/webhooks/bitla/events instead.',
  }, { status: 200 })
}

export async function GET() {
  return NextResponse.json({
    status: 'deprecated',
    message: 'This endpoint is deprecated. Please use /api/webhooks/bitla/events instead.',
    primary_endpoint: '/api/webhooks/bitla/events'
  })
}
