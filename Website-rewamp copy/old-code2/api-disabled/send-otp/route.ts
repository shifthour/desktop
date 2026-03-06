import { NextResponse } from "next/server"

export async function POST(request: Request) {
  const { phone } = await request.json()

  if (!phone) {
    return NextResponse.json({ success: false, error: "Phone number missing" }, { status: 400 })
  }

  // BYPASS: Always return success for any phone number
  console.log("OTP Bypass Active - Simulating successful OTP send for:", phone)
  return NextResponse.json({ 
    success: true, 
    message: "OTP sent successfully (bypass mode)",
    bypassMode: true 
  })
}