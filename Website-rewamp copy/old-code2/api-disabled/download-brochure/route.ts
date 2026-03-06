import { type NextRequest, NextResponse } from "next/server"

export async function GET(request: NextRequest) {
  // Always redirect to the static PDF inside /public/brochure/
  const pdfURL = new URL("/brochure/anahata-brochure.pdf", request.url)
  return NextResponse.redirect(pdfURL, 302)
}
