import { NextResponse, type NextRequest } from "next/server";
import { jwtVerify } from "jose";

function getJwtSecret() {
  const secret = process.env.SUPABASE_SERVICE_ROLE_KEY || "dev-fallback-secret-key-min-32-chars!!";
  return new TextEncoder().encode(secret);
}

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Public routes — no auth required
  if (
    pathname.startsWith("/login") ||
    pathname.startsWith("/api/auth") ||
    pathname.startsWith("/api/health") ||
    pathname.startsWith("/api/setup")
  ) {
    // If user is logged in and tries to access login page, redirect to dashboard
    if (pathname.startsWith("/login")) {
      const token = request.cookies.get("yajur-session")?.value;
      if (token) {
        try {
          await jwtVerify(token, getJwtSecret());
          return NextResponse.redirect(new URL("/", request.url));
        } catch {
          // Invalid token, let them see login page
        }
      }
    }
    return NextResponse.next();
  }

  // Protected routes — check JWT
  const token = request.cookies.get("yajur-session")?.value;

  if (!token) {
    return NextResponse.redirect(new URL("/login", request.url));
  }

  try {
    await jwtVerify(token, getJwtSecret());
    return NextResponse.next();
  } catch {
    // Invalid/expired token — redirect to login
    const response = NextResponse.redirect(new URL("/login", request.url));
    response.cookies.delete("yajur-session");
    return response;
  }
}

export const config = {
  matcher: [
    "/((?!_next/static|_next/image|favicon.ico|logo.png|.*\\.(?:svg|png|jpg|jpeg|gif|webp)$).*)",
  ],
};
