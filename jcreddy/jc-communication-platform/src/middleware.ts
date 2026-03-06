import { NextResponse, type NextRequest } from 'next/server'

export async function middleware(request: NextRequest) {
  const sessionCookie = request.cookies.get('jc_session')

  const isLoginPage = request.nextUrl.pathname === '/login'
  const isApiRoute = request.nextUrl.pathname.startsWith('/api')
  const isPublicAsset = request.nextUrl.pathname.startsWith('/_next') ||
                        request.nextUrl.pathname.startsWith('/favicon') ||
                        request.nextUrl.pathname.includes('.')

  // Allow API routes and public assets
  if (isApiRoute || isPublicAsset) {
    return NextResponse.next()
  }

  // Check if user has a valid session
  const hasSession = sessionCookie?.value ? true : false

  // If user is not logged in and trying to access protected route
  if (!hasSession && !isLoginPage) {
    const url = request.nextUrl.clone()
    url.pathname = '/login'
    return NextResponse.redirect(url)
  }

  // If user is logged in and trying to access login page
  if (hasSession && isLoginPage) {
    const url = request.nextUrl.clone()
    url.pathname = '/'
    return NextResponse.redirect(url)
  }

  return NextResponse.next()
}

export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     */
    '/((?!_next/static|_next/image|favicon.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp)$).*)',
  ],
}
