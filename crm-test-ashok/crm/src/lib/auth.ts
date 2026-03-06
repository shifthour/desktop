import { User, UserRole } from '@prisma/client'
import * as argon2 from 'argon2'
import { cookies } from 'next/headers'
import { redirect } from 'next/navigation'
import { db } from './db'

export interface SessionUser {
  id: string
  email: string
  name: string
  role: UserRole
  orgId: string
}

export async function hashPassword(password: string): Promise<string> {
  const salt = process.env.AUTH_PASSWORD_SALT || 'default-salt-change-in-production'
  return await argon2.hash(password + salt)
}

export async function verifyPassword(password: string, hashedPassword: string): Promise<boolean> {
  const salt = process.env.AUTH_PASSWORD_SALT || 'default-salt-change-in-production'
  try {
    return await argon2.verify(hashedPassword, password + salt)
  } catch {
    return false
  }
}

export async function login(email: string, password: string): Promise<SessionUser | null> {
  const user = await db.user.findUnique({
    where: { email },
    include: { org: true }
  })

  if (!user || !(await verifyPassword(password, user.passwordHash))) {
    return null
  }

  return {
    id: user.id,
    email: user.email,
    name: user.name,
    role: user.role,
    orgId: user.orgId
  }
}

export async function setSession(user: SessionUser) {
  const session = JSON.stringify(user)
  const cookieStore = await cookies()
  cookieStore.set('session', session, {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: 60 * 60 * 24 * 7 // 7 days
  })
}

export async function getSession(): Promise<SessionUser | null> {
  try {
    const cookieStore = await cookies()
    const sessionCookie = cookieStore.get('session')
    
    if (!sessionCookie?.value) {
      return null
    }

    return JSON.parse(sessionCookie.value) as SessionUser
  } catch {
    return null
  }
}

export async function logout() {
  const cookieStore = await cookies()
  cookieStore.delete('session')
}

export async function requireAuth(): Promise<SessionUser> {
  const session = await getSession()
  if (!session) {
    redirect('/login')
  }
  return session
}

export async function requireRole(allowedRoles: UserRole[]): Promise<SessionUser> {
  const session = await requireAuth()
  if (!allowedRoles.includes(session.role)) {
    redirect('/')
  }
  return session
}