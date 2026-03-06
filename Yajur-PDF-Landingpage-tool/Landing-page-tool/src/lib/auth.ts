import bcrypt from "bcryptjs";
import { SignJWT, jwtVerify } from "jose";
import { cookies } from "next/headers";

const COOKIE_NAME = "yajur-session";
const JWT_EXPIRY = "7d";

function getJwtSecret() {
  const secret = process.env.SUPABASE_SERVICE_ROLE_KEY || "dev-fallback-secret-key-min-32-chars!!";
  return new TextEncoder().encode(secret);
}

export async function hashPassword(password: string): Promise<string> {
  return bcrypt.hash(password, 12);
}

export async function verifyPassword(
  password: string,
  hash: string
): Promise<boolean> {
  return bcrypt.compare(password, hash);
}

export async function createSessionToken(
  userId: string,
  email: string,
  role: string
): Promise<string> {
  return new SignJWT({ userId, email, role })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(JWT_EXPIRY)
    .sign(getJwtSecret());
}

export async function verifySessionToken(
  token: string
): Promise<{ userId: string; email: string; role: string } | null> {
  try {
    const { payload } = await jwtVerify(token, getJwtSecret());
    return payload as { userId: string; email: string; role: string };
  } catch {
    return null;
  }
}

export async function getSessionFromCookies(): Promise<{
  userId: string;
  email: string;
  role: string;
} | null> {
  const cookieStore = await cookies();
  const token = cookieStore.get(COOKIE_NAME)?.value;
  if (!token) return null;
  return verifySessionToken(token);
}

export { COOKIE_NAME };
