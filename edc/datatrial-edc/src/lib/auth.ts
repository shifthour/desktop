import { NextAuthOptions } from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";
import bcrypt from "bcryptjs";
import prisma from "./prisma";

export const authOptions: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      name: "Credentials",
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        if (!credentials?.email || !credentials?.password) {
          throw new Error("Email and password are required");
        }

        const user = await prisma.user.findUnique({
          where: { email: credentials.email },
        });

        if (!user) {
          throw new Error("Invalid email or password");
        }

        if (!user.isActive) {
          throw new Error("Account is deactivated. Please contact administrator.");
        }

        // Check if account is locked
        if (user.lockedUntil && user.lockedUntil > new Date()) {
          throw new Error("Account is locked. Please try again later.");
        }

        const isPasswordValid = await bcrypt.compare(
          credentials.password,
          user.password
        );

        if (!isPasswordValid) {
          // Increment failed login attempts
          const newAttempts = user.failedLoginAttempts + 1;
          const updates: { failedLoginAttempts: number; lockedUntil?: Date } = {
            failedLoginAttempts: newAttempts,
          };

          // Lock account after 3 failed attempts for 30 minutes
          if (newAttempts >= 3) {
            updates.lockedUntil = new Date(Date.now() + 30 * 60 * 1000);
          }

          await prisma.user.update({
            where: { id: user.id },
            data: updates,
          });

          throw new Error("Invalid email or password");
        }

        // Reset failed attempts and update last login
        await prisma.user.update({
          where: { id: user.id },
          data: {
            failedLoginAttempts: 0,
            lockedUntil: null,
            lastLogin: new Date(),
          },
        });

        // Create audit log for login
        await prisma.auditLog.create({
          data: {
            tableName: "User",
            recordId: user.id,
            action: "LOGIN",
            userId: user.id,
            userEmail: user.email,
          },
        });

        return {
          id: user.id,
          email: user.email,
          name: `${user.firstName} ${user.lastName}`,
          firstName: user.firstName,
          lastName: user.lastName,
          mustChangePassword: user.mustChangePassword,
        };
      },
    }),
  ],
  session: {
    strategy: "jwt",
    maxAge: 30 * 60, // 30 minutes session timeout per 21 CFR Part 11
  },
  callbacks: {
    async jwt({ token, user }) {
      if (user) {
        token.id = user.id;
        token.email = user.email;
        token.firstName = (user as { firstName?: string }).firstName;
        token.lastName = (user as { lastName?: string }).lastName;
        token.mustChangePassword = (user as { mustChangePassword?: boolean }).mustChangePassword;
      }
      return token;
    },
    async session({ session, token }) {
      if (token && session.user) {
        (session.user as { id?: string }).id = token.id as string;
        (session.user as { firstName?: string }).firstName = token.firstName as string;
        (session.user as { lastName?: string }).lastName = token.lastName as string;
        (session.user as { mustChangePassword?: boolean }).mustChangePassword = token.mustChangePassword as boolean;
      }
      return session;
    },
  },
  pages: {
    signIn: "/login",
    error: "/login",
  },
};

// Helper to check password requirements (21 CFR Part 11 compliant)
export function validatePassword(password: string): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  if (password.length < 8) {
    errors.push("Password must be at least 8 characters long");
  }
  if (!/[A-Z]/.test(password)) {
    errors.push("Password must contain at least one uppercase letter");
  }
  if (!/[a-z]/.test(password)) {
    errors.push("Password must contain at least one lowercase letter");
  }
  if (!/[0-9]/.test(password)) {
    errors.push("Password must contain at least one number");
  }
  if (!/[!@#$%^&*(),.?":{}|<>]/.test(password)) {
    errors.push("Password must contain at least one special character");
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

export async function hashPassword(password: string): Promise<string> {
  return bcrypt.hash(password, 10);
}
