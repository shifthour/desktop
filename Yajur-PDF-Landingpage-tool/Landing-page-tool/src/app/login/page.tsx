import Image from "next/image";
import { LoginForm } from "@/components/auth/login-form";

export default function LoginPage() {
  return (
    <div className="flex min-h-screen items-center justify-center px-4">
      <div className="w-full max-w-md">
        {/* Logo */}
        <div className="flex flex-col items-center mb-8">
          <div className="relative h-72 w-72 mb-4">
            <Image
              src="/logo.png"
              alt="Yajur Knowledge Solutions"
              fill
              className="object-contain"
            />
          </div>
          <p className="mt-1 text-sm text-gray-500">
            Sign in to your account
          </p>
        </div>

        {/* Login Card */}
        <div className="glass-card rounded-2xl p-8">
          <LoginForm />
        </div>

        <p className="mt-6 text-center text-[11px] text-gray-700">
          Contact your admin if you need an account
        </p>
      </div>
    </div>
  );
}
