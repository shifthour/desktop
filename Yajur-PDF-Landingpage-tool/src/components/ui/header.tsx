"use client";

import Image from "next/image";
import { useRouter } from "next/navigation";
import Link from "next/link";

export function Header() {
  const router = useRouter();

  return (
    <header className="sticky top-0 z-50 border-b border-dark-border bg-dark-bg/80 backdrop-blur-xl">
      <div className="mx-auto flex h-20 max-w-7xl items-center justify-between px-6">
        <Link href="/" className="flex items-center gap-4 group">
          <div className="relative h-14 w-44 overflow-hidden">
            <Image
              src="/logo.png"
              alt="Yajur Knowledge Solutions"
              fill
              className="object-contain object-left"
              priority
            />
          </div>
        </Link>

        <nav className="flex items-center gap-4">
          <button
            onClick={() => router.push("/")}
            className="text-sm text-gray-400 transition-colors hover:text-white cursor-pointer"
          >
            Dashboard
          </button>
          <Link
            href="/project/new"
            className="gradient-brand rounded-lg px-4 py-2 text-sm font-semibold text-white transition-all hover:opacity-90 hover:shadow-lg hover:shadow-brand-purple/25"
          >
            + New Project
          </Link>
        </nav>
      </div>
    </header>
  );
}
