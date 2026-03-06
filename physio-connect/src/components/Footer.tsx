import Link from "next/link";
import Image from "next/image";

export default function Footer() {
  return (
    <footer className="bg-navy text-gray-300">
      {/* Top wave */}
      <div className="bg-cream">
        <svg viewBox="0 0 1440 60" fill="none" className="w-full">
          <path d="M0 60L48 53.3C96 46.7 192 33.3 288 26.7C384 20 480 20 576 26.7C672 33.3 768 46.7 864 46.7C960 46.7 1056 33.3 1152 26.7C1248 20 1344 20 1392 20L1440 20V60H1392C1344 60 1248 60 1152 60C1056 60 960 60 864 60C768 60 672 60 576 60C480 60 384 60 288 60C192 60 96 60 48 60H0Z" fill="#1A2332"/>
        </svg>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12 -mt-1">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-8 lg:gap-12">
          {/* Brand */}
          <div className="sm:col-span-2 lg:col-span-1">
            <div className="mb-5 inline-block bg-white rounded-2xl px-5 py-3">
              <Image src="/logo.png" alt="PhysioConnect" width={500} height={180} className="h-20 w-auto" />
            </div>
            <p className="text-sm text-gray-400 leading-relaxed">
              Expert physiotherapy at your doorstep. HCPC-verified physiotherapists across the UK, available for home visits.
            </p>
            <div className="flex gap-3 mt-5">
              {["M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z",
                "M23.953 4.57a10 10 0 01-2.825.775 4.958 4.958 0 002.163-2.723c-.951.555-2.005.959-3.127 1.184a4.92 4.92 0 00-8.384 4.482C7.69 8.095 4.067 6.13 1.64 3.162a4.822 4.822 0 00-.666 2.475c0 1.71.87 3.213 2.188 4.096a4.904 4.904 0 01-2.228-.616v.06a4.923 4.923 0 003.946 4.827 4.996 4.996 0 01-2.212.085 4.936 4.936 0 004.604 3.417 9.867 9.867 0 01-6.102 2.105c-.39 0-.779-.023-1.17-.067a13.995 13.995 0 007.557 2.209c9.053 0 13.998-7.496 13.998-13.985 0-.21 0-.42-.015-.63A9.935 9.935 0 0024 4.59z",
                "M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 01-2.063-2.065 2.064 2.064 0 112.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"
              ].map((path, i) => (
                <a key={i} href="#" className="w-9 h-9 rounded-full bg-navy-light flex items-center justify-center hover:bg-primary transition-colors duration-300">
                  <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d={path} /></svg>
                </a>
              ))}
            </div>
          </div>

          {/* For Patients */}
          <div>
            <h4 className="text-white font-semibold mb-4 text-sm uppercase tracking-wider">For Patients</h4>
            <ul className="space-y-3 text-sm">
              <li><Link href="/search" className="hover:text-primary-300 transition-colors">Find a Physiotherapist</Link></li>
              <li><Link href="/search" className="hover:text-primary-300 transition-colors">Browse Specialisations</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">How It Works</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">Pricing</Link></li>
            </ul>
          </div>

          {/* For Physios */}
          <div>
            <h4 className="text-white font-semibold mb-4 text-sm uppercase tracking-wider">For Physiotherapists</h4>
            <ul className="space-y-3 text-sm">
              <li><Link href="/join" className="hover:text-primary-300 transition-colors">Join as a Physio</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">Physio Dashboard</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">HCPC Requirements</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">Resources</Link></li>
            </ul>
          </div>

          {/* Support */}
          <div>
            <h4 className="text-white font-semibold mb-4 text-sm uppercase tracking-wider">Support</h4>
            <ul className="space-y-3 text-sm">
              <li><Link href="/" className="hover:text-primary-300 transition-colors">Help Centre</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">Contact Us</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">Privacy Policy</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">Terms of Service</Link></li>
              <li><Link href="/" className="hover:text-primary-300 transition-colors">Complaints Procedure</Link></li>
            </ul>
          </div>
        </div>

        {/* Trust badges */}
        <div className="border-t border-gray-700/50 mt-10 pt-8">
          <div className="flex flex-wrap justify-center gap-6 mb-6">
            {[
              { label: "HCPC Registered", icon: "✓" },
              { label: "DBS Checked", icon: "🛡" },
              { label: "Fully Insured", icon: "📋" },
              { label: "ICO Registered", icon: "🔒" },
            ].map((badge) => (
              <div key={badge.label} className="flex items-center gap-2 text-sm text-gray-400">
                <span className="text-primary-300">{badge.icon}</span>
                {badge.label}
              </div>
            ))}
          </div>
          <div className="flex flex-col sm:flex-row justify-between items-center gap-4">
            <p className="text-sm text-gray-500">&copy; 2026 PhysioConnect Ltd. All rights reserved. Registered in England & Wales.</p>
            <p className="text-xs text-gray-600">This platform facilitates booking only. It does not provide medical advice.</p>
          </div>
        </div>
      </div>
    </footer>
  );
}
