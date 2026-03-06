import Link from "next/link"
import { Home, Phone, Mail, ArrowLeft } from "lucide-react"

export default function NotFound() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-gray-100 flex items-center justify-center px-4">
      <div className="max-w-2xl w-full text-center">
        {/* 404 Animation */}
        <div className="relative mb-8">
          <h1 className="text-9xl font-bold text-blue-600 opacity-20">404</h1>
          <div className="absolute inset-0 flex items-center justify-center">
            <Home className="h-20 w-20 text-blue-600 animate-pulse" />
          </div>
        </div>

        {/* Error Message */}
        <h2 className="text-3xl md:text-4xl font-bold text-gray-800 mb-4">
          Oops! Page Not Found
        </h2>
        <p className="text-lg text-gray-600 mb-8">
          The page you're looking for doesn't exist. It might have been moved or deleted.
        </p>

        {/* Quick Links */}
        <div className="space-y-4 mb-8">
          <p className="text-gray-700 font-semibold">Here are some helpful links:</p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              href="/"
              className="inline-flex items-center justify-center px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-semibold transition-colors"
            >
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to Home
            </Link>
            <Link
              href="/#floor_plan"
              className="inline-flex items-center justify-center px-6 py-3 bg-gray-600 hover:bg-gray-700 text-white rounded-lg font-semibold transition-colors"
            >
              View Floor Plans
            </Link>
          </div>
        </div>

        {/* Contact Information */}
        <div className="border-t border-gray-300 pt-8">
          <p className="text-gray-700 font-semibold mb-4">Need Help? Contact Us:</p>
          <div className="flex flex-col sm:flex-row gap-6 justify-center">
            <a
              href="tel:+917338628777"
              className="flex items-center justify-center space-x-2 text-gray-600 hover:text-blue-600 transition-colors"
            >
              <Phone className="h-5 w-5" />
              <span>7338628777</span>
            </a>
            <a
              href="mailto:info@anahata-soukya.in"
              className="flex items-center justify-center space-x-2 text-gray-600 hover:text-blue-600 transition-colors"
            >
              <Mail className="h-5 w-5" />
              <span>info@anahata-soukya.in</span>
            </a>
          </div>
        </div>

        {/* Project Info */}
        <div className="mt-12 p-6 bg-white rounded-lg shadow-md">
          <h3 className="text-xl font-semibold text-gray-800 mb-3">
            About Anahata - Luxury Apartments in Whitefield
          </h3>
          <p className="text-gray-600 mb-4">
            Premium 2BHK & 3BHK apartments starting from ₹89 Lakhs* at Soukya Road, Whitefield, Bangalore.
            Experience luxury living with 50+ amenities across 5 acres.
          </p>
          <Link
            href="/#about"
            className="text-blue-600 hover:text-blue-700 font-semibold"
          >
            Learn More About Anahata →
          </Link>
        </div>
      </div>
    </div>
  )
}