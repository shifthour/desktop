import type { Metadata } from 'next'
import { Inter, Playfair_Display, Montserrat } from 'next/font/google'
import './globals.css'

const inter = Inter({
  subsets: ['latin'],
  variable: '--font-inter',
  display: 'swap',
})

const playfair = Playfair_Display({
  subsets: ['latin'],
  variable: '--font-playfair',
  display: 'swap',
})

const montserrat = Montserrat({
  subsets: ['latin'],
  variable: '--font-montserrat',
  display: 'swap',
})

export const metadata: Metadata = {
  title: {
    default: 'Ishtika Homes - Premium Real Estate Developer | Luxury Apartments in Bangalore',
    template: '%s | Ishtika Homes'
  },
  description: 'Ishtika Homes is a leading real estate developer in Bangalore offering premium residential apartments. Discover luxury 2BHK, 3BHK & 4BHK apartments with world-class amenities.',
  keywords: ['real estate', 'luxury apartments', 'Bangalore properties', 'residential projects', '2BHK apartments', '3BHK apartments', 'property developer', 'Ishtika Homes'],
  authors: [{ name: 'Ishtika Homes' }],
  creator: 'Ishtika Homes',
  publisher: 'Ishtika Homes',
  formatDetection: {
    email: false,
    address: false,
    telephone: false,
  },
  metadataBase: new URL('https://ishtikahomes.com'),
  alternates: {
    canonical: '/',
  },
  openGraph: {
    type: 'website',
    locale: 'en_IN',
    url: 'https://ishtikahomes.com',
    siteName: 'Ishtika Homes',
    title: 'Ishtika Homes - Premium Real Estate Developer',
    description: 'Discover luxury residential apartments in Bangalore with world-class amenities and modern architecture.',
    images: [
      {
        url: '/images/og-image.jpg',
        width: 1200,
        height: 630,
        alt: 'Ishtika Homes - Luxury Apartments',
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Ishtika Homes - Premium Real Estate Developer',
    description: 'Discover luxury residential apartments in Bangalore',
    images: ['/images/og-image.jpg'],
  },
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      'max-video-preview': -1,
      'max-image-preview': 'large',
      'max-snippet': -1,
    },
  },
  verification: {
    google: 'your-google-verification-code',
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" className="scroll-smooth">
      <head>
        <link rel="icon" href="/favicon.ico" />
        <link rel="apple-touch-icon" href="/apple-touch-icon.png" />
      </head>
      <body className={`${inter.variable} ${playfair.variable} ${montserrat.variable} antialiased`}>
        {children}
      </body>
    </html>
  )
}
