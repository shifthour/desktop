import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Ishtika Homes - Premium Real Estate Developer | Luxury Apartments in Bangalore',
  description: 'Ishtika Homes is a leading real estate developer in Bangalore offering premium residential apartments with world-class amenities and modern architecture.',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="bg-gray-50">
        {children}
      </body>
    </html>
  )
}
