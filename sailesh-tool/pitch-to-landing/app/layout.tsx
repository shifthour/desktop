import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Pitch to Landing | Convert PDFs to Interactive Landing Pages',
  description: 'Upload your pitch deck PDF and get an interactive landing page deployed to Vercel in seconds.',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
      </head>
      <body className="min-h-screen bg-gray-50">
        {children}
      </body>
    </html>
  )
}
