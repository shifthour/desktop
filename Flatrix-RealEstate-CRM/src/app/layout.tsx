import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import { Toaster } from 'react-hot-toast'
import { AuthProvider } from '@/contexts/AuthContext'
import ClientLayoutWrapper from '@/components/ClientLayoutWrapper'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'Flatrix CRM - Real Estate Channel Partner Management',
  description: 'Comprehensive CRM solution for real estate channel partners',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <AuthProvider>
          {children}
          <Toaster position="top-right" />
          <ClientLayoutWrapper />
        </AuthProvider>
      </body>
    </html>
  )
}