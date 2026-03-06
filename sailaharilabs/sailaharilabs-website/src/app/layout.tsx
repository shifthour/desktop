import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Header from "@/components/layout/Header";
import Footer from "@/components/layout/Footer";
import WhatsAppButton from "@/components/ui/WhatsAppButton";

const inter = Inter({
  subsets: ["latin"],
  display: "swap",
});

export const metadata: Metadata = {
  title: "Sailahari Labxcite | Precision Laboratory Instruments",
  description: "Discover high-quality laboratory instruments for research, healthcare, and industry. Custom instrumentation, calibration services, and technical support.",
  keywords: "laboratory instruments, lab equipment, calibration services, custom instrumentation, research equipment, technical support",
  authors: [{ name: "Sailahari Labxcite" }],
  creator: "Sailahari Labxcite",
  publisher: "Sailahari Labxcite",
  robots: "index, follow",
  openGraph: {
    type: "website",
    locale: "en_US",
    url: "https://sailaharilabxcite.infinityfree.me",
    siteName: "Sailahari Labxcite",
    title: "Sailahari Labxcite | Precision Laboratory Instruments",
    description: "Discover high-quality laboratory instruments for research, healthcare, and industry.",
  },
};

export const viewport = {
  width: "device-width",
  initialScale: 1,
  themeColor: "#f97066",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.className} antialiased gradient-bg min-h-screen`}>
        <div className="grid-pattern min-h-screen">
          <Header />
          <main>{children}</main>
          <Footer />
          <WhatsAppButton />
        </div>
      </body>
    </html>
  );
}
