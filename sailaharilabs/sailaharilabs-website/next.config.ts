import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Enable optimizations
  compiler: {
    removeConsole: process.env.NODE_ENV === "production",
  },
  
  // Image optimization
  images: {
    unoptimized: true,
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'images.unsplash.com',
      },
      {
        protocol: 'https',
        hostname: 'i0.wp.com',
      },
      {
        protocol: 'https',
        hostname: 'labgig.in',
      },
    ],
  },

  // Performance optimizations
  experimental: {
    optimizePackageImports: ['@heroicons/react'],
  },

  // Headers for better performance and security
  async headers() {
    return [
      {
        source: '/(.*)',
        headers: [
          {
            key: 'X-Frame-Options',
            value: 'DENY',
          },
          {
            key: 'X-Content-Type-Options',
            value: 'nosniff',
          },
          {
            key: 'Referrer-Policy',
            value: 'strict-origin-when-cross-origin',
          },
        ],
      },
    ];
  },
};

export default nextConfig;
