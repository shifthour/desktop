import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  images: {
    domains: ['images.unsplash.com', 'picsum.photos'],
    formats: ['image/avif', 'image/webp'],
  },
  reactStrictMode: true,
};

export default nextConfig;
