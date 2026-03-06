/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    serverComponentsExternalPackages: ['pdf-parse']
  },
  api: {
    bodyParser: false,
    responseLimit: '50mb',
  },
}

module.exports = nextConfig
