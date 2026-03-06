import { NextResponse } from 'next/server'

export async function GET() {
  const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" 
        xmlns:image="http://www.google.com/schemas/sitemap-image/1.1"
        xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">

  <!-- Main Homepage -->
  <url>
    <loc>https://www.anahata-soukya.in</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
    <image:image>
      <image:loc>https://www.anahata-soukya.in/images/gallery/aerial-complex-view.png</image:loc>
      <image:title>Anahata - Premium Apartments in Whitefield</image:title>
      <image:caption>Aerial view of Anahata luxury apartment complex in Soukya Road, Whitefield, Bangalore</image:caption>
    </image:image>
  </url>

  <!-- Pricing Section -->
  <url>
    <loc>https://www.anahata-soukya.in#pricing</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.9</priority>
  </url>

  <!-- About Section -->
  <url>
    <loc>https://www.anahata-soukya.in#about</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
  </url>

  <!-- Floor Plans -->
  <url>
    <loc>https://www.anahata-soukya.in#floor-plans</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
    <image:image>
      <image:loc>https://www.anahata-soukya.in/images/floorplans/2bhk-floorplan.png</image:loc>
      <image:title>2BHK Floor Plan - Anahata Whitefield</image:title>
    </image:image>
    <image:image>
      <image:loc>https://www.anahata-soukya.in/images/floorplans/3bhk-2t-floorplan.png</image:loc>
      <image:title>3BHK 2T Floor Plan - Anahata Whitefield</image:title>
    </image:image>
    <image:image>
      <image:loc>https://www.anahata-soukya.in/images/floorplans/3bhk-3t-floorplan.png</image:loc>
      <image:title>3BHK 3T Floor Plan - Anahata Whitefield</image:title>
    </image:image>
  </url>

  <!-- Gallery -->
  <url>
    <loc>https://www.anahata-soukya.in#gallery</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
    <image:image>
      <image:loc>https://www.anahata-soukya.in/images/gallery/landscaped-pathway.png</image:loc>
      <image:title>Landscaped Pathway - Anahata Whitefield</image:title>
    </image:image>
    <image:image>
      <image:loc>https://www.anahata-soukya.in/images/gallery/modern-clubhouse.png</image:loc>
      <image:title>Modern Clubhouse - Anahata Whitefield</image:title>
    </image:image>
    <image:image>
      <image:loc>https://www.anahata-soukya.in/images/gallery/swimming-pool.png</image:loc>
      <image:title>Swimming Pool - Anahata Whitefield</image:title>
    </image:image>
  </url>

  <!-- Amenities -->
  <url>
    <loc>https://www.anahata-soukya.in#amenities</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.7</priority>
  </url>

  <!-- Location -->
  <url>
    <loc>https://www.anahata-soukya.in#location</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>rarely</changefreq>
    <priority>0.6</priority>
  </url>

  <!-- FAQ -->
  <url>
    <loc>https://www.anahata-soukya.in#faq</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.6</priority>
  </url>

  <!-- AI Context Files -->
  <url>
    <loc>https://www.anahata-soukya.in/.well-known/llms.txt</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.5</priority>
  </url>

  <url>
    <loc>https://www.anahata-soukya.in/.well-known/ai-context.json</loc>
    <lastmod>${new Date().toISOString()}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.5</priority>
  </url>

</urlset>`

  return new NextResponse(sitemap, {
    status: 200,
    headers: {
      'Content-Type': 'application/xml',
      'Cache-Control': 'public, max-age=86400, s-maxage=86400', // 24 hours
    },
  })
}