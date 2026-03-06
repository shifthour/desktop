/** @type {import('next-sitemap').IConfig} */
module.exports = {
  siteUrl: 'https://sailaharilabxcite.infinityfree.me',
  generateRobotsTxt: true,
  sitemapSize: 7000,
  changefreq: 'weekly',
  priority: 1.0,
  exclude: ['/admin/*', '/api/*'],
  additionalPaths: async (config) => [
    await config.transform(config, '/'),
    await config.transform(config, '/services'),
    await config.transform(config, '/about'),
    await config.transform(config, '/contact'),
    await config.transform(config, '/case-studies'),
  ],
  robotsTxtOptions: {
    policies: [
      {
        userAgent: '*',
        allow: '/',
        disallow: ['/admin/', '/api/']
      }
    ]
  }
}