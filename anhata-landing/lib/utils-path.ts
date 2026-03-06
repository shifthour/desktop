// Base path for static assets when deployed to subdirectory
export const BASE_PATH = process.env.NODE_ENV === 'production' ? '/anahata' : ''

// Helper function to get image path with base path
export function getImagePath(path: string): string {
  return `${BASE_PATH}${path}`
}
