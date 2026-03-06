import { db } from './db'

export interface DedupeResult {
  isDuplicate: boolean
  duplicateOf?: string
  existingLeadId?: string
}

export async function checkForDuplicates(
  orgId: string,
  phone: string,
  email?: string
): Promise<DedupeResult> {
  // Clean phone number for comparison
  const cleanPhone = phone.replace(/\D/g, '')
  
  // First check by phone number (primary duplicate detection)
  const existingByPhone = await db.lead.findFirst({
    where: {
      orgId,
      phone: {
        contains: cleanPhone.slice(-10) // Match last 10 digits
      }
    },
    select: {
      id: true,
      phone: true,
      email: true,
      createdAt: true
    },
    orderBy: {
      createdAt: 'asc' // Get the earliest lead as the original
    }
  })

  if (existingByPhone) {
    return {
      isDuplicate: true,
      duplicateOf: existingByPhone.id,
      existingLeadId: existingByPhone.id
    }
  }

  // Check by email if provided
  if (email) {
    const existingByEmail = await db.lead.findFirst({
      where: {
        orgId,
        email: {
          equals: email,
          mode: 'insensitive'
        }
      },
      select: {
        id: true,
        phone: true,
        email: true,
        createdAt: true
      },
      orderBy: {
        createdAt: 'asc'
      }
    })

    if (existingByEmail) {
      return {
        isDuplicate: true,
        duplicateOf: existingByEmail.id,
        existingLeadId: existingByEmail.id
      }
    }
  }

  return {
    isDuplicate: false
  }
}

export function normalizePhone(phone: string): string {
  // Remove all non-digit characters
  const cleaned = phone.replace(/\D/g, '')
  
  // Handle different formats
  if (cleaned.length === 10) {
    return `+91${cleaned}`
  } else if (cleaned.length === 11 && cleaned.startsWith('0')) {
    // Handle numbers with leading zero (09876543210 -> +919876543210)
    return `+91${cleaned.slice(1)}`
  } else if (cleaned.length === 12 && cleaned.startsWith('91')) {
    return `+${cleaned}`
  } else if (cleaned.length === 13 && cleaned.startsWith('91')) {
    return `+${cleaned.slice(0, -1)}`
  }
  
  // Return as-is if we can't normalize
  return phone
}

export function normalizeEmail(email: string): string {
  return email.toLowerCase().trim()
}

export async function findSimilarLeads(
  orgId: string,
  phone: string,
  email?: string,
  excludeId?: string
): Promise<Array<{
  id: string
  name: string | null
  phone: string
  email: string | null
  source: string
  createdAt: Date
  matchReason: string
}>> {
  const cleanPhone = phone.replace(/\D/g, '')
  const similarLeads = []

  // Find by phone
  const phoneMatches = await db.lead.findMany({
    where: {
      orgId,
      phone: {
        contains: cleanPhone.slice(-10)
      },
      ...(excludeId && { NOT: { id: excludeId } })
    },
    select: {
      id: true,
      name: true,
      phone: true,
      email: true,
      source: true,
      createdAt: true
    }
  })

  similarLeads.push(...phoneMatches.map(lead => ({
    ...lead,
    matchReason: 'Phone number match'
  })))

  // Find by email
  if (email) {
    const emailMatches = await db.lead.findMany({
      where: {
        orgId,
        email: {
          equals: email,
          mode: 'insensitive'
        },
        ...(excludeId && { NOT: { id: excludeId } })
      },
      select: {
        id: true,
        name: true,
        phone: true,
        email: true,
        source: true,
        createdAt: true
      }
    })

    // Add email matches that aren't already included
    const existingIds = new Set(similarLeads.map(lead => lead.id))
    emailMatches.forEach(lead => {
      if (!existingIds.has(lead.id)) {
        similarLeads.push({
          ...lead,
          matchReason: 'Email match'
        })
      }
    })
  }

  return similarLeads.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
}