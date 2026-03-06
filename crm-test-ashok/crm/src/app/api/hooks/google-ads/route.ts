import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { checkForDuplicates, normalizePhone, normalizeEmail } from '@/lib/dedupe'
import { scoreLead } from '@/lib/scoring'
import { sendAssignmentEmail } from '@/lib/email'
import { ActivityType, UserRole } from '@prisma/client'

const googleAdsLeadSchema = z.object({
  google_key: z.string(),
  campaign_id: z.string(),
  adgroup_id: z.string().optional(),
  creative_id: z.string().optional(),
  gclid: z.string().optional(),
  user_column_data: z.array(z.object({
    column_id: z.string(),
    string_value: z.string().optional(),
    boolean_value: z.boolean().optional()
  }))
})

// Map Google Ads column IDs to our field names
const COLUMN_ID_MAP: Record<string, string> = {
  'FULL_NAME': 'name',
  'FIRST_NAME': 'firstName',
  'LAST_NAME': 'lastName',
  'EMAIL': 'email',
  'PHONE_NUMBER': 'phone',
  'BUDGET': 'budget',
  'PREFERRED_BEDROOMS': 'bedrooms',
  'PREFERRED_LOCATION': 'location',
  'INTERESTED_IN': 'interest',
  'COMMENTS': 'comments'
}

// Simple rate limiting
const rateLimitMap = new Map<string, { count: number, resetTime: number }>()
const RATE_LIMIT_WINDOW = 60 * 1000 // 1 minute
const RATE_LIMIT_MAX_REQUESTS = 100

function checkRateLimit(ip: string): boolean {
  const now = Date.now()
  const record = rateLimitMap.get(ip)

  if (!record || now > record.resetTime) {
    rateLimitMap.set(ip, { count: 1, resetTime: now + RATE_LIMIT_WINDOW })
    return true
  }

  if (record.count >= RATE_LIMIT_MAX_REQUESTS) {
    return false
  }

  record.count++
  return true
}

export async function POST(request: NextRequest) {
  try {
    // Basic rate limiting
    const ip = request.ip || 'unknown'
    if (!checkRateLimit(ip)) {
      return NextResponse.json(
        { error: 'Rate limit exceeded' },
        { status: 429 }
      )
    }

    const body = await request.json()
    const leadData = googleAdsLeadSchema.parse(body)

    // Extract user data
    const userData: Record<string, string> = {}
    
    leadData.user_column_data.forEach(column => {
      const fieldName = COLUMN_ID_MAP[column.column_id] || column.column_id.toLowerCase()
      if (column.string_value) {
        userData[fieldName] = column.string_value
      } else if (column.boolean_value !== undefined) {
        userData[fieldName] = column.boolean_value.toString()
      }
    })

    // Construct name from first/last name if full name not provided
    let name = userData.name || userData.full_name
    if (!name && (userData.firstName || userData.lastName)) {
      name = [userData.firstName, userData.lastName].filter(Boolean).join(' ')
    }

    const phone = userData.phone
    const email = userData.email || null

    if (!phone) {
      return NextResponse.json(
        { error: 'Phone number is required' },
        { status: 400 }
      )
    }

    // Default org for webhooks
    const defaultOrg = await db.org.findFirst()
    if (!defaultOrg) {
      throw new Error('No organization found')
    }

    // Normalize contact information
    const normalizedPhone = normalizePhone(phone)
    const normalizedEmail = email ? normalizeEmail(email) : null

    // Check for duplicates
    const dupeCheck = await checkForDuplicates(
      defaultOrg.id,
      normalizedPhone,
      normalizedEmail || undefined
    )

    // Parse budget
    let budgetMin: number | null = null
    let budgetMax: number | null = null
    if (userData.budget) {
      const budgetMatch = userData.budget.match(/(\d+)(?:\s*-\s*(\d+))?(?:\s*(?:lakh|cr|crore))?/i)
      if (budgetMatch) {
        budgetMin = parseInt(budgetMatch[1]) * (userData.budget.includes('cr') ? 10000000 : 100000)
        budgetMax = budgetMatch[2] ? parseInt(budgetMatch[2]) * (userData.budget.includes('cr') ? 10000000 : 100000) : budgetMin
      }
    }

    // Parse bedrooms
    const bedroomsPref = userData.bedrooms ? parseInt(userData.bedrooms) : null

    // Extract UTM parameters from gclid or set defaults
    const campaignName = `campaign_${leadData.campaign_id}`
    
    const leadPayload = {
      orgId: defaultOrg.id,
      name: name || null,
      phone: normalizedPhone,
      email: normalizedEmail,
      source: 'google_ads',
      campaign: campaignName,
      adset: leadData.adgroup_id,
      ad: leadData.creative_id,
      gclid: leadData.gclid,
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: campaignName.toLowerCase().replace(/\s+/g, '_'),
      bedroomsPref,
      budgetMin,
      budgetMax,
      locationPref: userData.location || null,
      duplicateOf: dupeCheck.isDuplicate ? dupeCheck.existingLeadId : null
    }

    // Calculate score
    const score = scoreLead(leadPayload)

    // Round-robin assignment
    const agents = await db.user.findMany({
      where: {
        orgId: defaultOrg.id,
        role: UserRole.AGENT
      },
      select: {
        id: true,
        name: true,
        email: true,
        _count: {
          select: {
            assignedLeads: {
              where: {
                createdAt: {
                  gte: new Date(Date.now() - 24 * 60 * 60 * 1000) // Last 24 hours
                }
              }
            }
          }
        }
      },
      orderBy: {
        assignedLeads: {
          _count: 'asc'
        }
      }
    })

    const assignedAgent = agents[0]

    // Create the lead
    const lead = await db.lead.create({
      data: {
        ...leadPayload,
        assignedToId: assignedAgent?.id || null,
        score
      },
      include: {
        project: true,
        assignedTo: true
      }
    })

    // Create webhook activity
    await db.activity.create({
      data: {
        leadId: lead.id,
        type: ActivityType.ASSIGNMENT,
        note: `Lead received from Google Ads webhook`,
        payload: {
          source: 'google_ads_webhook',
          google_key: leadData.google_key,
          campaign_id: leadData.campaign_id,
          gclid: leadData.gclid,
          raw_data: userData,
          assigned_to: assignedAgent?.name
        }
      }
    })

    // Send assignment email
    if (assignedAgent) {
      try {
        await sendAssignmentEmail(assignedAgent, lead)
      } catch (error) {
        console.error('Failed to send assignment email:', error)
      }
    }

    return NextResponse.json({
      success: true,
      leadId: lead.id,
      message: 'Lead processed successfully',
      isDuplicate: dupeCheck.isDuplicate,
      score,
      assignedTo: assignedAgent?.name
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { error: 'Invalid webhook data', details: error.errors },
        { status: 400 }
      )
    }

    console.error('Google Ads webhook error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}