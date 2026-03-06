import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { checkForDuplicates, normalizePhone, normalizeEmail } from '@/lib/dedupe'
import { scoreLead } from '@/lib/scoring'
import { sendAssignmentEmail } from '@/lib/email'
import { ActivityType, UserRole } from '@prisma/client'

const metaLeadSchema = z.object({
  leadgen_id: z.string(),
  page_id: z.string(),
  form_id: z.string(),
  adgroup_id: z.string().optional(),
  ad_id: z.string().optional(),
  campaign_id: z.string().optional(),
  field_data: z.array(z.object({
    name: z.string(),
    values: z.array(z.string())
  }))
})

// Simple rate limiting - in production, use Redis or a proper rate limiter
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
    const leadData = metaLeadSchema.parse(body)

    // Extract field data into a more usable format
    const fields = Object.fromEntries(
      leadData.field_data.map(field => [
        field.name.toLowerCase(),
        field.values[0] || ''
      ])
    )

    // Map common field names
    const name = fields.full_name || fields.name || fields.first_name || null
    const phone = fields.phone_number || fields.phone || fields.mobile || null
    const email = fields.email || null
    const budget = fields.budget || fields.budget_range || null
    const bedrooms = fields.bedrooms || fields.bhk || fields.rooms || null
    const location = fields.location || fields.city || fields.area || null

    if (!phone) {
      return NextResponse.json(
        { error: 'Phone number is required' },
        { status: 400 }
      )
    }

    // Default org for webhooks - in production, you might identify org by subdomain or API key
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

    // Parse budget range
    let budgetMin: number | null = null
    let budgetMax: number | null = null
    if (budget) {
      const budgetMatch = budget.match(/(\d+)(?:\s*-\s*(\d+))?(?:\s*(?:lakh|cr|crore))?/i)
      if (budgetMatch) {
        budgetMin = parseInt(budgetMatch[1]) * (budget.includes('cr') ? 10000000 : 100000)
        budgetMax = budgetMatch[2] ? parseInt(budgetMatch[2]) * (budget.includes('cr') ? 10000000 : 100000) : budgetMin
      }
    }

    // Parse bedrooms
    const bedroomsPref = bedrooms ? parseInt(bedrooms.toString()) : null

    const leadPayload = {
      orgId: defaultOrg.id,
      name,
      phone: normalizedPhone,
      email: normalizedEmail,
      source: 'meta',
      campaign: leadData.campaign_id,
      adset: leadData.adgroup_id,
      ad: leadData.ad_id,
      fbclid: `meta_${leadData.leadgen_id}`,
      utmSource: 'facebook',
      utmMedium: 'social',
      bedroomsPref,
      budgetMin,
      budgetMax,
      locationPref: location,
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
        note: `Lead received from Meta webhook (Form: ${leadData.form_id})`,
        payload: {
          source: 'meta_webhook',
          leadgen_id: leadData.leadgen_id,
          form_id: leadData.form_id,
          page_id: leadData.page_id,
          raw_fields: fields,
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

    console.error('Meta webhook error:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}