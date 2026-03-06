import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { requireAuth } from '@/lib/auth'
import { checkForDuplicates, normalizePhone, normalizeEmail } from '@/lib/dedupe'
import { scoreLead } from '@/lib/scoring'
import { maskPhone, maskEmail } from '@/lib/mask'
import { sendAssignmentEmail } from '@/lib/email'
import { ActivityType, Stage, UserRole } from '@prisma/client'

const createLeadSchema = z.object({
  name: z.string().optional(),
  phone: z.string().min(10, 'Phone number is required'),
  email: z.string().email().optional().or(z.literal('')),
  source: z.string().min(1, 'Source is required'),
  campaign: z.string().optional(),
  adset: z.string().optional(),
  ad: z.string().optional(),
  gclid: z.string().optional(),
  fbclid: z.string().optional(),
  utmSource: z.string().optional(),
  utmMedium: z.string().optional(),
  utmCampaign: z.string().optional(),
  bedroomsPref: z.number().int().positive().optional(),
  budgetMin: z.number().int().positive().optional(),
  budgetMax: z.number().int().positive().optional(),
  locationPref: z.string().optional(),
  projectId: z.string().optional()
})

const listLeadsSchema = z.object({
  stage: z.string().optional(),
  assignedTo: z.string().optional(),
  source: z.string().optional(),
  project: z.string().optional(),
  q: z.string().optional(), // search query
  from: z.string().optional(),
  to: z.string().optional(),
  page: z.string().default('1'),
  limit: z.string().default('50')
})

export async function POST(request: NextRequest) {
  try {
    const session = await requireAuth()
    const body = await request.json()
    const leadData = createLeadSchema.parse(body)

    // Normalize contact information
    const normalizedPhone = normalizePhone(leadData.phone)
    const normalizedEmail = leadData.email ? normalizeEmail(leadData.email) : null

    // Check for duplicates
    const dupeCheck = await checkForDuplicates(
      session.orgId,
      normalizedPhone,
      normalizedEmail || undefined
    )

    // Calculate lead score
    const score = scoreLead({
      ...leadData,
      phone: normalizedPhone,
      email: normalizedEmail,
      duplicateOf: dupeCheck.isDuplicate ? dupeCheck.existingLeadId : null
    })

    // Round-robin assignment to agents
    const agents = await db.user.findMany({
      where: {
        orgId: session.orgId,
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
                stage: {
                  in: [Stage.NEW, Stage.CONTACTED, Stage.SCHEDULED]
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

    const assignedAgent = agents[0] // Agent with least active leads

    // Create the lead
    const lead = await db.lead.create({
      data: {
        orgId: session.orgId,
        name: leadData.name || null,
        phone: normalizedPhone,
        email: normalizedEmail,
        source: leadData.source,
        campaign: leadData.campaign || null,
        adset: leadData.adset || null,
        ad: leadData.ad || null,
        gclid: leadData.gclid || null,
        fbclid: leadData.fbclid || null,
        utmSource: leadData.utmSource || null,
        utmMedium: leadData.utmMedium || null,
        utmCampaign: leadData.utmCampaign || null,
        bedroomsPref: leadData.bedroomsPref || null,
        budgetMin: leadData.budgetMin || null,
        budgetMax: leadData.budgetMax || null,
        locationPref: leadData.locationPref || null,
        projectId: leadData.projectId || null,
        assignedToId: assignedAgent?.id || null,
        duplicateOf: dupeCheck.isDuplicate ? dupeCheck.existingLeadId : null,
        score
      },
      include: {
        project: true,
        assignedTo: true
      }
    })

    // Create assignment activity
    if (assignedAgent) {
      await db.activity.create({
        data: {
          leadId: lead.id,
          userId: assignedAgent.id,
          type: ActivityType.ASSIGNMENT,
          note: `Lead assigned to ${assignedAgent.name}`,
          payload: {
            assignedBy: 'system',
            reason: 'round_robin',
            score: score
          }
        }
      })

      // Send assignment email
      try {
        await sendAssignmentEmail(assignedAgent, lead)
      } catch (error) {
        console.error('Failed to send assignment email:', error)
      }
    }

    return NextResponse.json({
      id: lead.id,
      message: 'Lead created successfully',
      isDuplicate: dupeCheck.isDuplicate,
      duplicateOf: dupeCheck.existingLeadId,
      score,
      assignedTo: assignedAgent?.name
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { message: 'Invalid input', errors: error.errors },
        { status: 400 }
      )
    }

    console.error('Create lead error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}

export async function GET(request: NextRequest) {
  try {
    const session = await requireAuth()
    const { searchParams } = new URL(request.url)
    const params = listLeadsSchema.parse(Object.fromEntries(searchParams))

    const page = parseInt(params.page)
    const limit = Math.min(parseInt(params.limit), 100) // Cap at 100
    const skip = (page - 1) * limit

    // Build where clause
    const where: any = {
      orgId: session.orgId
    }

    if (params.stage) {
      where.stage = params.stage
    }

    if (params.assignedTo) {
      if (params.assignedTo === 'me') {
        where.assignedToId = session.id
      } else {
        where.assignedToId = params.assignedTo
      }
    }

    if (params.source) {
      where.source = params.source
    }

    if (params.project) {
      where.projectId = params.project
    }

    if (params.q) {
      where.OR = [
        { name: { contains: params.q, mode: 'insensitive' } },
        { phone: { contains: params.q } },
        { email: { contains: params.q, mode: 'insensitive' } },
        { campaign: { contains: params.q, mode: 'insensitive' } }
      ]
    }

    if (params.from || params.to) {
      where.createdAt = {}
      if (params.from) {
        where.createdAt.gte = new Date(params.from)
      }
      if (params.to) {
        where.createdAt.lte = new Date(params.to)
      }
    }

    // Get leads with pagination
    const [leads, total] = await Promise.all([
      db.lead.findMany({
        where,
        include: {
          project: {
            select: { id: true, name: true }
          },
          assignedTo: {
            select: { id: true, name: true }
          },
          activities: {
            select: { 
              type: true, 
              at: true, 
              note: true,
              user: {
                select: { name: true }
              }
            },
            orderBy: { at: 'desc' },
            take: 1
          }
        },
        orderBy: [
          { score: 'desc' },
          { createdAt: 'desc' }
        ],
        skip,
        take: limit
      }),
      db.lead.count({ where })
    ])

    // Mask PII in the list view
    const maskedLeads = leads.map(lead => ({
      ...lead,
      phone: maskPhone(lead.phone),
      email: lead.email ? maskEmail(lead.email) : null,
      lastActivity: lead.activities[0] || null,
      activities: undefined // Remove full activities from list
    }))

    return NextResponse.json({
      leads: maskedLeads,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
        hasNext: page * limit < total,
        hasPrev: page > 1
      }
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { message: 'Invalid query parameters', errors: error.errors },
        { status: 400 }
      )
    }

    console.error('List leads error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}