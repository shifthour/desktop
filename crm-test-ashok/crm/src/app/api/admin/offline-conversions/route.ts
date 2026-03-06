import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import { db } from '@/lib/db'
import { requireRole } from '@/lib/auth'
import { Stage, UserRole } from '@prisma/client'

const querySchema = z.object({
  from: z.string().optional(),
  to: z.string().optional(),
  limit: z.string().default('100')
})

export async function GET(request: NextRequest) {
  try {
    // Only admins and managers can access offline conversions
    await requireRole([UserRole.ADMIN, UserRole.MANAGER])
    
    const { searchParams } = new URL(request.url)
    const params = querySchema.parse(Object.fromEntries(searchParams))
    const limit = Math.min(parseInt(params.limit), 500) // Cap at 500

    // Build where clause for booked leads with gclid
    const where: any = {
      stage: Stage.BOOKED,
      gclid: {
        not: null
      }
    }

    if (params.from || params.to) {
      where.updatedAt = {}
      if (params.from) {
        where.updatedAt.gte = new Date(params.from)
      }
      if (params.to) {
        where.updatedAt.lte = new Date(params.to)
      }
    }

    // Get booked leads with Google Ads attribution
    const bookedLeads = await db.lead.findMany({
      where,
      include: {
        project: {
          select: {
            id: true,
            name: true
          }
        },
        activities: {
          where: {
            type: 'STATUS_CHANGE',
            payload: {
              path: ['newStage'],
              equals: Stage.BOOKED
            }
          },
          orderBy: {
            at: 'desc'
          },
          take: 1
        }
      },
      orderBy: {
        updatedAt: 'desc'
      },
      take: limit
    })

    // Format data for Google Ads offline conversion import
    const conversions = bookedLeads.map(lead => {
      const conversionTime = lead.activities[0]?.at || lead.updatedAt
      
      // Google Ads Offline Conversion format
      return {
        // Required fields for Google Ads
        conversionName: 'Property_Booking', // This should match your Google Ads conversion action name
        gclid: lead.gclid,
        conversionTime: conversionTime.toISOString(),
        conversionValue: lead.budgetMax || lead.budgetMin || 10000000, // Use budget as conversion value or default to 1cr
        conversionCurrency: 'INR',
        
        // Optional fields for tracking
        leadId: lead.id,
        leadName: lead.name,
        leadPhone: lead.phone?.slice(-4), // Masked for privacy
        projectName: lead.project?.name,
        source: lead.source,
        campaign: lead.campaign,
        utmCampaign: lead.utmCampaign,
        
        // Additional metadata
        bedroomsPref: lead.bedroomsPref,
        locationPref: lead.locationPref,
        createdAt: lead.createdAt,
        bookedAt: conversionTime
      }
    })

    // Calculate summary statistics
    const summary = {
      totalConversions: conversions.length,
      totalValue: conversions.reduce((sum, conv) => sum + conv.conversionValue, 0),
      avgConversionValue: conversions.length > 0 ? Math.round(conversions.reduce((sum, conv) => sum + conv.conversionValue, 0) / conversions.length) : 0,
      dateRange: {
        from: params.from,
        to: params.to
      }
    }

    // Group by campaign for insights
    const campaignStats = conversions.reduce((acc, conv) => {
      const campaign = conv.campaign || 'Unknown'
      if (!acc[campaign]) {
        acc[campaign] = {
          conversions: 0,
          totalValue: 0
        }
      }
      acc[campaign].conversions++
      acc[campaign].totalValue += conv.conversionValue
      return acc
    }, {} as Record<string, { conversions: number, totalValue: number }>)

    return NextResponse.json({
      conversions,
      summary,
      campaignStats,
      exportInstructions: {
        note: 'This data can be imported into Google Ads for offline conversion tracking',
        requiredColumns: ['conversionName', 'gclid', 'conversionTime', 'conversionValue', 'conversionCurrency'],
        format: 'CSV or JSON',
        documentation: 'https://developers.google.com/google-ads/api/docs/conversions/upload-offline-conversions'
      }
    })

  } catch (error) {
    if (error instanceof z.ZodError) {
      return NextResponse.json(
        { message: 'Invalid query parameters', errors: error.errors },
        { status: 400 }
      )
    }

    console.error('Offline conversions error:', error)
    return NextResponse.json(
      { message: 'Internal server error' },
      { status: 500 }
    )
  }
}