import { PrismaClient, UserRole, Stage, ActivityType } from '@prisma/client'
import * as argon2 from 'argon2'

const prisma = new PrismaClient()

async function main() {
  // Create organization
  const org = await prisma.org.create({
    data: {
      name: 'Whitefield Real Estate Partners'
    }
  })

  // Create users with hashed passwords (using the same salt as the auth system)
  const salt = process.env.AUTH_PASSWORD_SALT || 'default-salt-change-in-production'
  
  const adminUser = await prisma.user.create({
    data: {
      orgId: org.id,
      email: 'admin@crm.com',
      name: 'Admin User',
      role: UserRole.ADMIN,
      passwordHash: await argon2.hash('admin123' + salt)
    }
  })

  const managerUser = await prisma.user.create({
    data: {
      orgId: org.id,
      email: 'manager@crm.com',
      name: 'Manager User',
      role: UserRole.MANAGER,
      passwordHash: await argon2.hash('manager123' + salt)
    }
  })

  const agentUser = await prisma.user.create({
    data: {
      orgId: org.id,
      email: 'agent@crm.com',
      name: 'Agent User',
      role: UserRole.AGENT,
      passwordHash: await argon2.hash('agent123' + salt)
    }
  })

  // Create projects
  const project1 = await prisma.project.create({
    data: {
      orgId: org.id,
      name: 'Prestige Lakeside Habitat',
      location: 'Whitefield, Bangalore',
      builder: 'Prestige Group'
    }
  })

  const project2 = await prisma.project.create({
    data: {
      orgId: org.id,
      name: 'Brigade Cornerstone Utopia',
      location: 'Whitefield, Bangalore',
      builder: 'Brigade Group'
    }
  })

  // Sample lead data with realistic distribution
  const leadData = [
    // Google Ads leads
    ...Array.from({ length: 25 }, (_, i) => ({
      orgId: org.id,
      projectId: Math.random() > 0.5 ? project1.id : project2.id,
      name: `Google Lead ${i + 1}`,
      phone: `+91987654${String(3210 + i).padStart(4, '0')}`,
      email: `google.lead${i + 1}@email.com`,
      source: 'google_ads',
      campaign: Math.random() > 0.5 ? 'Whitefield_Properties_Search' : 'Premium_Apartments_Bangalore',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: Math.random() > 0.5 ? 'whitefield_search' : 'premium_apartments',
      gclid: `Cj0KCQjw${Math.random().toString(36).substring(7)}`,
      bedroomsPref: Math.random() > 0.3 ? (Math.random() > 0.5 ? 2 : 3) : null,
      budgetMin: Math.random() > 0.5 ? 6000000 : 8000000,
      budgetMax: Math.random() > 0.5 ? 12000000 : 15000000,
      locationPref: Math.random() > 0.3 ? 'Whitefield' : 'Bangalore East',
      assignedToId: agentUser.id
    })),
    
    // Meta leads
    ...Array.from({ length: 20 }, (_, i) => ({
      orgId: org.id,
      projectId: Math.random() > 0.5 ? project1.id : project2.id,
      name: `Facebook Lead ${i + 1}`,
      phone: `+91876543${String(2100 + i).padStart(4, '0')}`,
      email: `meta.lead${i + 1}@email.com`,
      source: 'meta',
      campaign: 'Luxury_Apartments_Whitefield',
      adset: Math.random() > 0.5 ? 'Premium_Properties_25-40' : 'Family_Homes_30-45',
      utmSource: 'facebook',
      utmMedium: 'social',
      fbclid: `IwAR${Math.random().toString(36).substring(7)}`,
      bedroomsPref: Math.random() > 0.2 ? (Math.random() > 0.6 ? 3 : 2) : null,
      budgetMin: Math.random() > 0.5 ? 7000000 : 9000000,
      budgetMax: Math.random() > 0.5 ? 14000000 : 18000000,
      locationPref: 'Whitefield',
      assignedToId: agentUser.id
    })),
    
    // Website leads
    ...Array.from({ length: 15 }, (_, i) => ({
      orgId: org.id,
      projectId: Math.random() > 0.5 ? project1.id : project2.id,
      name: `Website Lead ${i + 1}`,
      phone: `+91765432${String(1000 + i).padStart(4, '0')}`,
      email: `website.lead${i + 1}@email.com`,
      source: 'website',
      utmSource: 'organic',
      utmMedium: 'referral',
      bedroomsPref: Math.random() > 0.4 ? (Math.random() > 0.7 ? 4 : 3) : null,
      budgetMin: Math.random() > 0.5 ? 8000000 : 10000000,
      budgetMax: Math.random() > 0.5 ? 15000000 : 20000000,
      locationPref: Math.random() > 0.5 ? 'Whitefield' : 'Bangalore',
      assignedToId: agentUser.id
    })),
    
    // CSV import leads
    ...Array.from({ length: 20 }, (_, i) => ({
      orgId: org.id,
      projectId: Math.random() > 0.5 ? project1.id : project2.id,
      name: `CSV Lead ${i + 1}`,
      phone: `+91654321${String(500 + i).padStart(4, '0')}`,
      email: `csv.lead${i + 1}@email.com`,
      source: 'csv',
      campaign: 'Database_Import_Q4',
      bedroomsPref: Math.random() > 0.3 ? (Math.random() > 0.5 ? 2 : 3) : null,
      budgetMin: Math.random() > 0.5 ? 5000000 : 7000000,
      budgetMax: Math.random() > 0.5 ? 10000000 : 12000000,
      locationPref: Math.random() > 0.4 ? 'Whitefield' : null,
      assignedToId: agentUser.id
    }))
  ]

  // Create some duplicate leads (same phone numbers)
  const duplicateLeads = [
    {
      ...leadData[0],
      name: 'Duplicate Lead 1',
      email: 'duplicate1@email.com',
      source: 'website',
      duplicateOf: leadData[0].phone
    },
    {
      ...leadData[5],
      name: 'Duplicate Lead 2',
      email: 'duplicate2@email.com',
      source: 'meta',
      duplicateOf: leadData[5].phone
    }
  ]

  leadData.push(...duplicateLeads)

  // Assign realistic stages
  const stages = [Stage.NEW, Stage.CONTACTED, Stage.SCHEDULED, Stage.VISITED, Stage.NEGOTIATION, Stage.BOOKED, Stage.DROPPED]
  const stageWeights = [0.3, 0.25, 0.15, 0.1, 0.08, 0.07, 0.05] // Higher weight for early stages

  leadData.forEach((lead, index) => {
    const random = Math.random()
    let cumulativeWeight = 0
    for (let i = 0; i < stages.length; i++) {
      cumulativeWeight += stageWeights[i]
      if (random <= cumulativeWeight) {
        lead.stage = stages[i]
        break
      }
    }
  })

  // Create leads
  for (const leadInfo of leadData) {
    const lead = await prisma.lead.create({
      data: leadInfo
    })

    // Create initial activity for each lead
    await prisma.activity.create({
      data: {
        leadId: lead.id,
        userId: agentUser.id,
        type: ActivityType.ASSIGNMENT,
        note: 'Lead assigned to agent',
        payload: { assignedBy: 'system', reason: 'initial_assignment' }
      }
    })

    // Add some activities based on stage
    if (lead.stage !== Stage.NEW) {
      await prisma.activity.create({
        data: {
          leadId: lead.id,
          userId: agentUser.id,
          type: ActivityType.CALL,
          note: 'Initial contact call made'
        }
      })
    }

    if ([Stage.SCHEDULED, Stage.VISITED, Stage.NEGOTIATION, Stage.BOOKED].includes(lead.stage)) {
      await prisma.siteVisit.create({
        data: {
          leadId: lead.id,
          projectId: lead.projectId,
          scheduledAt: new Date(Date.now() + Math.random() * 7 * 24 * 60 * 60 * 1000), // Random date within next week
          status: lead.stage === Stage.VISITED ? 'DONE' : 'SCHEDULED'
        }
      })
    }
  }

  console.log('Seed data created successfully!')
  console.log(`Created:`)
  console.log(`- 1 Organization: ${org.name}`)
  console.log(`- 3 Users (admin@crm.com/admin123, manager@crm.com/manager123, agent@crm.com/agent123)`)
  console.log(`- 2 Projects in Whitefield`)
  console.log(`- ${leadData.length} Leads with realistic distribution`)
  console.log(`- Activities and site visits for advanced stage leads`)
}

main()
  .catch((e) => {
    console.error(e)
    process.exit(1)
  })
  .finally(async () => {
    await prisma.$disconnect()
  })