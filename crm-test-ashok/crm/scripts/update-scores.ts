import { PrismaClient } from '@prisma/client'
import { scoreLead } from '../src/lib/scoring'

const prisma = new PrismaClient()

async function updateLeadScores() {
  console.log('Updating lead scores...')
  
  const leads = await prisma.lead.findMany()
  
  let updated = 0
  
  for (const lead of leads) {
    const newScore = scoreLead(lead)
    
    if (newScore !== lead.score) {
      await prisma.lead.update({
        where: { id: lead.id },
        data: { score: newScore }
      })
      updated++
      
      console.log(`Updated ${lead.name || 'Lead'} (${lead.source}): ${lead.score} -> ${newScore}`)
    }
  }
  
  console.log(`\nUpdated scores for ${updated} leads.`)
}

updateLeadScores()
  .catch((e) => {
    console.error(e)
    process.exit(1)
  })
  .finally(async () => {
    await prisma.$disconnect()
  })