import { Resend } from 'resend'
import { Lead, User, Project, SiteVisit } from '@prisma/client'

const resend = process.env.RESEND_API_KEY ? new Resend(process.env.RESEND_API_KEY) : null

interface LeadWithRelations extends Lead {
  project?: Project | null
  assignedTo?: User | null
}

export async function sendAssignmentEmail(
  agent: User,
  lead: LeadWithRelations
): Promise<boolean> {
  if (!resend) {
    console.warn('RESEND_API_KEY not set, skipping assignment email')
    return false
  }

  try {
    const leadUrl = `${process.env.NEXTAUTH_URL || 'http://localhost:3000'}/leads/${lead.id}`
    
    await resend.emails.send({
      from: 'CRM System <noreply@crm.com>',
      to: [agent.email],
      subject: `New Lead Assigned: ${lead.name || 'Unknown'} - ${lead.source}`,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <h2 style="color: #4f46e5;">New Lead Assignment</h2>
          
          <div style="background: #f8fafc; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h3>Lead Details</h3>
            <p><strong>Name:</strong> ${lead.name || 'Not provided'}</p>
            <p><strong>Phone:</strong> ${lead.phone}</p>
            <p><strong>Email:</strong> ${lead.email || 'Not provided'}</p>
            <p><strong>Source:</strong> ${lead.source}</p>
            ${lead.campaign ? `<p><strong>Campaign:</strong> ${lead.campaign}</p>` : ''}
            ${lead.project ? `<p><strong>Project:</strong> ${lead.project.name}</p>` : ''}
            <p><strong>Score:</strong> ${lead.score}/100</p>
          </div>

          <div style="background: #f0f9ff; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h3>Preferences</h3>
            ${lead.bedroomsPref ? `<p><strong>Bedrooms:</strong> ${lead.bedroomsPref} BHK</p>` : ''}
            ${lead.budgetMin || lead.budgetMax ? 
              `<p><strong>Budget:</strong> ₹${lead.budgetMin ? (lead.budgetMin / 100000).toFixed(0) + 'L' : 'Any'} - ₹${lead.budgetMax ? (lead.budgetMax / 100000).toFixed(0) + 'L' : 'Any'}</p>` 
              : ''
            }
            ${lead.locationPref ? `<p><strong>Location:</strong> ${lead.locationPref}</p>` : ''}
          </div>

          <div style="margin: 30px 0;">
            <a href="${leadUrl}" 
               style="background: #4f46e5; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">
              View Lead Details
            </a>
          </div>

          <p style="color: #6b7280; font-size: 14px;">
            Please follow up on this lead within 15 minutes to maintain our SLA.
          </p>
        </div>
      `
    })

    return true
  } catch (error) {
    console.error('Failed to send assignment email:', error)
    return false
  }
}

export async function sendVisitReminderEmail(
  agent: User,
  lead: LeadWithRelations,
  siteVisit: SiteVisit,
  hoursUntilVisit: number
): Promise<boolean> {
  if (!resend) {
    console.warn('RESEND_API_KEY not set, skipping reminder email')
    return false
  }

  try {
    const leadUrl = `${process.env.NEXTAUTH_URL || 'http://localhost:3000'}/leads/${lead.id}`
    const visitTime = new Intl.DateTimeFormat('en-IN', {
      dateStyle: 'medium',
      timeStyle: 'short',
      timeZone: 'Asia/Kolkata'
    }).format(siteVisit.scheduledAt)

    await resend.emails.send({
      from: 'CRM System <noreply@crm.com>',
      to: [agent.email],
      subject: `Site Visit Reminder: ${lead.name || 'Lead'} - ${hoursUntilVisit}h remaining`,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <h2 style="color: #4f46e5;">Site Visit Reminder</h2>
          
          <div style="background: #fef3c7; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #f59e0b;">
            <h3 style="margin-top: 0;">Upcoming Site Visit in ${hoursUntilVisit} hours</h3>
            <p><strong>Scheduled Time:</strong> ${visitTime}</p>
          </div>

          <div style="background: #f8fafc; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h3>Lead Details</h3>
            <p><strong>Name:</strong> ${lead.name || 'Not provided'}</p>
            <p><strong>Phone:</strong> ${lead.phone}</p>
            <p><strong>Email:</strong> ${lead.email || 'Not provided'}</p>
            ${lead.project ? `<p><strong>Project:</strong> ${lead.project.name}</p>` : ''}
          </div>

          <div style="margin: 30px 0;">
            <a href="${leadUrl}" 
               style="background: #4f46e5; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">
              View Lead Details
            </a>
          </div>

          <p style="color: #6b7280; font-size: 14px;">
            Please confirm the visit with the lead before the scheduled time.
          </p>
        </div>
      `
    })

    return true
  } catch (error) {
    console.error('Failed to send reminder email:', error)
    return false
  }
}

export async function sendDailyDigest(
  agent: User,
  overduLeads: Lead[],
  pendingLeads: Lead[]
): Promise<boolean> {
  if (!resend) {
    console.warn('RESEND_API_KEY not set, skipping digest email')
    return false
  }

  try {
    const leadsUrl = `${process.env.NEXTAUTH_URL || 'http://localhost:3000'}/leads`
    
    await resend.emails.send({
      from: 'CRM System <noreply@crm.com>',
      to: [agent.email],
      subject: `Daily Digest - ${overduLeads.length} overdue, ${pendingLeads.length} pending`,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <h2 style="color: #4f46e5;">Daily Lead Digest</h2>
          
          <div style="display: flex; gap: 20px; margin: 20px 0;">
            <div style="flex: 1; background: #fef2f2; padding: 20px; border-radius: 8px; border-left: 4px solid #ef4444;">
              <h3 style="margin-top: 0; color: #dc2626;">Overdue Leads</h3>
              <div style="font-size: 24px; font-weight: bold;">${overduLeads.length}</div>
            </div>
            <div style="flex: 1; background: #fef3c7; padding: 20px; border-radius: 8px; border-left: 4px solid #f59e0b;">
              <h3 style="margin-top: 0; color: #d97706;">Pending Follow-ups</h3>
              <div style="font-size: 24px; font-weight: bold;">${pendingLeads.length}</div>
            </div>
          </div>

          ${overduLeads.length > 0 ? `
          <div style="margin: 30px 0;">
            <h3>Top Overdue Leads</h3>
            <div style="background: #f8fafc; padding: 15px; border-radius: 8px;">
              ${overduLeads.slice(0, 5).map(lead => `
                <div style="border-bottom: 1px solid #e5e7eb; padding: 10px 0;">
                  <strong>${lead.name || 'Unknown'}</strong><br>
                  <small style="color: #6b7280;">${lead.phone} • ${lead.source} • Score: ${lead.score}</small>
                </div>
              `).join('')}
            </div>
          </div>
          ` : ''}

          <div style="margin: 30px 0;">
            <a href="${leadsUrl}" 
               style="background: #4f46e5; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">
              View All Leads
            </a>
          </div>
        </div>
      `
    })

    return true
  } catch (error) {
    console.error('Failed to send digest email:', error)
    return false
  }
}