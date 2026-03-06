// Cloudflare Worker for CRM Cron Jobs
// This worker calls the CRM API endpoints on a schedule

export default {
  async scheduled(event, env, ctx) {
    const baseUrl = env.BASE_URL // Set this as a Worker secret: https://your-app.vercel.app

    if (!baseUrl) {
      console.error('BASE_URL environment variable is not set')
      return
    }

    const results = {
      timestamp: new Date().toISOString(),
      jobs: []
    }

    try {
      // Run SLA check (overdue leads and notifications)
      console.log('Running SLA check...')
      const slaResponse = await fetch(`${baseUrl}/api/cron/sla`, {
        method: 'GET',
        headers: {
          'User-Agent': 'CRM-Cron-Worker/1.0'
        }
      })

      const slaData = await slaResponse.json()
      results.jobs.push({
        name: 'sla-check',
        status: slaResponse.ok ? 'success' : 'failed',
        response: slaData
      })

      if (!slaResponse.ok) {
        console.error('SLA check failed:', slaData)
      } else {
        console.log('SLA check completed:', slaData.processed)
      }

      // Run visit reminders
      console.log('Running visit reminders...')
      const remindersResponse = await fetch(`${baseUrl}/api/cron/reminders`, {
        method: 'GET',
        headers: {
          'User-Agent': 'CRM-Cron-Worker/1.0'
        }
      })

      const remindersData = await remindersResponse.json()
      results.jobs.push({
        name: 'visit-reminders',
        status: remindersResponse.ok ? 'success' : 'failed',
        response: remindersData
      })

      if (!remindersResponse.ok) {
        console.error('Visit reminders failed:', remindersData)
      } else {
        console.log('Visit reminders completed:', remindersData.processed)
      }

    } catch (error) {
      console.error('Cron job execution failed:', error)
      results.jobs.push({
        name: 'error',
        status: 'failed',
        error: error.message
      })
    }

    // Log results for debugging
    console.log('Cron execution results:', results)
    
    return new Response(JSON.stringify(results), {
      headers: { 'Content-Type': 'application/json' }
    })
  },

  // Optional: Handle HTTP requests for manual testing
  async fetch(request, env, ctx) {
    // Allow manual trigger via HTTP for testing
    if (request.method === 'GET' && new URL(request.url).pathname === '/trigger') {
      return await this.scheduled(null, env, ctx)
    }

    return new Response('CRM Cron Worker v1.0\n\nEndpoints:\n- GET /trigger - Manually trigger cron jobs', {
      headers: { 'Content-Type': 'text/plain' }
    })
  }
}