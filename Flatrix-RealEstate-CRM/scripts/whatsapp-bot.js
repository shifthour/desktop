/**
 * WhatsApp Automation Bot for Flatrix Real Estate CRM
 *
 * This script connects to WhatsApp Web and automatically sends
 * greeting messages + brochure + video to new CRM leads.
 *
 * Usage:
 *   node scripts/whatsapp-bot.js
 *
 * First run: Scan the QR code with your WhatsApp phone.
 * Subsequent runs: Auto-connects using saved session.
 */

const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const { createClient } = require('@supabase/supabase-js');
const path = require('path');

// Load env vars from .env.local
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

// ============================================
// Configuration
// ============================================
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const POLL_INTERVAL = 30000; // Check for new leads every 30 seconds
const DELAY_BETWEEN_MESSAGES = 3000; // 3s between each message to same lead
const DELAY_BETWEEN_LEADS = 5000; // 5s between different leads

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env.local');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ============================================
// Media files (in project folder)
// ============================================
const BROCHURE_PATH = path.join(__dirname, '..', 'public', 'whatsapp-media', 'Ishtika_Anahata_Brochure.pdf');
const VIDEO_PATH = path.join(__dirname, '..', 'public', 'whatsapp-media', 'Anahata_Video_Tour.mp4');

// ============================================
// Message templates
// ============================================
function getAnahataMessage(name) {
  return `Dear ${name},

Thank you for your interest in Ishtika Anahata, located on Soukya road in Whitefield, a peaceful, well-connected community offering spacious homes with higher ceilings and quality finishes.

Here's a quick Video Tour to help you experience the project from wherever you are.

Regards,
Anahata Team`;
}

function getDefaultMessage(name) {
  return `Dear ${name},

We got to know that you're exploring options for a new flat in Whitefield, Bangalore.
We'd love to introduce you to Ishtika Anahata, our premium residential project located on *Soukya Road, Whitefield* — a peaceful, well-connected community offering spacious homes with higher ceilings and quality finishes.

Here's a quick Video Tour to help you experience the project from wherever you are.

Regards,
Anahata Team`;
}

function isAnahataProject(projectName) {
  if (!projectName) return false;
  return projectName.toLowerCase().includes('anahata');
}

// ============================================
// Phone number formatting
// ============================================
function formatPhone(phone) {
  let cleaned = phone.replace(/[\s\-\(\)\+]/g, '');
  // If 10-digit Indian number, prepend 91
  if (cleaned.length === 10 && /^[6-9]/.test(cleaned)) {
    cleaned = '91' + cleaned;
  }
  return cleaned;
}

// ============================================
// Database helpers
// ============================================
async function getUnmessagedLeads() {
  // Get leads created in last 24 hours with status NEW
  const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  const { data: leads, error } = await supabase
    .from('flatrix_leads')
    .select('id, name, phone, project_name, status, created_at')
    .eq('status', 'NEW')
    .gte('created_at', twentyFourHoursAgo)
    .order('created_at', { ascending: true });

  if (error) {
    console.error('[DB] Error fetching leads:', error.message);
    return [];
  }
  if (!leads || leads.length === 0) return [];

  // Check which leads have already been messaged
  const leadIds = leads.map(l => l.id);
  const { data: logs } = await supabase
    .from('flatrix_whatsapp_logs')
    .select('lead_id')
    .in('lead_id', leadIds)
    .eq('message_type', 'greeting');

  const messagedIds = new Set((logs || []).map(l => l.lead_id));
  return leads.filter(l => l.phone && !messagedIds.has(l.id));
}

async function logMessage(lead, messageType, status, errorMessage = null) {
  await supabase.from('flatrix_whatsapp_logs').insert({
    lead_id: lead.id,
    lead_phone: lead.phone,
    lead_name: lead.name,
    project_name: lead.project_name,
    message_type: messageType,
    wa_template_name: isAnahataProject(lead.project_name) ? 'anahata_script' : 'default_script',
    status: status,
    error_message: errorMessage,
    sent_at: status === 'sent' ? new Date().toISOString() : null,
    created_at: new Date().toISOString(),
  });
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================
// WhatsApp Client
// ============================================
const client = new Client({
  authStrategy: new LocalAuth({
    dataPath: path.join(__dirname, '..', '.wwebjs_auth'),
  }),
  puppeteer: {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  },
});

let isReady = false;

client.on('qr', (qr) => {
  console.log('\n========================================');
  console.log('  Scan this QR code with your WhatsApp');
  console.log('========================================\n');
  qrcode.generate(qr, { small: true });
  console.log('\nWaiting for scan...\n');
});

client.on('authenticated', () => {
  console.log('[WA] Authenticated successfully');
});

client.on('auth_failure', (msg) => {
  console.error('[WA] Authentication failed:', msg);
});

client.on('ready', () => {
  isReady = true;
  console.log('\n========================================');
  console.log('  WhatsApp Bot is READY!');
  console.log('  Checking for new leads every 30s...');
  console.log('========================================\n');
  startPolling();
});

client.on('disconnected', (reason) => {
  isReady = false;
  console.log('[WA] Disconnected:', reason);
  console.log('[WA] Attempting to reconnect...');
  client.initialize();
});

// ============================================
// Send messages to a lead
// ============================================
async function sendMessagesToLead(lead) {
  const phone = formatPhone(lead.phone);
  const chatId = `${phone}@c.us`;
  const name = lead.name || 'there';
  const anahata = isAnahataProject(lead.project_name);

  console.log(`\n[SENDING] ${lead.name} (${lead.phone}) - Project: ${lead.project_name || 'N/A'} [${anahata ? 'Anahata' : 'Default'}]`);

  try {
    // Check if number is registered on WhatsApp
    const isRegistered = await client.isRegisteredUser(chatId);
    if (!isRegistered) {
      console.log(`  [SKIP] ${phone} is not on WhatsApp`);
      await logMessage(lead, 'greeting', 'failed', 'Number not registered on WhatsApp');
      return;
    }

    // 1. Send greeting message
    const message = anahata ? getAnahataMessage(name) : getDefaultMessage(name);
    await client.sendMessage(chatId, message);
    await logMessage(lead, 'greeting', 'sent');
    console.log('  [OK] Greeting sent');

    await delay(DELAY_BETWEEN_MESSAGES);

    // 2. Send brochure PDF
    try {
      const brochure = MessageMedia.fromFilePath(BROCHURE_PATH);
      await client.sendMessage(chatId, brochure, {
        caption: 'Ishtika Anahata - Project Brochure',
      });
      await logMessage(lead, 'brochure', 'sent');
      console.log('  [OK] Brochure sent');
    } catch (err) {
      console.error('  [FAIL] Brochure:', err.message);
      await logMessage(lead, 'brochure', 'failed', err.message);
    }

    await delay(DELAY_BETWEEN_MESSAGES);

    // 3. Send video
    try {
      const video = MessageMedia.fromFilePath(VIDEO_PATH);
      await client.sendMessage(chatId, video, {
        caption: 'Ishtika Anahata - Video Tour',
      });
      await logMessage(lead, 'video', 'sent');
      console.log('  [OK] Video sent');
    } catch (err) {
      console.error('  [FAIL] Video:', err.message);
      await logMessage(lead, 'video', 'failed', err.message);
    }

    console.log(`  [DONE] All messages sent to ${lead.name}`);
  } catch (err) {
    console.error(`  [ERROR] Failed for ${lead.name}:`, err.message);
    await logMessage(lead, 'greeting', 'failed', err.message);
  }
}

// ============================================
// Polling loop
// ============================================
async function pollForNewLeads() {
  if (!isReady) return;

  try {
    const leads = await getUnmessagedLeads();

    if (leads.length > 0) {
      console.log(`\n[POLL] Found ${leads.length} new lead(s) to message`);
      for (const lead of leads) {
        await sendMessagesToLead(lead);
        await delay(DELAY_BETWEEN_LEADS);
      }
    }
  } catch (err) {
    console.error('[POLL] Error:', err.message);
  }
}

function startPolling() {
  // Run immediately on start
  pollForNewLeads();
  // Then poll every 30 seconds
  setInterval(pollForNewLeads, POLL_INTERVAL);
}

// ============================================
// Start
// ============================================
console.log('\n========================================');
console.log('  Flatrix CRM - WhatsApp Bot');
console.log('========================================');
console.log('  Brochure:', BROCHURE_PATH);
console.log('  Video:', VIDEO_PATH);
console.log('  Poll interval:', POLL_INTERVAL / 1000 + 's');
console.log('========================================\n');
console.log('Initializing WhatsApp connection...\n');

client.initialize();

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down...');
  await client.destroy();
  process.exit(0);
});
