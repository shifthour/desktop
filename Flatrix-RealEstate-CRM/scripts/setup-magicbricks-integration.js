#!/usr/bin/env node

/**
 * Setup script for MagicBricks Integration
 *
 * This script helps you:
 * 1. Generate a secure API key for MagicBricks integration
 * 2. Create a system user for handling MagicBricks leads
 * 3. Update your .env.local file with the necessary credentials
 */

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function question(query) {
  return new Promise(resolve => rl.question(query, resolve));
}

function generateSecureApiKey() {
  // Generate a 32-byte random key and convert to base64
  return crypto.randomBytes(32).toString('base64url');
}

async function main() {
  console.log('=================================');
  console.log('MagicBricks Integration Setup');
  console.log('=================================\n');

  // Generate API Key
  const apiKey = generateSecureApiKey();
  console.log('✓ Generated secure API key for MagicBricks integration\n');
  console.log('Your API Key (keep this secure):');
  console.log('─────────────────────────────────');
  console.log(apiKey);
  console.log('─────────────────────────────────\n');

  // Check if .env.local exists
  const envPath = path.join(process.cwd(), '.env.local');
  let envContent = '';

  if (fs.existsSync(envPath)) {
    envContent = fs.readFileSync(envPath, 'utf8');
  } else {
    console.log('⚠ .env.local file not found. Creating new file...\n');
  }

  // Ask for System User ID
  console.log('System User Setup:');
  console.log('─────────────────────────────────');
  console.log('You need to specify a system user ID that will be used as the creator');
  console.log('for all leads imported from MagicBricks.\n');

  const useExisting = await question('Do you have an existing user ID? (yes/no): ');

  let systemUserId;
  if (useExisting.toLowerCase() === 'yes' || useExisting.toLowerCase() === 'y') {
    systemUserId = await question('Enter the user ID (cuid format): ');
  } else {
    console.log('\n⚠ Please create an admin/system user first in your CRM.');
    console.log('Then run this script again with the user ID.\n');
    console.log('For now, you can use a placeholder. Make sure to update it later!');
    systemUserId = 'PLACEHOLDER_USER_ID';
  }

  // Update or add environment variables
  const magicbricksApiKeyPattern = /MAGICBRICKS_API_KEY=.*/;
  const systemUserIdPattern = /SYSTEM_USER_ID=.*/;

  if (magicbricksApiKeyPattern.test(envContent)) {
    envContent = envContent.replace(magicbricksApiKeyPattern, `MAGICBRICKS_API_KEY=${apiKey}`);
  } else {
    envContent += `\n# MagicBricks Integration\nMAGICBRICKS_API_KEY=${apiKey}\n`;
  }

  if (systemUserIdPattern.test(envContent)) {
    envContent = envContent.replace(systemUserIdPattern, `SYSTEM_USER_ID=${systemUserId}`);
  } else {
    envContent += `SYSTEM_USER_ID=${systemUserId}\n`;
  }

  // Write to .env.local
  fs.writeFileSync(envPath, envContent);

  console.log('\n✓ Configuration saved to .env.local\n');

  // Get deployment URL
  console.log('Deployment Information:');
  console.log('─────────────────────────────────');
  const deploymentUrl = await question('Enter your deployment URL (e.g., https://your-domain.vercel.app): ');

  console.log('\n=================================');
  console.log('Setup Complete!');
  console.log('=================================\n');

  console.log('Share the following information with MagicBricks team:\n');
  console.log('─────────────────────────────────────────────────────');
  console.log('1. CRM Service Provider Name:');
  console.log('   Flatrix Real Estate CRM (Custom Solution)\n');

  console.log('2. Integration Type:');
  console.log('   Push Integration (Webhook)\n');

  console.log('3. Working Endpoint:');
  console.log(`   ${deploymentUrl}/api/integrations/magicbricks\n`);

  console.log('4. Sample URL:');
  console.log(`   ${deploymentUrl}/api/integrations/magicbricks\n`);

  console.log('5. API Key:');
  console.log(`   ${apiKey}\n`);

  console.log('6. Method:');
  console.log('   POST\n');

  console.log('7. URL Parameters:');
  console.log('   None (data sent in request body)\n');

  console.log('8. Request Headers:');
  console.log('   Content-Type: application/json');
  console.log('   x-api-key: <API_KEY_FROM_ABOVE>\n');

  console.log('9. Request Body Format (JSON):');
  console.log('   {');
  console.log('     "phone": "required",');
  console.log('     "name": "optional",');
  console.log('     "email": "optional",');
  console.log('     "message": "optional",');
  console.log('     "property_type": "optional",');
  console.log('     "budget": "optional",');
  console.log('     "location": "optional"');
  console.log('   }\n');

  console.log('10. Success Response:');
  console.log('   Status: 201 Created');
  console.log('   Body: { "success": true, "leadId": "...", "action": "created" }\n');

  console.log('─────────────────────────────────────────────────────\n');

  console.log('⚠ IMPORTANT SECURITY NOTES:');
  console.log('─────────────────────────────────────────────────────');
  console.log('1. Keep your API key secure and never commit it to version control');
  console.log('2. The API key is stored in .env.local (already in .gitignore)');
  console.log('3. Make sure to set the same environment variables in your production');
  console.log('   deployment (Vercel/etc.)\n');

  if (systemUserId === 'PLACEHOLDER_USER_ID') {
    console.log('⚠ WARNING: Remember to update SYSTEM_USER_ID in .env.local');
    console.log('   with a real user ID before deploying!\n');
  }

  rl.close();
}

main().catch(error => {
  console.error('Error:', error);
  rl.close();
  process.exit(1);
});
