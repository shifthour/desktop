// One-time script to fix existing deals that should have multiple products
// Run this script with: node fix-existing-deals.js

const fetch = require('node-fetch');

async function fixExistingDeals() {
  console.log('Starting to fix existing deals...');
  
  try {
    // First, get all current deals
    const dealsResponse = await fetch('http://localhost:3000/api/deals');
    const dealsData = await dealsResponse.json();
    const deals = dealsData.deals || [];
    
    console.log(`Found ${deals.length} total deals`);
    
    // Get all leads to match products
    const leadsResponse = await fetch('http://localhost:3000/api/leads');
    const leadsData = await leadsResponse.json();
    const leads = leadsData.leads || [];
    
    console.log(`Found ${leads.length} total leads`);
    
    // For each deal, check if it has a corresponding lead with multiple products
    for (const deal of deals) {
      // Find matching lead by account name
      const matchingLead = leads.find(lead => 
        (lead.account_name === deal.account_name || lead.lead_name === deal.account_name) &&
        lead.lead_status === 'Qualified'
      );
      
      if (matchingLead) {
        console.log(`\nChecking deal: ${deal.account_name}`);
        
        // Get lead products
        const leadProductsResponse = await fetch(`http://localhost:3000/api/leads/${matchingLead.id}/products`);
        const leadProductsData = await leadProductsResponse.json();
        const leadProducts = leadProductsData.products || [];
        
        console.log(`  Lead has ${leadProducts.length} products`);
        
        // Get deal products
        const dealProductsResponse = await fetch(`http://localhost:3000/api/deals/${deal.id}/products`);
        const dealProductsData = await dealProductsResponse.json();
        const dealProducts = dealProductsData.products || [];
        
        console.log(`  Deal has ${dealProducts.length} products`);
        
        // If lead has multiple products but deal doesn't, fix it
        if (leadProducts.length > 1 && dealProducts.length <= 1) {
          console.log(`  ⚠️ Deal needs fix! Adding ${leadProducts.length} products...`);
          
          // Prepare products for insertion
          const productsToAdd = leadProducts.map(product => ({
            product_name: product.product_name,
            quantity: product.quantity || 1,
            price_per_unit: product.price_per_unit || 0,
            notes: product.notes || null
          }));
          
          // Call the fix endpoint
          const fixResponse = await fetch('http://localhost:3000/api/deals/fix-products', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              deal_id: deal.id,
              products: productsToAdd
            })
          });
          
          const fixResult = await fixResponse.json();
          
          if (fixResponse.ok) {
            console.log(`  ✅ Successfully fixed deal ${deal.account_name}`);
          } else {
            console.log(`  ❌ Failed to fix deal: ${fixResult.error}`);
          }
        }
      }
    }
    
    console.log('\n✅ Fix process completed!');
    
  } catch (error) {
    console.error('Error during fix process:', error);
  }
}

// Run the fix
fixExistingDeals();