import csv

# Read all keywords
keywords = []
with open('/tmp/keywords_only.csv', 'r') as f:
    for line in f:
        keyword = line.strip().strip('"')
        if keyword:
            keywords.append(keyword)

print(f"Total keywords found: {len(keywords)}")

# Define strategic ad groups
ad_groups = {
    "🎯 Brand - Anahata & Ishtika": [],
    "📍 Soukya Road Specific": [],
    "🏢 Whitefield - 2BHK Focused": [],
    "🏠 Whitefield - 3BHK Focused": [],
    "🌟 Whitefield - General Location": [],
    "💼 ITPL & Tech Parks": [],
    "🏗️ New Projects & Under Construction": [],
    "💎 Luxury & Premium Features": [],
    "🏊 Pool & Gym Amenities": [],
    "🌿 Amenities & Lifestyle": [],
    "📐 Floor Plans & Layouts": [],
    "💰 Price & Budget Focused": [],
    "🏘️ Bangalore - 2BHK General": [],
    "🏡 Bangalore - 3BHK General": [],
    "🏙️ Bangalore - General Real Estate": [],
    "🔍 Generic Apartment Searches": [],
}

# Categorize keywords
for keyword in keywords:
    kw_lower = keyword.lower()
    
    # Priority 1: Brand keywords (Highest priority)
    if 'anahata' in kw_lower or 'istika' in kw_lower or 'ishtika' in kw_lower:
        ad_groups["🎯 Brand - Anahata & Ishtika"].append(keyword)
    
    # Priority 2: Soukya Road (Very specific location)
    elif 'soukya' in kw_lower or 'samethanahalli' in kw_lower:
        ad_groups["📍 Soukya Road Specific"].append(keyword)
    
    # Priority 3: ITPL area
    elif 'itpl' in kw_lower or 'tech park' in kw_lower:
        ad_groups["💼 ITPL & Tech Parks"].append(keyword)
    
    # Priority 4: Whitefield + 2BHK combination
    elif 'whitefield' in kw_lower and ('2 bhk' in kw_lower or '2bhk' in kw_lower):
        ad_groups["🏢 Whitefield - 2BHK Focused"].append(keyword)
    
    # Priority 5: Whitefield + 3BHK combination
    elif 'whitefield' in kw_lower and ('3 bhk' in kw_lower or '3bhk' in kw_lower):
        ad_groups["🏠 Whitefield - 3BHK Focused"].append(keyword)
    
    # Priority 6: Whitefield general
    elif 'whitefield' in kw_lower:
        ad_groups["🌟 Whitefield - General Location"].append(keyword)
    
    # Priority 7: New projects/construction
    elif any(word in kw_lower for word in ['new project', 'under construction', 'new apartment', 'brand new', 'newer apartment', 'construction']):
        ad_groups["🏗️ New Projects & Under Construction"].append(keyword)
    
    # Priority 8: Pool & Gym specific
    elif any(word in kw_lower for word in ['pool', 'swimming', 'gym', 'fitness']):
        ad_groups["🏊 Pool & Gym Amenities"].append(keyword)
    
    # Priority 9: Floor plans
    elif 'floor plan' in kw_lower or 'carpet area' in kw_lower or 'building plan' in kw_lower:
        ad_groups["📐 Floor Plans & Layouts"].append(keyword)
    
    # Priority 10: Luxury keywords
    elif any(word in kw_lower for word in ['luxury', 'premium', 'high rise', 'star', 'upmarket', 'million dollar', 'exquisite', 'gorgeous', 'stunning']):
        ad_groups["💎 Luxury & Premium Features"].append(keyword)
    
    # Priority 11: Amenities focused
    elif 'amenity' in kw_lower or 'amenities' in kw_lower or 'lifestyle' in kw_lower or 'green space' in kw_lower:
        ad_groups["🌿 Amenities & Lifestyle"].append(keyword)
    
    # Priority 12: Price/Budget keywords
    elif any(word in kw_lower for word in ['price', 'cost', 'budget', 'buy']):
        if '2 bhk' in kw_lower or '2bhk' in kw_lower:
            ad_groups["🏘️ Bangalore - 2BHK General"].append(keyword)
        elif '3 bhk' in kw_lower or '3bhk' in kw_lower:
            ad_groups["🏡 Bangalore - 3BHK General"].append(keyword)
        else:
            ad_groups["💰 Price & Budget Focused"].append(keyword)
    
    # Priority 13: 2BHK Bangalore general
    elif '2 bhk' in kw_lower or '2bhk' in kw_lower:
        ad_groups["🏘️ Bangalore - 2BHK General"].append(keyword)
    
    # Priority 14: 3BHK Bangalore general
    elif '3 bhk' in kw_lower or '3bhk' in kw_lower:
        ad_groups["🏡 Bangalore - 3BHK General"].append(keyword)
    
    # Priority 15: Bangalore general real estate
    elif 'bangalore' in kw_lower or 'bengaluru' in kw_lower:
        ad_groups["🏙️ Bangalore - General Real Estate"].append(keyword)
    
    # Priority 16: Generic apartment searches
    else:
        ad_groups["🔍 Generic Apartment Searches"].append(keyword)

# Split large groups and create final structure
final_groups = {}
group_counter = 1

for group_name, keywords_list in ad_groups.items():
    if len(keywords_list) == 0:
        continue
    
    # Split into chunks of 20
    if len(keywords_list) <= 20:
        final_groups[f"Ad Group {group_counter}: {group_name}"] = {
            "keywords": keywords_list,
            "count": len(keywords_list)
        }
        group_counter += 1
    else:
        parts = (len(keywords_list) + 19) // 20  # Calculate number of parts needed
        for i in range(0, len(keywords_list), 20):
            chunk = keywords_list[i:i+20]
            part_num = (i // 20) + 1
            final_groups[f"Ad Group {group_counter}: {group_name} (Part {part_num}/{parts})"] = {
                "keywords": chunk,
                "count": len(chunk)
            }
            group_counter += 1

# Create the output file
output_text = []
output_text.append("=" * 80)
output_text.append("🎯 GOOGLE ADS KEYWORD GROUPS FOR ANAHATA PROJECT")
output_text.append("=" * 80)
output_text.append("")
output_text.append("📊 STRATEGIC GROUPING FOR MAXIMUM PERFORMANCE")
output_text.append("-" * 80)
output_text.append("")

# Add strategic recommendations
output_text.append("⚡ PRIORITY AD GROUPS (Launch these first):")
output_text.append("1. Brand campaigns (Anahata/Ishtika) - Highest ROI expected")
output_text.append("2. Soukya Road specific - Most relevant location")
output_text.append("3. Whitefield 2BHK & 3BHK - Core product offering")
output_text.append("")
output_text.append("💡 BIDDING STRATEGY RECOMMENDATIONS:")
output_text.append("• Brand keywords: Use Target Impression Share (95%+)")
output_text.append("• Location specific: Enhanced CPC with location bid adjustments")
output_text.append("• Generic keywords: Start with Manual CPC, then switch to Target CPA")
output_text.append("")
output_text.append("-" * 80)
output_text.append("")

# Add all groups
for group_name, group_data in final_groups.items():
    output_text.append(f"\n{group_name}")
    output_text.append(f"Keywords Count: {group_data['count']}")
    output_text.append("-" * 50)
    for keyword in group_data['keywords']:
        output_text.append(f'"{keyword}"')

# Add summary
output_text.append("")
output_text.append("=" * 80)
output_text.append("📈 SUMMARY STATISTICS")
output_text.append("-" * 80)
output_text.append(f"✅ Total Ad Groups Created: {len(final_groups)}")
output_text.append(f"✅ Total Keywords Organized: {sum(g['count'] for g in final_groups.values())}")
output_text.append(f"✅ Original Keywords Count: {len(keywords)}")
output_text.append("")

# Add recommendations
output_text.append("🎯 OPTIMIZATION RECOMMENDATIONS:")
output_text.append("")
output_text.append("1. NEGATIVE KEYWORDS TO ADD:")
output_text.append("   • rental, rent, lease, pg, hostel")
output_text.append("   • resale, old, used")
output_text.append("   • 1 bhk, studio, villa, plot")
output_text.append("")
output_text.append("2. MATCH TYPE STRATEGY:")
output_text.append("   • Brand terms: Exact match")
output_text.append("   • Location specific: Phrase match")
output_text.append("   • Generic terms: Broad match modifier")
output_text.append("")
output_text.append("3. BUDGET ALLOCATION:")
output_text.append("   • 30% - Brand & Soukya Road campaigns")
output_text.append("   • 40% - Whitefield specific campaigns")
output_text.append("   • 20% - Bangalore general campaigns")
output_text.append("   • 10% - Testing & optimization")
output_text.append("")
output_text.append("4. AD COPY FOCUS POINTS:")
output_text.append("   • Starting ₹89 Lakhs")
output_text.append("   • No Pre-EMI")
output_text.append("   • 50+ Amenities")
output_text.append("   • Soukya Road, Whitefield")
output_text.append("   • By Ishtika Builders")

# Save to file
with open('/Users/safestorage/Desktop/anhata-landing/anahata-keyword-ad-groups.txt', 'w') as f:
    f.write('\n'.join(output_text))

# Print to console
print('\n'.join(output_text))

print("\n" + "="*80)
print("✅ File saved as: anahata-keyword-ad-groups.txt")
print("="*80)