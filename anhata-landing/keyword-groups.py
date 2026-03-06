import csv
import json

# Read keywords from CSV - skip first 2 rows
keywords = []
with open('/tmp/keywords.csv', 'r') as f:
    lines = f.readlines()
    # Skip first 2 rows and read from 3rd row
    reader = csv.DictReader(lines[2:])
    for row in reader:
        if row['Keyword'] and 'Total:' not in str(row['Keyword']) and row['Keyword'] != 'Keyword':
            keyword = row['Keyword'].strip('"')
            status = row['Keyword status']
            if status == 'Enabled':
                keywords.append(keyword)

# Define ad groups based on keyword themes
ad_groups = {
    "Brand - Anahata & Ishtika": [],
    "Location - Whitefield Core": [],
    "Location - Soukya Road Specific": [],
    "Location - ITPL & Nearby": [],
    "2BHK - Whitefield": [],
    "3BHK - Whitefield": [],
    "2BHK & 3BHK - General Bangalore": [],
    "Luxury Apartments - Features": [],
    "Luxury - Amenities Focus": [],
    "Price & Budget Keywords": [],
    "Floor Plans & Layouts": [],
    "Swimming Pool & Gym": [],
    "New Projects & Construction": [],
    "Generic Apartments Sale": [],
    "High Rise & Buildings": [],
}

# Categorize keywords
for keyword in keywords:
    kw_lower = keyword.lower()
    
    # Brand keywords
    if 'anahata' in kw_lower or 'istika' in kw_lower:
        ad_groups["Brand - Anahata & Ishtika"].append(keyword)
    
    # Soukya Road specific
    elif 'soukya' in kw_lower or 'samethanahalli' in kw_lower:
        ad_groups["Location - Soukya Road Specific"].append(keyword)
    
    # ITPL area
    elif 'itpl' in kw_lower:
        ad_groups["Location - ITPL & Nearby"].append(keyword)
    
    # 2BHK Whitefield specific
    elif '2 bhk' in kw_lower and 'whitefield' in kw_lower:
        ad_groups["2BHK - Whitefield"].append(keyword)
    elif '2bhk' in kw_lower and 'whitefield' in kw_lower:
        ad_groups["2BHK - Whitefield"].append(keyword)
    
    # 3BHK Whitefield specific
    elif '3 bhk' in kw_lower and 'whitefield' in kw_lower:
        ad_groups["3BHK - Whitefield"].append(keyword)
    elif '3bhk' in kw_lower and 'whitefield' in kw_lower:
        ad_groups["3BHK - Whitefield"].append(keyword)
    
    # Whitefield general
    elif 'whitefield' in kw_lower:
        ad_groups["Location - Whitefield Core"].append(keyword)
    
    # 2BHK general Bangalore
    elif '2 bhk' in kw_lower or '2bhk' in kw_lower:
        ad_groups["2BHK & 3BHK - General Bangalore"].append(keyword)
    
    # 3BHK general Bangalore
    elif '3 bhk' in kw_lower or '3bhk' in kw_lower:
        ad_groups["2BHK & 3BHK - General Bangalore"].append(keyword)
    
    # Swimming pool & gym
    elif 'pool' in kw_lower or 'gym' in kw_lower or 'swimming' in kw_lower:
        ad_groups["Swimming Pool & Gym"].append(keyword)
    
    # Floor plans
    elif 'floor plan' in kw_lower or 'carpet area' in kw_lower or 'plans' in kw_lower:
        ad_groups["Floor Plans & Layouts"].append(keyword)
    
    # Luxury amenities
    elif 'amenities' in kw_lower or 'amenity' in kw_lower:
        ad_groups["Luxury - Amenities Focus"].append(keyword)
    
    # Luxury features
    elif 'luxury' in kw_lower or 'premium' in kw_lower or 'high rise' in kw_lower or 'star' in kw_lower:
        ad_groups["Luxury Apartments - Features"].append(keyword)
    
    # Price related
    elif 'price' in kw_lower or 'cost' in kw_lower or 'buy' in kw_lower:
        ad_groups["Price & Budget Keywords"].append(keyword)
    
    # New construction
    elif 'new' in kw_lower or 'construction' in kw_lower or 'project' in kw_lower:
        ad_groups["New Projects & Construction"].append(keyword)
    
    # Building related
    elif 'building' in kw_lower or 'high rise' in kw_lower or 'residential' in kw_lower:
        ad_groups["High Rise & Buildings"].append(keyword)
    
    # Generic sale
    else:
        ad_groups["Generic Apartments Sale"].append(keyword)

# Split large groups if needed
final_groups = {}
group_counter = 1

for group_name, keywords_list in ad_groups.items():
    if len(keywords_list) == 0:
        continue
    
    if len(keywords_list) <= 20:
        final_groups[f"Ad Group {group_counter}: {group_name}"] = keywords_list
        group_counter += 1
    else:
        # Split into multiple groups
        for i in range(0, len(keywords_list), 20):
            chunk = keywords_list[i:i+20]
            part = (i // 20) + 1
            final_groups[f"Ad Group {group_counter}: {group_name} - Part {part}"] = chunk
            group_counter += 1

# Print results
print("\n" + "="*80)
print("GOOGLE ADS KEYWORD GROUPS FOR ANAHATA PROJECT")
print("="*80 + "\n")

total_keywords = 0
for group_name, keywords_list in final_groups.items():
    print(f"\n{group_name}")
    print(f"Keywords Count: {len(keywords_list)}")
    print("-" * 50)
    for i, keyword in enumerate(keywords_list, 1):
        print(f"{i}. {keyword}")
    total_keywords += len(keywords_list)

print(f"\n{'='*80}")
print(f"SUMMARY:")
print(f"Total Ad Groups: {len(final_groups)}")
print(f"Total Keywords Organized: {total_keywords}")
print(f"Original Keywords Count: {len(keywords)}")

# Save to file
with open('/Users/safestorage/Desktop/anhata-landing/keyword-ad-groups.txt', 'w') as f:
    f.write("GOOGLE ADS KEYWORD GROUPS FOR ANAHATA PROJECT\n")
    f.write("="*80 + "\n\n")
    
    for group_name, keywords_list in final_groups.items():
        f.write(f"\n{group_name}\n")
        f.write(f"Keywords Count: {len(keywords_list)}\n")
        f.write("-" * 50 + "\n")
        for i, keyword in enumerate(keywords_list, 1):
            f.write(f"{i}. {keyword}\n")
    
    f.write(f"\n{'='*80}\n")
    f.write(f"SUMMARY:\n")
    f.write(f"Total Ad Groups: {len(final_groups)}\n")
    f.write(f"Total Keywords Organized: {total_keywords}\n")
    
print("\nResults saved to: keyword-ad-groups.txt")