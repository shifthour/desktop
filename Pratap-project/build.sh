#!/bin/bash
# Build script: injects environment variables into config.js
cat > config.js << EOF
const SUPABASE_URL = '${SUPABASE_URL}';
const SUPABASE_ANON_KEY = '${SUPABASE_ANON_KEY}';
EOF
