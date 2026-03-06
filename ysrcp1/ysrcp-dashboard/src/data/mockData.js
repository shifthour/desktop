// Mock Data for YSRCP Social Media Dashboard

export const partyData = {
  ysrcp: {
    name: 'YSRCP',
    fullName: 'YSR Congress Party',
    color: '#0066CC',
    leader: 'YS Jagan Mohan Reddy',
    handle: '@yaborajagan',
    logo: 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/YSR_Congress_Party_logo.svg/200px-YSR_Congress_Party_logo.svg.png'
  },
  tdp: {
    name: 'TDP',
    fullName: 'Telugu Desam Party',
    color: '#FFD700',
    leader: 'N. Chandrababu Naidu',
    handle: '@ncaborajagan',
    logo: 'https://upload.wikimedia.org/wikipedia/commons/thumb/9/9a/Telugu_Desam_Party_Flag.svg/200px-Telugu_Desam_Party_Flag.svg.png'
  }
};

export const overallStats = {
  ysrcp: {
    sentimentScore: 62,
    totalReach: 12500000,
    totalEngagement: 892000,
    totalFollowers: 8200000,
    postsToday: 156,
    avgEngagementRate: 4.2,
    shareOfVoice: 58
  },
  tdp: {
    sentimentScore: 38,
    totalReach: 9800000,
    totalEngagement: 654000,
    totalFollowers: 6100000,
    postsToday: 134,
    avgEngagementRate: 3.1,
    shareOfVoice: 42
  }
};

export const platformStats = {
  twitter: {
    name: 'Twitter / X',
    icon: 'twitter',
    ysrcp: {
      followers: 2800000,
      followersGrowth: 12500,
      posts: 45,
      reach: 4200000,
      engagement: 285000,
      engagementRate: 4.8,
      sentiment: { positive: 58, negative: 22, neutral: 20 }
    },
    tdp: {
      followers: 2100000,
      followersGrowth: 8200,
      posts: 38,
      reach: 3100000,
      engagement: 198000,
      engagementRate: 3.2,
      sentiment: { positive: 42, negative: 35, neutral: 23 }
    }
  },
  facebook: {
    name: 'Facebook',
    icon: 'facebook',
    ysrcp: {
      followers: 3200000,
      followersGrowth: 15600,
      posts: 52,
      reach: 5800000,
      engagement: 412000,
      engagementRate: 5.2,
      sentiment: { positive: 61, negative: 19, neutral: 20 }
    },
    tdp: {
      followers: 2400000,
      followersGrowth: 9800,
      posts: 48,
      reach: 4200000,
      engagement: 287000,
      engagementRate: 3.8,
      sentiment: { positive: 39, negative: 38, neutral: 23 }
    }
  },
  instagram: {
    name: 'Instagram',
    icon: 'instagram',
    ysrcp: {
      followers: 1500000,
      followersGrowth: 22000,
      posts: 28,
      reach: 2100000,
      engagement: 156000,
      engagementRate: 6.8,
      sentiment: { positive: 65, negative: 15, neutral: 20 }
    },
    tdp: {
      followers: 980000,
      followersGrowth: 12500,
      posts: 24,
      reach: 1400000,
      engagement: 98000,
      engagementRate: 4.2,
      sentiment: { positive: 44, negative: 32, neutral: 24 }
    }
  },
  youtube: {
    name: 'YouTube',
    icon: 'youtube',
    ysrcp: {
      followers: 720000,
      followersGrowth: 8500,
      posts: 12,
      reach: 3200000,
      engagement: 89000,
      engagementRate: 2.8,
      sentiment: { positive: 54, negative: 28, neutral: 18 }
    },
    tdp: {
      followers: 580000,
      followersGrowth: 5200,
      posts: 10,
      reach: 2400000,
      engagement: 71000,
      engagementRate: 2.1,
      sentiment: { positive: 38, negative: 42, neutral: 20 }
    }
  },
  news: {
    name: 'News & Media',
    icon: 'newspaper',
    ysrcp: {
      mentions: 245,
      reach: 8500000,
      sentiment: { positive: 48, negative: 32, neutral: 20 }
    },
    tdp: {
      mentions: 312,
      reach: 9200000,
      sentiment: { positive: 52, negative: 28, neutral: 20 }
    }
  }
};

export const trendingHashtags = [
  { tag: '#YSRCPForPeople', count: 45200, sentiment: 'positive', party: 'ysrcp', trending: true },
  { tag: '#JaganWelfarSchemes', count: 38400, sentiment: 'positive', party: 'ysrcp', trending: true },
  { tag: '#APPolitics', count: 32100, sentiment: 'neutral', party: 'both', trending: true },
  { tag: '#TDPFails', count: 28900, sentiment: 'negative', party: 'tdp', trending: true },
  { tag: '#NavaratnaluSuccess', count: 25600, sentiment: 'positive', party: 'ysrcp', trending: false },
  { tag: '#ChandrababuNaidu', count: 22300, sentiment: 'neutral', party: 'tdp', trending: true },
  { tag: '#AndhraPradesh', count: 19800, sentiment: 'neutral', party: 'both', trending: false },
  { tag: '#YSJagan', count: 18500, sentiment: 'positive', party: 'ysrcp', trending: true },
  { tag: '#TDPGovernment', count: 15200, sentiment: 'negative', party: 'tdp', trending: false },
  { tag: '#APDevelopment', count: 12400, sentiment: 'neutral', party: 'both', trending: false }
];

export const alerts = [
  {
    id: 1,
    type: 'warning',
    title: 'Negative Trend Detected',
    message: 'Spike in negative mentions about power cuts in Vizag region',
    time: '15 mins ago',
    platform: 'twitter',
    priority: 'high'
  },
  {
    id: 2,
    type: 'info',
    title: 'TDP Campaign Launch',
    message: 'TDP launching new social media campaign #TDPForFuture',
    time: '45 mins ago',
    platform: 'all',
    priority: 'medium'
  },
  {
    id: 3,
    type: 'success',
    title: 'Viral Content',
    message: 'Video on Amma Vodi scheme crossed 1M views',
    time: '2 hours ago',
    platform: 'youtube',
    priority: 'low'
  },
  {
    id: 4,
    type: 'warning',
    title: 'Competitor Activity',
    message: 'TDP influencers coordinating attack on welfare schemes',
    time: '3 hours ago',
    platform: 'twitter',
    priority: 'high'
  }
];

export const topPosts = {
  ysrcp: [
    {
      id: 1,
      platform: 'instagram',
      content: 'CM YS Jagan launches new phase of Jagananna Vidya Deevena, benefiting 16 lakh students...',
      likes: 125000,
      comments: 8500,
      shares: 12400,
      reach: 2800000,
      sentiment: 'positive',
      time: '4 hours ago',
      image: true
    },
    {
      id: 2,
      platform: 'twitter',
      content: 'Navaratnalu completing 4 years of transforming lives. 2.3 crore families benefited from welfare schemes.',
      likes: 45600,
      comments: 3200,
      shares: 8900,
      reach: 1200000,
      sentiment: 'positive',
      time: '6 hours ago',
      image: false
    },
    {
      id: 3,
      platform: 'facebook',
      content: 'Amma Vodi scheme: Every mother is now an education minister for her children.',
      likes: 89000,
      comments: 5600,
      shares: 15200,
      reach: 3200000,
      sentiment: 'positive',
      time: '8 hours ago',
      image: true
    }
  ],
  tdp: [
    {
      id: 1,
      platform: 'twitter',
      content: 'TDP promises to restore Amaravati as the capital of Andhra Pradesh.',
      likes: 32000,
      comments: 4500,
      shares: 5600,
      reach: 980000,
      sentiment: 'positive',
      time: '3 hours ago',
      image: false
    },
    {
      id: 2,
      platform: 'facebook',
      content: 'Chandrababu Naidu addressing public meeting in Guntur district.',
      likes: 28500,
      comments: 3200,
      shares: 4800,
      reach: 850000,
      sentiment: 'neutral',
      time: '5 hours ago',
      image: true
    },
    {
      id: 3,
      platform: 'youtube',
      content: 'Exclusive interview with TDP leadership on development agenda.',
      likes: 18200,
      comments: 2100,
      shares: 3400,
      reach: 620000,
      sentiment: 'neutral',
      time: '12 hours ago',
      image: true
    }
  ]
};

export const sentimentTrend = [
  { date: 'Nov 24', ysrcp: 58, tdp: 42 },
  { date: 'Nov 25', ysrcp: 55, tdp: 45 },
  { date: 'Nov 26', ysrcp: 62, tdp: 38 },
  { date: 'Nov 27', ysrcp: 60, tdp: 40 },
  { date: 'Nov 28', ysrcp: 65, tdp: 35 },
  { date: 'Nov 29', ysrcp: 58, tdp: 42 },
  { date: 'Nov 30', ysrcp: 62, tdp: 38 }
];

export const engagementTrend = [
  { date: 'Nov 24', ysrcp: 720000, tdp: 520000 },
  { date: 'Nov 25', ysrcp: 680000, tdp: 580000 },
  { date: 'Nov 26', ysrcp: 850000, tdp: 490000 },
  { date: 'Nov 27', ysrcp: 920000, tdp: 610000 },
  { date: 'Nov 28', ysrcp: 780000, tdp: 550000 },
  { date: 'Nov 29', ysrcp: 890000, tdp: 620000 },
  { date: 'Nov 30', ysrcp: 892000, tdp: 654000 }
];

export const regionalData = [
  { district: 'Visakhapatnam', ysrcp: 65, tdp: 35, mentions: 45000 },
  { district: 'Vijayawada', ysrcp: 48, tdp: 52, mentions: 38000 },
  { district: 'Guntur', ysrcp: 55, tdp: 45, mentions: 32000 },
  { district: 'Tirupati', ysrcp: 68, tdp: 32, mentions: 28000 },
  { district: 'Kurnool', ysrcp: 72, tdp: 28, mentions: 22000 },
  { district: 'Nellore', ysrcp: 61, tdp: 39, mentions: 25000 },
  { district: 'Rajahmundry', ysrcp: 52, tdp: 48, mentions: 30000 },
  { district: 'Kadapa', ysrcp: 78, tdp: 22, mentions: 18000 },
  { district: 'Anantapur', ysrcp: 64, tdp: 36, mentions: 20000 },
  { district: 'Kakinada', ysrcp: 58, tdp: 42, mentions: 26000 },
  { district: 'Eluru', ysrcp: 54, tdp: 46, mentions: 15000 },
  { district: 'Ongole', ysrcp: 66, tdp: 34, mentions: 17000 },
  { district: 'Srikakulam', ysrcp: 60, tdp: 40, mentions: 14000 }
];

export const influencers = [
  {
    id: 1,
    name: 'Political Analyst AP',
    handle: '@PoliticalAP',
    followers: 520000,
    platform: 'twitter',
    sentiment: 'pro-ysrcp',
    recentMentions: 45,
    engagement: 125000,
    verified: true
  },
  {
    id: 2,
    name: 'Telugu News Hub',
    handle: '@TeluguNewsHub',
    followers: 890000,
    platform: 'twitter',
    sentiment: 'neutral',
    recentMentions: 78,
    engagement: 245000,
    verified: true
  },
  {
    id: 3,
    name: 'AP Politics Daily',
    handle: '@APPoliticsDaily',
    followers: 320000,
    platform: 'twitter',
    sentiment: 'pro-tdp',
    recentMentions: 52,
    engagement: 98000,
    verified: false
  },
  {
    id: 4,
    name: 'Vizag News Network',
    handle: '@VizagNews',
    followers: 210000,
    platform: 'twitter',
    sentiment: 'neutral',
    recentMentions: 34,
    engagement: 67000,
    verified: true
  },
  {
    id: 5,
    name: 'Youth Voice AP',
    handle: '@YouthVoiceAP',
    followers: 180000,
    platform: 'instagram',
    sentiment: 'pro-ysrcp',
    recentMentions: 28,
    engagement: 89000,
    verified: false
  },
  {
    id: 6,
    name: 'Andhra Updates',
    handle: '@AndhraUpdates',
    followers: 450000,
    platform: 'facebook',
    sentiment: 'neutral',
    recentMentions: 62,
    engagement: 156000,
    verified: true
  }
];

export const contentPerformance = {
  byType: {
    ysrcp: [
      { type: 'Videos', engagement: 42, count: 85 },
      { type: 'Images', engagement: 28, count: 320 },
      { type: 'Text', engagement: 18, count: 450 },
      { type: 'Reels/Shorts', engagement: 12, count: 45 }
    ],
    tdp: [
      { type: 'Videos', engagement: 35, count: 62 },
      { type: 'Images', engagement: 32, count: 280 },
      { type: 'Text', engagement: 22, count: 380 },
      { type: 'Reels/Shorts', engagement: 11, count: 38 }
    ]
  },
  byTopic: [
    { topic: 'Welfare Schemes', ysrcp: 85000, tdp: 32000 },
    { topic: 'Development', ysrcp: 62000, tdp: 58000 },
    { topic: 'Agriculture', ysrcp: 48000, tdp: 35000 },
    { topic: 'Education', ysrcp: 72000, tdp: 28000 },
    { topic: 'Healthcare', ysrcp: 55000, tdp: 42000 },
    { topic: 'Infrastructure', ysrcp: 38000, tdp: 65000 }
  ]
};

export const wordCloudData = [
  { text: 'Welfare', value: 85 },
  { text: 'Development', value: 72 },
  { text: 'Jagan', value: 68 },
  { text: 'Schemes', value: 65 },
  { text: 'Naidu', value: 58 },
  { text: 'Amaravati', value: 52 },
  { text: 'Education', value: 48 },
  { text: 'Youth', value: 45 },
  { text: 'Agriculture', value: 42 },
  { text: 'Jobs', value: 40 },
  { text: 'Healthcare', value: 38 },
  { text: 'Infrastructure', value: 35 },
  { text: 'Villages', value: 32 },
  { text: 'Women', value: 30 },
  { text: 'Students', value: 28 }
];

// Google Trends Data
export const googleTrendsData = {
  // Overall search interest (0-100 scale)
  searchInterest: {
    ysrcp: 68,
    tdp: 45,
    trend: '+12%' // YSRCP trend vs last week
  },

  // 30-day search interest timeline
  searchTimeline: [
    { date: 'Nov 1', ysrcp: 45, tdp: 42 },
    { date: 'Nov 5', ysrcp: 52, tdp: 48 },
    { date: 'Nov 10', ysrcp: 58, tdp: 44 },
    { date: 'Nov 15', ysrcp: 72, tdp: 51 },
    { date: 'Nov 20', ysrcp: 65, tdp: 46 },
    { date: 'Nov 25', ysrcp: 78, tdp: 52 },
    { date: 'Nov 30', ysrcp: 68, tdp: 45 }
  ],

  // Regional search interest by district
  regionalInterest: [
    { district: 'Kadapa', ysrcp: 92, tdp: 8 },
    { district: 'Kurnool', ysrcp: 78, tdp: 22 },
    { district: 'Anantapur', ysrcp: 71, tdp: 29 },
    { district: 'Tirupati', ysrcp: 69, tdp: 31 },
    { district: 'Nellore', ysrcp: 65, tdp: 35 },
    { district: 'Visakhapatnam', ysrcp: 62, tdp: 38 },
    { district: 'Guntur', ysrcp: 52, tdp: 48 },
    { district: 'Vijayawada', ysrcp: 45, tdp: 55 },
    { district: 'Rajahmundry', ysrcp: 48, tdp: 52 }
  ],

  // Related search queries
  relatedQueries: {
    ysrcp: [
      { query: 'YSRCP welfare schemes', interest: 100, change: '+450%', isBreakout: true },
      { query: 'YS Jagan latest news', interest: 85, change: '+120%', isBreakout: false },
      { query: 'Amma Vodi scheme', interest: 78, change: '+95%', isBreakout: false },
      { query: 'Rythu Bharosa status', interest: 72, change: '+80%', isBreakout: false },
      { query: 'Jagananna Vidya Deevena', interest: 68, change: '+200%', isBreakout: true },
      { query: 'YSRCP MLA list', interest: 45, change: '+25%', isBreakout: false }
    ],
    tdp: [
      { query: 'Chandrababu Naidu speech', interest: 100, change: '+85%', isBreakout: false },
      { query: 'TDP manifesto 2024', interest: 72, change: '+150%', isBreakout: true },
      { query: 'Amaravati capital', interest: 65, change: '+60%', isBreakout: false },
      { query: 'TDP alliance news', interest: 58, change: '+45%', isBreakout: false },
      { query: 'Nara Lokesh', interest: 52, change: '+70%', isBreakout: false }
    ]
  },

  // Breakout topics (sudden spikes)
  breakoutTopics: [
    { topic: 'YSRCP welfare schemes', growth: '+450%', party: 'ysrcp' },
    { topic: 'Jagananna Vidya Deevena', growth: '+200%', party: 'ysrcp' },
    { topic: 'TDP manifesto 2024', growth: '+150%', party: 'tdp' },
    { topic: 'AP election news', growth: '+180%', party: 'neutral' },
    { topic: 'YS Jagan rally', growth: '+320%', party: 'ysrcp' }
  ],

  // Comparison topics - what people search when comparing
  comparisonSearches: [
    { topic: 'Welfare Schemes', ysrcp: 85, tdp: 32 },
    { topic: 'Development', ysrcp: 58, tdp: 62 },
    { topic: 'Jobs & Employment', ysrcp: 48, tdp: 55 },
    { topic: 'Agriculture', ysrcp: 72, tdp: 45 },
    { topic: 'Education', ysrcp: 82, tdp: 35 },
    { topic: 'Healthcare', ysrcp: 68, tdp: 42 }
  ]
};
