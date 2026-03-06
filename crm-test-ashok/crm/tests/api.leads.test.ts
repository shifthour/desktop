import { describe, it, expect, beforeEach, vi } from 'vitest'

// Mock the database and auth modules
vi.mock('../src/lib/db', () => ({
  db: {
    lead: {
      create: vi.fn(),
      findMany: vi.fn(),
      findUnique: vi.fn(),
      count: vi.fn(),
    },
    user: {
      findMany: vi.fn(),
    },
    activity: {
      create: vi.fn(),
    },
  },
}))

vi.mock('../src/lib/auth', () => ({
  requireAuth: vi.fn(() => Promise.resolve({
    id: 'user1',
    email: 'test@example.com',
    orgId: 'org1',
    role: 'AGENT',
  })),
}))

vi.mock('../src/lib/dedupe', () => ({
  checkForDuplicates: vi.fn(() => Promise.resolve({
    isDuplicate: false,
  })),
  normalizePhone: vi.fn((phone) => `+91${phone}`),
  normalizeEmail: vi.fn((email) => email?.toLowerCase()),
}))

vi.mock('../src/lib/scoring', () => ({
  scoreLead: vi.fn(() => 75),
}))

vi.mock('../src/lib/email', () => ({
  sendAssignmentEmail: vi.fn(() => Promise.resolve(true)),
}))

describe('/api/leads', () => {
  describe('POST /api/leads', () => {
    it('should validate required fields', async () => {
      const { POST } = await import('../src/app/api/leads/route')
      const mockRequest = {
        json: () => Promise.resolve({}),
      } as any

      const response = await POST(mockRequest)
      expect(response.status).toBe(400)
    })

    it('should validate phone number', async () => {
      const { POST } = await import('../src/app/api/leads/route')
      const mockRequest = {
        json: () => Promise.resolve({
          phone: '123', // Too short
          source: 'website',
        }),
      } as any

      const response = await POST(mockRequest)
      expect(response.status).toBe(400)
    })

    it('should validate email format', async () => {
      const { POST } = await import('../src/app/api/leads/route')
      const mockRequest = {
        json: () => Promise.resolve({
          phone: '9876543210',
          email: 'invalid-email',
          source: 'website',
        }),
      } as any

      const response = await POST(mockRequest)
      expect(response.status).toBe(400)
    })

    it('should accept valid lead data', async () => {
      // Mock successful database operations
      const { db } = await import('../src/lib/db')
      
      vi.mocked(db.user.findMany).mockResolvedValue([
        {
          id: 'agent1',
          name: 'Test Agent',
          email: 'agent@test.com',
          _count: { assignedLeads: 5 },
        } as any,
      ])

      vi.mocked(db.lead.create).mockResolvedValue({
        id: 'lead1',
        name: 'Test Lead',
        phone: '+919876543210',
        email: 'test@example.com',
        source: 'website',
        score: 75,
        assignedToId: 'agent1',
      } as any)

      vi.mocked(db.activity.create).mockResolvedValue({} as any)

      const { POST } = await import('../src/app/api/leads/route')
      const mockRequest = {
        json: () => Promise.resolve({
          name: 'Test Lead',
          phone: '9876543210',
          email: 'test@example.com',
          source: 'website',
          bedroomsPref: 3,
          budgetMin: 5000000,
          budgetMax: 10000000,
        }),
      } as any

      const response = await POST(mockRequest)
      const data = await response.json()

      expect(response.status).toBe(200)
      expect(data).toHaveProperty('id')
      expect(data).toHaveProperty('score', 75)
      expect(data).toHaveProperty('message', 'Lead created successfully')
    })
  })

  describe('GET /api/leads', () => {
    it('should return paginated leads', async () => {
      const { db } = await import('../src/lib/db')
      
      const mockLeads = [
        {
          id: 'lead1',
          name: 'Test Lead 1',
          phone: '+919876543210',
          email: 'test1@example.com',
          source: 'website',
          stage: 'NEW',
          score: 75,
          createdAt: new Date(),
          project: null,
          assignedTo: { id: 'agent1', name: 'Test Agent' },
          activities: [],
        },
      ]

      vi.mocked(db.lead.findMany).mockResolvedValue(mockLeads as any)
      vi.mocked(db.lead.count).mockResolvedValue(1)

      const { GET } = await import('../src/app/api/leads/route')
      const mockRequest = {
        url: 'http://localhost:3000/api/leads?page=1&limit=50',
      } as any

      const response = await GET(mockRequest)
      const data = await response.json()

      expect(response.status).toBe(200)
      expect(data).toHaveProperty('leads')
      expect(data).toHaveProperty('pagination')
      expect(data.leads).toHaveLength(1)
      expect(data.pagination.total).toBe(1)
    })

    it('should handle search queries', async () => {
      const { db } = await import('../src/lib/db')
      
      vi.mocked(db.lead.findMany).mockResolvedValue([] as any)
      vi.mocked(db.lead.count).mockResolvedValue(0)

      const { GET } = await import('../src/app/api/leads/route')
      const mockRequest = {
        url: 'http://localhost:3000/api/leads?q=john&stage=NEW',
      } as any

      const response = await GET(mockRequest)
      const data = await response.json()

      expect(response.status).toBe(200)
      expect(data.leads).toEqual([])
    })

    it('should handle invalid query parameters', async () => {
      const { GET } = await import('../src/app/api/leads/route')
      const mockRequest = {
        url: 'http://localhost:3000/api/leads?limit=invalid',
      } as any

      const response = await GET(mockRequest)
      expect(response.status).toBe(400)
    })
  })
})