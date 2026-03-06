import { clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

// Utility function (if exists in lib/utils.ts, otherwise this is a sample)
function cn(...inputs: any[]) {
  return twMerge(clsx(inputs))
}

describe('Utility Functions', () => {
  describe('cn (className utility)', () => {
    it('should merge class names correctly', () => {
      const result = cn('text-red-500', 'bg-blue-500')
      expect(result).toBe('text-red-500 bg-blue-500')
    })

    it('should handle conditional classes', () => {
      const result = cn('base-class', true && 'conditional-class', false && 'hidden-class')
      expect(result).toBe('base-class conditional-class')
    })

    it('should merge Tailwind classes correctly with conflicts', () => {
      const result = cn('p-4', 'p-6')
      expect(result).toBe('p-6') // Should keep the last padding value
    })

    it('should handle arrays of classes', () => {
      const result = cn(['class1', 'class2'], 'class3')
      expect(result).toBe('class1 class2 class3')
    })

    it('should handle empty inputs', () => {
      const result = cn()
      expect(result).toBe('')
    })
  })

  describe('Data Validation', () => {
    it('should validate email format', () => {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/

      expect(emailRegex.test('test@example.com')).toBe(true)
      expect(emailRegex.test('invalid-email')).toBe(false)
      expect(emailRegex.test('test@')).toBe(false)
    })

    it('should validate phone number format', () => {
      const phoneRegex = /^[\d\s\-+()]+$/

      expect(phoneRegex.test('1234567890')).toBe(true)
      expect(phoneRegex.test('+1 (234) 567-8900')).toBe(true)
      expect(phoneRegex.test('invalid-phone')).toBe(false)
    })
  })

  describe('String Manipulation', () => {
    it('should capitalize first letter', () => {
      const capitalize = (str: string) => str.charAt(0).toUpperCase() + str.slice(1)

      expect(capitalize('hello')).toBe('Hello')
      expect(capitalize('world')).toBe('World')
      expect(capitalize('')).toBe('')
    })

    it('should format currency', () => {
      const formatCurrency = (amount: number) => {
        return new Intl.NumberFormat('en-US', {
          style: 'currency',
          currency: 'USD',
        }).format(amount)
      }

      expect(formatCurrency(1000)).toBe('$1,000.00')
      expect(formatCurrency(0)).toBe('$0.00')
      expect(formatCurrency(99.99)).toBe('$99.99')
    })
  })

  describe('Date Utilities', () => {
    it('should format date correctly', () => {
      const formatDate = (date: Date) => {
        return date.toLocaleDateString('en-US', {
          year: 'numeric',
          month: 'long',
          day: 'numeric',
        })
      }

      const testDate = new Date('2024-01-15')
      const formatted = formatDate(testDate)
      expect(formatted).toContain('2024')
      expect(formatted).toContain('January')
    })
  })
})
