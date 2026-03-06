import { describe, it, expect } from 'vitest'
import { normalizePhone, normalizeEmail } from '../src/lib/dedupe'

describe('Deduplication Utils', () => {
  describe('normalizePhone', () => {
    it('should normalize 10-digit phone numbers', () => {
      expect(normalizePhone('9876543210')).toBe('+919876543210')
      expect(normalizePhone('8765432109')).toBe('+918765432109')
    })

    it('should handle phone numbers with country code', () => {
      expect(normalizePhone('919876543210')).toBe('+919876543210')
      expect(normalizePhone('+919876543210')).toBe('+919876543210')
    })

    it('should handle formatted phone numbers', () => {
      expect(normalizePhone('+91 98765 43210')).toBe('+919876543210')
      expect(normalizePhone('98765-43210')).toBe('+919876543210')
      expect(normalizePhone('(98765) 43210')).toBe('+919876543210')
    })

    it('should handle phone numbers with leading zero', () => {
      expect(normalizePhone('09876543210')).toBe('+919876543210')
    })

    it('should return original if cannot normalize', () => {
      expect(normalizePhone('123')).toBe('123')
      expect(normalizePhone('invalid')).toBe('invalid')
    })

    it('should remove all non-digit characters', () => {
      expect(normalizePhone('+91-9876-543-210')).toBe('+919876543210')
      expect(normalizePhone('91 (987) 654-3210')).toBe('+919876543210')
    })
  })

  describe('normalizeEmail', () => {
    it('should convert to lowercase', () => {
      expect(normalizeEmail('Test@Example.Com')).toBe('test@example.com')
      expect(normalizeEmail('USER@DOMAIN.COM')).toBe('user@domain.com')
    })

    it('should trim whitespace', () => {
      expect(normalizeEmail('  test@example.com  ')).toBe('test@example.com')
      expect(normalizeEmail('\tuser@domain.com\n')).toBe('user@domain.com')
    })

    it('should handle already normalized emails', () => {
      expect(normalizeEmail('test@example.com')).toBe('test@example.com')
    })

    it('should handle empty string', () => {
      expect(normalizeEmail('')).toBe('')
    })

    it('should handle complex email addresses', () => {
      expect(normalizeEmail('User.Name+Tag@Example-Domain.co.in')).toBe('user.name+tag@example-domain.co.in')
    })
  })
})