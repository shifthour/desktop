export function maskPhone(phone: string): string {
  if (!phone) return ''
  
  // Handle Indian phone numbers (+91 XXXXX XXXXX)
  const cleaned = phone.replace(/\D/g, '')
  
  if (cleaned.length >= 10) {
    const countryCode = cleaned.startsWith('91') ? '+91 ' : ''
    const number = cleaned.startsWith('91') ? cleaned.slice(2) : cleaned.slice(-10)
    
    if (number.length === 10) {
      return `${countryCode}••••• ${number.slice(-4)}`
    }
  }
  
  // Fallback for other formats
  if (phone.length > 4) {
    return phone.slice(0, 3) + '••••' + phone.slice(-2)
  }
  
  return phone
}

export function maskEmail(email: string): string {
  if (!email) return ''
  
  const [local, domain] = email.split('@')
  if (!local || !domain) return email
  
  if (local.length <= 2) {
    return `${local}••@${domain}`
  }
  
  const maskedLocal = local.charAt(0) + '••' + (local.length > 3 ? local.slice(-1) : '')
  return `${maskedLocal}@${domain}`
}

export function unmaskValue(value: string): string {
  // In a real application, you might want to implement
  // actual unmasking logic based on user permissions
  return value
}