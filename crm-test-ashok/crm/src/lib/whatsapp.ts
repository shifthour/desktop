export function waLink(phone: string, text?: string): string {
  if (!phone) return '#'
  
  // Clean phone number - remove all non-digits
  const cleanPhone = phone.replace(/\D/g, '')
  
  // Ensure it starts with country code
  let formattedPhone = cleanPhone
  if (cleanPhone.length === 10) {
    formattedPhone = '91' + cleanPhone // Add India country code
  } else if (cleanPhone.startsWith('0')) {
    formattedPhone = '91' + cleanPhone.slice(1) // Replace leading 0 with country code
  }
  
  const baseUrl = `https://wa.me/${formattedPhone}`
  
  if (text) {
    const encodedText = encodeURIComponent(text)
    return `${baseUrl}?text=${encodedText}`
  }
  
  return baseUrl
}

export function getDefaultWhatsAppMessage(leadName?: string, projectName?: string): string {
  const greeting = leadName ? `Hi ${leadName}` : 'Hi'
  const projectText = projectName ? ` regarding ${projectName}` : ''
  
  return `${greeting}, thank you for your interest in our properties${projectText}. I'm reaching out to assist you with your requirements. When would be a good time to discuss your preferences?`
}