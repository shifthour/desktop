import { Phone, MessageCircle } from "lucide-react"

export default function MobileFooter() {
  return (
    <>
      {/* Mobile Footer - Only visible on mobile */}
      <div className="md:hidden fixed bottom-0 left-0 right-0 bg-[#E67E22] border-t border-[#c96b05] flex z-50 mobile-footer-fixed">
        <a href="tel:+917338628777" className="flex-1 flex flex-col items-center justify-center py-2 border-r border-[#c96b05]">
          <Phone size={24} className="text-white mb-1" />
          <span className="text-white text-xs font-medium">Call Us</span>
        </a>
        <a href="https://wa.me/917338628777" className="flex-1 flex flex-col items-center justify-center py-2">
          <MessageCircle size={24} className="text-white mb-1" />
          <span className="text-white text-xs font-medium">Text Us</span>
        </a>
      </div>

      {/* Spacer for mobile footer */}
      <div className="md:hidden h-16"></div>
    </>
  )
}
