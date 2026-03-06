import React, { useEffect } from 'react';
import { motion } from 'framer-motion';
import { Shield, Mail, Phone, MapPin, Clock, FileText, Cookie, Users, Lock, Globe, Baby, UserCheck, RefreshCw, MessageSquare } from 'lucide-react';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';

const sections = [
  { id: 'introduction', number: '01', title: 'Introduction', icon: FileText },
  { id: 'information-we-collect', number: '02', title: 'Information We Collect', icon: Users },
  { id: 'how-we-use', number: '03', title: 'How We Use Your Information', icon: Shield },
  { id: 'cookies', number: '04', title: 'Cookies & Tracking Technologies', icon: Cookie },
  { id: 'sharing', number: '05', title: 'Sharing Your Information', icon: Globe },
  { id: 'data-retention', number: '06', title: 'Data Retention', icon: Clock },
  { id: 'security', number: '07', title: 'Data Security', icon: Lock },
  { id: 'third-party', number: '08', title: 'Third-Party Websites & Links', icon: Globe },
  { id: 'children', number: '09', title: "Children's Privacy", icon: Baby },
  { id: 'your-rights', number: '10', title: 'Your Rights', icon: UserCheck },
  { id: 'changes', number: '11', title: 'Changes to This Policy', icon: RefreshCw },
  { id: 'contact', number: '12', title: 'Contact Us', icon: MessageSquare },
];

function SectionHeader({ number, title, id }) {
  return (
    <div id={id} className="scroll-mt-32">
      <p className="text-xs font-medium tracking-[0.22em] uppercase text-orange-500 mb-2">Section {number}</p>
      <h2 className="text-2xl md:text-3xl font-light text-gray-800 mb-5">{title}</h2>
    </div>
  );
}

function CheckItem({ children }) {
  return (
    <li className="flex items-start gap-3">
      <span className="flex-shrink-0 w-5 h-5 mt-0.5 rounded-full bg-orange-500 flex items-center justify-center">
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M2 6l3 3 5-5" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
      </span>
      <span className="text-gray-600 text-[15px] leading-relaxed">{children}</span>
    </li>
  );
}

function HighlightBox({ children }) {
  return (
    <div className="bg-orange-50 border-l-3 border-orange-400 rounded-r-lg p-5 my-6" style={{ borderLeftWidth: '3px', borderLeftColor: '#f97316' }}>
      <p className="text-[15px] text-gray-800 leading-relaxed">{children}</p>
    </div>
  );
}

function InfoCard({ icon, title, description }) {
  return (
    <div className="bg-white border border-gray-200 rounded-xl p-5">
      <span className="text-2xl mb-3 block">{icon}</span>
      <h4 className="text-base font-medium text-gray-800 mb-1">{title}</h4>
      <p className="text-sm text-gray-500 leading-relaxed">{description}</p>
    </div>
  );
}

function NoteBox({ children }) {
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4 mt-5">
      <span className="text-[10px] font-medium tracking-[0.15em] uppercase text-gray-400 block mb-1">Note</span>
      <p className="text-sm text-gray-600 leading-relaxed">{children}</p>
    </div>
  );
}

export default function PrivacyPolicy() {
  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  return (
    <div className="bg-white min-h-screen pt-24">
      <Navbar />
      <WhatsAppButton />

      {/* Hero Section */}
      <section className="py-16 md:py-24 px-4 bg-gradient-to-b from-gray-900 to-gray-800 relative overflow-hidden">
        <div className="absolute top-[-100px] right-[-100px] w-[380px] h-[380px] rounded-full bg-orange-500/5" />
        <div className="absolute bottom-[-70px] left-[8%] w-[220px] h-[220px] rounded-full bg-orange-500/5" />
        <div className="max-w-3xl mx-auto text-center relative z-10">
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="text-xs font-medium tracking-[0.24em] uppercase text-orange-400 mb-4"
          >
            Legal & Compliance
          </motion.p>
          <motion.h1
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            className="text-4xl md:text-6xl font-light text-white mb-6"
          >
            Privacy Policy
          </motion.h1>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-base text-white/50 max-w-xl mx-auto leading-relaxed"
          >
            At Ishtika Homes, your privacy matters as much as your comfort. This policy explains how we collect, use, and protect your personal information when you interact with us.
          </motion.p>
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 }}
            className="flex flex-wrap justify-center gap-3 mt-8"
          >
            <span className="bg-white/5 border border-white/10 rounded-full px-4 py-1.5 text-xs text-white/40">Last Updated: February 22, 2026</span>
            <span className="bg-white/5 border border-white/10 rounded-full px-4 py-1.5 text-xs text-white/40">Effective Immediately</span>
            <span className="bg-white/5 border border-white/10 rounded-full px-4 py-1.5 text-xs text-white/40">Applies to ishtikahomes.com</span>
          </motion.div>
        </div>
      </section>

      {/* Table of Contents - Mobile/Desktop */}
      <div className="bg-gray-50 border-b border-gray-200">
        <div className="max-w-3xl mx-auto px-4 py-6">
          <p className="text-xs font-medium tracking-wider uppercase text-gray-400 mb-3">Contents</p>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
            {sections.map((s) => (
              <a
                key={s.id}
                href={`#${s.id}`}
                className="text-sm text-gray-500 hover:text-orange-500 transition-colors py-1"
              >
                <span className="text-orange-400 mr-1.5">{s.number}.</span> {s.title}
              </a>
            ))}
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-3xl mx-auto px-4 py-16 md:py-20">

        {/* Section 1: Introduction */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="01" title="Introduction" id="introduction" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            Ishtika Homes ("we," "our," or "us") is a premium residential real estate developer offering thoughtfully crafted 2BHK and 3BHK apartments in Bangalore. We are committed to protecting the privacy of everyone who visits our website, enquires about our properties, or engages with our services.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            This Privacy Policy describes the types of information we collect from you, how that information is used, with whom it may be shared, and the choices you have with respect to your data. It applies to all information collected through our website at <a href="https://www.ishtikahomes.com/" className="text-orange-500 hover:underline">ishtikahomes.com</a>, as well as any related communications, enquiry forms, or services offered by Ishtika Homes.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            By accessing or using our website, you acknowledge that you have read and understood this Privacy Policy. If you do not agree with its terms, please refrain from using our website or submitting your personal information to us.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            We may update this policy from time to time to reflect changes in our practices or applicable law. When we make significant changes, we will revise the "Last Updated" date at the top of this page. We encourage you to review this policy periodically.
          </p>
        </motion.section>

        {/* Section 2: Information We Collect */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="02" title="Information We Collect" id="information-we-collect" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We collect information in two primary ways: information you provide directly to us, and information collected automatically when you visit our website.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Information You Provide</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            When you fill out an enquiry form, request a site visit, download a brochure, or otherwise contact us, you may provide us with personal information such as:
          </p>
          <ul className="space-y-3 mb-6">
            <CheckItem><strong>Identity information</strong> — your full name and any other identifying details you choose to share.</CheckItem>
            <CheckItem><strong>Contact details</strong> — email address, phone number, and residential or mailing address.</CheckItem>
            <CheckItem><strong>Property interest details</strong> — your budget range, preferred configuration (2BHK or 3BHK), intended use (self-use or investment), and timeline for purchase.</CheckItem>
            <CheckItem><strong>Communication records</strong> — messages, enquiries, or feedback submitted through our website or correspondence with our team.</CheckItem>
          </ul>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Information Collected Automatically</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            When you visit ishtikahomes.com, certain information is collected automatically by our systems and third-party analytics tools. This includes:
          </p>
          <ul className="space-y-3 mb-6">
            <CheckItem><strong>Usage data</strong> — pages visited, time spent on each page, links clicked, scroll depth, and navigation paths across our site.</CheckItem>
            <CheckItem><strong>Device and browser data</strong> — IP address, browser type and version, operating system, screen resolution, and device type.</CheckItem>
            <CheckItem><strong>Referral information</strong> — the source through which you arrived at our website (e.g., search engine, advertisement, social media, direct URL).</CheckItem>
            <CheckItem><strong>Cookie and tracking data</strong> — data captured by cookies and similar technologies as described in our Cookies section below.</CheckItem>
          </ul>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-6">
            <InfoCard icon="📋" title="Enquiry Forms" description="Data you submit when requesting information about our apartments or scheduling a site visit." />
            <InfoCard icon="📊" title="Analytics" description="Behavioral and usage data collected automatically to help us improve the website experience." />
            <InfoCard icon="🍪" title="Cookies" description="Small data files stored on your device that help us remember your preferences and understand usage patterns." />
            <InfoCard icon="📣" title="Marketing Channels" description="Data from advertising platforms where you may have interacted with our campaigns before visiting the site." />
          </div>
        </motion.section>

        {/* Section 3: How We Use Your Information */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="03" title="How We Use Your Information" id="how-we-use" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We use the information we collect for the following purposes, always ensuring it is relevant and proportionate to our legitimate business needs:
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Responding to Enquiries</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            When you contact us with a property enquiry, we use your name, phone number, and email address to follow up with relevant information about our apartments, pricing, availability, and site visit scheduling. Our sales and customer relations team may reach out to you via phone, email, or WhatsApp based on the contact preferences you share with us.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Improving Our Website and Services</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We analyse aggregated and anonymised usage data to understand which pages are most visited, where visitors drop off, what content is most engaging, and how the overall website experience can be improved. This helps us make our website easier to navigate and more informative for prospective homebuyers.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Marketing and Advertising</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            With your consent or where permitted by law, we may use your information to send you updates about new project launches, special offers, events, or relevant news from Ishtika Homes. We also use anonymised behavioral data to optimise our advertising campaigns across search engines and social media platforms. You may opt out of marketing communications at any time by following the unsubscribe instructions in any email we send or by contacting us directly.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Legal and Compliance Obligations</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We may use or disclose your personal information where required to do so by law, court order, regulatory authority, or other legal process. This includes compliance with obligations under India's Real Estate (Regulation and Development) Act (RERA) and any other applicable real estate or financial regulations.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Fraud Prevention and Security</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            We use certain data to detect, investigate, and prevent fraudulent enquiries, bot activity, or other security threats on our website. This helps us protect both our visitors and our own systems from harm.
          </p>
        </motion.section>

        {/* Section 4: Cookies & Tracking Technologies */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="04" title="Cookies & Tracking Technologies" id="cookies" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            Our website uses cookies and similar tracking technologies to enhance your browsing experience, analyse website traffic, and support our marketing efforts. A cookie is a small text file stored on your device when you visit a website. Cookies cannot access other data on your device or identify you personally on their own.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Types of Cookies We Use</h3>
          <ul className="space-y-3 mb-6">
            <CheckItem><strong>Strictly necessary cookies</strong> — essential for the website to function. These enable basic features such as page navigation and form submission. You cannot opt out of these cookies.</CheckItem>
            <CheckItem><strong>Analytics cookies</strong> — help us understand how visitors interact with our website by collecting and reporting information anonymously. We use these to measure site performance and identify areas for improvement.</CheckItem>
            <CheckItem><strong>Functional cookies</strong> — remember your preferences and settings to provide a more personalised experience on subsequent visits.</CheckItem>
            <CheckItem><strong>Advertising and targeting cookies</strong> — used to deliver relevant advertisements and track the effectiveness of our marketing campaigns on platforms such as Google and Meta (Facebook/Instagram).</CheckItem>
          </ul>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Behavioural Analytics</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We use Microsoft Clarity, a behavioural analytics tool, to capture how users interact with our website through session recordings, heatmaps, and click tracking. This helps us understand user journeys and improve our site. We also partner with Microsoft Advertising for targeted advertising. Website usage data is captured using first and third-party cookies. For more information, visit the <a href="https://privacy.microsoft.com/en-us/privacystatement" target="_blank" rel="noopener noreferrer" className="text-orange-500 hover:underline">Microsoft Privacy Statement</a>.
          </p>

          <HighlightBox>
            We improve our products and advertising by using Microsoft Clarity to see how you use our website. By using our site, you agree that we and Microsoft can collect and use this data.
          </HighlightBox>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Managing Your Cookie Preferences</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            You can control or disable cookies through your browser settings. Most browsers allow you to refuse new cookies, delete existing cookies, or receive a warning before a cookie is stored. Please note that disabling cookies may affect the functionality of certain features on our website. Refer to your browser's help documentation for specific instructions on managing cookies.
          </p>

          <NoteBox>
            Deleting cookies does not prevent them from being set again on your next visit unless you configure your browser to block them. Third-party opt-out tools are also available from providers such as <a href="https://optout.networkadvertising.org/" target="_blank" rel="noopener noreferrer" className="text-orange-500 hover:underline">Network Advertising Initiative</a> and <a href="https://www.youronlinechoices.com/" target="_blank" rel="noopener noreferrer" className="text-orange-500 hover:underline">Your Online Choices</a>.
          </NoteBox>
        </motion.section>

        {/* Section 5: Sharing Your Information */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="05" title="Sharing Your Information" id="sharing" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            Ishtika Homes does not sell, rent, or trade your personal information to any third party for their own commercial purposes. We may, however, share your information in the following limited circumstances:
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Service Providers and Technology Partners</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We work with trusted third-party service providers who assist us in operating our website, conducting our business, and communicating with you. These include website hosting providers, email marketing platforms, CRM systems, analytics tools, and advertising platforms. These parties are contractually obligated to keep your information confidential and use it only for the purposes for which it was shared.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Channel Partners and Real Estate Agents</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            Where you have submitted an enquiry and we believe a channel partner or authorised real estate agent may be better placed to assist you in your local area, we may share your contact details with such partners. We work only with authorised and vetted channel partners who are required to handle your data responsibly.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Legal Requirements</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We may disclose your personal information if required to do so by law or in response to valid requests by public authorities (e.g., a court or a government agency). We may also disclose information to enforce our terms and conditions, protect our rights or the rights of others, or investigate potential fraud or illegal activity.
          </p>

          <h3 className="text-lg font-medium text-gray-800 mt-8 mb-3">Business Transfers</h3>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            In the event of a merger, acquisition, restructuring, or sale of assets, your personal data may be transferred as part of that transaction. We will notify you via email or a prominent notice on our website if such a transfer affects your data, and we will ensure that the receiving entity honours the commitments set out in this Privacy Policy.
          </p>
        </motion.section>

        {/* Section 6: Data Retention */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="06" title="Data Retention" id="data-retention" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We retain personal information only for as long as necessary to fulfil the purposes for which it was collected, including for the purposes of satisfying any legal, regulatory, accounting, or reporting requirements.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            For property enquiry data, we typically retain records for up to 3 years from the date of your initial enquiry, as this reflects the typical decision-making timeline for a property purchase. If you become a customer of Ishtika Homes, we may retain relevant information for a longer period as required under RERA and other applicable real estate laws.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            Analytics data collected through cookies and similar technologies is typically retained for 12 to 26 months, depending on the tool and configuration. After this period, data is either deleted or anonymised.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            If you request the deletion of your personal data and there is no ongoing legal obligation requiring us to retain it, we will delete or anonymise your information within 30 days of receiving your request.
          </p>
        </motion.section>

        {/* Section 7: Data Security */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="07" title="Data Security" id="security" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We take the security of your personal information seriously and implement appropriate technical and organisational measures to protect it against unauthorised access, disclosure, alteration, or destruction. These measures include:
          </p>
          <ul className="space-y-3 mb-6">
            <CheckItem>Secure HTTPS encryption for all data transmitted between your browser and our website.</CheckItem>
            <CheckItem>Access controls ensuring that only authorised personnel can access personal data relevant to their role.</CheckItem>
            <CheckItem>Regular review and assessment of our data handling and storage practices.</CheckItem>
            <CheckItem>Use of reputable and security-certified third-party service providers for hosting and data processing.</CheckItem>
          </ul>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            While we strive to protect your personal information, no method of transmission over the internet or electronic storage is completely secure. We cannot guarantee absolute security, but we are committed to promptly addressing any breach or vulnerability that comes to our attention.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            If you believe your personal information has been compromised or misused in connection with our services, please contact us immediately at the details provided in the Contact section below.
          </p>
        </motion.section>

        {/* Section 8: Third-Party Websites & Links */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="08" title="Third-Party Websites & Links" id="third-party" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            Our website may contain links to third-party websites, social media platforms, map services, or other external resources. These links are provided for your convenience and informational purposes. Ishtika Homes does not control the content or privacy practices of these external sites and is not responsible for how they collect or use your data.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            We recommend reviewing the privacy policies of any third-party websites you visit through links on our site. Common third-party platforms whose links may appear on our website include Google Maps, Instagram, Facebook, YouTube, and WhatsApp. Each of these platforms maintains its own privacy policy and data practices independent of Ishtika Homes.
          </p>
        </motion.section>

        {/* Section 9: Children's Privacy */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="09" title="Children's Privacy" id="children" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            Our website and services are intended for adults who are considering purchasing or learning about residential properties. We do not knowingly collect personal information from individuals under the age of 18. If we become aware that we have inadvertently collected personal information from a child, we will take prompt steps to delete that information from our records.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            If you believe that a minor has submitted personal information to us through our website, please contact us and we will work to address the matter promptly.
          </p>
        </motion.section>

        {/* Section 10: Your Rights */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="10" title="Your Rights" id="your-rights" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            Depending on your location and the applicable data protection laws, you may have certain rights regarding the personal information we hold about you. Under India's Digital Personal Data Protection Act (DPDPA) 2023 and, where applicable, international frameworks such as the GDPR, these rights may include:
          </p>
          <ul className="space-y-3 mb-6">
            <CheckItem><strong>Right to access</strong> — you may request a copy of the personal information we hold about you and information about how it is used.</CheckItem>
            <CheckItem><strong>Right to correction</strong> — you may request that we correct any personal data that is inaccurate or incomplete.</CheckItem>
            <CheckItem><strong>Right to erasure</strong> — you may request that we delete your personal information where there is no compelling reason for us to continue processing it.</CheckItem>
            <CheckItem><strong>Right to withdraw consent</strong> — where we rely on your consent to process your personal data, you may withdraw that consent at any time. Withdrawal does not affect the lawfulness of processing carried out before withdrawal.</CheckItem>
            <CheckItem><strong>Right to restrict processing</strong> — you may request that we restrict the use of your data in certain circumstances, such as while a dispute is resolved.</CheckItem>
            <CheckItem><strong>Right to data portability</strong> — where applicable, you may request that we provide your personal data to you in a structured, commonly used, and machine-readable format.</CheckItem>
            <CheckItem><strong>Right to object</strong> — you may object to our processing of your personal data where we rely on legitimate interests as our legal basis, including for direct marketing purposes.</CheckItem>
          </ul>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            To exercise any of these rights, please contact us using the details in the Contact section. We will acknowledge your request within 5 business days and aim to fulfil it within 30 days, unless a longer period is permitted by applicable law. We may request proof of identity before processing your request to protect your data from unauthorised access.
          </p>

          <NoteBox>
            Some rights may be subject to limitations or exemptions under applicable law. We will always explain our reasoning if we are unable to fulfil a particular request.
          </NoteBox>
        </motion.section>

        {/* Section 11: Changes to This Policy */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="mb-16 pb-16 border-b border-gray-100"
        >
          <SectionHeader number="11" title="Changes to This Policy" id="changes" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            We reserve the right to update or modify this Privacy Policy at any time. Changes will be posted on this page with a revised "Last Updated" date at the top of the document. We encourage you to check this page periodically to stay informed about how we are protecting your information.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed">
            Where a change is material — for example, where we introduce a new way of using your data — we will endeavour to notify you directly, such as by email or by displaying a notice on our website. Continued use of our website after any changes to this policy constitutes your acceptance of the updated terms.
          </p>
        </motion.section>

        {/* Section 12: Contact Us */}
        <motion.section
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
        >
          <SectionHeader number="12" title="Contact Us" id="contact" />
          <p className="text-gray-600 text-[15px] leading-relaxed mb-4">
            If you have any questions, concerns, or requests relating to this Privacy Policy or your personal data, we would love to hear from you. Our team is committed to addressing all privacy-related enquiries promptly and transparently.
          </p>
          <p className="text-gray-600 text-[15px] leading-relaxed mb-6">
            Please feel free to reach out through any of the channels below and we will get back to you as soon as possible, typically within 5 to 7 business days.
          </p>

          <div className="bg-gray-900 rounded-2xl p-8 md:p-10">
            <h3 className="text-xl font-light text-white mb-6">Ishtika Homes</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <span className="text-[10px] font-medium tracking-[0.15em] uppercase text-white/30 block mb-1">Website</span>
                <p className="text-white/70 text-sm"><a href="https://www.ishtikahomes.com/" className="text-orange-400 hover:underline">www.ishtikahomes.com</a></p>
              </div>
              <div>
                <span className="text-[10px] font-medium tracking-[0.15em] uppercase text-white/30 block mb-1">Location</span>
                <p className="text-white/70 text-sm">Bangalore, Karnataka, India</p>
              </div>
              <div>
                <span className="text-[10px] font-medium tracking-[0.15em] uppercase text-white/30 block mb-1">Email</span>
                <p className="text-white/70 text-sm"><a href="mailto:privacy@ishtikahomes.com" className="text-orange-400 hover:underline">privacy@ishtikahomes.com</a></p>
              </div>
              <div>
                <span className="text-[10px] font-medium tracking-[0.15em] uppercase text-white/30 block mb-1">Response Time</span>
                <p className="text-white/70 text-sm">Within 5-7 business days</p>
              </div>
            </div>
          </div>
        </motion.section>

      </div>

      <Footer />
    </div>
  );
}
