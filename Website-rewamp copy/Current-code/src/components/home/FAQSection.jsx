import React from 'react';
import { motion } from 'framer-motion';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

const faqs = [
  {
    question: "What is the best area to buy apartments in Bangalore?",
    answer: "For convenience, growth, and connectivity, Whitefield, the IT hubs, and the well-developed residential areas are the best choices. These locations offer excellent infrastructure, proximity to workplaces, schools, hospitals, and shopping centers, making them ideal for families and professionals alike.",
  },
  {
    question: "Is it better to buy 2BHK or 3BHK apartments in Bangalore?",
    answer: "2BHK apartments are ideal for single professionals and small families, offering affordability and smart space utilization. 3BHK apartments are better suited for larger families or those needing extra space for home offices, guest rooms, or growing families. The choice depends on your family size, lifestyle needs, and budget.",
  },
  {
    question: "Are Ishtika Homes apartments vastu-compliant?",
    answer: "Yes, all Ishtika Homes apartments are 100% vastu-compliant. We follow authentic Vastu Shastra design principles to promote positive energies, harmony, and balanced living in every home. Our architects carefully plan each unit to ensure proper orientation, room placements, and energy flow.",
  },
  {
    question: "Is buying apartments in Bangalore a good investment?",
    answer: "Yes, apartments in Bangalore enjoy excellent rental demand, strong capital appreciation, and great long-term value. As India's IT capital with a booming economy and growing infrastructure, Bangalore continues to attract professionals and families, making real estate here a smart investment choice.",
  },
  {
    question: "Why should I buy apartments from Ishtika Homes in Bangalore?",
    answer: "Ishtika Homes has 12+ years of experience in the real estate sector. We provide apartment offerings in Bangalore that are 100% vastu compliant and of great quality. Key reasons include: over 12 years in real estate development, several completed residential projects, thousands of satisfied families, transparent quality construction, and on-time project delivery.",
  },
];

export default function FAQSection() {
  return (
    <section className="bg-white py-8 md:py-12 px-4">
      <div className="max-w-3xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">
            FAQs
          </p>
          <h2 className="text-3xl md:text-4xl font-light text-gray-800 mb-4">
            Home Buying FAQs
          </h2>
          <p className="text-gray-600">
            Find answers to your questions about buying apartments in Bangalore
          </p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.2 }}
          viewport={{ once: true }}
        >
          <Accordion type="single" collapsible className="space-y-4">
            {faqs.map((faq, index) => (
              <AccordionItem
                key={index}
                value={`item-${index}`}
                className="bg-gray-50 border border-gray-200 rounded-xl px-6 data-[state=open]:border-orange-400 data-[state=open]:bg-white"
              >
                <AccordionTrigger className="text-left text-gray-800 hover:text-orange-500 hover:no-underline py-6 font-medium">
                  {faq.question}
                </AccordionTrigger>
                <AccordionContent className="text-gray-600 pb-6 leading-relaxed">
                  {faq.answer}
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        </motion.div>
      </div>
    </section>
  );
}