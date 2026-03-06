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
    question: "What is the possession timeline for Anahata?",
    answer: "Anahata is scheduled for possession in March 2028. The construction is progressing on schedule, and we provide regular updates to our buyers.",
  },
  {
    question: "Is Anahata a Vastu-compliant project?",
    answer: "Yes, Anahata is 100% Vastu-compliant. All apartments are designed according to authentic Vastu Shastra principles to ensure positive energy flow and harmonious living.",
  },
  {
    question: "What are the available configurations?",
    answer: "Anahata offers 2 BHK apartments (1164-1279 sq ft), 3 BHK 2T apartments (1511-1591 sq ft), and 3 BHK 3T apartments (1670-1758 sq ft).",
  },
  {
    question: "What is the payment plan?",
    answer: "We offer flexible payment plans with no Pre-EMI till possession. Detailed payment schedules are available with our sales team during site visits.",
  },
  {
    question: "What amenities are included in the project?",
    answer: "Anahata features 50+ premium amenities including swimming pool, fully-equipped gym, clubhouse, children's play area, landscaped gardens, 24/7 security, power backup, and much more.",
  },
  {
    question: "Is home loan assistance available?",
    answer: "Yes, we have tie-ups with leading banks and financial institutions to help you secure home loans at competitive interest rates.",
  },
  {
    question: "What is the carpet area vs super built-up area?",
    answer: "The sizes mentioned are super built-up areas. Carpet area constitutes approximately 70-75% of the super built-up area. Detailed breakdowns are available in the floor plans.",
  },
  {
    question: "Are there any maintenance charges?",
    answer: "Yes, monthly maintenance charges will be applicable after possession to cover common area maintenance, security, and amenities upkeep. Details will be shared during the booking process.",
  },
];

export default function AnahataFAQ() {
  return (
    <section className="py-6 md:py-20 px-4 bg-gray-50">
      <div className="max-w-4xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <h2 className="text-3xl md:text-4xl font-light text-gray-800 mb-4">
            Frequently Asked Questions
          </h2>
          <p className="text-gray-600">
            Find answers to common questions about Anahata
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
                className="bg-white border border-gray-200 rounded-xl px-6 data-[state=open]:border-orange-400"
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