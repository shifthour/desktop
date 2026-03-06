import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from "@/components/ui/button";

const testimonials = [
  {
    quote: "Booking our dream home at Anahata was the best decision for our family. The 3BHK apartment design is spacious, well-ventilated, and the vaastu compliance gives us peace of mind. We're excited about the clubhouse and children's play area. The construction quality and regular updates from Ishtika Homes have been exceptional. Can't wait for possession in March 2028!",
    name: "Rajesh & Lakshmi Reddy",
    project: "Anahata",
    location: "Anahata, Whitefield",
    badge: "Ongoing Project",
    image: "https://www.ishtikahomes.com/assets/img/all-images/client1.jpg",
  },
  {
    quote: "Living in Vashishta, JP Nagar has been a dream come true. The location is perfect with schools, hospitals, and shopping centers nearby. What impressed us most was the transparent pricing and no hidden costs. Ishtika Homes truly cares about their customers and it shows in every interaction.",
    name: "Arjun & Priya Sharma",
    project: "Vashishta",
    location: "Vashishta, JP Nagar",
    badge: null,
    image: "https://www.ishtikahomes.com/assets/img/all-images/client3.jpg",
  },
  {
    quote: "Our apartment in White Pearl is not just a house, it's a home. The elegant design, sustainable construction practices, and the beautiful landscaping make it a perfect blend of luxury and nature. We appreciate Ishtika Homes' commitment to timely delivery and quality construction.",
    name: "Venkatesh & Anjali Rao",
    project: "White Pearl",
    location: "White Pearl, Bellary",
    badge: null,
    image: "https://www.ishtikahomes.com/assets/img/all-images/client4.jpg",
  },
  {
    quote: "After visiting multiple projects, we chose Advaitha for its spacious layout and modern amenities. The vaastu-compliant design was a major factor for us. The customer service team at Ishtika Homes has been excellent, always available to address our queries even after possession!",
    name: "Karthik & Divya Murthy",
    project: "Advaitha",
    location: "Advaitha, Bangalore",
    badge: null,
    image: "https://www.ishtikahomes.com/assets/img/all-images/client5.jpg",
  },
  {
    quote: "Investing in Anahata was one of our best financial decisions. We booked our home here after thorough research, and the gated community design offers excellent security and lifestyle amenities. The construction progress has been remarkable and Ishtika Homes keeps us updated regularly. Highly recommended for families looking for their dream home!",
    name: "Ramesh & Sowmya Gowda",
    project: "Anahata",
    location: "Anahata, Whitefield",
    badge: "Ongoing Project",
    image: "https://www.ishtikahomes.com/assets/img/all-images/client6.jpg",
  },
];

export default function TestimonialsSection() {
  const [current, setCurrent] = useState(0);

  const getImage = (index) => testimonials[index].image;

  const next = () => setCurrent((prev) => (prev + 1) % testimonials.length);
  const prev = () => setCurrent((prev) => (prev - 1 + testimonials.length) % testimonials.length);

  return (
    <section className="bg-white py-8 md:py-12 px-4 overflow-hidden">
      <div className="max-w-5xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">
            Testimonials
          </p>
          <h2 className="text-3xl md:text-4xl font-light text-gray-800">
            Real Stories from Happy Homeowners
          </h2>
        </motion.div>

        <div className="relative">
          <AnimatePresence mode="wait">
            <motion.div
              key={current}
              initial={{ opacity: 0, x: 50 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -50 }}
              transition={{ duration: 0.5 }}
              className="bg-white border border-gray-200 rounded-3xl p-8 md:p-12 shadow-lg"
            >
              <div className="flex items-center justify-between mb-6">
                <div className="flex gap-1">
                  {[...Array(5)].map((_, i) => (
                    <svg key={i} className="w-5 h-5 fill-yellow-400" viewBox="0 0 20 20">
                      <path d="M10 15l-5.878 3.09 1.123-6.545L.489 6.91l6.572-.955L10 0l2.939 5.955 6.572.955-4.756 4.635 1.123 6.545z" />
                    </svg>
                  ))}
                </div>
                <svg className="w-6 h-6 flex-shrink-0" viewBox="0 0 24 24">
                  <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
                  <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
                  <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
                  <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
                </svg>
              </div>
              
              <p className="text-gray-700 text-lg md:text-xl leading-relaxed mb-8">
                "{testimonials[current].quote}"
              </p>

              <div className="flex items-center gap-3">
                <div className="w-16 h-16 flex-shrink-0">
                  <img
                    src={getImage(current)}
                    alt={testimonials[current].name}
                    className="w-16 h-16 rounded-full object-cover border-2 border-orange-400"
                  />
                </div>
                <div>
                  <h4 className="text-gray-800 font-semibold text-base">{testimonials[current].name}</h4>
                  <p className="text-gray-500 text-sm">{testimonials[current].location}</p>
                  {testimonials[current].badge && (
                    <span className="inline-block px-2 py-0.5 bg-orange-100 text-orange-600 text-xs rounded-full font-medium mt-1">
                      {testimonials[current].badge}
                    </span>
                  )}
                </div>
              </div>
            </motion.div>
          </AnimatePresence>

          {/* Navigation */}
          <div className="flex justify-center gap-4 mt-8">
            <Button
              variant="outline"
              size="icon"
              onClick={prev}
              className="rounded-full border-gray-300 text-gray-700 hover:bg-orange-500 hover:border-orange-500 hover:text-white"
            >
              <ChevronLeft className="w-5 h-5" />
            </Button>
            
            <div className="flex items-center gap-2">
              {testimonials.map((_, index) => (
                <button
                  key={index}
                  onClick={() => setCurrent(index)}
                  className={`w-2 h-2 rounded-full transition-all ${
                    index === current 
                      ? 'w-6 bg-orange-500' 
                      : 'bg-gray-300 hover:bg-gray-400'
                  }`}
                />
              ))}
            </div>
            
            <Button
              variant="outline"
              size="icon"
              onClick={next}
              className="rounded-full border-gray-300 text-gray-700 hover:bg-orange-500 hover:border-orange-500 hover:text-white"
            >
              <ChevronRight className="w-5 h-5" />
            </Button>
          </div>
        </div>
      </div>
    </section>
  );
}