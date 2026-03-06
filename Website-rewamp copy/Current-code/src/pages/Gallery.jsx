import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { X } from 'lucide-react';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';
import EditableImage from '@/components/EditableImage';

const galleryImages = [
  { url: "https://www.ishtikahomes.com/assets/img/aerial-view-bg.jpg", category: "anahata", title: "Anahata" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/anahata-img2.png", category: "anahata", title: "Anahata" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/anahata-img3.png", category: "anahata", title: "Anahata" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/anahata-img4.png", category: "anahata", title: "Anahata" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/anahata-img5.png", category: "anahata", title: "Anahata" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/anahata-img6.png", category: "anahata", title: "Anahata" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/krishna-img1.png", category: "krishna", title: "Krishna" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/krishna-img2.png", category: "krishna", title: "Krishna" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/krishna-img3.png", category: "krishna", title: "Krishna" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/krishna-img4.png", category: "krishna", title: "Krishna" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/krishna-img5.png", category: "krishna", title: "Krishna" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/krishna-img6.png", category: "krishna", title: "Krishna" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vashishta-img1.png", category: "vashishta", title: "Vashishta" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vashishta-img2.png", category: "vashishta", title: "Vashishta" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vashishta-img3.png", category: "vashishta", title: "Vashishta" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vashishta-img4.png", category: "vashishta", title: "Vashishta" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vashishta-img5.png", category: "vashishta", title: "Vashishta" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vashishta-img6.png", category: "vashishta", title: "Vashishta" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/naadam-img1.png", category: "naadam", title: "Naadam" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/naadam-img2.png", category: "naadam", title: "Naadam" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/naadam-img3.png", category: "naadam", title: "Naadam" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/naadam-img4.png", category: "naadam", title: "Naadam" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/naadam-img5.png", category: "naadam", title: "Naadam" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/naadam-img6.png", category: "naadam", title: "Naadam" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vyasa-img1.png", category: "vyasa", title: "Vyasa" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vyasa-img2.png", category: "vyasa", title: "Vyasa" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vyasa-img3.png", category: "vyasa", title: "Vyasa" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vyasa-img4.png", category: "vyasa", title: "Vyasa" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/vyasa-img5.png", category: "vyasa", title: "Vyasa" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/agastya-img1.png", category: "agastya", title: "Agastya" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/agastya-img2.png", category: "agastya", title: "Agastya" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/agastya-img3.png", category: "agastya", title: "Agastya" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/agastya-img4.png", category: "agastya", title: "Agastya" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/agastya-img5.png", category: "agastya", title: "Agastya" },
  { url: "https://www.ishtikahomes.com/assets/img/all-images/agastya-img6.png", category: "agastya", title: "Agastya" },
];

const allProjects = ["anahata", "krishna", "vashishta", "naadam", "vyasa", "agastya"];

export default function Gallery() {
  const [projectFilter, setProjectFilter] = useState("all");
  const [selectedImage, setSelectedImage] = useState(null);

  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  const filteredImages = projectFilter === "all"
    ? galleryImages
    : galleryImages.filter(img => img.category === projectFilter);

  return (
    <div className="bg-white min-h-screen pt-24">
      <Navbar />
      <WhatsAppButton />

      {/* Hero */}
      <section className="py-20 px-4 bg-gradient-to-b from-orange-50 to-white">
        <div className="max-w-4xl mx-auto text-center">
          <motion.h1
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            className="text-4xl md:text-6xl font-light text-gray-800 mb-6"
          >
            Our <span className="text-orange-500">Gallery</span>
          </motion.h1>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-lg text-gray-600"
          >
            Explore our stunning collection of completed and ongoing projects
          </motion.p>
        </div>
      </section>

      {/* Filter */}
      <section className="py-6 px-4">
        <div className="max-w-7xl mx-auto flex flex-col items-center gap-4">
          <div className="flex flex-wrap justify-center gap-2">
            {["all", ...allProjects].map(proj => (
              <button
                key={proj}
                onClick={() => setProjectFilter(proj)}
                className={`px-5 py-2 rounded-full text-sm font-medium transition-all border ${
                  projectFilter === proj
                    ? "bg-orange-500 text-white border-orange-500"
                    : "bg-white text-gray-600 border-gray-300 hover:border-orange-400 hover:text-orange-500"
                }`}
              >
                {proj === "all" ? "All" : proj.charAt(0).toUpperCase() + proj.slice(1)}
              </button>
            ))}
          </div>
        </div>
      </section>

      {/* Gallery Grid */}
      <section className="py-12 px-4 pb-24">
        <div className="max-w-7xl mx-auto">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 md:gap-6">
            {filteredImages.map((image, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: index * 0.05 }}
                className="group cursor-pointer"
                onClick={() => setSelectedImage(image)}
              >
                <div className="relative aspect-[4/3] overflow-hidden rounded-xl shadow-md hover:shadow-xl transition-all">
                  <EditableImage
                    imageKey={`gallery-${image.category}-${index}`}
                    src={image.url}
                    alt={image.title}
                    className="w-full h-full object-cover transition-transform duration-500 group-hover:scale-110"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 group-hover:opacity-100 transition-opacity">
                    <div className="absolute bottom-4 left-4 right-4 text-center">
                      <p className="text-white font-medium">{image.title}</p>
                    </div>
                  </div>
                </div>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* Lightbox */}
      {selectedImage && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="fixed inset-0 bg-black/95 z-50 flex items-center justify-center p-4"
          onClick={() => setSelectedImage(null)}
        >
          <button
            className="absolute top-4 right-4 text-white hover:text-orange-400 transition-colors"
            onClick={() => setSelectedImage(null)}
          >
            <X className="w-8 h-8" />
          </button>
          <motion.img
            initial={{ scale: 0.8 }}
            animate={{ scale: 1 }}
            src={selectedImage.url}
            alt={selectedImage.title}
            className="max-w-full max-h-full object-contain rounded-lg"
          />
        </motion.div>
      )}

      <Footer />
    </div>
  );
}
