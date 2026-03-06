import React from 'react';
import { motion } from 'framer-motion';
import { MapPin, ArrowRight } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import EditableImage from '@/components/EditableImage';
import projectsData from '@/data/projects';

// Pick 3 featured ongoing projects from static data
const featuredNames = ["Anahata", "Naadam", "Krishna"];
const projects = featuredNames.map(name => {
  const p = projectsData.find(proj => proj.name === name);
  return {
    name: p.name,
    location: p.location,
    description: p.description,
    image: p.image,
  };
});

export default function FeaturedProjects() {
  return (
    <section className="bg-white py-8 md:py-12 px-4">
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <h2 className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">
            Featured Projects
          </h2>
          <p className="text-2xl md:text-3xl text-gray-800 font-light">
            Discover our premium residential developments across Bangalore
          </p>
        </motion.div>

        <div className="grid md:grid-cols-3 gap-8">
          {projects.map((project, index) => (
            <motion.div
              key={project.name}
              initial={{ opacity: 0, y: 40 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: index * 0.15 }}
              viewport={{ once: true }}
              className="group"
            >
              <div className="relative overflow-hidden rounded-2xl bg-gray-100 shadow-lg hover:shadow-xl transition-shadow h-[300px] sm:h-[350px] md:h-[400px]">
                <div className="absolute inset-0">
                  <EditableImage
                    imageKey={`featured-project-${project.name.toLowerCase()}`}
                    src={project.image}
                    alt={project.name}
                    className="w-full h-full object-cover transition-transform duration-700 group-hover:scale-110"
                  />
                </div>
                <div className="absolute inset-0 bg-gradient-to-t from-black/90 via-black/50 to-transparent pointer-events-none" />
                
                <div className="absolute bottom-0 left-0 right-0 p-6">
                  <h3 className="text-2xl font-semibold text-white mb-2">
                    {project.name}
                  </h3>
                  
                  <div className="flex items-center gap-2 text-orange-400 text-sm mb-3 font-medium">
                    <MapPin className="w-4 h-4" />
                    <span>{project.location}</span>
                  </div>
                  
                  <p className="text-white text-sm leading-relaxed mb-4 line-clamp-2">
                    {project.description}
                  </p>
                  
                  <Link to={createPageUrl(project.name)}>
                    <Button
                      variant="ghost"
                      className="p-0 h-auto text-orange-400 hover:text-orange-300 hover:bg-transparent group/btn font-medium"
                    >
                      View Details
                      <ArrowRight className="w-4 h-4 ml-2 transition-transform group-hover/btn:translate-x-1" />
                    </Button>
                  </Link>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}