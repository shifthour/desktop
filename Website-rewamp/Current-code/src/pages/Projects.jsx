import React, { useState, useEffect } from 'react';
import { Helmet } from 'react-helmet-async';
import { motion } from 'framer-motion';
import { MapPin, Home, Calendar, Building2, ArrowRight } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import projectsData from '@/data/projects';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';
import EditableImage from '@/components/EditableImage';

const projectOrder = ['anahata', 'krishna', 'naadam', 'vyasa', 'vashishta'];

const sortProjects = (projects) => {
  return [...projects].sort((a, b) => {
    const aName = a.name.toLowerCase().replace(/\s+/g, '');
    const bName = b.name.toLowerCase().replace(/\s+/g, '');
    const aIndex = projectOrder.indexOf(aName);
    const bIndex = projectOrder.indexOf(bName);
    
    if (aIndex === -1 && bIndex === -1) return 0;
    if (aIndex === -1) return 1;
    if (bIndex === -1) return -1;
    return aIndex - bIndex;
  });
};

export default function Projects() {
  const [filter, setFilter] = useState("ongoing");

  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  const sortedProjects = sortProjects(projectsData);
  
  const filteredProjects = sortedProjects.filter(p => p.status === filter);

  return (
    <div className="bg-white min-h-screen pt-24">
      <Helmet>
        <title>Our Projects | Ishtika Homes - Premium Apartments in Karnataka</title>
        <meta name="description" content="Explore Ishtika Homes projects - Anahata in Whitefield, Krishna in Hosapete, Naadam, Vyasa, and more. Premium 2BHK & 3BHK Vaastu-compliant apartments across Karnataka." />
        <link rel="canonical" href="https://www.ishtikahomes.com/projects" />
        <meta property="og:title" content="Our Projects | Ishtika Homes" />
        <meta property="og:description" content="Explore premium residential developments across Karnataka by Ishtika Homes. Ongoing and completed projects." />
        <meta property="og:url" content="https://www.ishtikahomes.com/projects" />
        <meta property="og:type" content="website" />
      </Helmet>
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
            Our <span className="text-orange-500">Projects</span>
          </motion.h1>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-lg text-gray-600"
          >
            Discover premium residential developments across Karnataka
          </motion.p>
        </div>
      </section>

      {/* Filter Tabs */}
      <section className="py-8 px-4">
        <div className="max-w-7xl mx-auto flex justify-center">
          <Tabs value={filter} onValueChange={setFilter}>
            <TabsList className="bg-gray-100">
              <TabsTrigger value="ongoing">Ongoing</TabsTrigger>
              <TabsTrigger value="completed">Completed</TabsTrigger>
              <TabsTrigger value="upcoming">Upcoming</TabsTrigger>
            </TabsList>
          </Tabs>
        </div>
      </section>

      {/* Projects Grid / Upcoming Section */}
      <section className="py-12 px-4">
        <div className="max-w-7xl mx-auto">
          {filter === "upcoming" ? (
            <div className="bg-gradient-to-br from-gray-900 to-gray-800 rounded-3xl p-8 md:p-12 text-white shadow-2xl">
              <h2 className="text-3xl md:text-4xl font-light text-orange-500 mb-4 text-center">
                Exciting New Projects Coming Soon!
              </h2>
              <p className="text-gray-300 text-center mb-8 text-lg">
                We are thrilled to announce our upcoming premium developments in prime locations:
              </p>
              
              <div className="space-y-6 max-w-4xl mx-auto mb-8">
                <div className="bg-gradient-to-r from-orange-900/40 to-orange-800/40 rounded-xl p-6 border border-orange-700/30">
                  <div className="flex items-start gap-4">
                    <div className="bg-orange-500/20 p-3 rounded-lg flex-shrink-0">
                      <Building2 className="w-6 h-6 text-orange-400" />
                    </div>
                    <div>
                      <h3 className="text-xl font-semibold text-orange-400 mb-2">Two High-Rise Projects</h3>
                      <p className="text-gray-300">
                        Strategically located in Whitefield, these premium high-rise developments will offer modern amenities and exceptional connectivity.
                      </p>
                    </div>
                  </div>
                </div>
                
                <div className="bg-gradient-to-r from-orange-900/40 to-orange-800/40 rounded-xl p-6 border border-orange-700/30">
                  <div className="flex items-start gap-4">
                    <div className="bg-orange-500/20 p-3 rounded-lg flex-shrink-0">
                      <Home className="w-6 h-6 text-orange-400" />
                    </div>
                    <div>
                      <h3 className="text-xl font-semibold text-orange-400 mb-2">Exclusive Villa Project</h3>
                      <p className="text-gray-300">
                        A luxurious villa community in Whitefield, designed for those who seek privacy, space, and sophisticated living.
                      </p>
                    </div>
                  </div>
                </div>
                
                <div className="bg-gray-800/50 rounded-xl p-4 border border-gray-700">
                  <p className="text-sm text-gray-400">
                    <span className="font-semibold text-orange-400">Note:</span> RERA approval is currently in progress. Project names and detailed specifications will be disclosed upon approval.
                  </p>
                </div>
              </div>
              
              <div className="text-center">
                <Link to={createPageUrl("Contact")}>
                  <Button 
                    size="lg" 
                    style={{ backgroundColor: '#FF8C00', color: 'white' }}
                    className="hover:opacity-90 px-8 py-6 text-base"
                  >
                    Get Notified
                    <ArrowRight className="w-5 h-5 ml-2" />
                  </Button>
                </Link>
              </div>
            </div>
          ) : filteredProjects.length === 0 ? (
            <div className="text-center py-20">
              <p className="text-gray-500">No projects found</p>
            </div>
          ) : (
            <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
              {filteredProjects.map((project, index) => (
                <motion.div
                  key={project.id}
                  initial={{ opacity: 0, y: 40 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: index * 0.1 }}
                  className="group h-full"
                >
                  <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden shadow-md hover:shadow-xl transition-all h-full flex flex-col">
                    {/* Image */}
                    <div className="relative h-48 md:h-56 overflow-hidden flex-shrink-0">
                      <EditableImage
                        imageKey={`project-${project.name.toLowerCase().replace(/\s+/g, '')}`}
                        src={project.image || "/assets/img/aerial-view-bg.jpg"}
                        alt={project.name}
                        className="w-full h-full object-cover transition-transform duration-700 group-hover:scale-110"
                      />
                    <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-black/20 to-transparent" />
                    
                    {/* Status Badge */}
                    <div className="absolute top-4 right-4">
                      <span className={`px-3 py-1 rounded-full text-xs font-semibold ${
                        project.status === 'ongoing' 
                          ? 'bg-blue-500 text-white' 
                          : 'bg-green-500 text-white'
                      }`}>
                        {project.status === 'ongoing' ? 'Ongoing' : 'Completed'}
                      </span>
                    </div>

                    <div className="absolute bottom-4 left-4 right-4">
                      <h3 className="text-2xl font-semibold text-white mb-2">{project.name}</h3>
                      <div className="flex items-center gap-2 text-orange-400 text-sm">
                        <MapPin className="w-4 h-4" />
                        <span>{project.location}</span>
                      </div>
                    </div>
                  </div>

                  {/* Content */}
                  <div className="p-6 flex flex-col flex-grow">
                    <p className="text-gray-600 text-sm mb-4 leading-relaxed flex-grow">
                      {project.description}
                    </p>

                    <div className="space-y-2 mb-6">
                      <div className="flex items-center gap-2 text-sm text-gray-700">
                        <Home className="w-4 h-4 text-orange-500" />
                        <span>{project.units} | {project.configuration}</span>
                      </div>
                      <div className="flex items-center gap-2 text-sm text-gray-700">
                        <Calendar className="w-4 h-4 text-orange-500" />
                        <span>{project.possession}</span>
                      </div>
                    </div>

                    {(project.status === 'ongoing' || project.name === 'Vashishta') && (
                      <Link to={createPageUrl(project.name)}>
                        <Button className="w-full bg-orange-500 hover:bg-orange-600 text-white">
                          View Details
                        </Button>
                      </Link>
                    )}
                  </div>
                </div>
              </motion.div>
              ))}
            </div>
          )}
        </div>
      </section>

      <Footer />
    </div>
  );
}