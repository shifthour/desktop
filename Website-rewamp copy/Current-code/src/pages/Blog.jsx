import React, { useEffect } from 'react';
import { motion } from 'framer-motion';
import { Calendar, Clock, ArrowRight, User } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';
import blogPosts from '@/data/blogPosts';

export default function Blog() {
  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

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
            Our <span className="text-orange-500">Blog</span>
          </motion.h1>
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-lg text-gray-600"
          >
            Latest insights, tips, and news about real estate and home buying
          </motion.p>
        </div>
      </section>

      {/* Blog Grid */}
      <section className="py-12 px-4">
        <div className="max-w-7xl mx-auto">
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
            {blogPosts.map((post, index) => (
              <motion.article
                key={post.id}
                initial={{ opacity: 0, y: 40 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1 }}
                className="group bg-white border border-gray-200 rounded-2xl overflow-hidden shadow-md hover:shadow-xl transition-all"
              >
                {/* Image */}
                <Link to={`${createPageUrl('BlogPost')}?id=${post.id}`}>
                  <div className="relative aspect-[16/10] overflow-hidden cursor-pointer bg-gray-200">
                    <img
                      src={post.image || 'https://www.ishtikahomes.com/assets/img/all-images/blog-img1.png'}
                      alt={post.featuredImageAlt || post.title}
                      className="w-full h-full object-cover transition-transform duration-700 group-hover:scale-110"
                    />
                    <div className="absolute top-4 left-4">
                      <span className="bg-orange-500 text-white px-3 py-1 rounded-full text-xs font-semibold">
                        {post.category}
                      </span>
                    </div>
                  </div>
                </Link>

                {/* Content */}
                <div className="p-6">
                  <div className="flex items-center gap-4 text-xs text-gray-500 mb-3">
                    <div className="flex items-center gap-1">
                      <Calendar className="w-3 h-3" />
                      <span>{post.date}</span>
                    </div>
                    <div className="flex items-center gap-1">
                      <Clock className="w-3 h-3" />
                      <span>{post.readTime}</span>
                    </div>
                  </div>

                  <Link to={`${createPageUrl('BlogPost')}?id=${post.id}`}>
                    <h3 className="text-xl font-semibold text-gray-800 mb-3 line-clamp-2 group-hover:text-orange-500 transition-colors cursor-pointer">
                      {post.title}
                    </h3>
                  </Link>

                  <p className="text-gray-600 text-sm leading-relaxed mb-4 line-clamp-3">
                    {post.excerpt}
                  </p>

                  <div className="flex items-center justify-between pt-4 border-t border-gray-100">
                    <div className="flex items-center gap-2 text-sm text-gray-600">
                      <User className="w-4 h-4" />
                      <span>{post.author}</span>
                    </div>

                    <Link to={`${createPageUrl('BlogPost')}?id=${post.id}`}>
                      <Button
                        variant="ghost"
                        className="p-0 h-auto text-orange-500 hover:text-orange-600 hover:bg-transparent group/btn"
                      >
                        Read
                        <ArrowRight className="w-4 h-4 ml-1 transition-transform group-hover/btn:translate-x-1" />
                      </Button>
                    </Link>
                  </div>
                </div>
              </motion.article>
            ))}
          </div>
        </div>
      </section>

      <Footer />
    </div>
  );
}
