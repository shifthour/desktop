import React from 'react';
import { motion } from 'framer-motion';
import { Calendar, ArrowRight, Clock } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import allBlogPosts from '@/data/blogPosts';

const blogPosts = allBlogPosts.slice(0, 3);

export default function BlogSection() {
  return (
    <section className="bg-gray-50 py-24 px-4">
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <p className="text-sm tracking-[0.3em] uppercase text-orange-500 mb-4 font-semibold">
            Latest Insights
          </p>
          <h2 className="text-3xl md:text-4xl font-light text-gray-800 mb-4">
            From Our Blog
          </h2>
          <p className="text-gray-600 max-w-2xl mx-auto">
            Stay updated with the latest news, tips, and insights about real estate and home buying
          </p>
        </motion.div>

        <div className="grid md:grid-cols-3 gap-8 mb-12">
          {blogPosts.map((post, index) => (
            <motion.article
              key={post.title}
              initial={{ opacity: 0, y: 40 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: index * 0.15 }}
              viewport={{ once: true }}
              className="group bg-white rounded-2xl overflow-hidden shadow-md hover:shadow-xl transition-all duration-300"
            >
              {/* Image */}
              <div className="relative aspect-[16/10] overflow-hidden">
                <img
                  src={post.image}
                  alt={post.title}
                  className="w-full h-full object-cover transition-transform duration-700 group-hover:scale-110"
                />
                <div className="absolute top-4 left-4">
                  <span className="bg-orange-500 text-white px-3 py-1 rounded-full text-xs font-semibold">
                    {post.category}
                  </span>
                </div>
              </div>

              {/* Content */}
              <div className="p-6">
                <div className="flex items-center gap-4 text-sm text-gray-500 mb-3">
                  <div className="flex items-center gap-1">
                    <Calendar className="w-4 h-4" />
                    <span>{post.date}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <Clock className="w-4 h-4" />
                    <span>{post.readTime}</span>
                  </div>
                </div>

                <Link to={`${createPageUrl('BlogPost')}?id=${post.id}`}>
                  <h3 className="text-xl font-semibold text-gray-800 mb-3 line-clamp-2 group-hover:text-orange-500 transition-colors">
                    {post.title}
                  </h3>
                </Link>

                <p className="text-gray-600 text-sm leading-relaxed mb-4 line-clamp-3">
                  {post.excerpt}
                </p>

                <Link to={`${createPageUrl('BlogPost')}?id=${post.id}`}>
                  <Button
                    variant="ghost"
                    className="p-0 h-auto text-orange-500 hover:text-orange-600 hover:bg-transparent group/btn font-medium"
                  >
                    Read More
                    <ArrowRight className="w-4 h-4 ml-2 transition-transform group-hover/btn:translate-x-1" />
                  </Button>
                </Link>
              </div>
            </motion.article>
          ))}
        </div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.5 }}
          viewport={{ once: true }}
          className="text-center"
        >
          <Button
            size="lg"
            variant="outline"
            className="border-orange-500 text-orange-500 hover:bg-orange-500 hover:text-white rounded-full px-8"
            asChild
          >
            <Link to={createPageUrl('Blog')}>
              View All Articles
              <ArrowRight className="w-4 h-4 ml-2" />
            </Link>
          </Button>
        </motion.div>
      </div>
    </section>
  );
}