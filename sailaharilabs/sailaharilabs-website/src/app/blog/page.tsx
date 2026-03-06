'use client';

import { motion, useInView, AnimatePresence } from 'framer-motion';
import { useRef, useState } from 'react';
import Image from 'next/image';

// Blog Posts Data
const blogPosts = [
  {
    id: 1,
    title: 'CRAFTING CAPTIVATING HEADLINES: YOUR AWESOME POST TITLE GOES HERE',
    excerpt: 'Engaging Introductions: Capturing Your Audience\'s Interest The initial impression your blog post makes is crucial, and that\'s where your introduction [...]',
    category: 'General',
    author: 'rajasekardl6',
    date: 'December 15, 2025',
    image: 'https://images.unsplash.com/photo-1582719471384-894fbb16e074?w=800',
    comments: 0,
  },
  {
    id: 2,
    title: 'THE ART OF DRAWING READERS IN: YOUR ATTRACTIVE POST TITLE GOES HERE',
    excerpt: 'Engaging Introductions: Capturing Your Audience\'s Interest The initial impression your blog post makes is crucial, and that\'s where your introduction.',
    category: 'General',
    author: 'rajasekardl6',
    date: 'December 12, 2025',
    image: 'https://images.unsplash.com/photo-1576086213369-97a306d36557?w=800',
    comments: 0,
  },
  {
    id: 3,
    title: 'MASTERING THE FIRST IMPRESSION: YOUR INTRIGUING POST TITLE GOES HERE',
    excerpt: 'Engaging Introductions: Capturing Your Audience\'s Interest The initial impression your blog post makes is crucial, and that\'s where your introduction.',
    category: 'General',
    author: 'rajasekardl6',
    date: 'December 10, 2025',
    image: 'https://images.unsplash.com/photo-1581091226825-a6a2a5aee158?w=800',
    comments: 0,
  },
  {
    id: 4,
    title: 'HELLO WORLD!',
    excerpt: 'Welcome to WordPress. This is your first post. Edit or delete it, then start writing!',
    category: 'Uncategorized',
    author: 'rajasekardl6',
    date: 'December 1, 2025',
    image: 'https://images.unsplash.com/photo-1532187863486-abf9dbad1b69?w=800',
    comments: 1,
  },
];

// Categories
const categories = ['All', 'General', 'Research', 'Technology', 'Uncategorized'];

export default function BlogPage() {
  return (
    <div className="overflow-hidden">
      {/* Hero Section */}
      <HeroSection />

      {/* Blog Grid Section */}
      <BlogGridSection />
    </div>
  );
}

// Hero Section
function HeroSection() {
  return (
    <section className="relative pt-32 pb-16 spotlight">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8 }}
          className="text-center max-w-4xl mx-auto"
        >
          <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mx-auto mb-8 rounded-full" />
          <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-white leading-tight mb-6">
            INSIGHTS AND INNOVATIONS IN{' '}
            <span className="gradient-text">LABORATORY SOLUTIONS</span>
          </h1>
          <p className="text-gray-400 text-lg leading-relaxed">
            Stay updated with the latest news, research findings, and innovations
            in the world of laboratory equipment and technology.
          </p>
        </motion.div>
      </div>
    </section>
  );
}

// Blog Grid Section
function BlogGridSection() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });
  const [activeCategory, setActiveCategory] = useState('All');
  const [showComingSoon, setShowComingSoon] = useState(false);

  // Filter posts based on active category
  const filteredPosts = activeCategory === 'All'
    ? blogPosts
    : blogPosts.filter(post => post.category === activeCategory);

  const handleReadPost = (e: React.MouseEvent) => {
    e.preventDefault();
    setShowComingSoon(true);
    setTimeout(() => setShowComingSoon(false), 2000);
  };

  return (
    <section ref={ref} className="py-16 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        {/* Coming Soon Modal */}
        <AnimatePresence>
          {showComingSoon && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm"
              onClick={() => setShowComingSoon(false)}
            >
              <motion.div
                initial={{ scale: 0.8, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                exit={{ scale: 0.8, opacity: 0 }}
                className="glass-card rounded-2xl p-8 text-center max-w-sm mx-4"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="w-16 h-16 rounded-full bg-gradient-to-br from-[#f97066] to-[#fb923c] flex items-center justify-center mx-auto mb-4">
                  <svg className="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </div>
                <h3 className="text-2xl font-bold text-white mb-2">Coming Soon</h3>
                <p className="text-gray-400">This blog post will be available shortly. Stay tuned!</p>
              </motion.div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Categories Filter */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.6 }}
          className="flex flex-wrap justify-center gap-3 mb-12"
        >
          {categories.map((category) => (
            <motion.button
              key={category}
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              onClick={() => setActiveCategory(category)}
              className={`px-3 sm:px-5 py-2 sm:py-2.5 rounded-xl text-xs sm:text-sm font-medium transition-all duration-300 ${
                activeCategory === category
                  ? 'bg-gradient-to-r from-[#f97066] to-[#fb923c] text-white'
                  : 'glass text-gray-300 hover:text-white hover:border-[#f97066]/50'
              }`}
            >
              {category}
            </motion.button>
          ))}
        </motion.div>

        {/* Blog Grid */}
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
          <AnimatePresence mode="wait">
            {filteredPosts.length > 0 ? (
              filteredPosts.map((post, index) => (
                <motion.article
                  key={post.id}
                  initial={{ opacity: 0, y: 30 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -30 }}
                  transition={{ duration: 0.4, delay: index * 0.1 }}
                  className="glass-card rounded-2xl overflow-hidden group"
                >
                  {/* Image */}
                  <div
                    className="block relative h-56 overflow-hidden cursor-pointer"
                    onClick={handleReadPost}
                  >
                    <Image
                      src={post.image}
                      alt={post.title}
                      fill
                      className="object-cover transition-transform duration-500 group-hover:scale-110"
                    />
                    <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0f] via-transparent to-transparent opacity-60" />

                    {/* Category Badge */}
                    <div className="absolute top-4 left-4">
                      <span className="px-3 py-1 rounded-lg bg-[#f97066]/90 text-white text-xs font-medium">
                        {post.category}
                      </span>
                    </div>
                  </div>

                  {/* Content */}
                  <div className="p-6">
                    <h2
                      className="text-lg font-bold text-white mb-3 line-clamp-2 group-hover:text-[#f97066] transition-colors duration-300 cursor-pointer"
                      onClick={handleReadPost}
                    >
                      {post.title}
                    </h2>

                    {/* Meta */}
                    <div className="flex items-center gap-4 text-xs text-gray-500 mb-4">
                      <span className="flex items-center gap-1">
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                        </svg>
                        {post.date}
                      </span>
                      <span className="flex items-center gap-1">
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 8h10M7 12h4m1 8l-4-4H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-3l-4 4z" />
                        </svg>
                        {post.comments} Comment{post.comments !== 1 ? 's' : ''}
                      </span>
                    </div>

                    <p className="text-gray-400 text-sm leading-relaxed mb-4 line-clamp-3">
                      {post.excerpt}
                    </p>

                    <motion.span
                      whileHover={{ x: 5 }}
                      onClick={handleReadPost}
                      className="inline-flex items-center gap-2 text-[#f97066] text-sm font-medium cursor-pointer"
                    >
                      Read Post
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                      </svg>
                    </motion.span>
                  </div>
                </motion.article>
              ))
            ) : (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="col-span-full text-center py-16"
              >
                <div className="w-20 h-20 rounded-full bg-gradient-to-br from-[#f97066]/20 to-[#fb923c]/20 flex items-center justify-center mx-auto mb-6">
                  <svg className="w-10 h-10 text-[#f97066]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 20H5a2 2 0 01-2-2V6a2 2 0 012-2h10a2 2 0 012 2v1m2 13a2 2 0 01-2-2V7m2 13a2 2 0 002-2V9a2 2 0 00-2-2h-2m-4-3H9M7 16h6M7 8h6v4H7V8z" />
                  </svg>
                </div>
                <h3 className="text-2xl font-bold text-white mb-2">No Posts Found</h3>
                <p className="text-gray-400">There are no posts in the &quot;{activeCategory}&quot; category yet.</p>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Load More Button */}
        {filteredPosts.length > 0 && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={isInView ? { opacity: 1, y: 0 } : {}}
            transition={{ duration: 0.6, delay: 0.5 }}
            className="text-center mt-12"
          >
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              onClick={() => setShowComingSoon(true)}
              className="btn-secondary"
            >
              Load More Posts
            </motion.button>
          </motion.div>
        )}
      </div>
    </section>
  );
}
