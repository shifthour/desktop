import React from 'react';
import { motion } from 'framer-motion';
import { Share2, Facebook, Linkedin, Twitter, Mail, Phone } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Link, useSearchParams } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import blogPosts from '@/data/blogPosts';

export default function BlogSidebar({ currentPostId }) {
  const shareUrl = window.location.href;
  const shareTitle = "Check out this article from Ishtika Homes";

  const relatedBlogs = blogPosts
    .filter(post => post.id !== currentPostId)
    .slice(0, 3);

  const handleShare = (platform) => {
    const urls = {
      facebook: `https://www.facebook.com/sharer/sharer.php?u=${encodeURIComponent(shareUrl)}`,
      twitter: `https://twitter.com/intent/tweet?url=${encodeURIComponent(shareUrl)}&text=${encodeURIComponent(shareTitle)}`,
      linkedin: `https://www.linkedin.com/sharing/share-offsite/?url=${encodeURIComponent(shareUrl)}`,
      email: `mailto:?subject=${encodeURIComponent(shareTitle)}&body=${encodeURIComponent(shareUrl)}`
    };
    
    window.open(urls[platform], '_blank', 'width=600,height=400');
  };

  return (
    <div className="space-y-8 lg:sticky lg:top-24">
      {/* Social Sharing */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="bg-white rounded-xl p-6 border border-gray-200"
      >
        <div className="flex items-center gap-2 mb-4">
          <Share2 className="w-5 h-5 text-orange-500" />
          <h3 className="font-semibold text-gray-800">Share this article</h3>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={() => handleShare('facebook')}
            className="flex-1"
          >
            <Facebook className="w-4 h-4" />
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => handleShare('twitter')}
            className="flex-1"
          >
            <Twitter className="w-4 h-4" />
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => handleShare('linkedin')}
            className="flex-1"
          >
            <Linkedin className="w-4 h-4" />
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => handleShare('email')}
            className="flex-1"
          >
            <Mail className="w-4 h-4" />
          </Button>
        </div>
      </motion.div>

      {/* CTA */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="rounded-xl p-6 text-white"
        style={{ backgroundColor: '#FF8C00' }}
      >
        <h3 className="text-xl font-semibold mb-2">Ready to Find Your Dream Home?</h3>
        <p className="text-white/90 text-sm mb-4">
          Connect with us today and explore our premium projects
        </p>
        <div className="space-y-2">
          <Button
            className="w-full bg-white hover:bg-gray-100"
            style={{ color: '#FF8C00' }}
            asChild
          >
            <Link to={createPageUrl('Contact')}>Contact Us</Link>
          </Button>
          <Button
            variant="outline"
            className="w-full border-2 border-white bg-transparent hover:bg-white/10 text-white"
            asChild
          >
            <a href="tel:+917338628777">
              <Phone className="w-4 h-4 mr-2" />
              Call Now
            </a>
          </Button>
        </div>
      </motion.div>

      {/* Related Blogs */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
        className="bg-white rounded-xl p-6 border border-gray-200"
      >
        <h3 className="font-semibold text-gray-800 mb-4">Related Articles</h3>
        <div className="space-y-4">
          {relatedBlogs.map((blog) => (
            <Link
              key={blog.id}
              to={`${createPageUrl('BlogPost')}?id=${blog.id}`}
              className="flex gap-3 group"
            >
              <img
                src={blog.image}
                alt={blog.title}
                className="w-20 h-20 rounded-lg object-cover"
              />
              <div className="flex-1">
                <h4 className="text-sm font-medium text-gray-800 group-hover:text-orange-500 transition-colors line-clamp-2">
                  {blog.title}
                </h4>
                <p className="text-xs text-gray-500 mt-1">{blog.date}</p>
              </div>
            </Link>
          ))}
        </div>
      </motion.div>
    </div>
  );
}