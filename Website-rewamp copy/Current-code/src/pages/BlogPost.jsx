import React from 'react';
import { motion } from 'framer-motion';
import { Calendar, Clock, User, ArrowLeft } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { useEffect } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import Navbar from '@/components/Navbar';
import WhatsAppButton from '@/components/WhatsAppButton';
import Footer from '@/components/home/Footer';
import BlogSidebar from '@/components/blog/BlogSidebar';
import BlogCtaSection from '@/components/blog/BlogCtaSection';
import blogPosts from '@/data/blogPosts';


export default function BlogPost() {
  const urlParams = new URLSearchParams(window.location.search);
  const postId = urlParams.get('id');

  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  const post = blogPosts.find(p => p.id === postId);

  if (!post) {
    return (
      <div className="bg-white min-h-screen pt-24">
        <Navbar />
        <div className="flex flex-col items-center justify-center py-20">
          <p className="text-gray-600 mb-4">Blog post not found</p>
          <Link to={createPageUrl('Blog')}>
            <Button>Back to Blog</Button>
          </Link>
        </div>
        <Footer />
      </div>
    );
  }

  return (
    <div className="bg-white min-h-screen pt-24">
      <Navbar />
      <WhatsAppButton />

      <article className="py-12 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          <div className="grid lg:grid-cols-12 gap-8 lg:gap-12">
            <div className="lg:col-span-8">
              <div className="max-w-[750px] mx-auto">
                <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-10">
                  <Link to={createPageUrl('Blog')}>
                    <Button variant="ghost" size="sm">
                      <ArrowLeft className="w-4 h-4 mr-2" />
                      Back to Blog
                    </Button>
                  </Link>
                </div>

                <motion.div
                  initial={{ opacity: 0, y: 30 }}
                  animate={{ opacity: 1, y: 0 }}
                >
                  <div className="mb-6">
                    <span className="bg-orange-500 text-white px-4 py-1.5 rounded-full text-xs sm:text-sm font-semibold">
                      {post.category}
                    </span>
                  </div>

                  <h1 className="text-3xl sm:text-4xl md:text-5xl font-semibold text-gray-900 mb-8 leading-[1.2]">
                    {post.h1Tag || post.title}
                  </h1>

                  <div className="flex flex-wrap items-center gap-4 sm:gap-6 text-sm text-gray-600 mb-10 pb-8 border-b border-gray-200">
                    <div className="flex items-center gap-2">
                      <User className="w-4 h-4" />
                      <span>{post.author}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Calendar className="w-4 h-4" />
                      <span>{post.date}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Clock className="w-4 h-4" />
                      <span>{post.readTime}</span>
                    </div>
                  </div>

                  <img
                    src={post.image || 'https://www.ishtikahomes.com/assets/img/all-images/blog-img1.png'}
                    alt={post.featuredImageAlt || post.title}
                    className="w-full rounded-2xl mb-12 shadow-md"
                  />

                  {post.content && (() => {
                    const content = post.content;
                    const paragraphs = content.split('</p>').filter(p => p.trim());
                    const totalParas = paragraphs.length;

                    if (totalParas < 6) {
                      return (
                        <div
                          className="article-content"
                          style={{ fontSize: '18px', lineHeight: '1.8', color: '#374151' }}
                          dangerouslySetInnerHTML={{ __html: content }}
                        />
                      );
                    }

                    const parts = [];
                    const firstCtaAfter = Math.floor(totalParas * 0.25);
                    const secondCtaAfter = Math.floor(totalParas * 0.75);

                    paragraphs.forEach((para, index) => {
                      parts.push(
                        <div
                          key={`para-${index}`}
                          className="article-content"
                          style={{ fontSize: '18px', lineHeight: '1.8', color: '#374151' }}
                          dangerouslySetInnerHTML={{ __html: para + '</p>' }}
                        />
                      );

                      if (index === firstCtaAfter) {
                        parts.push(
                          <BlogCtaSection
                            key={`cta-1-${postId}`}
                            textKeyPrefix={`blog-cta-1-${postId}`}
                            defaultTitle="Ready to Own Your Dream Home?"
                            defaultDescription="Connect with our experts to find the perfect property that matches your aspirations."
                            defaultButtonText="Enquire Now"
                            defaultButtonLink="Contact"
                            defaultImage="https://www.ishtikahomes.com/assets/img/all-images/about-img1.png"
                          />
                        );
                      }

                      if (index === secondCtaAfter) {
                        parts.push(
                          <BlogCtaSection
                            key={`cta-2-${postId}`}
                            textKeyPrefix={`blog-cta-2-${postId}`}
                            defaultTitle="Ready to Own Your Dream Home?"
                            defaultDescription="Connect with our experts to find the perfect property that matches your aspirations."
                            defaultButtonText="Enquire Now"
                            defaultButtonLink="Contact"
                            defaultImage="https://www.ishtikahomes.com/assets/img/all-images/about-img1.png"
                          />
                        );
                      }
                    });

                    return <>{parts}</>;
                  })()}

                  <style>{`
                  .article-content {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                  }
                  .article-content h1, .article-content h2, .article-content h3,
                  .article-content h4, .article-content h5, .article-content h6 {
                    color: #111827 !important; font-weight: 700 !important;
                    line-height: 1.3 !important; margin-top: 1.5rem !important; margin-bottom: 0.75rem !important;
                  }
                  .article-content h2 { font-size: 1.875rem !important; margin-top: 2rem !important; padding-bottom: 0.5rem !important; border-bottom: 2px solid #e5e7eb !important; }
                  .article-content h3 { font-size: 1.5rem !important; }
                  .article-content p { margin-bottom: 1.25rem !important; line-height: 1.8 !important; font-size: 18px !important; color: #374151 !important; }
                  .article-content strong, .article-content b { font-weight: 700 !important; color: #111827 !important; }
                  .article-content a { color: #f97316 !important; text-decoration: none !important; font-weight: 500 !important; }
                  .article-content a:hover { color: #ea580c !important; text-decoration: underline !important; }
                  .article-content ul, .article-content ol { margin: 1rem 0 1.25rem !important; padding-left: 2rem !important; display: block !important; }
                  .article-content ul { list-style-type: disc !important; }
                  .article-content ol { list-style-type: decimal !important; }
                  .article-content ul li, .article-content ol li { margin-bottom: 0.625rem !important; line-height: 1.8 !important; font-size: 18px !important; color: #374151 !important; display: list-item !important; }
                  .article-content ul li::marker { color: #f97316 !important; }
                  .article-content ol li::marker { color: #f97316 !important; font-weight: 700 !important; }
                  .article-content img { width: 100% !important; height: auto !important; border-radius: 1rem !important; margin: 1.5rem 0 !important; }
                  .article-content blockquote { border-left: 4px solid #f97316 !important; background: rgba(249,115,22,0.05) !important; padding: 1.25rem 1.5rem !important; margin: 1.5rem 0 !important; font-style: italic !important; }
                  @media (max-width: 640px) {
                    .article-content h2 { font-size: 1.5rem !important; }
                    .article-content h3 { font-size: 1.25rem !important; }
                    .article-content p, .article-content li { font-size: 17px !important; }
                  }
                  `}</style>

                </motion.div>
              </div>
            </div>

            <div className="lg:col-span-4">
              <div className="lg:sticky lg:top-24">
                <BlogSidebar currentPostId={postId} />
              </div>
            </div>
          </div>
        </div>
      </article>

      <Footer />
    </div>
  );
}
