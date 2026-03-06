'use client';

import { useEffect, useState, useRef } from 'react';

export default function Home() {
  const [isMuted, setIsMuted] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);

  const toggleMute = () => {
    if (videoRef.current) {
      videoRef.current.muted = !videoRef.current.muted;
      setIsMuted(videoRef.current.muted);
    }
  };

  // Load external resources
  useEffect(() => {
    // Load Google Fonts
    const fontLink = document.createElement('link');
    fontLink.href = 'https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;500;600;700;800&family=Inter:wght@300;400;500;600;700;800&family=Montserrat:wght@300;400;500;600;700;800&display=swap';
    fontLink.rel = 'stylesheet';
    document.head.appendChild(fontLink);

    // Load Font Awesome
    const fontAwesome = document.createElement('link');
    fontAwesome.href = 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css';
    fontAwesome.rel = 'stylesheet';
    document.head.appendChild(fontAwesome);

    // Load AOS CSS
    const aosCSS = document.createElement('link');
    aosCSS.href = 'https://unpkg.com/aos@2.3.1/dist/aos.css';
    aosCSS.rel = 'stylesheet';
    document.head.appendChild(aosCSS);

    // Load Swiper CSS
    const swiperCSS = document.createElement('link');
    swiperCSS.href = 'https://cdn.jsdelivr.net/npm/swiper@11/swiper-bundle.min.css';
    swiperCSS.rel = 'stylesheet';
    document.head.appendChild(swiperCSS);
  }, []);

  // Video initialization effect
  useEffect(() => {
    const heroVideo = videoRef.current;

    if (heroVideo) {
      // Disable text tracks (subtitles/captions) if any
      if (heroVideo.textTracks) {
        for (let i = 0; i < heroVideo.textTracks.length; i++) {
          heroVideo.textTracks[i].mode = 'hidden';
        }
      }

      // Try to play with sound on page load
      heroVideo.muted = false;
      heroVideo.volume = 1.0;

      // Attempt to play with sound
      const playPromise = heroVideo.play();
      if (playPromise !== undefined) {
        playPromise.catch(() => {
          // Autoplay with sound was prevented, try after user interaction
          console.log('Autoplay with sound prevented, waiting for user interaction');

          // Play with sound on first user interaction
          const enableAudio = () => {
            if (heroVideo) {
              heroVideo.muted = false;
              heroVideo.volume = 1.0;
              heroVideo.play();
            }
          };
          document.addEventListener('click', enableAudio, { once: true });
        });
      }
    }
  }, []);

  useEffect(() => {
    // Dynamically load AOS
    const loadAOS = async () => {
      const AOS = (await import('aos')).default;
      await import('aos/dist/aos.css');

      AOS.init({
        duration: 800,
        once: true,
        offset: 200,
        disable: false,
        startEvent: 'DOMContentLoaded',
        initClassName: 'aos-init',
        animatedClassName: 'aos-animate',
        mirror: false
      });
    };

    // Dynamically load Swiper
    const loadSwiper = async () => {
      const Swiper = (await import('swiper')).default;
      const { Autoplay, Pagination } = await import('swiper/modules');
      await import('swiper/css');
      await import('swiper/css/pagination');

      new Swiper('.testimonialSwiper', {
        modules: [Autoplay, Pagination],
        slidesPerView: 1,
        spaceBetween: 30,
        loop: true,
        autoplay: {
          delay: 5000,
          disableOnInteraction: false,
        },
        pagination: {
          el: '.swiper-pagination',
          clickable: true,
        },
        breakpoints: {
          768: {
            slidesPerView: 2,
          },
          1024: {
            slidesPerView: 3,
          }
        }
      });
    };

    loadAOS();
    loadSwiper();

    // Sidebar Menu
    const menuBtn = document.getElementById('menuBtn');
    const sidebarMenu = document.getElementById('sidebarMenu');
    const closeMenuBtn = document.getElementById('closeMenuBtn');
    const menuOverlay = document.getElementById('menuOverlay');
    const sidebarMenuLinks = document.querySelectorAll('.sidebar-menu-link');

    // Open menu
    if (menuBtn) {
      menuBtn.addEventListener('click', () => {
        if (sidebarMenu) sidebarMenu.classList.add('active');
        if (menuOverlay) menuOverlay.classList.add('active');
        document.body.style.overflow = 'hidden';
      });
    }

    // Close menu
    if (closeMenuBtn) {
      closeMenuBtn.addEventListener('click', () => {
        if (sidebarMenu) sidebarMenu.classList.remove('active');
        if (menuOverlay) menuOverlay.classList.remove('active');
        document.body.style.overflow = '';
      });
    }

    // Close menu when clicking overlay
    if (menuOverlay) {
      menuOverlay.addEventListener('click', () => {
        if (sidebarMenu) sidebarMenu.classList.remove('active');
        if (menuOverlay) menuOverlay.classList.remove('active');
        document.body.style.overflow = '';
      });
    }

    // Close menu when clicking a link
    sidebarMenuLinks.forEach(link => {
      link.addEventListener('click', () => {
        if (sidebarMenu) sidebarMenu.classList.remove('active');
        if (menuOverlay) menuOverlay.classList.remove('active');
        document.body.style.overflow = '';
      });
    });

    // Header Scroll Effect
    window.addEventListener('scroll', () => {
      const header = document.getElementById('header');
      if (window.scrollY > 50) {
        header?.classList.add('bg-white', 'shadow-lg');
      } else {
        header?.classList.remove('shadow-lg');
      }
    });

    // Counter Animation
    const counters = document.querySelectorAll('.counter');
    const speed = 200;

    const observerOptions = {
      threshold: 0.5
    };

    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          const counter = entry.target as HTMLElement;

          // Skip animation for static counters
          if (counter.classList.contains('static-counter')) {
            observer.unobserve(counter);
            return;
          }

          const target = +(counter.getAttribute('data-target') || 0);
          const updateCount = () => {
            const count = +counter.innerText;
            const increment = target / speed;

            if (count < target) {
              counter.innerText = Math.ceil(count + increment).toString();
              setTimeout(updateCount, 10);
            } else {
              counter.innerText = target.toString();
            }
          };
          updateCount();
          observer.unobserve(counter);
        }
      });
    }, observerOptions);

    counters.forEach(counter => observer.observe(counter));

    // Project Filter
    const filterButtons = document.querySelectorAll('.project-filter');
    const projectCards = document.querySelectorAll('.project-card');

    filterButtons.forEach(button => {
      button.addEventListener('click', () => {
        const filter = button.getAttribute('data-filter');

        // Update active button
        filterButtons.forEach(btn => {
          btn.classList.remove('bg-gold-500', 'text-white');
          btn.classList.add('bg-gray-200', 'text-gray-700');
        });
        button.classList.remove('bg-gray-200', 'text-gray-700');
        button.classList.add('bg-gold-500', 'text-white');

        // Filter projects
        projectCards.forEach(card => {
          const cardElement = card as HTMLElement;
          if (filter === 'all' || card.getAttribute('data-status') === filter) {
            cardElement.style.display = 'block';
            card.classList.add('animate__animated', 'animate__fadeIn');
          } else {
            cardElement.style.display = 'none';
          }
        });
      });
    });

    // Contact Form Submission
    const contactForm = document.getElementById('contactForm');
    if (contactForm) {
      contactForm.addEventListener('submit', function(e) {
        e.preventDefault();

        // Show success message
        alert('Thank you for your interest! Our team will contact you within 24 hours.');

        // Reset form
        (e.target as HTMLFormElement).reset();

        // In production, send data to server
        // fetch('/api/contact', { method: 'POST', body: new FormData(this) })
      });
    }

    // Smooth scroll for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
      anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const href = (e.currentTarget as HTMLAnchorElement).getAttribute('href');
        if (href) {
          const target = document.querySelector(href);
          if (target) {
            const offsetTop = (target as HTMLElement).offsetTop - 80;
            window.scrollTo({
              top: offsetTop,
              behavior: 'smooth'
            });
          }
        }
      });
    });
  }, []);

  return (
    <>
      {/* Header/Navigation */}
      <header className="fixed w-full top-0 z-50 transition-all duration-300" id="header" style={{ background: 'rgba(255, 255, 255, 0.95)', boxShadow: '0 2px 10px rgba(0,0,0,0.05)' }}>
        <div className="container mx-auto px-4 py-6 md:py-8">
          <div className="flex items-center justify-between">
            {/* Menu Button (Left - All Screens with spacing) */}
            <button className="order-1 flex items-center gap-3 hover:opacity-70 transition group py-2" id="menuBtn" style={{ marginLeft: '80px' }}>
              <span className="menu-text font-light text-lg md:text-xl uppercase" style={{ fontFamily: "'Montserrat', sans-serif", letterSpacing: '0.15em', color: '#333333' }}>MENU</span>
              <div className="flex flex-col gap-[5px] justify-center items-end">
                <span className="menu-line block w-[24px] h-[2.5px] transition-all" style={{ background: '#333333' }}></span>
                <span className="menu-line block w-[36px] h-[2.5px] transition-all" style={{ background: '#333333' }}></span>
                <span className="menu-line block w-[24px] h-[2.5px] transition-all" style={{ background: '#333333' }}></span>
              </div>
            </button>

            {/* Logo (Center) */}
            <a href="#home" className="flex items-center order-2 absolute left-1/2 transform -translate-x-1/2 lg:static lg:transform-none">
              <img src="images/ishtika-logo.png" alt="Ishtika Homes" className="h-14 md:h-16 lg:h-20 w-auto" style={{ background: 'transparent' }} />
            </a>

            {/* CTA Button (Right) */}
            <div className="hidden lg:flex items-center space-x-4 order-3">
              <a href="tel:+919876543210" className="flex items-center hover:opacity-70 transition">
                <i className="fas fa-phone mr-2" style={{ color: '#333333', fontSize: '18px' }}></i>
                <span className="font-light text-base md:text-lg" style={{ fontFamily: "'Montserrat', sans-serif", color: '#333333', letterSpacing: '0.05em' }}>+91 98765 43210</span>
              </a>
            </div>

            {/* Mobile Spacer (Right) */}
            <div className="lg:hidden order-3 w-10"></div>
          </div>
        </div>
      </header>

      {/* Sidebar Menu - Modern Design */}
      <div className="sidebar-menu shadow-2xl" id="sidebarMenu" style={{ background: 'linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%)' }}>
        <div className="p-10 h-full overflow-y-auto" style={{ display: 'flex', flexDirection: 'column' }}>
          {/* Header */}
          <div className="flex items-center justify-between mb-12">
            <h3 style={{
              fontFamily: "'Montserrat', sans-serif",
              fontSize: '28px',
              fontWeight: '700',
              color: '#1a1a1a',
              letterSpacing: '0.1em',
              textTransform: 'uppercase'
            }}>Menu</h3>
            <button
              id="closeMenuBtn"
              style={{
                width: '44px',
                height: '44px',
                borderRadius: '50%',
                background: '#f0f0f0',
                border: 'none',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                cursor: 'pointer',
                transition: 'all 0.3s ease',
                color: '#333'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = '#333';
                e.currentTarget.style.color = '#fff';
                e.currentTarget.style.transform = 'rotate(90deg)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = '#f0f0f0';
                e.currentTarget.style.color = '#333';
                e.currentTarget.style.transform = 'rotate(0deg)';
              }}
            >
              <i className="fas fa-times" style={{ fontSize: '20px' }}></i>
            </button>
          </div>

          {/* Navigation Links */}
          <nav style={{ display: 'flex', flexDirection: 'column', gap: '8px', flex: 1 }}>
            <a
              href="#about"
              className="sidebar-menu-link"
              style={{
                fontFamily: "'Montserrat', sans-serif",
                color: '#1a1a1a',
                fontSize: '18px',
                fontWeight: '500',
                padding: '18px 24px',
                borderRadius: '12px',
                transition: 'all 0.3s ease',
                display: 'flex',
                alignItems: 'center',
                gap: '16px',
                background: 'transparent',
                border: '1px solid transparent',
                letterSpacing: '0.1em',
                textTransform: 'uppercase'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'linear-gradient(135deg, #333333 0%, #1a1a1a 100%)';
                e.currentTarget.style.color = '#ffffff';
                e.currentTarget.style.paddingLeft = '32px';
                e.currentTarget.style.borderColor = '#333';
                e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'transparent';
                e.currentTarget.style.color = '#1a1a1a';
                e.currentTarget.style.paddingLeft = '24px';
                e.currentTarget.style.borderColor = 'transparent';
                e.currentTarget.style.boxShadow = 'none';
              }}
            >
              <i className="fas fa-info-circle" style={{ fontSize: '20px', width: '24px' }}></i>
              <span>About Us</span>
            </a>

            <a
              href="#projects"
              className="sidebar-menu-link"
              style={{
                fontFamily: "'Montserrat', sans-serif",
                color: '#1a1a1a',
                fontSize: '18px',
                fontWeight: '500',
                padding: '18px 24px',
                borderRadius: '12px',
                transition: 'all 0.3s ease',
                display: 'flex',
                alignItems: 'center',
                gap: '16px',
                background: 'transparent',
                border: '1px solid transparent',
                letterSpacing: '0.1em',
                textTransform: 'uppercase'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'linear-gradient(135deg, #333333 0%, #1a1a1a 100%)';
                e.currentTarget.style.color = '#ffffff';
                e.currentTarget.style.paddingLeft = '32px';
                e.currentTarget.style.borderColor = '#333';
                e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'transparent';
                e.currentTarget.style.color = '#1a1a1a';
                e.currentTarget.style.paddingLeft = '24px';
                e.currentTarget.style.borderColor = 'transparent';
                e.currentTarget.style.boxShadow = 'none';
              }}
            >
              <i className="fas fa-building" style={{ fontSize: '20px', width: '24px' }}></i>
              <span>Our Portfolio</span>
            </a>

            <a
              href="#testimonials"
              className="sidebar-menu-link"
              style={{
                fontFamily: "'Montserrat', sans-serif",
                color: '#1a1a1a',
                fontSize: '18px',
                fontWeight: '500',
                padding: '18px 24px',
                borderRadius: '12px',
                transition: 'all 0.3s ease',
                display: 'flex',
                alignItems: 'center',
                gap: '16px',
                background: 'transparent',
                border: '1px solid transparent',
                letterSpacing: '0.1em',
                textTransform: 'uppercase'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'linear-gradient(135deg, #333333 0%, #1a1a1a 100%)';
                e.currentTarget.style.color = '#ffffff';
                e.currentTarget.style.paddingLeft = '32px';
                e.currentTarget.style.borderColor = '#333';
                e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'transparent';
                e.currentTarget.style.color = '#1a1a1a';
                e.currentTarget.style.paddingLeft = '24px';
                e.currentTarget.style.borderColor = 'transparent';
                e.currentTarget.style.boxShadow = 'none';
              }}
            >
              <i className="fas fa-star" style={{ fontSize: '20px', width: '24px' }}></i>
              <span>Testimonials</span>
            </a>

            <a
              href="#contact"
              className="sidebar-menu-link"
              style={{
                fontFamily: "'Montserrat', sans-serif",
                color: '#1a1a1a',
                fontSize: '18px',
                fontWeight: '500',
                padding: '18px 24px',
                borderRadius: '12px',
                transition: 'all 0.3s ease',
                display: 'flex',
                alignItems: 'center',
                gap: '16px',
                background: 'transparent',
                border: '1px solid transparent',
                letterSpacing: '0.1em',
                textTransform: 'uppercase'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'linear-gradient(135deg, #333333 0%, #1a1a1a 100%)';
                e.currentTarget.style.color = '#ffffff';
                e.currentTarget.style.paddingLeft = '32px';
                e.currentTarget.style.borderColor = '#333';
                e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'transparent';
                e.currentTarget.style.color = '#1a1a1a';
                e.currentTarget.style.paddingLeft = '24px';
                e.currentTarget.style.borderColor = 'transparent';
                e.currentTarget.style.boxShadow = 'none';
              }}
            >
              <i className="fas fa-envelope" style={{ fontSize: '20px', width: '24px' }}></i>
              <span>Contact Us</span>
            </a>
          </nav>

          {/* Divider */}
          <div style={{
            borderTop: '2px solid #e5e7eb',
            margin: '32px 0',
            background: 'linear-gradient(90deg, transparent, #e5e7eb, transparent)'
          }}></div>

          {/* Phone Number - CTA Style */}
          <a
            href="tel:+919876543210"
            className="sidebar-menu-link"
            style={{
              fontFamily: "'Montserrat', sans-serif",
              color: '#ffffff',
              fontSize: '18px',
              fontWeight: '600',
              padding: '20px 24px',
              borderRadius: '12px',
              transition: 'all 0.3s ease',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '12px',
              background: 'linear-gradient(135deg, #333333 0%, #1a1a1a 100%)',
              border: '2px solid #333',
              boxShadow: '0 4px 16px rgba(0,0,0,0.2)',
              letterSpacing: '0.05em'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = 'linear-gradient(135deg, #1a1a1a 0%, #000000 100%)';
              e.currentTarget.style.transform = 'translateY(-2px)';
              e.currentTarget.style.boxShadow = '0 6px 20px rgba(0,0,0,0.3)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = 'linear-gradient(135deg, #333333 0%, #1a1a1a 100%)';
              e.currentTarget.style.transform = 'translateY(0)';
              e.currentTarget.style.boxShadow = '0 4px 16px rgba(0,0,0,0.2)';
            }}
          >
            <i className="fas fa-phone" style={{ fontSize: '18px' }}></i>
            <span>+91 98765 43210</span>
          </a>
        </div>
      </div>

      {/* Menu Overlay */}
      <div className="menu-overlay" id="menuOverlay"></div>

      {/* Hero Section */}
      <section id="home" className="relative h-screen flex items-center justify-center overflow-hidden">
        {/* Background Video */}
        <div className="absolute inset-0">
          <video ref={videoRef} id="heroVideo" autoPlay loop playsInline className="w-full h-full object-cover">
            <source src="images/hero-video.mp4" type="video/mp4" />
            Your browser does not support the video tag.
          </video>
          <div className="hero-overlay absolute inset-0"></div>
        </div>

        {/* Mute/Unmute Button */}
        <button onClick={toggleMute} className="absolute top-24 right-6 md:top-28 md:right-10 bg-white/20 backdrop-blur-sm hover:bg-white/30 text-white rounded-full p-3 md:p-4 transition z-20 shadow-lg">
          <i className={`fas ${isMuted ? 'fa-volume-mute' : 'fa-volume-up'} text-xl md:text-2xl`}></i>
        </button>

        {/* Scroll Indicator */}
        <div className="absolute bottom-10 left-1/2 transform -translate-x-1/2 text-white animate-bounce z-10">
          <i className="fas fa-chevron-down text-2xl"></i>
        </div>
      </section>

      {/* Stats Section */}
      <section className="py-6 md:py-8 bg-white">
        <div className="container mx-auto px-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6 md:gap-8 text-center">
            <div data-aos="fade-left" data-aos-delay="0">
              <div className="counter" style={{ color: '#f59e0b' }} data-target="12">0</div>
              <p className="text-base md:text-lg font-medium mt-1 text-gray-700">Projects Delivered</p>
            </div>
            <div data-aos="fade-left" data-aos-delay="100">
              <div className="counter" style={{ color: '#f59e0b' }} data-target="5">0</div>
              <p className="text-base md:text-lg font-medium mt-1 text-gray-700">Ongoing Projects</p>
            </div>
            <div data-aos="fade-left" data-aos-delay="200">
              <div className="counter static-counter" style={{ color: '#f59e0b' }} data-value="2000+">2000+</div>
              <p className="text-base md:text-lg font-medium mt-1 text-gray-700">Happy Families</p>
            </div>
            <div data-aos="fade-left" data-aos-delay="300">
              <div className="counter" style={{ color: '#f59e0b' }} data-target="12">0</div>
              <p className="text-base md:text-lg font-medium mt-1 text-gray-700">Years of Experience</p>
            </div>
          </div>
        </div>
      </section>

      {/* About Section */}
      <section id="about" className="py-20 bg-white">
        <div className="container mx-auto px-4">
          <div className="grid md:grid-cols-2 gap-12 items-center">
            <div data-aos="fade-right">
              <img src="https://images.unsplash.com/photo-1600607687920-4e2a09cf159d?w=800&q=80" alt="About Ishtika Homes" className="rounded-2xl shadow-2xl" />
            </div>
            <div data-aos="fade-left">
              <span className="text-gold-600 font-semibold tracking-widest uppercase text-sm">About Ishtika Homes</span>
              <h2 className="text-4xl md:text-5xl font-bold text-gray-900 mt-4 mb-6">Building Excellence for Over 15 Years</h2>
              <p className="text-gray-600 text-lg leading-relaxed mb-6">
                Ishtika Homes is a leading real estate developer committed to creating exceptional living spaces that blend luxury, comfort, and sustainability. With over 15 years of excellence in the industry, we have delivered landmark residential projects that have redefined modern living.
              </p>
              <p className="text-gray-600 text-lg leading-relaxed mb-8">
                Our commitment to quality, timely delivery, and customer satisfaction has made us one of the most trusted names in Bangalore's real estate landscape.
              </p>

              <div className="space-y-4">
                <div className="flex items-start">
                  <div className="w-12 h-12 bg-gold-100 rounded-full flex items-center justify-center mr-4 flex-shrink-0">
                    <i className="fas fa-check text-gold-600 text-xl"></i>
                  </div>
                  <div>
                    <h4 className="font-bold text-lg text-gray-900">Premium Quality</h4>
                    <p className="text-gray-600">World-class construction standards and premium materials</p>
                  </div>
                </div>
                <div className="flex items-start">
                  <div className="w-12 h-12 bg-gold-100 rounded-full flex items-center justify-center mr-4 flex-shrink-0">
                    <i className="fas fa-check text-gold-600 text-xl"></i>
                  </div>
                  <div>
                    <h4 className="font-bold text-lg text-gray-900">Timely Delivery</h4>
                    <p className="text-gray-600">Committed to delivering projects on schedule</p>
                  </div>
                </div>
                <div className="flex items-start">
                  <div className="w-12 h-12 bg-gold-100 rounded-full flex items-center justify-center mr-4 flex-shrink-0">
                    <i className="fas fa-check text-gold-600 text-xl"></i>
                  </div>
                  <div>
                    <h4 className="font-bold text-lg text-gray-900">Customer Satisfaction</h4>
                    <p className="text-gray-600">Dedicated to exceeding customer expectations</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Projects Section */}
      <section id="projects" className="py-20 bg-gray-50">
        <div className="container mx-auto px-4">
          <div className="text-center mb-12" data-aos="fade-up">
            <span className="text-gold-600 font-semibold tracking-widest uppercase text-sm">Our Portfolio</span>
            <h2 className="text-4xl md:text-5xl font-bold text-gray-900 mt-4 mb-4">Featured Projects</h2>
            <p className="text-gray-600 text-lg max-w-2xl mx-auto">
              Discover our range of premium residential projects designed for modern living
            </p>
          </div>

          {/* Project Filter Buttons */}
          <div className="flex justify-center space-x-4 mb-12" data-aos="fade-up">
            <button className="px-6 py-2 bg-gold-500 text-white font-semibold rounded-lg project-filter" data-filter="all">All Projects</button>
            <button className="px-6 py-2 bg-gray-200 text-gray-700 font-semibold rounded-lg hover:bg-gray-300 project-filter" data-filter="ongoing">Ongoing</button>
            <button className="px-6 py-2 bg-gray-200 text-gray-700 font-semibold rounded-lg hover:bg-gray-300 project-filter" data-filter="upcoming">Upcoming</button>
            <button className="px-6 py-2 bg-gray-200 text-gray-700 font-semibold rounded-lg hover:bg-gray-300 project-filter" data-filter="completed">Completed</button>
          </div>

          {/* Projects Grid */}
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
            {/* Project 1: Ishtika Anahata */}
            <div className="project-card bg-white rounded-2xl overflow-hidden shadow-lg" data-status="ongoing" data-aos="fade-up">
              <div className="relative overflow-hidden h-64">
                <img src="https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=600&q=80" alt="Ishtika Anahata" className="w-full h-full object-cover" />
                <div className="absolute top-4 left-4">
                  <span className="badge badge-ongoing">Ongoing</span>
                </div>
                <div className="absolute top-4 right-4">
                  <span className="badge bg-gold-500 text-white">Featured</span>
                </div>
              </div>
              <div className="p-6">
                <h3 className="text-2xl font-bold text-gray-900 mb-2">Ishtika Anahata</h3>
                <p className="text-gray-600 mb-4">
                  <i className="fas fa-map-marker-alt text-gold-500 mr-2"></i>Whitefield, Bangalore
                </p>
                <p className="text-gray-700 mb-4">
                  Luxury 2, 3 & 4 BHK apartments with world-class amenities and smart home features
                </p>
                <div className="flex flex-wrap gap-2 mb-4">
                  <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm">2 BHK</span>
                  <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm">3 BHK</span>
                  <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm">4 BHK</span>
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-500">Starting from</p>
                    <p className="text-2xl font-bold text-gold-600">₹1.2 Cr*</p>
                  </div>
                  <a href="#contact" className="px-6 py-2 bg-primary-600 text-white font-semibold rounded-lg hover:bg-primary-700 transition">
                    View Details
                  </a>
                </div>
              </div>
            </div>

            {/* Project 2: Ishtika Tranquil Heights */}
            <div className="project-card bg-white rounded-2xl overflow-hidden shadow-lg" data-status="upcoming" data-aos="fade-up" data-aos-delay="100">
              <div className="relative overflow-hidden h-64">
                <img src="https://images.unsplash.com/photo-1600607687920-4e2a09cf159d?w=600&q=80" alt="Ishtika Tranquil Heights" className="w-full h-full object-cover" />
                <div className="absolute top-4 left-4">
                  <span className="badge badge-upcoming">Upcoming</span>
                </div>
              </div>
              <div className="p-6">
                <h3 className="text-2xl font-bold text-gray-900 mb-2">Ishtika Tranquil Heights</h3>
                <p className="text-gray-600 mb-4">
                  <i className="fas fa-map-marker-alt text-gold-500 mr-2"></i>Electronic City, Bangalore
                </p>
                <p className="text-gray-700 mb-4">
                  Modern 2 & 3 BHK apartments with excellent connectivity to IT parks and metro
                </p>
                <div className="flex flex-wrap gap-2 mb-4">
                  <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm">2 BHK</span>
                  <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm">3 BHK</span>
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-500">Starting from</p>
                    <p className="text-2xl font-bold text-gold-600">₹85 L*</p>
                  </div>
                  <a href="#contact" className="px-6 py-2 bg-primary-600 text-white font-semibold rounded-lg hover:bg-primary-700 transition">
                    View Details
                  </a>
                </div>
              </div>
            </div>

            {/* Project 3: Ishtika Green Valley */}
            <div className="project-card bg-white rounded-2xl overflow-hidden shadow-lg" data-status="completed" data-aos="fade-up" data-aos-delay="200">
              <div className="relative overflow-hidden h-64">
                <img src="https://images.unsplash.com/photo-1600607687939-ce8a6c25118c?w=600&q=80" alt="Ishtika Green Valley" className="w-full h-full object-cover" />
                <div className="absolute top-4 left-4">
                  <span className="badge badge-completed">Completed</span>
                </div>
              </div>
              <div className="p-6">
                <h3 className="text-2xl font-bold text-gray-900 mb-2">Ishtika Green Valley</h3>
                <p className="text-gray-600 mb-4">
                  <i className="fas fa-map-marker-alt text-gold-500 mr-2"></i>Sarjapur Road, Bangalore
                </p>
                <p className="text-gray-700 mb-4">
                  Eco-friendly living with 200 happy families already moved in
                </p>
                <div className="flex flex-wrap gap-2 mb-4">
                  <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm">2 BHK</span>
                  <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm">3 BHK</span>
                  <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full text-sm">4 BHK</span>
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-500">Handed Over</p>
                    <p className="text-xl font-bold text-green-600">December 2022</p>
                  </div>
                  <a href="#contact" className="px-6 py-2 bg-gray-600 text-white font-semibold rounded-lg hover:bg-gray-700 transition">
                    View Details
                  </a>
                </div>
              </div>
            </div>
          </div>

          <div className="text-center mt-12" data-aos="fade-up">
            <a href="#contact" className="inline-block px-8 py-4 bg-gradient-to-r from-gold-500 to-gold-600 text-white font-bold rounded-lg hover:from-gold-600 hover:to-gold-700 transition transform hover:-translate-y-1 shadow-lg">
              View All Projects
            </a>
          </div>
        </div>
      </section>

      {/* Testimonials Section */}
      <section id="testimonials" className="py-20 bg-gray-50">
        <div className="container mx-auto px-4">
          <div className="text-center mb-12" data-aos="fade-up">
            <span className="text-gold-600 font-semibold tracking-widest uppercase text-sm">What Our Customers Say</span>
            <h2 className="text-4xl md:text-5xl font-bold text-gray-900 mt-4 mb-4">Happy Homeowners</h2>
            <p className="text-gray-600 text-lg max-w-2xl mx-auto">
              Real stories from families who chose to make Ishtika Homes their home
            </p>
          </div>

          {/* Testimonials Slider */}
          <div className="swiper testimonialSwiper max-w-5xl mx-auto" data-aos="fade-up">
            <div className="swiper-wrapper">
              {/* Testimonial 1 */}
              <div className="swiper-slide">
                <div className="testimonial-card">
                  <div className="flex items-center mb-4">
                    <img src="https://i.pravatar.cc/80?img=12" alt="Rajesh Kumar" className="w-16 h-16 rounded-full mr-4" />
                    <div>
                      <h4 className="font-bold text-lg text-gray-900">Rajesh Kumar</h4>
                      <p className="text-gray-600">Software Engineer</p>
                      <div className="flex text-gold-500 mt-1">
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                      </div>
                    </div>
                  </div>
                  <p className="text-gray-700 italic leading-relaxed">
                    "Ishtika Anahata has exceeded all our expectations. The quality of construction, attention to detail, and the amenities are world-class. The team was professional and transparent throughout the buying process. Highly recommended!"
                  </p>
                  <p className="text-sm text-gray-500 mt-4">Ishtika Anahata Resident</p>
                </div>
              </div>

              {/* Testimonial 2 */}
              <div className="swiper-slide">
                <div className="testimonial-card">
                  <div className="flex items-center mb-4">
                    <img src="https://i.pravatar.cc/80?img=5" alt="Priya Sharma" className="w-16 h-16 rounded-full mr-4" />
                    <div>
                      <h4 className="font-bold text-lg text-gray-900">Priya Sharma</h4>
                      <p className="text-gray-600">Marketing Manager</p>
                      <div className="flex text-gold-500 mt-1">
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                      </div>
                    </div>
                  </div>
                  <p className="text-gray-700 italic leading-relaxed">
                    "We moved into our dream home at Ishtika Green Valley last year. The location is perfect, the community is wonderful, and the maintenance is excellent. Thank you Ishtika Homes for making our dream come true!"
                  </p>
                  <p className="text-sm text-gray-500 mt-4">Ishtika Green Valley Resident</p>
                </div>
              </div>

              {/* Testimonial 3 */}
              <div className="swiper-slide">
                <div className="testimonial-card">
                  <div className="flex items-center mb-4">
                    <img src="https://i.pravatar.cc/80?img=33" alt="Arun Menon" className="w-16 h-16 rounded-full mr-4" />
                    <div>
                      <h4 className="font-bold text-lg text-gray-900">Arun Menon</h4>
                      <p className="text-gray-600">Business Owner</p>
                      <div className="flex text-gold-500 mt-1">
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                        <i className="fas fa-star"></i>
                      </div>
                    </div>
                  </div>
                  <p className="text-gray-700 italic leading-relaxed">
                    "The best decision we made was buying our apartment at Ishtika Anahata. The smart home features, the clubhouse, and the location - everything is perfect. The Ishtika team delivered on every promise."
                  </p>
                  <p className="text-sm text-gray-500 mt-4">Ishtika Anahata Resident</p>
                </div>
              </div>
            </div>
            <div className="swiper-pagination mt-8"></div>
          </div>
        </div>
      </section>

      {/* Contact/CTA Section */}
      <section id="contact" className="py-20 bg-white">
        <div className="container mx-auto px-4">
          <div className="grid md:grid-cols-2 gap-12">
            {/* Contact Info */}
            <div data-aos="fade-right">
              <span className="text-gold-600 font-semibold tracking-widest uppercase text-sm">Get In Touch</span>
              <h2 className="text-4xl md:text-5xl font-bold text-gray-900 mt-4 mb-6">Let's Find Your Dream Home</h2>
              <p className="text-gray-600 text-lg leading-relaxed mb-8">
                Schedule a site visit or get more information about our projects. Our team is here to help you find the perfect home.
              </p>

              <div className="space-y-6">
                <div className="flex items-start">
                  <div className="w-12 h-12 bg-gold-100 rounded-full flex items-center justify-center mr-4 flex-shrink-0">
                    <i className="fas fa-phone text-gold-600 text-xl"></i>
                  </div>
                  <div>
                    <h4 className="font-bold text-lg text-gray-900">Call Us</h4>
                    <a href="tel:+919876543210" className="text-primary-600 hover:text-primary-700">+91 98765 43210</a>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="w-12 h-12 bg-gold-100 rounded-full flex items-center justify-center mr-4 flex-shrink-0">
                    <i className="fas fa-envelope text-gold-600 text-xl"></i>
                  </div>
                  <div>
                    <h4 className="font-bold text-lg text-gray-900">Email Us</h4>
                    <a href="mailto:info@ishtikahomes.com" className="text-primary-600 hover:text-primary-700">info@ishtikahomes.com</a>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="w-12 h-12 bg-gold-100 rounded-full flex items-center justify-center mr-4 flex-shrink-0">
                    <i className="fas fa-map-marker-alt text-gold-600 text-xl"></i>
                  </div>
                  <div>
                    <h4 className="font-bold text-lg text-gray-900">Visit Us</h4>
                    <p className="text-gray-600">Sales Office, Bangalore</p>
                  </div>
                </div>

                <div className="flex items-start">
                  <div className="w-12 h-12 bg-gold-100 rounded-full flex items-center justify-center mr-4 flex-shrink-0">
                    <i className="fas fa-clock text-gold-600 text-xl"></i>
                  </div>
                  <div>
                    <h4 className="font-bold text-lg text-gray-900">Office Hours</h4>
                    <p className="text-gray-600">Mon - Sun: 9:00 AM - 7:00 PM</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Contact Form */}
            <div data-aos="fade-left">
              <div className="bg-gray-50 p-8 rounded-2xl shadow-lg">
                <h3 className="text-2xl font-bold text-gray-900 mb-6">Schedule a Site Visit</h3>
                <form id="contactForm" className="space-y-4">
                  <div>
                    <input type="text" name="name" placeholder="Your Name *" required className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-gold-500" />
                  </div>
                  <div>
                    <input type="email" name="email" placeholder="Email Address *" required className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-gold-500" />
                  </div>
                  <div>
                    <input type="tel" name="phone" placeholder="Phone Number *" required className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-gold-500" />
                  </div>
                  <div>
                    <select name="project" className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-gold-500">
                      <option value="">Select Project</option>
                      <option value="anahata">Ishtika Anahata</option>
                      <option value="tranquil">Ishtika Tranquil Heights</option>
                      <option value="green">Ishtika Green Valley</option>
                    </select>
                  </div>
                  <div>
                    <select name="configuration" className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-gold-500">
                      <option value="">Select Configuration</option>
                      <option value="2bhk">2 BHK</option>
                      <option value="3bhk">3 BHK</option>
                      <option value="4bhk">4 BHK</option>
                    </select>
                  </div>
                  <div>
                    <textarea name="message" placeholder="Message (Optional)" rows={3} className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-gold-500"></textarea>
                  </div>
                  <button type="submit" className="w-full px-8 py-4 bg-gradient-to-r from-gold-500 to-gold-600 text-white font-bold rounded-lg hover:from-gold-600 hover:to-gold-700 transition transform hover:-translate-y-1 shadow-lg">
                    <i className="fas fa-paper-plane mr-2"></i>Submit Request
                  </button>
                  <p className="text-sm text-gray-600 text-center">We'll contact you within 24 hours</p>
                </form>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-gray-900 text-white py-12">
        <div className="container mx-auto px-4">
          <div className="grid md:grid-cols-4 gap-8">
            {/* Company Info */}
            <div>
              <div className="mb-4">
                <img src="images/ishtika-logo.png" alt="Ishtika Homes" className="h-14 w-auto" style={{ background: 'transparent' }} />
              </div>
              <p className="text-gray-400 mb-4">
                Building dreams, creating legacies. Trusted real estate developer in Bangalore with 15+ years of excellence.
              </p>
              <div className="flex space-x-4">
                <a href="#" className="w-10 h-10 bg-gray-800 rounded-full flex items-center justify-center hover:bg-gold-500 transition">
                  <i className="fab fa-facebook-f"></i>
                </a>
                <a href="#" className="w-10 h-10 bg-gray-800 rounded-full flex items-center justify-center hover:bg-gold-500 transition">
                  <i className="fab fa-instagram"></i>
                </a>
                <a href="#" className="w-10 h-10 bg-gray-800 rounded-full flex items-center justify-center hover:bg-gold-500 transition">
                  <i className="fab fa-linkedin-in"></i>
                </a>
                <a href="#" className="w-10 h-10 bg-gray-800 rounded-full flex items-center justify-center hover:bg-gold-500 transition">
                  <i className="fab fa-youtube"></i>
                </a>
              </div>
            </div>

            {/* Quick Links */}
            <div>
              <h4 className="text-lg font-bold mb-4">Quick Links</h4>
              <ul className="space-y-2">
                <li><a href="#about" className="text-gray-400 hover:text-gold-400 transition">About Us</a></li>
                <li><a href="#projects" className="text-gray-400 hover:text-gold-400 transition">Projects</a></li>
                <li><a href="#testimonials" className="text-gray-400 hover:text-gold-400 transition">Testimonials</a></li>
                <li><a href="#contact" className="text-gray-400 hover:text-gold-400 transition">Contact</a></li>
              </ul>
            </div>

            {/* Projects */}
            <div>
              <h4 className="text-lg font-bold mb-4">Our Projects</h4>
              <ul className="space-y-2">
                <li><a href="#" className="text-gray-400 hover:text-gold-400 transition">Ishtika Anahata</a></li>
                <li><a href="#" className="text-gray-400 hover:text-gold-400 transition">Ishtika Tranquil Heights</a></li>
                <li><a href="#" className="text-gray-400 hover:text-gold-400 transition">Ishtika Green Valley</a></li>
              </ul>
            </div>

            {/* Newsletter */}
            <div>
              <h4 className="text-lg font-bold mb-4">Newsletter</h4>
              <p className="text-gray-400 mb-4">Subscribe for exclusive updates and offers</p>
              <form className="flex">
                <input type="email" placeholder="Your email" className="flex-1 px-4 py-2 bg-gray-800 text-white rounded-l-lg focus:outline-none focus:ring-2 focus:ring-gold-500" />
                <button type="submit" className="px-4 py-2 bg-gold-500 text-white rounded-r-lg hover:bg-gold-600 transition">
                  <i className="fas fa-paper-plane"></i>
                </button>
              </form>
            </div>
          </div>

          <div className="border-t border-gray-800 mt-8 pt-8 text-center">
            <p className="text-gray-400">
              &copy; 2024 Ishtika Homes. All Rights Reserved. | <a href="#" className="text-gold-400 hover:text-gold-500">Privacy Policy</a> | <a href="#" className="text-gold-400 hover:text-gold-500">Terms & Conditions</a>
            </p>
          </div>
        </div>
      </footer>

      {/* Bottom Action Bar */}
      <div className="bottom-action-bar">
        <a href="tel:+919876543210" className="action-btn">
          <i className="fas fa-phone"></i>
          <span>Call</span>
        </a>
        <a href="https://wa.me/919876543210?text=Hi%2C%20I%20am%20interested%20in%20Ishtika%20Homes%20projects" target="_blank" className="action-btn">
          <i className="fab fa-whatsapp"></i>
          <span>WhatsApp</span>
        </a>
        <a href="#contact" className="action-btn">
          <i className="fas fa-comments"></i>
          <span>Chat</span>
        </a>
        <a href="#contact" className="action-btn">
          <i className="fas fa-paper-plane"></i>
          <span>Enquire</span>
        </a>
      </div>

    </>
  );
}
