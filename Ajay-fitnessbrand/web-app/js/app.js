// ================================================
// Global Variables
// ================================================

const navbar = document.getElementById('navbar');
const navToggle = document.getElementById('navToggle');
const navMenu = document.getElementById('navMenu');
const scrollToTopBtn = document.getElementById('scrollToTop');

// Video Modal Elements
const videoModal = document.getElementById('videoModal');
const watchDemoBtn = document.getElementById('watchDemoBtn');
const closeVideoModal = document.getElementById('closeVideoModal');
const videoModalOverlay = document.querySelector('.video-modal-overlay');
const demoVideo = document.getElementById('demoVideo');

// ================================================
// Navigation Scroll Effect
// ================================================

window.addEventListener('scroll', () => {
    if (window.scrollY > 50) {
        navbar.classList.add('scrolled');
    } else {
        navbar.classList.remove('scrolled');
    }

    // Show/hide scroll to top button
    if (window.scrollY > 300) {
        scrollToTopBtn.classList.add('visible');
    } else {
        scrollToTopBtn.classList.remove('visible');
    }
});

// ================================================
// Mobile Navigation Toggle
// ================================================

navToggle.addEventListener('click', () => {
    navMenu.classList.toggle('active');

    // Animate hamburger menu
    const spans = navToggle.querySelectorAll('span');
    spans[0].style.transform = navMenu.classList.contains('active')
        ? 'rotate(45deg) translate(5px, 5px)'
        : 'none';
    spans[1].style.opacity = navMenu.classList.contains('active') ? '0' : '1';
    spans[2].style.transform = navMenu.classList.contains('active')
        ? 'rotate(-45deg) translate(7px, -6px)'
        : 'none';
});

// Close mobile menu when clicking on a link
document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', () => {
        navMenu.classList.remove('active');
        const spans = navToggle.querySelectorAll('span');
        spans[0].style.transform = 'none';
        spans[1].style.opacity = '1';
        spans[2].style.transform = 'none';
    });
});

// ================================================
// Smooth Scroll
// ================================================

document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));

        if (target) {
            const offsetTop = target.offsetTop - 80; // Account for fixed navbar
            window.scrollTo({
                top: offsetTop,
                behavior: 'smooth'
            });
        }
    });
});

// ================================================
// Scroll to Top Button
// ================================================

scrollToTopBtn.addEventListener('click', () => {
    window.scrollTo({
        top: 0,
        behavior: 'smooth'
    });
});

// ================================================
// Video Demo Modal
// ================================================

// Demo video URL - Smart Watch / Fitness Band Demo
// Current: Fitness smartwatch demo video
const demoVideoUrl = 'https://www.youtube.com/embed/T96l1V7Vqqk?autoplay=1&rel=0&modestbranding=1';
// Replace with YOUR actual IHW demo video when ready
// Format: 'https://www.youtube.com/embed/YOUR_VIDEO_ID?autoplay=1&rel=0';

// Open video modal
if (watchDemoBtn) {
    watchDemoBtn.addEventListener('click', (e) => {
        e.preventDefault();
        openVideoModal();
    });
}

// Close video modal
if (closeVideoModal) {
    closeVideoModal.addEventListener('click', closeVideoModalFunc);
}

if (videoModalOverlay) {
    videoModalOverlay.addEventListener('click', closeVideoModalFunc);
}

function openVideoModal() {
    if (videoModal && demoVideo) {
        videoModal.classList.add('active');
        document.body.style.overflow = 'hidden'; // Prevent background scroll

        // Check if we have a video URL
        if (demoVideoUrl && demoVideoUrl.includes('youtube.com') || demoVideoUrl.includes('vimeo.com')) {
            // Show video, hide placeholder
            const placeholder = document.getElementById('videoPlaceholder');
            if (placeholder) placeholder.style.display = 'none';
            demoVideo.style.display = 'block';

            // Load video with autoplay
            demoVideo.src = demoVideoUrl;
        } else {
            // Show placeholder, hide video
            const placeholder = document.getElementById('videoPlaceholder');
            if (placeholder) placeholder.style.display = 'flex';
            demoVideo.style.display = 'none';
        }

        // Track event
        trackEvent('Demo', 'Watch Video', 'Hero Section');
    }
}

function closeVideoModalFunc() {
    if (videoModal && demoVideo) {
        videoModal.classList.remove('active');
        document.body.style.overflow = ''; // Restore scroll

        // Stop video by removing src
        demoVideo.src = '';
    }
}

// Close modal with Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && videoModal.classList.contains('active')) {
        closeVideoModalFunc();
    }
});

// ================================================
// Intersection Observer for Animations
// ================================================

const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
};

const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.classList.add('aos-animate');
            // Optionally unobserve after animation
            // observer.unobserve(entry.target);
        }
    });
}, observerOptions);

// Observe all elements with data-aos attribute
document.querySelectorAll('[data-aos]').forEach(el => {
    observer.observe(el);
});

// ================================================
// Counter Animation
// ================================================

function animateCounter(element, target, duration = 2000) {
    const start = 0;
    const increment = target / (duration / 16); // 60fps
    let current = start;

    const timer = setInterval(() => {
        current += increment;
        if (current >= target) {
            element.textContent = formatNumber(target);
            clearInterval(timer);
        } else {
            element.textContent = formatNumber(Math.floor(current));
        }
    }, 16);
}

function formatNumber(num) {
    if (num >= 10000) {
        return (num / 1000).toFixed(0) + 'K+';
    }
    return num.toString();
}

// Animate stats when hero section is in view
const heroStats = document.querySelectorAll('.stat-number');
let statsAnimated = false;

const heroObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting && !statsAnimated) {
            statsAnimated = true;
            // Animate each stat
            const stats = [
                { element: heroStats[0], target: 10000 },
                { element: heroStats[1], target: 99.9 },
                { element: heroStats[2], target: 24 }
            ];

            stats.forEach((stat, index) => {
                setTimeout(() => {
                    if (index === 1) {
                        animateDecimal(stat.element, stat.target, 2000);
                    } else if (index === 2) {
                        stat.element.textContent = '24/7';
                    } else {
                        animateCounter(stat.element, stat.target, 2000);
                    }
                }, index * 200);
            });
        }
    });
}, { threshold: 0.5 });

if (document.querySelector('.hero-stats')) {
    heroObserver.observe(document.querySelector('.hero-stats'));
}

function animateDecimal(element, target, duration = 2000) {
    const start = 0;
    const increment = target / (duration / 16);
    let current = start;

    const timer = setInterval(() => {
        current += increment;
        if (current >= target) {
            element.textContent = target.toFixed(1) + '%';
            clearInterval(timer);
        } else {
            element.textContent = current.toFixed(1) + '%';
        }
    }, 16);
}

// ================================================
// AI Dashboard Progress Bars Animation
// ================================================

const aiSection = document.querySelector('.ai-section');
let aiAnimated = false;

const aiObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting && !aiAnimated) {
            aiAnimated = true;
            // Animate progress bars
            const progressBars = document.querySelectorAll('.ai-progress-fill');
            progressBars.forEach((bar, index) => {
                setTimeout(() => {
                    const targetWidth = bar.style.width;
                    bar.style.width = '0%';
                    setTimeout(() => {
                        bar.style.width = targetWidth;
                    }, 100);
                }, index * 200);
            });
        }
    });
}, { threshold: 0.3 });

if (aiSection) {
    aiObserver.observe(aiSection);
}

// ================================================
// Pricing Card Hover Effects
// ================================================

const pricingCards = document.querySelectorAll('.pricing-card');

pricingCards.forEach(card => {
    card.addEventListener('mouseenter', function() {
        this.style.borderColor = getComputedStyle(document.documentElement)
            .getPropertyValue('--primary-color');
    });

    card.addEventListener('mouseleave', function() {
        if (!this.classList.contains('featured')) {
            this.style.borderColor = getComputedStyle(document.documentElement)
                .getPropertyValue('--border-color');
        }
    });
});

// ================================================
// Feature Cards Stagger Animation
// ================================================

const featureCards = document.querySelectorAll('.feature-card');

featureCards.forEach((card, index) => {
    card.style.animationDelay = `${index * 0.1}s`;
});

// ================================================
// Active Link Highlighting
// ================================================

const sections = document.querySelectorAll('section[id]');
const navLinks = document.querySelectorAll('.nav-link');

window.addEventListener('scroll', () => {
    let current = '';

    sections.forEach(section => {
        const sectionTop = section.offsetTop;
        const sectionHeight = section.clientHeight;

        if (window.scrollY >= (sectionTop - 100)) {
            current = section.getAttribute('id');
        }
    });

    navLinks.forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('href') === `#${current}`) {
            link.classList.add('active');
        }
    });
});

// ================================================
// Form Validation (if needed in future)
// ================================================

function validateEmail(email) {
    const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return re.test(email);
}

function validatePhone(phone) {
    const re = /^[0-9]{10}$/;
    return re.test(phone.replace(/\s/g, ''));
}

// ================================================
// Dynamic Year in Footer
// ================================================

const currentYear = new Date().getFullYear();
const footerText = document.querySelector('.footer-bottom p');
if (footerText) {
    footerText.innerHTML = `&copy; ${currentYear} IHW. All rights reserved.`;
}

// ================================================
// Lazy Load Images (if images are added)
// ================================================

if ('IntersectionObserver' in window) {
    const imageObserver = new IntersectionObserver((entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const img = entry.target;
                img.src = img.dataset.src;
                img.classList.remove('lazy');
                imageObserver.unobserve(img);
            }
        });
    });

    const lazyImages = document.querySelectorAll('img.lazy');
    lazyImages.forEach(img => imageObserver.observe(img));
}

// ================================================
// Performance Optimization - Debounce
// ================================================

function debounce(func, wait = 20, immediate = true) {
    let timeout;
    return function() {
        const context = this;
        const args = arguments;
        const later = function() {
            timeout = null;
            if (!immediate) func.apply(context, args);
        };
        const callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func.apply(context, args);
    };
}

// ================================================
// Parallax Effect for Hero Section
// ================================================

window.addEventListener('scroll', debounce(() => {
    const scrolled = window.scrollY;
    const heroCards = document.querySelectorAll('.hero-card');

    heroCards.forEach((card, index) => {
        const speed = 0.5 + (index * 0.1);
        const yPos = -(scrolled * speed);
        card.style.transform = `translateY(${yPos}px)`;
    });
}));

// ================================================
// Record Cards Animation in Health Records Section
// ================================================

const recordCards = document.querySelectorAll('.record-card');

recordCards.forEach((card, index) => {
    card.style.animationDelay = `${index * 1.5}s`;
});

// ================================================
// Button Click Effects
// ================================================

document.querySelectorAll('.btn').forEach(button => {
    button.addEventListener('click', function(e) {
        const ripple = document.createElement('span');
        const rect = this.getBoundingClientRect();
        const size = Math.max(rect.width, rect.height);
        const x = e.clientX - rect.left - size / 2;
        const y = e.clientY - rect.top - size / 2;

        ripple.style.width = ripple.style.height = size + 'px';
        ripple.style.left = x + 'px';
        ripple.style.top = y + 'px';
        ripple.classList.add('ripple');

        this.appendChild(ripple);

        setTimeout(() => {
            ripple.remove();
        }, 600);
    });
});

// Add ripple styles dynamically
const style = document.createElement('style');
style.textContent = `
    .btn {
        position: relative;
        overflow: hidden;
    }

    .ripple {
        position: absolute;
        border-radius: 50%;
        background: rgba(255, 255, 255, 0.6);
        transform: scale(0);
        animation: ripple-animation 0.6s ease-out;
        pointer-events: none;
    }

    @keyframes ripple-animation {
        to {
            transform: scale(4);
            opacity: 0;
        }
    }

    .nav-link.active {
        color: var(--primary-color);
        font-weight: 600;
    }
`;
document.head.appendChild(style);

// ================================================
// Console Welcome Message
// ================================================

console.log('%c IHW Platform ', 'background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; font-size: 20px; padding: 10px 20px; border-radius: 5px;');
console.log('%c Welcome to IHW - Your Complete Health & Fitness Solution ', 'font-size: 14px; color: #667eea;');
console.log('%c Built with ❤️ for better health ', 'font-size: 12px; color: #6b7280;');

// ================================================
// Initialize on DOM Load
// ================================================

document.addEventListener('DOMContentLoaded', () => {
    // Add loaded class to body for any CSS animations
    document.body.classList.add('loaded');

    // Preload critical animations
    setTimeout(() => {
        document.querySelectorAll('[data-aos]').forEach(el => {
            el.style.transition = 'all 0.6s ease';
        });
    }, 100);
});

// ================================================
// Handle External Links
// ================================================

document.querySelectorAll('a[href^="http"]').forEach(link => {
    link.setAttribute('target', '_blank');
    link.setAttribute('rel', 'noopener noreferrer');
});

// ================================================
// Keyboard Navigation Accessibility
// ================================================

document.addEventListener('keydown', (e) => {
    // Escape key closes mobile menu
    if (e.key === 'Escape' && navMenu.classList.contains('active')) {
        navMenu.classList.remove('active');
        const spans = navToggle.querySelectorAll('span');
        spans[0].style.transform = 'none';
        spans[1].style.opacity = '1';
        spans[2].style.transform = 'none';
    }

    // Ctrl/Cmd + K for quick navigation (optional feature)
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault();
        // Could add a search/navigation modal here
    }
});

// ================================================
// Track User Interactions (Analytics Ready)
// ================================================

function trackEvent(category, action, label) {
    // Integration point for analytics (Google Analytics, Mixpanel, etc.)
    console.log('Track Event:', { category, action, label });

    // Example: Google Analytics
    // if (typeof gtag !== 'undefined') {
    //     gtag('event', action, {
    //         'event_category': category,
    //         'event_label': label
    //     });
    // }
}

// Track button clicks
document.querySelectorAll('.btn').forEach(btn => {
    btn.addEventListener('click', function() {
        const text = this.textContent.trim();
        trackEvent('Button', 'Click', text);
    });
});

// Track navigation
navLinks.forEach(link => {
    link.addEventListener('click', function() {
        const section = this.getAttribute('href');
        trackEvent('Navigation', 'Click', section);
    });
});

// ================================================
// Error Handling
// ================================================

window.addEventListener('error', (e) => {
    console.error('An error occurred:', e.message);
    // Could send to error tracking service
});

window.addEventListener('unhandledrejection', (e) => {
    console.error('Unhandled promise rejection:', e.reason);
    // Could send to error tracking service
});

// ================================================
// Performance Monitoring
// ================================================

if ('performance' in window) {
    window.addEventListener('load', () => {
        setTimeout(() => {
            const perfData = performance.getEntriesByType('navigation')[0];
            console.log('Page Load Time:', perfData.loadEventEnd - perfData.fetchStart, 'ms');

            // Track performance metrics
            trackEvent('Performance', 'Page Load', `${Math.round(perfData.loadEventEnd - perfData.fetchStart)}ms`);
        }, 0);
    });
}

// ================================================
// Service Worker Registration (for PWA - optional)
// ================================================

if ('serviceWorker' in navigator) {
    // Uncomment to enable service worker
    // window.addEventListener('load', () => {
    //     navigator.serviceWorker.register('/sw.js')
    //         .then(registration => console.log('SW registered:', registration))
    //         .catch(error => console.log('SW registration failed:', error));
    // });
}

// ================================================
// Human Anatomy Interactive Feature
// ================================================

// Anatomy data for each body part
const anatomyData = {
    head: {
        title: 'Head & Brain',
        subtitle: 'Neurological System',
        icon: 'fa-brain',
        metrics: [
            { label: 'Sleep Quality', value: '85', unit: '%', status: 'good' },
            { label: 'Stress Level', value: '32', unit: '%', status: 'good' },
            { label: 'Focus Score', value: '78', unit: '/100', status: 'good' },
            { label: 'Meditation Time', value: '15', unit: 'min', status: 'good' }
        ],
        insights: [
            { icon: 'fa-check-circle', text: 'Your sleep quality has improved by 12% this week' },
            { icon: 'fa-lightbulb', text: 'Consider 10 minutes of meditation daily to reduce stress' }
        ]
    },
    heart: {
        title: 'Heart',
        subtitle: 'Cardiovascular System',
        icon: 'fa-heartbeat',
        metrics: [
            { label: 'Heart Rate', value: '72', unit: 'BPM', status: 'good' },
            { label: 'HRV', value: '65', unit: 'ms', status: 'good' },
            { label: 'Blood Pressure', value: '120/80', unit: 'mmHg', status: 'good' },
            { label: 'Resting HR', value: '58', unit: 'BPM', status: 'good' }
        ],
        insights: [
            { icon: 'fa-check-circle', text: 'Your heart rate is within normal range' },
            { icon: 'fa-arrow-up', text: 'HRV has increased 8% - indicates good recovery' }
        ]
    },
    lungs: {
        title: 'Lungs',
        subtitle: 'Respiratory System',
        icon: 'fa-lungs',
        metrics: [
            { label: 'SpO2', value: '98', unit: '%', status: 'good' },
            { label: 'Respiratory Rate', value: '16', unit: 'bpm', status: 'good' },
            { label: 'VO2 Max', value: '45', unit: 'ml/kg/min', status: 'good' },
            { label: 'Breathing Score', value: '92', unit: '/100', status: 'good' }
        ],
        insights: [
            { icon: 'fa-check-circle', text: 'Oxygen saturation is excellent' },
            { icon: 'fa-lightbulb', text: 'Try breathing exercises to improve VO2 max' }
        ]
    },
    stomach: {
        title: 'Stomach',
        subtitle: 'Digestive System',
        icon: 'fa-apple-alt',
        metrics: [
            { label: 'Hydration', value: '78', unit: '%', status: 'good' },
            { label: 'Calorie Intake', value: '1,850', unit: 'kcal', status: 'good' },
            { label: 'Protein', value: '85', unit: 'g', status: 'good' },
            { label: 'Water Intake', value: '2.5', unit: 'L', status: 'good' }
        ],
        insights: [
            { icon: 'fa-check-circle', text: 'Your nutrition balance is on track' },
            { icon: 'fa-tint', text: 'Drink 500ml more water to reach optimal hydration' }
        ]
    },
    'left-arm': {
        title: 'Arms',
        subtitle: 'Muscular System',
        icon: 'fa-dumbbell',
        metrics: [
            { label: 'Strength Score', value: '82', unit: '/100', status: 'good' },
            { label: 'Muscle Mass', value: '42', unit: '%', status: 'good' },
            { label: 'Workouts/Week', value: '4', unit: 'sessions', status: 'good' },
            { label: 'Recovery', value: '88', unit: '%', status: 'good' }
        ],
        insights: [
            { icon: 'fa-trophy', text: 'Strength improved 15% in the last month' },
            { icon: 'fa-fire', text: 'Upper body workouts are well-balanced' }
        ]
    },
    'right-arm': {
        title: 'Arms',
        subtitle: 'Muscular System',
        icon: 'fa-dumbbell',
        metrics: [
            { label: 'Strength Score', value: '82', unit: '/100', status: 'good' },
            { label: 'Muscle Mass', value: '42', unit: '%', status: 'good' },
            { label: 'Workouts/Week', value: '4', unit: 'sessions', status: 'good' },
            { label: 'Recovery', value: '88', unit: '%', status: 'good' }
        ],
        insights: [
            { icon: 'fa-trophy', text: 'Strength improved 15% in the last month' },
            { icon: 'fa-fire', text: 'Upper body workouts are well-balanced' }
        ]
    },
    'left-leg': {
        title: 'Legs',
        subtitle: 'Muscular & Skeletal System',
        icon: 'fa-running',
        metrics: [
            { label: 'Steps Today', value: '8,542', unit: 'steps', status: 'good' },
            { label: 'Distance', value: '6.2', unit: 'km', status: 'good' },
            { label: 'Active Minutes', value: '85', unit: 'min', status: 'good' },
            { label: 'Calories Burned', value: '420', unit: 'kcal', status: 'good' }
        ],
        insights: [
            { icon: 'fa-chart-line', text: 'Daily step goal achieved 6 days in a row!' },
            { icon: 'fa-shoe-prints', text: 'Walking distance increased 25% this month' }
        ]
    },
    'right-leg': {
        title: 'Legs',
        subtitle: 'Muscular & Skeletal System',
        icon: 'fa-running',
        metrics: [
            { label: 'Steps Today', value: '8,542', unit: 'steps', status: 'good' },
            { label: 'Distance', value: '6.2', unit: 'km', status: 'good' },
            { label: 'Active Minutes', value: '85', unit: 'min', status: 'good' },
            { label: 'Calories Burned', value: '420', unit: 'kcal', status: 'good' }
        ],
        insights: [
            { icon: 'fa-chart-line', text: 'Daily step goal achieved 6 days in a row!' },
            { icon: 'fa-shoe-prints', text: 'Walking distance increased 25% this month' }
        ]
    },
    torso: {
        title: 'Core & Torso',
        subtitle: 'Overall Body Metrics',
        icon: 'fa-user',
        metrics: [
            { label: 'Body Fat', value: '18', unit: '%', status: 'good' },
            { label: 'BMI', value: '23.5', unit: '', status: 'good' },
            { label: 'Weight', value: '72', unit: 'kg', status: 'good' },
            { label: 'Body Temperature', value: '36.8', unit: '°C', status: 'good' }
        ],
        insights: [
            { icon: 'fa-check-circle', text: 'Your body composition is in healthy range' },
            { icon: 'fa-balance-scale', text: 'Weight has been stable for the past 2 weeks' }
        ]
    }
};

// Get DOM elements
const bodyParts = document.querySelectorAll('.body-part');
const anatomyPlaceholder = document.getElementById('anatomyPlaceholder');
const anatomyInfo = document.getElementById('anatomyInfo');
const anatomyTitle = document.getElementById('anatomyTitle');
const anatomySubtitle = document.getElementById('anatomySubtitle');
const anatomyIcon = document.getElementById('anatomyIcon');
const anatomyMetrics = document.getElementById('anatomyMetrics');
const anatomyInsights = document.getElementById('anatomyInsights');

// Function to display anatomy data
function displayAnatomyData(partName) {
    const data = anatomyData[partName];

    if (!data) return;

    // Remove active class from all parts
    bodyParts.forEach(part => part.classList.remove('active'));

    // Add active class to clicked part
    const activePart = document.querySelector(`[data-part="${partName}"]`);
    if (activePart) {
        activePart.classList.add('active');
    }

    // Hide placeholder and show info
    if (anatomyPlaceholder) anatomyPlaceholder.style.display = 'none';
    if (anatomyInfo) anatomyInfo.style.display = 'block';

    // Update title and icon
    if (anatomyTitle) anatomyTitle.textContent = data.title;
    if (anatomySubtitle) anatomySubtitle.textContent = data.subtitle;
    if (anatomyIcon) anatomyIcon.innerHTML = `<i class="fas ${data.icon}"></i>`;

    // Update metrics
    if (anatomyMetrics) {
        anatomyMetrics.innerHTML = data.metrics.map(metric => `
            <div class="anatomy-metric-card">
                <div class="anatomy-metric-label">${metric.label}</div>
                <div class="anatomy-metric-value">
                    <span>${metric.value}</span>
                    <span class="anatomy-metric-unit">${metric.unit}</span>
                </div>
                <span class="anatomy-metric-status status-${metric.status}">
                    ${metric.status === 'good' ? '✓ Normal' : metric.status === 'warning' ? '⚠ Monitor' : '⚠ Alert'}
                </span>
            </div>
        `).join('');
    }

    // Update insights
    if (anatomyInsights) {
        anatomyInsights.innerHTML = `
            <h4><i class="fas fa-lightbulb"></i> Insights & Recommendations</h4>
            ${data.insights.map(insight => `
                <div class="anatomy-insight-item">
                    <i class="fas ${insight.icon}"></i>
                    <p>${insight.text}</p>
                </div>
            `).join('')}
        `;
    }

    // Track event
    trackEvent('Anatomy', 'View Part', partName);
}

// Add click event listeners to all body parts
if (bodyParts) {
    bodyParts.forEach(part => {
        part.addEventListener('click', function() {
            const partName = this.getAttribute('data-part');
            displayAnatomyData(partName);
        });

        // Optional: Show on hover (you can enable this if you prefer hover instead of click)
        part.addEventListener('mouseenter', function() {
            const partName = this.getAttribute('data-part');
            // Uncomment below to enable hover functionality
            // displayAnatomyData(partName);
        });
    });
}

// Action button handlers
const viewDetailedAnalysisBtn = document.getElementById('viewDetailedAnalysis');
const trackThisMetricBtn = document.getElementById('trackThisMetric');

if (viewDetailedAnalysisBtn) {
    viewDetailedAnalysisBtn.addEventListener('click', function() {
        trackEvent('Anatomy', 'View Detailed Analysis', 'Button Click');
        // Add your logic here
        alert('Detailed analysis feature coming soon!');
    });
}

if (trackThisMetricBtn) {
    trackThisMetricBtn.addEventListener('click', function() {
        trackEvent('Anatomy', 'Track Metric', 'Button Click');
        // Add your logic here
        alert('Tracking feature coming soon!');
    });
}

// ================================================
// Export functions for testing or external use
// ================================================

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        validateEmail,
        validatePhone,
        debounce,
        trackEvent
    };
}
