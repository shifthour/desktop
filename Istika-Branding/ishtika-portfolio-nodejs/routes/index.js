const express = require('express');
const router = express.Router();
const nodemailer = require('nodemailer');
const supabase = require('../config/supabase');

// Company Information
const companyInfo = {
  name: 'Ishtika Homes',
  tagline: 'Building Dreams, One At A Time!',
  description: 'At Ishtika Homes, we blend luxury and nature to create exceptional living spaces in Karnataka.',
  phone: {
    bengaluru: '7338628777',
    bellary: '96866 58656'
  },
  email: 'sales@ishtikahomes.com',
  address: 'Ishtika Homes Pvt Ltd. #937, 21st main, 9th cross road, 2nd phase, JP Nagar, Bengaluru, Karnataka 560022',
  social: {
    facebook: '#',
    instagram: '#',
    linkedin: '#',
    youtube: '#'
  },
  stats: {
    experience: 12,
    establishedProjects: 12,
    ongoingProjects: 5,
    happyResidents: 1000
  }
};

// Featured Projects Data (3 projects for homepage)
const featuredProjects = [
  {
    id: 'anahata',
    slug: '/anahata',  // Points to static Next.js landing page
    name: 'Anahata',
    location: 'Whitefield, Bengaluru',
    fullLocation: 'Sy No. 5, Samethanahalli Village, Bengaluru, Karnataka 560067',
    image: '/assets/img/aerial-view-bg.jpg',
    description: 'Tucked just off Soukya Road in the fast-growing Whitefield corridor, Anahata offers residents an elegant escape from the bustle of Bengaluru.',
    units: 440,
    type: '2 & 3 BHK',
    brochure: '/assets/img/pdf/Anahata.pdf'
  },
  {
    id: 'vashishta',
    slug: '/projects/vashishta',
    name: 'Vashishta',
    location: 'JP Nagar, Bengaluru',
    fullLocation: 'Anjanapura BDA Layout, JP Nagar 9th Phase, Bengaluru - 560 083',
    image: '/assets/img/all-images/features-img2.png',
    description: 'A premium residential project in JP Nagar, blending comfort, affordability, and vaastu compliance amidst natural landscapes.',
    sqft: '1.25 Lakh',
    area: 'JP Nagar'
  },
  {
    id: 'krishna',
    slug: '/projects/krishna',
    name: 'Krishna',
    location: 'Hosapete, Vijayanagara',
    fullLocation: 'Door No. 326 & 327, Beside Venkateshwara Kalyana Mantapa, Ward No. 03, 1st Main Road, Patel Nagar, Hosapete, Vijayanagara (Dist) - 583 201',
    image: '/assets/img/all-images/features-img1.png',
    description: 'A 2.25 lakh Sqft residential project in Hospet spanning 1.25 acres, offering 119 luxury flats with proximity to key landmarks, ideal for families.',
    sqft: '2.25 Lakh',
    area: '1.25 acres',
    units: 119
  }
];

// Projects Data
const projects = {
  ongoing: [
    {
      id: 'anahata',
      slug: '/anahata',  // Points to static Next.js landing page
      name: 'Anahata',
      location: 'Bengaluru',
      image: '/assets/img/aerial-view-bg.jpg',
      description: 'A 440-unit, 2 & 3 BHK gated community in Bengaluru blending resort-style living with vaastu-compliant design for lasting comfort.',
      units: 440,
      type: '2 & 3 BHK'
    },
    {
      id: 'krishna',
      slug: '/projects/krishna',
      name: 'Krishna',
      location: 'Hosapete',
      image: '/assets/img/all-images/features-img1.png',
      description: 'This 2.25 lakh Sqft residential project in Hospet spans 1.25 acres, offering 119 luxury flats with proximity to key landmarks, ideal for families.',
      sqft: '2.25 Lakh',
      area: '1.25 acres',
      units: 119
    },
    {
      id: 'vashishta',
      slug: '/projects/vashishta',
      name: 'Vashishta',
      location: 'Bengaluru',
      image: '/assets/img/all-images/features-img2.png',
      description: 'A 1.25 lakh Sqft premium residential project in JP Nagar, blending comfort, affordability, and vaastu compliance amidst natural landscapes.',
      sqft: '1.25 Lakh',
      area: 'JP Nagar'
    },
    {
      id: 'naadam',
      slug: '/projects/naadam',
      name: 'Naadam',
      location: 'Ballary',
      image: '/assets/img/all-images/features-img3.png',
      description: 'A 1.42 lakh Sqft project in Bellary featuring lush landscapes, walkways, and kids\' play areas, providing tranquility and a nature-centric life.',
      sqft: '1.42 Lakh'
    },
    {
      id: 'vyasa',
      slug: '/projects/vyasa',
      name: 'Vyasa',
      location: 'Ballary',
      image: '/assets/img/all-images/features-img4.png',
      description: 'An 80,000 Sqft residential project in Bellary, offering vaastu-compliant luxury apartments set against beautiful natural landscapes.',
      sqft: '80,000'
    }
  ],
  completed: [
    { name: 'Agastya', image: '/assets/img/all-images/agastya-img3.png' },
    { name: 'Sunrise', image: '/assets/img/all-images/gallery-img1.png' },
    { name: 'Advaitha', image: '/assets/img/all-images/gallery-img3.png' },
    { name: 'White Pearl', image: '/assets/img/all-images/gallery-img6.png' },
    { name: 'Pride', image: '/assets/img/all-images/gallery-img7.png' },
    { name: 'Flora', image: '/assets/img/all-images/gallery-img8.png' },
    { name: 'Arcadia', image: '/assets/img/all-images/gallery-img13.png' },
    { name: 'Chanasya', image: '/assets/img/all-images/gallery-img14.png' },
    { name: 'Prakriti', image: '/assets/img/all-images/gallery-img11.png' }
  ]
};

// Testimonials Data
const testimonials = [
  {
    name: 'Rajesh & Lakshmi Reddy',
    location: 'Anahata, Whitefield',
    project: 'Anahata',
    image: '/assets/img/all-images/client1.jpg',
    rating: 5,
    quote: 'Booking our dream home at Anahata was the best decision for our family. The 3BHK apartment design is spacious, well-ventilated, and the vaastu compliance gives us peace of mind. We\'re excited about the clubhouse and children\'s play area. The construction quality and regular updates from Ishtika Homes have been exceptional. Can\'t wait for possession in March 2028!'
  },
  {
    name: 'Arjun & Priya Sharma',
    location: 'Vashishta, JP Nagar',
    project: 'Vashishta',
    image: '/assets/img/all-images/client3.jpg',
    rating: 5,
    quote: 'Living in Vashishta, JP Nagar has been a dream come true. The location is perfect with schools, hospitals, and shopping centers nearby. What impressed us most was the transparent pricing and no hidden costs. Ishtika Homes truly cares about their customers and it shows in every interaction.'
  },
  {
    name: 'Venkatesh & Anjali Rao',
    location: 'White Pearl, Bellary',
    project: 'White Pearl',
    image: '/assets/img/all-images/client4.jpg',
    rating: 5,
    quote: 'Our apartment in White Pearl is not just a house, it\'s a home. The elegant design, sustainable construction practices, and the beautiful landscaping make it a perfect blend of luxury and nature. We appreciate Ishtika Homes\' commitment to timely delivery and quality construction.'
  },
  {
    name: 'Karthik & Divya Murthy',
    location: 'Advaitha, Bangalore',
    project: 'Advaitha',
    image: '/assets/img/all-images/client5.jpg',
    rating: 5,
    quote: 'After visiting multiple projects, we chose Advaitha for its spacious layout and modern amenities. The vaastu-compliant design was a major factor for us. The customer service team at Ishtika Homes has been excellent, always available to address our queries even after possession!'
  },
  {
    name: 'Ramesh & Sowmya Gowda',
    location: 'Anahata, Whitefield',
    project: 'Anahata',
    image: '/assets/img/all-images/client6.jpg',
    rating: 5,
    quote: 'Investing in Anahata was one of our best financial decisions. We booked our home here after thorough research, and the gated community design offers excellent security and lifestyle amenities. The construction progress has been remarkable and Ishtika Homes keeps us updated regularly. Highly recommended for families looking for their dream home!'
  }
];

// Gallery Data - Multiple images per project
const galleryImages = [
  // Krishna Project
  { project: 'krishna', name: 'Krishna', image: '/assets/img/all-images/krishna-img1.png' },
  { project: 'krishna', name: 'Krishna', image: '/assets/img/all-images/krishna-img2.png' },
  { project: 'krishna', name: 'Krishna', image: '/assets/img/all-images/krishna-img3.png' },
  { project: 'krishna', name: 'Krishna', image: '/assets/img/all-images/krishna-img4.png' },
  { project: 'krishna', name: 'Krishna', image: '/assets/img/all-images/krishna-img5.png' },
  { project: 'krishna', name: 'Krishna', image: '/assets/img/all-images/krishna-img6.png' },

  // Vashishta Project
  { project: 'vashishta', name: 'Vashishta', image: '/assets/img/all-images/vashishta-img1.png' },
  { project: 'vashishta', name: 'Vashishta', image: '/assets/img/all-images/vashishta-img2.png' },
  { project: 'vashishta', name: 'Vashishta', image: '/assets/img/all-images/vashishta-img3.png' },
  { project: 'vashishta', name: 'Vashishta', image: '/assets/img/all-images/vashishta-img4.png' },
  { project: 'vashishta', name: 'Vashishta', image: '/assets/img/all-images/vashishta-img5.png' },
  { project: 'vashishta', name: 'Vashishta', image: '/assets/img/all-images/vashishta-img6.png' },

  // Naadam Project
  { project: 'naadam', name: 'Naadam', image: '/assets/img/all-images/naadam-img1.png' },
  { project: 'naadam', name: 'Naadam', image: '/assets/img/all-images/naadam-img2.png' },
  { project: 'naadam', name: 'Naadam', image: '/assets/img/all-images/naadam-img3.png' },
  { project: 'naadam', name: 'Naadam', image: '/assets/img/all-images/naadam-img4.png' },
  { project: 'naadam', name: 'Naadam', image: '/assets/img/all-images/naadam-img5.png' },
  { project: 'naadam', name: 'Naadam', image: '/assets/img/all-images/naadam-img6.png' },

  // Vyasa Project
  { project: 'vyasa', name: 'Vyasa', image: '/assets/img/all-images/vyasa-img1.png' },
  { project: 'vyasa', name: 'Vyasa', image: '/assets/img/all-images/vyasa-img2.png' },
  { project: 'vyasa', name: 'Vyasa', image: '/assets/img/all-images/vyasa-img3.png' },
  { project: 'vyasa', name: 'Vyasa', image: '/assets/img/all-images/vyasa-img4.png' },
  { project: 'vyasa', name: 'Vyasa', image: '/assets/img/all-images/vyasa-img5.png' },

  // Agastya Project
  { project: 'agastya', name: 'Agastya', image: '/assets/img/all-images/agastya-img1.png' },
  { project: 'agastya', name: 'Agastya', image: '/assets/img/all-images/agastya-img2.png' },
  { project: 'agastya', name: 'Agastya', image: '/assets/img/all-images/agastya-img3.png' },
  { project: 'agastya', name: 'Agastya', image: '/assets/img/all-images/agastya-img4.png' },
  { project: 'agastya', name: 'Agastya', image: '/assets/img/all-images/agastya-img5.png' },
  { project: 'agastya', name: 'Agastya', image: '/assets/img/all-images/agastya-img6.png' },

  // Anahata Project
  { project: 'anahata', name: 'Anahata', image: '/assets/img/aerial-view-bg.jpg' },
  { project: 'anahata', name: 'Anahata', image: '/assets/img/all-images/anahata-img2.png' },
  { project: 'anahata', name: 'Anahata', image: '/assets/img/all-images/anahata-img3.png' },
  { project: 'anahata', name: 'Anahata', image: '/assets/img/all-images/anahata-img4.png' },
  { project: 'anahata', name: 'Anahata', image: '/assets/img/all-images/anahata-img5.png' },
  { project: 'anahata', name: 'Anahata', image: '/assets/img/all-images/anahata-img6.png' }
];

// Core Benefits
const coreBenefits = [
  {
    icon: '/img/icons/luxury.png',
    title: 'Luxurious Living Spaces',
    description: 'Elegant and modern homes designed with high-quality materials and finishes.'
  },
  {
    icon: '/img/icons/eco.png',
    title: 'Eco-Friendly Practices',
    description: 'Sustainable construction methods and natural materials promote environmental responsibility.'
  },
  {
    icon: '/img/icons/time.png',
    title: 'Timely Delivery',
    description: 'Commitment to completing projects on schedule without compromising on quality.'
  },
  {
    icon: '/img/icons/service.png',
    title: 'Exceptional Service',
    description: 'Dedicated customer support and professional guidance throughout the home-buying process.'
  }
];

// Home Page
router.get('/', (req, res) => {
  res.render('index', {
    title: 'Ishtika Homes - Crafting Homes, Inspiring Lifestyles',
    currentPage: 'home',
    companyInfo,
    projects,
    featuredProjects,
    testimonials
  });
});

// About Page
router.get('/about', (req, res) => {
  res.render('about', {
    title: 'About Us - Ishtika Homes',
    currentPage: 'about',
    companyInfo,
    coreBenefits,
    testimonials
  });
});

// Projects Page
router.get('/projects', (req, res) => {
  res.render('projects', {
    title: 'Our Projects - Ishtika Homes',
    currentPage: 'projects',
    companyInfo,
    projects
  });
});

// Gallery Page
router.get('/gallery', (req, res) => {
  res.render('gallery', {
    title: 'Gallery - Ishtika Homes',
    currentPage: 'gallery',
    companyInfo,
    projects,
    galleryImages
  });
});

// Contact Page
router.get('/contact', (req, res) => {
  res.render('contact', {
    title: 'Contact Us - Ishtika Homes',
    currentPage: 'contact',
    companyInfo,
    success: req.query.success,
    error: req.query.error
  });
});

// Privacy Policy Page
router.get('/policy', (req, res) => {
  res.render('policy', {
    title: 'Privacy Policy - Ishtika Homes',
    currentPage: 'policy',
    companyInfo
  });
});

// Blog Page
router.get('/blog', (req, res) => {
  res.render('blog', {
    title: 'Blog - Real Estate Insights | Ishtika Homes',
    currentPage: 'blog',
    companyInfo
  });
});

// Anahata Project Detail Page
router.get('/projects/anahata', (req, res) => {
  res.render('anahata', {
    title: 'Anahata - Ishtika Homes',
    currentPage: 'projects',
    companyInfo
  });
});

// Vashishta Project Detail Page
router.get('/projects/vashishta', (req, res) => {
  res.render('vashishta', {
    title: 'Vashishta - Ishtika Homes',
    currentPage: 'projects',
    companyInfo
  });
});

// Krishna Project Detail Page
router.get('/projects/krishna', (req, res) => {
  res.render('krishna', {
    title: 'Krishna - Ishtika Homes',
    currentPage: 'projects',
    companyInfo
  });
});

// Naadam Project Detail Page
router.get('/projects/naadam', (req, res) => {
  res.render('naadam', {
    title: 'Naadam - Ishtika Homes',
    currentPage: 'projects',
    companyInfo
  });
});

// Vyasa Project Detail Page
router.get('/projects/vyasa', (req, res) => {
  res.render('vyasa', {
    title: 'Vyasa - Ishtika Homes',
    currentPage: 'projects',
    companyInfo
  });
});

// Agastya Project Detail Page
router.get('/projects/agastya', (req, res) => {
  res.render('agastya', {
    title: 'Agastya - Ishtika Homes',
    currentPage: 'projects',
    companyInfo
  });
});

// Contact Form Submission
router.post('/contact', async (req, res) => {
  const { name, name2, email, phone, subject, message } = req.body;

  try {
    // Concatenate first name and last name
    const fullName = `${name} ${name2 || ''}`.trim();

    // Insert data into Supabase flatrix_leads table
    const { data, error: supabaseError } = await supabase
      .from('flatrix_leads')
      .insert([
        {
          name: fullName,
          email: email,
          phone: phone,
          source: 'Ishtika direct website',
          project_name: subject || null,
          notes: message
        }
      ]);

    if (supabaseError) {
      console.error('Supabase error:', supabaseError);
      throw supabaseError;
    }

    console.log('Lead saved to Supabase successfully:', data);

    // Configure email transport (keep existing email functionality)
    if (process.env.SMTP_HOST && process.env.SMTP_USER) {
      const transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST,
        port: process.env.SMTP_PORT,
        secure: false,
        auth: {
          user: process.env.SMTP_USER,
          pass: process.env.SMTP_PASS
        }
      });

      // Email content
      const mailOptions = {
        from: process.env.SMTP_FROM,
        to: process.env.SMTP_TO,
        subject: `New Contact Form Submission: ${subject || 'No Subject'}`,
        html: `
          <h2>New Contact Form Submission</h2>
          <p><strong>Name:</strong> ${fullName}</p>
          <p><strong>Email:</strong> ${email}</p>
          <p><strong>Phone:</strong> ${phone}</p>
          <p><strong>Subject:</strong> ${subject || 'N/A'}</p>
          <p><strong>Message:</strong></p>
          <p>${message}</p>
        `
      };

      // Send email (optional - won't fail if not configured)
      try {
        await transporter.sendMail(mailOptions);
        console.log('Email sent successfully');
      } catch (emailError) {
        console.error('Email sending failed (non-critical):', emailError.message);
      }
    }

    res.redirect('/contact?success=1');
  } catch (error) {
    console.error('Error processing contact form:', error);
    res.redirect('/contact?error=1');
  }
});

module.exports = router;
