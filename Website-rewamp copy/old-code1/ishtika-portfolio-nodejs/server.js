const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Company Information (needed for error pages)
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
  }
};

// Middleware
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, 'public')));

// Set EJS as templating engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Static files - using express.static for Vercel serverless compatibility
// Files will be included in the function via includeFiles in vercel.json

// Routes
const indexRoutes = require('./routes/index');
const apiRoutes = require('./routes/api');

app.use('/', indexRoutes);
app.use('/api', apiRoutes);

// 404 Handler
app.use((req, res) => {
  res.status(404).render('404', {
    title: 'Page Not Found',
    currentPage: '404',
    companyInfo
  });
});

// Error Handler
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).render('error', {
    title: 'Server Error',
    error: err.message,
    currentPage: 'error',
    companyInfo
  });
});

// Start Server (only when running locally, not in Vercel)
if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Ishtika Homes Portfolio Server running on http://localhost:${PORT}`);
  });
}

// Export for Vercel serverless
module.exports = app;
