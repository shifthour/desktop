# Serene Living - PG Management Portal

A comprehensive web-based PG (Paying Guest) management system to efficiently manage rooms, tenants, rent collection, and generate reports.

## Features

- 🏠 **Room Management**: Create and manage rooms with multiple beds
- 👥 **Tenant Management**: Track tenant information, deposits, and joining dates
- 💰 **Rent Collection**: Monthly rent tracking with payment modes and status
- 📊 **Dashboard**: Real-time statistics and occupancy rates
- 📈 **Reports**: Monthly rent collection reports
- 🔐 **Authentication**: Secure login system
- 📱 **Responsive Design**: Works on desktop, tablet, and mobile devices
- 📥 **Excel Import**: Import existing data from Excel spreadsheets

## Tech Stack

### Backend
- Node.js + Express
- SQLite Database (better-sqlite3)
- JWT Authentication
- bcryptjs for password hashing

### Frontend
- React 18
- React Router for navigation
- Tailwind CSS for styling
- Axios for API calls
- Lucide React for icons

## Prerequisites

- Node.js (v16 or higher)
- npm or yarn

## Installation

### 1. Clone or navigate to the project directory

```bash
cd Sukesh-Serene
```

### 2. Install Backend Dependencies

```bash
cd backend
npm install
```

### 3. Install Frontend Dependencies

```bash
cd ../frontend
npm install
```

## Setup & Running

### Option 1: Import Existing Excel Data

If you have the SereneLiving_2025.xlsx file:

```bash
# From the backend directory
cd backend
node importData.js
```

This will:
- Create the database schema
- Import rooms, beds, tenants, and rent payments
- Create a default admin user

**Default Login Credentials:**
- Email: `admin@sereneliving.com`
- Password: `admin123`

### Option 2: Start Fresh (Without Excel Import)

The database will be created automatically when you start the backend server.

You'll need to:
1. Register a user via the API or create one manually
2. Add rooms, tenants, and rent data through the web interface

### Running the Application

#### Start Backend Server

```bash
# From the backend directory
cd backend
npm start

# For development with auto-reload
npm run dev
```

The backend will run on `http://localhost:5000`

#### Start Frontend Development Server

```bash
# From the frontend directory (in a new terminal)
cd frontend
npm run dev
```

The frontend will run on `http://localhost:3002`

### Access the Application

Open your browser and navigate to: `http://localhost:3002`

Login with:
- Email: `admin@sereneliving.com`
- Password: `admin123`

## Project Structure

```
Sukesh-Serene/
├── backend/
│   ├── src/
│   │   ├── config/          # Database configuration
│   │   ├── controllers/     # Request handlers
│   │   ├── middleware/      # Authentication middleware
│   │   ├── models/          # Data models
│   │   ├── routes/          # API routes
│   │   ├── utils/           # Utility functions (Excel import)
│   │   └── server.js        # Express server setup
│   ├── importData.js        # Excel import script
│   ├── package.json
│   └── .env                 # Environment variables
│
├── frontend/
│   ├── src/
│   │   ├── components/      # Reusable components
│   │   ├── context/         # React context (Auth)
│   │   ├── pages/           # Page components
│   │   ├── services/        # API service layer
│   │   ├── App.jsx          # Main app component
│   │   └── main.jsx         # Entry point
│   ├── package.json
│   └── index.html
│
└── README.md
```

## API Endpoints

### Authentication
- `POST /api/auth/register` - Register new user
- `POST /api/auth/login` - Login
- `GET /api/auth/me` - Get current user

### Dashboard
- `GET /api/dashboard/stats` - Get dashboard statistics
- `GET /api/dashboard/report` - Get monthly report

### Rooms
- `GET /api/rooms` - Get all rooms
- `GET /api/rooms/:id` - Get room by ID
- `POST /api/rooms` - Create room
- `PUT /api/rooms/:id` - Update room
- `DELETE /api/rooms/:id` - Delete room
- `GET /api/rooms/beds` - Get all beds

### Tenants
- `GET /api/tenants` - Get all tenants
- `GET /api/tenants/:id` - Get tenant by ID
- `POST /api/tenants` - Create tenant
- `PUT /api/tenants/:id` - Update tenant
- `DELETE /api/tenants/:id` - Delete tenant
- `POST /api/tenants/:id/vacate` - Mark tenant as vacated

### Rent Collection
- `GET /api/rent` - Get rent payments (with month/year filter)
- `POST /api/rent` - Create rent payment
- `PUT /api/rent/:id` - Update rent payment
- `DELETE /api/rent/:id` - Delete rent payment
- `POST /api/rent/generate` - Generate monthly rent for all tenants

## Usage Guide

### Adding a Room
1. Navigate to "Rooms" from the sidebar
2. Click "Add Room"
3. Enter room number, sharing type, and total beds
4. The system will automatically create bed entries

### Adding a Tenant
1. Navigate to "Tenants" from the sidebar
2. Click "Add Tenant"
3. Fill in tenant details including:
   - Name and mobile number (required)
   - Bed assignment (select from available vacant beds)
   - Rent amount, deposit, joining date
   - Aadhar, address, and other details
4. Click "Create"

### Recording Rent Payments
1. Navigate to "Rent Collection" from the sidebar
2. Select the month and year
3. Click "Generate Rent" to create entries for all active tenants
4. Click edit on any entry to record payments
5. Update the paid amount, payment date, and mode

### Viewing Dashboard
- The dashboard shows:
  - Total rooms and beds
  - Occupancy statistics
  - Current month rent collection status
  - Recent payments
  - Collection rate

## Database Schema

### Tables
- **users** - Admin users with authentication
- **rooms** - Room information
- **beds** - Individual beds in rooms
- **tenants** - Tenant details and assignments
- **rent_payments** - Monthly rent tracking
- **expenses** - Expense tracking (future feature)

## Environment Variables

Backend `.env` file:
```
PORT=5000
JWT_SECRET=your_secret_key_here
NODE_ENV=development
DB_PATH=./database.db
```

## Building for Production

### Backend
```bash
cd backend
npm start
```

### Frontend
```bash
cd frontend
npm run build
```

The built files will be in `frontend/dist` directory.

## Troubleshooting

### Port Already in Use
If port 5000 or 3000 is already in use:
- Backend: Change `PORT` in `backend/.env`
- Frontend: Change port in `frontend/vite.config.js`

### Database Issues
Delete `backend/database.db` and restart the server to reset the database.

### Import Issues
Ensure the Excel file `SereneLiving_2025.xlsx` is in the root directory when running the import script.

## Security Notes

⚠️ **Important**:
- Change the default admin password after first login
- Update the `JWT_SECRET` in production
- Use environment variables for sensitive data
- Enable HTTPS in production

## Future Enhancements

- Expense tracking and management
- SMS/Email notifications for rent reminders
- Receipt generation (PDF)
- Automated backup system
- Multi-property support
- Advanced reporting and analytics
- Mobile app

## Support

For issues or questions, please check the documentation or create an issue.

## License

Private - Internal Use Only

---

**Serene Living PG Management Portal** - Simplifying PG Management
