# Quick Start Guide

## 🚀 Get Started in 3 Steps

### Step 1: Install Dependencies

```bash
# Install backend dependencies
cd backend
npm install

# Install frontend dependencies
cd ../frontend
npm install
```

### Step 2: Import Your Excel Data (Optional)

```bash
# From the root directory
cd backend
node importData.js
```

This will create a default admin user:
- **Email**: admin@sereneliving.com
- **Password**: admin123

### Step 3: Start the Application

**Terminal 1 - Backend:**
```bash
cd backend
npm start
```

**Terminal 2 - Frontend:**
```bash
cd frontend
npm run dev
```

### Or use the automated start script:

```bash
# From the root directory
./start.sh
```

## 🌐 Access the Portal

Open your browser and visit: **http://localhost:3002**

Login with:
- Email: `admin@sereneliving.com`
- Password: `admin123`

## 📋 What You Can Do

### 1. Dashboard
- View occupancy statistics
- Track rent collection
- See recent payments

### 2. Rooms
- Add new rooms
- Manage beds
- View room occupancy

### 3. Tenants
- Add tenant details
- Assign beds
- Track deposits and rent

### 4. Rent Collection
- Generate monthly rent
- Record payments
- Track pending amounts

## 🎯 Quick Tips

- **Generate Monthly Rent**: Click "Generate Rent" in Rent Collection to auto-create entries for all active tenants
- **Add Rooms First**: Before adding tenants, create rooms and beds
- **Vacant Beds**: Only vacant beds appear in tenant assignment dropdown
- **Monthly View**: Use month/year filters in Rent Collection to view specific periods

## 🔧 Troubleshooting

**Port Already in Use?**
- Backend: Edit `backend/.env` and change PORT
- Frontend: Edit `frontend/vite.config.js` and change server port

**Need to Reset?**
- Delete `backend/database.db`
- Run `node importData.js` again

## 📞 Need Help?

Check the main README.md for detailed documentation.

---

Happy Managing! 🏠
