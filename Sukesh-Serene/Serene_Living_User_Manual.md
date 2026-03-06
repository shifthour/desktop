# SERENE LIVING PG MANAGEMENT SYSTEM
## User Manual

---

**Version:** 1.0
**Date:** October 2025
**Application URL:** https://serene-living.vercel.app

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Getting Started](#2-getting-started)
3. [Admin Portal](#3-admin-portal)
   - 3.1 [Login](#31-admin-login)
   - 3.2 [Dashboard](#32-dashboard)
   - 3.3 [PG Properties Management](#33-pg-properties-management)
   - 3.4 [Room Management](#34-room-management)
   - 3.5 [Tenant Management](#35-tenant-management)
   - 3.6 [Payment Management](#36-payment-management)
   - 3.7 [Payment Review](#37-payment-review)
   - 3.8 [Expense Management](#38-expense-management)
4. [Tenant Portal](#4-tenant-portal)
   - 4.1 [Tenant Login](#41-tenant-login)
   - 4.2 [First Login & Password Change](#42-first-login--password-change)
   - 4.3 [Tenant Dashboard](#43-tenant-dashboard)
   - 4.4 [Payment Submission](#44-payment-submission)
5. [Troubleshooting](#5-troubleshooting)
6. [Support & Contact](#6-support--contact)

---

## 1. Introduction

### 1.1 About Serene Living PG Management System

Serene Living PG Management System is a comprehensive web-based application designed to streamline the management of Paying Guest (PG) accommodations. The system provides two distinct portals:

- **Admin Portal**: For property managers to manage properties, rooms, tenants, payments, and expenses
- **Tenant Portal**: For tenants to view their payment status and submit payment proofs

### 1.2 Key Features

**Admin Features:**
- Multi-property management
- Room allocation and tracking
- Tenant registration and management
- Payment tracking and rent collection
- Payment proof review and verification
- Expense tracking and reporting
- Real-time dashboard with analytics

**Tenant Features:**
- Secure login with personalized credentials
- View current rent status
- Submit payment proofs with screenshots
- Track payment history
- Change password functionality

### 1.3 System Requirements

- Modern web browser (Chrome, Firefox, Safari, Edge)
- Internet connection
- Device: Desktop, Laptop, Tablet, or Mobile phone

---

## 2. Getting Started

### 2.1 Access URLs

**Admin Portal:** https://serene-living.vercel.app
**Tenant Portal:** https://serene-living.vercel.app/tenant/login

### 2.2 Default Admin Credentials

**Email:** admin@sereneliving.com
**Password:** admin123

⚠️ **Important:** Please change the default password after first login for security purposes.

---

## 3. Admin Portal

### 3.1 Admin Login

**Steps to Login:**

1. Navigate to https://serene-living.vercel.app
2. Enter your admin email address
3. Enter your password
4. Click the "Login" button

**Login Page Elements:**
- Serene Living logo and branding
- Email input field
- Password input field (masked for security)
- Login button

**After Successful Login:**
- You will be redirected to the Dashboard
- Your session will remain active until you logout

---

### 3.2 Dashboard

The Dashboard provides a comprehensive overview of your PG business at a glance.

**Dashboard Components:**

1. **Summary Cards**
   - **Total Properties**: Shows the count of all registered PG properties
   - **Total Rooms**: Displays the total number of rooms across all properties
   - **Active Tenants**: Number of currently active tenants
   - **Total Revenue**: Sum of all collected payments for the current month

2. **Recent Activity Section**
   - Recent tenant registrations
   - Latest payment submissions
   - Pending payment approvals
   - Recent expense entries

3. **Navigation Menu** (Left Sidebar)
   - Dashboard
   - PG Properties
   - Rooms
   - Tenants
   - Payments
   - Payment Review
   - Expenses
   - Logout button

**Quick Actions from Dashboard:**
- Click on any card to view detailed information
- Use the navigation menu to access specific sections

---

### 3.3 PG Properties Management

Manage all your PG properties from a centralized location.

#### 3.3.1 Adding a New Property

**Steps:**

1. Click on "PG Properties" in the left navigation menu
2. Click the "+ Create New PG Property" button
3. Fill in the property details:
   - **Property Name**: Enter a unique name (e.g., "Serene Heights", "Green Villa PG")
   - **Location**: Enter the complete address
   - **Total Capacity**: Enter maximum number of tenants the property can accommodate
4. Click "Create" button
5. A success message will appear, and the property will be added to the list

**Processing Indicator:**
- While creating, you'll see "Creating..." with a loading spinner
- This ensures you know the system is processing your request

#### 3.3.2 Viewing Properties

The properties page displays all registered properties in a card layout showing:
- Property Name
- Location
- Total Capacity
- Edit and Delete options

#### 3.3.3 Editing a Property

**Steps:**

1. Locate the property you want to edit
2. Click the "Edit" button on the property card
3. Modify the required fields
4. Click "Update" button
5. Changes will be saved immediately

**Note:** The update button will show "Updating..." while processing.

#### 3.3.4 Deleting a Property

**Steps:**

1. Click the "Delete" button on the property card
2. Confirm the deletion when prompted
3. The property will be removed from the system

**Processing Indicator:**
- The delete button shows "Deleting..." with a spinner during the process

⚠️ **Warning:** Deleting a property will affect associated rooms and tenant assignments. Ensure all tenants are moved before deletion.

---

### 3.4 Room Management

Manage individual rooms within your PG properties.

#### 3.4.1 Adding a New Room

**Steps:**

1. Click on "Rooms" in the left navigation menu
2. Click the "+ Create New Room" button
3. Fill in the room details:
   - **PG Property**: Select from dropdown list of registered properties
   - **Room Number**: Enter unique room identifier (e.g., "101", "A-1", "First Floor Room 1")
   - **Rent Amount**: Enter monthly rent in rupees
   - **Occupancy Status**: Select from:
     - Vacant
     - Occupied
     - Under Maintenance
4. Click "Create" button

**Field Validations:**
- Room number must be unique within the property
- Rent amount must be a positive number
- All fields are mandatory

#### 3.4.2 Viewing Rooms

The rooms page displays:
- Property name
- Room number
- Monthly rent
- Current occupancy status
- Edit and Delete options

**Status Indicators:**
- Vacant: Room is available for new tenant
- Occupied: Room is currently occupied
- Under Maintenance: Room is temporarily unavailable

#### 3.4.3 Editing Room Details

**Steps:**

1. Locate the room you want to edit
2. Click the "Edit" button
3. Modify the required fields (property, room number, rent, status)
4. Click "Update" button

**Common Use Cases:**
- Updating rent amount
- Changing occupancy status
- Correcting room number
- Moving room to different property

#### 3.4.4 Deleting a Room

**Steps:**

1. Click the "Delete" button on the room entry
2. Confirm deletion
3. Room will be removed from the system

⚠️ **Note:** Ensure no active tenant is assigned to the room before deletion.

---

### 3.5 Tenant Management

Comprehensive tenant information management and registration.

#### 3.5.1 Registering a New Tenant

**Steps:**

1. Click on "Tenants" in the navigation menu
2. Click "+ Create New Tenant" button
3. Fill in the tenant registration form:

   **Personal Information:**
   - **Name**: Full name of the tenant
   - **Email**: Valid email address (will be used for login)
   - **Phone**: 10-digit mobile number
   - **ID Proof**: Upload Aadhaar card or other government ID

   **Accommodation Details:**
   - **PG Property**: Select the property from dropdown
   - **Room**: Select assigned room from dropdown
   - **Rent Amount**: Enter monthly rent (auto-filled based on room)
   - **Due Date**: Select monthly due date (1-31)

   **Emergency Contact:**
   - **Emergency Contact Name**: Full name
   - **Emergency Contact Phone**: 10-digit mobile number

4. Click "Create" button

**After Tenant Creation:**
- Tenant receives auto-generated login credentials
- Default password: "Welcome123"
- Tenant must change password on first login

**Validation Rules:**
- Email must be unique across all tenants
- Phone number must be unique
- Room can only have one active tenant
- All mandatory fields must be filled

#### 3.5.2 Viewing Tenant List

The tenant list displays:
- Tenant name
- Email and phone number
- Assigned property and room
- Monthly rent amount
- Due date
- Current status (Active/Inactive)
- Edit and Delete options

**Status Meaning:**
- **Active**: Currently residing in the PG
- **Inactive**: No longer residing (soft deleted)

#### 3.5.3 Editing Tenant Information

**Steps:**

1. Click "Edit" button next to the tenant
2. Modify required information
3. Click "Update" button

**Editable Fields:**
- Personal information
- Room assignment
- Rent amount
- Due date
- Emergency contact details

**Note:** Email cannot be changed as it's the login identifier.

#### 3.5.4 Removing a Tenant

**Steps:**

1. Click "Delete" button next to the tenant
2. Confirm the action
3. Tenant status changes to "Inactive"

**Important Points:**
- This is a soft delete (data is retained)
- Room becomes available for new tenant
- Tenant loses access to the tenant portal
- Payment history is preserved
- Email and phone can be reused for new registration

---

### 3.6 Payment Management

Track and manage rent payments for all tenants.

#### 3.6.1 Recording a New Payment

**Steps:**

1. Click on "Payments" in the navigation menu
2. Click "+ Create New Payment" button
3. Fill in payment details:
   - **Tenant**: Select from dropdown of active tenants
   - **Amount**: Enter payment amount received
   - **Payment Date**: Select the date payment was received
   - **Payment Method**: Choose from:
     - Cash
     - Online Transfer
     - UPI
     - Cheque
     - Other
   - **Status**: Select current status:
     - Paid
     - Pending
     - Overdue
   - **Notes**: Add any additional information (optional)

4. Click "Create" button

**Auto-filled Information:**
- Rent amount displays the tenant's monthly rent
- Current date is selected by default

#### 3.6.2 Viewing Payment Records

The payments page displays a comprehensive list showing:
- Tenant name and room
- Payment amount
- Payment date
- Payment method
- Payment status
- Notes
- Edit and Delete options

**Status Color Coding:**
- **Paid**: Green indicator - Payment received and recorded
- **Pending**: Yellow/Orange indicator - Payment expected but not received
- **Overdue**: Red indicator - Payment past due date

#### 3.6.3 Editing Payment Records

**Steps:**

1. Locate the payment record
2. Click "Edit" button
3. Modify required fields
4. Click "Update" button

**Common Editing Scenarios:**
- Correcting payment amount
- Updating payment method
- Changing payment status
- Adding notes

#### 3.6.4 Deleting Payment Records

**Steps:**

1. Click "Delete" button on the payment record
2. Confirm deletion
3. Record will be permanently removed

⚠️ **Use with caution:** This action cannot be undone.

---

### 3.7 Payment Review

Review and verify payment proofs submitted by tenants through the tenant portal.

#### 3.7.1 Accessing Payment Review Section

1. Click on "Payment Review" in the navigation menu
2. View list of pending payment submissions

#### 3.7.2 Viewing Submitted Payment Proofs

The payment review page displays:
- Tenant name and room details
- Submission date and time
- Payment amount claimed
- Payment screenshot (uploaded by tenant)
- Transaction ID (if provided)
- Payment method
- Notes from tenant
- Approval status (Pending/Approved/Rejected)

#### 3.7.3 Reviewing a Payment Submission

**Steps:**

1. Click on a pending payment submission
2. View the uploaded payment screenshot:
   - Click on the image to view full size
   - Check transaction details
   - Verify amount matches claimed payment
3. Review transaction ID if provided
4. Check payment date
5. Read any notes from the tenant

#### 3.7.4 Approving a Payment

**Steps:**

1. After verifying the payment proof
2. Click "Approve" button
3. Payment status changes to "Approved"
4. A payment record is automatically created in the Payments section

**What Happens After Approval:**
- Payment is marked as verified
- Tenant can see approved status in their portal
- Payment appears in the Payments list
- Dashboard statistics are updated

#### 3.7.5 Rejecting a Payment

**Steps:**

1. If payment proof is invalid or unclear
2. Click "Reject" button
3. Optionally add rejection reason
4. Status changes to "Rejected"

**Rejection Scenarios:**
- Screenshot is unclear or unreadable
- Amount doesn't match
- Transaction appears fraudulent
- Wrong payment month

**After Rejection:**
- Tenant is notified
- Tenant can resubmit with correct proof
- No payment record is created

#### 3.7.6 Filtering Payment Submissions

Use filters to view:
- All submissions
- Pending reviews only
- Approved payments
- Rejected submissions
- Specific date range
- Specific tenant

---

### 3.8 Expense Management

Track and manage all property-related expenses.

#### 3.8.1 Adding a New Expense

**Steps:**

1. Click on "Expenses" in the navigation menu
2. Click "+ Create New Expense" button
3. Fill in expense details:
   - **Property**: Select the property from dropdown
   - **Category**: Choose expense category:
     - Maintenance
     - Utilities (Electricity, Water)
     - Repairs
     - Cleaning
     - Security
     - Miscellaneous
   - **Amount**: Enter expense amount
   - **Date**: Select date of expense
   - **Description**: Add detailed description of the expense
   - **Vendor/Payee**: Name of person/company paid (optional)

4. Click "Create" button

**Processing Indicator:**
- Shows "Creating..." while adding the expense

#### 3.8.2 Viewing Expense Records

The expenses page displays:
- Property name
- Expense category
- Amount spent
- Date of expense
- Description
- Vendor/Payee name
- Edit and Delete options

**Summary Section:**
- Total expenses for current month
- Category-wise breakdown
- Property-wise breakdown

#### 3.8.3 Editing Expense Records

**Steps:**

1. Click "Edit" button on the expense
2. Modify required fields
3. Click "Update" button

**Common Edits:**
- Correcting amount
- Updating category
- Adding more details to description
- Changing date

#### 3.8.4 Deleting Expense Records

**Steps:**

1. Click "Delete" button
2. Confirm deletion
3. Expense is removed

**Note:** Shows "Deleting..." while processing.

#### 3.8.5 Expense Reports

Generate expense reports:
- Monthly expense summary
- Property-wise expense breakdown
- Category-wise analysis
- Year-to-date expenses

**Report Features:**
- View total expenses
- Compare month-over-month
- Identify expense trends
- Export data (if needed)

---

## 4. Tenant Portal

The tenant portal provides a simplified interface for tenants to manage their rent payments and view their account information.

### 4.1 Tenant Login

**Access URL:** https://serene-living.vercel.app/tenant/login

**Steps to Login:**

1. Navigate to the tenant login page
2. Enter your email address (provided by admin)
3. Enter your password
   - First-time login: Use default password "Welcome123"
   - Subsequent logins: Use your changed password
4. Click "Login" button

**Login Page Features:**
- Serene Living branding
- Email input field
- Password input field (masked)
- Login button
- Professional teal-themed design

**After Successful Login:**
- First-time users: Redirected to Change Password page
- Regular users: Redirected to Tenant Dashboard

---

### 4.2 First Login & Password Change

#### 4.2.1 First Time Login Process

**What Happens:**
1. System detects first login
2. Automatically redirects to password change page
3. Must change password before accessing dashboard

#### 4.2.2 Changing Password

**Steps:**

1. On the Change Password page:
   - **Current Password**: Enter "Welcome123" (or current password)
   - **New Password**: Enter your desired password
   - **Confirm New Password**: Re-enter new password for verification

2. Click "Change Password" button

**Password Requirements:**
- Minimum 6 characters recommended
- Should be memorable but secure
- Don't use easily guessable passwords

**After Password Change:**
- Success message displayed
- Redirected to Tenant Dashboard
- Use new password for future logins

**Accessing Password Change Later:**
- Available from tenant dashboard
- Update password anytime for security

---

### 4.3 Tenant Dashboard

The Tenant Dashboard is your central hub for viewing rent information and managing payments.

#### 4.3.1 Dashboard Overview

**Information Displayed:**

1. **Personal Information Section**
   - Your name
   - Email address
   - Phone number
   - Room number
   - Property name

2. **Rent Information**
   - Monthly rent amount
   - Rent due date
   - Current payment status

3. **Current Month Rent Status**
   - **Paid**: Green indicator showing payment received
   - **Pending**: Yellow/Orange indicator showing payment pending
   - **Overdue**: Red indicator if payment is past due

4. **Payment History**
   - List of previous payments
   - Payment dates
   - Amounts paid
   - Payment methods used
   - Payment status

5. **Pending Submissions**
   - Payment proofs awaiting approval
   - Submission date
   - Current review status

#### 4.3.2 Navigation Options

From the dashboard, you can:
- View payment history
- Submit new payment proof
- Change password
- View personal information
- Logout

---

### 4.4 Payment Submission

Submit proof of rent payment for admin verification.

#### 4.4.1 How to Submit Payment Proof

**Steps:**

1. From Tenant Dashboard, click "Submit Payment" button
2. Fill in the payment submission form:

   **Required Information:**
   - **Payment Amount**: Enter the amount you paid
   - **Payment Date**: Select the date you made payment
   - **Payment Method**: Choose from:
     - Online Transfer
     - UPI
     - Cash
     - Cheque
     - Other

   **Payment Proof:**
   - **Screenshot**: Upload payment screenshot
     - Click "Choose File" or "Upload" button
     - Select screenshot from your device
     - Supported formats: JPG, PNG, PDF
     - Maximum file size: 10MB

   **Additional Details:**
   - **Transaction ID**: Enter transaction reference number (if applicable)
   - **Notes**: Add any additional information

3. Click "Submit" button

**Submission Guidelines:**

**For Online/UPI Payments:**
- Take clear screenshot of successful transaction
- Ensure transaction ID is visible
- Amount should be clearly shown
- Date should be visible
- Screenshot should not be cropped or edited

**For Cash Payments:**
- Inform admin in advance
- Take photo of receipt (if provided)
- Add clear notes about cash payment

**For Cheque Payments:**
- Photo of cheque leaf
- Include cheque number in notes

#### 4.4.2 Tracking Submission Status

**Status Types:**

1. **Pending**
   - Submission received
   - Awaiting admin review
   - Status shows in yellow/orange
   - Typically reviewed within 24-48 hours

2. **Approved**
   - Payment verified by admin
   - Status shows in green
   - Payment recorded in system
   - Rent status updated to "Paid"

3. **Rejected**
   - Payment proof was unclear or invalid
   - Status shows in red
   - Check rejection reason (if provided)
   - Resubmit with correct information

#### 4.4.3 Viewing Submission History

**What You Can See:**
- All previous submissions
- Submission date and time
- Payment amount
- Current status
- Admin comments (if any)

**Where to Find:**
- Tenant Dashboard → "Payment History" section
- Shows both approved and pending submissions

#### 4.4.4 Resubmitting a Rejected Payment

**Steps:**

1. Check reason for rejection
2. Prepare correct payment proof
3. Click "Submit Payment" again
4. Fill in details accurately
5. Upload clear screenshot
6. Add clarification in notes section
7. Submit for review

---

## 5. Troubleshooting

### 5.1 Common Login Issues

**Problem: Cannot login to admin portal**

**Solutions:**
- Verify you're using correct email: admin@sereneliving.com
- Check password: Default is admin123
- Ensure caps lock is OFF
- Clear browser cache and cookies
- Try different browser
- Check internet connection

**Problem: Tenant cannot login**

**Solutions:**
- Verify email address is correct (check with admin)
- Use default password "Welcome123" for first login
- Check if account is active (contact admin)
- Clear browser cache
- Try different browser

### 5.2 Payment Submission Issues

**Problem: Cannot upload payment screenshot**

**Solutions:**
- Check file size (must be under 10MB)
- Verify file format (JPG, PNG, or PDF only)
- Try compressing the image
- Ensure stable internet connection
- Try different browser
- Check if file is corrupted

**Problem: Submission shows "pending" for too long**

**Solutions:**
- Wait 24-48 hours for review
- If urgent, contact admin directly
- Ensure screenshot was clear and readable
- Check if additional information was requested

### 5.3 General Issues

**Problem: Page not loading properly**

**Solutions:**
- Refresh the page (F5 or Ctrl+R)
- Clear browser cache
- Check internet connection
- Try incognito/private mode
- Update browser to latest version
- Try different browser

**Problem: Changes not saving**

**Solutions:**
- Check internet connection
- Look for error messages
- Wait for "Creating..." or "Updating..." to complete
- Don't close browser during processing
- Try submitting again if it fails

**Problem: Cannot see recent updates**

**Solutions:**
- Refresh the page
- Clear browser cache
- Logout and login again
- Check if operation completed successfully

### 5.4 Technical Support

If problems persist:
1. Note the error message (if any)
2. Take screenshot of the issue
3. Contact technical support
4. Provide detailed description of the problem
5. Mention what you were trying to do
6. Include your username/email

---

## 6. Support & Contact

### 6.1 Getting Help

For any issues, questions, or support needs:

**Technical Support:**
- Email: support@sereneliving.com
- Response Time: 24-48 hours
- Available: Monday - Saturday, 9 AM - 6 PM

**Admin Support:**
- Contact your property administrator
- Email: admin@sereneliving.com

### 6.2 Feedback & Suggestions

We value your feedback to improve the system:
- Share your experience
- Suggest new features
- Report bugs or issues
- Send feedback to: feedback@sereneliving.com

### 6.3 Important Notes

**Security Best Practices:**
- Never share your password
- Change default password immediately
- Logout after each session
- Don't access from public computers
- Report suspicious activity immediately

**Data Privacy:**
- Your personal information is secure
- Payment details are encrypted
- Data is not shared with third parties
- Regular security updates are applied

**System Updates:**
- System may undergo periodic maintenance
- Updates improve functionality and security
- Advance notice provided for scheduled maintenance
- Check for announcements on login page

---

## Appendix

### A. Glossary of Terms

**PG (Paying Guest)**: Accommodation where tenants pay rent to stay temporarily

**Admin Portal**: Management interface for property administrators

**Tenant Portal**: Interface for tenants to manage their account and payments

**Soft Delete**: Marking record as inactive instead of permanent deletion

**Payment Proof**: Screenshot or document showing payment transaction

**Due Date**: Date by which monthly rent must be paid

**Occupancy Status**: Current availability status of a room

**Transaction ID**: Unique reference number for payment transactions

### B. Quick Reference Guide

**Admin Quick Actions:**
- Add Property: PG Properties → Create New PG Property
- Add Room: Rooms → Create New Room
- Add Tenant: Tenants → Create New Tenant
- Record Payment: Payments → Create New Payment
- Review Payment: Payment Review → View pending submissions
- Add Expense: Expenses → Create New Expense

**Tenant Quick Actions:**
- Submit Payment: Dashboard → Submit Payment
- Change Password: Dashboard → Change Password
- View History: Dashboard → Payment History
- Check Status: Dashboard → Current Month Status

### C. Keyboard Shortcuts

**General:**
- F5: Refresh page
- Ctrl + R: Reload page
- Ctrl + F: Find on page
- Esc: Close modal/popup

**Form Actions:**
- Tab: Move to next field
- Shift + Tab: Move to previous field
- Enter: Submit form (when applicable)

---

**Document End**

*This document is subject to updates. Latest version available at application login page.*

**Version History:**
- Version 1.0 - October 2025 - Initial Release

---

© 2025 Serene Living PG Management System. All rights reserved.

