# EDC Clinical Trials System

A comprehensive Electronic Data Capture (EDC) system designed for clinical trials management with 21 CFR Part 11 compliance.

## Features

- **Study Management**: Create and manage clinical studies with phase-based control
- **Form Builder**: Design eCRF forms with validation rules and edit checks
- **UAT Workspace**: Test and validate forms before production deployment
- **Site Data Entry**: Role-based data entry with real-time validation
- **Query Management**: Automated and manual query generation with workflow
- **Audit Trail**: Complete 21 CFR Part 11 compliant audit logging
- **Data Export**: Export in multiple formats including CDISC SDTM/ADaM
- **Security**: Role-based access control with MFA support

## Technology Stack

- **Backend**: Node.js, Express.js
- **Database**: PostgreSQL with Sequelize ORM
- **Authentication**: JWT with bcrypt
- **Security**: Helmet, CORS, rate limiting
- **Logging**: Winston
- **Testing**: Jest

## Prerequisites

- Node.js (v14 or higher)
- PostgreSQL (v12 or higher)
- npm or yarn

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd edc_project
```

2. Install dependencies:
```bash
npm install
```

3. Set up PostgreSQL databases:
```sql
CREATE DATABASE edc_clinical_trials;
CREATE DATABASE edc_audit_trails;
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your database credentials and other settings
```

5. Run database migrations:
```bash
npm run migrate
```

6. Start the server:
```bash
# Development mode
npm run dev

# Production mode
npm start
```

## API Endpoints

### Authentication
- `POST /api/auth/register` - Register new user
- `POST /api/auth/login` - User login
- `POST /api/auth/logout` - User logout
- `POST /api/auth/change-password` - Change password
- `POST /api/auth/forgot-password` - Request password reset
- `POST /api/auth/reset-password/:token` - Reset password

### Health Check
- `GET /health` - System health status
- `GET /api/version` - API version info

## User Roles

1. **STUDY_ADMIN**: Full system administration
2. **STUDY_DESIGNER**: Create and manage forms
3. **CRC (Clinical Research Coordinator)**: Data entry
4. **PI (Principal Investigator)**: Data entry and approval
5. **DATA_MANAGER**: Review and query management
6. **CRA_MONITOR**: Site monitoring and queries
7. **READ_ONLY**: View-only access
8. **AUDITOR**: Compliance and audit review

## Study Phases

1. **STARTUP**: Initial setup, limited data entry
2. **CONDUCT**: Active data collection
3. **CLOSEOUT**: Final review and database lock

## Compliance

This system is designed to meet:
- 21 CFR Part 11 (Electronic Records and Signatures)
- Good Clinical Practice (GCP)
- GDPR (configurable)
- HIPAA (configurable)

## Security Features

- Password complexity requirements
- Account lockout after failed attempts
- Session management
- Audit trail for all actions
- Electronic signatures
- Data encryption at rest and in transit
- Role-based access control
- Multi-factor authentication (optional)

## Development

### Running Tests
```bash
npm test
```

### Linting
```bash
npm run lint
npm run lint:fix
```

### Database Management
```bash
# Run migrations
npm run migrate

# Undo last migration
npm run migrate:undo

# Create new migration
npm run migrate:create --name=migration-name
```

## Deployment

1. Set NODE_ENV to production
2. Configure production database
3. Set secure JWT secrets
4. Enable SSL/TLS
5. Configure rate limiting
6. Set up monitoring
7. Configure backup strategy

## License

Proprietary - All rights reserved

## Support

For support, please contact the development team.