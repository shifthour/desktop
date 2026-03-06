const { sequelize, auditSequelize } = require('../config/database');
const User = require('./User');
const Study = require('./Study');
const Site = require('./Site');
const Subject = require('./Subject');
const Form = require('./Form');
const FormData = require('./FormData');
const Visit = require('./Visit');
const Query = require('./Query');
const AuditTrail = require('./AuditTrail');

// User associations
User.hasMany(Study, { as: 'managedStudies', foreignKey: 'studyManagerId' });
User.hasMany(Study, { as: 'dataManagerStudies', foreignKey: 'dataManagerId' });
User.hasMany(Study, { as: 'piStudies', foreignKey: 'primaryInvestigatorId' });
User.hasMany(Site, { as: 'piSites', foreignKey: 'principalInvestigatorId' });
User.hasMany(Site, { as: 'coordinatedSites', foreignKey: 'coordinatorId' });
User.hasMany(Form, { as: 'createdForms', foreignKey: 'createdBy' });
User.hasMany(Form, { as: 'modifiedForms', foreignKey: 'lastModifiedBy' });
User.hasMany(FormData, { as: 'enteredData', foreignKey: 'enteredBy' });
User.hasMany(FormData, { as: 'verifiedData', foreignKey: 'verifiedBy' });
User.hasMany(Query, { as: 'raisedQueries', foreignKey: 'raisedBy' });
User.hasMany(Query, { as: 'assignedQueries', foreignKey: 'assignedTo' });

// Study associations
Study.belongsTo(User, { as: 'primaryInvestigator', foreignKey: 'primaryInvestigatorId' });
Study.belongsTo(User, { as: 'studyManager', foreignKey: 'studyManagerId' });
Study.belongsTo(User, { as: 'dataManager', foreignKey: 'dataManagerId' });
Study.hasMany(Site, { foreignKey: 'studyId' });
Study.hasMany(Subject, { foreignKey: 'studyId' });
Study.hasMany(Form, { foreignKey: 'studyId' });
Study.hasMany(Visit, { foreignKey: 'studyId' });
Study.hasMany(FormData, { foreignKey: 'studyId' });
Study.hasMany(Query, { foreignKey: 'studyId' });

// Site associations
Site.belongsTo(Study, { foreignKey: 'studyId' });
Site.belongsTo(User, { as: 'principalInvestigator', foreignKey: 'principalInvestigatorId' });
Site.belongsTo(User, { as: 'coordinator', foreignKey: 'coordinatorId' });
Site.hasMany(Subject, { foreignKey: 'siteId' });
Site.hasMany(Visit, { foreignKey: 'siteId' });
Site.hasMany(FormData, { foreignKey: 'siteId' });
Site.hasMany(Query, { foreignKey: 'siteId' });

// Subject associations
Subject.belongsTo(Study, { foreignKey: 'studyId' });
Subject.belongsTo(Site, { foreignKey: 'siteId' });
Subject.hasMany(Visit, { foreignKey: 'subjectId' });
Subject.hasMany(FormData, { foreignKey: 'subjectId' });
Subject.hasMany(Query, { foreignKey: 'subjectId' });

// Form associations
Form.belongsTo(Study, { foreignKey: 'studyId' });
Form.belongsTo(User, { as: 'creator', foreignKey: 'createdBy' });
Form.belongsTo(User, { as: 'modifier', foreignKey: 'lastModifiedBy' });
Form.belongsTo(User, { as: 'promoter', foreignKey: 'promotedBy' });
Form.belongsTo(User, { as: 'uatTester', foreignKey: 'uatTestedBy' });
Form.belongsTo(User, { as: 'uatApprover', foreignKey: 'uatApprovedBy' });
Form.hasMany(FormData, { foreignKey: 'formId' });
Form.hasMany(Query, { foreignKey: 'formId' });

// FormData associations
FormData.belongsTo(Form, { foreignKey: 'formId' });
FormData.belongsTo(Subject, { foreignKey: 'subjectId' });
FormData.belongsTo(Visit, { foreignKey: 'visitId' });
FormData.belongsTo(Site, { foreignKey: 'siteId' });
FormData.belongsTo(Study, { foreignKey: 'studyId' });
FormData.belongsTo(User, { as: 'enteredByUser', foreignKey: 'enteredBy' });
FormData.belongsTo(User, { as: 'verifiedByUser', foreignKey: 'verifiedBy' });
FormData.belongsTo(User, { as: 'lockedByUser', foreignKey: 'lockedBy' });
FormData.hasMany(Query, { foreignKey: 'formDataId' });

// Visit associations
Visit.belongsTo(Study, { foreignKey: 'studyId' });
Visit.belongsTo(Subject, { foreignKey: 'subjectId' });
Visit.belongsTo(Site, { foreignKey: 'siteId' });
Visit.belongsTo(User, { as: 'creator', foreignKey: 'createdBy' });
Visit.belongsTo(User, { as: 'modifier', foreignKey: 'lastModifiedBy' });
Visit.hasMany(FormData, { foreignKey: 'visitId' });
Visit.hasMany(Query, { foreignKey: 'visitId' });

// Query associations
Query.belongsTo(Study, { foreignKey: 'studyId' });
Query.belongsTo(Site, { foreignKey: 'siteId' });
Query.belongsTo(Subject, { foreignKey: 'subjectId' });
Query.belongsTo(Visit, { foreignKey: 'visitId' });
Query.belongsTo(Form, { foreignKey: 'formId' });
Query.belongsTo(FormData, { foreignKey: 'formDataId' });
Query.belongsTo(User, { as: 'raisedByUser', foreignKey: 'raisedBy' });
Query.belongsTo(User, { as: 'assignedToUser', foreignKey: 'assignedTo' });
Query.belongsTo(User, { as: 'respondedByUser', foreignKey: 'respondedBy' });
Query.belongsTo(User, { as: 'reviewedByUser', foreignKey: 'reviewedBy' });
Query.belongsTo(User, { as: 'closedByUser', foreignKey: 'closedBy' });

const syncDatabase = async () => {
  try {
    await sequelize.authenticate();
    console.log('Main database connection established successfully.');
    
    await auditSequelize.authenticate();
    console.log('Audit database connection established successfully.');
    
    if (process.env.NODE_ENV === 'development') {
      await sequelize.sync({ alter: true });
      await auditSequelize.sync({ alter: true });
      console.log('Database synchronized successfully.');
    } else {
      await sequelize.sync();
      await auditSequelize.sync();
    }
  } catch (error) {
    console.error('Unable to connect to the database:', error);
    throw error; // Throw error instead of exiting process
  }
};

module.exports = {
  sequelize,
  auditSequelize,
  User,
  Study,
  Site,
  Subject,
  Form,
  FormData,
  Visit,
  Query,
  AuditTrail,
  syncDatabase
};