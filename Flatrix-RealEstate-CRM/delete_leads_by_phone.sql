-- SQL script to delete leads with specific phone numbers
-- Run this in Supabase SQL Editor

DELETE FROM flatrix_leads 
WHERE phone IN (
  '7602775968',
  '8055212665',
  '8839133525',
  '6237348232',
  '9003240893',
  '8756561086',
  '9529999419',
  '9986073952',
  '8500755077',
  '7996819521',
  '8918559652',
  '8805813647',
  '9566193762',
  '9448945404',
  '9900736456',
  '9036455447',
  '9585777609',
  '8233595800',
  '7736192779',
  '8271612153',
  '9744896620',
  '8197517946',
  '9663525530',
  '9559914102',
  '8546688922',
  '9342505636',
  '9972246536',
  '9600703996',
  '7996636423',
  '9448287408'
);

-- This will show how many records were affected
-- You can run this query before the DELETE to see how many records will be deleted:
-- SELECT COUNT(*) FROM flatrix_leads WHERE phone IN (...same list...);