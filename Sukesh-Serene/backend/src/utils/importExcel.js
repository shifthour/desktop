import XLSX from 'xlsx';
import db from '../config/database.js';
import bcrypt from 'bcryptjs';

export const importExcelData = (excelPath) => {
  try {
    console.log('📊 Starting Excel import...');

    // Read the Excel file
    const workbook = XLSX.readFile(excelPath);

    // Import Room Master
    const roomMasterSheet = workbook.Sheets['RoomMaster'];
    if (roomMasterSheet) {
      const roomData = XLSX.utils.sheet_to_json(roomMasterSheet);
      console.log(`Found ${roomData.length} rows in RoomMaster`);

      const roomsMap = new Map();

      for (const row of roomData) {
        if (!row['Room No'] || row['Room No'] === 'Room No') continue;

        const roomNo = parseInt(row['Room No']);
        const sharingType = row['Sharing Type'];
        const bedNo = parseInt(row['Beds']);
        const status = row['Status'] || 'Vacant';

        // Create or get room
        if (!roomsMap.has(roomNo)) {
          const existingRoom = db.prepare('SELECT id FROM rooms WHERE room_no = ?').get(roomNo);

          if (!existingRoom) {
            const maxBeds = db.prepare(
              'SELECT MAX(bed_no) as max_bed FROM beds WHERE room_id IN (SELECT id FROM rooms WHERE room_no = ?)'
            ).get(roomNo);

            const totalBeds = maxBeds?.max_bed || parseInt(sharingType?.match(/\d+/)?.[0]) || 2;

            const roomResult = db.prepare(
              'INSERT INTO rooms (room_no, sharing_type, total_beds) VALUES (?, ?, ?)'
            ).run(roomNo, sharingType, totalBeds);

            roomsMap.set(roomNo, roomResult.lastInsertRowid);
          } else {
            roomsMap.set(roomNo, existingRoom.id);
          }
        }

        const roomId = roomsMap.get(roomNo);

        // Create bed if doesn't exist
        const existingBed = db.prepare(
          'SELECT id FROM beds WHERE room_id = ? AND bed_no = ?'
        ).get(roomId, bedNo);

        if (!existingBed) {
          const bedResult = db.prepare(
            'INSERT INTO beds (room_id, bed_no, status) VALUES (?, ?, ?)'
          ).run(roomId, bedNo, status.toLowerCase());

          // Create tenant if exists
          if (row['Tenant Name']) {
            const tenantData = {
              bed_id: bedResult.lastInsertRowid,
              name: row['Tenant Name'],
              mobile: row['Mobile Number']?.toString() || '',
              aadhar: row['Aadhar No']?.toString() || null,
              address: row['Address'] || null,
              joining_date: row['Joining Date'] || null,
              rent_amount: row['Rent'] || null,
              deposit_amount: row['Deposit'] || null,
              deposit_paid_date: row['Deposit Paid Date'] || null,
              deposit_account: row['Deposit paid account'] || null,
              remarks: row['Remarks'] || null,
            };

            db.prepare(`
              INSERT INTO tenants (
                bed_id, name, mobile, aadhar, address, joining_date,
                rent_amount, deposit_amount, deposit_paid_date, deposit_account, remarks
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).run(
              tenantData.bed_id,
              tenantData.name,
              tenantData.mobile,
              tenantData.aadhar,
              tenantData.address,
              tenantData.joining_date,
              tenantData.rent_amount,
              tenantData.deposit_amount,
              tenantData.deposit_paid_date,
              tenantData.deposit_account,
              tenantData.remarks
            );

            // Update bed status to occupied
            db.prepare('UPDATE beds SET status = ? WHERE id = ?')
              .run('occupied', bedResult.lastInsertRowid);
          }
        }
      }

      console.log('✅ Room Master imported successfully');
    }

    // Import Rent Payments from monthly sheets
    const monthSheets = ['July Rentals', 'Aug Rentals', 'sept rentals', 'Oct rentals'];

    for (const sheetName of monthSheets) {
      if (!workbook.Sheets[sheetName]) continue;

      const rentData = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);
      console.log(`Found ${rentData.length} rows in ${sheetName}`);

      const monthMatch = sheetName.match(/(July|Aug|sept|Oct)/i);
      const month = monthMatch ? monthMatch[1] : '';
      const monthMap = {
        'July': 'July',
        'Aug': 'August',
        'sept': 'September',
        'Oct': 'October'
      };
      const fullMonth = monthMap[month] || month;
      const year = 2025;

      for (const row of rentData) {
        const roomNo = parseInt(row['Room No'] || row['Room']);
        const bedNo = parseInt(row['Beds'] || row['Bed No']);
        const tenantName = row['Name'] || row['Tenant Name'] || row['Column 3'] || row['Column 3 . '];

        if (!roomNo || !bedNo || !tenantName) continue;

        // Find tenant and bed
        const tenant = db.prepare(`
          SELECT t.id, t.bed_id, t.rent_amount
          FROM tenants t
          JOIN beds b ON t.bed_id = b.id
          JOIN rooms r ON b.room_id = r.id
          WHERE r.room_no = ? AND b.bed_no = ? AND t.name = ?
        `).get(roomNo, bedNo, tenantName);

        if (tenant) {
          const rentAmount = parseFloat(row['Rent']) || tenant.rent_amount || 0;
          const paidAmount = parseFloat(row['Paid'] || row['Paid account']) || 0;
          const balance = rentAmount - paidAmount;
          const paymentDate = row['Payment Date'] || null;
          const paymentMode = row['Mode'] || null;

          // Check if payment already exists
          const existing = db.prepare(`
            SELECT id FROM rent_payments
            WHERE tenant_id = ? AND month = ? AND year = ?
          `).get(tenant.id, fullMonth, year);

          if (!existing) {
            db.prepare(`
              INSERT INTO rent_payments (
                tenant_id, bed_id, month, year, rent_amount, paid_amount,
                balance, payment_date, payment_mode
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).run(
              tenant.id,
              tenant.bed_id,
              fullMonth,
              year,
              rentAmount,
              paidAmount,
              balance,
              paymentDate,
              paymentMode
            );
          }
        }
      }

      console.log(`✅ ${sheetName} imported successfully`);
    }

    console.log('✅ Excel import completed successfully!');
    return { success: true, message: 'Excel data imported successfully' };
  } catch (error) {
    console.error('❌ Error importing Excel:', error);
    return { success: false, error: error.message };
  }
};

// Create default admin user
export const createDefaultUser = () => {
  try {
    const existing = db.prepare('SELECT id FROM users WHERE email = ?').get('admin@sereneliving.com');

    if (!existing) {
      const hashedPassword = bcrypt.hashSync('admin123', 10);
      db.prepare('INSERT INTO users (email, password, name, role) VALUES (?, ?, ?, ?)')
        .run('admin@sereneliving.com', hashedPassword, 'Admin', 'admin');
      console.log('✅ Default admin user created');
      console.log('   Email: admin@sereneliving.com');
      console.log('   Password: admin123');
    }
  } catch (error) {
    console.error('Error creating default user:', error);
  }
};
