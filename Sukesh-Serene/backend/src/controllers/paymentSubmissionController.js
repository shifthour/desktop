import supabase from '../config/database.js';

export const createPaymentSubmission = async (req, res) => {
  try {
    const { tenant_id, month, year, amount, payment_screenshot, payment_method, paid_to, submission_date } = req.body;

    // Check if pending, approved, or directpayment submission exists for this month/year
    const { data: existing, error } = await supabase
      .from('serene_payment_submissions')
      .select('id, status')
      .eq('tenant_id', tenant_id)
      .eq('month', month)
      .eq('year', year)
      .in('status', ['pending', 'approved', 'directpayment'])
      .single();

    if (error && error.code !== 'PGRST116') throw error; // PGRST116 = no rows returned

    if (existing) {
      let errorMessage = 'Payment for this month is already pending review';
      if (existing.status === 'approved') {
        errorMessage = 'Payment for this month has already been approved';
      } else if (existing.status === 'directpayment') {
        errorMessage = 'Payment for this month has already been recorded by admin';
      }
      return res.status(400).json({ error: errorMessage });
    }

    // If there's a rejected submission, delete it before creating new one
    const { data: rejectedSubmission } = await supabase
      .from('serene_payment_submissions')
      .select('id')
      .eq('tenant_id', tenant_id)
      .eq('month', month)
      .eq('year', year)
      .eq('status', 'rejected')
      .single();

    if (rejectedSubmission) {
      await supabase
        .from('serene_payment_submissions')
        .delete()
        .eq('id', rejectedSubmission.id);
    }

    const { data: result, error: insertError } = await supabase
      .from('serene_payment_submissions')
      .insert([{
        tenant_id,
        month,
        year,
        amount,
        payment_screenshot,
        payment_method: payment_method || 'upi',
        paid_to: paid_to || null,
        submission_date,
        status: 'pending'
      }])
      .select()
      .single();

    if (insertError) throw insertError;

    res.status(201).json({
      message: 'Payment submitted successfully',
      submissionId: result.id
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getAllPaymentSubmissions = async (req, res) => {
  try {
    const { status } = req.query;

    let query = supabase
      .from('serene_payment_submissions')
      .select(`
        *,
        serene_tenants!inner(
          name,
          room_id,
          serene_rooms!left(
            room_number,
            pg_id,
            serene_pg_properties!left(
              pg_name
            )
          )
        )
      `)
      .order('created_at', { ascending: false });

    if (status) {
      query = query.eq('status', status);
    }

    const { data: submissions, error } = await query;

    if (error) throw error;

    // Flatten nested data structures to match original flat format
    const flattenedSubmissions = submissions.map(ps => ({
      ...ps,
      tenant_name: ps.serene_tenants?.name,
      room_id: ps.serene_tenants?.room_id,
      room_number: ps.serene_tenants?.serene_rooms?.room_number,
      pg_id: ps.serene_tenants?.serene_rooms?.pg_id,
      pg_name: ps.serene_tenants?.serene_rooms?.serene_pg_properties?.pg_name,
      serene_tenants: undefined
    }));

    res.json(flattenedSubmissions);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getTenantPaymentSubmissions = async (req, res) => {
  try {
    const { tenant_id } = req.params;

    const { data: submissions, error } = await supabase
      .from('serene_payment_submissions')
      .select('*')
      .eq('tenant_id', tenant_id)
      .order('created_at', { ascending: false });

    if (error) throw error;

    res.json(submissions);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getNotPaidTenants = async (req, res) => {
  try {
    const { month, year, pg_id } = req.query;

    if (!month || !year) {
      return res.status(400).json({ error: 'Month and year are required' });
    }

    // Get all active tenants
    let tenantsQuery = supabase
      .from('serene_tenants')
      .select(`
        id,
        name,
        phone,
        monthly_rent,
        room_id,
        serene_rooms!left(
          room_number,
          pg_id,
          serene_pg_properties!left(
            pg_name
          )
        )
      `)
      .eq('status', 'active');

    const { data: tenants, error: tenantsError } = await tenantsQuery;
    if (tenantsError) throw tenantsError;

    // Get all submissions (pending, approved, or directpayment) for this month/year
    const { data: submissions, error: submissionsError } = await supabase
      .from('serene_payment_submissions')
      .select('tenant_id')
      .eq('month', month)
      .eq('year', parseInt(year))
      .in('status', ['pending', 'approved', 'directpayment']);

    if (submissionsError) throw submissionsError;

    // Get tenant IDs who have submitted
    const submittedTenantIds = new Set(submissions.map(s => s.tenant_id));

    // Filter tenants who have NOT submitted
    let notPaidTenants = tenants.filter(t => !submittedTenantIds.has(t.id));

    // If pg_id filter is provided, apply it
    if (pg_id) {
      notPaidTenants = notPaidTenants.filter(t =>
        t.serene_rooms?.pg_id === parseInt(pg_id)
      );
    }

    // Flatten the data
    const flattenedTenants = notPaidTenants.map(t => ({
      id: t.id,
      tenant_id: t.id,
      tenant_name: t.name,
      phone_number: t.phone,
      monthly_rent: t.monthly_rent,
      room_id: t.room_id,
      room_number: t.serene_rooms?.room_number,
      pg_id: t.serene_rooms?.pg_id,
      pg_name: t.serene_rooms?.serene_pg_properties?.pg_name,
      month: month,
      year: parseInt(year),
      status: 'notpaid'
    }));

    res.json(flattenedTenants);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const reviewPaymentSubmission = async (req, res) => {
  try {
    const { id } = req.params;
    const { status, reviewed_by } = req.body;

    if (!['approved', 'rejected'].includes(status)) {
      return res.status(400).json({ error: 'Invalid status. Must be approved or rejected' });
    }

    // If approved, create a payment record
    if (status === 'approved') {
      // Get the submission details
      const { data: submission, error: submissionError } = await supabase
        .from('serene_payment_submissions')
        .select('*')
        .eq('id', id)
        .single();

      if (submissionError) throw submissionError;

      if (!submission) {
        return res.status(404).json({ error: 'Payment submission not found' });
      }

      // Create payment record
      const { error: paymentError } = await supabase
        .from('serene_payments')
        .insert([{
          tenant_id: submission.tenant_id,
          payment_type: 'rent',
          amount: submission.amount,
          payment_date: submission.submission_date,
          payment_method: (submission.payment_method || 'upi').toUpperCase(),
          month: submission.month,
          year: submission.year,
          description: submission.payment_method === 'cash' && submission.paid_to
            ? `Cash payment received by ${submission.paid_to}`
            : 'Payment approved from tenant submission'
        }]);

      if (paymentError) throw paymentError;
    }

    // Update submission status
    const { error: updateError } = await supabase
      .from('serene_payment_submissions')
      .update({
        status,
        reviewed_at: new Date().toISOString(),
        reviewed_by
      })
      .eq('id', id);

    if (updateError) throw updateError;

    res.json({ message: 'Payment submission reviewed successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
