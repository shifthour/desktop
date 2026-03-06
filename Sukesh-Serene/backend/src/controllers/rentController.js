import supabase from '../config/database.js';

export const getAllPayments = async (req, res) => {
  try {
    const { month, year, tenant_id } = req.query;

    // Build query with Supabase
    let query = supabase
      .from('serene_payments')
      .select(`
        *,
        serene_tenants!inner (
          name,
          phone,
          room_id,
          serene_rooms (
            room_number
          )
        )
      `)
      .order('payment_date', { ascending: false });

    // Apply filters
    if (month) {
      query = query.eq('month', month);
    }
    if (year) {
      query = query.eq('year', parseInt(year));
    }
    if (tenant_id) {
      query = query.eq('tenant_id', parseInt(tenant_id));
    }

    const { data, error } = await query;

    if (error) throw error;

    // Flatten the nested structure to match original format
    const payments = data.map(payment => {
      const tenant = payment.serene_tenants;
      const room = tenant.serene_rooms;

      return {
        ...payment,
        tenant_name: tenant.name,
        tenant_phone: tenant.phone,
        room_number: room ? room.room_number : null,
        // Remove nested objects
        serene_tenants: undefined
      };
    }).map(({ serene_tenants, ...rest }) => rest);

    res.json(payments);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const createPayment = async (req, res) => {
  try {
    const {
      tenant_id,
      payment_type,
      amount,
      payment_date,
      payment_method,
      month,
      year,
      description
    } = req.body;

    const { data, error } = await supabase
      .from('serene_payments')
      .insert([{
        tenant_id,
        payment_type,
        amount,
        payment_date,
        payment_method,
        month,
        year,
        description
      }])
      .select()
      .single();

    if (error) throw error;

    // If payment_type is 'rent', also create a directpayment record in payment_submissions
    if (payment_type === 'rent' && month && year) {
      // Check if a submission already exists for this tenant/month/year
      const { data: existingSubmission } = await supabase
        .from('serene_payment_submissions')
        .select('id')
        .eq('tenant_id', tenant_id)
        .eq('month', month)
        .eq('year', year)
        .single();

      // Only create if no existing submission
      if (!existingSubmission) {
        await supabase
          .from('serene_payment_submissions')
          .insert([{
            tenant_id,
            month,
            year,
            amount,
            payment_method: payment_method || 'DIRECT',
            submission_date: payment_date,
            status: 'directpayment'
          }]);
      }
    }

    res.status(201).json({
      message: 'Payment recorded successfully',
      paymentId: data.id
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const updatePayment = async (req, res) => {
  try {
    const { id } = req.params;
    const {
      payment_type,
      amount,
      payment_date,
      payment_method,
      month,
      year,
      description,
      status
    } = req.body;

    const { error } = await supabase
      .from('serene_payments')
      .update({
        payment_type,
        amount,
        payment_date,
        payment_method,
        month,
        year,
        description,
        status
      })
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Payment updated successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const deletePayment = async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('serene_payments')
      .delete()
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Payment deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
