import supabase from '../config/database.js';

export const getAllExpenses = async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('serene_expenses')
      .select(`
        *,
        serene_pg_properties!left (
          pg_name
        )
      `)
      .order('expense_date', { ascending: false })
      .order('created_at', { ascending: false });

    if (error) throw error;

    // Transform the data to match the original format
    const expenses = data.map(expense => ({
      ...expense,
      pg_name: expense.serene_pg_properties?.pg_name || null,
      serene_pg_properties: undefined
    }));

    res.json(expenses);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getExpenseById = async (req, res) => {
  try {
    const { id } = req.params;

    const { data, error } = await supabase
      .from('serene_expenses')
      .select('*')
      .eq('id', id)
      .single();

    if (error) {
      if (error.code === 'PGRST116') {
        return res.status(404).json({ error: 'Expense not found' });
      }
      throw error;
    }

    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const createExpense = async (req, res) => {
  try {
    const {
      pg_id,
      category,
      description,
      amount,
      expense_date,
      payment_method,
      bill_image
    } = req.body;

    const { data, error } = await supabase
      .from('serene_expenses')
      .insert([{
        pg_id: pg_id || null,
        category,
        description: description || null,
        amount,
        expense_date,
        payment_method: payment_method || null,
        bill_image: bill_image || null
      }])
      .select()
      .single();

    if (error) throw error;

    res.status(201).json({
      message: 'Expense created successfully',
      expenseId: data.id
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const updateExpense = async (req, res) => {
  try {
    const { id } = req.params;
    const {
      pg_id,
      category,
      description,
      amount,
      expense_date,
      payment_method,
      bill_image
    } = req.body;

    const { error } = await supabase
      .from('serene_expenses')
      .update({
        pg_id: pg_id || null,
        category,
        description: description || null,
        amount,
        expense_date,
        payment_method: payment_method || null,
        bill_image: bill_image || null
      })
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Expense updated successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const deleteExpense = async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('serene_expenses')
      .delete()
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Expense deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
