const express = require('express');
const router = express.Router();
const supabase = require('../config/supabase');

// API endpoint for Anahata project lead submission
router.post('/submit-lead', async (req, res) => {
  try {
    const { name, phone, email, source, status_of_save, action, preferred_type } = req.body;

    if (!name || !phone) {
      return res.status(400).json({ success: false, error: "Name and phone are required" });
    }

    if (action === "update") {
      // Update existing record by phone number
      const { data, error } = await supabase
        .from("flatrix_leads")
        .update({
          email: email?.trim() || null,
          status_of_save: status_of_save || "saved",
          preferred_type: preferred_type || null,
          updated_at: new Date().toISOString(),
        })
        .eq("phone", phone.trim())
        .eq("project_name", "Anahata")
        .select();

      if (error) {
        console.error("Supabase update error:", error);
        return res.status(500).json({ success: false, error: "Failed to update lead data" });
      }

      if (data && data.length > 0) {
        console.log("Lead updated successfully:", data);
        return res.json({
          success: true,
          message: "Lead data updated successfully",
          data: data,
        });
      } else {
        // If no record found to update, insert new one
        console.log("No existing record found, inserting new record");
      }
    }

    // Insert new record (default action or fallback)
    const { data, error } = await supabase
      .from("flatrix_leads")
      .insert([
        {
          project_name: "Anahata",
          name: name.trim(),
          phone: phone.trim(),
          email: email?.trim() || null,
          source: source || "unknown",
          status_of_save: status_of_save || "saved",
          preferred_type: preferred_type || null,
        },
      ])
      .select();

    if (error) {
      console.error("Supabase insert error:", error);
      return res.status(500).json({ success: false, error: "Failed to save lead data" });
    }

    console.log("Lead inserted successfully:", data);

    return res.json({
      success: true,
      message: "Lead data saved successfully",
      data: data,
    });
  } catch (error) {
    console.error("API error:", error);
    return res.status(500).json({ success: false, error: "Internal server error" });
  }
});

module.exports = router;
