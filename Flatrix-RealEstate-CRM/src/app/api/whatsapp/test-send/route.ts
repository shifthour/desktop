import { NextRequest, NextResponse } from 'next/server';
import { sendWhatsAppToNewLead } from '@/lib/whatsapp';

// POST - Send a test WhatsApp message
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { phone, name, project_name } = body;

    if (!phone) {
      return NextResponse.json(
        { success: false, error: 'Phone number is required' },
        { status: 400 }
      );
    }

    // Use a test lead ID
    const testLeadData = {
      id: '00000000-0000-0000-0000-000000000001',
      name: name || 'Test User',
      phone,
      project_name: project_name || null,
    };

    await sendWhatsAppToNewLead(testLeadData);

    return NextResponse.json({
      success: true,
      message: 'Test messages triggered. Check the Logs tab for delivery status.',
    });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}
