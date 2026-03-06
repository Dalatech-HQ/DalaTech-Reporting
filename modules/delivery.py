"""
delivery.py — Auto-delivery module for DALA reports.

Supports:
  - Email delivery via SMTP (Gmail, Outlook, or custom SMTP)
  - WhatsApp delivery via Twilio WhatsApp API

Configuration via environment variables (see .env.example):
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM
  TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM
"""

import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# ── Config from environment ───────────────────────────────────────────────────

def _smtp_config():
    return {
        'host':     os.environ.get('SMTP_HOST', 'smtp.gmail.com'),
        'port':     int(os.environ.get('SMTP_PORT', '587')),
        'user':     os.environ.get('SMTP_USER', ''),
        'password': os.environ.get('SMTP_PASSWORD', ''),
        'from':     os.environ.get('SMTP_FROM', os.environ.get('SMTP_USER', '')),
    }


def _twilio_config():
    return {
        'sid':   os.environ.get('TWILIO_ACCOUNT_SID', ''),
        'token': os.environ.get('TWILIO_AUTH_TOKEN', ''),
        'from':  os.environ.get('TWILIO_WHATSAPP_FROM', 'whatsapp:+14155238886'),
    }


def smtp_configured():
    cfg = _smtp_config()
    return bool(cfg['user'] and cfg['password'])


def twilio_configured():
    cfg = _twilio_config()
    return bool(cfg['sid'] and cfg['token'])


# ── Email ─────────────────────────────────────────────────────────────────────

def send_report_email(recipient_email, brand_name, month_label,
                      pdf_path=None, html_url=None, kpis_summary=None):
    """
    Send a brand's monthly report via email.

    Args:
        recipient_email: str — brand contact email
        brand_name:      str
        month_label:     str — e.g. 'Feb 2026'
        pdf_path:        str — absolute path to PDF (attached if provided)
        html_url:        str — link to interactive HTML (included in body)
        kpis_summary:    dict — optional {'revenue', 'qty', 'stores', 'grade'}

    Returns:
        (success: bool, message: str)
    """
    if not smtp_configured():
        return False, "SMTP not configured. Set SMTP_USER and SMTP_PASSWORD in .env"

    cfg = _smtp_config()

    # Build email body
    kpis_html = ''
    if kpis_summary:
        rev = kpis_summary.get('revenue', 0)
        qty = kpis_summary.get('qty', 0)
        stores = kpis_summary.get('stores', 0)
        grade = kpis_summary.get('grade', '-')
        kpis_html = f"""
        <table style="border-collapse:collapse;width:100%;margin:16px 0;">
          <tr>
            <td style="padding:12px;background:#F4F6FA;border-radius:6px;text-align:center;">
              <div style="font-size:11px;color:#7A849E;text-transform:uppercase;">Revenue</div>
              <div style="font-size:20px;font-weight:700;color:#1B2B5E;">&#8358;{rev:,.0f}</div>
            </td>
            <td style="padding:4px;"></td>
            <td style="padding:12px;background:#F4F6FA;border-radius:6px;text-align:center;">
              <div style="font-size:11px;color:#7A849E;text-transform:uppercase;">Qty Sold</div>
              <div style="font-size:20px;font-weight:700;color:#1B2B5E;">{qty:,.1f}</div>
            </td>
            <td style="padding:4px;"></td>
            <td style="padding:12px;background:#F4F6FA;border-radius:6px;text-align:center;">
              <div style="font-size:11px;color:#7A849E;text-transform:uppercase;">Stores</div>
              <div style="font-size:20px;font-weight:700;color:#1B2B5E;">{stores}</div>
            </td>
            <td style="padding:4px;"></td>
            <td style="padding:12px;background:#1B2B5E;border-radius:6px;text-align:center;">
              <div style="font-size:11px;color:rgba(255,255,255,0.7);text-transform:uppercase;">Grade</div>
              <div style="font-size:24px;font-weight:700;color:white;">{grade}</div>
            </td>
          </tr>
        </table>"""

    html_link_section = ''
    if html_url:
        html_link_section = f"""
        <p style="margin:16px 0;">
          <a href="{html_url}" style="background:#E8192C;color:white;padding:10px 20px;
             border-radius:6px;text-decoration:none;font-weight:600;">
            View Interactive Dashboard
          </a>
        </p>"""

    body_html = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;color:#1A1A2E;max-width:600px;margin:0 auto;">
      <div style="background:#E8192C;padding:24px;border-radius:8px 8px 0 0;">
        <h1 style="color:white;margin:0;font-size:22px;">DALA Technologies</h1>
        <p style="color:rgba(255,255,255,0.85);margin:4px 0 0;">{month_label} Sales Report</p>
      </div>
      <div style="background:white;padding:24px;border:1px solid #DDE3ED;border-top:none;border-radius:0 0 8px 8px;">
        <p>Dear <strong>{brand_name}</strong> team,</p>
        <p>Your <strong>{month_label}</strong> sales report is now ready. Please find your
           performance summary below.</p>
        {kpis_html}
        {html_link_section}
        <p style="color:#7A849E;font-size:12px;margin-top:24px;">
          This report was generated by DALA Technologies Analytics Platform.<br>
          For questions, contact your DALA account manager.
        </p>
      </div>
    </html></body>"""

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"{brand_name} — {month_label} Sales Report | DALA Technologies"
    msg['From'] = cfg['from']
    msg['To'] = recipient_email
    msg.attach(MIMEText(body_html, 'html'))

    # Attach PDF
    if pdf_path and os.path.isfile(pdf_path):
        with open(pdf_path, 'rb') as f:
            pdf_part = MIMEApplication(f.read(), _subtype='pdf')
            pdf_part.add_header(
                'Content-Disposition', 'attachment',
                filename=os.path.basename(pdf_path)
            )
            msg.attach(pdf_part)

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(cfg['host'], cfg['port']) as server:
            server.ehlo()
            server.starttls(context=ctx)
            server.login(cfg['user'], cfg['password'])
            server.sendmail(cfg['from'], recipient_email, msg.as_string())
        return True, f"Email sent to {recipient_email}"
    except Exception as e:
        return False, f"Email failed: {e}"


def send_bulk_reports(brand_contacts, month_label, pdf_dir=None, base_url=None, ds=None):
    """
    Send reports to all brand partners that have email addresses.

    Args:
        brand_contacts: list of dicts from DataStore.get_all_tokens()
        month_label:    'Feb 2026'
        pdf_dir:        path to output/pdf/ folder
        base_url:       base URL for portal links (optional)
        ds:             DataStore (for fetching kpis_summary)

    Returns:
        list of {brand_name, success, message}
    """
    results = []
    for contact in brand_contacts:
        email = contact.get('email')
        if not email:
            continue
        brand = contact['brand_name']
        safe = brand.replace(' ', '_').replace("'", '').replace('/', '-')
        pdf_path = None
        if pdf_dir:
            # Try to find the most recent PDF for this brand
            for f in (os.listdir(pdf_dir) if os.path.isdir(pdf_dir) else []):
                if f.startswith(safe) and f.endswith('.pdf'):
                    pdf_path = os.path.join(pdf_dir, f)
                    break

        html_url = None
        if base_url and contact.get('token'):
            html_url = f"{base_url}/portal/{contact['token']}"

        kpis_summary = None
        if ds:
            latest = ds.get_latest_report()
            if latest:
                bk = ds.get_brand_kpis_single(latest['id'], brand)
                if bk:
                    kpis_summary = {
                        'revenue': bk['total_revenue'],
                        'qty':     bk['total_qty'],
                        'stores':  bk['num_stores'],
                        'grade':   bk['perf_grade'],
                    }

        ok, msg = send_report_email(
            recipient_email=email,
            brand_name=brand,
            month_label=month_label,
            pdf_path=pdf_path,
            html_url=html_url,
            kpis_summary=kpis_summary,
        )
        results.append({'brand_name': brand, 'success': ok, 'message': msg})
    return results


# ── WhatsApp (Twilio) ─────────────────────────────────────────────────────────

def send_whatsapp_summary(whatsapp_number, brand_name, month_label, kpis_summary=None):
    """
    Send a WhatsApp summary message via Twilio.

    Args:
        whatsapp_number: str — recipient number e.g. '+2348012345678'
        brand_name:      str
        month_label:     str
        kpis_summary:    dict — optional {'revenue', 'qty', 'stores', 'grade'}

    Returns:
        (success: bool, message: str)
    """
    if not twilio_configured():
        return False, "Twilio not configured. Set TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN in .env"

    try:
        from twilio.rest import Client
    except ImportError:
        return False, "Twilio package not installed. Run: pip install twilio"

    cfg = _twilio_config()
    to_number = f"whatsapp:{whatsapp_number}" if not whatsapp_number.startswith('whatsapp:') else whatsapp_number

    body = f"*DALA Technologies — {month_label} Report*\n\n"
    body += f"Hello *{brand_name}*! Your monthly sales report is ready.\n\n"

    if kpis_summary:
        rev = kpis_summary.get('revenue', 0)
        qty = kpis_summary.get('qty', 0)
        stores = kpis_summary.get('stores', 0)
        grade = kpis_summary.get('grade', '-')
        body += f"📊 *Revenue:* ₦{rev:,.0f}\n"
        body += f"📦 *Qty Sold:* {qty:,.1f} packs\n"
        body += f"🏪 *Stores:* {stores}\n"
        body += f"⭐ *Performance Grade:* {grade}\n\n"

    body += "Log in to the DALA portal to view your full interactive dashboard and download your PDF report."

    try:
        client = Client(cfg['sid'], cfg['token'])
        message = client.messages.create(
            body=body,
            from_=cfg['from'],
            to=to_number,
        )
        return True, f"WhatsApp sent (SID: {message.sid})"
    except Exception as e:
        return False, f"WhatsApp failed: {e}"


def send_bulk_whatsapp(brand_contacts, month_label, ds=None):
    """
    Send WhatsApp summaries to all brand partners that have WhatsApp numbers.
    Returns list of {brand_name, success, message}.
    """
    results = []
    for contact in brand_contacts:
        wa = contact.get('whatsapp')
        if not wa:
            continue
        brand = contact['brand_name']
        kpis_summary = None
        if ds:
            latest = ds.get_latest_report()
            if latest:
                bk = ds.get_brand_kpis_single(latest['id'], brand)
                if bk:
                    kpis_summary = {
                        'revenue': bk['total_revenue'],
                        'qty':     bk['total_qty'],
                        'stores':  bk['num_stores'],
                        'grade':   bk['perf_grade'],
                    }
        ok, msg = send_whatsapp_summary(wa, brand, month_label, kpis_summary)
        results.append({'brand_name': brand, 'success': ok, 'message': msg})
    return results
