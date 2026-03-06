# Google Drive Auto-Sync Setup Guide

## Overview
This system automatically syncs Excel files from Google Drive folders into your DALA Analytics database.

## Features
- ✅ **Automatic Detection** - Monitors folders every hour for new/modified files
- ✅ **Smart Import** - Extracts date ranges from filenames
- ✅ **Duplicate Prevention** - Won't re-import unchanged files
- ✅ **Error Handling** - Failed imports are logged and can be retried
- ✅ **Admin Dashboard** - Monitor sync status at `/drive-sync`
- ✅ **Multiple Folders** - Supports 2025 and 2026 folders (easily expandable)

---

## Setup Instructions

### Step 1: Create Google Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or use existing)
3. Enable the **Google Drive API**:
   - Go to "APIs & Services" > "Library"
   - Search for "Google Drive API"
   - Click "Enable"

4. Create Service Account:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Name: `dala-analytics-sync`
   - Click "Create and Continue"
   - Role: Select "Viewer" (or "Editor" if you want to move files after import)
   - Click "Done"

5. Create Key:
   - Click on your service account
   - Go to "Keys" tab
   - Click "Add Key" > "Create New Key"
   - Choose "JSON"
   - Download the file (it will be named something like `dala-analytics-abc123.json`)

### Step 2: Share Google Drive Folders

1. Open your Google Drive folders:
   - 2025 Sales Reports: https://drive.google.com/drive/folders/1I6b9ytn6XR0QHtr9tBRzXXe1xhMKW7dD
   - 2026 Sales Reports: https://drive.google.com/drive/folders/1dLbLm-O66ySffXUHlmAsHNIiayKHkEol

2. For each folder:
   - Right-click folder → "Share"
   - Add the service account email (found in the JSON file, looks like: `dala-analytics@project.iam.gserviceaccount.com`)
   - Set permission to "Viewer" (or "Editor")
   - Click "Send"

### Step 3: Deploy to Railway

1. **Upload Credentials**:
   ```bash
   # Rename the downloaded JSON file
   mv dala-analytics-abc123.json google_credentials.json
   
   # Upload to your project root
   ```

2. **Set Environment Variable** (optional - for security):
   ```bash
   # In Railway dashboard, add environment variable:
   GOOGLE_CREDENTIALS_PATH = /app/google_credentials.json
   ```

3. **Deploy**:
   ```bash
   git push origin master
   ```

### Step 4: Verify Setup

1. Visit: `https://your-app.railway.app/drive-sync`
2. You should see:
   - Two folders listed (2025 and 2026)
   - Sync status: "Active"
   - Files count: 0 (initially)

3. Click "🔄 Check Now" to manually trigger first sync

---

## How It Works

### File Naming Convention
The system extracts dates from filenames. Best formats:
- `February_2026_Sales.xlsx` → Feb 1-28, 2026
- `Sales_2025_12.xlsx` → Dec 1-31, 2025
- `Q1_2025_Report.xlsx` → Jan 1 - Mar 31, 2025

If no date found, it uses the folder's default year + current month.

### Duplicate Detection
Files are tracked by:
- Google Drive file ID
- Last modified timestamp

If a file changes, it will be re-imported.

### Sync Schedule
- Automatic: Every 60 minutes
- Manual: Click "Check Now" button anytime

---

## Troubleshooting

### "Credentials not found" error
- Ensure `google_credentials.json` is in project root
- Or set `GOOGLE_CREDENTIALS_PATH` environment variable

### "Access denied" error
- Verify service account has access to Drive folders
- Check that Google Drive API is enabled

### Files not importing
- Check file format: Must be `.xlsx` (Excel)
- Check filename: Should contain month name or date
- Check `/drive-sync` dashboard for error messages

### Data not appearing in dashboard
- Ensure files have the required columns
- Check logs in Railway dashboard
- Verify date ranges are being extracted correctly

---

## Customization

### Add More Folders
Edit `modules/drive_sync.py`:
```python
DRIVE_FOLDERS = [
    {
        'name': '2025 Sales Reports',
        'id': '1I6b9ytn6XR0QHtr9tBRzXXe1xhMKW7dD',
        'year': 2025,
    },
    {
        'name': '2026 Sales Reports', 
        'id': '1dLbLm-O66ySffXUHlmAsHNIiayKHkEol',
        'year': 2026,
    },
    # Add new folder here
    {
        'name': '2024 Archive',
        'id': 'YOUR_FOLDER_ID_HERE',
        'year': 2024,
    },
]
```

### Change Sync Frequency
In `modules/drive_sync.py`, modify:
```python
def run_sync_service(interval_minutes: int = 60):  # Change to desired minutes
```

### Auto-Processing (Future Enhancement)
To automatically generate reports after import, modify the `_import_file` method to call the generation pipeline.

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/drive-sync` | GET | Admin dashboard |
| `/api/drive-sync/trigger` | POST | Manual sync trigger |
| `/api/drive-sync/toggle` | POST | Enable/disable auto-sync |

---

## Security Notes

1. **Never commit** `google_credentials.json` to Git
2. **Restrict service account** to only necessary folders
3. **Use environment variables** for production deployments
4. **Monitor access logs** in Google Cloud Console

---

## Support

For issues or questions:
1. Check Railway logs: `railway logs`
2. Check sync state file: `cache/drive_sync_state.json`
3. Verify Google Drive API quota: [Google Cloud Console](https://console.cloud.google.com/)
