# Telegram Saved Messages Downloader

Automatically downloads any file you send or forward to your Telegram "Saved Messages" directly into a folder on your server/computer.

---

## 1. Quick Setup

### Get your Telegram API credentials
1. Go to **https://my.telegram.org** and log in with your phone number.
2. Click **API development tools**.
3. Create an app (enter any name) and copy your `api_id` and `api_hash`.

---

## 2. Install & Configure

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create .env file (copy .env.example)
cp .env.example .env
```

Edit `.env`:
```env
API_ID=12345678
API_HASH=your_api_hash_here
DOWNLOAD_DIR=/HDD/Hard_Disk_Drive/Downloads/
HEALTHCHECK_URL=https://hc-ping.com/5ec4eef6-440d-4825-9292-2d368be9d02f
```

---

## 3. Run & Commands

```bash
python main.py
```

### Available Commands in Saved Messages:
* **`/help`** or **`/start`** — Displays usage instructions, available commands, and server storage info.
* **`/status`** — Checks server storage, active download progress, and the queued files list.
* **`/cancel`** — Cancels the active download (or reply `/cancel` to a specific queued item).

### Key Features:
* **Sequential Queue:** Forward 10+ files at once; the bot processes them one by one at full speed without network congestion or rate limits.
* **High-Speed Parallel Streams:** Uses 8 parallel MTProto streams per active download.
* **Auto-Delete:** Once a download completes, the original forwarded message is automatically deleted from Telegram to free cloud storage.
* **Uptime Monitoring:** Heartbeat integration with Healthchecks.io.

---

## 4. Run 24/7 on Linux Server (Systemd)

Use the included [`telegram-downloader.service`](telegram-downloader.service) file:

```bash
# Copy service file
sudo cp telegram-downloader.service /etc/systemd/system/

# Reload & start
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-downloader

# Check logs
journalctl -u telegram-downloader -f
```
