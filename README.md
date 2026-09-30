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
DOWNLOAD_DIR=./downloads
```

---

## 3. Run & Commands

```bash
python main.py
```

### Available Commands in Saved Messages:
* **`/help`** or **`/start`** — Displays usage instructions, available commands, and server storage info.
* **`/status`** — Checks server free disk space and active download progress.
* **`/cancel`** — Cancels ongoing download and deletes incomplete files on the server.

### Features:
* **Auto-Delete:** Once a download finishes successfully, the original forwarded file is automatically deleted from your Saved Messages to keep Telegram clean.
* **High-Speed Parallel:** Uses 8 parallel MTProto streams to maximize line speed.

---

## 4. Run 24/7 on Linux Server (Systemd)

1. Copy this project folder (including `telegram_session.session`) to your Linux server.
2. Create a service file:
   ```bash
   sudo nano /etc/systemd/system/tg-downloader.service
   ```
3. Paste:
   ```ini
   [Unit]
   Description=Telegram File Downloader
   After=network.target

   [Service]
   Type=simple
   User=YOUR_LINUX_USERNAME
   WorkingDirectory=/home/YOUR_LINUX_USERNAME/telegram_downloader
   ExecStart=/usr/bin/python3 /home/YOUR_LINUX_USERNAME/telegram_downloader/main.py
   Restart=always
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```
4. Start it:
   ```bash
   sudo systemctl enable --now tg-downloader
   ```

To check logs: `journalctl -u tg-downloader -f`
