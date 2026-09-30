import asyncio
import logging
import math
import os
import re
import shutil
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, events, utils
from telethon.network import MTProtoSender
from telethon.tl.functions.upload import GetFileRequest

# -----------------------------------------------------------------------------
# 1. CONFIGURATION
# -----------------------------------------------------------------------------
load_dotenv()

API_ID = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "").strip() or None
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads")).expanduser().resolve()
SESSION_NAME = os.getenv("SESSION_NAME", "telegram_session").strip()
HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL", "").strip()

# Parallel download settings
PART_SIZE = 512 * 1024  # 512 KB per chunk
MAX_WORKERS = 8         # 8 parallel connections

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger("Downloader")

# Active downloads tracker: {message_id: {"task": Task, "status_msg": Message, "dest": Path, "name": str, "size": int}}
active_downloads: dict[int, dict] = {}


# -----------------------------------------------------------------------------
# 2. HELPER FUNCTIONS
# -----------------------------------------------------------------------------
def format_size(bytes_num: int | float | None) -> str:
    """Format bytes to human-readable string (e.g. 15.20 MB)."""
    if bytes_num is None or bytes_num <= 0:
        return "Unknown size"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_num < 1024:
            return f"{bytes_num:.2f} {unit}"
        bytes_num /= 1024
    return f"{bytes_num:.2f} PB"


def format_time(seconds: float) -> str:
    """Format seconds into human-readable string (e.g. 1m 20s)."""
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def create_progress_bar(percentage: float, length: int = 10) -> str:
    """Create a progress bar like [====>     ]."""
    filled = max(0, min(length, int(round(length * (percentage / 100.0)))))
    if filled == 0:
        return "." * length
    if filled == length:
        return "=" * length
    return "=" * (filled - 1) + ">" + "." * (length - filled)


def get_unique_filename(directory: Path, filename: str) -> Path:
    """Sanitize filename and avoid overwriting existing files by appending (1), (2)..."""
    clean_name = re.sub(r'[\\/*?:"<>|]', "_", Path(filename).name).strip() or "file"
    target = directory / clean_name

    if not target.exists():
        return target

    stem, suffix = target.stem, target.suffix
    counter = 1
    while (directory / f"{stem} ({counter}){suffix}").exists():
        counter += 1
    return directory / f"{stem} ({counter}){suffix}"


def get_file_info(message) -> tuple[str, int | None]:
    """Extract filename and file size from a Telegram message."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = None
    size = getattr(message.file, "size", None)

    if message.file and message.file.name:
        name = message.file.name
    elif message.file and message.file.ext:
        name = f"file_{timestamp}{message.file.ext}"
    elif message.photo:
        name = f"photo_{timestamp}.jpg"
    else:
        name = f"download_{timestamp}.bin"

    return name, size


def get_disk_space_info(path: Path) -> tuple[str, str]:
    """Returns free and total disk space for the target download path."""
    try:
        total, used, free = shutil.disk_usage(path if path.exists() else path.parent)
        return format_size(free), format_size(total)
    except Exception:
        return "Unknown", "Unknown"


async def healthcheck_heartbeat_loop() -> None:
    """Sends periodic alive signal to Healthchecks.io if configured."""
    if not HEALTHCHECK_URL:
        return
    logger.info(f"Healthcheck monitoring active: {HEALTHCHECK_URL}")
    while True:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, lambda: urllib.request.urlopen(HEALTHCHECK_URL, timeout=10)
            )
            logger.debug("Healthcheck heartbeat ping sent.")
        except Exception as exc:
            logger.warning(f"Healthcheck heartbeat ping failed: {exc}")
        # Send heartbeat ping every 10 minutes
        await asyncio.sleep(600)


# -----------------------------------------------------------------------------
# 3. HIGH-SPEED PARALLEL MTPROTO DOWNLOADER
# -----------------------------------------------------------------------------
async def create_dc_sender(client: TelegramClient, dc_id: int) -> MTProtoSender:
    """Creates a dedicated sender connection to the specified Datacenter."""
    dc = await client._get_dc(dc_id)
    if dc_id == client.session.dc_id:
        sender = MTProtoSender(client.session.auth_key, loggers=client._log)
        await sender.connect(
            client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=client._log,
                proxy=client._proxy,
                local_addr=client._local_addr,
            )
        )
        return sender
    else:
        return await client._create_exported_sender(dc_id)


async def download_file_parallel(
    client: TelegramClient,
    message,
    destination: Path,
    file_size: int | None,
    mode_state: dict,
    progress_callback=None,
) -> None:
    """Downloads file using parallel connections with graceful fallback."""
    if not file_size or file_size < 2 * 1024 * 1024:
        mode_state["mode"] = "Standard Sequential (Small File)"
        logger.info(f"Using {mode_state['mode']}")
        await message.download_media(file=str(destination), progress_callback=progress_callback)
        return

    try:
        dc_id, location = utils.get_input_location(message.media)
        if not dc_id:
            dc_id = client.session.dc_id
    except Exception as exc:
        mode_state["mode"] = f"Standard Sequential (Fallback: {type(exc).__name__})"
        logger.warning(f"Falling back: {mode_state['mode']}")
        await message.download_media(file=str(destination), progress_callback=progress_callback)
        return

    part_count = math.ceil(file_size / PART_SIZE)
    workers_count = min(MAX_WORKERS, part_count)

    queue = asyncio.Queue()
    for i in range(part_count):
        queue.put_nowait(i)

    downloaded_bytes = 0
    progress_lock = asyncio.Lock()

    destination.parent.mkdir(parents=True, exist_ok=True)
    with open(destination, "wb") as fp:
        fp.truncate(file_size)

    senders = []
    try:
        for _ in range(workers_count):
            s = await create_dc_sender(client, dc_id)
            senders.append(s)
        mode_state["mode"] = f"Fast Parallel ({workers_count} Streams)"
        logger.info(f"Using {mode_state['mode']}")
    except Exception as exc:
        mode_state["mode"] = f"Standard Sequential (Fallback: {type(exc).__name__} - {exc})"
        logger.warning(f"Falling back: {mode_state['mode']}")
        for s in senders:
            try:
                await s.disconnect()
            except Exception:
                pass
        await message.download_media(file=str(destination), progress_callback=progress_callback)
        return

    async def worker(sender: MTProtoSender):
        nonlocal downloaded_bytes
        with open(destination, "r+b") as fp:
            while not queue.empty():
                try:
                    part_index = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                offset = part_index * PART_SIZE
                limit = min(PART_SIZE, file_size - offset)

                for attempt in range(3):
                    try:
                        result = await sender.send(
                            GetFileRequest(location=location, offset=offset, limit=limit)
                        )
                        chunk_bytes = result.bytes
                        fp.seek(offset)
                        fp.write(chunk_bytes)

                        async with progress_lock:
                            downloaded_bytes += len(chunk_bytes)
                            if progress_callback:
                                await progress_callback(downloaded_bytes, file_size)
                        break
                    except Exception as err:
                        if attempt == 2:
                            raise err
                        await asyncio.sleep(1)

                queue.task_done()

    try:
        tasks = [asyncio.create_task(worker(s)) for s in senders]
        await asyncio.gather(*tasks)
    finally:
        for s in senders:
            try:
                await s.disconnect()
            except Exception:
                pass


# -----------------------------------------------------------------------------
# 4. DOWNLOAD TASK WORKER
# -----------------------------------------------------------------------------
async def process_download(message, destination: Path, file_size: int | None, status_msg) -> None:
    """Processes the download lifecycle including progress, completion, and error cleanup."""
    start_time = time.time()
    last_update = [0.0]
    mode_state = {"mode": "Fast Parallel (Initializing...)"}

    async def progress_callback(current_bytes: int, total_bytes: int) -> None:
        now = time.time()
        total = total_bytes or file_size or 0

        if (now - last_update[0]) < 3.0 and current_bytes != total:
            return

        last_update[0] = now
        elapsed = max(0.1, now - start_time)
        speed = current_bytes / elapsed

        if total > 0:
            percentage = (current_bytes / total) * 100.0
            eta = (total - current_bytes) / speed if speed > 0 else 0
            bar = create_progress_bar(percentage)

            text = (
                f"**[DOWNLOADING] Progress Update**\n\n"
                f"**File:** `{destination.name}`\n"
                f"**Mode:** `{mode_state['mode']}`\n"
                f"**Progress:** `[{bar}]` **{percentage:.1f}%**\n"
                f"**Size:** `{format_size(current_bytes)} / {format_size(total)}`\n"
                f"**Speed:** `{format_size(speed)}/s` | **ETA:** `{format_time(eta)}`\n\n"
                f"_Send /cancel to stop this download._"
            )
        else:
            text = (
                f"**[DOWNLOADING] Progress Update**\n\n"
                f"**File:** `{destination.name}`\n"
                f"**Mode:** `{mode_state['mode']}`\n"
                f"**Downloaded:** `{format_size(current_bytes)}`\n"
                f"**Speed:** `{format_size(speed)}/s`\n\n"
                f"_Send /cancel to stop this download._"
            )

        try:
            await status_msg.edit(text)
        except Exception:
            pass

    try:
        await download_file_parallel(
            client=message.client,
            message=message,
            destination=destination,
            file_size=file_size,
            mode_state=mode_state,
            progress_callback=progress_callback,
        )

        elapsed = max(0.1, time.time() - start_time)
        final_size = destination.stat().st_size if destination.exists() else (file_size or 0)
        avg_speed = final_size / elapsed

        logger.info(f"Done: {destination.name} in {elapsed:.1f}s ({format_size(avg_speed)}/s)")

        # Success feedback
        await status_msg.edit(
            f"**[SUCCESS] Download Complete**\n\n"
            f"**File:** `{destination.name}`\n"
            f"**Mode:** `{mode_state['mode']}`\n"
            f"**Size:** `{format_size(final_size)}`\n"
            f"**Time:** `{format_time(elapsed)}` (Avg `{format_size(avg_speed)}/s`)\n"
            f"**Saved To:** `{destination}`"
        )

        # Delete the original forwarded message to free cloud storage
        try:
            await message.delete()
            logger.info(f"Deleted original Telegram message for {destination.name}")
        except Exception as del_err:
            logger.warning(f"Could not delete original message: {del_err}")

    except asyncio.CancelledError:
        logger.info(f"Download cancelled by user: {destination.name}")
        if destination.exists():
            try:
                destination.unlink()
            except OSError:
                pass
        await status_msg.edit(
            f"**[CANCELLED] Download Cancelled by User**\n\n"
            f"**File:** `{destination.name}`\n"
            f"**Status:** Download stopped and partial file deleted."
        )

    except Exception as exc:
        logger.error(f"Failed to download {destination.name}: {exc}")
        if destination.exists():
            try:
                destination.unlink()
            except OSError:
                pass

        await status_msg.edit(
            f"**[ERROR] Download Failed**\n\n"
            f"**File:** `{destination.name}`\n"
            f"**Mode:** `{mode_state['mode']}`\n"
            f"**Reason:** `{type(exc).__name__}: {str(exc)}`"
        )

    finally:
        active_downloads.pop(message.id, None)


# -----------------------------------------------------------------------------
# 5. COMMAND & EVENT HANDLERS
# -----------------------------------------------------------------------------
async def handle_help_command(message) -> None:
    """Displays help information and available commands."""
    free_space, total_space = get_disk_space_info(DOWNLOAD_DIR)
    text = (
        "**[HELP] Telegram Saved Messages Downloader**\n\n"
        "**Available Commands:**\n"
        "• `/help` or `/start` — Show this help message\n"
        "• `/status` — View server disk space and active downloads\n"
        "• `/cancel` — Cancel ongoing download and delete partial file\n\n"
        "**How to Use:**\n"
        "1. Forward or send any file, video, or document to Saved Messages.\n"
        "2. The server downloads it automatically at high speed (parallel streams).\n"
        "3. Once finished, the original forwarded file is auto-deleted from Telegram.\n\n"
        f"**Download Folder:** `{DOWNLOAD_DIR}`\n"
        f"**Server Disk Space:** `{free_space} free / {total_space} total`"
    )
    await message.reply(text)


async def handle_status_command(message) -> None:
    """Displays server storage and active downloads status."""
    free_space, total_space = get_disk_space_info(DOWNLOAD_DIR)
    lines = [
        "**[STATUS] Downloader & Server Status**\n",
        f"**Download Folder:** `{DOWNLOAD_DIR}`",
        f"**Server Storage:** `{free_space} free` of `{total_space}`",
    ]

    if active_downloads:
        lines.append(f"\n**Active Downloads ({len(active_downloads)}):**")
        for info in active_downloads.values():
            size_str = format_size(info.get("size"))
            lines.append(f"• `{info['name']}` ({size_str})")
        lines.append("\n_Send /cancel to stop the active download._")
    else:
        lines.append("\n**Active Downloads:** None (idle)")

    await message.reply("\n".join(lines))


async def handle_cancel_command(event) -> None:
    """Handles /cancel command to stop active downloads."""
    message = event.message
    reply_to = message.reply_to_msg_id

    target_id = None
    if reply_to:
        for msg_id, info in list(active_downloads.items()):
            if msg_id == reply_to or info["status_msg"].id == reply_to:
                target_id = msg_id
                break
    else:
        if active_downloads:
            target_id = list(active_downloads.keys())[-1]

    if target_id and target_id in active_downloads:
        download_info = active_downloads[target_id]
        download_info["task"].cancel()
        try:
            await message.delete()
        except Exception:
            pass
    else:
        await message.reply("**[INFO] No active download found to cancel.**")


async def handle_new_saved_message(event) -> None:
    """Triggered when any message arrives in 'Saved Messages'."""
    message = event.message

    if message.text:
        cmd = message.text.strip().lower()
        if cmd in ["/help", "/start"]:
            await handle_help_command(message)
            return
        elif cmd == "/status":
            await handle_status_command(message)
            return
        elif cmd == "/cancel":
            await handle_cancel_command(event)
            return

    if not message.media:
        return

    raw_name, file_size = get_file_info(message)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    destination = get_unique_filename(DOWNLOAD_DIR, raw_name)

    logger.info(f"Incoming: {destination.name} ({format_size(file_size)})")

    status_msg = await message.reply(
        f"**[STARTING] Download Started**\n\n"
        f"**File:** `{destination.name}`\n"
        f"**Size:** `{format_size(file_size)}`\n"
        f"**Folder:** `{destination.parent}`\n\n"
        f"_Send /cancel to stop this download._"
    )

    task = asyncio.create_task(process_download(message, destination, file_size, status_msg))
    active_downloads[message.id] = {
        "task": task,
        "status_msg": status_msg,
        "dest": destination,
        "name": destination.name,
        "size": file_size,
    }


# -----------------------------------------------------------------------------
# 6. APP ENTRYPOINT
# -----------------------------------------------------------------------------
async def main() -> None:
    if not API_ID or not API_HASH:
        print("\n[ERROR] Missing API_ID or API_HASH in .env file.")
        print("Get them for free at: https://my.telegram.org\n")
        sys.exit(1)

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    free_space, total_space = get_disk_space_info(DOWNLOAD_DIR)
    print(f"\nDownload Folder: {DOWNLOAD_DIR}")
    print(f"Server Storage: {free_space} free / {total_space} total")
    print("Connecting to Telegram...")

    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)

    client.add_event_handler(handle_new_saved_message, events.NewMessage(chats="me"))

    await client.start(phone=PHONE_NUMBER)

    user = await client.get_me()
    print(f"[STATUS] Connected as: {user.first_name} (ID: {user.id})")
    print("[STATUS] Commands: /help, /status, /cancel")
    print("[STATUS] Forward any file to 'Saved Messages' to download it.\n")

    # Start background healthcheck heartbeat task if configured
    if HEALTHCHECK_URL:
        asyncio.create_task(healthcheck_heartbeat_loop())

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\n[INFO] Stopped.")
