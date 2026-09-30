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

# Queue management state
download_queue: asyncio.Queue = asyncio.Queue()
queued_items: list[dict] = []
current_download: dict | None = None


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
                limit = PART_SIZE

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
# 4. SEQUENTIAL QUEUE WORKER
# -----------------------------------------------------------------------------
async def process_single_download(item: dict) -> None:
    """Handles the lifecycle of a single queued download item."""
    message = item["message"]
    destination = item["destination"]
    file_size = item["file_size"]
    status_msg = item["status_msg"]

    start_time = time.time()
    last_update = [0.0]
    mode_state = {"mode": "Fast Parallel (Initializing...)"}

    # Initial start notification
    try:
        await status_msg.edit(
            f"**[STARTING] Download Started**\n\n"
            f"**File:** `{destination.name}`\n"
            f"**Size:** `{format_size(file_size)}`\n"
            f"**Folder:** `{destination.parent}`\n\n"
            f"_Send /cancel to stop this download._"
        )
    except Exception:
        pass

    async def progress_callback(current_bytes: int, total_bytes: int) -> None:
        now = time.time()
        total = total_bytes or file_size or 0

        if (now - last_update[0]) < 3.0 and current_bytes != total:
            return

        last_update[0] = now
        elapsed = max(0.1, now - start_time)
        speed = current_bytes / elapsed

        queue_note = f" (Queue: {len(queued_items)} waiting)" if queued_items else ""

        if total > 0:
            percentage = (current_bytes / total) * 100.0
            eta = (total - current_bytes) / speed if speed > 0 else 0
            bar = create_progress_bar(percentage)

            text = (
                f"**[DOWNLOADING] Progress Update**{queue_note}\n\n"
                f"**File:** `{destination.name}`\n"
                f"**Mode:** `{mode_state['mode']}`\n"
                f"**Progress:** `[{bar}]` **{percentage:.1f}%**\n"
                f"**Size:** `{format_size(current_bytes)} / {format_size(total)}`\n"
                f"**Speed:** `{format_size(speed)}/s` | **ETA:** `{format_time(eta)}`\n\n"
                f"_Send /cancel to stop this download._"
            )
        else:
            text = (
                f"**[DOWNLOADING] Progress Update**{queue_note}\n\n"
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

        # Delete original forwarded message
        try:
            await message.delete()
            logger.info(f"Deleted original Telegram message for {destination.name}")
        except Exception as del_err:
            logger.warning(f"Could not delete original message: {del_err}")

    except asyncio.CancelledError:
        logger.info(f"Download cancelled: {destination.name}")
        if destination.exists():
            try:
                destination.unlink()
            except OSError:
                pass
        try:
            await status_msg.edit(
                f"**[CANCELLED] Download Cancelled**\n\n"
                f"**File:** `{destination.name}`\n"
                f"**Status:** Stopped and partial file deleted."
            )
        except Exception:
            pass

    except Exception as exc:
        logger.error(f"Failed to download {destination.name}: {exc}")
        if destination.exists():
            try:
                destination.unlink()
            except OSError:
                pass
        try:
            await status_msg.edit(
                f"**[ERROR] Download Failed**\n\n"
                f"**File:** `{destination.name}`\n"
                f"**Mode:** `{mode_state['mode']}`\n"
                f"**Reason:** `{type(exc).__name__}: {str(exc)}`"
            )
        except Exception:
            pass


async def queue_worker_loop(client: TelegramClient) -> None:
    """Continuously processes downloads from the queue one by one."""
    global current_download
    while True:
        item = await download_queue.get()
        if item in queued_items:
            queued_items.remove(item)

        # Skip if item was cancelled/deleted while sitting in queue
        if item.get("cancelled", False):
            download_queue.task_done()
            continue

        # Double check if message was deleted before starting
        try:
            msg_check = await client.get_messages("me", ids=item["message"].id)
            if not msg_check or not getattr(msg_check, "media", None):
                logger.info(f"Message {item['message'].id} was deleted before start. Skipping...")
                try:
                    await item["status_msg"].delete()
                except Exception:
                    pass
                download_queue.task_done()
                continue
        except Exception:
            pass

        # Set as active download
        download_task = asyncio.create_task(process_single_download(item))
        current_download = {
            "item": item,
            "task": download_task,
        }

        try:
            await download_task
        except Exception as err:
            logger.error(f"Worker task error: {err}")
        finally:
            current_download = None
            download_queue.task_done()


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
        "• `/status` — View server disk space, active download, and queue\n"
        "• `/cancel` — Cancel active download or a queued item\n\n"
        "**How to Use:**\n"
        "1. Forward or send any files to Saved Messages (even 10+ files at once).\n"
        "2. The server processes them sequentially in a queue at maximum speed (8 streams).\n"
        "3. Deleting any file from Saved Messages automatically cancels and removes it from queue.\n"
        "4. Completed files are auto-deleted from Telegram to free cloud storage.\n\n"
        f"**Download Folder:** `{DOWNLOAD_DIR}`\n"
        f"**Server Disk Space:** `{free_space} free / {total_space} total`"
    )
    await message.reply(text)


async def handle_status_command(message) -> None:
    """Displays server storage, active download, and queue status."""
    free_space, total_space = get_disk_space_info(DOWNLOAD_DIR)
    lines = [
        "**[STATUS] Downloader & Server Status**\n",
        f"**Download Folder:** `{DOWNLOAD_DIR}`",
        f"**Server Storage:** `{free_space} free` of `{total_space}`",
    ]

    if current_download:
        item = current_download["item"]
        lines.append(f"\n**Currently Downloading:**\n• `{item['destination'].name}` ({format_size(item['file_size'])})")
    else:
        lines.append("\n**Currently Downloading:** None (idle)")

    if queued_items:
        lines.append(f"\n**Download Queue ({len(queued_items)}):**")
        for idx, item in enumerate(queued_items, start=1):
            lines.append(f"#{idx}: `{item['destination'].name}` ({format_size(item['file_size'])})")
        lines.append("\n_Send /cancel to stop current download or reply /cancel to a queued item._")
    else:
        lines.append("**Download Queue:** Empty")

    await message.reply("\n".join(lines))


async def handle_cancel_command(event) -> None:
    """Handles /cancel command to stop active download or dequeue items."""
    global current_download
    message = event.message
    reply_to = message.reply_to_msg_id

    # 1. Check if replying to a queued item
    if reply_to:
        for item in list(queued_items):
            if item["message"].id == reply_to or item["status_msg"].id == reply_to:
                item["cancelled"] = True
                queued_items.remove(item)
                try:
                    await item["status_msg"].edit(
                        f"**[CANCELLED] Removed from Queue**\n\n"
                        f"**File:** `{item['destination'].name}`\n"
                        f"**Status:** Item removed from download queue."
                    )
                    await message.delete()
                except Exception:
                    pass
                return

        # Check if replying to the currently active download
        if current_download:
            active_item = current_download["item"]
            if active_item["message"].id == reply_to or active_item["status_msg"].id == reply_to:
                current_download["task"].cancel()
                try:
                    await message.delete()
                except Exception:
                    pass
                return

    # 2. If not replying to a specific message, cancel the active download
    if current_download:
        current_download["task"].cancel()
        try:
            await message.delete()
        except Exception:
            pass
    elif queued_items:
        last_item = queued_items.pop()
        last_item["cancelled"] = True
        try:
            await last_item["status_msg"].edit(
                f"**[CANCELLED] Removed from Queue**\n\n"
                f"**File:** `{last_item['destination'].name}`"
            )
            await message.delete()
        except Exception:
            pass
    else:
        await message.reply("**[INFO] No active download or queued items to cancel.**")


async def handle_deleted_messages(event) -> None:
    """Triggered when messages are deleted in Saved Messages; auto-manages queue and active downloads."""
    global current_download
    deleted_ids = set(event.deleted_ids)

    # 1. Check if the active downloading message was deleted
    if current_download:
        active_msg_id = current_download["item"]["message"].id
        active_status_id = current_download["item"]["status_msg"].id
        if active_msg_id in deleted_ids or active_status_id in deleted_ids:
            logger.info(f"Active download message was deleted by user. Cancelling...")
            current_download["task"].cancel()

    # 2. Check if any queued item was deleted
    for item in list(queued_items):
        if item["message"].id in deleted_ids or item["status_msg"].id in deleted_ids:
            logger.info(f"Queued message {item['destination'].name} was deleted by user. Removing from queue...")
            item["cancelled"] = True
            if item in queued_items:
                queued_items.remove(item)
            try:
                await item["status_msg"].delete()
            except Exception:
                pass


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

    logger.info(f"Enqueued: {destination.name} ({format_size(file_size)})")

    is_busy = (current_download is not None) or (not download_queue.empty())
    queue_pos = len(queued_items) + 1

    if is_busy:
        status_msg = await message.reply(
            f"**[QUEUED] Added to Queue (Position: #{queue_pos})**\n\n"
            f"**File:** `{destination.name}`\n"
            f"**Size:** `{format_size(file_size)}`\n\n"
            f"_Will download automatically when earlier files finish._"
        )
    else:
        status_msg = await message.reply(
            f"**[STARTING] Preparing Download...**\n\n"
            f"**File:** `{destination.name}`\n"
            f"**Size:** `{format_size(file_size)}`\n"
            f"**Folder:** `{destination.parent}`"
        )

    item = {
        "message": message,
        "status_msg": status_msg,
        "destination": destination,
        "file_size": file_size,
        "cancelled": False,
    }

    queued_items.append(item)
    await download_queue.put(item)


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

    # Listen to new and deleted messages in 'Saved Messages'
    client.add_event_handler(handle_new_saved_message, events.NewMessage(chats="me"))
    client.add_event_handler(handle_deleted_messages, events.MessageDeleted(chats="me"))

    await client.start(phone=PHONE_NUMBER)

    user = await client.get_me()
    print(f"[STATUS] Connected as: {user.first_name} (ID: {user.id})")
    print("[STATUS] Commands: /help, /status, /cancel")
    print("[STATUS] Forward any file to 'Saved Messages' to download it.\n")

    # Start the sequential queue worker
    asyncio.create_task(queue_worker_loop(client))

    # Start background healthcheck heartbeat task if configured
    if HEALTHCHECK_URL:
        asyncio.create_task(healthcheck_heartbeat_loop())

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\n[INFO] Stopped.")
