"""
Google Colab Telegram Uploader

This script uploads files from a mounted Google Drive folder to a Telegram Channel using a Telegram Bot.
It supports files up to 2GB by using the Pyrogram MTProto library.

To run this on Google Colab, first install the dependencies:
!pip install pyrogram tgcrypto

Usage Example:
python colab_telegram_uploader.py --api_id 12345 --api_hash abcde --bot_token 123:abc --channel_id -1001234567890 --folder_path /content/drive/MyDrive/MyFolder --media_type all --limit 0
"""

import os
import sys
import json
import re
import argparse
import asyncio
import mimetypes
import urllib.request
import subprocess
import datetime
from pyrogram import Client
from pyrogram.errors import FloodWait

import time
from typing import Optional, Tuple


def append_error_log(filename: str, content: str) -> str:
    try:
        drive_logs = os.path.join(get_drive_root(), "telegram_upload_logs")
        os.makedirs(drive_logs, exist_ok=True)
        log_path = os.path.join(drive_logs, "error_dump.txt")
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"--- Error dump for {filename} at {datetime.datetime.now().isoformat()} ---\n")
            f.write(content + '\n\n')
        return "error_dump.txt"
    except Exception as e:
        print(f"Error writing to error dump log: {e}")
        return ""



def probe_duration_with_err(path: str):
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', path]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
        if result.returncode == 0:
            duration_str = result.stdout.strip()
            if duration_str:
                return float(duration_str), None
        return None, result.stderr.strip() or "ffprobe failed with no stderr output"
    except Exception as e:
        return None, str(e)


def probe_duration(path: str) -> Tuple[Optional[float], Optional[str]]:
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', path]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
        if result.returncode == 0:
            duration_str = result.stdout.strip()
            if duration_str:
                return float(duration_str), None
        return None, result.stderr.strip() or "ffprobe failed with no stderr output"
    except Exception as e:
        return None, str(e)

def remux_container(path: str) -> Optional[str]:
    try:
        drive_root = get_drive_root(path)
        temp_dir = os.path.join(drive_root, "temp_upload_processing")
        os.makedirs(temp_dir, exist_ok=True)
        filename = os.path.basename(path)
        name, ext = os.path.splitext(filename)
        out_path = os.path.join(temp_dir, f"{name}_remuxed_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}{ext}")
        cmd = ['ffmpeg', '-y', '-i', path, '-c', 'copy', out_path]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=300)
        if result.returncode == 0 and os.path.exists(out_path):
            return out_path
        return None
    except Exception as e:
        print(f"Error in remux_container: {e}")
        return None

def prepare_thumbnail_if_needed(thumb_path: str) -> str:
    try:
        drive_root = get_drive_root(thumb_path)
        temp_dir = os.path.join(drive_root, "temp_upload_processing")
        os.makedirs(temp_dir, exist_ok=True)
        res = prepare_thumbnail(thumb_path, temp_dir)
        if res and os.path.exists(res):
            return res
    except Exception as e:
        print(f"PIL prepare_thumbnail failed: {e}")

    # Fallback to ffmpeg conversion if PIL fails or is unavailable
    try:
        drive_root = get_drive_root(thumb_path)
        temp_dir = os.path.join(drive_root, "temp_upload_processing")
        os.makedirs(temp_dir, exist_ok=True)
        out_path = os.path.join(temp_dir, f"ffmpeg_thumb_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}.jpg")
        cmd = ['ffmpeg', '-y', '-i', thumb_path, '-vframes', '1', '-q:v', '2', out_path]
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        if os.path.exists(out_path):
            return out_path
    except Exception as e:
        print(f"ffmpeg fallback thumbnail conversion failed: {e}")

    return thumb_path

def clean_caption(caption):
    """
    Removes any URLs or domain names from the caption string.
    """
    # Regex to match HTTP/HTTPS URLs or simple domain names (e.g. www.example.com, example.org)
    # Using a more strict pattern for domains to avoid matching regular dotted text like "My.Video.mkv"
    url_pattern = r'(https?://\S+|www\.\S+|\b[a-zA-Z0-9-]+\.(?:com|org|net|io|co|us|uk|info|biz|me|tv|cc)\b(?:/\S*)?)'

    # Substitute the URLs with empty string
    cleaned = re.sub(url_pattern, '', caption)

    # Clean up extra spaces that might be left behind
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # Remove dangling leading hyphens/pipes
    cleaned = re.sub(r'^[-_|]\s*', '', cleaned)
    # Remove dangling trailing hyphens/pipes (optional)
    cleaned = re.sub(r'\s*[-_|]$', '', cleaned)

    return cleaned.strip()

def pyrogram_progress_wrapper(total_bytes: int, filename: str):
    start_time = time.time()
    def progress(current: int, total: int):
        now = time.time()
        elapsed = now - start_time
        if elapsed > 0:
            speed = current / elapsed
            eta = (total - current) / speed if speed > 0 else 0
        else:
            speed = 0
            eta = 0
        percent = (current / total) * 100 if total > 0 else 0
        speed_mb = speed / (1024 * 1024)
        print(f"\r{filename} | Size: {total/(1024*1024):.1f}MB | Uploaded: {current/(1024*1024):.1f}MB | {percent:.1f}% | Speed: {speed_mb:.1f}MB/s | ETA: {int(eta)}s", end="")
        if current >= total:
            print()
    return progress

try:
    from PIL import Image
except ImportError:
    pass # PILLOW may not be installed in all environments, we will try to handle it.

# Constants
TRACKER_FILE_NAME = "uploaded_files_tracker.txt"

# Extend mimetypes map for common media
mimetypes.add_type('video/mkv', '.mkv')
mimetypes.add_type('video/webm', '.webm')
mimetypes.add_type('video/mp4', '.mp4')
mimetypes.add_type('image/jpeg', '.jpg')
mimetypes.add_type('image/jpeg', '.jpeg')
mimetypes.add_type('image/png', '.png')
mimetypes.add_type('image/webp', '.webp')


def load_uploaded_files(tracker_file_path):
    """
    Loads the set of already uploaded filenames from a text tracker file.
    If the file doesn't exist, it checks for a legacy JSON file and migrates it.
    Returns an empty set if neither exist.
    """
    uploaded = set()

    # Check for legacy JSON tracker to ensure backward compatibility
    legacy_json_path = tracker_file_path.replace(".txt", ".json")
    if os.path.exists(legacy_json_path) and not os.path.exists(tracker_file_path):
        try:
            with open(legacy_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    uploaded = set(data)
                    print(f"Migrating legacy JSON tracker ({len(uploaded)} files) to text format...")
                    # Save them to the new txt format
                    with open(tracker_file_path, 'w', encoding='utf-8') as txt_f:
                        for filename in uploaded:
                            txt_f.write(filename + '\n')
                    return uploaded
        except Exception as e:
            print(f"Warning: Failed to parse legacy JSON tracker {legacy_json_path}. Error: {e}")

    # Load from text tracker
    if os.path.exists(tracker_file_path):
        try:
            with open(tracker_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    filename = line.strip()
                    if filename:
                        uploaded.add(filename)
        except Exception as e:
            print(f"Warning: Failed to read {tracker_file_path}. Creating a new tracker. Error: {e}")

    return uploaded

def append_uploaded_file(tracker_file_path, filename):
    """
    Appends a single uploaded filename to the text tracker file.
    """
    try:
        with open(tracker_file_path, 'a', encoding='utf-8') as f:
            f.write(filename + '\n')
    except Exception as e:
        print(f"Error: Failed to append to tracker {tracker_file_path}. Error: {e}")

def append_log_tsv(logs_folder, filename, data):
    """
    Appends a tab-separated list of items to the specified log file.
    """
    import time
    filepath = os.path.join(logs_folder, filename)
    retries = 3
    for attempt in range(retries):
        try:
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write("\t".join(str(item) for item in data) + "\n")
            break
        except OSError as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"Error: Failed to append to log {filepath}. Error: {e}")
        except Exception as e:
            print(f"Error: Failed to append to log {filepath}. Error: {e}")
            break

def append_log(line: str):
    """
    Opens the chosen log file in append mode and writes a newline-terminated line.
    (Defaults to telegram_upload_logs/upload_log.txt in the drive root)
    """
    import time
    retries = 3
    for attempt in range(retries):
        try:
            drive_logs = os.path.join(get_drive_root(), "telegram_upload_logs")
            os.makedirs(drive_logs, exist_ok=True)
            log_path = os.path.join(drive_logs, "upload_log.txt")
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
            break
        except OSError as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"Error writing to upload log: {e}")
        except Exception as e:
            print(f"Error writing to upload log: {e}")
            break

def load_matched_pairs(logs_folder):
    """
    Loads previously matched pairs from matched_pairs.txt to prevent duplicate uploads.
    Returns a set of video file paths (or IDs) that were successfully uploaded.
    """
    import os
    import time
    matched = set()
    filepath = os.path.join(logs_folder, "matched_pairs.txt")

    if os.path.exists(filepath):
        retries = 3
        for attempt in range(retries):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.strip().split('	')
                        if len(parts) >= 2:
                            matched.add(parts[1]) # parts[1] is the video_drive_id_or_path
                break # Success
            except OSError as e:
                print(f"Warning: Colab Drive connection dropped while reading {filepath}. Retrying in {2 ** attempt}s... ({e})")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
                break
    return matched

def get_drive_root(path=None):
    """
    Attempts to extract the Google Drive root path from a given path.
    Typically on Colab this is /content/drive/MyDrive.
    If no path is given, or if it doesn't match the typical Colab pattern,
    it defaults to /content/drive/MyDrive if it exists, otherwise os.getcwd().
    """
    if path:
        path = os.path.abspath(path)
        if path.startswith('/content/drive/MyDrive'):
            return '/content/drive/MyDrive'
        elif path.startswith('/content/drive/Shareddrives'):
            # For shared drives, the root is usually /content/drive/Shareddrives/<DriveName>
            parts = path.split(os.sep)
            if len(parts) >= 5:
                return os.sep.join(parts[:5])
            return '/content/drive/Shareddrives'
        return os.path.dirname(path)

    # Absolute fallback
    if os.path.exists('/content/drive/MyDrive'):
        return '/content/drive/MyDrive'
    return os.getcwd()

def get_session_path(args, fallback_path=None):
    """
    Returns the directory to save the Pyrogram user session file.
    Priority:
    1. --session_path argument if provided
    2. Google Drive root derived from fallback_path (a folder_path or upload_bulk path)
    3. Absolute fallback to Google Drive Root (/content/drive/MyDrive)
    """
    if args.session_path:
        return args.session_path
    if fallback_path:
        drive_root = get_drive_root(fallback_path)
        return drive_root
    return get_drive_root()


def prepare_thumbnail(thumb_path, temp_folder):
    """
    Validates and standardizes a thumbnail to meet Telegram's requirements:
    - JPEG format
    - max 320x320 px (maintaining aspect ratio)
    - max 200 KB
    Returns the path to the normalized thumbnail (which may be a temp file),
    or None if normalization failed.
    """
    if 'PIL' not in sys.modules:
        print("PIL (Pillow) not found. Skipping advanced thumbnail processing. "
              "Please ensure thumbnails are pre-formatted.")
        return thumb_path

    try:
        from PIL import Image
        img = Image.open(thumb_path)

        # Check size and format
        needs_resize = img.width > 320 or img.height > 320
        needs_convert = img.format != 'JPEG'

        # Even if it looks ok, it could be > 200KB. Check file size.
        file_size_kb = os.path.getsize(thumb_path) / 1024
        needs_compress = file_size_kb > 200

        if not (needs_resize or needs_convert or needs_compress):
            return thumb_path

        # Needs processing
        if img.mode != 'RGB':
            img = img.convert('RGB')

        img.thumbnail((320, 320), Image.Resampling.LANCZOS)

        filename = f"thumb_{os.path.basename(thumb_path)}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}.jpg"
        temp_path = os.path.join(temp_folder, filename)

        # Save with optimization to hit < 200KB
        quality = 85
        img.save(temp_path, format='JPEG', quality=quality, optimize=True)

        # Double check size, reduce quality if needed
        while os.path.getsize(temp_path) / 1024 > 200:
            quality = max(10, quality - 10)
            img.save(temp_path, format='JPEG', quality=quality, optimize=True)
            if quality <= 10:
                break

        return temp_path
    except Exception as e:
        print(f"Error preparing thumbnail {thumb_path}: {e}")
        return None

def generate_fallback_thumbnail(video_path, temp_folder):
    """
    Uses ffmpeg to extract a frame from the middle of the video.
    Returns the path to the generated thumbnail, or None if failed.
    """
    filename = f"fallback_{os.path.basename(video_path)}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}.jpg"
    temp_path = os.path.join(temp_folder, filename)

    try:
        # Get duration first to find the middle
        duration_cmd = ['ffprobe', '-v', 'error', '-show_entries',
                        'format=duration', '-of',
                        'default=noprint_wrappers=1:nokey=1', video_path]

        duration_proc = subprocess.run(duration_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            duration = float(duration_proc.stdout.strip())
            timestamp = str(duration / 2)
        except ValueError:
            timestamp = "00:00:01" # Fallback if we can't parse duration

        # Extract frame
        extract_cmd = [
            'ffmpeg', '-y', '-ss', timestamp, '-i', video_path,
            '-vframes', '1', '-q:v', '2', temp_path
        ]

        subprocess.run(extract_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

        # Now normalize it using prepare_thumbnail
        normalized_path = prepare_thumbnail(temp_path, temp_folder)

        # If prepare_thumbnail created a new file, clean up the original extracted one
        if normalized_path and normalized_path != temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

        return normalized_path
    except Exception as e:
        print(f"Error generating fallback thumbnail for {video_path}: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return None


def parse_args():
    parser = argparse.ArgumentParser(description="Upload files from a local directory (like a mounted Google Drive) to a Telegram channel.")

    # Telegram Credentials
    parser.add_argument("--api_id", type=int, required=True, help="Your Telegram API ID (get it from my.telegram.org)")
    parser.add_argument("--api_hash", type=str, required=True, help="Your Telegram API Hash")
    parser.add_argument("--bot_token", type=str, required=True, help="Your Telegram Bot Token (from @BotFather)")

    # Target Configuration
    parser.add_argument("--channel_id", type=int, required=True, help="The integer ID of the target channel (e.g. -1001234567890)")
    parser.add_argument("--folder_path", type=str, required=False, help="Path to the folder you want to upload from")

    # Bulk Upload Configuration
    parser.add_argument("--upload_bulk", nargs=2, metavar=('VIDEO_FOLDER', 'THUMB_FOLDER'), required=False,
                        help="Bulk upload mode: Provide the video folder and thumbnail folder paths")

    # Behavior Configuration
    parser.add_argument("--media_type", type=str, choices=['photos', 'videos', 'both', 'all'], default='all',
                        help="Filter which type of files to upload. 'both' means photos and videos. 'all' means absolutely everything.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Maximum number of files to upload in this run. 0 means unlimited.")
    parser.add_argument("--workers", type=int, default=3,
                        help="Number of files to upload concurrently. Default is 3.")

    # Deduplication Configuration
    parser.add_argument("--delete_duplicates", action="store_true",
                        help="Scan the channel history and delete older duplicate videos with exactly the same caption.")
    parser.add_argument("--session_path", type=str, default=None,
                        help="Directory where the Pyrogram user session file will be saved. Should be a Google Drive path to persist across Colab restarts, e.g. /content/drive/MyDrive")

    return parser.parse_args()

def is_matching_media_type(filename, filter_type):
    """
    Checks if a file matches the requested media type filter based on mime type.
    """
    if filter_type == 'all':
        return True

    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type:
        return False

    is_video = mime_type.startswith('video/')
    is_photo = mime_type.startswith('image/')

    if filter_type == 'videos' and is_video:
        return True
    if filter_type == 'photos' and is_photo:
        return True
    if filter_type == 'both' and (is_video or is_photo):
        return True

    return False


async def upload_worker(app, queue, args, tracker_file, uploaded_files, uploaded_count_ref, progress_lock):
    while True:
        try:
            filename = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        file_path = os.path.join(args.folder_path, filename)
        caption = clean_caption(os.path.splitext(filename)[0])

        # Define a closure for progress so it doesn't garble output from other workers
        # We only print progress if there's a single worker to prevent messy logs
        async def prog_cb(current, total):
            if args.workers == 1:
                percent = (current / total) * 100
                import sys
                sys.stdout.write(f"\rUploading {filename}... {percent:.1f}%")
                sys.stdout.flush()



        print(f"\n--- Processing: {filename} ---")
        while True:
            try:
                mime_type, _ = mimetypes.guess_type(filename)
                is_video = mime_type and mime_type.startswith('video/')
                is_photo = mime_type and mime_type.startswith('image/')

                msg = None
                if is_video:
                    msg = await app.send_video(
                        chat_id=args.channel_id,
                        video=file_path,
                        caption=caption,
                        progress=prog_cb
                    )

                    # Check for zero duration
                    if msg and msg.video and getattr(msg.video, 'duration', -1) == 0:
                        drive_root = get_drive_root(file_path)
                        print(f"\nZero duration detected for {filename}. Initiating recovery flow...")
                        dur, err_str = probe_duration_with_err(file_path)
                        if dur and dur > 0:
                            print(f"ffprobe returned duration {dur}s. Re-sending...")
                            await msg.delete()
                            msg2 = await app.send_video(
                                chat_id=args.channel_id,
                                video=file_path,
                                caption=caption,
                                duration=int(dur),
                                progress=prog_cb
                            )
                            append_log(f"{datetime.datetime.now().isoformat()} | {filename} | RESENT | orig_msg_id={msg.id} | new_msg_id={msg2.id} | duration={int(dur)} | remux=no")
                        else:
                            print(f"ffprobe failed or duration 0. Attempting remux...")
                            remuxed_path = remux_container(file_path)
                            if remuxed_path:
                                dur_remux, _ = probe_duration_with_err(remuxed_path)
                                if dur_remux and dur_remux > 0:
                                    print(f"Remux successful. Re-sending...")
                                    await msg.delete()
                                    msg2 = await app.send_video(
                                        chat_id=args.channel_id,
                                        video=remuxed_path,
                                        caption=caption,
                                        duration=int(dur_remux),
                                        progress=prog_cb
                                    )
                                    append_log(f"{datetime.datetime.now().isoformat()} | {filename} | RESENT | orig_msg_id={msg.id} | new_msg_id={msg2.id} | duration={int(dur_remux)} | remux=yes")
                                    try: os.remove(remuxed_path)
                                    except: pass
                                else:
                                    print(f"Remux failed to yield valid duration. Logging failure.")
                                    sz = os.path.getsize(file_path) / (1024*1024)
                                    append_log(f"{datetime.datetime.now().isoformat()} | {filename} | FAIL | size={sz:.1f}MB | ffprobe_error=\"See error_dump.txt\"")
                                    append_error_log(filename, err_str or "ffprobe error")
                                    try: os.remove(remuxed_path)
                                    except: pass
                            else:
                                print(f"Remuxing failed completely. Logging failure.")
                                sz = os.path.getsize(file_path) / (1024*1024)
                                append_log(f"{datetime.datetime.now().isoformat()} | {filename} | FAIL | size={sz:.1f}MB | ffprobe_error=\"See error_dump.txt\"")
                                append_error_log(filename, err_str or "ffprobe error")
                    elif msg and msg.video:
                        drive_root = get_drive_root(file_path)
                        append_log(f"{datetime.datetime.now().isoformat()} | {filename} | SENT | msg_id={msg.id} | duration={getattr(msg.video, 'duration', 0)}")
                elif is_photo:
                    await app.send_photo(
                        chat_id=args.channel_id,
                        photo=file_path,
                        caption=caption,
                        progress=prog_cb
                    )
                else:
                    await app.send_document(
                        chat_id=args.channel_id,
                        document=file_path,
                        caption=caption,
                        progress=prog_cb
                    )

                # Use lock to safely update shared state
                async with progress_lock:
                    uploaded_files.add(filename)
                    append_uploaded_file(tracker_file, filename)
                    uploaded_count_ref[0] += 1

                print(f"\nSuccessfully uploaded: {filename}")
                await asyncio.sleep(3) # Hardcoded 3-second sleep between files
                break # Success! Break out of the while loop

            except FloodWait as e:
                print(f"\nFloodWait! Telegram says to wait {e.value} seconds. Sleeping...")
                await asyncio.sleep(e.value)
                continue # Retry from the top
            except Exception as e:
                print(f"\nFailed to upload {filename}. Error: {e}")
                break # Stop retrying for this file on other errors

        queue.task_done()


async def upload_files_async(args):
    """
    Main asynchronous loop to upload files.
    """
    # Create pyrogram client
    app = Client(
        "drive_uploader_bot",
        api_id=args.api_id,
        api_hash=args.api_hash,
        bot_token=args.bot_token,
        in_memory=True # Don't create session files
    )

    tracker_file = os.path.join(args.folder_path, TRACKER_FILE_NAME)
    uploaded_files = load_uploaded_files(tracker_file)
    print(f"Loaded {len(uploaded_files)} already uploaded files from tracker.")

    # Scan directory
    try:
        all_files = sorted(os.listdir(args.folder_path))
    except Exception as e:
        print(f"Error reading directory {args.folder_path}: {e}")
        return

    files_to_upload = []
    for f in all_files:
        if f == TRACKER_FILE_NAME:
            continue

        file_path = os.path.join(args.folder_path, f)
        if not os.path.isfile(file_path):
            continue

        if f in uploaded_files:
            continue

        if not is_matching_media_type(f, args.media_type):
            continue

        files_to_upload.append(f)

    print(f"Found {len(files_to_upload)} new files matching filter '{args.media_type}'.")

    if args.limit > 0:
        files_to_upload = files_to_upload[:args.limit]
        print(f"Limiting to {len(files_to_upload)} files for this run.")

    if not files_to_upload:
        print("Nothing to upload!")
        return

    # Start bot
    try:
        await app.start()
        print("Successfully connected to Telegram.")

        # Because we use an in_memory=True session with a bot token, Pyrogram doesn't natively
        # cache the private channel's access_hash, which triggers "PEER_ID_INVALID" on MTProto calls.
        # Bots can't use get_dialogs(), and injecting InputPeerChannel into app.storage fails serialization.
        #
        # THE FIX:
        # We ping the channel using the standard HTTP Bot API! The HTTP API doesn't require an access_hash.
        # By sending an actual dummy message via HTTP, Telegram instantly broadcasts an MTProto update
        # back to our Pyrogram client. Pyrogram automatically catches this update and seamlessly extracts
        # and caches the correct channel peer structure into its in-memory storage.
        print("Initializing channel cache via Bot API ping...")

        import json
        url = f"https://api.telegram.org/bot{args.bot_token}/sendMessage?chat_id={args.channel_id}&text=Initializing+Upload+Cache..."
        req = urllib.request.Request(url, method='POST')

        try:
            res = urllib.request.urlopen(req)
            data = json.loads(res.read())
            msg_id = data['result']['message_id']

            # Give Pyrogram a moment to process the incoming MTProto update and populate its cache
            await asyncio.sleep(2)

            # Clean up the dummy message via HTTP
            url_del = f"https://api.telegram.org/bot{args.bot_token}/deleteMessage?chat_id={args.channel_id}&message_id={msg_id}"
            urllib.request.urlopen(urllib.request.Request(url_del, method='POST'))

            # Verify the cache works by resolving the chat using MTProto
            chat = await app.get_chat(args.channel_id)
            print(f"Successfully cached and resolved target channel: {chat.title}")

        except urllib.error.HTTPError as e:
            print(f"Error: The bot could not access the channel via HTTP API. Is the bot an admin? Error: {e.read().decode()}")
            return

    except Exception as e:
        print(f"Error connecting to Telegram. Check your credentials and Channel ID: {e}")
        return

    queue = asyncio.Queue()
    for f in files_to_upload:
        queue.put_nowait(f)

    progress_lock = asyncio.Lock()
    uploaded_count_ref = [0]

    print(f"Starting {args.workers} concurrent upload workers...")

    workers = []
    for _ in range(args.workers):
        worker = asyncio.create_task(
            upload_worker(app, queue, args, tracker_file, uploaded_files, uploaded_count_ref, progress_lock)
        )
        workers.append(worker)

    # Wait for all items in the queue to be processed
    await queue.join()

    # Cancel workers that are now just waiting for queue.get()
    for w in workers:
        w.cancel()

    print(f"\nFinished uploading {uploaded_count_ref[0]} files!")

    # We must stop the bot client first before running deduplication with the user client
    await app.stop()

    # Run deduplication automatically
    await execute_deduplication_flow(args)

async def upload_bulk_async(args, video_map, thumb_map, logs_folder, temp_folder):
    """
    Main asynchronous loop for bulk uploading matching videos and thumbnails.
    """
    app = Client(
        "drive_uploader_bot_bulk",
        api_id=args.api_id,
        api_hash=args.api_hash,
        bot_token=args.bot_token,
        in_memory=True
    )


    try:
        await app.start()
        print("Successfully connected to Telegram.")

        print("Initializing channel cache via Bot API ping...")
        url = f"https://api.telegram.org/bot{args.bot_token}/sendMessage?chat_id={args.channel_id}&text=Initializing+Upload+Cache..."
        req = urllib.request.Request(url, method='POST')

        try:
            res = urllib.request.urlopen(req)
            data = json.loads(res.read())
            msg_id = data['result']['message_id']

            await asyncio.sleep(2)

            url_del = f"https://api.telegram.org/bot{args.bot_token}/deleteMessage?chat_id={args.channel_id}&message_id={msg_id}"
            urllib.request.urlopen(urllib.request.Request(url_del, method='POST'))

            chat = await app.get_chat(args.channel_id)
            print(f"Successfully cached and resolved target channel: {chat.title}")
        except urllib.error.HTTPError as e:
            print(f"Error: The bot could not access the channel via HTTP API. Is the bot an admin? Error: {e.read().decode()}")
            return

    except Exception as e:
        print(f"Error connecting to Telegram: {e}")
        return

    matched_pairs_set = load_matched_pairs(logs_folder)
    print(f"Loaded {len(matched_pairs_set)} already uploaded videos from tracker.")

    # Process pairs sequentially or using a small bounded pool
    # The prompt allows an internal worker pool; we'll keep it simple & sequential
    # for clear logs and streaming stability, since file I/O is local drive.

    upload_count = 0

    for basename, video_paths in video_map.items():
        if args.limit > 0 and upload_count >= args.limit:
            print(f"Reached configured limit of {args.limit} files. Stopping bulk upload.")
            break
        timestamp = datetime.datetime.now().isoformat()

        # 1. Duplicates check
        if len(video_paths) > 1:
            print(f"Skipping {basename}: Multiple videos found.")
            append_log_tsv(logs_folder, "duplicate_basenames.txt", [timestamp, basename, ",".join(video_paths)])
            continue

        video_path = video_paths[0]

        # Skip if already uploaded
        if video_path in matched_pairs_set:
            continue

        thumb_paths = thumb_map.get(basename, [])

        # Case-insensitive fallback lookup
        if not thumb_paths:
            for thumb_basename, paths in thumb_map.items():
                if thumb_basename.lower() == basename.lower():
                    thumb_paths = paths
                    break

        if len(thumb_paths) > 1:
            print(f"Skipping {basename}: Multiple thumbnails found.")
            append_log_tsv(logs_folder, "duplicate_basenames.txt", [timestamp, basename, ",".join(thumb_paths)])
            continue

        thumb_path = thumb_paths[0] if thumb_paths else None

        # 2. Thumbnail handling
        final_thumb_path = None
        loop = asyncio.get_event_loop()

        if thumb_path:
            final_thumb_path = await loop.run_in_executor(None, prepare_thumbnail, thumb_path, temp_folder)
            if not final_thumb_path:
                print(f"Failed to normalize thumbnail for {basename}, proceeding without it.")
                append_log_tsv(logs_folder, "failed_uploads.txt", [timestamp, video_path, f"Thumbnail normalization failed for {thumb_path}"])
        else:
            print(f"No thumbnail found for {basename}. Appending to unmatched_videos.txt and attempting fallback generation.")
            append_log_tsv(logs_folder, "unmatched_videos.txt", [timestamp, video_path])
            final_thumb_path = await loop.run_in_executor(None, generate_fallback_thumbnail, video_path, temp_folder)
            if not final_thumb_path:
                print(f"Fallback generation failed for {basename}. Proceeding without thumbnail.")

        # Also check for unmatched thumbnails just for logging purposes
        if thumb_path and not video_map.get(basename):
            # This condition is conceptually here if we iterated thumb_map instead,
            # but since we iterate video_map, we'll do a quick secondary pass for unmatched thumbs.
            pass

        # 3. Uploading
        caption = clean_caption(basename)
        print(f"\n--- Uploading: {basename} (Caption: {caption}) ---")
        try:
            # Simple retry logic (3 attempts)
            max_retries = 3
            msg = None
            attempt = 0
            while attempt < max_retries:
                try:
                    msg = await app.send_video(
                        chat_id=args.channel_id,
                        video=video_path,
                        caption=caption,
                        thumb=final_thumb_path,
                        progress=pyrogram_progress_wrapper(os.path.getsize(video_path), basename)
                    )

                    if msg and msg.video and getattr(msg.video, 'duration', -1) == 0:
                        print(f"\nZero duration detected for {basename}. Initiating recovery flow...")
                        drive_root = get_drive_root(video_path)
                        dur, err_str = probe_duration_with_err(video_path)

                        # Prepare thumbnail specifically for resend as per requirements
                        resend_thumb = prepare_thumbnail_if_needed(thumb_path) if thumb_path else final_thumb_path

                        if dur and dur > 0:
                            print(f"ffprobe returned duration {dur}s. Re-sending...")
                            await msg.delete()
                            msg2 = await app.send_video(
                                chat_id=args.channel_id,
                                video=video_path,
                                caption=caption,
                                duration=int(dur),
                                thumb=resend_thumb,
                                progress=pyrogram_progress_wrapper(os.path.getsize(video_path), basename)
                            )
                            append_log(f"{datetime.datetime.now().isoformat()} | {basename} | RESENT | orig_msg_id={msg.id} | new_msg_id={msg2.id} | duration={int(dur)} | remux=no")
                            msg = msg2
                        else:
                            print(f"ffprobe failed or duration 0. Attempting remux...")
                            remuxed_path = remux_container(video_path)
                            if remuxed_path:
                                dur_remux, _ = probe_duration_with_err(remuxed_path)
                                if dur_remux and dur_remux > 0:
                                    print(f"Remux successful. Re-sending...")
                                    await msg.delete()
                                    msg2 = await app.send_video(
                                        chat_id=args.channel_id,
                                        video=remuxed_path,
                                        caption=caption,
                                        duration=int(dur_remux),
                                        thumb=resend_thumb,
                                        progress=pyrogram_progress_wrapper(os.path.getsize(remuxed_path), basename)
                                    )
                                    append_log(f"{datetime.datetime.now().isoformat()} | {basename} | RESENT | orig_msg_id={msg.id} | new_msg_id={msg2.id} | duration={int(dur_remux)} | remux=yes")
                                    msg = msg2
                                    try: os.remove(remuxed_path)
                                    except: pass
                                else:
                                    print(f"Remux failed to yield valid duration. Logging failure.")
                                    sz = os.path.getsize(video_path) / (1024*1024)
                                    append_log(f"{datetime.datetime.now().isoformat()} | {basename} | FAIL | size={sz:.1f}MB | ffprobe_error=\"See error_dump.txt\"")
                                    append_error_log(basename, err_str or "ffprobe error")
                                    try: os.remove(remuxed_path)
                                    except: pass
                            else:
                                print(f"Remuxing failed completely. Logging failure.")
                                sz = os.path.getsize(video_path) / (1024*1024)
                                append_log(f"{datetime.datetime.now().isoformat()} | {basename} | FAIL | size={sz:.1f}MB | ffprobe_error=\"See error_dump.txt\"")
                                append_error_log(basename, err_str or "ffprobe error")
                    elif msg and msg.video:
                        drive_root = get_drive_root(video_path)
                        append_log(f"{datetime.datetime.now().isoformat()} | {basename} | SENT | msg_id={msg.id} | duration={getattr(msg.video, 'duration', 0)}")

                    break # Success
                except FloodWait as e:
                    print(f"\nFloodWait! Telegram says to wait {e.value} seconds. Sleeping...")
                    await asyncio.sleep(e.value)
                    continue # Do not increment attempt counter for flood wait
                except Exception as e:
                    print(f"Attempt {attempt+1} failed for {basename}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt) # Exponential backoff
                    else:
                        raise e
                attempt += 1

            # Log success
            append_log_tsv(logs_folder, "matched_pairs.txt", [timestamp, video_path, thumb_path or "No_Thumb", msg.id])
            matched_pairs_set.add(video_path)
            upload_count += 1
            print(f"Successfully uploaded: {basename}")
            await asyncio.sleep(3) # Hardcoded 3-second sleep between files

        except Exception as e:
            print(f"Failed to upload {basename}. Error: {e}")
            append_log_tsv(logs_folder, "failed_uploads.txt", [timestamp, video_path, str(e)])

        finally:
            # Cleanup temp thumbnail
            if final_thumb_path and final_thumb_path != thumb_path and os.path.exists(final_thumb_path):
                try:
                    os.remove(final_thumb_path)
                except Exception as e:
                    print(f"Failed to clean up temp file {final_thumb_path}: {e}")

    # Check for unmatched thumbnails
    timestamp = datetime.datetime.now().isoformat()

    # Build a set of lowercased video basenames for easy case-insensitive checking
    video_basenames_lower = {b.lower() for b in video_map.keys()}

    for basename, thumb_paths in thumb_map.items():
        if basename.lower() not in video_basenames_lower:
            for path in thumb_paths:
                append_log_tsv(logs_folder, "unmatched_thumbs.txt", [timestamp, path])

    print(f"\nBulk upload finished!")

    # We must stop the bot client first before running deduplication with the user client
    await app.stop()

    # Run deduplication automatically
    await execute_deduplication_flow(args)

async def remove_duplicate_videos(app, channel_id, logs_folder):
    """
    Scans the channel history to find and delete duplicate videos based on their exact captions.
    It performs a comprehensive scan by always checking newest messages (for continuous uploads)
    and continuing historical scans where it left off, avoiding redundant checks.
    Deletions are logged in a CSV file.
    """
    print("\n--- Starting Deduplication Scan ---")

    seen_captions_path = os.path.join(logs_folder, "seen_captions.json")
    scan_state_path = os.path.join(logs_folder, "deduplication_state.json")

    # User requested the CSV file in the root directory of drive
    drive_root = os.path.dirname(logs_folder) if "telegram_upload_logs" in logs_folder else get_drive_root(logs_folder)
    deleted_csv_path = os.path.join(drive_root, "deleted_duplicates.csv")

    seen_captions = set()
    state = {
        "highest_scanned_id": 0,    # The ID of the very first message we successfully checked (newest)
        "lowest_scanned_id": 0,     # The ID of the oldest message we successfully checked
        "historical_scan_done": False # Whether we have reached the very beginning of the channel history
    }

    # Load previously seen captions
    if os.path.exists(seen_captions_path):
        try:
            with open(seen_captions_path, "r", encoding="utf-8") as f:
                seen_captions = set(json.load(f))
            print(f"Loaded {len(seen_captions)} previously seen captions.")
        except Exception as e:
            print(f"Warning: Could not load seen_captions.json: {e}")

    # Load scan state
    if os.path.exists(scan_state_path):
        try:
            with open(scan_state_path, "r", encoding="utf-8") as f:
                loaded_state = json.load(f)
                state.update(loaded_state)
            print(f"Loaded deduplication state: {state}")
        except Exception as e:
            print(f"Warning: Could not load deduplication_state.json: {e}")

    # Legacy migration: If old last_scanned_message_id.txt exists, migrate it
    legacy_id_path = os.path.join(logs_folder, "last_scanned_message_id.txt")
    if os.path.exists(legacy_id_path) and state["lowest_scanned_id"] == 0:
        try:
            with open(legacy_id_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content.isdigit():
                    state["lowest_scanned_id"] = int(content)
                    print(f"Migrated legacy scan ID: {state['lowest_scanned_id']}")
        except Exception as e:
            pass

    # Create CSV header if it doesn't exist
    if not os.path.exists(deleted_csv_path):
        try:
            with open(deleted_csv_path, "w", encoding="utf-8") as f:
                f.write("Timestamp,MessageID,Caption\n")
        except Exception as e:
            print(f"Warning: Could not create deleted_duplicates.csv: {e}")

    messages_to_delete = []
    total_deleted = 0
    scanned_count = 0

    # We will do two passes if needed:
    # Pass 1: "Recent Scan". Scan from the absolute newest message downwards, until we hit
    # the highest_scanned_id from the *previous* run. This covers new uploads.
    # Pass 2: "Historical Scan". If the historical scan hasn't reached the beginning of the
    # channel yet, resume it from lowest_scanned_id downwards.

    # Helper function to process a single message
    async def process_message(message, current_id):
        nonlocal messages_to_delete, seen_captions, deleted_csv_path
        if getattr(message, 'video', None) and getattr(message, 'caption', None):
            caption = message.caption.strip()

            if caption in seen_captions:
                print(f"Found duplicate video with caption: '{caption}' (Message ID: {current_id})")
                messages_to_delete.append(current_id)

                # Real-time CSV updating as requested
                try:
                    with open(deleted_csv_path, "a", encoding="utf-8") as f:
                        escaped_caption = caption.replace('"', '""')
                        timestamp = datetime.datetime.now().isoformat()
                        f.write(f'"{timestamp}",{current_id},"{escaped_caption}"\n')
                        f.flush() # Force write to disk immediately
                except Exception as e:
                    print(f"Warning: Could not write to deleted_duplicates.csv: {e}")
            else:
                seen_captions.add(caption)

    # Helper function to flush batch deletions
    async def flush_deletions():
        nonlocal messages_to_delete, total_deleted
        if messages_to_delete:
            print(f"Deleting batch of {len(messages_to_delete)} duplicate messages...")
            try:
                await app.delete_messages(chat_id=channel_id, message_ids=messages_to_delete)
                total_deleted += len(messages_to_delete)
                messages_to_delete.clear()
                # Deletion can also be rate limited if done too frequently
                await asyncio.sleep(3)
            except FloodWait as e:
                print(f"FloodWait: Waiting for {e.value} seconds before continuing deletions...")
                await asyncio.sleep(e.value + 1)
                # Retry once
                await app.delete_messages(chat_id=channel_id, message_ids=messages_to_delete)
                total_deleted += len(messages_to_delete)
                messages_to_delete.clear()
            except Exception as e:
                print(f"Error deleting batch: {e}")
                messages_to_delete.clear()

    # Helper function to save state
    def save_state():
        try:
            with open(seen_captions_path, "w", encoding="utf-8") as f:
                json.dump(list(seen_captions), f)
            with open(scan_state_path, "w", encoding="utf-8") as f:
                json.dump(state, f)
        except Exception as e:
            print(f"Warning: Could not save state: {e}")

    # --- PASS 1: RECENT MESSAGES SCAN ---
    print(f"\n[Pass 1] Scanning for recently uploaded messages (down to {state['highest_scanned_id']})...")

    current_batch_size = 100
    current_delay = 1.0
    offset_id = 0  # 0 = start from the absolute newest message
    done = False
    new_highest_id = None
    end_recent = state["highest_scanned_id"]

    while not done:
        try:
            messages = app.get_chat_history(
                chat_id=channel_id,
                limit=current_batch_size,
                offset_id=offset_id
            )

            # Convert async generator to list for len check and processing
            messages_list = []
            async for m in messages:
                messages_list.append(m)

            if not messages_list:
                break

            last_id_in_batch = None
            for message in messages_list:
                if not message or message.empty:
                    continue

                current_id = message.id

                if new_highest_id is None:
                    new_highest_id = current_id

                if current_id <= end_recent:
                    done = True
                    break

                scanned_count += 1
                last_id_in_batch = current_id

                if state["lowest_scanned_id"] == 0 or current_id < state["lowest_scanned_id"]:
                    state["lowest_scanned_id"] = current_id

                await process_message(message, current_id)

            if len(messages_to_delete) >= 50:
                await flush_deletions()

            if scanned_count % 500 == 0 and scanned_count > 0:
                print(f"Scanned {scanned_count} recent messages. Saving progress...")
                if new_highest_id: state["highest_scanned_id"] = new_highest_id
                save_state()

            if last_id_in_batch is None or done:
                break

            offset_id = last_id_in_batch
            current_delay = max(0.5, current_delay * 0.95)
            await asyncio.sleep(current_delay)

        except FloodWait as e:
            print(f"\nFloodWait triggered! Waiting {e.value}s before resuming...")
            await asyncio.sleep(e.value + 1)
            current_delay = min(5.0, current_delay * 1.5)
            print(f"Adapting scanner: new_delay={current_delay:.2f}s")
            # offset_id is not advanced, loop continues with same offset_id
        except Exception as e:
            print(f"Error fetching recent messages: {e}")
            break

    if new_highest_id:
        state["highest_scanned_id"] = new_highest_id

    await flush_deletions()

    # --- PASS 2: HISTORICAL MESSAGES SCAN ---
    if not state["historical_scan_done"] and state["lowest_scanned_id"] > 0:
        print(f"\n[Pass 2] Resuming historical scan from message ID {state['lowest_scanned_id']} downwards to 1...")

        offset_id = state["lowest_scanned_id"]
        empty_batches_count = 0

        while offset_id > 0:
            try:
                messages = app.get_chat_history(
                    chat_id=channel_id,
                    limit=current_batch_size,
                    offset_id=offset_id
                )

                # Convert async generator to list
                messages_list = []
                async for m in messages:
                    messages_list.append(m)

                if not messages_list:
                    state["historical_scan_done"] = True
                    state["lowest_scanned_id"] = 1
                    break

                valid_messages_found = False
                last_id_in_batch = None

                for message in messages_list:
                    if not message or message.empty:
                        continue

                    valid_messages_found = True
                    current_id = message.id
                    scanned_count += 1
                    last_id_in_batch = current_id

                    if current_id < state["lowest_scanned_id"]:
                        state["lowest_scanned_id"] = current_id

                    await process_message(message, current_id)

                if not valid_messages_found:
                    empty_batches_count += 1
                else:
                    empty_batches_count = 0

                if empty_batches_count >= 5:
                    state["historical_scan_done"] = True
                    state["lowest_scanned_id"] = 1
                    break

                if len(messages_to_delete) >= 50:
                    await flush_deletions()

                if scanned_count % 500 == 0 and scanned_count > 0:
                    print(f"Scanned {scanned_count} total messages. Saving progress...")
                    save_state()

                if last_id_in_batch is None:
                    offset_id = max(0, offset_id - current_batch_size)
                else:
                    offset_id = last_id_in_batch

                current_delay = max(0.5, current_delay * 0.95)
                await asyncio.sleep(current_delay)

            except FloodWait as e:
                print(f"\nFloodWait triggered! Waiting {e.value}s before resuming...")
                await asyncio.sleep(e.value + 1)
                current_delay = min(5.0, current_delay * 1.5)
                print(f"Adapting scanner: new_delay={current_delay:.2f}s")
                # offset_id is not advanced, loop continues with same offset_id
            except Exception as e:
                print(f"Error fetching historical messages: {e}")
                break

    await flush_deletions()

    print("\nSaving final deduplication state...")
    save_state()
    print(f"--- Deduplication Complete: Scanned {scanned_count} messages this run, Deleted {total_deleted} duplicates. ---")

async def execute_deduplication_flow(args):
    """
    Creates a User Pyrogram Client and runs the deduplication logic.
    """
    fallback = args.folder_path or (args.upload_bulk[0] if args.upload_bulk else None)
    session_dir = get_session_path(args, fallback_path=fallback)
    os.makedirs(session_dir, exist_ok=True)
    session_name = os.path.join(session_dir, "drive_uploader_user")

    session_file = session_name + ".session"
    if not os.path.exists(session_file):
        print("="*60)
        print("FIRST RUN: Pyrogram user authentication required.")
        print("You will be prompted for your phone number and a")
        print("verification code sent by Telegram.")
        print(f"Session will be saved to: {session_file}")
        print("This only happens once. Future runs will reuse this session.")
        print("="*60)

    app = Client(
        session_name,
        api_id=args.api_id,
        api_hash=args.api_hash
    )

    try:
        await app.start()
        print("Successfully connected to Telegram for deduplication.")

        try:
            # Sync user dialogs to cache peers into the local database
            print("Syncing user dialogs to populate peer cache...")
            async for _ in app.get_dialogs(limit=50):
                pass

            chat = await app.get_chat(args.channel_id)
            print(f"Successfully resolved target channel: {chat.title}")
        except Exception as e:
            print(f"Error: Could not access the channel. Are you a member? Error: {e}")
            # Do NOT call await app.stop() here as the finally block handles it safely.
            return

        logs_folder = None
        if args.folder_path:
            logs_folder = os.path.join(get_drive_root(args.folder_path), "telegram_upload_logs")
        elif args.upload_bulk:
            logs_folder = os.path.join(get_drive_root(args.upload_bulk[0]), "telegram_upload_logs")
        else:
            # Fallback to absolute Google Drive root
            logs_folder = os.path.join(get_drive_root(), "telegram_upload_logs")

        os.makedirs(logs_folder, exist_ok=True)

        await remove_duplicate_videos(app, args.channel_id, logs_folder)

    except Exception as e:
        print(f"Error connecting to Telegram: {e}")
    finally:
        await app.stop()

async def run_deduplication_only(args):
    """
    Runs only the deduplication process without uploading any files.
    """
    await execute_deduplication_flow(args)

def main():
    args = parse_args()

    # Check if only deduplication should run
    if args.delete_duplicates and not args.folder_path and not args.upload_bulk:
        asyncio.run(run_deduplication_only(args))
        return

    if args.upload_bulk:
        video_folder, thumb_folder = args.upload_bulk
        if not os.path.isdir(video_folder):
            print(f"Error: Video directory '{video_folder}' does not exist.")
            sys.exit(1)
        if not os.path.isdir(thumb_folder):
            print(f"Error: Thumbnail directory '{thumb_folder}' does not exist.")
            sys.exit(1)

        drive_root = get_drive_root(video_folder)
        logs_folder = os.path.join(drive_root, "telegram_upload_logs")
        temp_folder = os.path.join(drive_root, "temp_upload_processing")

        os.makedirs(logs_folder, exist_ok=True)
        os.makedirs(temp_folder, exist_ok=True)

        # Generate matching maps
        print(f"Bulk upload starting... Logs at: {logs_folder}, Temp at: {temp_folder}")

        video_map = {}
        for f in os.listdir(video_folder):
            path = os.path.join(video_folder, f)
            if os.path.isfile(path):
                basename = os.path.splitext(f)[0]
                video_map.setdefault(basename, []).append(path)

        thumb_map = {}
        for f in os.listdir(thumb_folder):
            path = os.path.join(thumb_folder, f)
            if os.path.isfile(path):
                basename = os.path.splitext(f)[0]
                thumb_map.setdefault(basename, []).append(path)

        # We need an event loop to run the async upload bulk logic
        asyncio.run(upload_bulk_async(args, video_map, thumb_map, logs_folder, temp_folder))
    else:
        if not args.folder_path:
            print("Error: Either --folder_path or --upload_bulk must be provided.")
            sys.exit(1)
        if not os.path.isdir(args.folder_path):
            print(f"Error: Directory '{args.folder_path}' does not exist.")
            sys.exit(1)
        # Run async loop
        asyncio.run(upload_files_async(args))

if __name__ == "__main__":
    main()
