# Google Colab Telegram Uploader

A highly customizable Python script designed specifically for **Google Colab** to upload files from your mounted Google Drive directly to a Telegram Channel or Group.

By utilizing the [Pyrogram](https://docs.pyrogram.org/) library (an MTProto framework), this script bypasses the standard 50MB Telegram Bot API limit, allowing you to **send files up to 2GB**.

## Features

* **No File Size Limits (Up to 2GB):** Uses your Bot Token combined with your Telegram API ID to send massive files.
* **Native Playable Media:** Photos and videos are uploaded using Telegram's native media methods (`send_photo` and `send_video`), so they can be viewed and played directly inside the Telegram app without needing to download them first as documents.
* **Automatic Captions:** The filename (without the extension) is automatically applied as the caption for every uploaded file.
* **Smart Tracking (No Duplicates):** The script maintains a tiny `uploaded_files_tracker.txt` file inside your target folder. If the script is stopped and restarted later, it will only upload new files.
* **Media Filtering:** Choose to upload only `photos`, only `videos`, `both`, or `all` files in the folder.

## How the "No Duplicate Uploads" System Works

The script is built to ensure that **no file is ever uploaded twice**, even if your Colab runtime crashes, disconnects, or if you stop the script manually.

### How it works:
1. **The Tracker File:** The first time the script runs, it automatically creates a small text file named `uploaded_files_tracker.txt` directly inside the `--folder_path` you specified (your Google Drive folder).
2. **Real-time Saving:** Every time a file finishes uploading successfully to Telegram, the script instantly writes the exact filename into this tracker file.
3. **Checking Before Upload:** The next time you run the script, it reads the tracker file first. It compares the list of already-uploaded files against the files currently in your Drive folder. It will **completely skip** any file that is already in the tracker list.

### How to set it up:
**It requires zero setup!** It is fully automatic. Because the tracker file is saved directly into your mounted Google Drive folder, it permanently persists across different Colab sessions.

### How to manage or reset it:
* **To force re-upload everything:** Simply go to your Google Drive folder and delete the `uploaded_files_tracker.txt` file. The next time the script runs, it will assume nothing has been uploaded.
* **To skip a specific file without uploading it:** You can open `uploaded_files_tracker.txt` in a text editor and manually add the filename on a new line. For example, add `my_video.mp4` on its own line.
* **Batch Limits:** Set a limit on how many files to upload per run to manage your time and bandwidth.

## Setup Instructions for Google Colab

1. **Mount Your Google Drive:** Open a new notebook in Google Colab and run the following code cell to access your files:
   ```python
   from google.colab import drive
   drive.mount('/content/drive')
   ```

2. **Install Dependencies (run in Colab/Notebook cell):** In the next cell, install the required libraries:
   ```bash
   !apt-get update && apt-get install -y ffmpeg
   !pip install pyrogram tgcrypto Pillow ffmpeg-python pymediainfo
   ```

3. **Upload the Script:** Upload `colab_telegram_uploader.py` to your Colab environment.

4. **Get Your Telegram Credentials:**
   * **API ID & API Hash:** Go to [my.telegram.org](https://my.telegram.org), log in, and create an application to get your `api_id` and `api_hash`.
   * **Bot Token:** Talk to [@BotFather](https://t.me/botfather) on Telegram to create a bot and get its token.
   * **Channel ID:** Add your bot to your private channel as an Administrator. You will need the channel's integer ID (usually starting with `-100...`).

## Usage & CLI Arguments

Run the script in a Colab cell using the `!` prefix:

```bash
!python colab_telegram_uploader.py --api_id 1234567 --api_hash your_hash --bot_token your_token --channel_id -1001234567890 --folder_path "/content/drive/MyDrive/MyVideos" [OPTIONS]
```

### Required Arguments
| Argument | Description |
| :--- | :--- |
| `--api_id` | Your integer API ID from my.telegram.org. |
| `--api_hash` | Your string API Hash from my.telegram.org. |
| `--bot_token` | The Bot Token provided by @BotFather. |
| `--channel_id` | The target chat/channel integer ID (e.g. `-1001234567890`). |
| `--folder_path` | The exact path to the folder in your mounted Google Drive (Required for standard mode). |

### Optional Arguments (Controls)
| Argument | Default | Description |
| :--- | :--- | :--- |
| `--upload_bulk` | `None` | Used instead of `--folder_path` to trigger bulk upload mode. Takes two arguments: `<video_folder>` and `<thumb_folder>`. It matches videos and thumbnails by their base filenames, processes the thumbnails, and uploads them together. If a thumbnail is missing, it automatically extracts a frame from the middle of the video. State is saved securely in a `telegram_upload_logs` folder in your Drive Root. |
| `--media_type` | `all` | Filter what to send. Choices: `photos`, `videos`, `both`, `all`. |
| `--limit` | `0` (Unlimited) | Maximum number of files to upload in a single execution. |
| `--workers` | `3` | How many files to upload parallelly/concurrently. Set to `1` if you want a clean progress bar. |

## Examples

### 1. Upload Everything
To simply back up an entire folder of files (documents, zips, photos, etc.) to your channel without limits:
```bash
!python colab_telegram_uploader.py \
  --api_id 1234567 \
  --api_hash abcdef1234567890 \
  --bot_token 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11 \
  --channel_id -1009876543210 \
  --folder_path "/content/drive/MyDrive/MyBackup"
```

### 2. Upload Only Videos
If your folder has a mix of subtitles, text files, and videos, but you only want to send the video files (`.mp4`, `.mkv`, etc.):
```bash
!python colab_telegram_uploader.py \
  --api_id 1234567 \
  --api_hash abcdef1234567890 \
  --bot_token 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11 \
  --channel_id -1009876543210 \
  --folder_path "/content/drive/MyDrive/Anime" \
  --media_type videos
```

### 3. Upload a Batch of Photos
If you have a massive folder of photos and only want to send 50 of them today:
```bash
!python colab_telegram_uploader.py \
  --api_id 1234567 \
  --api_hash abcdef1234567890 \
  --bot_token 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11 \
  --channel_id -1009876543210 \
  --folder_path "/content/drive/MyDrive/Vacation_Pics" \
  --media_type photos \
  --limit 50
```
*(The script remembers which 50 it sent. Running this exact command again tomorrow will send the **next** 50 photos!)*

### 4. Upload Both Photos and Videos (Skip documents)
To back up all your visual media but ignore `.pdf`, `.zip`, or text files in that directory:
```bash
!python colab_telegram_uploader.py \
  --api_id 1234567 \
  --api_hash abcdef1234567890 \
  --bot_token 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11 \
  --channel_id -1009876543210 \
  --folder_path "/content/drive/MyDrive/Camera_Uploads" \
  --media_type both
```

### 5. Bulk Upload Videos with Matching Thumbnails
If you have a folder full of videos and a separate folder full of their respective thumbnails (with matching base filenames), use `--upload_bulk`. The script will auto-pair them, standardize the thumbnail to Telegram's requirements (using PIL), generate fallback frames if a thumb is missing (using FFmpeg), and securely log the state locally inside your drive:
```bash
!python colab_telegram_uploader.py \
  --api_id 1234567 \
  --api_hash abcdef1234567890 \
  --bot_token 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11 \
  --channel_id -1009876543210 \
  --upload_bulk "/content/drive/MyDrive/Series/Videos" "/content/drive/MyDrive/Series/Thumbs"
```
