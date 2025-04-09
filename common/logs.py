import os
import datetime

def log(message):
    # Create "logs" directory if it does not exist
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Generate log filename based on the current date
    log_filename = f"logs_{datetime.date.today()}.log"
    log_path = os.path.join(log_dir, log_filename)

    # Prepare log entry with timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"

    # ✅ Ensure log file uses UTF-8 encoding
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(log_entry)

    print(f"Log written: {log_entry.strip()}")  # Optional: Print log entry to console
