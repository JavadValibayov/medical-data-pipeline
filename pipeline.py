import sys
import time
import os
import json
import hashlib
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- CONFIGURATION ---
INBOX_DIR = "./hospital_inbox"
PROCESSED_DIR = "./anonymized_data"
AUDIT_LOG = "audit_trail.json"

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SYSTEM] - %(message)s')

class AnonymizationHandler(FileSystemEventHandler):
    def __init__(self, executor):
        self.executor = executor

    def on_created(self, event):
        if not event.is_directory and not event.src_path.endswith(".DS_Store"):
            # Offload heavy processing to a separate thread (High Performance)
            self.executor.submit(self.process_medical_record, event.src_path)

    def process_medical_record(self, file_path):
        """Simulates stripping PII (Personally Identifiable Information) from a medical record."""
        try:
            filename = os.path.basename(file_path)
            logging.info(f"Detected new record: {filename}")
            
            # 1. Wait for file write to complete
            time.sleep(0.5)

            # 2. Read 'Sensitive' Data
            with open(file_path, 'r') as f:
                content = f.read()

            # 3. Anonymization Logic (The 'Healthcare' part)
            # We hash the content to simulate removing the Patient Name but keeping a unique ID
            patient_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
            anonymized_content = {
                "original_file": filename,
                "patient_token": f"ANON-{patient_hash}", # Replaces Real Name
                "status": "CLEARED_FOR_VISUALIZATION",
                "timestamp": time.time()
            }

            # 4. Save to Processed Directory (The 'Pipeline' part)
            output_path = os.path.join(PROCESSED_DIR, f"processed_{filename}.json")
            with open(output_path, 'w') as f:
                json.dump(anonymized_content, f, indent=4)

            # 5. Audit Logging (The 'Compliance' part)
            self.log_audit(filename, patient_hash)

            logging.info(f"Successfully anonymized: {filename} -> {output_path}")
            
            # 6. Cleanup original file (Simulate moving data out of unsecured zone)
            os.remove(file_path)

        except Exception as e:
            logging.error(f"Failed to process {file_path}: {e}")

    def log_audit(self, filename, hash_val):
        """Append to an audit trail for FDA compliance."""
        entry = {"file": filename, "hash": hash_val, "action": "ANONYMIZED", "time": time.ctime()}
        with open(AUDIT_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")

if __name__ == "__main__":
    # Ensure directories exist
    os.makedirs(INBOX_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Use a ThreadPool to handle multiple files simultaneously (Concurrency)
    executor = ThreadPoolExecutor(max_workers=4)
    
    event_handler = AnonymizationHandler(executor)
    observer = Observer()
    observer.schedule(event_handler, INBOX_DIR, recursive=False)
    
    logging.info(f"Pipeline Service Started. Monitoring {INBOX_DIR}...")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        executor.shutdown()
    observer.join()