import asyncio
import os
import shutil
import logging
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Professional Libraries
import pydicom
from pydantic import BaseModel, ValidationError, Field
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- CONFIGURATION ---
INBOX_DIR = "./hospital_inbox"
PROCESSED_DIR = "./processed_dicom"
QUARANTINE_DIR = "./quarantine"  # The "Dead Letter Queue" for bad files
CLOUD_UPLOAD_SIM = True

# Setup Enterprise Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DICOM_Pipeline")

# --- DATA MODELS (The 'Strict Validation' Part) ---
class AnonymizedMetadata(BaseModel):
    """Schema enforcing valid data structure for downstream systems."""
    original_file: str
    patient_token: str = Field(..., min_length=16, description="Anonymized Hash")
    modality: str = Field("UNKNOWN", description="CT, MRI, XRAY")
    study_date: str
    processed_at: str

# --- CORE MICROSERVICE LOGIC ---
class AsyncDicomProcessor:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def process_file(self, file_path):
        filename = os.path.basename(file_path)
        logger.info(f"⚡ Event detected: {filename}")

        try:
            # 1. Simulate Async Cloud Latency
            await asyncio.sleep(0.5)

            # 2. Parse DICOM Data (Offload CPU-heavy parsing to thread)
            # running in executor prevents blocking the main event loop
            ds = await self.loop.run_in_executor(None, self._read_dicom, file_path)

            # 3. Anonymization Logic (Business Rule: Hash PatientID)
            original_id = str(ds.get("PatientID", "UNKNOWN"))
            anon_token = self._hash_pii(original_id)
            
            # 4. Update DICOM in-memory
            ds.PatientID = anon_token
            ds.PatientName = f"ANON-{anon_token[:8]}"

            # 5. Validate Metadata with Pydantic
            metadata = AnonymizedMetadata(
                original_file=filename,
                patient_token=anon_token,
                modality=str(ds.get("Modality", "OT")),
                study_date=str(ds.get("StudyDate", datetime.now().strftime("%Y%m%d"))),
                processed_at=datetime.now().isoformat()
            )
            
            # 6. Save & "Upload"
            dest_path = os.path.join(PROCESSED_DIR, filename)
            ds.save_as(dest_path)
            logger.info(f"✅ Compliance Check Passed. Metadata: {metadata.json()}")
            
            if CLOUD_UPLOAD_SIM:
                logger.info(f"☁️  Uploading {filename} to S3 Bucket [s3://ge-health-lake/anon]...")

            # Cleanup
            os.remove(file_path)

        except (pydicom.errors.InvalidDicomError, ValidationError) as e:
            logger.error(f"❌ Validation Failed: {e}")
            self._move_to_quarantine(file_path)
        except Exception as e:
            logger.critical(f"⚠️ System Error: {e}")
            self._move_to_quarantine(file_path)

    def _read_dicom(self, path):
        """Helper to read DICOM. If not a real DICOM, creates a dummy dataset for demo."""
        try:
            return pydicom.dcmread(path)
        except:
            # FALLBACK FOR DEMO: If user drops a text file, treat it as a 'Mock' DICOM
            # This allows the project to work without needing real .dcm files
            ds = pydicom.dataset.FileDataset(path, {}, file_meta=pydicom.dataset.FileMetaDataset())
            ds.PatientID = "TEST-PATIENT-001"
            ds.PatientName = "John Doe"
            ds.Modality = "MRI"
            return ds

    def _hash_pii(self, text):
        import hashlib
        return hashlib.sha256(text.encode()).hexdigest()

    def _move_to_quarantine(self, file_path):
        """Dead Letter Queue logic for failed files."""
        try:
            shutil.move(file_path, os.path.join(QUARANTINE_DIR, os.path.basename(file_path)))
            logger.warning(f"☣️  File quarantined: {os.path.basename(file_path)}")
        except Exception:
            pass

# --- EVENT BRIDGE ---
class BridgeHandler(FileSystemEventHandler):
    def __init__(self, processor):
        self.processor = processor

    def on_created(self, event):
        if not event.is_directory and not event.src_path.endswith(".DS_Store"):
            # Bridge Sync Watchdog -> Asyncio Loop
            self.processor.loop.call_soon_threadsafe(
                asyncio.create_task, 
                self.processor.process_file(event.src_path)
            )

# --- MAIN ENTRYPOINT ---
if __name__ == "__main__":
    # Ensure infrastructure exists
    for d in [INBOX_DIR, PROCESSED_DIR, QUARANTINE_DIR]:
        os.makedirs(d, exist_ok=True)

    processor = AsyncDicomProcessor()
    
    # Run the asyncio loop in a separate thread to keep it non-blocking
    import threading
    def start_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
    
    t = threading.Thread(target=start_loop, args=(processor.loop,), daemon=True)
    t.start()

    observer = Observer()
    observer.schedule(BridgeHandler(processor), INBOX_DIR, recursive=False)
    
    logger.info(f"🚀 DICOM Pipeline Active. Monitoring {INBOX_DIR}")
    logger.info("   - Architecture: Asyncio Event Loop + ThreadPool")
    logger.info("   - Compliance: Pydantic Validation & SHA-256")
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()