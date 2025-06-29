import os
import json
import subprocess
import time
import datetime
import logging
import shutil

# --- Configuration ---
EVENT_BUS_DIR = "event_bus"
QUEUE_DIR = os.path.join(EVENT_BUS_DIR, "queue")
IN_PROGRESS_DIR = os.path.join(EVENT_BUS_DIR, "in_progress")
COMPLETED_DIR = os.path.join(EVENT_BUS_DIR, "completed")
LOG_DIR = "logs"
PYTHON_EXECUTABLE = "python"  # Or specific path to python3 if needed

# --- Setup Logging ---
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"runner_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()  # Also print to console
    ]
)

def get_task_files():
    """Scans the queue directory for new task files (JSON)."""
    if not os.path.exists(QUEUE_DIR):
        logging.warning(f"Queue directory {QUEUE_DIR} does not exist. Runner will idle.")
        return []
    return sorted([f for f in os.listdir(QUEUE_DIR) if f.endswith(".json")])

def move_task_file(task_filename, source_dir, dest_dir, suffix=""):
    """Moves a task file between directories, optionally adding a suffix."""
    source_path = os.path.join(source_dir, task_filename)

    if suffix:
        base, ext = os.path.splitext(task_filename)
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        new_task_filename = f"{base}{ext}{suffix}.{timestamp}"
    else:
        new_task_filename = task_filename

    dest_path = os.path.join(dest_dir, new_task_filename)

    try:
        os.makedirs(dest_dir, exist_ok=True) # Ensure dest_dir exists
        shutil.move(source_path, dest_path)
        logging.info(f"Moved task '{task_filename}' from '{source_dir}' to '{dest_path}'")
        return dest_path
    except Exception as e:
        logging.error(f"Error moving task '{task_filename}' from '{source_dir}' to '{dest_dir}': {e}")
        return None

def run_task(task_filepath_in_progress, task_filename_original):
    """Executes a task defined in a JSON file."""
    try:
        with open(task_filepath_in_progress, 'r') as f:
            task_config = json.load(f)
    except Exception as e:
        logging.error(f"Error reading task file '{task_filepath_in_progress}': {e}")
        move_task_file(os.path.basename(task_filepath_in_progress), IN_PROGRESS_DIR, IN_PROGRESS_DIR, suffix=".failed_read_error")
        return

    app_name = task_config.get("app_name")
    params = task_config.get("params", {})

    if not app_name:
        logging.error(f"Task file '{task_filepath_in_progress}' is missing 'app_name'.")
        move_task_file(os.path.basename(task_filepath_in_progress), IN_PROGRESS_DIR, IN_PROGRESS_DIR, suffix=".failed_missing_app_name")
        return

    app_script_path = os.path.join("apps", app_name, "run.py") # Convention: apps/<app_name>/run.py

    if not os.path.exists(app_script_path):
        logging.error(f"Application script '{app_script_path}' for app_name '{app_name}' not found.")
        move_task_file(os.path.basename(task_filepath_in_progress), IN_PROGRESS_DIR, IN_PROGRESS_DIR, suffix=".failed_script_not_found")
        return

    command = [PYTHON_EXECUTABLE, app_script_path]
    for param_name, param_value in params.items():
        command.append(f"--{param_name.replace('_', '-')}") # Convert snake_case to kebab-case for argparse
        command.append(str(param_value))

    logging.info(f"Executing task '{task_filename_original}': {' '.join(command)}")

    env = os.environ.copy()
    env["PYTHONPATH"] = "." # Ensure project root is in PYTHONPATH

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        stdout, stderr = process.communicate()

        if stdout:
            logging.info(f"--- STDOUT for {task_filename_original} ---")
            for line in stdout.splitlines():
                logging.info(line)
            logging.info(f"--- END STDOUT for {task_filename_original} ---")

        if process.returncode == 0:
            logging.info(f"Task '{task_filename_original}' completed successfully.")
            move_task_file(os.path.basename(task_filepath_in_progress), IN_PROGRESS_DIR, COMPLETED_DIR, suffix=".completed")
        else:
            logging.error(f"Task '{task_filename_original}' failed with return code {process.returncode}.")
            if stderr:
                logging.error(f"--- STDERR for {task_filename_original} ---")
                for line in stderr.splitlines():
                    logging.error(line)
                logging.error(f"--- END STDERR for {task_filename_original} ---")
            move_task_file(os.path.basename(task_filepath_in_progress), IN_PROGRESS_DIR, IN_PROGRESS_DIR, suffix=".failed")

    except Exception as e:
        logging.error(f"Exception during execution of task '{task_filename_original}': {e}")
        # Attempt to log stderr if available from a Popen exception context (less common)
        if hasattr(e, 'stderr') and e.stderr:
             logging.error(f"--- STDERR (from exception) for {task_filename_original} ---")
             for line in e.stderr.splitlines():
                logging.error(line)
             logging.error(f"--- END STDERR (from exception) for {task_filename_original} ---")
        move_task_file(os.path.basename(task_filepath_in_progress), IN_PROGRESS_DIR, IN_PROGRESS_DIR, suffix=".failed_exception")


def main_loop():
    """Main loop to monitor queue and process tasks."""
    logging.info("Runner started. Monitoring queue...")
    while True:
        task_files = get_task_files()
        if not task_files:
            time.sleep(5)  # Wait if queue is empty
            continue

        for task_filename in task_files:
            logging.info(f"Found new task: {task_filename}")

            # Move to in_progress
            in_progress_filepath = move_task_file(task_filename, QUEUE_DIR, IN_PROGRESS_DIR)

            if in_progress_filepath:
                run_task(in_progress_filepath, task_filename)
            else:
                logging.error(f"Failed to move task '{task_filename}' to in_progress. Skipping.")

        time.sleep(1) # Brief pause before rescanning queue

if __name__ == "__main__":
    # Ensure event bus directories exist
    os.makedirs(QUEUE_DIR, exist_ok=True)
    os.makedirs(IN_PROGRESS_DIR, exist_ok=True)
    os.makedirs(COMPLETED_DIR, exist_ok=True)

    try:
        main_loop()
    except KeyboardInterrupt:
        logging.info("Runner stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logging.critical(f"Runner encountered a critical error and will exit: {e}", exc_info=True)
    finally:
        logging.info("Runner shutting down.")
