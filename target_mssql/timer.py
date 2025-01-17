from datetime import datetime

class Timer:
    def __init__(self, logger, label="Code block"):
        self.label = label
        self.logger = logger

    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting '{self.label}'...")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        elapsed_time = (datetime.now() - self.start_time).total_seconds()
        minutes, seconds = divmod(int(elapsed_time), 60)
        self.logger.info(f"'{self.label}' took {minutes}:{seconds:02d} (mm:ss) to run.")

