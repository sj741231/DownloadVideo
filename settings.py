# -*- coding:utf-8 -*-
"""
settings for dlvideo project.
"""
# Request URL for get temporary download URL
BASE_URL = "http://demo.polyv.net/dlsource/5c0ad4c56c.php"

# Download file storage root path
DL_ROOT_PATH = "/data/download"

# Force download sign, default False
FORCE_DOWNLOAD = False

# ThreadPoolExecutor max_workers
MAX_WORKERS = 2

# Thread task execution waiting time(s). as_completed
TASK_WAITING_TIME = 3600 * 10
