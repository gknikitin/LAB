import os
import json

import dotenv

dotenv.load_dotenv()

"""Конфигурация API VK"""

REQ_VERSION: float = float(os.getenv("VAR_REQ_VERSION", 5.103))
REQ_COUNT: int = int(os.getenv("VAR_REQ_COUNT", 3))
REQ_FILTER: str = os.getenv("VAR_REQ_FILTER", "owner")

"""Конфигурация работы парсера"""

SINGLE_START: bool = os.getenv("VAR_SINGLE_START", "").lower() in ("true",)
TIME_TO_SLEEP: int = int(os.getenv("VAR_TIME_TO_SLEEP", 10))
SKIP_ADS_POSTS: bool = os.getenv("VAR_SKIP_ADS_POSTS", "").lower() in ("true",)
SKIP_COPYRIGHTED_POST: bool = os.getenv("VAR_SKIP_COPYRIGHTED_POST", "").lower() in ("true")
SKIP_REPOSTS: bool = os.getenv("VAR_SKIP_REPOSTS", "").lower() in ("true")

"""Конфигурация ключевых слов"""

WHITELIST: list = json.loads(os.getenv("VAR_WHITELIST", "[]"))
BLACKLIST: list = json.loads(os.getenv("VAR_BLACKLIST", "[]"))
