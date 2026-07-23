import logging
import logging.config
from pathlib import Path
from typing import Dict, Any, Optional
import yaml

class LoggerManager:
    _instance: Optional['LoggerManager'] = None
    _loggers: Dict[str, logging.Logger] = {}

    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: Optional[str] = None):
        if self._initialized:
            return
        self.config_path = config_path or Path(__file__).parent / "config" / "logger_config.yaml"
        self.config = self._load_config()
        self._configure_logging()
        self._initialized = True

    def _load_config(self) -> Dict[str, Any]:
        """Load logger config từ YAML, với fallback mặc định."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Fallback config mặc định
            return {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "format": '{"time":"%(asctime)s", "level":"%(levelname)s", "message":"%(message)s", "caller":"%(pathname)s:%(lineno)d"}',
                        "datefmt": "%Y-%m-%dT%H:%M:%S"
                    }
                },
                "handlers": {
                    "console": {
                        "class": "logging.StreamHandler",
                        "formatter": "default",
                        "level": "INFO"
                    },
                    "file_debug": {
                        "class": "logging.handlers.RotatingFileHandler",
                        "filename": str(Path(__file__).parent / "logs" / "debug.log"),
                        "maxBytes": 10 * 1024 * 1024,
                        "backupCount": 3,
                        "formatter": "default",
                        "level": "DEBUG"
                    },
                },
                "loggers": {
                    "root": {
                        "level": "INFO",
                        "handlers": ["console", "file_debug"]
                    }
                }
            }

    def _ensure_handler_dirs(self) -> None:
        """Create directories for any file-based handlers before dictConfig."""
        for handler in self.config.get("handlers", {}).values():
            filename = handler.get("filename")
            if filename:
                Path(filename).parent.mkdir(parents=True, exist_ok=True)

    def _configure_logging(self):
        """Cấu hình logging từ dict."""
        self._ensure_handler_dirs()
        logging.config.dictConfig(self.config)

    def configure_from_project_config(self, project_config_path: str) -> None:
        """Load logger routing from project YAML config and merge into current config."""
        try:
            with open(project_config_path, 'r', encoding='utf-8') as f:
                project_config = yaml.safe_load(f) or {}
        except Exception:
            return

        project_logger = project_config.get('project_params', {}).get('logger', {})
        if not isinstance(project_logger, dict):
            return

        # Apply the base logger settings if provided
        root_level = project_logger.get('level')
        if root_level and isinstance(root_level, str):
            self.config.setdefault('loggers', {}).setdefault('root', {})['level'] = root_level.upper()

        def _build_section(prefix: str, node: Any):
            if not isinstance(node, dict):
                return
            if 'files' in node and isinstance(node['files'], dict):
                logger_name = f"logger{prefix}"
                logger_level = node.get('level', project_logger.get('level', 'INFO')).upper()
                handlers = []
                storage_path = node.get('storage_path', 'shared/logger/logs')
                for level_name, filename in node['files'].items():
                    handler_name = f"{logger_name}.{level_name}"
                    handlers.append(handler_name)
                    self.config.setdefault('handlers', {})[handler_name] = {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'filename': str(Path(storage_path) / filename),
                        'maxBytes': node.get('max_size_mb', 10485760) * 1024 * 1024 if node.get('max_size_mb') else 10485760,
                        'backupCount': node.get('backup_count', 3),
                        'formatter': 'default',
                        'level': level_name.upper(),
                    }
                self.config.setdefault('loggers', {})[logger_name] = {
                    'level': logger_level,
                    'handlers': handlers,
                    'propagate': False,
                }
                return
            for key, value in node.items():
                _build_section(f".{key}", value)

        for section_name in ['ingestion_log', 'storage_log', 'processing_log', 'governance_log']:
            section = project_logger.get(section_name)
            _build_section(f".{section_name}", section)

        self._ensure_handler_dirs()
        logging.config.dictConfig(self.config)

    def get_logger(self, module_name: str) -> logging.Logger:
        """
        Lấy logger cho module cụ thể.
        :param module_name: Tên module (sử dụng __name__), ví dụ: 'platforms.ingestion.cophieu68.extract'
        :return: Logger instance
        """
        if module_name in self._loggers:
            return self._loggers[module_name]

        # Preserve explicit logger names for governance/ingestion/storage keys.
        if module_name.startswith("logger."):
            logger_name = module_name
        else:
            logger_name = f"etl.{module_name.replace('.', '_')}"

        logger = logging.getLogger(logger_name)

        # Nếu config có logger cụ thể cho module, sử dụng; ngược lại dùng root
        if logger_name in self.config.get("loggers", {}):
            pass  # Đã cấu hình trong dictConfig
        else:
            logger.setLevel(logging.INFO)  # Fallback

        self._loggers[module_name] = logger
        return logger

# Singleton instance
logger_manager = LoggerManager()