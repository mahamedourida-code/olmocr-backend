from typing import Optional, Union
from pydantic import Field
from pydantic_settings import BaseSettings
import os


class Settings(BaseSettings):
    """Application settings and configuration."""
    
    # OlmOCR API Configuration
    olmocr_api_key: str = Field(..., env="OLMOCR_API_KEY")
    olmocr_base_url: str = Field("https://api.deepinfra.com/v1/openai", env="OLMOCR_BASE_URL")
    olmocr_model: str = Field("allenai/olmOCR-7B-0725-FP8", env="OLMOCR_MODEL")
    
    # Application Configuration
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    max_file_size_mb: int = Field(10, env="MAX_FILE_SIZE_MB")
    max_batch_size: int = Field(100, env="MAX_BATCH_SIZE")
    file_retention_hours: int = Field(24, env="FILE_RETENTION_HOURS")
    
    # Server Configuration
    host: str = Field("0.0.0.0", env="HOST")
    port: int = Field(8080, env="PORT")
    
    # File Storage Configuration
    temp_storage_path: str = Field("/app/temp_storage", env="TEMP_STORAGE_PATH")
    cleanup_interval_hours: int = Field(1, env="CLEANUP_INTERVAL_HOURS")
    
    # API Configuration
    api_v1_prefix: str = Field("/api/v1", env="API_V1_PREFIX")
    docs_url: Optional[str] = Field("/docs", env="DOCS_URL")
    redoc_url: Optional[str] = Field("/redoc", env="REDOC_URL")
    
    # Logging Configuration
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_format: str = Field("json", env="LOG_FORMAT")
    
    # Redis Configuration
    redis_url: str = Field("redis://127.0.0.1:6379/0", env="REDIS_URL")
    redis_max_connections: int = Field(20, env="REDIS_MAX_CONNECTIONS")
    redis_connect_timeout: int = Field(10, env="REDIS_CONNECT_TIMEOUT")
    redis_socket_timeout: int = Field(30, env="REDIS_SOCKET_TIMEOUT")
    
    # Celery Configuration
    celery_broker_url: Optional[str] = Field(None, env="CELERY_BROKER_URL")
    celery_result_backend: Optional[str] = Field(None, env="CELERY_RESULT_BACKEND")
    celery_worker_concurrency: int = Field(2, env="CELERY_WORKER_CONCURRENCY")
    celery_task_serializer: str = Field("json", env="CELERY_TASK_SERIALIZER")
    celery_result_serializer: str = Field("json", env="CELERY_RESULT_SERIALIZER")
    
    # Performance Configuration
    max_concurrent_jobs: int = Field(10, env="MAX_CONCURRENT_JOBS")
    max_concurrent_ocr_calls: int = Field(3, env="MAX_CONCURRENT_OCR_CALLS")
    
    # Rate Limiting Configuration
    rate_limit_per_minute: int = Field(100, env="RATE_LIMIT_PER_MINUTE")
    rate_limit_burst: int = Field(20, env="RATE_LIMIT_BURST")
    
    # OlmOCR API Rate Limiting Configuration
    olmocr_base_delay_seconds: float = Field(2.0, env="OLMOCR_BASE_DELAY_SECONDS")
    olmocr_max_delay_seconds: float = Field(10.0, env="OLMOCR_MAX_DELAY_SECONDS")
    olmocr_jitter_factor: float = Field(0.5, env="OLMOCR_JITTER_FACTOR")
    olmocr_exponential_backoff: bool = Field(True, env="OLMOCR_EXPONENTIAL_BACKOFF")
    
    # CORS Configuration
    allowed_origins: Union[str, list] = Field(["https://exceletto.vercel.app", "http://localhost:3000"], env="ALLOWED_ORIGINS")
    allowed_hosts: Union[str, list] = Field(["*"], env="ALLOWED_HOSTS")
    cors_allow_credentials: bool = Field(True, env="CORS_ALLOW_CREDENTIALS")
    cors_allow_methods: Union[str, list] = Field(["GET", "POST", "PUT", "DELETE", "OPTIONS"], env="CORS_ALLOW_METHODS")
    cors_allow_headers: Union[str, list] = Field(["*"], env="CORS_ALLOW_HEADERS")
    cors_expose_headers: Union[str, list] = Field(["X-Request-ID"], env="CORS_EXPOSE_HEADERS")
    cors_max_age: int = Field(3600, env="CORS_MAX_AGE")
    cors_allow_origin_regex: Optional[str] = Field(r"https://.*\.vercel\.app", env="CORS_ALLOW_ORIGIN_REGEX")
    
    # Job Processing Configuration
    job_expiry_seconds: int = Field(86400, env="JOB_EXPIRY_SECONDS")  # 24 hours
    batch_processing_timeout: int = Field(1800, env="BATCH_PROCESSING_TIMEOUT")  # 30 minutes
    file_cleanup_delay: int = Field(3600, env="FILE_CLEANUP_DELAY")  # 1 hour

    # Concurrency Configuration
    max_concurrent_tasks: int = Field(50, env="MAX_CONCURRENT_TASKS")
    worker_concurrency: int = Field(4, env="WORKER_CONCURRENCY")

    # Supabase Configuration
    supabase_url: str = Field(..., env="SUPABASE_URL")
    supabase_anon_key: str = Field(..., env="SUPABASE_ANON_KEY")
    supabase_service_role_key: Optional[str] = Field(None, env="SUPABASE_SERVICE_ROLE_KEY")
    supabase_jwt_secret: Optional[str] = Field(None, env="SUPABASE_JWT_SECRET")
    supabase_storage_bucket: str = Field("jobs", env="SUPABASE_STORAGE_BUCKET")
    
    @property
    def effective_redis_url(self) -> str:
        """Get the effective Redis URL, preferring Railway Redis if available."""
        # Railway automatically provides REDIS_URL if Redis add-on is enabled
        railway_redis = os.getenv("REDISURL") or os.getenv("REDIS_URL")
        if railway_redis and "railway" in os.getenv("RAILWAY_ENVIRONMENT", ""):
            return railway_redis
        return self.redis_url
    
    @property
    def effective_celery_broker_url(self) -> str:
        """Get the effective Celery broker URL."""
        return self.celery_broker_url or self.effective_redis_url
    
    @property
    def effective_celery_result_backend(self) -> str:
        """Get the effective Celery result backend URL."""
        return self.celery_result_backend or self.effective_redis_url
    
    @property
    def max_file_size_bytes(self) -> int:
        """Convert MB to bytes for file size validation."""
        return self.max_file_size_mb * 1024 * 1024
    
    @property
    def file_retention_seconds(self) -> int:
        """Convert hours to seconds for file retention."""
        return self.file_retention_hours * 3600
    
    @property
    def max_file_size(self) -> int:
        """Convert MB to bytes for file size validation."""
        return self.max_file_size_mb * 1024 * 1024
    
    @property
    def cleanup_interval_seconds(self) -> int:
        """Convert hours to seconds for cleanup interval."""
        return self.cleanup_interval_hours * 3600
    
    @property
    def parsed_allowed_origins(self) -> list:
        """Parse allowed origins from environment variable or list."""
        if isinstance(self.allowed_origins, str):
            # If it's a string, split by comma and strip whitespace
            origins = [origin.strip() for origin in self.allowed_origins.split(",")]
            return [origin for origin in origins if origin]  # Filter out empty strings
        return self.allowed_origins
    
    @property
    def parsed_cors_allow_methods(self) -> list:
        """Parse CORS allowed methods from environment variable or list."""
        if isinstance(self.cors_allow_methods, str):
            methods = [method.strip().upper() for method in self.cors_allow_methods.split(",")]
            return [method for method in methods if method]
        return [method.upper() for method in self.cors_allow_methods]
    
    @property
    def parsed_cors_allow_headers(self) -> list:
        """Parse CORS allowed headers from environment variable or list."""
        if isinstance(self.cors_allow_headers, str):
            headers = [header.strip() for header in self.cors_allow_headers.split(",")]
            return [header for header in headers if header]
        return self.cors_allow_headers
    
    @property
    def parsed_cors_expose_headers(self) -> list:
        """Parse CORS expose headers from environment variable or list."""
        if isinstance(self.cors_expose_headers, str):
            headers = [header.strip() for header in self.cors_expose_headers.split(",")]
            return [header for header in headers if header]
        return self.cors_expose_headers
    
    class Config:
        env_file = ".env"
        case_sensitive = False


def get_settings() -> Settings:
    """Get the global settings instance."""
    return settings


# Global settings instance
settings = Settings()