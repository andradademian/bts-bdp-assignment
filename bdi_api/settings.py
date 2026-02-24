from os.path import dirname, join

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

import bdi_api

PROJECT_DIR = dirname(dirname(bdi_api.__file__))


class Settings(BaseSettings):
    source_url: str = Field(
        default="https://samples.adsbexchange.com/readsb-hist",
        description="Base URL to the website used to download the data.",
    )
    local_dir: str = Field(
        default=join(PROJECT_DIR, "data"),
        description="For any other value set env variable 'BDI_LOCAL_DIR'",
    )
    s3_bucket: str = Field(
        default="bdi-test",
        description="Call the api like `BDI_S3_BUCKET=yourbucket uvicorn ...`",
    )
    db_url: str = Field(
        default="sqlite:///hr_database.db",
        description="Database URL. Set BDI_DB_URL for PostgreSQL, e.g. postgresql://postgres:postgres@localhost:5432/hr_database",
    )
    mongo_url: str = Field(
        default="mongodb://localhost:27017",
        description="MongoDB connection URL. Set BDI_MONGO_URL for remote, e.g. mongodb+srv://user:pass@cluster.mongodb.net",
    )

    model_config = SettingsConfigDict(env_prefix="bdi_")

    @property
    def raw_dir(self) -> str:
        return join(self.local_dir, "raw")

    @property
    def prepared_dir(self) -> str:
        return join(self.local_dir, "prepared")
