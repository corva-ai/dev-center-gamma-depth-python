import pydantic


class Settings(pydantic.BaseSettings):
    provider: str
    collection: str = 'actual-gamma-depth'
    version: int = 1


SETTINGS = Settings()
