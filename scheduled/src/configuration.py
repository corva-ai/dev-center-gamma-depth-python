import pydantic


class Settings(pydantic.BaseSettings):
    provider: str
    actual_gamma_depth_collection: str = 'actual-gamma-depth'
    drillstring_collection: str = 'data.drillstring'
    wits_collection = 'wits'
    version: int = 1


SETTINGS = Settings()
