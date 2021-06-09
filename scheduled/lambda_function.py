from corva import Api, Cache, ScheduledEvent, scheduled

from src.gamma_depth import gamma_depth


@scheduled
def lambda_handler(event: ScheduledEvent, api: Api, cache: Cache) -> None:
    gamma_depth(event=event, api=api)
