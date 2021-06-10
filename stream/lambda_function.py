from corva import Api, Cache, StreamTimeEvent, stream

from src.gamma_depth import gamma_depth


@stream
def lambda_handler(event: StreamTimeEvent, api: Api, cache: Cache) -> None:
    gamma_depth(event=event, api=api)
