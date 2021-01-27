from corva import Corva

from src.gamma_depth import gamma_depth


def lambda_handler(event, context):
    corva = Corva()
    corva.stream(gamma_depth, event, filter_by_timestamp=True)
