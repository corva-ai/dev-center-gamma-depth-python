from corva import Corva

from src.gamma_depth import gamma_depth


def lambda_handler(event, context):
    corva = Corva(context=context)
    corva.scheduled(gamma_depth, event)
