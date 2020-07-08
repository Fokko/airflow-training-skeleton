from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

import requests


class RocketLaunchOperator(BaseOperator):
    ui_color = "#555"
    ui_fgcolor = "#fff"

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        query = "https://launchlibrary.net/1.4/launch?startdate={}&enddate={}".format(
            context["ds"], context["tomorrow_ds"]
        )
        print(f"Request: {query}")
        response = requests.get(query)
        print(f"Response was {response}")
        if response.status_code not in {200, 404}:
            # Launch Library returns 404
            # if no rocket launched in given interval.​
            response.raise_for_status()
        return response.json()
