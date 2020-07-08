# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from rocket_launch_operator import RocketLaunchOperator
from airflow.operators.python_operator import PythonOperator

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(10)}

dag = DAG(
    dag_id="fifth_exercise",
    default_args=args,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
)

download_rocket_launches = RocketLaunchOperator(
    task_id="download_rocket_launches", dag=dag
)


def _print_stats(ds, task_instance, **context):
    data = task_instance.xcom_pull(task_ids="download_rocket_launches")
    print("Date from xcom: {}".format(data))
    rockets_launched = [launch["name"] for launch in data["launches"]]
    rockets_str = ""

    if rockets_launched:
        rockets_str = f" ({' & '.join(rockets_launched)})"
        print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")
    else:
        print(f"No rockets found in {f.name}")


print_stats = PythonOperator(
    task_id="print_stats", python_callable=_print_stats, provide_context=True, dag=dag
)

download_rocket_launches >> print_stats
