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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(10)}

dag = DAG(
    dag_id="third_exercise",
    default_args=args,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
)


def print_date(execution_date, **kwargs):
    print("The execution_date is: {}".format(execution_date))


print_execution_time = PythonOperator(
    task_id="task1", dag=dag, python_callable=print_date, provide_context=True
)
the_end = DummyOperator(task_id="the_end", dag=dag)

for seconds in {1, 5, 10}:
    print_execution_time >> BashOperator(
        task_id="sleep_{}".format(seconds),
        bash_command="sleep {}".format(seconds),
        dag=dag,
    ) >> the_end
