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
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(10)}

dag = DAG(
    dag_id="fourth_exercise",
    default_args=args,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
)

weekday_person_to_email = {
    0: "Bob",  # Monday
    1: "Joe",  # Tuesday
    2: "Alice",  # Wednesday
    3: "Joe",  # Thursday
    4: "Alice",  # Friday​
    5: "Alice",  # Saturday
    6: "Alice",  # Sunday
}

weekdays = {
    0: "Monday",
    1: "Tuesday​",
    2: "Wednesday​",
    3: "Thursday​",
    4: "Friday​",
    5: "Saturday​",
    6: "Sunday​",
}


def get_weekday(execution_date, **kwargs):
    print("Today it is: {}".format(weekdays[execution_date.weekday()]))


print_execution_time = PythonOperator(
    task_id="print_weekday", dag=dag, python_callable=get_weekday, provide_context=True
)


def get_on_call(execution_date, **kwargs):
    return weekday_person_to_email[execution_date.weekday()]


branching = BranchPythonOperator(
    task_id="branching", dag=dag, python_callable=get_on_call, provide_context=True
)

print_execution_time >> branching

final_task = DummyOperator(task_id="final_task", dag=dag)

for person in set(weekday_person_to_email.values()):
    branching >> DummyOperator(task_id=person, dag=dag) >> final_task
