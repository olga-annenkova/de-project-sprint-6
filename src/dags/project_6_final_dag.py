import logging
import pendulum
from airflow.decorators import dag, task
import boto3
import os
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.hooks.base_hook import BaseHook

log = logging.getLogger(__name__)

# Файл, который будем грузить из s3 group_log.csv
FILE_TO_DOWNLOAD = 'group_log.csv'

# даг посчитает всю задачу финального проекта
# от загрузки из s3 до вывода конверсии по 10 самым старым группам

@dag(
    dag_id='load_group_log_and_calculate_conversion',
    schedule_interval=None, # запускаем руками
    start_date=pendulum.datetime(2025, 7, 6, tz="UTC"),
    catchup=False,
    tags=['s3', 'groups', 'conversion'],
    is_paused_upon_creation=False,
)
def load_group_log_and_calculate_conversion_dag():

    @task()
    def fetch_s3_file(key: str):
        # ключи в подключении airflow
        conn = BaseHook.get_connection('yandex_s3')
        aws_access_key_id = conn.login
        aws_secret_access_key = conn.password
        extra = conn.extra_dejson
        endpoint_url = extra.get('endpoint_url', 'https://storage.yandexcloud.net')

        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        output_dir = '/data'
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, key)

        s3_client.download_file(
            Bucket='sprint6',
            Key=key,
            Filename=output_path
        )
        log.info(f"Скачиваем {key} - {output_path}")

    @task()
    def print_first_10_lines():
        filepath = os.path.join('/data', FILE_TO_DOWNLOAD)
        print(f"----- {FILE_TO_DOWNLOAD} -----")
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 10:
                        break
                    print(line.rstrip())
        except FileNotFoundError:
            print("файла нет")

    @task()
    def load_group_log_to_staging():
        vertica_hook = VerticaHook(vertica_conn_id='vertica_conn')
        conn = vertica_hook.get_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                COPY STV202507096__STAGING.group_log (
                    group_id,
                    user_id,
                    user_id_from,
                    event,
                    datetime
                )
                FROM LOCAL '/data/group_log.csv'
                DELIMITER ','
                NULL ''
                REJECTED DATA AS TABLE STV202507096__STAGING.group_log_rejected
            """)
            conn.commit()
            log.info("Загрузили group_log ")

        except Exception as e:
            log.error(f"ошибка: {str(e)}")
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    @task()
    def calculate_group_conversion():
        vertica_hook = VerticaHook(vertica_conn_id='vertica_conn')
        conn = vertica_hook.get_conn()
        cur = conn.cursor()
        
        try:
            # Создаем таблицу 
            cur.execute("""
                CREATE TABLE IF NOT EXISTS STV202507096__DWH.groups_conversion (
                    group_id INT PRIMARY KEY,
                    created_at TIMESTAMP,
                    cnt_added_users INT,
                    cnt_users_with_messages INT,
                    conversion FLOAT
                )
            """)
            
            # Рассчитываем конверсию и записывем в таблицу
            cur.execute("""
                INSERT INTO STV202507096__DWH.groups_conversion
                WITH 
                -- Время создания группы
                group_creation AS (
                    SELECT 
                        group_id,
                        MIN(datetime) AS created_at
                    FROM STV202507096__STAGING.group_log
                    WHERE event = 'create'
                    GROUP BY group_id
                ),
                -- Количество добавленных пользователей
                added_users AS (
                    SELECT 
                        group_id,
                        COUNT(DISTINCT user_id) AS cnt_added_users
                    FROM STV202507096__STAGING.group_log
                    WHERE event = 'add'
                    GROUP BY group_id
                ),
                -- Пользователи, написавшие хотя бы одно сообщение - from
                active_users AS (
                    SELECT 
                        gl.group_id,
                        COUNT(DISTINCT gl.user_id) AS cnt_users_with_messages
                    FROM STV202507096__STAGING.group_log gl
                    JOIN STV202507096__STAGING.dialogs d 
                        ON gl.user_id = d.message_from AND gl.group_id = d.message_group
                    WHERE gl.event = 'add'
                    GROUP BY gl.group_id
                )
                SELECT 
                    gc.group_id,
                    gc.created_at,
                    au.cnt_added_users,
                    COALESCE(act.cnt_users_with_messages, 0) AS cnt_users_with_messages,
                    CASE 
                        WHEN au.cnt_added_users > 0 
                        THEN COALESCE(act.cnt_users_with_messages, 0)::FLOAT / au.cnt_added_users
                        ELSE 0 
                    END AS conversion
                FROM group_creation gc
                JOIN added_users au ON gc.group_id = au.group_id
                LEFT JOIN active_users act ON gc.group_id = act.group_id
                ORDER BY gc.created_at
                LIMIT 10  -- 10 самых старых групп
            """)
            conn.commit()
            log.info("конверсия посчитана")
     
        except Exception as e:
            log.error(f"ошибка в расчете конверсии: {str(e)}")
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    # Задачи
    download_task = fetch_s3_file.override(task_id='download_group_log')(FILE_TO_DOWNLOAD)
    print_task = print_first_10_lines()
    load_task = load_group_log_to_staging()
    conversion_task = calculate_group_conversion()

    # Устанавливаем последовательность
    download_task >> print_task >> load_task >> conversion_task

load_group_log_and_calculate_conversion_dag = load_group_log_and_calculate_conversion_dag()