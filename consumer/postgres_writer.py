import os
import psycopg2


class PostgresWriter:
    def __init__(self):
        # postgres credentials for local env
        # self.conn = psycopg2.connect(
        #     host="localhost",
        #     port=5432,
        #     dbname="data_pipeline",
        #     user="admin",
        #     password="admin",
        # )
        
        # postgres credentials for docker env
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        self.conn.autocommit = True

    def insert_transaction(self, event: dict):
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO transactions (
                    transaction_id,
                    user_id,
                    amount,
                    currency,
                    status,
                    event_time,
                    processing_time,
                    is_late_event
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event["transaction_id"],
                    event["user_id"],
                    event["amount"],
                    event["currency"],
                    event["status"],
                    event["event_time"],
                    event["processing_time"],
                    event["is_late_event"],
                ),
            )
