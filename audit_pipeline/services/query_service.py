from __future__ import annotations

import duckdb
import pandas as pd


def run_snapshot_query(
    layer: str,
    previous_df: pd.DataFrame,
    current_df: pd.DataFrame,
    chosen_query: str,
    custom_sql: str,
) -> tuple[str, pd.DataFrame]:
    connection = duckdb.connect(database=":memory:")
    try:
        connection.register("previous_data", previous_df)
        connection.register("current_data", current_df)

        if chosen_query == "Show only new records":
            sql = "SELECT * FROM current_data EXCEPT SELECT * FROM previous_data ORDER BY 1 LIMIT 200"
        elif chosen_query == "Compare snapshot changes":
            if layer == "gold":
                sql = """
                    SELECT
                        coalesce(c.partition_date, p.partition_date) AS partition_date,
                        coalesce(c.station_id, p.station_id) AS station_id,
                        coalesce(c.rows_processed, 0) AS current_rows,
                        coalesce(p.rows_processed, 0) AS previous_rows,
                        coalesce(c.rows_processed, 0) - coalesce(p.rows_processed, 0) AS delta_rows
                    FROM current_data c
                    FULL OUTER JOIN previous_data p
                        ON c.partition_date = p.partition_date
                       AND c.station_id = p.station_id
                    ORDER BY partition_date DESC, station_id
                    LIMIT 200
                """
            elif layer == "silver":
                sql = """
                    SELECT
                        station_id,
                        observation_date AS partition_key,
                        COUNT(*) AS current_rows
                    FROM current_data
                    GROUP BY 1, 2
                    ORDER BY partition_key DESC, station_id
                    LIMIT 200
                """
            else:
                sql = """
                    SELECT
                        station_id,
                        partition_date AS partition_key,
                        COUNT(*) AS current_rows
                    FROM current_data
                    GROUP BY 1, 2
                    ORDER BY partition_key DESC, station_id
                    LIMIT 200
                """
        elif chosen_query == "Aggregate by partition":
            if layer == "gold":
                sql = """
                    SELECT partition_date AS partition_key, SUM(rows_processed) AS row_count
                    FROM current_data
                    GROUP BY 1
                    ORDER BY partition_key DESC
                    LIMIT 200
                """
            elif layer == "silver":
                sql = """
                    SELECT observation_date AS partition_key, COUNT(*) AS row_count
                    FROM current_data
                    GROUP BY 1
                    ORDER BY partition_key DESC
                    LIMIT 200
                """
            else:
                sql = """
                    SELECT partition_date AS partition_key, COUNT(*) AS row_count
                    FROM current_data
                    GROUP BY 1
                    ORDER BY partition_key DESC
                    LIMIT 200
                """
        else:
            sql = custom_sql.strip() or "SELECT * FROM current_data LIMIT 100"

        result = connection.execute(sql).fetchdf()
        return sql, result
    finally:
        connection.close()

