import dlt
from dlt.sources.sql_database import sql_database


def load_select_tables_from_database(
    *, schema_name: str, table_names_to_fetch: list[str], scd2_tables: list[str]
) -> None:
    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="odoo2neon", destination="postgres", dataset_name=schema_name
    )

    # Fetch tables "family" and "clan"
    source = sql_database(table_names=table_names_to_fetch)

    source_system = "raw"  # Your desired prefix
    for _resource_name, resource in source.resources.items():
        # Incremental
        if resource.name in scd2_tables:
            resource.apply_hints(
                table_name=f"{source_system}__{resource.name}",
                incremental=dlt.sources.incremental("write_date"),
                write_disposition={"disposition": "merge", "strategy": "scd2"},
            )

        else:
            resource.apply_hints(
                table_name=f"{source_system}__{resource.name}",
                incremental=dlt.sources.incremental("write_date"),
                write_disposition="merge",
            )

    # Run the pipeline
    info = pipeline.run(source)

    # Print load info
    print(info)


if __name__ == "__main__":
    schema_name = "bronze"
    table_names_to_fetch = [
        "account_payment",
        "hr_expense",
        "sale_order",
        "sale_order_line",
        "account_payment_term",
        "account_move",
        "product_template",
        "product_product",
        "account_move_line",
        "res_partner",
    ]
    scd2_tables = ["product_template"]

    load_select_tables_from_database(
        schema_name=schema_name,
        table_names_to_fetch=table_names_to_fetch,
        scd2_tables=scd2_tables,
    )
