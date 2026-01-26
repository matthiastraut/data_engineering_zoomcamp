import click
import polars as pl
from sqlalchemy import create_engine
from tqdm.auto import tqdm


@click.command()
@click.option('--pg-user', default='postgres', help='PostgreSQL user')
@click.option('--pg-pass', default='postgres', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--parquet-file', default='green_tripdata_2025-11.parquet', help='Parquet file to ingest')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading parquet')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, parquet_file, target_table, chunksize):
    """Ingest taxi data from parquet file into PostgreSQL database."""
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Read parquet file with Polars and scan for memory-efficient processing
    df = pl.read_parquet(parquet_file)

    # Convert to pandas for SQLAlchemy compatibility
    pandas_df = df.to_pandas()
    
    # Write schema first (with empty dataframe)
    pandas_df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists='replace'
    )
    
    # Write data in chunks
    total_rows = len(pandas_df)
    num_chunks = (total_rows + chunksize - 1) // chunksize
    
    for i in tqdm(range(num_chunks), desc=f"Writing to {target_table}"):
        start_idx = i * chunksize
        end_idx = min((i + 1) * chunksize, total_rows)
        chunk = pandas_df.iloc[start_idx:end_idx]
        
        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append',
            index=False
        )

if __name__ == '__main__':
    run()
