from db_discovery_lib.file_loader import read_files_to_df
import typer

app = typer.Typer(help="Load the Thrive-AI sample CSVs into Postgres")

@app.command()
def loadcsv(
    db_name: str = typer.Option('postgres', help="Database name"),
    username: str = typer.Option('postgres', help="Username (optional)"),
    password: str = typer.Option('postgres', help="Password (optional)"),
    host: str = typer.Option('localhost', help="Host (optional)"),
    port: int = typer.Option('5432', help="Port (optional)")
):
    read_files_to_df(db_name, username, password, host, port)