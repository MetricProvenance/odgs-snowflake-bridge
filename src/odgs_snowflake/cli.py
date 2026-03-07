"""
ODGS Snowflake Bridge — CLI
=============================

Command-line interface for syncing Snowflake INFORMATION_SCHEMA
metadata into ODGS enforcement schemas.

Usage:
    odgs-snowflake sync \
        --account xy12345.eu-west-1 \
        --user odgs_service \
        --org acme_corp \
        --database PRODUCTION
"""

import os
import sys
import logging

try:
    import typer
except ImportError:
    print("Error: typer is required. Install with: pip install typer>=0.9.0")
    sys.exit(1)

from odgs_snowflake.bridge import SnowflakeBridge

app = typer.Typer(
    name="odgs-snowflake",
    help="Bridge: Snowflake Data Dictionary → ODGS Runtime Enforcement Schemas",
    no_args_is_help=True,
)


@app.command()
def sync(
    account: str = typer.Option(
        ..., "--account", "-a", help="Snowflake account identifier",
        envvar="SNOWFLAKE_ACCOUNT",
    ),
    user: str = typer.Option(
        ..., "--user", "-u", help="Snowflake username",
        envvar="SNOWFLAKE_USER",
    ),
    password: str = typer.Option(
        None, "--password", "-p", help="Snowflake password",
        envvar="SNOWFLAKE_PASSWORD",
    ),
    role: str = typer.Option(None, "--role", "-r", help="Snowflake role"),
    warehouse: str = typer.Option(None, "--warehouse", "-w", help="Snowflake warehouse"),
    org: str = typer.Option(
        ..., "--org", "-o", help="Organization name for URN namespace",
    ),
    database: str = typer.Option(
        ..., "--database", "-d", help="Snowflake database to scan",
    ),
    schema_filter: str = typer.Option(
        None, "--schema", "-s", help="Filter to specific schema",
    ),
    output: str = typer.Option(
        "./schemas/custom/", "--output", help="Output directory",
    ),
    output_type: str = typer.Option(
        "metrics", "--type", help="Output type: 'metrics' or 'rules'",
    ),
    severity: str = typer.Option(
        "WARNING", "--severity", help="Rule severity (HARD_STOP, WARNING, INFO)",
    ),
    authenticator: str = typer.Option(
        "snowflake", "--authenticator", help="Auth method (snowflake, externalbrowser)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
):
    """Sync Snowflake tables into ODGS JSON schemas."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s  %(message)s",
    )

    if not password and authenticator == "snowflake":
        typer.echo(
            "Error: Provide --password or set SNOWFLAKE_PASSWORD, "
            "or use --authenticator externalbrowser",
            err=True,
        )
        raise typer.Exit(1)

    try:
        bridge = SnowflakeBridge(
            account=account,
            user=user,
            password=password,
            role=role,
            warehouse=warehouse,
            organization=org,
            authenticator=authenticator,
        )
        filepath = bridge.sync(
            database=database,
            schema_filter=schema_filter,
            output_dir=output,
            output_type=output_type,
            severity=severity,
        )

        if filepath:
            typer.echo(f"\n✅ ODGS schema written to: {filepath}")
        else:
            typer.echo("\n⚠️  No tables found.", err=True)
            raise typer.Exit(1)

    except Exception as e:
        typer.echo(f"\n❌ Bridge Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def version():
    """Show the bridge version."""
    from odgs_snowflake import __version__
    typer.echo(f"odgs-snowflake-bridge v{__version__}")


if __name__ == "__main__":
    app()
