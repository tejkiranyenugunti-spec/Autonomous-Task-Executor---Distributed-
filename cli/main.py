"""
CLI for the Autonomous Task Execution Engine (talks to the Go REST API).
"""
from __future__ import annotations

import os
from typing import Any

import click
import requests


def api_base() -> str:
    return os.environ.get("ATE_API_URL", "http://localhost:8080").rstrip("/")


def jprint(data: Any) -> None:
    import json

    click.echo(json.dumps(data, indent=2, sort_keys=True))


@click.group()
def cli() -> None:
    """Autonomous Task Execution Engine CLI."""


@cli.command("submit")
@click.option("--instruction", required=True, help="High-level instruction to decompose and execute.")
@click.option("--priority", type=click.Choice(["high", "normal"]), default="normal")
def submit_cmd(instruction: str, priority: str) -> None:
    """Submit a new job to the engine."""
    r = requests.post(
        f"{api_base()}/jobs",
        json={"instruction": instruction, "priority": priority},
        timeout=60,
    )
    r.raise_for_status()
    jprint(r.json())


@cli.command("status")
@click.option("--job-id", required=True)
def status_cmd(job_id: str) -> None:
    """Fetch job status and subtask breakdown."""
    r = requests.get(f"{api_base()}/jobs/{job_id}", timeout=30)
    r.raise_for_status()
    jprint(r.json())


@cli.command("cancel")
@click.option("--job-id", required=True)
def cancel_cmd(job_id: str) -> None:
    """Cancel a running job."""
    r = requests.delete(f"{api_base()}/jobs/{job_id}", timeout=30)
    if r.status_code not in (200, 204):
        raise click.ClickException(f"cancel failed: {r.status_code} {r.text}")
    click.echo("cancelled")


@cli.command("list")
def list_cmd() -> None:
    """List all jobs known to the API."""
    r = requests.get(f"{api_base()}/jobs", timeout=30)
    r.raise_for_status()
    jprint(r.json())


if __name__ == "__main__":
    cli()
