#!/usr/bin/env python3
"""
Example client for peopledd REST API.
Demonstrates how to trigger analyses and poll results.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx


class PeopleddClient:
    """Simple client for peopledd API."""

    def __init__(self, base_url: str = "http://localhost:8000", timeout: float = 300):
        self.base_url = base_url
        self.timeout = timeout

    async def health(self) -> dict[str, Any]:
        """Check API health."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return response.json()

    async def start_analysis(
        self,
        company_name: str,
        country: str = "BR",
        company_type_hint: str = "auto",
        analysis_depth: str = "standard",
        **kwargs,
    ) -> str:
        """Start a new analysis, return run_id."""
        payload = {
            "company_name": company_name,
            "country": country,
            "company_type_hint": company_type_hint,
            "analysis_depth": analysis_depth,
            **kwargs,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.base_url}/analyze",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return data["run_id"]

    async def get_status(self, run_id: str) -> dict[str, Any]:
        """Get status of a run."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/runs/{run_id}/status")
            response.raise_for_status()
            return response.json()

    async def get_result(self, run_id: str) -> dict[str, Any]:
        """Get full result of a run."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/runs/{run_id}/result")
            response.raise_for_status()
            return response.json()

    async def get_brief(self, run_id: str) -> dict[str, Any]:
        """Get DD brief for a run."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/runs/{run_id}/brief")
            response.raise_for_status()
            return response.json()

    async def list_runs(self, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        """List recent runs."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/runs",
                params={"limit": limit, "offset": offset},
            )
            response.raise_for_status()
            return response.json()

    async def diff_runs(self, run_a: str, run_b: str) -> dict[str, Any]:
        """Compare two runs."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/runs/{run_a}/diff/{run_b}",
            )
            response.raise_for_status()
            return response.json()

    async def wait_for_completion(
        self,
        run_id: str,
        poll_interval: float = 5,
        max_wait: float = 3600,
    ) -> dict[str, Any]:
        """Wait for a run to complete."""
        start = time.time()
        while True:
            status = await self.get_status(run_id)
            print(f"Status: {status['status']}")

            if status["status"] in ("completed", "error"):
                return status

            if time.time() - start > max_wait:
                raise TimeoutError(f"Run {run_id} did not complete within {max_wait}s")

            await asyncio.sleep(poll_interval)


async def example_analyze_single() -> None:
    """Example: Analyze one company and print results."""
    client = PeopleddClient()

    # Check health
    health = await client.health()
    print(f"API Health: {health['status']}\n")

    # Start analysis
    print("Starting analysis for Itaú Unibanco...")
    run_id = await client.start_analysis(
        company_name="Itaú Unibanco",
        country="BR",
        company_type_hint="listed",
        ticker_hint="ITUB4",
    )
    print(f"Run ID: {run_id}\n")

    # Wait for completion
    print("Waiting for analysis to complete...")
    status = await client.wait_for_completion(run_id)
    print(f"Completed with status: {status['status']}\n")

    if status["status"] == "error":
        print(f"Error: {status['error']}")
        return

    # Fetch results
    print("Fetching results...")
    result = await client.get_result(run_id)
    brief = await client.get_brief(run_id)

    print(f"Entity: {result.get('entity_resolution', {}).get('resolved_name')}")
    print(f"Service Level: {result.get('degradation_profile', {}).get('service_level')}")
    print(f"\nDD Brief:\n{json.dumps(brief, indent=2)[:500]}...")


async def example_batch_analyze() -> None:
    """Example: Analyze multiple companies in parallel."""
    client = PeopleddClient()

    companies = [
        ("Itaú Unibanco", "BR"),
        ("Bradesco", "BR"),
        ("BTG Pactual", "BR"),
    ]

    print(f"Starting {len(companies)} analyses in parallel...\n")

    # Start all
    run_ids = []
    for company_name, country in companies:
        run_id = await client.start_analysis(
            company_name=company_name,
            country=country,
            company_type_hint="listed",
        )
        run_ids.append((run_id, company_name))
        print(f"Started: {company_name} (run_id={run_id})")

    print("\nWaiting for all to complete...")

    # Wait for all
    tasks = [
        client.wait_for_completion(run_id, poll_interval=10)
        for run_id, _ in run_ids
    ]
    results = await asyncio.gather(*tasks)

    for (run_id, company_name), status in zip(run_ids, results):
        print(f"{company_name}: {status['status']}")


async def example_list_and_diff() -> None:
    """Example: List runs and compare two."""
    client = PeopleddClient()

    # List recent
    print("Listing recent runs...")
    runs_data = await client.list_runs(limit=5)
    print(f"Found {runs_data['count']} total runs:\n")
    for run_info in runs_data["runs"][:3]:
        print(f"  - {run_info['run_id']}")

    # Compare if we have at least 2
    if len(runs_data["runs"]) >= 2:
        run_a = runs_data["runs"][0]["run_id"]
        run_b = runs_data["runs"][1]["run_id"]
        print(f"\nComparing {run_a[:8]}... vs {run_b[:8]}...\n")
        diff = await client.diff_runs(run_a, run_b)
        print(json.dumps(diff, indent=2)[:500])


async def main() -> None:
    """Run examples."""
    import sys

    example = sys.argv[1] if len(sys.argv) > 1 else "single"

    try:
        if example == "single":
            await example_analyze_single()
        elif example == "batch":
            await example_batch_analyze()
        elif example == "list":
            await example_list_and_diff()
        else:
            print(
                f"Unknown example: {example}\n"
                f"Usage: python examples.py [single|batch|list]"
            )
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
