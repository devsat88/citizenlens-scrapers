"""
CLI: python run_task.py <yaml_path> [--mode dry_run|staging|production]

Runs a single task YAML through the full pipeline.
Exit code 0 on success, 1 on failure.
"""
import argparse
import sys

from orchestrator import run_single_task


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single CitizenLens scraper task.")
    parser.add_argument("yaml_path", help="Path to the task YAML file")
    parser.add_argument(
        "--mode",
        choices=["dry_run", "staging", "production"],
        default="dry_run",
        help="Run mode (default: dry_run)",
    )
    args = parser.parse_args()

    result = run_single_task(args.yaml_path, mode=args.mode)

    print(f"Task:    {result['task']}")
    print(f"Status:  {result['status']}")
    print(f"Records: {result.get('records', 0)}")
    print(f"Loaded:  {result.get('loaded', 0)}")
    if result.get("error"):
        print(f"Error:   {result['error']}")

    sys.exit(0 if result["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
