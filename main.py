import argparse
import asyncio

from settings import CHAT_IDENTIFIER, client
from utils import (
    aggregate_messages,
    collect_last_7_days_messages,
    filter_discussed_threads,
    format_for_json_output,
    save_results_to_json,
)


async def run(group_identifier: str, output_file: str):
    """Run the pipeline for given group identifier and write JSON to output_file."""
    try:
        await client.start()
        messages = await collect_last_7_days_messages(group_identifier)

        # aggregate_messages is async in your refactor — keep using await
        sorted_messages = await aggregate_messages(messages)

        filtered_threads = filter_discussed_threads(sorted_messages)

        json_data = format_for_json_output(filtered_threads)

        await save_results_to_json(json_data, filename=output_file)
    finally:
        # always try to disconnect
        try:
            await client.disconnect()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(
        description="Collect last 7 days messages from a Telegram group and save aggregated JSON."
    )
    parser.add_argument(
        "group",
        nargs="?",
        default=CHAT_IDENTIFIER,
        help="Group identifier (username or full link). "
        "If omitted, the default from settings.py is used.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="result.json",
        help="Output JSON filename (default: result.json).",
    )

    args = parser.parse_args()

    # Run the async pipeline
    asyncio.run(run(args.group, args.output))


if __name__ == "__main__":
    main()
