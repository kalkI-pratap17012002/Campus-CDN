import argparse
import json
import sys
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from client.downloader import download_file


def upload_file(server_url: str, file_path: str) -> dict:
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("rb") as file_handle:
        response = httpx.post(
            f"{server_url.rstrip('/')}/upload",
            files={"file": (path.name, file_handle, "application/octet-stream")},
            timeout=300.0,
        )
    response.raise_for_status()
    return response.json()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Campus CDN client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    upload_parser = subparsers.add_parser("upload", help="Upload a file to the server")
    upload_parser.add_argument("--file", required=True, help="Path to the file to upload")
    upload_parser.add_argument("--server", required=True, help="Server base URL")

    download_parser = subparsers.add_parser("download", help="Download a file from the server")
    download_parser.add_argument("--file-id", required=True, help="File ID from a previous upload")
    download_parser.add_argument("--server", required=True, help="Server base URL")
    download_parser.add_argument("--output", required=True, help="Output file path or directory")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "upload":
        result = upload_file(args.server, args.file)
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "download":
        output_path = download_file(args.server, args.file_id, args.output)
        print(f"Downloaded file to {output_path}")
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
