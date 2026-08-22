from __future__ import annotations

import argparse
from pathlib import Path
import re
import subprocess
import sys


BLOCKED_SUFFIXES = {
    ".parquet",
    ".duckdb",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".arrow",
    ".feather",
    ".orc",
    ".zip",
    ".7z",
    ".rar",
}
BLOCKED_BASENAMES = {"cookies.json", "storage_state.json"}
MAX_SYNTHETIC_AIS_BYTES = 64 * 1024
MAX_TEXT_SCAN_BYTES = 1024 * 1024
RAW_AIS_NAME = re.compile(r"^(?:POS|STA)_OK_.*\.dat$", re.IGNORECASE)
WINDOWS_USER_PATH = re.compile(r"[A-Za-z]:\\Users\\([^\\\s]+)\\", re.IGNORECASE)
UNIX_HOME_PATH = re.compile("/" + r"home/([^/\s]+)/")
ALLOWED_PATH_MARKERS = {"...", "<user>", "username"}


def _content_violations(text: str) -> list[str]:
    violations: list[str] = []
    private_key_header = "BEGIN " + "OPENSSH PRIVATE KEY"
    github_token = re.compile("gh" + r"p_[A-Za-z0-9]{20,}")
    github_fine_grained_token = re.compile("github" + r"_pat_[A-Za-z0-9_]{20,}")

    if private_key_header in text:
        violations.append("contains an OpenSSH private-key header")
    if github_token.search(text) or github_fine_grained_token.search(text):
        violations.append("contains a GitHub credential pattern")

    for match in WINDOWS_USER_PATH.finditer(text):
        if match.group(1).lower() not in ALLOWED_PATH_MARKERS:
            violations.append("contains a concrete Windows user-profile path")
            break
    for match in UNIX_HOME_PATH.finditer(text):
        if match.group(1).lower() not in ALLOWED_PATH_MARKERS:
            violations.append("contains a concrete Unix home path")
            break
    return violations


def inspect_tracked_file(repo: Path, relative_path: Path) -> list[str]:
    """Return deterministic violation messages for one tracked file."""
    relative = Path(relative_path.as_posix())
    normalized = relative.as_posix()
    path = repo / relative
    violations: list[str] = []

    if not path.is_file():
        return [f"{normalized}: tracked path is not a regular file"]

    basename = relative.name.lower()
    suffix = relative.suffix.lower()

    if suffix in BLOCKED_SUFFIXES:
        violations.append(f"{normalized}: generated or archive file type is prohibited")
    if basename in BLOCKED_BASENAMES:
        violations.append(f"{normalized}: crawler or browser state is prohibited")
    if basename == ".env" or (basename.startswith(".env.") and basename != ".env.example"):
        violations.append(f"{normalized}: environment-secret file is prohibited")
    if suffix in {".pem", ".key", ".p12", ".pfx"} or basename.startswith(("id_rsa", "id_ed25519")):
        violations.append(f"{normalized}: private-key file is prohibited")

    if RAW_AIS_NAME.match(relative.name):
        is_fixture = relative.parent.as_posix() == "sample_data"
        if not is_fixture:
            violations.append(f"{normalized}: raw AIS is allowed only in sample_data")
        elif path.stat().st_size > MAX_SYNTHETIC_AIS_BYTES:
            violations.append(f"{normalized}: synthetic AIS fixture exceeds 64 KiB")

    if path.stat().st_size <= MAX_TEXT_SCAN_BYTES:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = ""
        for reason in _content_violations(text):
            violations.append(f"{normalized}: {reason}")

    return violations


def tracked_files(repo: Path) -> list[Path]:
    """Return paths from `git ls-files -z` or raise RuntimeError."""
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        message = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git ls-files failed: {message}")
    return sorted(
        (Path(value.decode("utf-8")) for value in completed.stdout.split(b"\0") if value),
        key=lambda value: value.as_posix(),
    )


def scan_repository(repo: Path) -> list[str]:
    """Return sorted violations for all tracked files."""
    root = repo.resolve()
    violations = [
        violation
        for relative_path in tracked_files(root)
        for violation in inspect_tracked_file(root, relative_path)
    ]
    return sorted(violations)


def main(argv: list[str] | None = None) -> int:
    """Parse `--repo`, print results, and return 0 or 1."""
    parser = argparse.ArgumentParser(description="Check tracked files for public-repository risks.")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    arguments = parser.parse_args(argv)

    try:
        violations = scan_repository(arguments.repo)
    except RuntimeError as error:
        print(error, file=sys.stderr)
        return 1

    if violations:
        for violation in violations:
            print(violation, file=sys.stderr)
        return 1

    print("Repository safety check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
