from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.check_repository_safety import inspect_tracked_file


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class RepositorySafetyTests(unittest.TestCase):
    def inspect(self, relative: str, content: bytes = b"test") -> list[str]:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            return inspect_tracked_file(root, Path(relative))

    def test_allows_small_synthetic_ais_fixture(self) -> None:
        self.assertEqual(self.inspect("sample_data/POS_OK_2026-03-02.dat"), [])

    def test_rejects_raw_ais_outside_sample_data(self) -> None:
        self.assertTrue(self.inspect("data/POS_OK_2026-03-02.dat"))

    def test_rejects_generated_database(self) -> None:
        self.assertTrue(self.inspect("output/result.parquet"))

    def test_rejects_private_key_header(self) -> None:
        marker = ("-----" + "BEGIN " + "OPENSSH PRIVATE KEY" + "-----").encode()
        self.assertTrue(self.inspect("notes.txt", marker))

    def test_rejects_concrete_windows_user_path(self) -> None:
        path = ("C:" + "\\Users\\Alice\\AIS\\input.dat").encode()
        self.assertTrue(self.inspect("notes.txt", path))

    def test_does_not_match_a_windows_path_across_lines(self) -> None:
        content = ("Use C:" + "\\Users\\... as a placeholder.\nThen D:\\AIS_DATA").encode()
        self.assertEqual(self.inspect("notes.txt", content), [])

    def test_scanner_source_does_not_trigger_its_own_patterns(self) -> None:
        self.assertEqual(
            inspect_tracked_file(PROJECT_ROOT, Path("scripts/check_repository_safety.py")),
            [],
        )


if __name__ == "__main__":
    unittest.main()
