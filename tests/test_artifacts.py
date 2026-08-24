from __future__ import annotations

import hashlib
from pathlib import Path
import tempfile
import unittest

from ais_tanker_pipeline.artifacts import (
    canonical_hash,
    file_signatures,
    read_manifest,
    sha256_file,
    write_json_atomic,
)


class ArtifactTests(unittest.TestCase):
    def test_hashes_content_and_canonical_data_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            second = root / "b.txt"
            first = root / "a.txt"
            first.write_bytes(b"alpha")
            second.write_bytes(b"beta")
            self.assertEqual(sha256_file(first), hashlib.sha256(b"alpha").hexdigest())
            self.assertEqual(
                [Path(item["path"]).name for item in file_signatures([second, first])],
                ["a.txt", "b.txt"],
            )
            self.assertEqual(canonical_hash({"b": 2, "a": 1}), canonical_hash({"a": 1, "b": 2}))

    def test_atomic_json_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "nested" / "manifest.json"
            payload = {"status": "complete", "counts": {"rows": 2}}
            write_json_atomic(target, payload)
            self.assertEqual(read_manifest(target), payload)
            self.assertEqual(list(target.parent.glob("*.partial-*")), [])


if __name__ == "__main__":
    unittest.main()
