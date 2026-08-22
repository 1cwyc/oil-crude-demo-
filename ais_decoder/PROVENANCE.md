# Bundled decoder provenance

The files `fast_duckdb.py`, `constants.py`, `parsers.py`, and `__init__.py` were
copied without algorithmic changes from the user's previously validated AIS
quality-check package:

`C:\Users\Legion\Desktop\Energy-Trade\AIS-part\check`

The portable tanker pipeline imports the existing `_static_query` and
`_position_query` SQL builders from this bundled copy. This avoids maintaining a
second AIS field decoder while allowing the release folder to run on another PC.

