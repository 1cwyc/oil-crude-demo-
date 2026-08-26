from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import yaml

from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.environment.config import _UniqueKeyLoader, _path_value

@dataclass(frozen=True)
class NetworkConfig:
    output_root: Path
    mapping_period: str
    raw: dict[str, object]
    @property
    def config_hash(self) -> str: return canonical_hash(self.raw)

def load_network_config(path: str | Path) -> NetworkConfig:
    path=Path(path).resolve()
    source=yaml.load(path.read_text(encoding='utf-8'),Loader=_UniqueKeyLoader)
    if not isinstance(source,dict) or set(source)!={'output_root','mapping_period'}:
        raise ValueError('network config must contain exactly output_root and mapping_period')
    period=source['mapping_period']
    if not isinstance(period,str) or not period:
        raise ValueError('mapping_period must be nonempty')
    root=_path_value(path,source['output_root'],'output_root')
    return NetworkConfig(root,period,{'output_root':str(root),'mapping_period':period})
