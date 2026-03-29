from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from chimera_lab.app import create_app


def main() -> None:
    app = create_app()
    services = app.state.services
    exported = services.publication_service.export_public_site()
    print(json.dumps(exported, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
