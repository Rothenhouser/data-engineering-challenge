"""Entry point: launch the Streamlit app."""

from __future__ import annotations

import os
import sys

from streamlit.web import cli as st_cli


def main() -> None:
    app = os.path.join(os.path.dirname(__file__), "app.py")
    sys.argv = ["streamlit", "run", app, *sys.argv[1:]]
    st_cli.main()


if __name__ == "__main__":
    main()
