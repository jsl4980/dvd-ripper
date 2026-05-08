#!/bin/sh
exec python3 "$(dirname "$0")/mock_makemkvcon.py" "$@"
