#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
#  Weather Data Explorer — Mac Launcher
#  Double-click this file in Finder to install packages and start the app.
#  First run installs packages; subsequent runs skip that step (a few seconds).
# ─────────────────────────────────────────────────────────────────────────────

# Move to the project root (directory containing this file)
cd "$(dirname "$0")"

echo "=================================================="
echo "  Weather Data Explorer"
echo "=================================================="
echo ""

# ── 1. Find Python ────────────────────────────────────────────────────────────
PYTHON=""

for candidate in \
    "$HOME/miniconda3/bin/python" \
    "$HOME/anaconda3/bin/python" \
    "$HOME/opt/miniconda3/bin/python" \
    "$HOME/opt/anaconda3/bin/python" \
    "/opt/homebrew/bin/python3" \
    "/usr/local/bin/python3"; do
    if [ -x "$candidate" ]; then
        PYTHON="$candidate"
        break
    fi
done

# Fall back to any python3 on PATH
if [ -z "$PYTHON" ]; then
    PYTHON=$(command -v python3 2>/dev/null)
fi

if [ -z "$PYTHON" ]; then
    osascript -e 'display alert "Python Not Found" message "Please install Miniconda (https://docs.conda.io/en/latest/miniconda.html) or Python 3.9+ from python.org, then try again." buttons {"OK"} default button "OK"'
    exit 1
fi

echo "Python : $PYTHON"
echo "Version: $("$PYTHON" --version 2>&1)"
echo ""

# ── 2. Install / update packages ─────────────────────────────────────────────
echo "Checking required packages…"
"$PYTHON" -m pip install --quiet --upgrade pip
"$PYTHON" -m pip install --quiet -r scripts/requirements.txt

if [ $? -ne 0 ]; then
    echo ""
    echo "ERROR: Package installation failed."
    echo "Check your internet connection and try again."
    read -p "Press Enter to close…"
    exit 1
fi

echo "All packages ready."
echo ""

# ── 3. Launch Streamlit ───────────────────────────────────────────────────────
echo "Starting app at → http://localhost:8501"
echo "Close this Terminal window to stop the server."
echo ""

"$PYTHON" -m streamlit run scripts/app.py
