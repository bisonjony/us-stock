#!/usr/bin/env bash
set -e

VENV_DIR=".venv"

echo "Creating virtual environment in $VENV_DIR ..."
python -m venv "$VENV_DIR"

if [[ "$OS" == "Windows_NT" ]]; then
  ACTIVATE="$VENV_DIR/Scripts/activate"
  PYTHON="$VENV_DIR/Scripts/python"
else
  ACTIVATE="$VENV_DIR/bin/activate"
  PYTHON="$VENV_DIR/bin/python"
fi

echo "Activating virtual environment ..."
source "$ACTIVATE"

echo "Upgrading pip ..."
$PYTHON -m pip install --upgrade pip

echo "Installing packages ..."
$PYTHON -m pip install -r env/requirements.txt

echo "Environment setup complete!"
echo "Activate with: source $ACTIVATE"