#!/bin/bash
set -e

# -----------------------------------------------------
# Define the repository root directory
# -----------------------------------------------------
REPO_ROOT=$(pwd)
echo "Repository root: $REPO_ROOT"

# -----------------------------------------------------
# Step 1: Ensure virtualenv is installed and create venv
# -----------------------------------------------------
if ! command -v virtualenv >/dev/null 2>&1; then
    echo "virtualenv not found. Installing via pip..."
    pip install virtualenv
else
    echo "virtualenv is already installed."
fi

# Create a virtual environment (if not already present) named "venv"
if [ ! -d "$REPO_ROOT/venv" ]; then
    echo "Creating local virtual environment in 'venv'..."
    virtualenv venv
else
    echo "Virtual environment 'venv' already exists."
fi

echo "Activating virtual environment..."
source "$REPO_ROOT/venv/bin/activate"

# Install required Python packages for tcga_tools.py
echo "Installing required Python packages (pandas and tqdm)..."
pip install pandas tqdm

# -----------------------------------------------------
# Step 2: Build gdc-client executable if not already available
# -----------------------------------------------------
if [ -f "$REPO_ROOT/gdc-client_exec" ]; then
    echo "gdc-client_exec already exists in repository root. Skipping build."
else
    echo "gdc-client_exec not found. Building gdc-client executable..."

    # Change directory to the gdc-client bin folder (assuming gdc-client is a submodule)
    cd "$REPO_ROOT/gdc-client/bin"

    echo "Running the package script to build gdc-client..."
    bash ./package

    # Look for the generated zip file (assuming one zip file is produced)
    ZIPFILE=$(ls *.zip | head -n 1)
    if [ -z "$ZIPFILE" ]; then
        echo "Error: No zip file found in $(pwd). Exiting."
        exit 1
    fi
    echo "Found zip file: $ZIPFILE"

    # Create a temporary directory for extraction
    TEMP_DIR="gdc_extract_temp"
    mkdir -p "$TEMP_DIR"
    echo "Extracting $ZIPFILE into temporary directory '$TEMP_DIR'..."
    unzip -o "$ZIPFILE" -d "$TEMP_DIR"

    # Determine executable name based on operating system
    if [[ "$(uname)" == "Linux" ]]; then
        EXEC_NAME="gdc-client"
    else
        EXEC_NAME="gdc-client.exe"
    fi

    # Find the executable in the temporary directory
    EXEC_PATH=$(find "$TEMP_DIR" -type f -name "$EXEC_NAME" | head -n 1)
    if [ -z "$EXEC_PATH" ]; then
        echo "Error: gdc-client executable not found in the extracted files."
        exit 1
    fi
    echo "Found gdc-client executable at: $EXEC_PATH"
    chmod +x "$EXEC_PATH"

    # -----------------------------------------------------
    # Step 3: Move the executable to the repository root
    # -----------------------------------------------------
    echo "Moving gdc-client executable to repository root as 'gdc-client_exec'..."
    mv "$EXEC_PATH" "$REPO_ROOT/gdc-client_exec"

    # Clean up the temporary extraction directory
    rm -rf "$TEMP_DIR"

    echo "gdc-client installation complete. The executable is located at $REPO_ROOT/gdc-client_exec."
fi

# Return to the repository root
cd "$REPO_ROOT"

echo "Local virtual environment 'venv' is set up and required Python packages are installed."
echo "Installation complete. You can now use './gdc-client_exec' for downloading data and run 'tcga_tools.py' within your virtual environment."
echo "To deactivate the virtual environment, run 'deactivate'."