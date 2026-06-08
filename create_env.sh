#!/bin/bash

echo "Starting TicketMgr setup..."

# =====================================================
# Go to project directory
# =====================================================
APP_DIR="/Czentrix/apps/TicketMgr"

cd "$APP_DIR" || {
    echo "ERROR: Project directory not found."
    exit 1
}

# =====================================================
# Create Virtual Environment
# =====================================================
echo "Creating virtual environment..."

if [ ! -d "venv" ]; then
    virtualenv venv -p /opt/python3.6.7/bin/python3 || {
        echo "ERROR: Failed to create virtual environment."
        exit 1
    }
fi

source venv/bin/activate || {
    echo "ERROR: Failed to activate virtual environment."
    exit 1
}

# =====================================================
# Install Dependencies
# =====================================================
echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing requirements..."
pip install -r requirements.txt || {
    echo "ERROR: Requirements installation failed."
    exit 1
}

echo "Requirements installed successfully."

# =====================================================
# Move log configuration files
# =====================================================
echo "Moving log configuration files..."

mkdir -p /etc/logConfig

if mv "$APP_DIR"/logConfig/*.conf /etc/logConfig/; then
    echo "Log configuration files moved successfully."

    # Remove source directory if empty
    rm -rf "$APP_DIR/logConfig"
    echo "Removed $APP_DIR/logConfig"
else
    echo "ERROR: Failed to move log configuration files."
    exit 1
fi

# =====================================================
# Move systemd service files
# =====================================================
echo "Moving service files..."

if mv "$APP_DIR"/system/*.service /etc/systemd/system/; then
    echo "Service files moved successfully."

    # Remove source directory if empty
    rm -rf "$APP_DIR/system"
    echo "Removed $APP_DIR/system"
else
    echo "ERROR: Failed to move service files."
    exit 1
fi

# =====================================================
# Reload systemd
# =====================================================
systemctl daemon-reload

# =====================================================
# Enable services
# =====================================================
systemctl enable tt-dialer3306.service
systemctl enable tt-dialer3307.service
systemctl enable tt-dialer3308.service
systemctl enable tt-dialer3309.service

echo "====================================================="
echo "TicketMgr setup completed successfully."
echo "====================================================="