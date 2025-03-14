#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -x  # Print commands and their arguments as they are executed

# Update package lists
sudo apt-get update

# Install required dependencies
sudo apt-get install -y git build-essential cmake flex bison linux-tools-generic

# Clone and build LinuxTracepoints
git clone https://github.com/microsoft/LinuxTracepoints
cd LinuxTracepoints
mkdir build && cd build
cmake .. && make

# Install perf-decode
sudo cp bin/perf-decode /usr/local/bin

# Cleanup
cd ../..
rm -rf LinuxTracepoints

# Verify installation
perf --version
perf-decode --help
return 0

