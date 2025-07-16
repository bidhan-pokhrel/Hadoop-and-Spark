#!/bin/bash
set -euxo pipefail

# Update package lists
sudo apt-get update

# Install R and dependencies
sudo apt-get install -y r-base r-base-dev libcurl4-openssl-dev libssl-dev libxml2-dev

# Install gdebi to handle .deb packages
sudo apt-get install -y gdebi-core

# Download and install RStudio Server (replace with the latest version URL from https://posit.co/download/rstudio-server/)
wget https://download2.rstudio.org/server/focal/amd64/rstudio-server-2025.05.1-513-amd64.deb
sudo gdebi rstudio-server-2025.05.1-513-amd64.deb


# Create an RStudio user (replace 'rstudio' with desired username)
sudo useradd -m rstudio
echo "rstudio:password" | sudo chpasswd  # Replace 'password' with a secure password
