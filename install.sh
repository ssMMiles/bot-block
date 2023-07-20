#!/bin/bash
set -e

cargo build --release
sudo cp target/release/botblock /usr/bin/botblock

sudo cp systemd/botblock.service /etc/systemd/system/botblock.service
sudo cp systemd/botblock.timer /etc/systemd/system/botblock.timer

sudo systemctl daemon-reload
sudo systemctl enable botblock.timer
sudo systemctl start botblock.timer