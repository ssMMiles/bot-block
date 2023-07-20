#!/bin/bash
set -e

cargo build --release
sudo cp target/release/bot-block /usr/local/bin

sudo cp systemd/bot-block.service /etc/systemd/system/bot-block.service
sudo cp systemd/bot-block.timer /etc/systemd/system/bot-block.timer

sudo systemctl daemon-reload
sudo systemctl enable bot-block.timer
sudo systemctl start bot-block.timer