#!/bin/bash
set -e

cargo build --release
cp target/release/bot-block /usr/local/bin

cp systemd/bot-block.service /etc/systemd/system/bot-block.service
cp systemd/bot-block.timer /etc/systemd/system/bot-block.timer

systemctl daemon-reload
systemctl enable bot-block.timer
systemctl start bot-block.timer