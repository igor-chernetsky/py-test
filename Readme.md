Logs

# logs cron
sudo journalctl -u cron -n 20 --no-pager

# custom script logs
tail -f /var/log/gdelt_fetcher.log

# last custom script logs
tail -50 /var/log/gdelt_fetcher.log

# the last files in s3
aws s3 ls s3://visorbacket/gdelt/ --recursive | sort | tail -5
