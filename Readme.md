Logs

# logs cron
sudo journalctl -u cron -n 20 --no-pager

# custom script logs
tail -f /var/log/gdelt_fetcher.log

# last custom script logs
tail -50 /var/log/gdelt_fetcher.log

# the last files in s3
aws s3 ls s3://visorbacket/gdelt/ --recursive | sort | tail -5

#env var
sudo cat /etc/systemd/system/fastapi.service.d/override.conf

# check normolization execution
grep "Normalization" /home/ec2-user/py-test/logs/normalizer.log | tail -5

# check last 10 normalizations
grep "finished at" /home/ec2-user/py-test/logs/normalizer.log | tail -10

# db connect psql "host=database-1.cpycqqk6qhjv.eu-north-1.rds.amazonaws.com port=5432 dbname=postgres user=postgres sslmode=require"