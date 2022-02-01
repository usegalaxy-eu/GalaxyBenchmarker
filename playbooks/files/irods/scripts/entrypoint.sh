#!/bin/bash

echo Installing iRODS...

python /var/lib/irods/scripts/setup_irods.py --json_configuration_file /scripts/server_config.json

if [ $? -eq 0 ]
then
  echo Installation done!
else
  echo "setup_irods failed"
  exit 1
fi

echo Starting server as user 'irods'...
su -c "python /var/lib/irods/scripts/irods_control.py start" irods

sleep infinity
