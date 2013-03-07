"""
This script will sync 2 folders if the destination folder is a mount point.
The objective is to avoid syncing to a mounted share when it is not mounted.
Will run as an infinite loop, to avoid multiple spawned processes that may
occur if the sync is slower than the crontab time.
Therefore detached running or start from init script is advisable.

Usage:
    python sync_to_mount sourcefolder destinationfolder

"""

import os, sys, logging, time

log = logging.getLogger('sync_to_mount')
log.setLevel(10)
fh = logging.FileHandler('sync_transfer.log')
formatter = logging.Formatter('%(asctime)s - PID:%(process)d - %(name)s - '
                                '%(levelname)s - %(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)

if len(sys.argv) != 3:
    print __doc__
    sys.exit()

src = sys.argv[1]
dst = sys.argv[2]

while True:
    # check mounts
    if not os.path.ismount(dst):
        log.warning('Destination folder is not a mount point. Will not sync now.')
    else:
        log.info('Syncing {0} folder to destination {1}'.format(src, dst))
        os.system('rsync -lts -r /mnt/datadrive/ /mnt/kalevalatmp/')
    
    time.sleep(900)

