"""
This script queries the logfiles of the MS machine, checks if there are new files,
and SCP's them to a server.
"""

import os, datetime, subprocess, logging, json, time

# prepare log
log = logging.getLogger(__name__)
log.setLevel(10)
fh = logging.FileHandler('orbitrap_file_transfer.log')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
log.addHandler(fh)
log.addHandler(ch)

# initialize
PATH_TO_LOG = os.path.join('C:\\', 'Thermo', 'Instruments', 'LTQ', 'system', 'logs')
DATEFORMAT = '%Y%m%d'

def main(interval):
    log.info('Started automatic file transfer for LTQ Orbitrap Velos.')
    while True:
        currentdate = datetime.datetime.now().strftime(DATEFORMAT)
        queue = get_queue()
        machine_log = get_logs(currentdate)
        if machine_log:
            queue = process_queue(machine_log, queue, currentdate)
            queue = transfer_files(queue, currentdate)
            update_queuefile(queue)
            log.info('Next iteration will be in {0} seconds'.format(interval))
        else:
            log.info('No logfile found for {0}, will try again at next iteration, in {1} seconds.'.format(currentdate, interval) )
        time.sleep(interval)


def get_queue():
    """load queues
    queue = {file: {status:open/closed/done, date: date_of_logfile}, file2: ETC}
    """
    try:
        with open('filequeue.json') as fp:
            queue = json.load(fp)
    except IOError:
        log.info('Could not open filequeue.json for queue reading. New empty queue generated.')
        return {}
    except ValueError:
        log.error('Could not parse JSON from filequeue.json for queue reading')
        raise
    else:
        log.info('Got queue from filequeue.json')
        return queue


def get_logs(currentdate):
    """open today's logfile"""
    current_logfile = os.path.join(PATH_TO_LOG, 'LTQ_{0}.LOG'.format(currentdate) )
    machine_log = []
    for tries in range(11):
        try:
            with open(current_logfile) as fp:
                for line in fp:
                    if line.strip():
                        machine_log.append( line.strip() )
                log.info('Opened logfile for {0}, read {1} lines.'.format(currentdate, len(machine_log)))
        except IOError:
            log.warning('Cannot open logfile for {0}, try {1}/10'.format(currentdate, tries) )
            if tries == 10:
                machine_log = False
            else:
                time.sleep(10)
        else:
            break
    return machine_log


def process_queue(machine_log, queue, currentdate):
    """Loop through log and put files in correct queues.
    """
    # At the end of the loop, the correct file will be in the open queue.
    current_filename = None # the file we are currently treating
    for logline in machine_log:
        logline = logline.split('  ')[1]
        if logline == 'Closed raw file':
            if current_filename:
                if queue[current_filename]['status'] == 'open':
                    queue[current_filename]['status'] = 'closed'
                    queue[current_filename]['date'] = currentdate
            else: # check if there is an open file from the previous day's log
                for fn in queue:
                    if queue[fn]['status'] == 'open':
                        queue[fn]['status'] = 'closed'
                        queue[fn]['date'] = currentdate
            current_filename = None
        
        elif 'Raw file created' in logline:
            current_filename = logline.split('=')[1].strip()
            if current_filename not in queue or queue[current_filename]['status'] != 'done':
                queue[current_filename] = {'status': 'open', 'date': currentdate}
            
    # remove old files from 'done' queue. Old: > 3 days.
    to_remove = []
    for fn in queue:
        if queue[fn]['status'] == 'done':
            olddate = datetime.datetime.strptime(queue[fn]['date'], DATEFORMAT)
            if olddate < datetime.datetime.strptime(currentdate, DATEFORMAT) - datetime.timedelta(3):
                to_remove.append(fn)
    for fn in to_remove:
        del(queue[fn])
        log.info('Removed old file with date {0} from the done queue.'.format(olddate))
    return queue


def transfer_files(queue, currentdate):
    transferred = False
    for fn in queue:
        if queue[fn]['status'] == 'closed':
            if not os.path.exists(fn):
                log.warning('Closed file {0} not found on local computer. Removed from queue.'.format(fn) )
                queue[fn]['status'] = 'done'
                continue
            try:
                subprocess.check_call(['C:\Program Files\ssh\pscp.exe', '-i', 'C:\Program Files\ssh\keys\orbi.ppk', fn, 'orbi@130.229.48.246:/mnt/incoming/'])
            except OSError:
                log.error('Could not call pscp.exe in the specified location c:\program files. Exiting')
                raise
            except subprocess.CalledProcessError:
                log.warning('Secure copying of file {0} to remote host failed.'.format(fn) )
            else:
                transferred = True
                log.info('File {0} copied to remote server.'.format(fn) )
                queue[fn]['status'] = 'done'
                queue[fn]['date'] = currentdate
    
    if not transferred:
        log.info('No files currently ready for transfer.')
    return queue


def update_queuefile(queue):
    # write queues to file:
    try:
        with open('filequeue.json', 'w') as fp:
            json.dump(queue, fp)
    except IOError:
        log.error('Could not open or parse filequeue.json for queue reading')
        raise
    else:
        log.info('Queue written to file.')



if __name__  == '__main__':
    main(1800)
