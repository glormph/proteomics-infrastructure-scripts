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
MAX_DAYS_IN_QUEUE = 5


def main(interval):
    log.info('Started automatic file transfer for LTQ Orbitrap Velos.')
    while True:
        currentdate = datetime.datetime.now().strftime(DATEFORMAT)
        yesterday = (datetime.datetime.now() - datetime.timedelta(1)).strftime(DATEFORMAT)
        queue = get_queue()
        machine_log = get_logs(currentdate, yesterday)
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


def get_logs(currentdate, yesterday):
    """read today and yesterday's logfile"""
    
    machine_log = []
    for date_of_log in [yesterday, currentdate]:
        logfile = os.path.join(PATH_TO_LOG, 'LTQ_{0}.LOG'.format(date_of_log) )

        for tries in range(11):
            try:
                with open(logfile) as fp:
                    for line in fp:
                        if line.strip():
                            machine_log.append( '{0}--{1}'.format(date_of_log, line.strip()) )
                    log.info('Opened logfile for {0}, accumulated {1} lines.'.format(date_of_log, len(machine_log)))
            except IOError:
                log.warning('Cannot open logfile for {0}, try {1}/10'.format(date_of_log, tries) )
                if tries == 10 and date_of_log == currentdate:
                    machine_log = False # no log today yet
                elif tries == 10 and date_of_log == yesterday:
                    break # there is no log from yesterday, machine may be new
                else:
                    time.sleep(10)
            else:
                break
    return machine_log


def process_queue(machine_log, queue, currentdate):
    """Loop through log and put files in correct queues.
    """
    current_file, current_time = None, None
    for logline in machine_log:
        logline = logline.split(':  ')
        timestamp = logline[0]
        logdate = timestamp.split('--')[0]
        logtext = logline[1]
        if logtext == 'Closed raw file':
            if current_file and current_time:
                queue[current_time]['status'] = 'closed'
                queue[current_time]['closed'] = logdate
            elif machine_log.index(':  '.join(logline)) == 0 and logdate == currentdate:
                log.warning('First line in todays log is a file closing, but no log files for yesterday found. Date--time: {0}'.format(timestamp))
            elif machine_log.index(':  '.join(logline)) == 0 and logdate != currentdate:
                log.info('First line in yesterdays log was a file closing.')
            current_file = None
            current_time = None
    
        elif 'Raw file created' in logtext:
            if timestamp not in queue: 
                current_file = logtext.split('=')[1].strip()
                current_time = timestamp
                queue[current_time] = {'file': current_file, 'status': 'open',
                'opened': logdate }
            elif queue[timestamp]['status'] == 'open':
                current_file = logtext.split('=')[1].strip()
                current_time = timestamp

    # remove old files from 'done' queue. Old: > MAX_DAYS_IN_QUEUE
    to_remove = []
    for timestamp in queue:
        if queue[timestamp]['status'] == 'done':
            olddate = datetime.datetime.strptime(queue[timestamp]['transferred'], DATEFORMAT)
            if olddate < datetime.datetime.strptime(currentdate, DATEFORMAT) - datetime.timedelta(MAX_DAYS_IN_QUEUE):
                to_remove.append(timestamp)
    for ts in to_remove:
        del(queue[ts])
        log.info('Removed old file with date {0} from the done queue.'.format(olddate))
    return queue


def transfer_files(queue, currentdate):
    transferred = False
    for timestamp in queue:
        fn = queue[timestamp]['file']
        if queue[timestamp]['status'] == 'closed':
            if not os.path.exists(fn):
                log.warning('Closed file {0} not found on local computer. Removed from queue.'.format(fn) )
                queue[timestamp]['status'] = 'done'
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
                queue[timestamp]['status'] = 'done'
                queue[timestamp]['transferred'] = currentdate
    
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
