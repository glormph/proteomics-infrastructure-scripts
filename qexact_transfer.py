import os, json, glob, datetime, logging, time, subprocess

# prepare log
log = logging.getLogger(__name__)
log.setLevel(10)
fh = logging.FileHandler('qexactive_file_transfer.log')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - PID:%(process)d - %(name)s - \
%(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
log.addHandler(fh)
log.addHandler(ch)

# initialize
LOG_DIR = os.path.join('C:\\', 'Xcalibur', 'system', 'Exactive', 'log')
DATEFORMAT = '%Y%m%d'
MAX_DAYS_IN_QUEUE = 14 # should be more than 1
MAX_AGE_LOGFILE_DAYS = 200

def _change_queue_file(queue, fn, status, date):
    queue[fn]['status'] = status
    queue[fn]['date'] = date
    return queue

def main(interval):
    log.info('Started automatic file transfer for LTQ Orbitrap Velos.')
    while True:
        currentdate = datetime.datetime.now().strftime(DATEFORMAT)
        queue = get_queue()
        logs = get_logs()
        if logs:
            queue = process_queue(logs, queue, currentdate)
            if queue:
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


def get_logs():
    for tries in range(11):
        log.info('Trying to find newest logfile, try {0}/10'.format(tries) )
        logs = glob.glob(os.path.join(LOG_DIR, 'Thermo Exactive--*'))
        if logs:
            break
        elif tries == 10:
            log.info('No logfiles for today found')
            return False
    ages = {}
    for logfile in logs:
        fn = logfile[logfile.index('Thermo Exactive'):]
        filedate = fn.replace('Thermo Exactive--', '')
        filedate = filedate[:10] # length of Thermo date format! Hardcoded, I know, I know...
        age = datetime.datetime.now() - \
            datetime.datetime.strptime(filedate, '%Y-%m-%d')
        ages[ age.days ] = logfile

    log.info('Newest logfile is {0} day(s) old - {1}'.format(min(ages),
        ages[min(ages)] ) )
    return ages[min(ages)]


def process_queue(logfile, queue, currentdate):
    fn = None # the file we are currently treating
    try:
        with open(logfile) as fp:
            for logline in fp:
                if 'Starting acquisition' in logline:
                    age = logline[logline.index('=')+1:logline.index(' ')]
                    age = datetime.datetime.now() - \
                            datetime.datetime.strptime(age, '%Y-%m-%d')
                    if age.days < MAX_DAYS_IN_QUEUE:
                        fn = logline[logline.index('Starting acquisition: Xcalibur will write ')+42: logline.index(' (and may add date/time to the name)')]
                        if fn not in queue:
                            queue[fn] = {}
                            queue = _change_queue_file(queue, fn, 'open', currentdate)
    
                elif 'Stopping acquisition' in logline:
                    if fn in queue and queue[fn]['status'] == 'open':
                        queue = _change_queue_file(queue, fn, 'acquisition stop', currentdate)
#                    else: # first logfile did not start with opening, search
#                          # queue for open files
#                        for filename in queue:
#                            if queue[filename]['status'] == 'open':
#                                queue = _change_queue_file(queue, filename, 'acquisition \
#                                    stop', currentdate)
    
                elif 'Storing acquisition scan' in logline:
                    # if there is a file for which acq has stopped, it is closed
                    # here, I think
                    if fn in queue and queue[fn]['status'] == 'acquisition stop':
                        queue = _change_queue_file(queue, fn, 'closed', currentdate)
                        fn = None
#                    else:
#                        for filename in queue:
#                            if queue[filename]['status'] == 'acquisition stop':
#                                queue = _change_queue_file(queue, filename, 'closed', currentdate)
    except IOError:
        log.error('Cannot open found logfile {0}. Something may be wrong. \
            Skipping this iteration.')
        return False
    
    # Remove old files from queue
    to_remove = []
    for fn in queue:
        age = datetime.datetime.strptime(queue[fn]['date'], DATEFORMAT) - \
             datetime.datetime.now()
        if queue[fn]['status'] == 'done' and age.days > MAX_DAYS_IN_QUEUE:
            to_remove.append(fn)
    for fn in to_remove:
        log.info('Removing old file with date {0} from the done \
          queue.'.format(queue[fn]['date']))
        del(queue[fn])
    
    return queue


def transfer_files(queue, currentdate):
    transferred = False
    for fn in queue:
        if queue[fn]['status'] == 'closed':
            if not os.path.exists(fn):
                log.warning('Closed file {0} not found on local computer. \
                    Marked as closed in queue.'.format(fn) )
                queue[fn]['status'] = 'done'
                continue
            try:
                subprocess.check_call(['C:\Program Files\ssh\pscp.exe', '-i',
                'C:\Program Files\ssh\keys\qexact.ppk', fn, 'qexact@130.229.48.246:/mnt/incoming/'])
            except OSError:
                log.error('Could not call pscp.exe in the specified location \
                c:\Program Files\ssh. Exiting')
                raise
            except subprocess.CalledProcessError:
                log.warning('Secure copying of file {0} to remote host failed. \
                    Will try again at next iteration'.format(fn) )
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
