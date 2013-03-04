import os, datetime, subprocess, logging, json, time, glob

# prepare log
log = logging.getLogger(__name__)
log.setLevel(10)
fh = logging.FileHandler('auto_file_transfer.log')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
log.addHandler(fh)
log.addHandler(ch)

#FIXME there is no acquisition stop in orbi yet it is required for something
# do we need to wait for it or is it enough without acqui stop?

# initialize FIXME
PATH_TO_LOG = os.path.join('C:\\', 'Thermo', 'Instruments', 'LTQ', 'system', 'logs')
DATEFORMAT = '%Y%m%d'
MAX_DAYS_IN_QUEUE = 5
keyfile = 'C:\Program Files\ssh\keys\orbi.ppk'

class BaseFileTransferrer(object):
    def __init__(self, name, interval, logdir):
        self.name = name
        self.interval = interval
        self.logdir = logdir

    def run(self):
        log.info('Started automatic file transfer for {0}'.format(self.name))
        while True:
            self.load_queue()
            self.lastclosed_timestamp = self.get_lastclosed_timestamp()
            self.read_log()
            if self.machine_log:
                self.put_log_in_queue()
                self.remove_old_from_queue()
                self.transfer_files()
                self.update_queue_log_files()
                log.info('Next iteration will be in {0} seconds'.format(self.interval))
            else:
                log.info('No logfile found, will try again at next '
                'iteration, in {0} seconds.'.format(self.interval) )
            time.sleep(self.interval)
    
    def read_log(self):
        return True
        
    def load_queue(self):
        """load queues
        queue = {file: {status:open/closed/done, date: date_of_logfile}, file2: ETC}
        """
        try:
            with open('filequeue.json') as fp:
                self.queue = json.load(fp)
        except IOError:
            log.info('Could not open filequeue.json for queue reading. New empty queue generated.')
            self.queue = {}
        except ValueError:
            log.error('Could not parse JSON from filequeue.json for queue reading')
            raise
        else:
            log.info('Got queue from filequeue.json')
    
    def update_queue_log_files(self):
        # write queues to file:
        try:
            with open('filequeue.json', 'w') as fp:
                json.dump(self.queue, fp)
        except IOError:
            log.error('Could not open filequeu.json for queue reading')
            raise
        else:
            log.info('Queue written to file.')

    def update_queue_entry(self, timestamp, **kwargs):
        for k in kwargs:
            self.queue[timestamp][k] = kwargs[k]
        
    def remove_old_from_queue(self):
        # Remove old files from queue
        to_remove = []
        for timestamp in self.queue:
            age = datetime.datetime.now() - self.queue[timestamp]['closedate']
            if self.queue[timestamp]['status'] == 'done' and age.days > MAX_DAYS_IN_QUEUE:
                to_remove.append(timestamp)
        for timestamp in to_remove:
            log.info('Removing old file with date {0} from the done '
                    'queue.'.format(self.queue[timestamp]['closedate']))
            del(self.queue[timestamp])
    
    def get_last_timestamps(self):
        try:
            with open('logrec.txt') as fp:
                last = json.load(fp)
        except IOError:
            log.error('Could not load timestamps from logrec.txt. New empty '
            'last timestamps generated.')
            self.lastopened_timestamp  = {}
            self.lastclosed_timestamp = {}
        else:
            self.lastopened_timestamp = datetime.datetime.strptime( \
                    last['last_opened'], '%Y%m%d %H:%M:%S.%f')
            self.lastclosed_timestamp = datetime.datetime.strptime( \
                    last['last_closed'], '%Y%m%d %H:%M:%S.%f')
            log.info('Read last open/close file timestamps')
    
    def set_lastopened_timestamp(self, timestamp):
        self.lastopened_timestamp = timestamp
        self.save_timestamps()

    def set_lastclosed_timestamp(self, timestamp):
        self.lastclosed_timestamp = timestamp
        self.save_timestamps()

    def save_timestamps(self):
        ts = {
            'last_opened': datetime.datetime.strftime( \
                self.lastopened_timestamp, '%Y%m%d %H:%M:%S.%f'),
            'last_closed': datetime.datetime.strftime( \
                self.lastclosed_timestamp, '%Y%m%d %H:%M:%S.%f')
            }
        try:
            with open('logrec.txt', 'w') as fp:
                json.dump(fp, ts)
        except IOError:
            log.error('Could not save timestamps in logrec.txt')
            raise
        else:
            log.info('Read last checked logtime')
    
    def put_log_in_queue(self):
        for timestamp, logline in self.machine_log:
            if self.start in logline:
                age = datetime.datetime.now() - timestamp
                if age.days < MAX_DAYS_IN_QUEUE:
                    fn = self.get_filename_from_logline(logline)
                    if timestamp not in self.queue:
                        self.set_lastopened_timestamp(timestamp)
                        self.queue[timestamp] = {'file': fn}
                        self.update_queue_entry(timestamp, status='open',
                                openeddate=timestamp.date())

            elif self.stop in logline:
                if self.lastopened_timestamp in self.queue and \
                        self.queue[self.lastopened_timestamp]['status'] == 'open':
                    self.update_queue_entry(timestamp, status='acquisition '
                            'stop', stoppeddate=timestamp.date() )

            elif self.store in logline:
                # if there is a file for which acq has stopped, it is closed
                # here, I think
                if self.lastopened_timestamp in self.queue and \
                        self.queue[self.lastopened_timestamp]['status'] == 'acquisition stop':
                    self.update_queue_entry(timestamp, status='closed', closeddate=timestamp.date() )
                    self.set_lastclosed_timestamp(timestamp)

    def transfer_files(self, queue, currentdate):
        transferred = False
        for timestamp in queue:
            fn = queue[timestamp]['file']
            if queue[timestamp]['status'] == 'closed':
                if not os.path.exists(fn):
                    # for example, file name changed by user before we can transfer
                    log.warning('Closed file {0} not found on local computer. Removed from queue.'.format(fn) )
                    queue[timestamp]['status'] = 'done'
                    queue[timestamp]['transferred'] = False
                    continue
                try:
                    subprocess.check_call(['C:\Program Files\ssh\pscp.exe', '-i',
                    self.keyfile, fn,
                            'orbi@130.229.48.246:/mnt/datadrive/'])
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


class OrbiFileTransferrer(BaseFileTransferrer):
    def __init__(self, name, interval, logdir):
        super(OrbiFileTransferrer, self).__init__(name, interval, logdir)
        self.start = 'Raw file created, actual name'
        self.stop = 'Stopping acquisition -- this doesnt exist in orbi log'
        self.store = 'Closed raw file'
        self.keyfile = keyfile

    def read_log(self):
        """read today and yesterday's logfile"""
        self.machine_log = []
        currentdate = datetime.datetime.now().strftime(DATEFORMAT) #TODO 20120425
        # FIX DATE FORMAT?
        yesterday = (datetime.datetime.now() - datetime.timedelta(1)).strftime(DATEFORMAT)
        # FIXME should we use only yester/today, or start from self.lastread ?
        # First start would in latter case take all logfiles!
        for date_of_log in [yesterday, currentdate]:
            logfile = os.path.join(self.logdir, 'LTQ_{0}.LOG'.format(date_of_log) )
            for tries in range(11):
                try:
                    with open(logfile) as fp:
                        for line in fp:
                            if line.strip():
                                logline = line.split(':  ')
                                timestamp = date_of_log + ' ' + logline[0]
                                timestamp = datetime.datetime(timestamp,
                                '%Y%m%d %H%M%S')
                                if timestamp > self.lastread:
                                    self.machine_log.append( (timestamp,logline[1]) )
                                  
                        log.info('Opened logfile for {0}, accumulated {1} '
                        'lines.'.format(date_of_log, len(self.machine_log)))
                except IOError:
                    log.warning('Cannot open logfile for {0}, try {1}/10'.format(date_of_log, tries) )
                    if tries == 10 and date_of_log == currentdate:
                        self.machine_log = False # no log today yet
                    elif tries == 10 and date_of_log == yesterday:
                        break # there is no log from yesterday, maybe started
                        # running today
                    else:
                        time.sleep(10)
                else:
                    break

    def get_filename_from_logline(logline):
        return logline.split('=')[1].strip()


class QExactiveFileTransferrer(BaseFileTransferrer):
    def __init__(self, name, interval, logdir):
        super(QExactiveFileTransferrer, self).__init__(name, interval, logdir)
        self.start = 'Starting acquisition'
        self.stop = 'Stopping acquisition'
        self.store = 'Storing acquisition scan'

    def read_log(self):
        self.machine_log = []
        for tries in range(11):
            log.info('Trying to find newest logfile, try {0}/10'.format(tries) )
            logs = glob.glob(os.path.join(self.logdir, 'Thermo Exactive--*'))
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
        # found newest logfile. Now parse the unread lines from it.
        with open(ages[min(ages)]) as fp:
            for line in fp:
                linetime = line[1:line.index(']')].split('=')[1].split('+')[0]
                linetime = datetime.datetime.strptime(linetime, '%Y-%m-%d '
                '%H:%M:%S.%f')
                if linetime > self.lastclosed_timestamp:
                    self.machine_log.append( (linetime, line) )

    def get_filename_from_logline(logline):
        return logline[logline.index('Starting acquisition: Xcalibur will write' \
                    )+42: logline.index(' (and may add date/time to the name)')]

                        
