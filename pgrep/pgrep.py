'''
Created on Feb 4, 2013

@author: nabeel
'''

import sys
import getopt
import os
import threading
import time
import logging

logger=''

def main():
    get_opt()
    master()
    
opts={}
def get_opt():
    global opts
    try:
        options, remainder = getopt.gnu_getopt(sys.argv[1:], 't:vq:Tf:p:')
    except getopt.GetoptError as err:
        logger.error('Error parsing options: ' + format(err))
        usage()
        sys.exit(2)
    
    opts['target_filename'] = sys.argv[len(sys.argv)-1]
    opts['thread_count'] = 1
    opts['verbrose'] = True
    opts['break_count'] = 0
    opts['profile'] = True
    opts['pattern'] = ''
    
    print(options)
    for opt,arg in options:
        print('opt='+opt+',arg='+arg)
        if opt in ('-t'):
            opts['thread_count'] = arg
        elif opt in ('-v'):
            opts['verbrose'] = True
        elif opt in ('-q'):
            opts['break_count'] = arg
        elif opt in ('-T'):
            opts['profile'] = True
        elif opt in ('-f'):
            opts['pattern_file'] = arg
            if opts.has_key('pattern'):
                logger.error('Error: Invalid Command line option, Either use -p or -f')
                usage()
                sys.exit(2)                
        elif opt in ('-p'):
            opts['pattern'] = arg
            if opts.has_key('pattern_file'):
                logger.error('Error: Invalid Command line option, Either use -p or -f')
                usage()
                sys.exit(2)                
        
# This function opens the file, jumps to offset, reads file line by line, \
# searches for pattern line by line, updates patterns_found_map, \
# reads until read_limit is reached, closes the file
#  
class worker_thread(threading.Thread):
    def __init__(self, threadID, name, read_start, read_limit):
        self.threadID = threadID
        #self.name = name
        self.read_start = read_start
        self.read_limit = read_limit
        threading.Thread.__init__(self)
    def run(self):
        logger.debug('ThrName='+self.name+'-'+str(self.read_limit))
        search_str = opts['pattern']
        try:
            f= open(opts['target_filename'],'r')
            f.seek(self.read_start)
            while f.tell() < self.read_limit:
                line = f.readline()
                if search_str in line:
                    print(self.getName() + '#' + line.strip())
        finally:
            f.close()

thread_data={}

def master():
    threads=[]
    tc = int(opts['thread_count'])
    target_filesize= os.path.getsize(opts['target_filename'])
    chunk_size=target_filesize/tc
    #create threads   
    for i in range(0,tc):
        read_limit = chunk_size*(i+1)
        if read_limit > target_filesize:
            read_limit=target_filesize
        thread = worker_thread(i, 'Thread' + str(i), chunk_size*i, read_limit)     
        thread.start()       
        threads.append(thread)
            
    for t in threads:
        logger.debug('Joining Thread' + t.getName())
        t.join()
    
 
    
    
def usage():
    logger.error('Usage:' + sys.argv[0] + '[options] [thread_count] [pattern|pattern_file] file|files')
    logger.error('\t\t -t:\t Number of Threads')
    logger.error('\t\t -v:\t verbrose')
    logger.error('\t\t -q num_of_matches:\t Exit after num_of_matches')
    logger.error('\t\t -T:\t record time')
    logger.error('\t\t -f pattern_file:\t use patterns from pattern_file separated by new_line')
    logger.error('\t\t -p:\t pattern')
    
if __name__ == '__main__':
    global logger
    logger = logging.getLogger('pgrep')
    logger.setLevel(logging.DEBUG)
    main()

