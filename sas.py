import urllib.request as req
import random
import re
from threading import Thread, Lock
import logging
import configparser
from optparse import OptionParser

log = logging.getLogger()
log.setLevel(logging.DEBUG)


def gen_urls(urls):
    '''generator of urls

takes array of urls from the list and return one by one in random order'''
    while len(urls) > 0:
        #yielding random url form list
        urln = random.randint(0, len(urls)-1)
        url = urls[urln]
        del urls[urln]
        yield url.strip()


def unanchor(link: str):
    '''fetch the url from a tag (<a href="url">)'''
    #pattern like <a href="group_with_url">
    pattern = r'<a.* href=[\"\'](?P<url>.*)[\"\'].*>'
    try:
        mat = re.search(pattern, link)
        #fetching url from a_href tag
        url = mat.group('url')
    except Exception as e:
        log.error("Cannot unanchor link: " + link)
        log.exception(e)
        log.error(f"unanchor returning {link}")
        #returning previous link if cannot parse
        return link
    else:
        if url is not None:
            return url
        else:
            return link


def getproxy(proxy_list):
    '''Return random proxy form proxy_list or None'''
    try:
        return random.choice(proxy_list)
    except Exception as e:
        log.info(f"Proxy: {e}, return None")
        return None


class WorkingThread(Thread):
    #parallel thread worker
    def __init__(self, worno: int, url_list, word_list, proxy_list):
        super().__init__()
        #worno - number for worker
        self.worno = worno
        #instanse of url generator (all for every worker)
        self.url_list = url_list
        self.word_list = word_list
        self.proxy_list = proxy_list
        self.log = logging.getLogger(name=f'W#{worno}')

    def run(self):
        '''true thread worker'''
        log = self.log
        while True:
            try:
                #fetching url from generator (with lock ofk)
                with gen_lock:
                    link = next(self.url_list)
                if is_link_wrapped:
                    url = unanchor(link)
                else:
                    url = link
                #check url html for key words
                iconic = self.is_consist(url)
                if iconic is None:
                    #if none - probably, site not reached - bad link
                    continue
                if iconic:
                    #there are key word - valid
                    with valid_lock:
                        valid_links.append(link + '\n')
                else:
                    #there are no key words - invalid link
                    with invalid_lock:
                        invalid_links.append(link + '\n')

            except StopIteration:
                #generator of url is empty - work done
                log.info("Reaches end of list of sites")
                break

    def is_consist(self, url):
        log = self.log
        '''fetching data from url, and check if one of words in data'''
        #take random proxy from proxy list
        proxy_list = self.proxy_list
        proxy = getproxy(proxy_list)
        while True:
            log.info(f"Try {url}, through proxy {proxy}")
            try:
                #creating url request
                r = req.Request(url)
                if proxy is not None:
                    #adds proxy to the request if exist
                    if 'http://' in url:
                        r.set_proxy(proxy, "http")
                    else:                    
                        r.set_proxy(proxy, "http")
                        r.set_proxy(proxy, "https")
                log.info(f"Request: {r.full_url}, has proxy: {r.has_proxy()}")
                with req.urlopen(r) as response:
                    #fetching code from url
                    code = response.read()\
                    .decode(encoding='utf-8', errors='ignore')
                    log.info(f"Got data form {url}")
                    #cheks code for words
                    for word in self.word_list:
                        if word in code:
                            log.info(f"@ {url} match word '{word}' - VALID")
                            return True
                    log.info(f"@ {url} - no matches - INVALID")
                    return False
            except Exception as e:
                #if connection error (for example, bad proxy)
                log.warning(f"{url} thru {proxy} Connection: {e}")
                log.warning("Try another proxy")
                #removing all entries of bad proxy
                proxy_list = proxy_list.copy()
                while proxy in proxy_list:
                    proxy_list.remove(proxy)
                if len(proxy_list) > 0:
                    #if there are another proxy in the list - choose random and
                    #another try
                    proxy = getproxy(proxy_list)
                    log.info(f"Match new proxy {proxy}")
                    continue
                else:
                    #there are no valid proxy in the list - site is cannot reached
                    log.error(f"No more proxies. {url} - bad link")
                    with bad_lock:
                        bad_links.append(url)
                    break


def main():
    #formatter for all log handlers
    formatter = logging.Formatter('%(levelname)4.4s:%(asctime)23.23s:\
%(name)-4.4s: %(message)s')
    #default output log handler to stdout (level - WARNING and higher)
    stdo = logging.StreamHandler()
    stdo.setLevel(logging.WARNING)
    stdo.setFormatter(formatter)
    log.addHandler(stdo)

    #parsing command line options
    parser = OptionParser(usage='usage: %prog [options]',
                          version='%prog v1.0')
    #add option of choising config file
    parser.add_option("-c", "--config",
                      action='store', type='string',
                      dest='config_path', default='config.txt', metavar='FILE',
                      help='path to config file [default: %default]')
    #add option for verbose output
    parser.add_option("-v", "--verbose",
                      action='store_true', dest='verbose', default = False,
                      help='verbosing all logs to stdout')
    #add option for withoit output
    parser.add_option("-q", "--quiet",
                      action='store_true', dest='quiet', default = False,
                      help="don't print messages to stdout")
    #option to save logs to file
    parser.add_option('-l', '--log',
                      action='store', type='string', dest='log_path',
                      default = None, metavar='FILE',
                      help='save all logs to FILE' )
    (options, args) = parser.parse_args()
    config_path = options.config_path
    if options.verbose:
        #level of stdout output log handler low to level DEBUG
        stdo.setLevel(logging.DEBUG)
    if options.quiet:
        #in quite mode remove stdout handler
        log.removeHandler(stdo)
    if options.log_path is not None:
        #output log handler to file (level - DEBUG and higher)
        ft = logging.FileHandler(options.log_path, mode = 'w')
        ft.setLevel(logging.DEBUG)
        ft.setFormatter(formatter)
        log.addHandler(ft)
    
    log.info("START")
    #set the config file path (from default or from command line arg)
    log.info("Config file name is 'config_path'")

    #generate default configs
    defaults = {
        'is_link_wrapped': False,
        'proxy_list_file_path': 'proxy.txt',
        'site_list_file_path': 'site.txt',
        'words_list_file_path': 'words.txt',
        'max_thread_count': 30,
        'path_to_valid_links': 'valid_links.txt',
        'path_to_invalid_links': 'invalid_links.txt',
        }
    #create config parser
    config = configparser.ConfigParser(defaults = defaults)
    try:
        with open(config_path, 'r') as conf:
            #reading configs from file
            log.info(f"Reading configs from '{config_path}'")
            config.read_file(conf)
            pass
    except FileNotFoundError:
        #if config file not exist - create them and write default config
        log.warning(f"Config file '{config_path}' not found")
        log.warning(f"Try to create '{config_path}'")
        with open(config_path, 'w') as conf:
            config.write(conf)
            log.info("Confige writed to '{config_path}'")

    #parsing config into variables
    log.info('CONFIGS')
    global is_link_wrapped
    is_link_wrapped = config.getboolean('DEFAULT','is_link_wrapped')
    log.info(f"is_link_wrapped = '{str(is_link_wrapped)}'")
    proxy_list_file_path = config.get('DEFAULT','proxy_list_file_path')
    log.info(f"proxy_list_file_path = '{proxy_list_file_path}'")
    site_list_file_path = config.get('DEFAULT','site_list_file_path')
    log.info(f"site_list_file_path = '{site_list_file_path}'")
    words_list_file_path = config.get('DEFAULT','words_list_file_path')
    log.info(f"words_list_file_path = '{words_list_file_path}'")
    max_thread_count = config.getint('DEFAULT', 'max_thread_count')
    log.info(f"max_thread_count = '{str(max_thread_count)}'")
    path_to_valid_links = config.get('DEFAULT','path_to_valid_links')
    log.info(f"path_to_valid_links = '{path_to_valid_links}'")
    path_to_invalid_links = config.get('DEFAULT','path_to_invalid_links')
    log.info(f"path_to_invalid_links = '{path_to_invalid_links}'")

    #fetching urls from list
    try:
        with open(site_list_file_path, 'r') as sites:
            urls = sites.readlines()
            log.info(f"Got urls from {site_list_file_path}\
- {len(urls)} entries")
    except Exception as e:
        log.critical('Cannot read file (list of sites) ' + site_list_file_path)
        log.exception(e)
        exit()
    else:
        #create generator of random url list
        url_list = gen_urls(urls)
    
    #fetching key words from file
    try:
        wordfile = open(words_list_file_path, 'r')
        word_list = wordfile.readlines()
        word_list = list(map(lambda x: x.strip(), word_list))
        log.info(f"Got words from '{words_list_file_path}' - {len(word_list)} entries")
    except Exception as e:
        log.critical("Cannot read file with kwords '{words_list_file_path}'")
        log.exception(e)
        exit()
    else:
        wordfile.close()

    #fetching proxy list form file
    try:
        proxyfile = open(proxy_list_file_path)
        proxy_list = proxyfile.readlines()
        proxy_list = list(map(lambda x: x.strip(), proxy_list))
        log.info(f"Got proxies from '{proxy_list_file_path}' - {len(proxy_list)} entries")
    except Exception as e:
        log.error(f"Cannot read from proxy file '{proxy_list_file_path}'")
        log.exception(e)
        log.error("Using Null list of proxies")
        proxy_list = None
    else:
        proxyfile.close

    #generating global variables for links
    global valid_links
    valid_links = []
    global invalid_links
    invalid_links = []
    global bad_links
    bad_links = list()

    #locks for parallel writing for workers
    global valid_lock
    valid_lock = Lock()
    global invalid_lock
    invalid_lock = Lock()
    global gen_lock
    gen_lock = Lock()
    global bad_lock
    bad_lock = Lock()

    #generating pool of thread workers
    threads = []
    for i in range(max_thread_count):
        mthread = WorkingThread(i, url_list, word_list, proxy_list)
        threads.append(mthread)
        mthread.start()
        log.info(f'Worker {i} started')

    #utilizing all done workers
    for t in threads:
        t.join()
        log.info(f'Worker {t.worno} joined')

    #writing valid links to the file
    with open(path_to_valid_links, 'w') as vl:
        vl.writelines(valid_links)
        log.info(f"Wrote {len(valid_links)} valid urls to '{path_to_valid_links}'")

    #writing all invalid links to the file
    with open(path_to_invalid_links, 'w') as ivl:
        ivl.writelines(invalid_links)
        log.info(f"Wrote {len(invalid_links)} invalid urls to '{path_to_valid_links}'")

    #writing bad links to the file if that exist
    if len(bad_links) > 0:
        with open("bad_links.txt", 'w') as bl:
            for url in bad_links:
                bl.write(url+'\n')
        log.warning(f"Wrote {len(bad_links)} urls to 'bad_links.txt'")

    log.info("FINISH")


if __name__ == '__main__':
    main()
