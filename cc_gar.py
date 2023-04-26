import os,re,csv,heapq,json,functools,threading,random,traceback,collections,datetime,time
from urllib import request,parse
import ssl,socket,requests
from faker import Faker
from bs4 import BeautifulSoup

MAX_URL_LENGTH = 1000

faker = Faker()
socket.setdefaulttimeout(30)
ssl._create_default_https_context = ssl._create_unverified_context

def get_ua() -> str:
    return r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
    #return faker.chrome()

def getdata(url,header=None,proxy=None,**k) -> bytes:
    '获取网页原始数据'
    headers = {'User-Agent':get_ua()}
    if header:
        headers.update(header)
    try:
        res = requests.get(url,headers=headers,proxies=proxy,**k)
        d = res.content
        if not d:return b''
        return d
    except Exception as e:
        print(repr(e))
    return b''

def filt_url(url):
    try:
        url,_ = url.split('#',1) # 过滤 #
    except ValueError:
        pass
    if len(url.encode('utf-8')) > MAX_URL_LENGTH:return # 过滤太长的url
    if re.search('[\x00-\x1f]',url):return # 过滤特殊字符
    if not re.match(r'https?\:\/\/',url):return # 过滤非法协议
    return url

def extract_links_soup(url,soup):
    ans = set()
    if isinstance(soup,str):return ans
    for a in soup('a'):
        try:
            v = a.attrs['href']
        except KeyError:
            continue
        try:
            v = parse.urljoin(url,v)
        except ValueError:
            continue
        ans.add(v)
    return ans

def extract_text_soup(soup):
    if isinstance(soup,str):return soup
    for script in soup(["script","style"]):
        script.extract()
    return soup.get_text()

def load_jsonl(fn:str):
    with open(fn,'r',encoding='utf-8') as f:
        for ln in f:
            ln = ln.strip()
            if not ln:continue
            yield json.loads(ln)

json_dumps = functools.partial(json.dumps,ensure_ascii=False,separators=(',',':'))

class CrawlRule:
    pt = '.*' # 匹配正则表达式
    soup = True # 是否要解析 soup
    follow = True # 是否跟踪链接
    func = None
    encoding = None # 解码网页使用的编码
    def __init__(self,**k):
        self.__dict__.update(k)

    def match_url(self,url:str):
        '仅匹配 url'
        p = re.fullmatch(self.pt,url)
        if p is None:return
        return self.func,p

    def match(self,url:str,html:bytes):
        '进行匹配，返回要传递的参数'
        p = re.fullmatch(self.pt,url)
        if p is None:return
        if self.encoding is not None:
            html = html.decode(self.encoding,'ignore')
        if self.soup or self.follow:
            soup = BeautifulSoup(html,'html.parser')
        s = soup if self.soup else html
        links = extract_links_soup(url,soup) if self.follow else []
        return self.func,p,s,links

def get_time_fn() -> str:
    '根据时间产生文件名'
    dt = datetime.datetime.now()
    ts = dt.isoformat('_')
    ts = re.sub(r'[/\-:]','',ts)
    ts = ts.replace('.','_')
    dt,_ = ts.split('_',1)
    return ts

class GarMgr:
    fn_q = os.path.join('q','q.csv')
    dn_d = 'data'
    data_size_limit = 25 << 20
    commit_size_limit = 100 << 20
    rules = [ # 重载此属性指定匹配规则
        CrawlRule(func='common')
        ]

    def __init__(self):
        self.t_init = time.time()
        self.q_seen = set()
        self.q_other = []
        self.load_url_seen()
        self.load_q()
        self.mutex = threading.Lock()
        self.dataq = collections.deque()
        self.dataf = None
        self.d_len_total = 0
        self.stopped = False

    def load_q(self):
        self.q_h = []
        with open(self.fn_q,'r',encoding='utf-8') as f:
            r = csv.reader(f)
            for url,p in r:
                url = filt_url(url)
                if not url:continue
                if url in self.q_seen:continue
                p = float(p)
                self.q_seen.add(url)
                self.q_h.append((p,url))
        heapq.heapify(self.q_h)
        print('任务个数:',len(self.q_h))
    
    def save_q(self):
        t = self.q_other.copy()
        for p,url in self.q_h:
            t.append((url,p))
        t.sort()
        with open(self.fn_q,'w',encoding='utf-8',newline='') as f:
            w = csv.writer(f)
            w.writerows(t)
    
    def append_q(self,url:str,p:float):
        url = filt_url(url)
        if not url:return
        if url in self.q_seen:return
        self.q_seen.add(url)
        heapq.heappush(self.q_h,(p,url))
    
    def pop_q(self) -> str:
        p,url = heapq.heappop(self.q_h)
        return url,p
    
    def load_url_seen(self):
        dn_d = self.dn_d
        n = 0
        for fn in os.listdir(dn_d):
            if not fn.endswith('.json'):continue
            fn = os.path.join(dn_d,fn)
            for da in load_jsonl(fn):
                self.q_seen.add(da['url'])
                n += 1
        print('数据个数:',n)
    
    def flush_data_file(self):
        if self.dataf is None:return
        self.d_len_total += self.dataf.tell()
        self.dataf.close()
        self.dataf = None
    
    def save_one_data(self,doc:dict):
        if self.dataf is None:
            fn = os.path.join(self.dn_d,get_time_fn() + '.json')
            self.dataf = open(fn,'w',encoding='utf-8')
        s = json_dumps(doc)
        self.dataf.write(s + '\n')
        if self.dataf.tell() >= self.data_size_limit:
            self.flush_data_file()
    
    def get_d_len_total(self):
        ans = self.d_len_total
        if self.dataf is not None:
            ans = ans + self.dataf.tell()
        return ans

    def add_tasks(self,urls,p0=1,p_rand=0.1):
        with self.mutex:
            for url in urls:
                p = p0 + random.uniform(-p_rand,p_rand)
                self.append_q(url,p)
    
    def fetch_task(self):
        with self.mutex:
            while True:
                try:
                    url,p = self.pop_q()
                except IndexError:
                    return
                t = self.convert_worker_url(url)
                if t is False:
                    #print('ignored:',url)
                    self.q_other.append((url,p))
                    continue
                if not t:
                    print('ignored converted:',url)
                    continue
                if t != url:
                    print('converted:',url,t)
                    self.append_q(t,p)
                    continue
                return t
    
    def match_url(self,url:str):
        for rule in self.rules:
            t = rule.match_url(url)
            if t:return t
    
    def match(self,url:str,html:bytes):
        for rule in self.rules:
            t = rule.match(url,html)
            if t:return t
    
    def convert_worker_url(self,url:str):
        t = self.match_url(url)
        if t is None:return False
        func,p = t
        if isinstance(func,str):
            func = getattr(self,f'prepare_{func}',None)
        if func is not None:
            t = func(p,url)
            return t
        return url

    def execute_match(self,url,func,p,s,links):
        '执行匹配结果'
        if isinstance(func,str):
            func = getattr(self,f'parse_{func}')
        doc = {}
        if func is not None:
            doc = func(p,s,url) # 解析器返回要添加的数据
        doc['url'] = url
        self.dataq.append(doc)
        tasks = []
        for url in links:
            t = self.convert_worker_url(url)
            if t is not False:
                if not t:continue
                url = t
            tasks.append(url)
        self.add_tasks(tasks)

    def crawl_one(self,url:str):
        '进行抓取并处理结果'
        html = getdata(url)
        if not html:return # 交给重试
        t = self.match(url,html)
        if not t:
            doc = {}
            doc['url'] = url
            self.dataq.append(doc)
            return
        try:
            self.execute_match(url,*t)
        except Exception:
            print('exc:',url)
            name = threading.current_thread().name
            with open(f'exc_{name}_trace.txt','w',encoding='utf-8') as f:
                f.write(f'# {url}\n\n')
                f.write(traceback.format_exc())
            with open(f'exc_{name}_page.html','wb') as f:
                f.write(html)
            raise

    def prepare_common(self,p,url:str):
        '准备抓取，可对 url 进行归一化等操作'
        return url

    def parse_common(self,p,soup,url:str):
        '进行解析'
        text = extract_text_soup(soup)
        return {'text':text}
    
    def th_robot(self,time_idle=1):
        while not self.stopped:
            url = self.fetch_task()
            if not url:
                time.sleep(time_idle)
                continue
            #print('crawl:',url)
            self.crawl_one(url)

    def flush_datas(self):
        has_data = False
        while True:
            try:
                doc = self.dataq.popleft()
            except IndexError:
                break
            has_data = True
            self.save_one_data(doc)
        return has_data

    def main_robot(self,tn=4,time_idle=1,time_run=300,time_wait=10):
        for _ in range(tn):
            t = threading.Thread(target=self.th_robot,daemon=True)
            t.start()
        t_end = self.t_init + time_run
        t_stop = t_end - time_wait - 2
        t_data = time.time()
        reason = None
        while True:
            time.sleep(time_idle)
            dt = time.time()
            if self.flush_datas():
                t_data = dt
            else:
                if dt - t_data > time_wait:
                    reason = reason or 'idle'
                    break
            if self.get_d_len_total() >= self.commit_size_limit:
                reason = 'data_limit'
                break
            if dt >= t_stop:
                self.stopped = True
                reason = 'time_limit'
                continue
            if dt >= t_end:
                reason = 'time_limit'
                break
        print('reason:',reason)
        with self.mutex:
            self.save_q()
        self.flush_datas()
        self.flush_data_file()
        print('exiting...')
