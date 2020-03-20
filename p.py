# -*- coding: utf-8
import os
import pdfplumber
import re
# import threadpool
import time
from multiprocessing import Pool

# statstics of process 3 x 182 pages pdf
# - 1 processes,    pre-compile regex: 151s
# - 3 processes,    pre-compile regex: 84s
# - 3 processes, no pre-compile regex: 84s
# - 3 threads  , no pre-compile regex: 165s
# - 3 threads  ,    pre-compile regex: 159s
# - 1 process  ,    pre-compile regex: 148s

def process_files(path):
    # NOTE: due to pdfplumber produced pdf object is not thread safe
    # multi-thread/process can't be used at page level, while file level is all right
    """
    # multi-thread
    pool = threadpool.ThreadPool(5)
    args = []
    for file in os.listdir(path):
        if file[-4:].lower() == ".pdf":
            f_p = os.path.join(path, file)
            args.append((None, {"path":f_p}))
    reqs = threadpool.makeRequests(process_file, args)
    for req in reqs:
         pool.putRequest(req)
    pool.wait()
    """
    # as processing local pdf is CPU heavy, not IO heavy, multi-process is more
    # suitable than multi-thread
    # multi-process
    pool = Pool(processes = 3)
    for file in os.listdir(path):
        if file[-4:].lower() == ".pdf":
            f_p = os.path.join(path, file)
            pool.apply_async(process_file, args=(f_p, ))
    pool.close()
    pool.join()

def process_file(path):
    print("processing...", path)
    pdf=pdfplumber.open(path)
    pages=pdf.pages
    regex = re.compile(r'SaaS(.*?)元，', re.S)
    # NOTE: precompile regex is faster than compile regex at runtime
    for i in range(len(pages)):
        page_no = i+1
        page = pages[i]
        process_page(path, page_no, page, regex)

def process_page(path, page_no, page, regex):
    print("  extracting page...", page_no, " of ", path)
    #print(path, page_no, text)
    text = page.extract_text()
    # saas = regex.findall(text)
    saas = re.findall(r'SaaS(.*?)元，', text, re.S)
    for i in range(len(saas)):
         if len(saas[i])<=50:
             print(saas[i])
         print(type(saas[i]))
         print(len(saas[i]))
         print('*'*20)

def main():
    dir=r'D:\学习\春节大数据\金融大数据\用友PDF'
    dir = '/Users/y/Working/typd/saars-for-dc'

    start_time = time.time()
    process_files(dir)
    print("--- %s seconds ---" % (time.time() - start_time))


main()
