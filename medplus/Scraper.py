__author__ = 'jagzviruz'

import re
import os
import errno

from bs4 import BeautifulSoup
import requests
import sys, threading, time
import pycurl

import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.medplus_data

# Step 1 : Open the category listing page, and download the category pages, to extract the Product IDs

class DownLoadPageWithPyCurlInThread(threading.Thread):
    def __init__(self, url, output_file):
        threading.Thread.__init__(self)
        self.curl = pycurl.Curl()
        self.curl.setopt(pycurl.URL, url)
        self.curl.setopt(pycurl.WRITEDATA, output_file)
        self.curl.setopt(pycurl.FOLLOWLOCATION, 1)
        self.curl.setopt(pycurl.MAXREDIRS, 5)
        self.curl.setopt(pycurl.NOSIGNAL, 1)

    def run(self):
        self.curl.perform()
        self.curl.close()
        sys.stdout.write(". ")
        sys.stdout.flush()


class GetLinksFromFile:
    def __init__(self, start_file):
        text_file = open(start_file, "r")
        self.start_urls = text_file.readlines()
        text_file.close()


def processDownloadedCategoryFile(filepath):
    print("\r\n** Processing "+filepath + " with BeautifulSoup\n\r")
    file_handle = open(filepath,encoding='ISO-8859-1')
    content = file_handle.read()
    beautiful_soup = BeautifulSoup(content, "lxml")

    category_name = beautiful_soup.find('h1')

    if category_name:
        category_name = category_name.string.strip()
    else:
        category_name = ''

    print("\r\n*** Category Name : "+ category_name)
    ind = filepath.rfind('/')
    folder_path = filepath[0:ind + 1]
    cat_modified_name = re.sub(r'\W+', '',category_name)
    new_file_path = folder_path + cat_modified_name + ".html";
    print("\r\n**** Renaming " + filepath + ' to ' + new_file_path)
    os.rename(filepath, new_file_path)

    prodLinksFile = open('prodlinks.txt', 'a')

    obj = {
        'category_name': str(category_name),
        'category_prod_links': []
    }

    replace_pattern = re.compile(r'[\[\]\"]')
    inputStringWithAllProductIds = beautiful_soup.find('input', {'id': 'productIdString'})

    if inputStringWithAllProductIds is None:
        categoriesWithNoProducts = open('CategoriesWithNoProducts.txt', 'a')
        categoriesWithNoProducts.write(category_name + '\r\n')
        categoriesWithNoProducts.close()
        return

    inputStringWithAllProductIds = inputStringWithAllProductIds.get('value')
    inputStringWithAllProductIds = replace_pattern.sub('', inputStringWithAllProductIds)
    inputStringWithAllProductIds = inputStringWithAllProductIds.replace('\u0026', '&')
    inputStringWithAllProductIds = inputStringWithAllProductIds.split(",")

    prodPath = 'http://www.medplusmart.com/product/'+ cat_modified_name +'/'

    for prod_id in inputStringWithAllProductIds:
        small_obj = {
            "prod_id": prod_id,
            "prod_cat": category_name,
            "prod_link": prodPath + prod_id
        }
        prod_path = prodPath + prod_id

        item = checkIfProductPresentInProducts(prod_id)
        if item is None:
            link_obj = {
                "prod_id": prod_id,
                "prod_cat": [category_name],
                "prod_link": prod_path
            }

            storeDataInCollection('products', link_obj)
            prodLinksFile.write(prod_id + ':' + prod_path + "\r\n")

        else:
            item['prod_cat'].append(category_name)
            storeDataInCollection('products', item, True)
        obj['category_prod_links'].append(small_obj)

    storeDataInCollection('categories', obj)
    prodLinksFile.close()

def checkIfProductPresentInProducts(prod_id):
    prod = db.products.find_one({
        'prod_id': prod_id
    })

    if prod:
        return prod
    else:
        return None

def storeDataInCollection(collection_name, doc_to_store, updateMode=False):

    if updateMode:
        item = db[collection_name].find_one({
         '_id' : doc_to_store['_id']
        })

        item.update(doc_to_store)

        db[collection_name].update_one({
            '_id': item['_id']
        }, {
            '$set': item
        }, upsert=False)

    else:
        db[collection_name].insert(doc_to_store)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

def download (urls, output_folder):
    fileno = 0
    t1 = time.time()
    for url in urls:
        f = open(output_folder + str(fileno) + ".html", "wb")
        t = DownLoadPageWithPyCurlInThread(url.rstrip(), f)
        t.start()
        fileno = fileno + 1
        t.join()
        f.close()
    t2 = time.time()
    print( "\n** %d seconds elapsed for %d uris" % (int(t2-t1), len(urls)))

def parallel_download (urls, output_folder):
    threads = []
    fileno = 0

    # Start one thread per URI in parallel
    t1 = time.time()
    for url in urls:
        f = open(output_folder + str(fileno) + ".html", "wb")
        t = DownLoadPageWithPyCurlInThread(url.rstrip(), f)
        t.start()
        threads.append((t, f))
        fileno = fileno + 1

    # # Wait for all threads to finish
    for thread, file in threads:
        thread.join()
        file.close()
        processDownloadedCategoryFile( file.name)


    t2 = time.time()
    print("\n** Multithreading, %d seconds elapsed for %d uris" % (int(t2-t1), len(urls) ) )

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def startDownloadingCategoryPagesInParallelThreads(category_urls, path='somedir/', num_threads=1):

    make_sure_path_exists(path)

    if (num_threads > 30):
        print ("*** It is not advisable to run more than 30 threads in parallel.\r\n")
        exit()

    elif (num_threads == 1):
        download(category_urls, path)

    else:
        subs = chunks(category_urls, num_threads)
        for sub in subs:
            parallel_download(sub, path)


# Step 1 : Hit the Master category page and build the mongo collections that will store the data.

# startCrawl = GetLinksFromFile("start-urls.txt")
# ind = 0
# print("* Crawler Started")
# print("*** Found " + str(len(startCrawl.start_urls)) + " urls to start crawling from")

# for start_url in startCrawl.start_urls:
#     rootpath = str(start_url)[0:str(start_url).index('/pharmaHome')]
#     ind += 1
#     r = requests.get(start_url)
#     beautiful_soup = BeautifulSoup(r.content, "lxml")
#     divs = beautiful_soup.find_all('ul', {'class': 'brandlist-inline'})
#     links = beautiful_soup.find_all('a', {'href': re.compile('^/drugsCategory/*')})

#     print("***** Found "+str(len(links)) + " categories *** \r\n")
#     print("******** Storing all category links in one file : 'AllCategoryLinks.txt' ,\r\n")
#     print("********  which will be used to crawl to get product details")

#     fileToStoreLinksIn = open('AllCategoryLinks.txt', 'w')

#     for link in links:
#         w = rootpath
#         w = w + str(link.get('href')) + '\r\n'
#         fileToStoreLinksIn.write(w)
#     fileToStoreLinksIn.close()


# print("* Crawler will start crawling links in AllCategoryLinks.txt")

# startCrawl = GetLinksFromFile("AllCategoryLinks.txt")


# startDownloadingCategoryPagesInParallelThreads(startCrawl.start_urls, 'categories/',  30)


# Step 2 :
source_filename=    "prodlinks.txt"
completed_filename= "completed_scraping.txt"
error_filename=     "error_scraping.txt"

def fetchAllProdlinks() :
    file = open(source_filename,"r")
    links = file.readlines()
    file.close()

    return links

def updateProductMetaInMongo(prod_id, obj):
    item = db.products.find_one({
        'prod_id': prod_id
    })
    item.update(obj)
    db.products.update_one({
        '_id': item['_id']
    }, {
        '$set': obj
    },upsert=False)

    print("Updated : "+ prod_id)

def getPageContentWithRequests(url, pid):
    try:
        r = requests.get(url)
        beautiful_soup = BeautifulSoup(r.content, "lxml")
        prod_name = str(beautiful_soup.find('h1').text)

        dosage_form = beautiful_soup.find('strong',{ 'itemprop':"dosageForm" } )
        if not dosage_form is None:
            dosage_form = dosage_form.text.strip()
        else:
            dosage_form = 'NA'

        comp = beautiful_soup.find('strong',{ 'itemprop':"activeIngredient" } )
        if not comp is None:
            comp = comp.text.strip()
        else:
            comp = 'NA'

        pack_size = beautiful_soup.find('strong',{ 'itemprop':"activeIngredient" } )
        if not pack_size is None:
            pack_size = pack_size.text.strip()
        else:
            pack_size = 'NA'

        manufacturer = beautiful_soup.find('strong',{ 'itemprop':"name" } )
        if not manufacturer is None:
            manufacturer = manufacturer.text.strip()
        else:
            manufacturer = 'NA'

        mrp = beautiful_soup.find('span',{ 'itemprop':"costPerUnit" } )
        if not mrp is None:
            mrp = mrp.text.strip()
        else:
            mrp = 'NA'

        prod_meta = {
            'comp':  comp,
            'pack_size': pack_size,
            'manufacturer': manufacturer,
            'mrp': mrp,
            'form': dosage_form
        }

        alternates = beautiful_soup.find_all('tr',{'itemprop' : 'relatedDrug'})

        alternatives = []

        for alternate in alternates:
            alternate = BeautifulSoup(str(alternate), "lxml")
            link = alternate.find('a').get('href')
            prod_id = link.split('/')[-1]

            product_name = alternate.find('h5')
            if not product_name is None:
                product_name = product_name.text.strip()
            else:
                product_name = 'NA'

            tmp = {
                'prod_name' : product_name,
                'prod_id' : prod_id,
                'prod_link' : "http://www.medplusmart.com" + link,
                'prod_mfg' : alternate.find('small', {'itemprop' : 'manufacturer'}).find('span').text.strip()
            }
            alternatives.append(tmp)

        prod_information_blocks = beautiful_soup.find_all('div', {'class': 'tabbable'})

        prod_information = []

        for block in prod_information_blocks:
            cont = BeautifulSoup(str(block), 'lxml')
            cont = cont.find('div', {'class': 'container-fluid'})

            if cont is None:
                continue

            titles = cont.find_all('h4')
            descs = cont.find_all('p')
            index = 0

            for title in titles:
                desc = descs[index]

                if not desc is None:
                    desc = desc.text.strip()
                else:
                    desc = 'NA'

                tmp = {
                    'title' : title.text.strip(),
                    'description' : desc
                }

                prod_information.append(tmp)
                index = index + 1


        obj = {
            'prod_name' : prod_name,
            'prod_meta' : prod_meta,
            'alternatives' : alternatives,
            'prod_information' : prod_information
        }

        updateProductMetaInMongo(pid, obj)
    
    except Exception: 
        error_file = open(error_filename, 'a')
        error_file.write(pid + ':' + url + '\r')
        error_file.close()
        pass

# ----------------------------------------------------------------------------------------------------------------------

prod_links_lines = fetchAllProdlinks()
lines_written = 0

for line in prod_links_lines:
    if lines_written == 0 :
        completed_file = open(completed_filename, "a")

    parts = line.split(':', 1)
    prod_id = parts[0]
    prod_link = parts[1]
    getPageContentWithRequests(prod_link, prod_id)
    completed_file.write(line + '\r')
    lines_written = lines_written + 1
    if lines_written == 100:
        completed_file.close()
        lines_written = 0

