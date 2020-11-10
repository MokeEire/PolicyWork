# -*- coding: utf-8 -*-
# Author: Mark Barrett - mbarrett@rand.org

# Purpose: Crawl Colorado Red Card Data webpages using Selenium and download all the files 
#		   to my local machine.

import selenium
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from bs4 import BeautifulSoup
import re
from itertools import chain
import os
import shutil

def newChromeBrowser(headless=False, downloadPath=None):
    """ Helper function that creates a new Selenium browser """
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('headless')
    if downloadPath is not None:
        prefs = {}
        try:
            os.makedirs(downloadPath)
        except FileExistsError:
            pass
        prefs["profile.default_content_settings.popups"]=0
        prefs["download.default_directory"]=downloadPath
        options.add_experimental_option("prefs", prefs)
    browser = webdriver.Chrome(options=options, executable_path='C:/Users/Mark/Documents/DataScience/chromedriver')
    return browser

# Found here: 
    # https://stackoverflow.com/questions/34515328/how-to-set-default-download-directory-in-selenium-chrome-capabilities
browser = newChromeBrowser(downloadPath='C:/Users/Mark/Documents/DataScience/Webscraping')

#browser.get('https://www.colorado.gov/pacific/cdphe/medical-marijuana-statistics-and-data')

#linkElem = browser.find_elements_by_css_selector('#content-inner li a')
#browser.find_elements_by_tag_name('a')
#links = browser.find_elements_by_id(re.compile('(20[0-9]{2} statistics)|Average patient count'))


## Testing when function not working
#test = soup_links[0]
#
#test.click()
#soup_level2 = BeautifulSoup(browser.page_source, 'lxml')
#
#month_elements = soup_level2.find_all('a', href=re.compile(r'drive\.google\.com'))
#month_elements_text = [re.findall(r'[A-Za-z]+', x.text) for x in month_elements]
#month_elements_text = list(chain.from_iterable(month_elements_text))
#drive_links = list(chain.from_iterable([browser.find_elements_by_link_text(element.text) for element in month_elements]))
#
#drive_links[1].click()
#download_button = browser.find_element_by_css_selector('.ndfHFb-c4YZDc-nupQLb-Bz112c')
#download_button.click()
#browser.back()




def get_datafiles():
    
    # First go to the data files page
    browser.get('https://dlapiper.taleo.net/careersection/uk/jobsearch.ftl?lang=en&radiusType=K&location=10605100331&searchExpanded=true&radius=1&portal=101430233')
    browser.implicitly_wait(2)
    browser.find_element_by_css_selector('.absolute a').click()
    # Get the links to the different years
    soup_level1 = BeautifulSoup(browser.page_source, 'lxml')
    soup_elements = soup_level1.find_all('a', href=re.compile(r'201[1-9]-medical-marijuana-registry-statistics'))
    
    for i in range(len(soup_elements)):
        # Get the links to the different years
        soup_level1 = BeautifulSoup(browser.page_source, 'lxml')
        soup_elements = soup_level1.find_all('a', href=re.compile(r'201[1-9]-medical-marijuana-registry-statistics'))
        soup_links = [browser.find_elements_by_link_text(element.text) for element in soup_elements]
        soup_links = list(chain.from_iterable(soup_links))
        
        # Click into each year
        try:
            soup_links[i].click()
            
            # Look at the google drive links for each month
            soup_level2 = BeautifulSoup(browser.page_source, 'lxml')
            month_elements = soup_level2.find_all('a', href=re.compile(r'drive\.google\.com'))
            month_elements_text = [re.findall(r'[A-Za-z]+', x.text) for x in month_elements]
            month_elements_text = list(chain.from_iterable(month_elements_text))
            # Click into each google drive link
            for j in range(len(month_elements_text)):
                if i != 3:
                    # For each link: click, find download button, download, then go back to the year
                    drive_links = list(chain.from_iterable([browser.find_elements_by_link_text(element) for element in month_elements_text]))
                else:
                    drive_links = list(chain.from_iterable([browser.find_elements_by_partial_link_text(element) for element in month_elements_text]))
                try:
                    drive_links[j].click()
                except StaleElementReferenceException:
                    pass
                
                # Download the file
                download_button = browser.find_element_by_css_selector('.ndfHFb-c4YZDc-nupQLb-Bz112c')
                download_button.click()
                # Back to the year
                browser.back()
        except StaleElementReferenceException:
            pass
        
        browser.get('https://www.colorado.gov/pacific/cdphe/medical-marijuana-statistics-and-data')

def arrange_files(where_to_put_the_files):
    if os.getcwd() != where_to_put_the_files:
        os.chdir(where_to_put_the_files)
    
    downloaded_files = os.scandir()
    years = ['2019','2018','2017', '2016','2015', 
             '2014','2013','2012','2011']
    # Make year directories
    for year in years:
        if year not in os.listdir():
            os.mkdir(year)
    
    for file in downloaded_files:
        # Check if each file is a pdf
        if ".pdf" not in file.name:
            continue
        # If the file has a year in it, move it to that folder
        for year in years:
            if year in file.name:
                shutil.move(file.path, os.getcwd()+'/'+year)
            

get_datafiles()
arrange_files('/Users/mbarrett/Documents/Projects/Drug Policy/MED Data')
    
    
    
        
