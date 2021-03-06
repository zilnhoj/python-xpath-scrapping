import requests #library to get the page we want to scrape
from lxml import html #Libary that uses xpath and allows us to get the data we need from the returned page
import pandas as pd #Library to create dataframes of the data we colled and output the data we need
from google2pandas import * #Library that allows us to build and use the 

# declare a variable for the URL of the page we are scrapping
url = 'https://www.gov.uk/highway-code'

# url = 'http://webarchive.nationalarchives.gov.uk/20140813095748/https://www.gov.uk/highway-code'

#Get the all the HTML content of the page we are scrappng 
pge = requests.get(url)

# Convert the content to a string Object 
tree = html.fromstring(pge.text)
# declare a variable tha contains all the data we want from the page using an xpath expression
listofslugs = tree.xpath('//*[@id="content"]/div[2]/ol/li[.]/a/@href')
# listofd = tree.xpath('//div[@id="wrapper"]/main[@id="content"]/div[@class="article-container group"]/article[@class="group"]/div[@class="inner"]/p[.]/a[.]/@href')
# set of empty dataframes we will append to later
df1 = pd.DataFrame()
df2 = pd.DataFrame()
df3 = pd.DataFrame()
df4 = pd.DataFrame()

ids = '53872948'
dimensions = ['pagePath']
metrics = ['pageviews', 'uniquePageviews', 'entrances', 'bounceRate', 'exitRate']
start_date = '2015-12-01'
end_date = '2015-12-31'

max_results = 5e5
chunksize = 5000
count = 0

for i in listofslugs:
	gg = str(i)
	filters='pagePath=~' + gg
	# try:
	hwy = {\
		    'ids'           : ids,
		    'metrics'       : metrics,
		    'dimensions'    : dimensions,
		    'start_date'    : start_date,
		    'end_date'		: end_date,
		    'filters'		: filters,
		    'max_results'   : 10}

	ga = GoogleAnalyticsQuery(token_file_name='analytics.dat')
	dfhwy, formatted_qry = ga.execute_query(**hwy)
	
	# p1 = pd.concat([x for x in dfhwy])
	df1 = df1.append(dfhwy) #add each result to the dataframe
	count+=1
	print 'general metrics: {}'.format(str(count)) #count to show on screen the progress of the script	

# This section of code adds a total, cumulaitive 

df1 = df1.reset_index(level=1)
df1 = df1.sort_values(by='pageviews', ascending=False)
df1['total'] = df1['pageviews'].sum()
df1.drop_duplicates(inplace=True)
df1['cumulative'] = df1['pageviews'].cumsum()
df1['percentage'] = df1['cumulative']/df1['total']*100.0


df1.to_csv('generalmetrics.csv', encoding='utf8')
# ============================================================================================================================================
#  next page
# ============================================================================================================================================


nextdimensions = ['pagePath', 'previousPagePath']
nextmetrics = ['pageviews']
nextcount = 0
for k in listofslugs:
	filters='previousPagePath=~{}'.format(str(k))
	# try:
	hwynext = {\
		    'ids'           : ids,
		    'metrics'       : nextmetrics,
		    'dimensions'    : nextdimensions,
		    'start_date'    : start_date,
		    'end_date'		: end_date,
		    'filters'		: filters,
		    'max_results'   : 10}

	
	hwynext, formatted_qry = ga.execute_query(**hwynext)

	df3 = df3.append(hwynext)
	nextcount+=1
	print 'next page count: {}'.format(str(nextcount))

		
df3 = df3.reset_index(level=1)
df3.drop_duplicates(inplace=True)
df3 = df3[['pagePath', 'previousPagePath', 'pageviews']]
df3.sort_values(by=['pageviews'], ascending=False, inplace=True)

df3.to_csv('nextpage.csv', encoding='utf8')
# ============================================================================================================================================
#  referring sites
# ============================================================================================================================================


refdimensions = ['landingPagePath','fullReferrer']
refmetrics = ['sessions','percentNewSessions', 'newUsers']
refcount = 0
# try:
for j in listofslugs:
	
	filters='landingPagePath=~{}'.format(str(j)),
	try:
		hwyref = {\
			    'ids'           : ids,
			    'metrics'       : refmetrics,
			    'dimensions'    : refdimensions,
			    'start_date'    : start_date,
			    'end_date'		: end_date,
			    'filters'		: filters,
			    'max_results'   : 10}

		ga = GoogleAnalyticsQuery(token_file_name='analytics.dat')
		dfref, formatted_qry = ga.execute_query(**hwyref)
	# ref = pd.concat([x for x in dfref])
		df2 = df2.append(dfref)
		refcount+=1
		print 'referring count: {}'.format(str(refcount))
	except KeyError:
		dfref = pd.DataFrame({'landingPagePath':str(j),'fullReferrer':'none', 'sessions':0,'percentNewSessions':0,'newUsers':0},index=[0])
		df3 = df2.append(dfref)

df2 = df2.reset_index(level=1)
df2.drop_duplicates(inplace=True)
df2 = df2.drop('index', axis=1)
df2 = df2[['fullReferrer', 'landingPagePath', 'sessions']]
df2 = df2.sort_values(by='sessions', ascending=False)

df2.to_csv('landingpages.csv', encoding='utf8')
# ============================================================================================================================================
#  Searches on page
# ============================================================================================================================================


searchdimensions = ['searchStartPage', 'searchKeyword']
searchmetrics = ['searchSessions','searchExits','searchUniques']
searchcount = 0

for l in listofslugs:
	try:
		filters=['searchStartPage=~{}'.format(str(l))]
		# try:
		hwysearch = {\
			    'ids'           : ids,
			    'metrics'       : searchmetrics,
			    'dimensions'    : searchdimensions,
			    'start_date'    : start_date,
			    'end_date'		: end_date,
			    'filters'		: filters,
			    'max_results'   : 10}

		ga = GoogleAnalyticsQuery(token_file_name='analytics.dat')
		dfsearch, formatted_qry = ga.execute_query(**hwyref)

		# searchpage = pd.concat([x for x in dfsearch])
		df4 = df4.append(dfsearch)
		searchcount+=1
	except KeyError:
		nores = pd.DataFrame({'searchStartPage':str(j),'searchKeyword':'none', 'searchSessions':0,'searchExits':0,'searchUniques':0},index=[0])
		df4 = df4.append(nores)
print 'search page count: {}'.format(str(searchcount))
# except Exception:
	# 	df4 = df4.append(searchpage)
		
df4 = df4.reset_index(level=1)
# df4['term'],df4['searchStartPage'] = zip(*df4['index'])
df4.drop_duplicates(inplace=True)
df4 = df4.drop('index', axis=1)
df4 = df4[['searchKeyword','searchStartPage','searchUniques']]

df4.to_csv('serchesonpage.csv',encoding='utf8')
