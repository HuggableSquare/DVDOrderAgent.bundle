from lxml.etree import *
from copy import deepcopy
import re

def handleDVDorder(showtree):
	# creates new ElementTree root, with DVD-Ordered information, including merging multi-part episode info
	
	# Create Tree
	dvdtree = Element('Data')
	comment = Comment('Data based on DVD Order')
	dvdtree.append(comment)	
	EpsAdded = []
	
	# Add Series data
	dvdtree.append(deepcopy(showtree.find('Series')))
	
	# Cycle through seasons/episodes in showtree, and 
	for seasonnum in getList(showtree,'Episode/DVD_season'):
		EpList = sorted(set([ int(thing[:-2]) for thing in getList(showtree,'Episode/DVD_episodenumber')]))
		for episodenum in EpList:
			SegmentList = []
			for item in showtree.findall('Episode'):
				if item.find('DVD_season').text == str(seasonnum):
					segmentnum = item.find('DVD_episodenumber').text
					if segmentnum == None: segmentnum = ''
					if segmentnum[:-2] == str(episodenum):
						Segment = {'name':item.find('EpisodeName').text, 'overview':item.find('Overview').text, 'EpNode':item }
						if Segment['name'] != None: SegmentList.append(Segment)
			if SegmentList != []:
				# Set basic data
				EpsAdded.append((str(seasonnum),str(episodenum)))
				episode = SubElement(dvdtree,'Episode')
				EpisodeName = SubElement(episode,'EpisodeName')
				EpisodeName.text = CreateEpName(SegmentList)
				SeasonNumber = SubElement(episode,'SeasonNumber')
				SeasonNumber.text = str(seasonnum)
				EpisodeNumber = SubElement(episode,'EpisodeNumber')
				EpisodeNumber.text = str(episodenum)
				Overview = SubElement(episode, 'Overview')
				Overview.text = CreateEpOverview(SegmentList)
				
				# Flesh out the data
				
				# Things we want a single entry for 	
				for tag in ['FirstAired', 'Language', 'filename']:  # to do: see if we can submit more than one image
					item = SubElement(episode, tag)                 # also probably hand image filename differently
					item.text = RetrieveSingleEntry(SegmentList, tag)
				
				# Things we want a piped list for
				for tag in ['Director', 'Writer', 'GuestStars']:
					item = SubElement(episode, tag)
					item.text = CompileDataList(SegmentList, tag)
				
				# Things we want an average value for
				for tag in ['Rating']:
					item = SubElement(episode, tag)
					item.text = GetAverageValue(SegmentList, tag)
					
				# Boolean Flag, set to 1 if any is 1
				for tag in ['EpImgFlag']:
					item = SubElement(episode, tag)
					item.text = GetBooleanFlag(SegmentList, tag)
				if str(seasonnum) == '0':
					for tag in ['airsafter_season','airsbefore_episode','airsbefore_season']:  
						item = SubElement(episode, tag)
						item.text = RetrieveSingleEntry(SegmentList, tag)	
	
	# Add stuff that doesn't have a dvd order
	Log('Eps Added')
	Log(EpsAdded)
	for ep in showtree.findall('Episode'):
		if ep.find('DVD_season').text == None:
			try:
				SeasonEpisodeTuple = (ep.find('SeasonNumber').text,ep.find('EpisodeNumber').text)
				if (SeasonEpisodeTuple not in EpsAdded) and (SeasonEpisodeTuple[1] != 0 ):
					dvdtree.append(ep)
					Log('Adding aired order: ')
					Log( SeasonEpisodeTuple)
			except:
				pass		
	return dvdtree



def testing():
	tvdbid = '72879'     # Animaniacs 
	tvdbid = '75545'     # Invader Zim 
	tvdbid = '73871'     # Futurama 
	tvdbid = '71173'     # Battlestar Galactica 1978 
	tvdbid = '79171'     # Space Ghost Coast to Coast
	showtree = ElementTree()
	showtree.parse('/Users/Shared/tvdbfiles/'+tvdbid+'/en.xml')
	x = tostring(handleDVDorder(showtree))
	#print x

def commonstring(string1, string2):
	common = ''
	for i in range(len(string1)):
		if string1[i] != string2[i]:
			break
	return string1[:i].rstrip(' (')

def getList(showtree,tag):
	bleck = showtree.findall(tag)
	miff = sorted(set([erg.text for erg in bleck]))
	try: miff.remove(None)
	except:pass
	return miff
	
def CreateEpName(SegmentList):
	data = ' / '.join([  seg['name']   for seg in SegmentList	])
	if len(SegmentList) > 1:
		try:
		 	merph = sorted(set([  seg['name'].split(' (')[0]   for seg in SegmentList	]))
			if len(merph) == 1:
				data = merph[0]
		except:
			pass
	return data
	
def CreateEpOverview(SegmentList):
	if len(SegmentList) == 1:
		data = SegmentList[0]['overview']
	else:		
		for seg in SegmentList:
			if seg['overview'] == None: seg['overview'] = ''
		data = '\n'.join([  seg['name'] + ':\n' + seg['overview']  for seg in SegmentList	])
	return data
	
def CompileDataList(SegmentList, tag):	
	try:
		listy = [ seg['EpNode'].find(tag).text for seg in SegmentList ]
		listylist = [item.split('|') for item in listy]
		flatlist = sorted(set([item for sublist in listylist for item in sublist]))
		for garbage in ['', 'None']:
			if garbage in flatlist: flatlist.remove(garbage)
		data = '|'+'|'.join(flatlist)+'|'
		if data == '||': data = ''
	except:
		data = ''
	return data

def RetrieveSingleEntry(SegmentList, tag):
	try:
		entries = sorted(set([  seg['EpNode'].find(tag).text  for seg in SegmentList  ]))
		try: entries.remove(None)
		except:pass
		data = entries[0]
	except:
		data = ''
	return data
	
def GetBooleanFlag(SegmentList, tag):
	bools = sorted(set([  seg['EpNode'].find(tag).text  for seg in SegmentList  ]))
	if '1' in bools:
		data = '1'
	else:
		data = '0'
	return data
	
def GetAverageValue(SegmentList, tag):
	try:
		ValueList = [ float(seg['EpNode'].find(tag).text) for seg in SegmentList ]
		data = sum(values, 0.0) / len(values)
	except:
		data = '0'
	return data