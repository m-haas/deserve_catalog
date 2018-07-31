# Routine to stitch together several catalogues from different sources
# Steps:
# 1) Open all csv files
# 2) Collect all events in one dataframe
# 3) Discard multiple entries (priority given)
# 4) Exclude fake events (EMEC list)(
# 5) Harmonize catalogue to Mw
# 6) Write out single catalogue.csv

import csv
import numpy as np
import openquake.hazardlib.geo as og

def init_cat():
    '''
    initialize the catalogue
    '''
    cat = {}

    keys=['eventID','year','month','day','hour','minute','second','longitude','latitude','magnitude','magnitudeType','depth','Agency','institution']
    for key in keys:
        cat[key]=[]

    return cat

def append_value(catalogue,key,value):
    #values assigned for no data entries (time, i.e. month,day,hour... and depth)
    no_time = '1'
    no_depth = '999'
    #deal with unspecified values
    if value=='':
        if key in ['month','day','hour','minute','second']:
            value = no_time
        elif key in ['depth']:
            value = no_depth
    catalogue[key].append(value)

def read_cat(filename,headerlines,delimiter,catalogue,count):
    '''
    reads in catalogue
    '''
    cat_name = filename[:-4]

    with open(filename,'r') as csvfile:
        reader = csv.reader(csvfile,delimiter=delimiter)
        #skip header
        for i in range(headerlines):
            next(reader,None)
        for row in reader:
        #Store data
            #remove whitespace
            row = [x.strip() for x in row]
            # data depending on catalogue
            if (cat_name=='AM06'):
            #AM06: Y M D N E MS
                cat['eventID'].append(count)
                cat['year'].append(row[0])
                #time values which may be empty
                append_value(cat,'month',row[1])
                append_value(cat,'day',row[2])
                append_value(cat,'hour','')
                append_value(cat,'minute','')
                append_value(cat,'second','')
                cat['longitude'].append(row[4])
                cat['latitude'].append(row[3])
                #depth value which may be empty
                append_value(cat,'depth','')
                cat['magnitude'].append(row[5])
                cat['magnitudeType'].append('Ms')
                cat['institution'].append('')
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='AM93'):
            #AM93: Id,Year,Month,Day,UTC,EpicenterN-E,h,Ms,Mb
                cat['eventID'].append(count)
                cat['year'].append(row[1])
                #time values which may be empty
                mapping={'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
                append_value(cat,'month',mapping[row[2]])
                append_value(cat,'day',row[3])
                append_value(cat,'hour',row[4][0:2])
                append_value(cat,'minute',row[4][2:4])
                append_value(cat,'second',row[4][4:6])
                cat['latitude'].append(row[5][0:5])
                cat['longitude'].append(row[5][6:11])
                #depth value which may be empty
                append_value(cat,'depth',row[6])
                if row[7]!='':
                    cat['magnitude'].append(row[7])
                    cat['magnitudeType'].append('Ms')
                else:
                    cat['magnitude'].append(row[8])
                    cat['magnitudeType'].append('Mb')
                cat['institution'].append('')
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='NCEDCA'):
            # ANSS: time,latitude,longitude,depth,mag,magnitudeType,nst,gap,dmin,rms,net,id,updated,place,type
                cat['eventID'].append(count)
                date_string = row[0]
                cat['year'].append(date_string[0:4])
                #time values which may be empty
                append_value(cat,'month',date_string[5:7])
                append_value(cat,'day',date_string[8:10])
                append_value(cat,'hour',date_string[11:13])
                append_value(cat,'minute',date_string[14:16])
                append_value(cat,'second',date_string[17:22])
                cat['longitude'].append(row[2])
                cat['latitude'].append(row[1])
                #depth value which may be empty
                append_value(cat,'depth',row[3])
                cat['magnitude'].append(row[4])
                cat['magnitudeType'].append(row[5])
                cat['institution'].append('')
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='ISC'):
            # EHB: EVENTID,AUTHOR ,DATE,TIME,LAT,LON,DEPTH,DEPFIX,AUTHOR,TYPE,MAG,AUTHOR,TYPE,MAG,AUTHOR,TYPE,MAG
                cat['eventID'].append(count)
                cat['year'].append(row[2][0:4])
                #time values which may be empty
                append_value(cat,'month',row[2][5:7])
                append_value(cat,'day',row[2][8:10])
                append_value(cat,'hour',row[3][0:2])
                append_value(cat,'minute',row[3][3:5])
                append_value(cat,'second',row[3][6:11])
                cat['longitude'].append(row[5])
                cat['latitude'].append(row[4])
                #depth value which may be empty
                append_value(cat,'depth',row[6])
                if row[16]!='':
                    cat['magnitude'].append(row[16])
                    cat['magnitudeType'].append('Mw')
                    cat['institution'].append(row[14])
                elif row[13]!='':
                    cat['magnitude'].append(row[13])
                    cat['magnitudeType'].append('Ms')
                    cat['institution'].append(row[11])
                else:
                    cat['magnitude'].append(row[10])
                    cat['magnitudeType'].append('Mb')
                    cat['institution'].append(row[8])
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='EMEC'):
            # EMEC: year;month;day;hour;minute;latitude;longitude;depth;intensity;M_orig;type;Mw ;Mw_err;reference
                cat['eventID'].append(count)
                cat['year'].append(row[0])
                #time values which may be empty
                append_value(cat,'month',row[1])
                append_value(cat,'day',row[2])
                append_value(cat,'hour',row[3])
                append_value(cat,'minute',row[4])
                append_value(cat,'second','')
                cat['longitude'].append(row[6])
                cat['latitude'].append(row[5])
                #depth value which may be empty
                append_value(cat,'depth',row[7])
                cat['magnitude'].append(row[11])
                cat['magnitudeType'].append('Mw')
                cat['institution'].append(row[13])
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='GII'):
            # GII: Date,Time(UTC),Millisecond,Md,Mb,Lat,Long,Depth(Km),Region,Type
            # 1903-03-29,00:00:00,00,5.6,5.6,32.0957,35.4954,10,E.Shomron,F
                cat['eventID'].append(count)
                cat['year'].append(row[0][0:4])
                #time values which may be empty
                append_value(cat,'month',row[0][5:7])
                append_value(cat,'day',row[0][8:10])
                append_value(cat,'hour',row[1][0:2])
                append_value(cat,'minute',row[1][3:5])
                append_value(cat,'second',row[1][6:8]+'.'+row[2])
                cat['longitude'].append(row[6])
                cat['latitude'].append(row[5])
                append_value(cat,'depth',row[7])
                if row[4]!='0.0':
                    cat['magnitude'].append(row[4])
                    cat['magnitudeType'].append('Mb')
                else:
                    cat['magnitude'].append(row[3])
                    cat['magnitudeType'].append('Md')
                cat['institution'].append('')
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='GSHAP_TR'):
                # GSHAP_TR: Cat,Year,Mo.,Da.,Hr.,Min.,Sec.,Latitude,Longitude,depth,mag_magtype,provider,mag_magtype,provider,Mw_source
                #ANK,11,,,,,,37.84,27.84,,6.40UK,ANK,,,6.70ANK
                cat['eventID'].append(count)
                cat['year'].append(row[1])
                #time values which may be empty
                append_value(cat,'month',row[2])
                append_value(cat,'day',row[3])
                append_value(cat,'hour',row[4])
                append_value(cat,'minute',row[5])
                append_value(cat,'second',row[6])
                cat['longitude'].append(row[8])
                cat['latitude'].append(row[7])
                #depth value which may be empty
                append_value(cat,'depth',row[9])
                cat['magnitude'].append(row[14][0:4])
                cat['magnitudeType'].append('Mw')
                cat['institution'].append(row[14][4:])
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='IRIS'):
            # IRIS: EventID | Time | Latitude | Longitude | Depth/km | Author | Catalog | Contributor | ContributorID | MagType | Magnitude | MagAuthor | EventLocationName
            # 57500|1965-01-25T12:18:34|34.56|32.84|20.0|ISC|ISC|ISC|1876422|mb|4.8|ISC|CYPRUS REGION
                cat['eventID'].append(count)
                date_string = row[1]
                cat['year'].append(date_string[0:4])
                cat['month'].append(date_string[5:7])
                cat['day'].append(date_string[8:10])
                cat['hour'].append(date_string[11:13])
                cat['minute'].append(date_string[14:16])
                cat['second'].append(date_string[17:19])
                cat['longitude'].append(row[3])
                cat['latitude'].append(row[2])
                #depth value which may be empty
                append_value(cat,'depth',row[4])
                cat['magnitude'].append(row[10])
                cat['magnitudeType'].append(row[9])
                cat['institution'].append(row[11])
                cat['Agency'].append(cat_name)
                count += 1
            #elif (cat_name=='IRIS_mw'):
            ## IRIS_mw: EventID,Time,Latitude,Longitude,Depth/km,Author,Catalog,Contributor,ContributorID,MagType,Magnitude,MagAuthor,EventLocationName
            #    cat['eventID'].append(count)
            #    date_string = row[1]
            #    cat['year'].append(date_string[0:4])
            #    cat['month'].append(date_string[5:7])
            #    cat['day'].append(date_string[8:10])
            #    cat['hour'].append(date_string[11:13])
            #    cat['minute'].append(date_string[14:16])
            #    cat['second'].append(date_string[17:19])
            #    cat['longitude'].append(row[3])
            #    cat['latitude'].append(row[2])
            #    #depth value which may be empty
            #    append_value(cat,'depth',row[4])
            #    cat['magnitude'].append(row[10])
            #    cat['magnitudeType'].append(row[9])
            #    cat['institution'].append(row[11])
            #    cat['Agency'].append(cat_name)
            #    count += 1
            elif (cat_name=='ISC-GEM'):
            # ISC: date,lat,lon,smajax,sminax,strike,q,depth,unc,q,mw,unc,q,s,mo,fac,mo_auth,mpp,mpr,mrr,mrt,mtp,mtt,eventid
                #bound to region
                if (float(row[1]) >= 24 and float(row[1]) <= 38 and float(row[2]) >= 29 and float(row[2]) <= 41):
                    cat['eventID'].append(count)
                    date_string = row[0]
                    cat['year'].append(date_string[0:4])
                    cat['month'].append(date_string[5:7])
                    cat['day'].append(date_string[8:10])
                    cat['hour'].append(date_string[11:13])
                    cat['minute'].append(date_string[14:16])
                    cat['second'].append(date_string[17:])
                    cat['longitude'].append(row[2])
                    cat['latitude'].append(row[1])
                    append_value(cat,'depth',row[7])
                    cat['magnitude'].append(row[10])
                    cat['magnitudeType'].append('Mw')
                    cat['institution'].append('')
                    cat['Agency'].append(cat_name)
                    count += 1
            elif (cat_name=='KHAIR'):
            # KHAIR: Year,Month,Day,Time,Ms,ML,Lat,Lon
            # 1157,8,15,10000,7.2,7.3,35.1,36.3
                cat['eventID'].append(count)
                cat['year'].append(row[0])
                #time values which may be empty
                append_value(cat,'month',row[1])
                append_value(cat,'day',row[2])
                append_value(cat,'hour',row[3][0:2])
                append_value(cat,'minute',row[3][2:4])
                append_value(cat,'second',row[3][4:])
                cat['longitude'].append(row[7])
                cat['latitude'].append(row[6])
                #depth value which may be empty
                append_value(cat,'depth','')
                if row[4]!='':
                    cat['magnitude'].append(row[4])
                    cat['magnitudeType'].append('Ms')
                elif row[5]!='':
                    cat['magnitude'].append(row[5])
                    cat['magnitudeType'].append('ML')
                else:
                    cat['magnitude'].append('')
                    cat['magnitudeType'].append('UK')
                cat['institution'].append('')
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='KOERI'):
            # KOERI: Number ,Event ID,Date,Origin Time, Latitude,Longitude, Depth(km), xM,MD,ML,Mw,Ms,Mb,Type,Location
            # 1,20141021120146,2014.10.21,12:01:46.38,39.9942,39.6635,4.5,3.9,0,3.9,0,0,0,Ke,DOGANKAVAK-KELKIT (GUMUSHANE) [East 2.4 km]
                cat['eventID'].append(count)
                cat['year'].append(row[2][0:4])
                cat['month'].append(row[2][5:7])
                cat['day'].append(row[2][8:10])
                cat['hour'].append(row[3][0:2])
                cat['minute'].append(row[3][3:5])
                cat['second'].append(row[3][6:])
                cat['longitude'].append(row[5])
                cat['latitude'].append(row[4])
                append_value(cat,'depth',row[6])
                #Magnitude
                if row[10]!= '0':
                    cat['magnitude'].append(row[10])
                    cat['magnitudeType'].append('Mw')
                elif row[11] != '0':
                    cat['magnitude'].append(row[11])
                    cat['magnitudeType'].append('Ms')
                elif row[9] != '0':
                    cat['magnitude'].append(row[9])
                    cat['magnitudeType'].append('ML')
                elif row[12] != '0':
                    cat['magnitude'].append(row[12])
                    cat['magnitudeType'].append('Mb')
                elif row[8] !=  '0':
                    cat['magnitude'].append(row[8])
                    cat['magnitudeType'].append('MD')
                else:
                    cat['magnitude'].append('')
                    cat['magnitudeType'].append('uk')
                cat['institution'].append('')
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='NCEDCB'):
            # NCEDC: DateTime,Latitude,Longitude,Depth,Magnitude,MagType,NbStations,Gap,Distance,RMS,Source,EventID
            # 1963/05/23 10:15:08.90,36.6000,30.0000,226.00,5.60,Mb,8,,,0.00,NEI,196305234013
                cat['eventID'].append(count)
                date_string = row[0]
                cat['year'].append(date_string[0:4])
                cat['month'].append(date_string[5:7])
                cat['day'].append(date_string[8:10])
                cat['hour'].append(date_string[11:13])
                cat['minute'].append(date_string[14:16])
                cat['second'].append(date_string[17:22])
                cat['longitude'].append(row[2])
                cat['latitude'].append(row[1])
                append_value(cat,'depth',row[3])
                cat['magnitude'].append(row[4])
                cat['magnitudeType'].append(row[5])
                cat['institution'].append(row[10])
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='SBEI'):
            # SBEI: Year,Month,Day,Lat,Lon,H,MS
            # 502,8,22,33.0,34.8,30,7.2
                cat['eventID'].append(count)
                cat['year'].append(row[0])
                #time values which may be empty
                append_value(cat,'month',row[1])
                append_value(cat,'day',row[2])
                append_value(cat,'hour','')
                append_value(cat,'minute','')
                append_value(cat,'second','')
                cat['longitude'].append(row[4])
                cat['latitude'].append(row[3])
                #depth value which may be empty
                append_value(cat,'depth',row[5])
                cat['magnitude'].append(row[6])
                cat['magnitudeType'].append('Ms')
                cat['institution'].append('')
                cat['Agency'].append(cat_name)
                count += 1
            elif (cat_name=='SHARE'):
            # En,Year,Mo,Da,Ho,Mi,Se,Ax,Lat,Lon,LatUnc,LonUnc,H,Io,Mw,MwUnc,TMw
            # 1919488.10,2006,12,31,18,57,22.00,,34.320,32.310,10.0,10.0,48.00,,4.10,0.20,co
                cat['eventID'].append(count)
                cat['year'].append(row[1])
                #time values which may be empty
                append_value(cat,'month',row[2])
                append_value(cat,'day',row[3])
                append_value(cat,'hour',row[4])
                append_value(cat,'minute',row[5])
                append_value(cat,'second',row[6])
                cat['longitude'].append(row[9])
                cat['latitude'].append(row[8])
                #depth value which may be empty
                append_value(cat,'depth',row[12])
                cat['magnitude'].append(row[14])
                cat['magnitudeType'].append('Mw')
                cat['institution'].append('')
                cat['Agency'].append(cat_name)
                count += 1
            else:
                print('error')

        return cat,count

def convert_strings(cat):
    '''
    Converts the stored string arrays in the catalogue dictionary to numbers (where meaningful)
    '''
    integers = ['eventID','year','month','day','hour','minute']
    floats = ['second','longitude','latitude','depth']
    strings = ['magnitudeType','Agency','institution']

    for key in cat:
        if key == 'magnitude':
            cat[key] = [float(x) if x!='' else 0. for x in cat[key]]
        elif key in integers:
            cat[key] = [int(x) for x in cat[key]]
        elif key in floats:
            cat[key] = [float(x) for x in cat[key]]
        elif key in strings:
            cat[key] = [x.upper() for x in cat[key]]
        else:
            raise KeyError('Key {} is not recognized and thus cannot be converted'.format(key))

    return cat

def cat_stats(cat,cats):
    '''
    calculates statistics of the catalogues the catalogue is composed of
    '''
    hierarchy = [
    'EMEC',
    'SBEI',
    'AM06',
    'KHAIR',
    'AM93',
    'GII',
    'NCEDCB',
    'ISC-GEM',
    'IRIS',
    'ISC',
    'KOERI',
    'NCEDCA',
    'SHARE',
    'GSHAP_TR']

    stat = {'ID':[],
            'name':[],
            'events':[],
            'minYear':[],
            'maxYear':[],
            'minLat':[],
            'maxLat':[],
            'minLon':[],
            'maxLon':[],
            'minMag':[],
            'maxMag':[],
            'magTypes':[],
            'missingDepths':[],
            'missingMagnitudes':[]
            }
    for i in cats:
        cat_name = files[i][0][:-4].upper()
        # find indices corresponding to catalogue
        idxs = [i for i, elem in enumerate(cat['Agency']) if elem == cat_name]
        # statistics
        stat['ID'].append(hierarchy.index(cat_name))
        stat['name'].append(cat_name)
        stat['events'].append(len(idxs))
        stat['minYear'].append(min([cat['year'][i] for i in idxs]))
        stat['maxYear'].append(max([cat['year'][i] for i in idxs]))
        stat['minLat'].append(min([cat['latitude'][i] for i in idxs]))
        stat['maxLat'].append(max([cat['latitude'][i] for i in idxs]))
        stat['minLon'].append(min([cat['longitude'][i] for i in idxs]))
        stat['maxLon'].append(max([cat['longitude'][i] for i in idxs]))
        mags = [cat['magnitude'][i] for i in idxs]
        stat['minMag'].append(min([mag for mag in mags if mag > 0]))
        stat['maxMag'].append(max([cat['magnitude'][i] for i in idxs]))
        # magnitude types in catalogue
        types = set([cat['magnitudeType'][i].upper() for i in idxs])
        tmp = ''
        for t in types:
            tmp+=t+','
        stat['magTypes'].append(tmp[:-1])
        # count missing depths
        depths = [cat['depth'][i] for i in idxs]
        tmp = [d for d in depths if d==999]
        stat['missingDepths'].append(len(tmp))
        # count missing magnitudes
        tmp = [m for m in mags if m==0]
        stat['missingMagnitudes'].append(len(tmp))


    print('Missing depth: ',sum(stat['missingDepths']))
    #print('Magnitude types: ',set([t[i] for t in [t.split(',') for t in set(stat['magTypes'])] for i in range(len(t))]))

    return stat

def write_csv(data,filename):
    '''
    Write the dictionary harmonized catalogues to a single csv
    '''

    with open(filename, 'w') as f:
        fieldnames = list(data.keys())
        writer = csv.DictWriter(f,fieldnames=fieldnames)
        writer.writeheader()
        for i in range(len(data[fieldnames[0]])):
            row = {}
            for key in data:
                row[key] = data[key][i]
            writer.writerow(row)

#HMTK functions to convert ymdhms into a decimal number
def leap_check(year):
    """
    Returns logical array indicating if year is a leap year
    """
    return np.logical_and((year % 4) == 0,
                          np.logical_or((year % 100 != 0), (year % 400) == 0))


def decimal_time(year, month, day, hour, minute, second):
    """
    Returns the full time as a decimal value
    :param year:
        Year of events (integer numpy.ndarray)
    :param month:
        Month of events (integer numpy.ndarray)
    :param day:
        Days of event (integer numpy.ndarray)
    :param hour:
        Hour of event (integer numpy.ndarray)
    :param minute:
        Minute of event (integer numpy.ndarray)
    :param second:
        Second of event (float numpy.ndarray)
    :returns decimal_time:
        Decimal representation of the time (as numpy.ndarray)
    """
    MARKER_NORMAL = np.array([0, 31, 59, 90, 120, 151, 181,
                          212, 243, 273, 304, 334])

    MARKER_LEAP = np.array([0, 31, 60, 91, 121, 152, 182,
                        213, 244, 274, 305, 335])

    SECONDS_PER_DAY = 86400.0

    tmonth = month - 1
    day_count = MARKER_NORMAL[tmonth] + day - 1
    id_leap = leap_check(year)
    leap_loc = np.where(id_leap)[0]
    day_count[leap_loc] = MARKER_LEAP[tmonth[leap_loc]] + day[leap_loc] - 1
    year_secs = (day_count.astype(float) * SECONDS_PER_DAY) + second + \
        (60. * minute.astype(float)) + (3600. * hour.astype(float))
    dtime = year.astype(float) + (year_secs / (365. * 24. * 3600.))
    dtime[leap_loc] = year[leap_loc].astype(float) + \
        (year_secs[leap_loc] / (366. * 24. * 3600.))
    return dtime

def duplicate_indices(cat,idx,tt,dt):
    '''
    returns duplicates of an event (stemming from different catalogues) according to
    a given hierarchy
    input: cat = catalogue
           idx = start index of the duplicate series (index of second entry with same dtime)
           tt = time threshold
           dt = distance threshold in deg
    output: list of idices to be removed from the catalogue later on
    '''
    hierarchy = [
    'EMEC',
    'SBEI',
    'AM06',
    'KHAIR',
    'AM93',
    'GII',
    'NCEDCB',
    'ISC-GEM',
    'IRIS',
    'ISC',
    'KOERI',
    'NCEDCA',
    'SHARE',
    'GSHAP_TR']

    #store indices of duplicates
    sec_in_a_year = 365*24*3600
    duplicate_idxs = [idx-1]
    duplicate=True
    while duplicate and idx <= len(cat['dtime']):
        #check if the thresholds for being seperate events are exceeded
        cond1 = (cat['dtime'][idx] - cat['dtime'][idx-1]) > (tt/sec_in_a_year)
        dist = og.Point(cat['longitude'][idx],cat['latitude'][idx]).distance(og.Point(cat['longitude'][idx-1],cat['latitude'][idx-1]))
        thresh = og.Point(0,cat['latitude'][idx]).distance(og.Point(0,cat['latitude'][idx]+dt))
        cond2 = dist > thresh
        if cond1 or cond2:
            duplicate = False
        else:
            duplicate_idxs.append(idx)
            idx+=1
            #prefer with depth
    complete = []
    for i in duplicate_idxs:
        if cat['depth'][i] != 999 and cat['depth'][i] != 0:
            complete.append(i)
    #if none of the duplicates have a depth entry
    if complete == []:
        complete = duplicate_idxs

    #store only the event entry of the highest rank catalogue available
    # find the catalogue idx of the event entry with the highest rank (smallest value, i.e. earliest in the hierarchy list)
    best = 99
    for i in complete:
        rank = hierarchy.index(cat['Agency'][i])
        if rank < best:
            best = rank
            idx_keep = i

    #remove the best index from the dublicate list
    duplicate_idxs.remove(idx_keep)

    #return list of to be cleaned entries from the catalogue and last checked idx
    return idx,duplicate_idxs

def harmonize_cat(cat,idx):
    '''
    Harmonize the catalogue to MW
    '''
    magnitudeType = cat['magnitudeType'][idx][0:2]
    mag = cat['magnitude'][idx]


    if magnitudeType in ['MW','M','MWB','MWC','MWR','MWW']:
        pass
    elif magnitudeType=='MS':
        '''
        Surface wave magnitude conversion according to Scordilis2006
        '''
        if mag >= 3.0 and mag <= 6.1:
            mag = 0.67 * mag + 2.07
        elif mag >= 6.2 and mag <= 8.2:
            mag = 0.99 * mag + 0.08
        else:
            del_event(cat,idx)
            #raise ValueError('Ms {} of event {} is out of defined bounds'.format(mag,idx))
    elif magnitudeType=='MB':
        '''
        Body wave magnitude conversion according to Johnston1996
        '''
        if mag >= 4.0 and mag <= 6.5 :
            logM0 = 18.23 + 0.679 * mag + 0.077 * mag**2
            mag = logM0*2/3 - 10.7
        else:
            del_event(cat,idx)
            #raise ValueError('Mb {} of event {} is out of defined bounds'.format(mag,idx))
    elif magnitudeType in ['MD','MC']:
        '''
        Body wave magnitude conversion according to Cagnan2012
        '''
        if mag >= 3.5 and mag <= 6.5 :
            mag = 0.986 * mag + 0.365
        else:
            del_event(cat,idx)
            #raise ValueError('Md {} of event {} is out of defined bounds'.format(mag,idx))
    elif magnitudeType=='ML':
        '''
        Local magnitude conversion according to Akkar2010
        '''
        if mag > 3.9 and mag < 6.8:
            mag =  0.953 * mag + 0.422
        else:
            del_event(cat,idx)
            #raise ValueError('Ml {} of event {} is out of defined bounds'.format(mag,idx))
    else:
        raise TypeError('No conversion for magnitude type {} of event {} defined'.format(magnitudeType,idx))

    #round to 1 digit
    try:
        cat['magnitude'][idx] = round(mag*10)/10
    except:
        print(idx)

    return cat

def del_event(cat,ID):
    '''
    Removes event with id ID
    '''
    for key in cat.keys():
        del cat[key][ID]

    return cat


######################################################
# MAIN PROGRAM
######################################################

# filename, headerlines, delimiter
files = [['AM06.csv',1,','],
         ['AM93.csv',1,','],
         ['NCEDCA.csv',1,','],
         ['ISC.csv',1,','],
         ['EMEC.csv',14,';'],
         ['GII.csv',1,','],
         ['GSHAP_TR.csv',1,','],
         ['IRIS.csv',1,','],
         ['ISC-GEM.csv',57,','],
         ['KHAIR.csv',1,','],
         ['KOERI.csv',1,','],
         ['NCEDCB.csv',1,','],
         ['SBEI.csv',1,','],
         ['SHARE.csv',1,',']]
########################################
#Combine all cataloges defined in files#
########################################
count=0
cat = init_cat()

#read in data
#comment out the line below if you want to process only the catalogue at files[only]
#only = [0,1,2,3,4,5,6,7,8,9,10,11,12,13]
if 'only' in locals():
    cats = only
else:
    cats = list(range(len(files)))

for i in cats:
    cat,count = read_cat(files[i][0],files[i][1],files[i][2],cat,count)

#convert strings to numbers
cat = convert_strings(cat)

#############################################################################
# Limit to lat 24.55-37.80 and lon 29.95-40.80                              #
# and remove events with missing magnitude or magnitude type or having z=0  #
#############################################################################

for i in sorted(range(len(cat['year'])),reverse=True):
    if (cat['magnitude'][i] == 0 or cat['magnitudeType'][i] in ['ME','','UK','UNK','MG'] or
            cat['latitude'][i] < 24.55 or cat['latitude'][i] > 37.8 or
            cat['longitude'][i] < 29.95 or cat['longitude'][i] > 40.8 or
            cat['depth'][i] == 0):
        cat = del_event(cat,i)

#############################################################################
# Identify and remove duplicates according to priorities of the catalogues  #
#############################################################################
#add dtime for each event to catalogue and sort ascending
cat['dtime'] = []
#for i in range(len(cat['magnitude'])):
#    cat['dtime'].append(decimal_time(cat['year'][i], cat['month'][i], cat['day'][i], cat['hour'][i], cat['minute'][i], cat['second'][i]))
cat['dtime'] = decimal_time(np.asarray(cat['year']), np.asarray(cat['month']), np.asarray(cat['day']), np.asarray(cat['hour']), np.asarray(cat['minute']), np.asarray(cat['second']))

#sort catalogue w.r.t. dtime
tmp = [x for x in cat.keys()]
tmp.remove('dtime')
for key in tmp:
    out_dtime,cat[key]=(list(t) for t in zip(*sorted(zip(cat['dtime'],cat[key]))))

#once all others are sorted store dtime sorted
cat['dtime']=sorted(cat['dtime'])

#max() cannot check empty lists
to_be_removed=[]
last_checked=0
for i in range(1,len(cat['dtime'])):
    #avoid checking indices twice
    if i < last_checked:
        pass
    else:
	#timethresholds(tt) are in seconds
	#distancether
        #ancient < 1000
        if cat['dtime'][i] < 1000:
            tt = 24*3600
            dt = 0.5
        #prior instrumental < 1900
        elif cat['dtime'][i] < 1900:
            tt = 12*3600
            dt = 0.3
        #before 1960
        elif cat['dtime'][i] < 1960:
            tt = 3600
            dt = 0.2
        #post 1960
        else:
            tt = 60
            dt = 0.1
        #identify start of a duplicate series
        if (cat['dtime'][i] - cat['dtime'][i-1] < tt):
            last_checked,dupl = duplicate_indices(cat,i,tt,dt)
            for elem in dupl:
            #for elem in duplicate_indices(cat,i,tt,dt):
                to_be_removed.append(elem)

print('Length including duplicates: ',len(cat['dtime']))

#clean up catalogue from the duplicates, backwards!! otherwise indices change
for i in sorted(to_be_removed,reverse=True):
    cat = del_event(cat,i)

print('Length excluding duplicates: ',len(cat['dtime']))


############################################
# Remove fake events                       #
############################################
#528: duplicate (identified manually)
#others: from EMEC documentation

year = [528,1111,1053,1115,1139,1152]
month = [1,1,1,1,10,9]

#remove all that match the above values
for i in sorted(range(len(cat['dtime'])),reverse=True):
    if (cat['year'][i],cat['month'][i]) in zip(year,month):
        cat = del_event(cat,i)

print('Length excluding fakes: ',len(cat['dtime']))

############################################
# Harmonize catalogue to Mw                #
############################################

#idxs = [i for i, elem in enumerate(cat['magnitudeType']) if elem not in ['MW','M','MWB','MWC','MWR','MWW']]
write_csv(cat,'test.csv')
for i in sorted(range(len(cat['dtime'])),reverse=True):
    cat = harmonize_cat(cat,i)

print('Final length after harmonizing: ',len(cat['dtime']))

############################################
# OPTIONAL: Remove unknown depth           #
############################################
#idxs = [i for i, elem in enumerate(cat['depth']) if elem == 999]:
#
#for i in sorted(idxs,reverse=True):
#    cat = del_event(cat,i)


############################################
# Write catalogue and stats                #
############################################

#write catalogue to file catalogue.csv
write_csv(cat,'catalogue.csv')

#calculate and print statistics to stats_catalogue.csv
stat = cat_stats(cat,cats)
write_csv(stat,'stats_catalogue.csv')

#idxs = [i for i, elem in enumerate(cat['magnitudeType']) if elem == 'MD']
#print('min',min([cat['magnitude'][i] for i in idxs]))
#print('max',max([cat['magnitude'][i] for i in idxs]))
#print(sorted([cat['magnitude'][i] for i in idxs]).index(3.5))
#print(sorted([cat['magnitude'][i] for i in idxs],reverse=True).index(6.8))

