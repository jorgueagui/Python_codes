# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 17:21:03 2017

@author: guerraaj
"""

# -*- coding: utf-8 -*-
"""
Created on Mon May 22 16:23:10 2017

@author: guerraaj
"""

import requests
import datetime
import numpy as np

def download_range(service_url, dataset, start, end, step=datetime.timedelta(days=30), **params):
    """
    service_url:    URL to get to the service. This is all the part before '/ui', e.g.
                    'http://cluster-r730-1:8002'
                    'http://api.flarecast.eu/property'
                    'http://localhost:8002'
                    Type: string
    dataset:        The dataset to download from
                    Type: string
    start, end:     Total start and end time of the data to download
                    Type: datetime
    step:           Time range of a single download slice
                    The total range (start - end) will be splitted up in smaller time ranges
                    with the size of 'step' and then every time range will be downloaded separately
                    Type: timedelta
    params:         Keyword argument, will be passed as query parameters to the http request url:
                    Examples:
                    property_type="sfunction_blos,sfunction_br"
                    nar=3120

    returns:        List with all entries, like you would download the whole time range in one request
                    Type: List of dicts
    """
    all_data = []

    while start < end:
        response = None
        end_step = min(start + step, end)
        try:
            params["time_start"] = "between(%s,%s)" % (
                start.isoformat(),
                end_step.isoformat()
            ),
            response = requests.get(
                "%s/region/%s/list" % (service_url, dataset),
                params=params
            )
        except requests.exceptions.BaseHTTPError as ex:
            print("exception while downloading: " % ex)

        if response is not None and response.status_code == 200:
            all_data.extend(response.json()["data"])
        else:
            resp_msg = response.json() if response is not None else ""
            print("error while downloading time range (%s - %s): %s" % (
                start, start + step, resp_msg
            ))
        start += step

    return all_data
    
# FUNCTION TO TRANSFORM LOCATION FORMAT
def location(loc):

    loc1 = []
    if loc != ' ':
        slat1 = loc[0:1]
        slon1 = loc[3:4]
        if slat1 == 'N':
            slat = 1
        else:
            slat = -1
        if slon1 == 'E':
            slon = -1
        else:
            slon = 1
        lat = int(float(loc[1:3]))
        lon = int(float(loc[4:6]))
        loc1.append(slat)
        loc1.append(slon)
        loc1.append(lat)
        loc1.append(lon)
        return loc1

# FUNCTION TO MATCH REGIONS
def comp_location(hc_loc,fc_lon,fc_lat,tol):
    # FIRST CONVERT HELCATS LOCATION FORMAT
    region_match = False
    #if len(hc_loc) == 1:
    hg_coor = location(hc_loc)
    #else:
    #hg_coor = hc_loc
    if fc_lon < 0:
        sfc_lon = -1
    else:
        sfc_lon = 1
    if fc_lat < 0:
        sfc_lat = -1
    else:
        sfc_lat = 1
    
    if (sfc_lon == hg_coor[1] and sfc_lat == hg_coor[0]):
        fc_d = np.sqrt(fc_lon*fc_lon + fc_lat*fc_lat)
        hc_d = np.sqrt(hg_coor[2]*hg_coor[2] + hg_coor[3]*hg_coor[3])
        diff_ = np.abs(fc_d - hc_d)       
        
        if diff_ < tol:    # TOL DEGREE IS THE TOLERANCE TO MATCH REGIONS
            region_match = True
    
    return region_match

# FUNCTION TO ROTATE REGIONS LOCATION
def rot_regions(nloc,ntime,srstime):
    loc = location(nloc)
    ar_lat = loc[0]*loc[2]
    ar_lon = loc[1]*loc[3]
    a=14.713
    b=-2.396
    c=-1.787
    minn = ntime - srstime
    if ntime > srstime:
        m = ntime - srstime
        minn = int(m.total_seconds()/60.)
    if ntime < srstime:
        m = srstime - ntime
        minn = -int(m.total_seconds()/60.)
    #
    rotation=a + b*(np.sin(ar_lat))**2.0 + c*(np.sin(ar_lat))**4.0 # In deg/day
    rotation=rotation/1440.0 # In deg/min
    ar_lon=ar_lon + minn*rotation # In degree
    if ar_lon > 0.:
        s1 = 'W'
    if ar_lon < 0.:
        s1 = 'E'
    if ar_lat < 0.:
        s2 = 'S'
    if ar_lat > 0.:
        s2 = 'N'
    nloc_lat = s2+"%02d" % loc[2]
    nloc_lon = s1+"%02d" % abs(ar_lon)
    new_loc = nloc_lat+nloc_lon
    
    return new_loc

if __name__ == "__main__":
    import iso8601
    import json
    import io
    import dateutil
    
    # SHARP DATA ONLY EXISTS SINCE SEPT 2012
    sharp_date = datetime.datetime(2012,9,1)
    # HELCATS/LOWCAT CATALOGUE FILENAME
    json_data=open("helcats_list.json").read()
    helcats_list = json.loads(json_data)
    # FLARECAST ACTIVE REGION PROPERTY -
    ps = "*" #ALL OR SELECT FROM LIST BELOW
    
    """
    LIST OF FLARECAST AR PROPERTY NAMES
    alpha_exp_cwt_blos, alpha_exp_cwt_br, alpha_exp_cwt_btot,    #WAVELET POWER SPECTRAL INDEX
    alpha_exp_fft_blos,alpha_exp_fft_br,alpha_exp_fft_btot,      #FOURIER POWER SPECTRAL INDEX
    beff_blos,beff_br,                                           #B EFFECTIVE
    decay_index_blos,decay_index_br,                             #DECAY INDEX
    flow_field_bvec,                                             #FLOW FIELD
    helicity_energy_bvec,                                        #HELICITY
    ising_energy_blos,ising_energy_br,                           #ISING ENERGY
    ising_energy_part_blos,ising_energy_part_br,                 #ISING ENERGY PARTITIONS
    mpil_blos,mpil_br,                                           #MPILs PARAMETERS
    nn_currents,                                                 #NON NEUTRALIZED CURRENTS
    r_value_blos_logr,r_value_br_logr,                           #R VALUE                      
    sharp_kw,                                                    #EXTENDED SHARP KEYWORDS
    wlsg_blos,wlsg_br,                                           #FALCONER'S WLSG
    mf_spectrum_blos,mf_spectrum_br,mf_spectrum_btot,            #MULTI-FRACTAL SPECTRUM
    sfunction_blos,sfunction_br,sfunction_btot,                  #STRUCTURE FUNCTION
    frdim_blos,frdim_br,frdim_btot,                              #FRACTAL DIMENSION
    gen_cor_dim_blos,gen_cor_dim_br,gen_cor_dim_btot,            #GENERALIZED CORRELATION DIMENSION
    gs_slf,                                                      #SUNSPOT-MAGNETIC PROPERTIES
    """  
    
    # EXTRACT FROM HELCATS LIST THOSE EVENTS WITH ASSOCIATED SOURCE REGIONS
    reduced_list = []
    for i in helcats_list:
        ind = i["FL_TYPE"]
        if ind == 'swpc' or ind == 'hessi':
            reduced_list.append(i)
    print 'Total CMEs with associatted Flare source region: ', len(reduced_list)
    
    
    # FOR THOSE EVENTS IN THE REDUCED LIST, WE KEEP THOSE AFTER SHARP DATA IS AVAILABLE (SHARP_DATE)
    for jj in enumerate(reduced_list):
        j = jj[1]
        print 'HELCATS CME event source region: ', jj[0],'.......'
        hel_date = j["FL_STARTTIME"]
        hel_date = dateutil.parser.parse(hel_date)
        idate = hel_date - datetime.timedelta(minutes=60)  # PLAY WITH THESE VALUES TO MATCH TIMES BETTER
        edate = hel_date + datetime.timedelta(minutes=5)
        
        if idate > sharp_date:
            print 'HELCATS date', hel_date
            nar = int(j["SRS_NO"])
            #
            loc1a = j["SMART_HGLATLON"]
            loc1 = loc1a.encode('ascii','ignore')
            print "Location according to Smart", loc1
            
            if loc1 == ' ':
                loc1a = j["SRS_LOC"]
                loc1a = loc1a.encode('ascii','ignore')
                print 'NOAA location at midnight', loc1a
                #CORRECT NOAA LOCATION TO EVENT ACTIAL TIME
                if loc1a == ' ':
                    continue
                stime = j["SRS_TIME"].encode('ascii','ignore')
                print 'SRS file time', stime
                srstime = dateutil.parser.parse(stime)
                loc1 = rot_regions(loc1a,hel_date,srstime)
                print 'Corrected location from NOAA', loc1
                
            yes = False
            
            if nar or loc1:
                nar = nar + 10000
                print 'NOAA number from HELCATS', nar
                
                idate = datetime.datetime.strftime(idate,'%Y-%m-%dT%H:%M:00Z')
                edate = datetime.datetime.strftime(edate,'%Y-%m-%dT%H:%M:00Z')
                start = iso8601.parse_date(idate)
                end   = iso8601.parse_date(edate)
                #KEEP production_02 CHECK API.FLARECAST.EU FOR MOST COMPLETE DATA PRODUCTION
                #DON'T NEED PROPERTIES AT THIS POINT, ONLY METADATA
                rdata = download_range("http://cluster-r730-1:8002", "production_02", start, end, property_type="", region_fields="*")
                
                if rdata:
                    print 'FLARECAST date', rdata[0]["time_start"]
                
                if yes == False:
                    for m in range(len(rdata)):
                        #print data[m]["time_start"]
                        nnar = rdata[m]["meta"]["nar"]
                        
                        if nnar:
                            if nar in nnar and len(nnar) == 1:
                                print 'Region matched by NOAA No', nar
                                # ADD A FIELD FOR QUALITY OF THE MATCH -- 0 MEANS MATCHED BY NOAA NUMBER
                                mm = rdata[m]["data"]
                                mm["fc_data_q"] = 0
                                yes = True
                                print 'Matched region HARP num ', rdata[m]["meta"]["harp"]
                                harpnum = rdata[m]["meta"]["harp"]
                                pass_time = dateutil.parser.parse(rdata[m]["time_start"],ignoretz=True)
                                break
                   
                if yes == False:
                    for m in range(len(rdata)):
                        tolerance = 15.0 # Degrees of total distance between FC region and HC source region
                        comp_regions = comp_location(loc1,rdata[m]["long_hg"],rdata[m]["lat_hg"],tolerance)
                        if comp_regions:
                            print 'Region matched by position'
                            print 'Region location from FLARECAST',rdata[m]["lat_hg"],rdata[m]["long_hg"]
                            # ADD A FIELD FOR QUALITY OF THE MATCH -- !=0 MEANS SOURCE REGION IS "fl_data_q" DEGREES FROM FLARECAST REGION
                            mm = rdata[m]["data"]
                            mm["fc_data_q"] = comp_regions
                            yes = True
                            print 'Matched region HARP num ', rdata[m]["meta"]["harp"]
                            harpnum = rdata[m]["meta"]["harp"]
                            pass_time = dateutil.parser.parse(rdata[m]["time_start"],ignoretz=True)
                            break
                
                if not yes:
                    print 'No SHARP Region matched to candidate source region'

                if yes == True:
                    # REQUESTING DATA FOR HARP UP TO DELTA HOURS BEFORE
                    idate = pass_time - datetime.timedelta(hours=24)  
                    edate = pass_time + datetime.timedelta(minutes=5)
                    idate = datetime.datetime.strftime(idate,'%Y-%m-%dT%H:%M:00Z')
                    edate = datetime.datetime.strftime(edate,'%Y-%m-%dT%H:%M:00Z')
                    start = iso8601.parse_date(idate)
                    end   = iso8601.parse_date(edate)
                    # 
                    rdatam = download_range("http://cluster-r730-1:8002", "production_02", start, end, step=datetime.timedelta(minutes=60), property_type=ps, region_fields="*")#, nar=str(narnum))
                    
                    #LOOPS FOR ALL REGIONS WITH THE SAME HARP NUMBER
                    #CREATE HERE A LIST OF THE PROPERTIES YOU WANT A TS
                    #CHECK OUTPUT OF HELCATS_match_FLARECAST_1.py FOR STTRUCTURE OF PROPERTIES
                    #25 IS THE NUMBER OF HOURS FOR TS
                    time = [str(np.nan) for i in range(25)]
                    r_values_br = [np.nan for i in range(25)]
                    alpha_fft_br = [np.nan for i in range(25)]
                    usiz_tot = [np.nan for i in range(25)]
                    di4_br = [np.nan for i in range(25)]
                    wlsg_br = [np.nan for i in range(25)]
                    usflux_total = [np.nan for i in range(25)]
                    ushz_ave = [np.nan for i in range(25)]
                    hgrad_bh_max = [np.nan for i in range(25)]
                    ushz_tot = [np.nan for i in range(25)]
                    ising_energy_blos = [np.nan for i in range(25)]
                    usiz_max = [np.nan for i in range(25)]
                    hz_max = [np.nan for i in range(25)]
                    jz_max = [np.nan for i in range(25)]
                    helicity_tot_dhdt = [np.nan for i in range(25)]
                    helicity_tot_dedt = [np.nan for i in range(25)]
                    #
                    delta_time = [i*1.0 for i in range(25)]
                    delta_time = delta_time[::-1]
                    #
                    for l in rdatam:
                        #
                        nharp = l["meta"]["harp"]
                        ttime = l["time_start"]
                        #
                        ttime = dateutil.parser.parse(ttime,ignoretz=True)
                        #
                        ddelta = pass_time - ttime
                        #
                        diff1 = ddelta.days*24 + ddelta.seconds//3600.
                        #
                        for t,i1 in zip(delta_time,range(25)):
                            if nharp == harpnum:
                                #
                                if diff1 == t:
                                    try:
                                        time[i1] = l["time_start"]
                                        r_values_br[i1] = l["data"]["r_value_br_logr"]
                                        alpha_fft_br[i1] = l["data"]["alpha_exp_fft_br"]['alpha']
                                        usiz_tot[i1] = l["data"]["sharp_kw"]['usiz']['total']
                                        di4_br[i1] = l["data"]["decay_index_br"]['max_l_over_hmin']
                                        wlsg_br[i1] = l["data"]["wlsg_br"]['value_int']
                                        usflux_total[i1] = l["data"]["sharp_kw"]['usflux']['total']
                                        ushz_ave[i1] = l["data"]["sharp_kw"]['ushz']['ave']
                                        hgrad_bh_max[i1] = l["data"]["sharp_kw"]['hgradbh']['max']
                                        ushz_tot[i1] = l["data"]["sharp_kw"]['ushz']['total']
                                        ising_energy_blos[i1] = l["data"]["ising_energy_blos"]['ising_energy']
                                        usiz_max[i1] = l["data"]["sharp_kw"]['usiz']['max']
                                        hz_max[i1] = l["data"]["sharp_kw"]['hz']['max']
                                        jz_max[i1] = l["data"]["sharp_kw"]['jz']['max']
                                        helicity_tot_dhdt[i1] = l["data"]["helicity_energy_bvec"]['abs_tot_dhdt']
                                        helicity_tot_dedt[i1] = l["data"]["helicity_energy_bvec"]['abs_tot_dedt']
                                    except:
                                        continue
                                else:
                                    continue
                    #MAKE A STRUCTURE WITH TS
                    fc_data = {'time': time, 'r_values_br':r_values_br,'alpha_fft_br':alpha_fft_br, 'usiz_tot': usiz_tot, 
                    'di4_br':di4_br, 'wlsg_br':wlsg_br, 'usflux_total':usflux_total, 'ushz_ave':ushz_ave, 
                    'hgrad_bh_max':hgrad_bh_max, 'usflux_total':usflux_total, 'ushz_tot':ushz_tot, 
                    'ising_energy_blos':ising_energy_blos, 'usiz_max': usiz_max, 'hz_max':hz_max,
                    'jz_max':jz_max, 'helicity_tot_dhdt':helicity_tot_dhdt, 'helicity_tot_dedt':helicity_tot_dedt}
                    j["FC_data"] = fc_data
                    #
                    
            print ' '
    # FIND THE NUMBER OF REGIONS MATCHED
    one = 0
    for l in reduced_list:
        try:
            ind = l["FC_data"]
            if ind:
                one += 1
        except:
            continue
    print 'Number of HELCATS events matched to FLARECAST regions:', one
    #
    with io.open('helcats_list_flarecast_properties_28July17_TS_top15_predictors_1.txt', 'w', encoding='utf-8') as f:
        f.write(json.dumps(reduced_list , ensure_ascii=False))
    