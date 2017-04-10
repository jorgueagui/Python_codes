# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 16:06:09 2017

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
    
# FUBNCTION TO TRANSFORM LOCATION FORMAT
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
def comp_location(hc_loc,fc_lon,fc_lat):
    # FIRST CONVERT HELCATS LOCATION INTO HG COORDINATES
    region_match = False
    hg_coor = location(hc_loc)
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
        
        if diff_ < 10.:    # 10 DEGREE IS THE TOLERANCE TO MATCH REGIONS
            region_match = True
    
    return region_match


if __name__ == "__main__":
    import iso8601
    import json
    import io
    import dateutil
    
    sharp_date = datetime.datetime(2012,9,1)
    json_data=open("flarecast_flare_lists/helcats_list.json").read()
    helcats_list = json.loads(json_data)
    ps = """alpha_exp_cwt_blos,alpha_exp_cwt_br,alpha_exp_cwt_btot,alpha_exp_fft_blos,alpha_exp_fft_br,alpha_exp_fft_btot,beff_blos,
    #        beff_br,decay_index_blos,decay_index_br,flow_field_bvec,helicity_energy_bvec,ising_energy_blos,ising_energy_br,
    #        ising_energy_part_blos,ising_energy_part_br,mpil_blos,mpil_br,nn_currents,r_value_blos_logr,r_value_br_logr,sharp_kw,
    #        wlsg_blos,wlsg_br"""
    #alpha_exp_cwt_blos,alpha_exp_cwt_br,alpha_exp_cwt_btot,alpha_exp_fft_blos,alpha_exp_fft_br,alpha_exp_fft_btot,beff_blos,
    #        beff_br,decay_index_blos,decay_index_br,flow_field_bvec,helicity_energy_bvec,ising_energy_blos,ising_energy_br,
    #        ising_energy_part_blos,ising_energy_part_br,mpil_blos,mpil_br,nn_currents,r_value_blos_logr,r_value_br_logr,sharp_kw,
    #        wlsg_blos,wlsg_br
    #mf_spectrum_blos,mf_spectrum_br,mf_spectrum_btot, sfunction_blos,sfunction_br,frdim_blos,frdim_br,frdim_btot,gen_cor_dim_blos,gen_cor_dim_br
    #gen_cor_dim_btot,gs_slf,sfunction_btot
    
    # EXTRACT FROM HELCATS LIST THOSE EVENTS WITH ASSOCIATED SOURCE REGIONS
    reduced_list = []
    for i in helcats_list:
        ind = i["OBSTYPE"]
        if ind == 'swpc':
            reduced_list.append(i)
    print 'Total CMEs with associatted Flare source region: ', len(reduced_list)
    
    # FOR THOSE EVENTS IN THE REDUCED LIST, WE KEEP THOSE AFTER SHARP DATA IS AVAILABLE (SHARP_DATE)
    for jj in enumerate(reduced_list):
        j = jj[1]
        print 'HELCATS CME event source region: ', jj[0],'.......'
        hel_date = j["STARTTIME"]
        hel_date = dateutil.parser.parse(hel_date)
        idate = hel_date - datetime.timedelta(minutes=60)  # PLAY WITH THESE VALUES TO MATCH TIMES BETTER
        edate = hel_date + datetime.timedelta(minutes=5)
        
        if idate > sharp_date:
            print 'HELCATS date', hel_date
            nar = int(j["NO"])
            loc1 = j["NOAALOC"]
            yes = False
            
            if nar:
                nar = nar + 10000
                print 'NOAA number from HELCATS', nar
                print 'NOAA location from HELCATS', loc1
                
                idate = datetime.datetime.strftime(idate,'%Y-%m-%dT%H:%M:00Z')
                edate = datetime.datetime.strftime(edate,'%Y-%m-%dT%H:%M:00Z')
                start = iso8601.parse_date(idate)
                end   = iso8601.parse_date(edate)
                data = download_range("http://api.flarecast.eu/property", "production_02", start, end, property_type=ps, region_fields="*")
                #print(json.dumps(data[0], indent=4))
                
                if data:
                    print 'FLARECAST date', data[0]["time_start"]
                
                for m in range(len(data)):
                    #print data[m]["time_start"]
                    nnar = data[m]["meta"]["nar"]
                    
                    if nnar:
                        try:
                            m = nnar.index(nar) # SHARP CAN BE ASSOCIATTED TO MORE THAN ONE NOAA REGIONS
                            nnar = nnar[m]
                        except:
                            nnar = 0
                        
                    if nnar == nar:
                        print 'Region matched by NOAA No'
                        j["FC_data"] = data[m]["data"]
                        yes = True
                        break
                    else:
                        comp_regions = comp_location(loc1,data[m]["long_hg"],data[m]["lat_hg"])
                        if comp_regions:
                            print 'Region matched by position'
                            print 'Region location from FLARECAST',data[m]["long_hg"],data[m]["lat_hg"]
                            j["FC_data"] = data[m]["data"]
                            yes = True
                            break
                if not yes:
                    print 'No SHARP Region matched to candidate source region'
    #print(json.dumps(reduced_list[-1], indent=4))
                #break
            print ' '
    with io.open('helcats_list_flarecast_properties_10Apr17.txt', 'w', encoding='utf-8') as f:
        f.write(json.dumps(reduced_list , ensure_ascii=False))
    
