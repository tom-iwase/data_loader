import pandas as pd
import datetime as dt
import pathlib
import requests
from requests.auth import HTTPDigestAuth
import cdflib

class DataLoader:
    
    def __init__(self, key, datetime_string=None):
        
        self.key  = key
        self.datetime = pd.to_datetime(datetime_string if datetime_string is not None else 0)
        self.datetime_string = self.datetime.strftime('%Y-%m-%d %H:%M:%S')
        self.path = ''
        self.content = None
        
        data_dir = '/mnt/d/research/data/'
        ergsc_link = 'https://ergsc.isee.nagoya-u.ac.jp/data/'
        setting_dict_func = lambda: {
 
            #usage
            #'KEY':{
            #     'path_format' : "PATH",                                                                  #Data path format(Use datetime.strftime() to get a path string)
            #     'url_format': "URL",                                                                     #URL format (Use datetime.strftime() to get a url string)
            #     'load_method': {'load_func': "FUNCTION", 'load_args': "{""ARG01"": ""VALUE01"", ...}"}   #Method function and arguments to load data
            #     'auth': {'auth_func': "FUNCTION", 'auth_args': "{""ARG01"": ""VALUE01"", ...}"}          #Authentication setting
            # }

            'calet_chd':{
                'path_format': data_dir + 'calet/cal-v1.1/CHD/level1.1/obs/%Y/CHD_%y%m%d.dat',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': '\s+', 'header':None}},
                'url_format': 'https://data.darts.isas.jaxa.jp/pub/' + 'calet/cal-v1.1/CHD/level1.1/obs/%Y/CHD_%y%m%d.dat',
            },

            'erg_orb':{
                'path_format': data_dir  + 'ergsc/satellite/erg/orb/def/%Y/erg_orb_l2_%Y%m%d_v03.cdf',
                'load_method': {'load_func' : cdflib.cdfread.CDF, 'load_args': {'path': self.path},},
                'url_format' : ergsc_link + 'ergsc/satellite/erg/orb/def/%Y/erg_orb_l2_%Y%m%d_v03.cdf',
            },

            'erg_pwe_ofa_spec':{
                'path_format': data_dir  + 'ergsc/satellite/erg/pwe/ofa/l2/spec/%Y/%m/erg_pwe_ofa_l2_spec_%Y%m%d_v02_01.cdf',
                'load_method': {'load_func' : cdflib.cdfread.CDF, 'load_args': {'path': self.path},},
                'url_format' : ergsc_link + 'ergsc/satellite/erg/pwe/ofa/l2/spec/%Y/%m/erg_pwe_ofa_l2_spec_%Y%m%d_v02_01.cdf',
                'auth': {'auth_func': HTTPDigestAuth, 'auth_args': {'username':'erg_project', 'password':'geospace'}},
            },

            'erg_mgf_64hz_dsi':{
                'path_format': data_dir + 'ergsc/satellite/erg/mgf/l2/64hz/%Y/%m/erg_mgf_l2_64hz_dsi_%Y%m%d%H_' +('v03.04.cdf' if(self.datetime<pd.to_datetime('2019-09-28')) else 'v04.04.cdf'),
                'load_method': {'load_func' : cdflib.cdfread.CDF, 'load_args': {'path': self.path},},
                'url_format' : ergsc_link + 'ergsc/satellite/erg/mgf/l2/64hz/%Y/%m/erg_mgf_l2_64hz_dsi_%Y%m%d%H_' +('v03.04.cdf' if(self.datetime<pd.to_datetime('2019-09-28')) else 'v04.04.cdf'),
                'auth': {'auth_func': HTTPDigestAuth, 'auth_args': {'username':'erg_project', 'password':'geospace'}},

            },

            'erg_mgf_8sec':{
                'path_format': data_dir + 'ergsc/satellite/erg/mgf/l2/8sec/%Y/%m/erg_mgf_l2_8sec_%Y%m%d_' +('v03.04.cdf' if(self.datetime<pd.to_datetime('2019-09-28')) else 'v04.04.cdf'),
                'load_method': {'load_func' : cdflib.cdfread.CDF, 'load_args': {'path': self.path},},
                'url_format' : ergsc_link + 'ergsc/satellite/erg/mgf/l2/8sec/%Y/%m/erg_mgf_l2_8sec_%Y%m%d_' +('v03.04.cdf' if(self.datetime<pd.to_datetime('2019-09-28')) else 'v04.04.cdf'),
                'auth': {'auth_func': HTTPDigestAuth, 'auth_args': {'username':'erg_project', 'password':'geospace'}},
            },

            #My data
            ##Event
            'calet_rep_event_v1':{
                'path_format': data_dir  + 'rep/event/calet_rep_event/calet_rep_event_%Y_v1.1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'calet_rep_event_v2':{
                'path_format': data_dir  + 'rep/event/calet_rep_event/calet_rep_event_%Y_v2.0.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj':{
                'path_format': data_dir  + 'rep/event/conj/conj_%Y_v1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_event':{
                'path_format': data_dir  + 'rep/event/conj_event/conj_event_%Y_v1.csv',  
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_rep_event_v1':{
                'path_format': data_dir  + 'rep/event/conj_rep_event/conj_rep_event_%Y_v1.1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_rep_event_v2':{
                'path_format': data_dir  + 'rep/event/conj_rep_event/conj_rep_event_%Y_v2.0.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            ##Position

            'iss_pos_mlt':{
                'path_format': data_dir  + 'rep/position/iss_pos_mlt/%Y/iss_pos_mlt_%y%m%d_v1.csv',         
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'iss_rep_pos_mlt':{
                'path_format': data_dir  + 'rep/position/iss_rep_pos_mlt/iss_rep_pos_mlt_%Y%m%d%H%M.txt',
                'load_method': {'load_func' : pd.read_table,      'load_args': {'filepath_or_buffer': self.path, 'delimiter': '\s+', 'header':None},},
            },

            'iss_rep_pos_lm':{
                'path_format': data_dir  + 'rep/position/iss_rep_pos_lm/iss_rep_pos_lm_%Y%m%d%H%M.txt',
                'load_method': {'load_func' : pd.read_table,      'load_args': {'filepath_or_buffer': self.path, 'delimiter': '\s+', 'header':None},},
            },

            ##Count
            'calet_mlt_count':{
                'path_format': data_dir  + 'rep/count/calet_mlt_count/calet_mlt_count_%Y_v1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'calet_rep_mlt_count_v1':{
                'path_format': data_dir  + 'rep/count/calet_rep_mlt_count/calet_rep_mlt_count_%Y_v1.1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'calet_rep_mlt_count_v2':{
                'path_format': data_dir  + 'rep/count/calet_rep_mlt_count/calet_rep_mlt_count_%Y_v2.0.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_mlt_count_v1':{
                'path_format': data_dir  + 'rep/count/conj_mlt_count/conj_mlt_count_%Y_v1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_mlt_count_v2':{
                'path_format': data_dir  + 'rep/count/conj_mlt_count/conj_mlt_count_%Y_v2.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_rep_mlt_count_v1':{
                'path_format': data_dir  + 'rep/count/conj_rep_mlt_count/conj_rep_mlt_count_%Y_v1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_rep_mlt_count_v2':{
                'path_format': data_dir  + 'rep/count/conj_rep_mlt_count/conj_rep_mlt_count_%Y_v2.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'calet_rep_mon_count_v1':{
                'path_format': data_dir  + 'rep/count/calet_rep_mon_count/calet_rep_mon_count_%Y_v1.1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'calet_rep_mon_count_v2':{
                'path_format': data_dir  + 'rep/count/calet_rep_mon_count/calet_rep_mon_count_%Y_v2.0.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_rep_mon_count_v1':{
                'path_format': data_dir  + 'rep/count/conj_rep_mon_count/conj_rep_mon_count_%Y_v1.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

            'conj_rep_mon_count_v2':{
                'path_format': data_dir  + 'rep/count/conj_rep_mon_count/conj_rep_mon_count_%Y_v2.csv',
                'load_method': {'load_func' : pd.read_csv,        'load_args': {'filepath_or_buffer': self.path, 'delimiter': ',', 'header':None},},
            },

        }

        self.path = pathlib.Path(self.datetime.strftime(setting_dict_func()[self.key]['path_format']))
        self.dict = setting_dict_func()[self.key]
        url_format = self.dict.get('url_format', None)
        self.url  = self.datetime.strftime(url_format) if (url_format is not None) else None        
        
    def add_log(self, log):
        log_file_path = 'data_loader_log.txt'
        print(log)
        with open(log_file_path, 'a') as f:
            print(dt.datetime.now().strftime('[%Y-%m-%d %H:%M:%S]'), log, file=f)

    #Download and save file
    def dl(self, overwriting=False):
        ''' 
        #usage
        overwriting: Overwrite setting 
        '''
        if self.url == '':
            self.add_log(f'[info] save() : {self.key} {self.datetime_string} data : This data does not have URL')
            return None

        #Overwriting confirmation
        if self.path.exists() and not overwriting:
            self.add_log(f'[info] save() : {self.key} {self.datetime_string} data : file exists and overwriting is False')
            return None

        #Authentication argument
        auth_dict = self.dict.get('auth', {})
        auth_func = auth_dict.get('auth_func', None)
        auth_args = auth_dict.get('auth_args', None)
        url_auth = auth_func(**auth_args) if auth_func is not None else None

        try:
            #Download a file
            res = requests.get(self.url, auth=url_auth)
            res.raise_for_status()

        except requests.exceptions.RequestException as e:
            self.add_log(f'[error] dl() : {self.key} {self.datetime_string} data : {type(e)} ({e})')
            return False

        #Prepare parent directoris
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.path, mode='wb') as f:
            f.write(res.content)
        self.add_log(f'[log] dl() : {self.key} {self.datetime_string} data : downloaded')
        return True

    #load data
    def load(self, dl=False): 

        #Download the unexsist file if "dl" is True  
        if not self.path.exists():
            if dl and self.dl():
                pass
            else:
                return None

        #Check file size
        try:
            if self.path.stat().st_size == 0:
                return None
            load_dict = self.dict.get('load_method', {})
            load_func = load_dict.get('load_func')
            load_args = load_dict.get('load_args')
            self.content = load_func(**load_args) if load_func is not None else None
            return self.content

        except (OSError,FileNotFoundError,PermissionError) as e:
            self.add_log(f'[error] load() : {self.key} {self.datetime_string} data : {type(e)} ({e})')
            return None


    #Save passed data 
    def save(self, overwriting=False):

        #Confirm overwriting
        if self.path.exists() and not overwriting:
            self.add_log(f'[log] save() : {self.key} {self.datetime_string} data : file exists and overwriting is False')
            return None

        #Prepare parent directoris
        self.path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.content).to_csv(**{'path_or_buf': self.path, 'header': False, 'index': False})

        self.add_log(f'[log] save() : save {self.key} {self.datetime_string} data : saved')

        return True
