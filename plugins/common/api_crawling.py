
import pandas as pd
import json
import requests
import datetime
from pytz import timezone
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



def get_data(api_key,filename,key,bucket_name):
    api_key_decode = requests.utils.unquote(api_key)

    params={'serviceKey':api_key_decode,
            'returnType':'json',
            'numOfRows':100,
            'pageNo':1,
            'sidoName':'서울',
            'ver':1.4}


    response=requests.get('http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty',params=params)
    contents=response.text
    weather_json=json.loads(contents)
    weather_json_items=weather_json['response']['body']['items']
    weather_data=pd.json_normalize(weather_json_items)
    
    
    weather_data=weather_data[['stationName','stationCode','mangName','sidoName',
                            'dataTime','so2Value','coValue','o3Value','pm10Value',              
                            'pm25Value','khaiValue','so2Flag','coFlag','o3Flag',
                            'pm10Flag','pm25Flag']]

    weather_data.columns=[['측정소명','측정소 코드','측정망 정보','시도명',
                        '측정일시','아황산가스 농도','일산화탄소 농도','오존 농도','미세먼지 농도',
                        '초미세먼지 농도','통합대기환경수치','아황산가스 플래그','일산화탄소 플래그','오존 플래그',
                        '미세먼지 플래그','초미세먼지 플래그']]
    
        
        
    code_start_time=datetime.datetime.now(timezone('Asia/Seoul')).strftime('%Y%m%d_%H%M%S') ## 파일명에 사용
    weather_data.to_csv('files/대기오염데이터_{0}.csv'.format(code_start_time),encoding='utf-8',index=False)
    
    hook = S3Hook('aws_key')
    hook.load_file(filename=filename.format(code_start_time),
                   key=key.format(code_start_time),
                   bucket_name=bucket_name,
                   replace=True)
