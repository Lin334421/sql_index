import csv
import time

from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info

bulk_data = []

with open('cctlds.csv','r') as f:
    cr = csv.reader(f)
    next(cr)
    for row in cr:
        region=''
        if row[0] in ('Ascension Island',
                      'Åland',
                      'European Union',
                      'Faeroe Islands',
                      'Saint Martin (officially the Collectivity of Saint Martin)',
                      'Galicia',
                      'French Guiana',
                      'Saint Martin (officially the Collectivity of Saint Martin)',
                      'South Georgia and the South Sandwich Islands','Saint Martin (officially the Collectivity of Saint Martin)',
                      'Montserrat','Pitcairn Islands','Réunion Island','Saint Helena','Svalbard and Jan Mayen Islands'
                      ,'Vatican City','Wallis and Futuna',
                      'Saint Barthélemy (informally also referred to as Saint Barth’s or Saint Barts)',
                      'Sint Eustatius',
                      'Bouvet Island',
                      'Catalonia','Czechia (Czech Republic)',
                      'Basque Country','British Indian Ocean Territory',
                      'North Cyprus (unrecognised, self-declared state)',
                      'Sint Eustatius','Saint-Pierre and Miquelon',
                      'Sint Maarten','Turks and Caicos Islands',
                      'French Southern and Antarctic Lands','United Kingdom (UK)','Mayotte'):
            region = '欧洲'
        elif row[0] in ('United Arab Emirates (UAE)'):
            region = '中东'
        elif row[0] in ('Burkina Faso',
                        'Gambia','Guinea-Bissau',
                        'Djibouti','Eritrea','Comoros',
                        'Western Sahara','Seychelles','Somaliland',
                        'South Sudan','Swaziland',
                        'Chad','Sint Maarten',
                        'Congo, Democratic Republic of the (Congo-Kinshasa)',
                        'Central African Republic',
                        'Côte d’Ivoire (Ivory Coast)',
                        'Cape Verde (in Portuguese: Cabo Verde)',
                        'Western Sahara',
                        'Gabon (officially: Gabonese Republic)',
                        'Equatorial Guinea',
                        'Lesotho',
                        'Macedonia, Republic of (the former Yugoslav Republic of Macedonia, FYROM)',
                        'Niger','Sierra Leone','São Tomé and Príncipe','Congo, Republic of the (Congo-Brazzaville)'):
            region = '非洲'
        elif row[0] in ('Bahamas','Saint Kitts and Nevis',
                        'Saint Lucia',
                        'United States Virgin Islands',
                        'American Samoa','Dominica','United States of America (USA)'):
            region = '北美'
        elif row[0] in ('Belize', 'Trinidad & Tobago','Antigua and Barbuda','Northern Mariana Islands'):
            region = '中美洲'
        elif row[0] in ('Cook Islands','Marshall Islands',
                        'Palau','Solomon Islands',
                        'Tonga',
                        'Vanuatu','Samoa','Federated States of Micronesia'):
            region = '大洋洲'
        elif row[0] in ('Christmas Island',
                        'Cocos (Keeling) Islands',
                        'Heard Island and McDonald Islands'):
            region = '澳洲'
        elif row[0] in ('Guyana',
                        'Saint Vincent and the Grenadines',
                        'Falkland Islands','Norfolk Island'):
            region = '南美洲'
        elif row[0] in ('Myanmar','East Timor (Timor-Leste)','East Timor (Timor-Leste)'):
            region = '东南亚'
        elif row[0] in ('Macau'):
            region = '中国'




        bulk_data.append({
            "country": row[0],
            "email_country_domain": row[1].strip(' '),
            "region":region,
            "inserted_at": int(time.time())
        })





ck_client = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])
ck_client.execute_no_params('truncate table  email_country_domain_country_map_local on cluster replicated')
time.sleep(2)
ck_client.execute_no_params('truncate table  cctlds_local on cluster replicated')
time.sleep(2)
ck_client.execute("insert into table email_country_domain_country_map values",bulk_data)
ck_client.execute_no_params("""
insert into table cctlds
select email_country_domain,
       country,
       multiIf(region = '中美', '中美洲',region = '南美', '南美洲', country = 'Taiwan', '中国台湾', region) as new_region,
       toUnixTimestamp(now())
from (select email_country_domain, country, if(a.region = '', b.region, a.region) as region
      from (select * from email_country_domain_country_map) as a global
               left join (select * from country_region_map) as b on a.country = b.country)
""")