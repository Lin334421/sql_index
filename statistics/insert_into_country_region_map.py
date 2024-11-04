# -*-coding:utf-8-*-
import time

from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info

country_or_region_area_map = {'Slovakia': '欧洲', 'Slovenia': '欧洲', 'Réunion': '欧洲', 'Greece': '欧洲',
                              'Germany': '欧洲', 'Switzerland': '欧洲',
                              'Poland': '欧洲', 'Nigeria': '非洲', 'Guinea': '非洲', 'Albania': '欧洲',
                              'New Zealand': '大洋洲','Türkiye':"欧洲",'02000':"欧洲",
                              'Thailand': '东南亚', 'Brazil': '南美', 'France': '欧洲', 'USA': '北美', 'Mexico': '北美',
                              'Russia': '俄国','Dubai - United Arab Emirates':'中东','79000':'欧洲',
                              'Botswana': '非洲', 'Madagascar': '非洲', 'Turkey': '欧洲', 'Niue': '大洋洲',
                              'Kosovo': '欧洲', 'Fiji': '大洋洲',
                              'Greenland': '欧洲', 'Sweden': '欧洲', 'Australia': '大洋洲', 'Zambia': '非洲',
                              'Guam': '北美', 'Cambodia': '东南亚',
                              'Japan': '日韩', 'Tunisia': '非洲', 'Finland': '欧洲', 'Cuba': '南美', 'Armenia': '西亚',
                              'Montenegro': '欧洲',
                              'Tuvalu': '大洋洲', 'Gibraltar': '欧洲', 'Denmark': '欧洲', 'Sudan': '非洲',
                              'Pakistan': '南亚', 'Hong kong': '中国',
                              'Taiwan': '中国', 'UK': '欧洲', 'Liberia': '非洲', 'Nepal': '南亚', 'Iran': '中东',
                              'Eswatini': '非洲',
                              'Austria': '欧洲', 'North West': '欧洲', 'Israel': '中东', 'Tanzania': '非洲',
                              'Vietnam': '东南亚', 'Benin': '非洲',
                              'Belgium': '欧洲', 'Romania': '欧洲', 'Argentina': '南美', 'Panama': '中美洲',
                              'Singapore': '东南亚',
                              'Belarus': '欧洲', 'Kiribati': '大洋洲', 'India': '印度', 'Uruguay': '南美',
                              'Zimbabwe': '非洲', 'Lithuania': '欧洲',
                              'Estonia': '欧洲', 'Portugal': '欧洲', 'Haiti': '北美', 'Brunei': '东南亚',
                              'Chile': '南美', 'Bulgaria': '欧洲',
                              'Middle East': '中东', 'Canada': '北美', 'Ghana': '非洲', 'United Kingdom': '欧洲',
                              'Netherlands': '欧洲',
                              'Malaysia': '东南亚', 'Europe': '欧洲', 'South Africa': '非洲', 'Uganda': '非洲',
                              'Paraguay': '南美',
                              'Barbados': '南美', 'Indonesia': '东南亚', 'Burundi': '非洲', 'Serbia': '欧洲',
                              'Sri Lanka': '南亚',
                              'North Korea': '朝鲜', 'Hungary': '欧洲', 'Ireland': '欧洲', 'Egypt': '非洲',
                              'Bahrain': '中东', 'Kenya': '非洲',
                              'Anguilla': '欧洲', 'Senegal': '非洲', 'Mali': '非洲', 'United States': '北美',
                              'Mongolia': '东亚', 'Ukraine': '欧洲',
                              'Spain': '欧洲', 'Luxembourg': '欧洲', 'Philippines': '东南亚', 'Mauritius': '非洲',
                              'Hong Kong': '中国',
                              'Afghanistan': '非洲', 'Algeria': '非洲', 'Saudi Arabia': '中东', 'Grenada': '北美',
                              'China': '中国',
                              'Eastern Europe': '欧洲', 'Norway': '欧洲', 'Italy': '欧洲',
                              'British indian ocean territory': '欧洲',
                              'South Korea': '日韩', 'Czechia': '欧洲', 'Tokelau': '大洋洲', 'Kuwait': '中东',
                              'Colombia': '南美',
                              'Bangladesh': '南亚', 'United Arab Emirates': '中东', 'Croatia': '欧洲', 'Latvia': '欧洲',
                              'Peru': '南美',
                              'Morocco': '非洲', 'Kazakhstan': '中亚', 'Iceland': '欧洲', 'Costa Rica': '中美',
                              'Georgia': '欧洲', 'Ecuador': '南美',
                              'Cyprus': '中东', 'Bosnia and Herzegovina': '欧洲', 'Venezuela': '南美',
                              'Moldova': '欧洲',
                              'Dominican Republic': '北美', 'Malta': '欧洲', 'Uzbekistan': '中亚', 'Jordan': '中东',
                              'North Macedonia': '欧洲',
                              'Lebanon': '中东', 'Azerbaijan': '欧洲', 'Cameroon': '非洲', 'Guatemala': '中美',
                              'Myanmar (Burma)': '东南亚',
                              'El Salvador': '中美', 'Bolivia': '南美', 'Puerto Rico': '北美', 'Ethiopia': '非洲',
                              'Iraq': '中东', 'Macao': '中国',
                              'Kyrgyzstan': '中亚', 'Antarctica': '南极洲', 'Jamaica': '中美', 'Nicaragua': '中美',
                              'Honduras': '中美',
                              'Rwanda': '非洲', 'Angola': '非洲', "Côte d'Ivoire": '非洲', 'Yemen': '中东',
                              'Palestine': '西亚', 'Syria': '中东',
                              'Qatar': '中东', 'New Caledonia': '欧洲', 'Maldives': '南亚',
                              'Trinidad and Tobago': '中美', 'Oman': '中东',
                              'French Polynesia': '欧洲', 'Jersey': '欧洲', 'Andorra': '欧洲',
                              'Democratic Republic of the Congo': '非洲',
                              'Tajikistan': '中亚', 'Liechtenstein': '欧洲', 'Isle of Man': '欧洲', 'Guernsey': '欧洲',
                              'Mozambique': '非洲',
                              'Monaco': '欧洲', 'Cayman Islands': '欧洲', 'Guadeloupe': '欧洲', 'Libya': '非洲',
                              'Martinique': '欧洲',
                              'Curaçao': '南美', 'Namibia': '非洲', 'Cape Verde': '非洲', 'Bermuda': '欧洲',
                              'San Marino': '欧洲',
                              'Republic of the Congo': '非洲', 'Somalia': '非洲', 'Malawi': '非洲',
                              'Faroe Islands': '欧洲', 'Suriname': '南美',
                              'Nauru': '大洋洲', 'Åland Islands': '欧洲', 'British Virgin Islands': '欧洲',
                              'Timor-Leste': '东南亚',
                              'Laos': '东南亚', 'The Bahamas': '北美', 'Papua New Guinea': '大洋洲', 'Togo': '非洲',
                              'Aruba': '南美',
                              'Mauritania': '非洲', 'Turkmenistan': '中亚', 'Bhutan': '南亚'}

bulk_data = []
insert_at = int(time.time()*1000)
for key, value in country_or_region_area_map.items():
    bulk_data.append({
        "country":key,
        "region":value,
        "insert_at":insert_at
    })





ck_client = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])
ck_client.execute("insert into table country_region_map values",bulk_data)
