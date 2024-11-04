import time

from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info

bulk_data = []


ck_client = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])

id_w_s_time_tz_data = ck_client.execute_no_params("""
select github_id,season_tz from season_tz_raw_data
""")

id_tz = {}

for data in id_w_s_time_tz_data:
    github_id = data[0]
    arrs = data[1]
    id_season = {}
    id_season['summer'] = {}
    id_season['winter'] = {}


    a_country_summer = False
    a_country_winter = False

    e_01_country_summer = False
    e_01_country_winter = False

    e_12_country_summer = False
    e_12_country_winter = False

    e_23_country_summer = False
    e_23_country_winter = False
    nz_country_summer = False
    nz_country_winter = False
    au_country_summer = False
    au_country_winter = False
    inferred_location = ''
    for arr in arrs:
        season = arr[0]
        tz = arr[1]
        day_count = arr[2]
        commit_count = arr[3]
        total_day_count = arr[4]
        total_commit_count = arr[5]
        day_count_percentage = arr[6]
        commit_count_percentage = arr[7]
        id_season[season][tz] = {
                "day_count": day_count,
                "commit_count": commit_count,
                "total_day_count": total_day_count,
                "total_commit_count": total_commit_count,
                "day_count_percentage": day_count_percentage,
                "commit_count_percentage": commit_count_percentage
            }
        if season == 'summer' and tz == -7 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            a_country_summer = True
        if season == 'winter' and tz == -8 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            a_country_winter = True
        if season == 'summer' and tz == -4 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            a_country_summer = True
        if season == 'winter' and tz == -5 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            a_country_winter = True
        if season == 'winter' and tz == 0 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            e_01_country_summer = True
        if season == 'summer' and tz == 1 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            e_01_country_winter = True
        if season == 'winter' and tz == 1 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            e_12_country_summer = True
        if season == 'summer' and tz == 2 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            e_12_country_winter = True
        if season == 'winter' and tz == 2 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            e_23_country_summer = True
        if season == 'summer' and tz == 3 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            e_23_country_winter = True
        if season == 'summer' and tz == 13 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            nz_country_summer = True
        if season == 'winter' and tz == 12 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            nz_country_winter = True
        if season == 'summer' and tz == 11 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            au_country_summer = True
        if season == 'winter' and tz == 10 and day_count_percentage >= 50 and commit_count_percentage >= 50:
            au_country_winter = True
        if season == 'winter' and tz == 9 and day_count_percentage >= 80 and commit_count_percentage >= 80:
            inferred_location = '日韩'
        if season == 'winter' and tz == 8 and day_count_percentage >= 80 and commit_count_percentage >= 80 and total_commit_count>1 and total_day_count>1:
            inferred_location = '中国'
        # if season == 'winter' and tz == 3 and day_count_percentage >= 60 and commit_count_percentage >= 60:
        #     inferred_location = '俄国'
    if a_country_winter and a_country_summer:
        inferred_location = '北美'
    if (e_01_country_summer and e_01_country_winter or
            e_12_country_summer and e_12_country_winter or
            e_23_country_summer and e_23_country_winter):
        inferred_location = '欧洲'
    if nz_country_summer and nz_country_winter:
        inferred_location = '新西兰区域'
    if au_country_winter and au_country_summer:
        inferred_location = '澳大利亚区域'

    #[]
    def sum_data(tz_union):
        union_day_count_percentage, union_commit_count_percentage = 0, 0
        for tz,season in tz_union:
            if id_season.get(season):
                tz_data = id_season.get(season).get(tz)

                if tz_data:
                    union_day_count_percentage += tz_data.get('day_count_percentage')
                    union_commit_count_percentage += tz_data.get('commit_count_percentage')
        return union_day_count_percentage, union_commit_count_percentage

    union_us_summer_day_count_percentage, union_us_summer_commit_count_percentage = sum_data([(-7,'summer'), (-4,'summer')])
    union_us_winter_day_count_percentage, union_us_winter_commit_count_percentage = sum_data([(-8,'winter'), (-5,'winter')])
    union_ru_winter_day_count_percentage, union_ru_winter_commit_count_percentage = sum_data([(3,'winter'), (4, 'winter'),(6,'winter'), (7, 'winter')])

    if (union_us_summer_day_count_percentage >= 50
            and union_us_summer_commit_count_percentage >= 50
            and union_us_winter_day_count_percentage >= 50
            and union_us_winter_commit_count_percentage >= 50):
        inferred_location = '美国'
    if union_ru_winter_day_count_percentage>70 and union_ru_winter_commit_count_percentage>70:
        inferred_location = '俄国'

    bulk_data.append(
        {
            "update_at_timestamp": int(time.time() * 1000),
            "github_id": github_id,
            "season_tz": arrs,
            "inferred_area": inferred_location
        }
    )

a = {
    'summer':{
        -7:{
            'day_count': 1,
            'commit_count': 2,
            'total_day_count': 3,
            'total_commit_count': 4,
            'day_count_percentage': 5
        },
        -4:{
            'day_count': 1,
            'commit_count': 2,
            'total_day_count': 3,
            'total_commit_count': 4,
            'day_count_percentage': 5
        }
    }
}
ck_client.execute_no_params("truncate table season_tz_inferred_location_local on cluster replicated")
time.sleep(2)
ck_client.execute("insert into table season_tz_inferred_location values", bulk_data)
