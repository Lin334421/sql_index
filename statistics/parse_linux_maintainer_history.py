# -*-coding:utf-8-*-
import copy
import time
import chardet
from clickhouse_driver import Client, connect
from git import Repo

from statistics.config import clickhouse_server_info


class CKServer:
    def __init__(self, host, port, user, password, database, settings={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()
# 初始化仓库对象，路径是本地仓库的路径
repo = Repo("/Users/jonas/gits/linux")
ck = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])
sql_ = """
select hexsha,
       committed_date,
       toYear(committed_date)       as year,
       toMonth(committed_date)      as month,
       toYYYYMM(committed_date) as year_month
from (select hexsha, committed_date
      from gits
      where search_key__owner = 'torvalds'
        and search_key__repo = 'linux'
        and toYYYYMM(committed_date) >= 201401
        and has(`files.file_name`, 'MAINTAINERS')
      order by committed_date
      limit 1 by toYYYYMMDD(committed_date))
limit 1 by year,month
"""
hexsha_days = ck.execute_no_params(sql_)






# 切换到指定的提交哈希
# commit_hash = "your_commit_hash"
# repo.git.checkout(commit_hash)

for hexsha_day in hexsha_days:
    month = hexsha_day[4]
    hexsha = hexsha_day[0]
    print(hexsha, month)
    repo.git.reset("--hard", hexsha)
    time.sleep(2)
    with open("/Users/jonas/gits/linux/MAINTAINERS", "rb") as file:
        raw_data = file.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        print(encoding)
    with open('/Users/jonas/gits/linux/MAINTAINERS', 'r',encoding=encoding) as f:
        all_lines = f.readlines()
        flag = False
        maintainer_json = {}
        module = ''
        maintainer_list = []
        for line in all_lines:
            if line.startswith('3C59X NETWORK DRIVER'):
                flag = True
            if not flag:
                continue
            if not (line.startswith('M:') or
                    line.startswith('L:') or
                    line.startswith('S:') or
                    line.startswith('F:') or
                    line.startswith('W:') or
                    line.startswith('T:') or
                    line.startswith('R:') or
                    line.startswith('B:') or
                    line.startswith('N:') or
                    line.startswith('K:') or
                    line.startswith('Q:') or
                    line.startswith('X:') or
                    line.startswith('P:') or
                    line.startswith('C:') or
                    line == '\n'):
                module = line[:-1]
                # print(line)
            # or line.startswith('R:')
            if line.startswith('M:'):
                if line.find('<') != -1:
                    a = line.split('<')[1]
                    if line.split('<')[0].startswith('M:\t'):
                        name = line.split('<')[0].split('\t')[-1]
                    elif line.split('<')[0].startswith('M:      '):
                        name = line.split('<')[0].split('      ')[-1]
                    # if name.startswith('M:      '):
                    #     name = name.split('M:      ')[-1]
                    #     print(name,'.........')
                    name = name.strip(' ').strip('"')
                    if a.endswith('\n'):
                        a = a[0:-2]
                        # print(a)
                email = a
                if email.endswith('>'):
                    email = email[0:-1]
                if email.find('> (') !=-1:
                    email = email.split('> (')[0]
                maintainer_list.append((email,name))
            if line == '\n':
                m_list = copy.deepcopy(maintainer_list)
                maintainer_json[module] = m_list
                maintainer_list.clear()
    bulk_data = []
    for module in maintainer_json:
        for email, name in maintainer_json[module]:
            bulk_data.append({
                "update_at_timestamp":int(time.time()*1000),
                "month": month,
                "module": module,
                "name": name,
                "email": email
            })
    # print(bulk_data)
    ck.execute('insert into table linux_maintainer_history values', bulk_data)

# print(bulk_data)


"""
--sql查询
select uniqIf(email,endsWith(email, 'huawei.com')) as all_huawei_maintainer_count,
       uniqIf(email,endsWith(email, 'google.com')) as all_google_maintainer_count,
       uniqIf(email,endsWith(email,'huawei.com') and lower(module) not like '%driver%') as huawei_maintainer_not_at_driver_module,
       uniqIf(email,endsWith(email,'google.com') and lower(module) not like '%driver%') as google_maintainer_not_at_driver_module
--        sum(endsWith(email, 'huawei.com') and lower(module) not like '%driver%')
from linux_maintainer_1
"""