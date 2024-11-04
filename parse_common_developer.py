import csv
lists =  []
final_result = []
head = ''
with open('data/common_developer.csv', 'r') as f:
    csv_reader = csv.reader(f)
    head = next(csv_reader)
    for row in csv_reader:
        print(row)
        repo1 = row[1]
        repo2 = row[5]
        author = row[2]
        flag = 0
        for list_ in lists:
            if author == list_[0] and repo1==list_[2] and repo2 == list_[1]:
                flag = 1
                break
        if not flag:
            final_result.append([author,repo1,row[3],repo2,row[-1]])
            lists.append([author,repo1,repo2])

with open('data/cleaned_common_developer.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['author','repo1','at_repo_1_commit_count','repo2','at_repo2_commit_count'])
    writer.writerows(final_result)

# print(len(final_result))
