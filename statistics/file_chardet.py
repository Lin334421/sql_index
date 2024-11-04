import chardet

# 先检测文件编码
with open("/Users/jonas/gits/linux/MAINTAINERS", "rb") as file:
    raw_data = file.read()
    result = chardet.detect(raw_data)
    encoding = result['encoding']
    print(encoding)

# 使用检测到的编码读取文件
# with open("your_file.txt", "r", encoding=encoding) as file:
#     content = file.read()