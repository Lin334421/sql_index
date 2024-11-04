

l = [0]
a = 0
b = 0
while 1:
    while l[a]%2==0 and a < len(l)-1:
        a+=1
    while l[b]%2 == 1 and b < len(l)-1 or l[b]%2 == 0 and b<a:
        b+=1
    if l[b]%2 == 1 and b == len(l) - 1:
        break
    temp = l[a]
    l[a] = l[b]
    l[b] = temp
    if a == b == len(l)-1:
        break

print(l)