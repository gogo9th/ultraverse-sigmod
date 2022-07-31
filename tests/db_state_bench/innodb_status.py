import sys
import subprocess

if len(sys.argv) < 2:
    print('input your parameter')
    exit(1)

target_idx = int(sys.argv[1])

if target_idx < 0 or target_idx > 8:
    print('valid range is 0 ~ 8')
    exit(1)

process = subprocess.Popen(
    ['sh', '-c', 'echo "SHOW ENGINE INNODB STATUS\G" | mysql -uroot -p123456'], stdout=subprocess.PIPE)
output, error = process.communicate()

info_dict = {}
tmp_list = []

output = output.decode('ascii').splitlines()

title = ''
i = 0
while i < len(output):
    line = output[i]
    i += 1
    if len(line) < 1:
        continue

    if line[0] == '-' and line[-1] == '-':
        if len(title) > 1:
            info_dict[title] = tmp_list

        tmp_list = []

        title = output[i]
        i += 2
        line = output[i]
        i += 1

    tmp_list.append(line)

key = list(info_dict.keys())[target_idx]
print(key)
for i in info_dict[key]:
    print(i)