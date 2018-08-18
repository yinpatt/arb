import subprocess
import os, sys
import time
while True:
    try:
        os.system("pkill -f crypto.py")
        os.system('python3 /home/yinpatt/Documents/project/arb/code/crypto.py')
    except:
        print('error')