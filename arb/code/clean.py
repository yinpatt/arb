import os
import pandas as pd
import time
os.chdir('/home/yinpatt/Documents/project/arb/current')

while True:
    try:
        now_file = os.listdir()[:1000]
        if len(now_file)>0:
            l = []
            for i in now_file:
                print(len(l))
                try:
                    l.append(pd.read_csv(i))
                except:
                    ''
            df = pd.concat(l)
            with open('/home/yinpatt/Documents/project/arb/data/price.csv', 'a') as f:
                df.to_csv(f, header=False, index = False)
            [os.remove(i) for i in now_file]
        print('cleaned file: '+ str(len(now_file)))
        time.sleep(0.2)
    except:
        print('error')