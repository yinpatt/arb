import pandas as pd
#import os
import time
#import matplotlib.pyplot as plt
import tailer as tl
import io
import datetime

def arb(df, x, c):
    df = df.copy()
    ap = pd.DataFrame(df.groupby(['time'], sort=False)['ap'].min())#.drop_duplicates()
    bp = pd.DataFrame(df.groupby(['time'], sort=False)['bp'].max())#.drop_duplicates()
    ap['key'] = ap.ap.astype(str)+ap.index.astype(str)
    bp['key'] = bp.bp.astype(str)+bp.index.astype(str)

    df['apkey'] = df.ap.astype(str)+df.time.astype(str)
    df['bpkey'] = df.bp.astype(str)+df.time.astype(str)

    df['ap_y'] = list(pd.merge(df, ap, left_on = 'apkey', right_on = 'key', how = 'left')['ap_y'])
    df['bp_y'] = list(pd.merge(df, bp, left_on = 'bpkey', right_on = 'key', how = 'left')['bp_y'])
    df = df[list(df.columns)[:7]+['ap_y', 'bp_y']]
    df = df.fillna(-1)
    df['ap_y'] = df.ap_y.apply(lambda x: 1 if x>0 else 0 )
    df['bp_y'] = df.bp_y.apply(lambda x: 1 if x>0 else 0 )
    ask = df[df.ap_y==1][['ap','time','ex','am']]
    ask = ask.sort_values('time')
    ask = ask.reset_index()
    bid = df[df.bp_y==1][['bp','time','ex','bm']]
    bid = bid.sort_values('time')
    bid = bid.reset_index()
    ask.columns = ['index','price','time','ex','v']
    ask['type'] = 'bestask'
    bid.columns = ['index','price','time','ex','v']
    bid['type'] = 'bestbid'
    bex = list(bid.groupby('time')['ex'].apply(lambda x: ','.join(list(set(x)))))
    bv = list(bid.groupby('time')['v'].apply(lambda x: list(x)))
    bid = bid[['price','time','type']].drop_duplicates()
    bid['ex_b'] = bex
    bid['v'] = bv
    aex = list(ask.groupby('time')['ex'].apply(lambda x: ','.join(list(set(x)))))
    av = list(ask.groupby('time')['v'].apply(lambda x: list(x)))
    ask = ask[['price','time','type']].drop_duplicates()
    ask['ex_a'] = aex
    ask['v'] = av
    ask2 = ask.copy()
    ask2['ask'] = ask2.price
    ask2 = ask2[['time','ex_a','ask','v']]
    bid2 = bid.copy()
    bid2['bid'] = bid2.price
    bid2 = bid2[['time','ex_b','bid','v']]
    #result = pd.concat([ask2,bid2] ,axis = 1)
    result = pd.merge(ask2,bid2, on = 'time', how = 'left')
    result['arb'] = result.bid-result.ask
    result = result[result.arb>x].sort_values('time', ascending = False)
    result['coins'] = c
    return result

def get(i,df):
    temp = df[df.coins==i]
    ask = temp[temp.type=='ask'].copy()
    bid = temp[temp.type=='bid'].copy()

    ask = ask[ask.price==min(ask.price)]
    bid = bid[bid.price==max(bid.price)]
    ask.index = [0]
    bid.index = [0]
    bid.columns = ['time','ex_b','bid', 'v_y','coins','type']
    ask.columns = ['time','ex_a','ask', 'v_x','coins','type']
    x = [(ask[['time','ex_a', 'ask','v_x']]),(bid[['ex_b', 'bid','v_y', 'coins']])]
    temp = pd.concat(x, axis = 1).copy()
    temp['arb'] = (list(temp.bid)[0]-list(temp.ask)[0])/(list(temp.ask)[0])

    if list(bid.bid)[0]>list(ask.ask)[0]:
        return temp
    else:
        return temp[temp.time=='']


while True:
    try:
        fname = '/home/yinpatt/Documents/project/arb/data/price.csv'
        file = open(fname)
        lastLines = tl.tail(file,10000) #to read last 15 lines, change it  to any value.
        file.close()
        df=pd.read_csv(io.StringIO('\n'.join(lastLines)), header=None)
        df.columns = ['ap', 'am', 'bp', 'bm', 'coins', 'ex', 'time']
        df = df[df.time.apply(lambda x: type(x)==float)]
        df['time'] = df.time.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(x))))

        #df['time'] = df.time.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x)))
        df['time'] = pd.to_datetime(df.time)
        #df = df[df.bp<df.ap]
        df = df[df.ex!='bm']
        #df = df.tail(10000)

        print(datetime.datetime.now())
        
        unique_coins = list(set(df.coins))
        #result = pd.concat([arb(df[df.coins==i],0.00013,i) for i in unique_coins])
        #result = arb(df,0.00013)
        
        df['key'] = df['coins']+df['ex']
        unique_key = list(set(df.key))
        subset = pd.concat([df[df.key==i].tail(1) for i in unique_key])
        #max_subsettime = max(subset.time)
        #subset = subset[max_subsettime-subset.time<'00:00:30']
        
        
        #subset = df[df.time==max(df.time)].copy()
        t1 = subset[['time','ex','ap','am','coins']]
        t1.columns = ['time','exchange','price','volume','coins']
        t1['type'] = 'ask'
        t2 = subset[['time','ex','bp','bm','coins']]
        t2.columns = ['time','exchange','price','volume','coins']
        t2['type'] = 'bid'
        t = pd.concat([t1,t2], axis = 0)
        t = t.sort_values('price',ascending = False)
        t.to_csv('current_price.csv', index = False)
        ll = []
        for i in set(t.coins):
            try:
                ll.append(get(i,t) )
            except:
                print(i)
        result = pd.concat(ll)
        #result = pd.concat(get(i,t) for i in set(t.coins))
        print(result)
        with open('/home/yinpatt/Documents/project/arb/data/history_arb.csv', 'a') as f:
            result.to_csv(f, header=False, index = False)
        result = pd.read_csv('/home/yinpatt/Documents/project/arb/data/history_arb.csv', error_bad_lines = False)
        result = result.sort_values('time', ascending = False).drop_duplicates()
        result = result.head(10)
        result.to_csv('/home/yinpatt/Documents/project/arb/data/arb.csv', index = False)
        time.sleep(2)
    except Exception as ex:
        print(ex)