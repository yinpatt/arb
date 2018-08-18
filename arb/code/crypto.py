import os
from multiprocessing.dummy import Pool as ThreadPool 
import time
import requests
import pandas as pd
os.chdir('/home/yinpatt/Documents/project/arb/current')
import requests
import time
import pandas as pd
#import os
import time
#import matplotlib.pyplot as plt
import tailer as tl
import io
import datetime

class crypto():
    def cy_get(self):
        target_coins = ['USDT','ETH','BCH','ETC','NEO','TRX','QTUM','IOTA','BTC']
        target = ['BCH/BTC','BTC/USDT','ETC/BTC','ETH/BTC','ETHD/BTC','NEO/BTC','QTUM/BTC','TRX/BTC']
        r = requests.get('https://www.cryptopia.co.nz/api/GetMarkets')
        r = eval(r.text.replace('true','True').replace('false','False').replace('null','None'))
        df = pd.DataFrame(r['Data'])
        df['platform'] = 'cy'
        df = df[['AskPrice','BaseVolume','BidPrice','BuyBaseVolume','Label','platform']]
        #df = df[df.Label.apply(lambda x: sum([i.lower() in x.lower() for i in target_coins])>1)]
        #df = df[df.Label.apply(lambda x: ('btc' in x.lower()))]
        df = df[df.Label.isin(target)]
        df['Label']= df.Label.apply(lambda x: x.lower().replace('btc','').replace('/','')+'btc')
        t = time.time()
        df['time'] = t
        df.columns = range(len(df.columns))
        for tic in df[4]:
            df[df[4]==tic].to_csv('cy_'+tic+'_'+str(t)+'.csv', index = False)
        print('cy done')
        return df

    def hb_get(self):
        tics = ['etcbtc', 'ethbtc', 'bchbtc','etcbtc','neobtc','trxbtc','qtumbtc','iotabtc']
        l = []
        for tic in tics:
            r = requests.get('https://api.huobi.pro/market/depth?symbol='+tic+'&type=step1')
            temp = eval(r.content)['tick']
            df = pd.DataFrame(temp['asks'][0]+temp['bids'][0]+[tic,'hb']).transpose()
            t = time.time()
            df['time'] = t
            df.columns = range(len(df.columns))
            df.to_csv('hb_'+tic+'_'+str(t)+'.csv', index = False)
            l.append(df)
        print('hb done')
        return pd.concat(l)

    def ht_get(self):
        target = ['ETHBTC', 'ETCBTC', 'BCHBTC', 'NEOBTC', 'TRXBTC', 'QTUMBTC', 'IOTABTC']
        t = time.time()
        l = []
        for tic in target:
            try:
                r = requests.get('https://api.hitbtc.com/api/2/public/orderbook/'+tic)
                df = pd.DataFrame(eval(r.text)).head(1)
                ap = df.ask.iloc[0]['price']
                av = df.ask.iloc[0]['size']
                bp = df.bid.iloc[0]['price']
                bv = df.bid.iloc[0]['size']
                df = pd.DataFrame([ap,av,bp,bv,tic.lower(),'ht',t]).transpose()
                df.to_csv('ht_'+tic.lower()+'_'+str(t)+'.csv', index = False)
                l.append(df)
            except:
                'error'
        df = pd.concat(l)
        print('ht done')
        return df

    def po_get(self):
        l = []
        for ticker in ['BTC_ZEC','BTC_ETH','BTC_ETC','BTC_LTC']:
            a = eval(requests.get('https://poloniex.com/public?command=returnOrderBook&currencyPair='+ticker+'&depth=10').content)
            tic = ticker.lower().replace('btc','').replace('_','').replace('neos','neo')+'btc'
            t = time.time()
            df = [a['asks'][0][0], a['asks'][0][1], a['bids'][0][0],a['bids'][0][1],tic,'po', t]
            df = pd.DataFrame([df])
            df.to_csv('po_'+tic+'_'+str(t)+'.csv', index = False)
            l.append(df)
        print('po done')
        return pd.concat(l)

    def zb_get(self):
        r = requests.get('http://api.zb.cn/data/v1/allTicker')
        df = pd.DataFrame(eval(r.content)).transpose()
        df['coins'] = df.index
        target_coins = ['btcusdt',
         'etcbtc',
         'ethbtc',
         'neobtc',
         'qtumbtc',]
        df = df[df.coins.apply(lambda x: x in target_coins)]
        df = df[['sell', 'vol','buy','vol','coins']]
        df['platform'] = 'zb'
        t = time.time()
        df['time'] = t
        df.index = range(len(df))
        df.columns = range(len(df.columns))
        for tic in df[4]:
            df[df[4]==tic].to_csv('zb_'+tic+'_'+str(t)+'.csv', index = False)
        print('zb done')
        return df

    def bf_get(self):
        l = []
        for ticker in ['ethbtc','etcbtc','ethbtc','zecbtc','neobtc','trxbtc','ltcbtc','qtmbtc']:
            time.sleep(0.1)
            try:
                #bf_ethbtc = bf.order_book(ticker)
                r = requests.get('https://api.bitfinex.com/v1/book/'+ticker, timeout=1)
                bf_ethbtc = eval(r.text)
                #print(bf_ethbtc)
            #bm_ethbtc = [bm_ethbtc['asks']['highbid'],bm_ethbtc['asks']['volume'],bm_ethbtc['bids']['highbid'],bm_ethbtc['bids']['volume'],'ethbtc','bm']

                bf_ethbtc_ap = bf_ethbtc['asks'][1]['price']
                bf_ethbtc_am = bf_ethbtc['asks'][1]['amount']
                bf_ethbtc_bp = bf_ethbtc['bids'][-1]['price']
                bf_ethbtc_bm = bf_ethbtc['bids'][-1]['amount']

                tic = ticker.replace('btc','')+'btc'

                t = time.time()
                df = [bf_ethbtc_ap,bf_ethbtc_am,bf_ethbtc_bp,bf_ethbtc_bm, tic,'bf', t]
                df = pd.DataFrame([df])
                df.to_csv('bf_'+tic+'_'+str(t)+'.csv', index = False)
                l.append(df)
            except Exception as e:
                ''
        print('bf done')
        if len(l)>0:
            return df
        else:
            'nothing'

    def get_price(self,exchange):
        return eval('self.'+exchange+'_get()')
    
    def run(self):
        before = time.time()
        pool = ThreadPool(6) 
        results = pool.map(self.get_price, ['ht','zb','cy','hb','bf','po'])
        pool.close() 
        pool.join()
        after = time.time()
        print(after-before)
        return pd.concat(results)

    def arb(self,df, x, c):
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

    def get(self,i,df):
        temp = df[df.coins==i]
        ask = temp[temp.type=='ask'].copy()
        bid = temp[temp.type=='bid'].copy()

        ask = ask[ask.price==min(ask.price)].head(1)
        bid = bid[bid.price==max(bid.price)].head(1)
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

    def run_arb(self):
        try:
            fname = '/home/yinpatt/Documents/project/arb/data/price.csv'
            file = open(fname)
            lastLines = tl.tail(file,10000) #to read last 15 lines, change it  to any value.
            file.close()
            df=pd.read_csv(io.StringIO('\n'.join(lastLines)), header=None,error_bad_lines=False )
            df.columns = ['ap', 'am', 'bp', 'bm', 'coins', 'ex', 'time']
            df = df[df.ap.astype(str).apply(lambda x: '2018-' not in x)]
            #df = df[df.time!='time']
            l = []
            for x in df.time:
                try:
                    l.append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((x))))
                except:
                    l.append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((1533539462.937796))))

            df['time'] = l
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
            df = df[df.ap.astype(str).apply(lambda x: '2018-' not in x)]
            df['ap'] = df.ap.apply(lambda x: float(x))
            subset = pd.concat([df[df.key==i].tail(1) for i in unique_key])


            #subset = df[df.time==max(df.time)].copy()
            t1 = subset[['time','ex','ap','am','coins']]
            t1.columns = ['time','exchange','price','volume','coins']
            t1['type'] = 'ask'
            t2 = subset[['time','ex','bp','bm','coins']]
            t2.columns = ['time','exchange','price','volume','coins']
            t2['type'] = 'bid'
            t = pd.concat([t1,t2], axis = 0)
            t = t.sort_values('price',ascending = False)
            #print(t)
            t.to_csv('/home/yinpatt/Documents/project/arb/data/current_price.csv', index = False)
            result = pd.concat(self.get(i,t) for i in set(t.coins))
            #print(result)
            self.result = result
            with open('/home/yinpatt/Documents/project/arb/data/history_arb.csv', 'a') as f:
                result.to_csv(f, header=False, index = False)
            result = pd.read_csv('/home/yinpatt/Documents/project/arb/data/history_arb.csv')
            result = result.sort_values('time', ascending = False).drop_duplicates()
            result = result.head(10)
            
            result.to_csv('/home/yinpatt/Documents/project/arb/data/arb.csv', index = False)
            time.sleep(2)
        except Exception as ex:
            print(ex)
    
c = crypto()
while True:
    try:
        c.run_arb()
        c.run()
        time.sleep(8)
    except:
        print('error')