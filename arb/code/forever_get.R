a=1
while(a==1)
{   system('pkill -f crypto.py', wait = FALSE)
    print("killed!!")
    Sys.sleep(900)
    print(Sys.time())
  }
