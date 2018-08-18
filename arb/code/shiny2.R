library(shiny)
require(shinydashboard)
library(ggplot2)
library(dplyr)
library(htmlwidgets)

library(formattable)
tt = Sys.time()
setwd('/home/yinpatt/gs/project/arb')
price = read.csv('/home/yinpatt/Documents/project/arb/data/current_price.csv')
buy = read.csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vTngs1qxdmzOyo6SPXesP-aVyQ5FewecvJzIzS4PONM1e3NzO3uSmVcxL2ZvKk0T0tC2HqaC1nvLCz9/pub?gid=0&single=true&output=csv')
sell = read.csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vTngs1qxdmzOyo6SPXesP-aVyQ5FewecvJzIzS4PONM1e3NzO3uSmVcxL2ZvKk0T0tC2HqaC1nvLCz9/pub?gid=916464146&single=true&output=csv')

lasttime = Sys.time()

recommendation <- read.csv('https://raw.githubusercontent.com/amrrs/sample_revenue_dashboard_shiny/master/recommendation.csv',stringsAsFactors = F,header=T)
head(recommendation)





#Dashboard header carrying the title of the dashboard
header <- dashboardHeader(title = "Crypto arbitrage")  
#Sidebar content of the dashboard
sidebar <- dashboardSidebar(
  sidebarMenu(
    numericInput("obs", "Arbitrage %", 0.02, min = 0, max = 1, step = 0.01),
    menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
    menuItem("Visit-us", icon = icon("send",lib='glyphicon'), 
             href = "https://www.google.com")
  )
)

frow1 <- fluidRow(
  valueBoxOutput("value1")
  ,valueBoxOutput("value2")
  ,valueBoxOutput("value3")
)
frow2 <- fluidRow( 
  box(
    width = 12,
    title = "Arbitrage"
    ,status = "primary"
    ,solidHeader = TRUE 
    ,collapsible = TRUE 
    #,plotOutput("revenuebyPrd", height = "300px")
    ,formattableOutput("table")
  )
  ,box(
    width = 12,
    title = "Current Price"
    ,status = "primary"
    ,solidHeader = TRUE 
    ,collapsible = TRUE 
    ,formattableOutput("table2")
  ) 
)
# combine the two fluid rows to make the body
body <- dashboardBody(frow1, frow2)

lasttime = as.character(lasttime)

#completing the ui part with dashboardPage
ui <- dashboardPage(title = 'Crypto arbitrage', header, sidebar, body, skin='red')

# create the server functions for the dashboard  
server <- function(input, output) { 
  #some data manipulation to derive the values of KPI boxes
  total.revenue <- lasttime
  sales.account <- recommendation %>% group_by(Account) %>% summarise(value = sum(Revenue)) %>% filter(value==max(value))
  prof.prod <- recommendation %>% group_by(Product) %>% summarise(value = sum(Revenue)) %>% filter(value==max(value))
  #creating the valueBoxOutput content
  output$value1 <- renderValueBox({
    invalidateLater(1000)
    lasttime = Sys.time()
    lasttime = as.character(lasttime)
    valueBox(
      formatC('Last update', format="d", big.mark=',')
      ,paste(lasttime)
      ,icon = icon("stats",lib='glyphicon')
      ,color = "orange")  
  })

  #creating the plotOutput content
  output$revenuebyPrd <- renderPlot({
    ggplot(data = recommendation, 
           aes(x=Product, y=Revenue, fill=factor(Region))) + 
      geom_bar(position = "dodge", stat = "identity") + ylab("Revenue (in Euros)") + 
      xlab("Product") + theme(legend.position="bottom" 
                              ,plot.title = element_text(size=15, face="bold")) + 
      ggtitle("Revenue by Product") + labs(fill = "Region")
  })
  output$revenuebyRegion <- renderPlot({
    ggplot(data = recommendation, 
           aes(x=Account, y=Revenue, fill=factor(Region))) + 
      geom_bar(position = "dodge", stat = "identity") + ylab("Revenue (in Euros)") + 
      xlab("Account") + theme(legend.position="bottom" 
                              ,plot.title = element_text(size=15, face="bold")) + 
      ggtitle("Revenue by Region") + labs(fill = "Region")
  })
  
  output$table <- renderFormattable({
    invalidateLater(3000)
    df = read.csv('/home/yinpatt/Documents/project/arb/data/arb.csv')
    
    if (as.numeric(tt - Sys.time())>300)
    {buy = read.csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vTngs1qxdmzOyo6SPXesP-aVyQ5FewecvJzIzS4PONM1e3NzO3uSmVcxL2ZvKk0T0tC2HqaC1nvLCz9/pub?gid=0&single=true&output=csv')
    sell = read.csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vTngs1qxdmzOyo6SPXesP-aVyQ5FewecvJzIzS4PONM1e3NzO3uSmVcxL2ZvKk0T0tC2HqaC1nvLCz9/pub?gid=916464146&single=true&output=csv')
    tt = Sys.time()
    }
    affected = colnames(buy)[3: length(colnames(buy))]
    for (i in 1:length(affected)){
    buy[,affected[i]] = as.numeric(gsub('%', '', buy[,affected[i]]))/100
    sell[,affected[i]] = as.numeric(gsub('%', '', sell[,affected[i]]))/100}
    

    
    ask_after_cost = sapply(0:length(df)+1, function(i)
    {
      temp = buy[buy$s==as.character(df$ex_a[i]), as.character(df$coins[i])]
      if(length(temp)==0){temp = 0.002}
      temp
    })
    bid_after_cost = sapply(0:length(df)+1, function(i)
    {
      temp = sell[sell$s==as.character(df$ex_b[i]), as.character(df$coins[i])]
      if(length(temp)==0){temp = 0.002}
      temp
      })

    df$arb = df$bid*(1-(bid_after_cost))/df$ask*(1+(ask_after_cost))-1
    
    
    df$arb = percent(df$arb)
    df$v_x = sapply( df$v_x,function(x)
    {m = gsub("\\]", "", gsub("\\[", "", x))
    m = sapply(as.list(strsplit(m,split=',', fixed=TRUE)[[1]]), function(x) as.numeric(x))
    m[1]
    })
    
    df$v_y = sapply( df$v_y,function(x)
    {m = gsub("\\]", "", gsub("\\[", "", x))
    m = sapply(as.list(strsplit(m,split=',', fixed=TRUE)[[1]]), function(x) as.numeric(x))
    m[1]
    })
    
    df['volume'] = ifelse(df$v_x<df$v_y, df$v_x, df$v_y)
    df$volume = round(df$volume,4)
    df$ask = round(df$ask,6)
    df$bid = round(df$bid,6)
    df['ask_exchange'] = df$ex_a
    df['bid_exchange'] = df$ex_b
    df['arb_percentage'] = df$arb
    df = df[,c('time','ask_exchange','ask','bid_exchange','bid','volume','arb_percentage','coins')]
    df = df[df$arb>(input$obs/100),]
    df = head(df,10)
    btchkd = head(price[price$coins=='usdtbtc' & price$type=='bid',]$price)*7.8
    df['hkd'] = as.character(round(as.numeric(df$arb)*df$volume*btchkd,2))
    #df['hkd'] = df$arb*df$volume*btchkd
    #df['coins'] = 'ethbtc'
    
    xtable = formattable(df, list(
      exchange =formatter(
        "span",
        style = x ~ ifelse(x == "Technology", 
                           style(font.weight = "bold"), style(font.weight = "bold"))),
      volume = color_tile("white", "orange"),
      ask_exchange = formatter(
        "span",
        style = x ~ style(color = 'green')),
      ask = formatter(
        "span",
        style = x ~ style(color = 'green')),
      bid_exchange = formatter(
        "span",
        style = x ~ style(color = 'red')),
      bid = formatter(
        "span",
        style = x ~ style(color = 'red')),
      arb = formatter(
        "span",
        style = x ~ style(color = ifelse(x > 0.0002 , "red", "blue")))
    ))
    
    print(Sys.time())
    
    xtable})
  
  output$table2 <- renderFormattable({
    invalidateLater(1000)
    price = read.csv('/home/yinpatt/Documents/project/arb/data/current_price.csv')
    price$volume = round(price$volume,2)
    price$price = round(price$price,6)
    xprice = formattable(price, list(
      exchange =formatter(
        "span",
        style = x ~ ifelse(x == "Technology", 
                           style(font.weight = "bold"), style(font.weight = "bold"))),
      volume = color_tile("white", "orange"),
      type = formatter(
        "span",
        style = x ~ style(color = ifelse(x == 'ask' , "red", "green")))
    ))
    xprice})
}

shinyApp(ui, server, options=list(port = 8050, host = '0.0.0.0'))
#shinyApp(ui, server)
