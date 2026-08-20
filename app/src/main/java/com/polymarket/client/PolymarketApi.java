package com.polymarket.client;

import android.os.Handler;
import android.os.Looper;
import okhttp3.*;
import org.json.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class PolymarketApi {
    public interface Callback { void done(boolean ok, String message); }
    public interface MarketCallback { void markets(List<Market> markets, String error); }
    public interface FeedCallback { void update(FeedTick tick); }
    public interface SpotCallback { void price(double price, String error); }
    public interface CryptoCallback { void price(double price, String source, String error); }
    public interface BookCallback { void book(OrderBookInfo info, String error); }

    public static class Market {
        public String id, question, yesToken, noToken; public long startMs, endMs; public double targetPrice=-1;
        public Market(String id,String question,String yes,String no,long startMs,long endMs,double target){
            this.id=id;this.question=question;this.yesToken=yes;this.noToken=no;this.startMs=startMs;this.endMs=endMs;this.targetPrice=target;
        }
    }
    public static class OrderBookInfo {
        public double tickSize=0.01, minOrderSize=1; public boolean negRisk=false; public List<double[]> asks=new ArrayList<>(), bids=new ArrayList<>();
    }
    public static class FeedTick {
        public String eventType, assetId, bestBid, bestAsk, lastPrice, raw; public List<String[]> bids=new ArrayList<>(), asks=new ArrayList<>();
        public FeedTick(String t,String a,String bid,String ask,String last,String raw){eventType=t;assetId=a;bestBid=bid;bestAsk=ask;lastPrice=last;this.raw=raw;}
    }

    private final OkHttpClient http=new OkHttpClient.Builder()
            .connectTimeout(12,TimeUnit.SECONDS).readTimeout(20,TimeUnit.SECONDS).pingInterval(15,TimeUnit.SECONDS).build();
    private final Handler main=new Handler(Looper.getMainLooper()); private WebSocket socket; private WebSocket cryptoSocket; private FeedCallback activeFeed; private Runnable heartbeat; private Runnable cryptoHeartbeat;

    public void testGamma(Callback cb){get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1",cb,"Gamma");}
    public void testClob(Callback cb){get("https://clob.polymarket.com/time",cb,"CLOB");}
    public void testData(Callback cb){get("https://data-api.polymarket.com/trades?limit=1",cb,"Data API");}
    public void testRelayer(Callback cb){get("https://relayer-v2.polymarket.com/health",cb,"Relayer");}

    public void fetchBtcMarkets(MarketCallback cb){
        // New integrations should use keyset pagination. Ordering by endDate makes the first page useful for short-lived markets.
        String url="https://gamma-api.polymarket.com/markets/keyset?limit=100&closed=false&order=endDate&ascending=true";
        Request r=new Request.Builder().url(url).build();
        http.newCall(r).enqueue(new okhttp3.Callback(){
            public void onFailure(Call c,IOException e){main.post(()->cb.markets(Collections.emptyList(),"Gamma error: "+e.getClass().getSimpleName()+": "+e.getMessage()));}
            public void onResponse(Call c,Response r)throws IOException{
                String body=r.body()!=null?r.body().string():"";
                if(!r.isSuccessful()){main.post(()->cb.markets(Collections.emptyList(),"Gamma HTTP "+r.code()));return;}
                try{
                    JSONObject wrapper=new JSONObject(body); JSONArray a=wrapper.optJSONArray("markets");
                    if(a==null) a=new JSONArray(body);
                    List<Market> out=new ArrayList<>();
                    long now=System.currentTimeMillis();
                    for(int i=0;i<a.length();i++){
                        JSONObject o=a.getJSONObject(i);
                        if(!o.optBoolean("active",true)||o.optBoolean("closed",false)) continue;
                        String q=o.optString("question",""); String slug=o.optString("slug","");
                        String text=(q+" "+slug+" "+o.optString("description","")).toLowerCase(Locale.US);
                        if(!text.contains("btc")&&!text.contains("bitcoin")) continue;
                        String yes="",no=""; JSONArray ids=null;
                        String raw=o.optString("clobTokenIds","");
                        if(!raw.isEmpty()) try{ids=new JSONArray(raw);}catch(Exception ignored){}
                        if(ids==null) ids=o.optJSONArray("clobTokenIds");
                        if(ids!=null){if(ids.length()>0)yes=ids.optString(0);if(ids.length()>1)no=ids.optString(1);}
                        long start=parseTime(o.optString("startDate",o.optString("start_date","")));
                        long end=parseTime(o.optString("endDate",o.optString("end_date","")));
                        double target=parseTarget(q+" "+o.optString("description",""));
                        if(end>now && !yes.isEmpty()) out.add(new Market(o.optString("id",slug),q,yes,no,start,end,target));
                    }
                    Collections.sort(out,(x,y)->Long.compare(x.endMs,y.endMs));
                    main.post(()->cb.markets(out,""));
                }catch(Exception e){main.post(()->cb.markets(Collections.emptyList(),"Invalid Gamma response: "+e.getMessage()));}
            }
        });
    }

    public void fetchFiveMinuteBtcMarket(MarketCallback cb){
        // The recurring BTC Up/Down 5-minute series is a better discovery source than
        // the general market list. Gamma can contain many active markets, while this
        // series gives us the continuously generated 5-minute BTC windows directly.
        String url="https://gamma-api.polymarket.com/events?series_slug=btc-up-or-down-5m&closed=false&limit=500&order=endDate&ascending=true";
        fetchFiveMinuteFromEvents(url,(markets,error)->{
            if(!error.isEmpty() || markets.isEmpty()){
                // Fallback for installations where the series_slug filter is unavailable.
                String fallback="https://gamma-api.polymarket.com/events?active=true&closed=false&limit=500&order=endDate&ascending=true";
                fetchFiveMinuteFromEvents(fallback,(fallbackMarkets,fallbackError)->{
                    if(!fallbackError.isEmpty()||fallbackMarkets.isEmpty()){
                        String search="https://gamma-api.polymarket.com/public-search?q=btc%20up%20or%20down%205%20minute&limit=50";
                        fetchFiveMinuteFromEvents(search,(searchMarkets,searchError)->{
                            if(!searchError.isEmpty()||searchMarkets.isEmpty()){
                                cb.markets(Collections.emptyList(),"No active BTC Up/Down 5-minute market found in Gamma. "+(searchError.isEmpty()?"":searchError));
                            }else cb.markets(searchMarkets,"");
                        });
                        return;
                    }
                    cb.markets(fallbackMarkets,"");
                });
                return;
            }
            cb.markets(markets,"");
        });
    }

    private void fetchFiveMinuteFromEvents(String url,MarketCallback cb){
        Request r=new Request.Builder().url(url).build();
        http.newCall(r).enqueue(new okhttp3.Callback(){
            public void onFailure(Call c,IOException e){main.post(()->cb.markets(Collections.emptyList(),"Gamma events error: "+e.getClass().getSimpleName()+": "+e.getMessage()));}
            public void onResponse(Call c,Response r)throws IOException{
                String body=r.body()!=null?r.body().string():"";
                if(!r.isSuccessful()){main.post(()->cb.markets(Collections.emptyList(),"Gamma events HTTP "+r.code()));return;}
                try{
                    JSONArray events;
                    if(body.trim().startsWith("[")) events=new JSONArray(body);
                    else {JSONObject wrapper=new JSONObject(body);events=wrapper.optJSONArray("events");if(events==null)events=new JSONArray();}
                    List<Market> out=new ArrayList<>();
                    long now=System.currentTimeMillis();
                    HashSet<String> seen=new HashSet<>();
                    for(int i=0;i<events.length();i++){
                        JSONObject ev=events.optJSONObject(i); if(ev==null)continue;
                        String evSlug=ev.optString("slug","").toLowerCase(Locale.US);
                        String evTitle=(ev.optString("title","")+" "+ev.optString("description","")).toLowerCase(Locale.US);
                        long evStart=parseTime(ev.optString("eventStartTime",ev.optString("startDate","")));
                        long evEnd=parseTime(ev.optString("endDate",ev.optString("end_date","")));
                        boolean seriesLike=evSlug.startsWith("btc-updown-5m-") || evTitle.contains("bitcoin up or down") || evTitle.contains("btc up or down");
                        if(!seriesLike)continue;
                        JSONArray markets=ev.optJSONArray("markets");
                        if(markets==null)continue;
                        for(int j=0;j<markets.length();j++){
                            JSONObject m=markets.optJSONObject(j);if(m==null)continue;
                            if(m.optBoolean("closed",false)||!m.optBoolean("active",true))continue;
                            if(m.has("acceptingOrders")&&!m.optBoolean("acceptingOrders",true))continue;
                            String q=m.optString("question",ev.optString("title","") );
                            String slug=m.optString("slug",evSlug);
                            String text=(q+" "+slug).toLowerCase(Locale.US);
                            long start=parseTime(m.optString("startDate",m.optString("start_date",""))); if(start<=0)start=evStart;
                            long end=parseTime(m.optString("endDate",m.optString("end_date",""))); if(end<=0)end=evEnd;
                            long duration=start>0&&end>start?end-start:0;
                            boolean five=duration>=4*60*1000L&&duration<=6*60*1000L || text.contains("5 min")||text.contains("5-min")||text.contains("5 minute")||text.contains("5-minute")||text.matches(".*\\b5m\\b.*");
                            if(!five||start<=0||end<=now)continue;
                            // Only select the currently active window. Future pre-listed windows
                            // are useful for discovery later, but should not replace the live one.
                            if(now<start)continue;
                            String raw=m.optString("clobTokenIds",""); JSONArray ids=null;
                            if(!raw.isEmpty())try{ids=new JSONArray(raw);}catch(Exception ignored){}
                            if(ids==null)ids=m.optJSONArray("clobTokenIds");
                            if(ids==null||ids.length()<2)continue;
                            String up=ids.optString(0),down=ids.optString(1);if(up.isEmpty()||down.isEmpty())continue;
                            String id=m.optString("id",slug);if(seen.contains(id))continue;seen.add(id);
                            double target=parseTarget(q+" "+m.optString("description",""));
                            out.add(new Market(id,q,up,down,start,end,target));
                        }
                    }
                    Collections.sort(out,(a,b)->Long.compare(a.endMs,b.endMs));
                    main.post(()->cb.markets(out,""));
                }catch(Exception e){main.post(()->cb.markets(Collections.emptyList(),"Invalid Gamma events response: "+e.getMessage()));}
            }
        });
    }

    public void fetchOrderBook(String tokenId, BookCallback cb){
        Request r=new Request.Builder().url("https://clob.polymarket.com/book?token_id="+tokenId).build();
        http.newCall(r).enqueue(new okhttp3.Callback(){
            public void onFailure(Call c,IOException e){main.post(()->cb.book(null,e.getClass().getSimpleName()+": "+e.getMessage()));}
            public void onResponse(Call c,Response r)throws IOException{
                String body=r.body()!=null?r.body().string():"";
                if(!r.isSuccessful()){main.post(()->cb.book(null,"HTTP "+r.code()+" "+body));return;}
                try{
                    JSONObject o=new JSONObject(body); OrderBookInfo info=new OrderBookInfo();
                    info.tickSize=o.optDouble("tick_size",0.01); info.minOrderSize=o.optDouble("min_order_size",1); info.negRisk=o.optBoolean("neg_risk",false);
                    JSONArray asks=o.optJSONArray("asks"), bids=o.optJSONArray("bids");
                    if(asks!=null) for(int i=0;i<asks.length();i++){JSONObject x=asks.optJSONObject(i);if(x!=null)info.asks.add(new double[]{x.optDouble("price",-1),x.optDouble("size",0)});}
                    if(bids!=null) for(int i=0;i<bids.length();i++){JSONObject x=bids.optJSONObject(i);if(x!=null)info.bids.add(new double[]{x.optDouble("price",-1),x.optDouble("size",0)});}
                    main.post(()->cb.book(info,""));
                }catch(Exception e){main.post(()->cb.book(null,"Invalid book response: "+e.getMessage()));}
            }
        });
    }

    public void fetchSpotBtc(SpotCallback cb){
        Request r=new Request.Builder()
                .url("https://api.exchange.coinbase.com/products/BTC-USD/ticker")
                .header("User-Agent","PolymarketClient/0.8.8.2")
                .header("Accept","application/json").build();
        http.newCall(r).enqueue(new okhttp3.Callback(){
            public void onFailure(Call c,IOException e){ fetchSpotBtcFallback(cb); }
            public void onResponse(Call c,Response r)throws IOException{
                String b=r.body()!=null?r.body().string():"";
                try{
                    double p=new JSONObject(b).optDouble("price",-1);
                    if(r.isSuccessful()&&p>0) main.post(()->cb.price(p,"coinbase"));
                    else fetchSpotBtcFallback(cb);
                }catch(Exception e){ fetchSpotBtcFallback(cb); }
            }
        });
    }
    private void fetchSpotBtcFallback(SpotCallback cb){
        Request r=new Request.Builder()
                .url("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
                .header("User-Agent","PolymarketClient/0.8.8.2")
                .header("Accept","application/json").build();
        http.newCall(r).enqueue(new okhttp3.Callback(){
            public void onFailure(Call c,IOException e){ main.post(()->cb.price(-1,"fallback")); }
            public void onResponse(Call c,Response r)throws IOException{
                String b=r.body()!=null?r.body().string():"";
                try{
                    double p=new JSONObject(b).optDouble("price",-1);
                    main.post(()->cb.price(p,"binance"));
                }catch(Exception e){main.post(()->cb.price(-1,"fallback"));}
            }
        });
    }

    public void connectCryptoBtcFeed(CryptoCallback cb){
        disconnectCryptoBtcFeed();
        Request r=new Request.Builder().url("wss://ws-live-data.polymarket.com").build();
        cryptoSocket=http.newWebSocket(r,new WebSocketListener(){
            public void onOpen(WebSocket ws,Response response){
                try{
                    JSONObject sub=new JSONObject(); sub.put("action","subscribe"); JSONArray subs=new JSONArray();
                    JSONObject chain=new JSONObject(); chain.put("topic","crypto_prices_chainlink"); chain.put("type","*"); chain.put("filters", "{\"symbol\":\"btc/usd\"}"); subs.put(chain);
                    JSONObject binance=new JSONObject(); binance.put("topic","crypto_prices"); binance.put("type","update"); binance.put("filters","btcusdt"); subs.put(binance);
                    sub.put("subscriptions",subs); ws.send(sub.toString());
                    cryptoHeartbeat=new Runnable(){public void run(){try{if(cryptoSocket!=null)cryptoSocket.send("PING");}catch(Exception ignored){}main.postDelayed(this,5000);}};
                    main.postDelayed(cryptoHeartbeat,5000);
                }catch(Exception e){main.post(()->cb.price(-1,"RTDS",e.getMessage()));}
            }
            public void onMessage(WebSocket ws,String text){
                if("PONG".equalsIgnoreCase(text.trim()))return;
                try{
                    JSONObject o=new JSONObject(text); String topic=o.optString("topic",""); JSONObject p=o.optJSONObject("payload"); if(p==null)return;
                    double v=p.optDouble("value",-1);
                    String symbol=p.optString("symbol","");
                    if(v<=0){String f=p.optString("full_accuracy_value","");try{v=Double.parseDouble(f);}catch(Exception ignored){}}
                    if(v<=0){JSONArray data=p.optJSONArray("data");if(data!=null&&data.length()>0){JSONObject last=data.optJSONObject(data.length()-1);if(last!=null){v=last.optDouble("value",-1);symbol=last.optString("symbol",symbol);}}}
                    if(topic.contains("chainlink") && !symbol.isEmpty() && !"btc/usd".equalsIgnoreCase(symbol))return;
                    if(v>0){ final double value=v; main.post(()->cb.price(value,topic,"")); }
                }catch(Exception ignored){}
            }
            public void onClosed(WebSocket ws,int code,String reason){stopCryptoHeartbeat();main.post(()->cb.price(-1,"RTDS","closed "+code+" "+reason));}
            public void onFailure(WebSocket ws,Throwable t,Response r){stopCryptoHeartbeat();main.post(()->cb.price(-1,"RTDS",t.getClass().getSimpleName()+": "+t.getMessage()));}
        });
    }
    public void disconnectCryptoBtcFeed(){stopCryptoHeartbeat();if(cryptoSocket!=null){cryptoSocket.close(1000,"user");cryptoSocket=null;}}
    private void stopCryptoHeartbeat(){if(cryptoHeartbeat!=null){main.removeCallbacks(cryptoHeartbeat);cryptoHeartbeat=null;}}

    public void connectMarketFeed(String yes,String no,FeedCallback cb){
        disconnectMarketFeed(); activeFeed=cb;
        Request r=new Request.Builder().url("wss://ws-subscriptions-clob.polymarket.com/ws/market").build();
        socket=http.newWebSocket(r,new WebSocketListener(){
            public void onOpen(WebSocket ws,Response response){
                try{
                    JSONObject sub=new JSONObject();sub.put("type","market");
                    JSONArray ids=new JSONArray();if(yes!=null&&!yes.isEmpty())ids.put(yes);if(no!=null&&!no.isEmpty())ids.put(no);
                    sub.put("assets_ids",ids);sub.put("initial_dump",true);sub.put("custom_feature_enabled",true);ws.send(sub.toString());
                    post(cb,new FeedTick("CONNECTED","","","","","SUBSCRIBED "+ids.length()+" TOKEN(S)"));
                    if(heartbeat!=null)main.removeCallbacks(heartbeat);
                    heartbeat=new Runnable(){public void run(){try{if(socket!=null)socket.send("PING");}catch(Exception ignored){}main.postDelayed(this,10000);}};
                    main.postDelayed(heartbeat,10000);
                }catch(Exception e){post(cb,new FeedTick("ERROR","","","","","SUBSCRIPTION ERROR: "+e.getMessage()));}
            }
            public void onMessage(WebSocket ws,String text){if("PONG".equalsIgnoreCase(text.trim()))return;parseMessages(text,cb);}
            public void onClosed(WebSocket ws,int code,String reason){stopHeartbeat();post(cb,new FeedTick("DISCONNECTED","","","","","FEED CLOSED "+code+" "+reason));}
            public void onFailure(WebSocket ws,Throwable t,Response r){stopHeartbeat();post(cb,new FeedTick("ERROR","","","","","WEBSOCKET "+t.getClass().getSimpleName()+": "+t.getMessage()));}
        });
    }

    private void parseMessages(String text,FeedCallback cb){try{if(text.trim().startsWith("[")){JSONArray a=new JSONArray(text);for(int i=0;i<a.length();i++)parseObject(a.optJSONObject(i),cb,text);}else parseObject(new JSONObject(text),cb,text);}catch(Exception e){post(cb,new FeedTick("RAW","","","","",text));}}
    private void parseObject(JSONObject o,FeedCallback cb,String raw){
        if(o==null)return;
        String type=o.optString("event_type",o.optString("type","update"));
        String asset=o.optString("asset_id",o.optString("token_id",""));
        String bid=o.optString("best_bid",o.optString("bid","")),ask=o.optString("best_ask",o.optString("ask","")),last=o.optString("price",o.optString("last_trade_price",""));
        FeedTick t=new FeedTick(type,asset,bid,ask,last,raw);
        if(type.equals("book")){
            fillBook(t,o.optJSONArray("bids"),true);fillBook(t,o.optJSONArray("asks"),false);
            if(t.bestBid.isEmpty())t.bestBid=bestFrom(t.bids,true);if(t.bestAsk.isEmpty())t.bestAsk=bestFrom(t.asks,false);
            t.lastPrice=o.optString("last_trade_price",last);
        } else if(type.equals("price_change")){
            JSONArray changes=o.optJSONArray("price_changes"); if(changes==null) changes=o.optJSONArray("priceChanges");
            if(changes!=null){for(int i=0;i<changes.length();i++){JSONObject c=changes.optJSONObject(i);if(c!=null){FeedTick x=new FeedTick(type,c.optString("asset_id",c.optString("tokenId",asset)),c.optString("best_bid",c.optString("bestBid",bid)),c.optString("best_ask",c.optString("bestAsk",ask)),c.optString("price",last),raw);post(cb,x);} }return;}
        } else if(type.equals("best_bid_ask")){
            t.bestBid=o.optString("best_bid",o.optString("bestBid",bid));t.bestAsk=o.optString("best_ask",o.optString("bestAsk",ask));
        } else if(type.equals("last_trade_price")){
            t.lastPrice=o.optString("price",last);
        }
        post(cb,t);
    }
    private void fillBook(FeedTick t,JSONArray a,boolean bid){if(a==null)return;for(int i=0;i<Math.min(20,a.length());i++){JSONObject x=a.optJSONObject(i);if(x!=null){String p=x.optString("price","");String s=x.optString("size",x.optString("quantity",""));(bid?t.bids:t.asks).add(new String[]{p,s});}}}
    private String bestFrom(List<String[]> a,boolean max){double v=max?-1:Double.MAX_VALUE;String s="";for(String[] x:a)try{double d=Double.parseDouble(x[0]);if((max&&d>v)||(!max&&d<v)){v=d;s=x[0];}}catch(Exception ignored){}return s;}
    private double parseTarget(String q){java.util.regex.Matcher m=java.util.regex.Pattern.compile("\\$\\s*([0-9][0-9,]*(?:\\.[0-9]+)?)").matcher(q);if(m.find())try{return Double.parseDouble(m.group(1).replace(",",""));}catch(Exception ignored){}return -1;}
    private long parseTime(String s){
        if(s==null||s.trim().isEmpty())return 0;
        String x=s.trim();
        try{if(x.matches("\\d+")){long n=Long.parseLong(x);return n<100000000000L?n*1000L:n;}}catch(Exception ignored){}
        try{return java.time.Instant.parse(x).toEpochMilli();}catch(Exception e){return 0;}
    }
    public void disconnectMarketFeed(){stopHeartbeat();if(socket!=null){socket.close(1000,"user");socket=null;}activeFeed=null;}
    private void stopHeartbeat(){if(heartbeat!=null){main.removeCallbacks(heartbeat);heartbeat=null;}}
    private void get(String url,Callback cb,String label){Request r=new Request.Builder().url(url).build();http.newCall(r).enqueue(new okhttp3.Callback(){public void onFailure(Call c,IOException e){finish(cb,false,label+" error: "+e.getClass().getSimpleName()+": "+e.getMessage());}public void onResponse(Call c,Response r)throws IOException{String b=r.body()!=null?r.body().string():"";finish(cb,r.isSuccessful(),label+(r.isSuccessful()?" CONNECTED":" HTTP "+r.code())+(b.isEmpty()?"":" | RESPONSE OK"));}});}
    private void finish(Callback cb,boolean ok,String msg){main.post(()->cb.done(ok,msg));}private void post(FeedCallback cb,FeedTick t){main.post(()->cb.update(t));}
}
