import redis.clients.jedis.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Chapter04 {
    public static final void main(String[] args) {
        new Chapter04().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        //testListItem(conn, false);
        //testPurchaseItem(conn);
        testBenchmarkUpdateToken(conn);
    }

    public void testListItem(Jedis conn, boolean nested) {
        if (!nested){
            System.out.println("\n----- testListItem -----");
        }

        System.out.println("We need to set up just enough state so that a user can list an item");
        String seller = "userX";
        String item = "itemX";

        //添加商品至卖家(userX)的包裹
        conn.sadd("inventory:" + seller, item);
        //获得卖家的所有商品
        Set<String> goods = conn.smembers("inventory:" + seller);

        System.out.println("The user's inventory has:");
        for (String good : goods){
            System.out.println("  " + good);
        }
        assert goods.size() > 0;
        System.out.println();

        System.out.println("Listing the item...");

        //添加商品到商场货架
        boolean success = listItem(conn, item, seller, 10);

        System.out.println("Listing the item succeeded? " + success);
        assert success;

        Set<Tuple> r = conn.zrangeWithScores("market:", 0, -1);
        System.out.println("The market contains:");
        for (Tuple tuple : r){
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert r.size() > 0;
    }

    /*
     * 购买商品
     */
    public void testPurchaseItem(Jedis conn) {
        //商品上架
        System.out.println("\n----- testPurchaseItem -----");
        testListItem(conn, true);

        //设置买家信息(钱包)
        System.out.println("We need to set up just enough state so a user can buy an item");
        conn.hset("users:userY", "funds", "125");
        Map<String,String> r = conn.hgetAll("users:userY");
        System.out.println("The user has some money:");
        for (Map.Entry<String,String> entry : r.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;
        assert r.get("funds") != null;
        System.out.println();

        System.out.println("Let's purchase an item");
        boolean p = purchaseItem(conn, "userY", "itemX", "userX", 10);
        System.out.println("Purchasing an item succeeded? " + p);
        assert p;
        r = conn.hgetAll("users:userY");
        System.out.println("Their money is now:");
        for (Map.Entry<String,String> entry : r.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;

        String buyer = "userY";
        Set<String> i = conn.smembers("inventory:" + buyer);
        System.out.println("Their inventory is now:");
        for (String member : i){
            System.out.println("  " + member);
        }
        assert i.size() > 0;
        assert i.contains("itemX");
        assert conn.zscore("market:", "itemX.userX") == null;
    }

    /*
     * 测试批处理 Redis 请求相对传统请求的效率
     */
    public void testBenchmarkUpdateToken(Jedis conn) {
        System.out.println("\n----- testBenchmarkUpdate -----");
        benchmarkUpdateToken(conn, 5);
    }

    /*
     * 将卖家商品添加至商场货架
     * 启用 WATCH 监控卖家的包裹, 如果发生变化则 continue
     * 乐观锁机制
     */
    public boolean listItem(
            final Jedis conn, final String itemId, String sellerId, double price) {

        final String inventory = "inventory:" + sellerId;
        String item = itemId + '.' + sellerId;
        long end = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < end) {
            conn.watch(inventory);
            if (!conn.sismember(inventory, itemId)){
                conn.unwatch();
                return false;
            }

            //这个为了测试 WATCH
            //测试结果：事务会被回滚，但是trans.exec() 的返回是个 sizew0 的list
            /*new Thread(){
                public void run(){
                    conn.srem(inventory, itemId);
                }
            }.start();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            Transaction trans = conn.multi();
            trans.zadd("market:", price, item);

            //这行代码是配合上面的测试的, 测试事务是不是真的会回滚
            //trans.zadd("market:", price, "something for test watch");

            trans.srem(inventory, itemId);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null){
                continue;
            }
            return true;
        }

        return false;
    }

    /*
     * 购买商品
     */
    public boolean purchaseItem(
            Jedis conn, String buyerId, String itemId, String sellerId, double lprice) {

        String buyer = "users:" + buyerId;
        String seller = "users:" + sellerId;
        String item = itemId + '.' + sellerId;
        String inventory = "inventory:" + buyerId;
        long end = System.currentTimeMillis() + 10000;

        while (System.currentTimeMillis() < end){
            conn.watch("market:", buyer);

            double price = conn.zscore("market:", item);
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));
            if (price != lprice || price > funds){
                conn.unwatch();
                return false;
            }

            Transaction trans = conn.multi();
            trans.hincrBy(seller, "funds", (int)price);
            trans.hincrBy(buyer, "funds", (int)-price);
            trans.sadd(inventory, itemId);
            trans.zrem("market:", item);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            //应该是result size 等于0时，表示事务失败并回滚
            if (results == null || results.size() <= 0){
                continue;
            }

            return true;
        }

        return false;
    }

    /*
     * 测试 2 种方式在 duration 秒内各自执行了多少次
     */
    public void benchmarkUpdateToken(Jedis conn, int duration) {
        try{
            @SuppressWarnings("rawtypes")
            Class[] args = new Class[]{
                Jedis.class, String.class, String.class, String.class};
            Method[] methods = new Method[]{
                this.getClass().getDeclaredMethod("updateToken", args),
                this.getClass().getDeclaredMethod("updateTokenPipeline", args),
            };
            for (Method method : methods){
                int count = 0;
                long start = System.currentTimeMillis();
                long end = start + (duration * 1000);
                while (System.currentTimeMillis() < end){
                    count++;
                    method.invoke(this, conn, "token", "user", "item");
                }

                //输出执行了多少次  以及 每秒钟执行多少次
                long delta = System.currentTimeMillis() - start;
                System.out.println(
                        method.getName() + ' ' +
                        count + ' ' +
                        (delta / 1000) + ' ' +
                        (count / (delta / 1000)));
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    /*
     * 传统方式 发送Redis命令
     */
    public void updateToken(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        conn.zadd("recent:", timestamp, token);
        if (item != null) {
            conn.zadd("viewed:" + token, timestamp, item);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    /*
     * 通过 pipeline 发送 redis 命令
     */
    public void updateTokenPipeline(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        Pipeline pipe = conn.pipelined();
        pipe.multi();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null){
            pipe.zadd("viewed:" + token, timestamp, item);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, item);
        }

        pipe.exec();

//        Response<List<Object>> result = pipe.exec();
//        System.out.println(result);
    }
}
