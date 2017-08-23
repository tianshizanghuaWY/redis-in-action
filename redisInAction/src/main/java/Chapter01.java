import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter01 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static void main(String[] args) {
        new Chapter01().run();
        //new Chapter01().test();
    }

    public void test(){
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        conn.set("wy", "goood");
        System.out.println(conn.get("wy"));
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        //发布一篇文章
        String articleId = postArticle(
            conn, "username", "A title", "http://www.google.com");

        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String,String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String,String> entry : articleData.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();

        //投票
        articleVote(conn, "other_user", "article:" + articleId);

        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;
        System.out.println("The currently highest-scoring articles are:");

        //分页获取 "热" 文章
        List<Map<String,String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;

        //将文章添加到 分组中
        addGroups(conn, articleId, new String[]{"new-group"});
        System.out.println("We added the article to a new group, other articles include:");

        //获得分组下的文章
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /*
     * 发布一篇文章
     * 递增获得文章id， 并返回
     * 使用 "voted:" + id 集合来保存投票人信息
     * 使用 "article:" + id 散列来保存文章信息：标题，链接，作者，票数
     * 使用 "score:" + id 有序队列来保存文章的综合排序热度
     * 使用 "time:" + id 有序队列来保存文章的最近更新热度
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        //递增获得"文章"记录的ID ？
        String articleId = String.valueOf(conn.incr("article:"));

        //记录文章投票的人，并设置一星期后过期
        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String,String> articleData = new HashMap<String,String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");

        conn.hmset(article, articleData); //保存点赞信息
        conn.zadd("score:", now + VOTE_SCORE, article);//文章的热度排序 - 综合
        conn.zadd("time:", now, article);                    //文章时间热度排序

        return articleId;
    }

    /*
     * 投票
     */
    public void articleVote(Jedis conn, String user, String article) {
        //一星期之前的文章不再投票
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        if (conn.zscore("time:", article) < cutoff){
            return;
        }

        String articleId = article.substring(article.indexOf(':') + 1);
        if (conn.sadd("voted:" + articleId, user) == 1) {
            //保存投票人成功后，递增排序热度，增加投票数
            conn.zincrby("score:", VOTE_SCORE, article);
            conn.hincrBy(article, "votes", 1l);
        }
    }


    public List<Map<String,String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    /*
     * 按照排序规则分页获取文章
     * order： 排序规则： "score:" "time:"
     */
    public List<Map<String,String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        //排序后分页获取文章 ID
        Set<String> ids = conn.zrevrange(order, start, end);
        List<Map<String,String>> articles = new ArrayList<Map<String,String>>();
        for (String id : ids){
            Map<String,String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }

    /*
     * 给文章分类
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    /*
     * 获得分组下的文章
     */
    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        String key = order + group;
        if (!conn.exists(key)) {
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }

    private void printArticles(List<Map<String,String>> articles){
        for (Map<String,String> article : articles){
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String,String> entry : article.entrySet()){
                if (entry.getKey().equals("id")){
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
