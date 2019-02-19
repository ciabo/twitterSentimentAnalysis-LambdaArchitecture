package fastlayer.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SentimentRepository {
    private static final String TABLE_NAME = "sentimentCount";
    private Session session;

    public SentimentRepository(Session session) {
        this.session = session;
    }

    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("keyword text, ")
                .append("sentiment int,")
                .append("count int,")
                .append("PRIMARY KEY (keyword, sentiment) );");
        final String query = sb.toString();
        session.execute(query);
    }

    public int selectCountFromKey(String keyword, int sentiment) {
        StringBuilder sb = new StringBuilder("SELECT count FROM ")
                .append(TABLE_NAME)
                .append(" WHERE keyword = '")
                .append(keyword)
                .append("' AND sentiment = ")
                .append(sentiment)
                .append(";");
        final String query = sb.toString();
        ResultSet rs = session.execute(query);
        Row r = rs.one();
        return r.getInt("count");
    }

    public void updateCount(String keyword, int sentiment, int newCount) {
        StringBuilder sb = new StringBuilder("UPDATE ")
                .append(TABLE_NAME)
                .append(" SET count = ")
                .append(newCount)
                .append(" WHERE keyword = '")
                .append(keyword)
                .append("' AND sentiment = ")
                .append(sentiment)
                .append(";");
        final String query = sb.toString();
        session.execute(query);
    }

    public void deleteTable() {
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(TABLE_NAME);

        final String query = sb.toString();
        session.execute(query);
    }
}
