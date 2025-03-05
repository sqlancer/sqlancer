package sqlancer.postgres.gen;

public class PostgresExplainGenerator {

    private PostgresExplainGenerator(){

    }
    
    public static String explain(String selectStr) throws Exception{
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        sb.append(selectStr);
        return sb.toString();
    }

}
