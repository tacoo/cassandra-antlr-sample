package tac.cassandra.antlr;

import org.antlr.runtime.ANTLRFileStream;
import org.antlr.runtime.CommonTokenStream;
import org.apache.cassandra.cql3.CqlLexer;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;

public class Main {
    public static void main(String[] args) throws Exception {

        ANTLRFileStream fileStream = new ANTLRFileStream("test.cql", "utf8");
        CqlLexer lexer = new CqlLexer(fileStream);
        CommonTokenStream token = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(token);
        ParsedStatement query = parser.query();
        if (query.getClass().getDeclaringClass() == CreateTableStatement.class) {
            CreateTableStatement.RawStatement cts = (CreateTableStatement.RawStatement) query;
            Prepared prepared = cts.prepare();
            CreateTableStatement cts2 = (CreateTableStatement) prepared.statement;
            cts2.getCFMetaData()
                    .getColumnMetadata()
                    .values()
                    .stream()
                    .forEach(cd -> System.out.println(cd));
        }
    }
}
