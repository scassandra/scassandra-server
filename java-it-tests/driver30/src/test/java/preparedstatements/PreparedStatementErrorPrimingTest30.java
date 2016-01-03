package preparedstatements;

import cassandra.CassandraExecutor30;
import org.junit.Ignore;

@Ignore
public class PreparedStatementErrorPrimingTest30 extends PreparedStatementErrorPrimingTest {

    public PreparedStatementErrorPrimingTest30() {
        super(new CassandraExecutor30());
    }
}
