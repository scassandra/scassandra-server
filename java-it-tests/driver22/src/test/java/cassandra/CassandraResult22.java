package cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import common.CassandraResult;
import common.CassandraRow;

import java.util.List;
import java.util.stream.Collectors;

public class CassandraResult22 implements CassandraResult {

    private ResultSet resultSet;
    private ResponseStatus result;

    public CassandraResult22(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.result = new SuccessStatus();
    }

    public CassandraResult22(ResponseStatus result) {
        this.result = result;
    }

    @Override
    public List<CassandraRow> rows() {
        List<Row> all = resultSet.all();
        return all.stream().map(CassandraRow22::new).collect(Collectors.toList());
    }

    @Override
    public ResponseStatus status() {
        return result;
    }
}
