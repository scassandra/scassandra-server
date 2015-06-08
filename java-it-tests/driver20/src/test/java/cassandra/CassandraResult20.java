package cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import common.CassandraResult;
import common.CassandraRow;

import java.util.List;
import java.util.stream.Collectors;

public class CassandraResult20 implements CassandraResult {

    private ResultSet resultSet;
    private ResponseStatus result;

    public CassandraResult20(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.result = new SuccessStatus();
    }

    public CassandraResult20(ResponseStatus unavailable) {
        result = unavailable;
    }

    @Override
    public List<CassandraRow> rows() {
        List<Row> all = resultSet.all();
        return all.stream().map(CassandraRow20::new).collect(Collectors.toList());
    }

    @Override
    public ResponseStatus status() {
        return result;
    }
}
