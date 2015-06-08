package common;

public class WhereEquals {
    private String field;
    private String value;

    public WhereEquals(String field, String value) {
        this.field = field;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public String getValue() {
        return value;
    }
}
