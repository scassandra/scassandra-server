package common;

public class WhereEquals<T> {
    private String field;
    private T value;

    public WhereEquals(String field, T value) {
        this.field = field;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public T getValue() {
        return value;
    }
}
