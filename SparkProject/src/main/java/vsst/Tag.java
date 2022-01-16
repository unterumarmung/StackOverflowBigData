package vsst;

import java.util.Objects;

public class Tag {
    public int questionId;
    public String name;

    public static Tag fromCsv(String line) {
        String[] splitted = line.split(",");
        Tag tag = new Tag();
        tag.questionId = Integer.parseInt(splitted[0]);
        tag.name = splitted[1];
        return tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tag tag = (Tag) o;
        return Objects.equals(name, tag.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
