package vsst;

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
    public String toString() {
        return "Tag{" +
                "questionId=" + questionId +
                ", name='" + name + '\'' +
                '}';
    }
}
