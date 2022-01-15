package vsst;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;


public class Question {
    public int id;
    public OffsetDateTime creationDate;
    public List<Answer> answers = new ArrayList<>();
    public List<Tag> tags = new ArrayList<>();

    public Question() {
    }

    public Question(int id, OffsetDateTime creationDate, List<Answer> answers, List<Tag> tags) {
        this.id = id;
        this.creationDate = creationDate;
        this.answers = answers;
        this.tags = tags;
    }

    public static Question fromCsv(String line) {
        try {
            String[] splitted = line.split(",");
            Question question = new Question();
            question.id = Integer.parseInt(splitted[0]);
            question.creationDate = OffsetDateTime.parse(splitted[2]);
            return question;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "Question{" +
                "id=" + id +
                ", creationDate=" + creationDate +
                ", answers=" + answers +
                ", tags=" + tags +
                '}';
    }

}
