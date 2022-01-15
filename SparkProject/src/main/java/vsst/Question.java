package vsst;

import java.time.OffsetDateTime;
import java.util.Arrays;

public class Question {
    public int id;
    public OffsetDateTime creationDate;
    public Answer[] answers;
    public Tag[] tags;

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
                ", answers=" + Arrays.toString(answers) +
                ", tags=" + Arrays.toString(tags) +
                '}';
    }
}
