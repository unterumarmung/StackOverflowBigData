package vsst;


import java.time.OffsetDateTime;

public class Answer {
    public int questionId;
    public OffsetDateTime creationDate;
    public int score;

    public static Answer fromCsv(String line) {
        try {
            String[] splitted = line.split(",");
            Answer answer = new Answer();
            answer.creationDate = OffsetDateTime.parse(splitted[2]);
            answer.questionId = Integer.parseInt(splitted[3]);
            answer.score = Integer.parseInt(splitted[4]);
            return answer;
        } catch (Exception e) {
            return null;
        }

    }

    @Override
    public String toString() {
        return "Answer{" +
                "questionId=" + questionId +
                ", creationDate=" + creationDate +
                ", score=" + score +
                '}';
    }
}
