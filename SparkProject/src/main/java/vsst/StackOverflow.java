package vsst;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Objects;
import java.util.regex.Pattern;

public final class StackOverflow {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String QUESTIONS_PATH = "/datasets/2021/stackoverflow-sample/Questions.csv";
    private static final String ANSWERS_PATH = "/datasets/2021/stackoverflow-sample/Answers.csv";
    private static final String TAGS_PATH = "/datasets/2021/stackoverflow-sample/Tags.csv";

    private static Tuple2<Integer, Question> parseQuestion(String line) {
        Question question = Question.fromCsv(line);
        if (question == null) {
            return null;
        }
        return new Tuple2<>(question.id, question);
    }

    private static Tuple2<Integer, Tag> parseTag(String line) {
        Tag tag = Tag.fromCsv(line);
        return new Tuple2<>(tag.questionId, tag);
    }

    private static Tuple2<Integer, Answer> parseAnswer(String line) {
        Answer answer = Answer.fromCsv(line);
        if (answer == null) {
            return null;
        }
        return new Tuple2<>(answer.questionId, answer);
    }

    private static <Key, Value> void debug(JavaPairRDD<Key, Value> pairs) {
        pairs.foreach((keyValue) -> System.out.println(keyValue._1() + " -> " + keyValue._2()));
    }

    private static JavaRDD<String> skipCsvHeader(JavaRDD<String> data) {
        String header = data.first();
        return data.filter((line) -> !Objects.equals(line, header));
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: StackOverflow <output directory>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("StackOverflow").getOrCreate();


        JavaRDD<String> rawQuestions = skipCsvHeader(spark.read().textFile(QUESTIONS_PATH).javaRDD());
        JavaRDD<String> rawAnswers = skipCsvHeader(spark.read().textFile(ANSWERS_PATH).javaRDD());
        JavaRDD<String> rawTags = skipCsvHeader(spark.read().textFile(TAGS_PATH).javaRDD());

        JavaPairRDD<Integer, Question> questions = rawQuestions.mapToPair(StackOverflow::parseQuestion).filter(Objects::nonNull);
        JavaPairRDD<Integer, Answer> answers = rawAnswers.mapToPair(StackOverflow::parseAnswer).filter(Objects::nonNull);
        JavaPairRDD<Integer, Tag> tags = rawTags.mapToPair(StackOverflow::parseTag);

        StackOverflow.debug(tags);

        spark.stop();
    }
}
