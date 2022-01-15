package vsst;

import org.apache.commons.collections.ListUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private static Question reduceTags(Question lhs, Question rhs) {
        return new Question(lhs.id, lhs.creationDate, lhs.answers, new ArrayList<Tag>(ListUtils.union(lhs.tags, rhs.tags)));
    }

    private static Question reduceAnswers(Question lhs, Question rhs) {
        return new Question(lhs.id, lhs.creationDate, new ArrayList<Answer>(ListUtils.union(lhs.answers, rhs.answers)), lhs.tags);
    }

    private static JavaPairRDD<Tag, Duration> calculateMeanTimeForAnswer(JavaPairRDD<Integer, Question> questions) {
        class CreationDates {
            public CreationDates(OffsetDateTime question, List<OffsetDateTime> answers) {
                this.question = question;
                this.answers = answers;
            }

            OffsetDateTime question;
            List<OffsetDateTime> answers;

            @Override
            public String toString() {
                return "CreationDates{" +
                        "question=" + question +
                        ", answers=" + answers +
                        '}';
            }
        }
        JavaPairRDD<Tag, CreationDates> tagsToDates = questions
                .values()
                .flatMapToPair(question -> question.tags.stream()
                        .map(tag ->
                                new Tuple2<>(tag,
                                        new CreationDates(question.creationDate,
                                                question.answers.stream().map(answer -> answer.creationDate)
                                                        .collect(Collectors.toList())))).iterator());
        debug(tagsToDates);
        return null;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: StackOverflow <output directory>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("StackOverflow")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();


        JavaRDD<String> rawQuestions = skipCsvHeader(spark.read().textFile(QUESTIONS_PATH).javaRDD());
        JavaRDD<String> rawAnswers = skipCsvHeader(spark.read().textFile(ANSWERS_PATH).javaRDD());
        JavaRDD<String> rawTags = skipCsvHeader(spark.read().textFile(TAGS_PATH).javaRDD());

        JavaPairRDD<Integer, Question> questions = rawQuestions.mapToPair(StackOverflow::parseQuestion).filter(Objects::nonNull);
        JavaPairRDD<Integer, Answer> answers = rawAnswers.mapToPair(StackOverflow::parseAnswer).filter(Objects::nonNull);
        JavaPairRDD<Integer, Tag> tags = rawTags.mapToPair(StackOverflow::parseTag);

        JavaPairRDD<Integer, Question> questionsWithTags = questions
                .leftOuterJoin(tags)
                .filter((questionWithTag) -> questionWithTag._2()._2().isPresent())
                .mapValues(questionWithTag ->
                        new Question(questionWithTag._1().id, questionWithTag._1().creationDate,
                                questionWithTag._1().answers, new ArrayList<Tag>(Collections.singletonList(questionWithTag._2().get()))))
                .reduceByKey(StackOverflow::reduceTags);

        JavaPairRDD<Integer, Question> fullQuestions = questionsWithTags
                .leftOuterJoin(answers)
                .filter((questionWithAnswer) -> questionWithAnswer._2()._2().isPresent())
                .mapValues(questionWithAnswer ->
                        new Question(questionWithAnswer._1().id, questionWithAnswer._1().creationDate,
                                new ArrayList<Answer>(Collections.singletonList(questionWithAnswer._2().get())), questionWithAnswer._1().tags))
                .reduceByKey(StackOverflow::reduceAnswers);


        calculateMeanTimeForAnswer(fullQuestions);

        spark.stop();
    }
}
