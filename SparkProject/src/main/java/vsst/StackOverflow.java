package vsst;

import org.apache.commons.collections.ListUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.math.BigInteger;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class StackOverflow {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String QUESTIONS_PATH = "/datasets/2021/stackoverflow-sample/Questions.csv";
    private static final String ANSWERS_PATH = "/datasets/2021/stackoverflow-sample/Answers.csv";
    private static final String TAGS_PATH = "/datasets/2021/stackoverflow-sample/Tags.csv";
    private static final Duration DAY = Duration.ofDays(1);

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


    private static JavaPairRDD<Tag, Tuple2<Long, Double>> calculateMeanTimeForAnswer(JavaPairRDD<Integer, Question> questions) {
        class CreationDates {
            OffsetDateTime question;
            List<OffsetDateTime> answers;

            public CreationDates(OffsetDateTime question, List<OffsetDateTime> answers) {
                this.question = question;
                this.answers = answers;
            }

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

        JavaPairRDD<Tag, Long> tagToAnswerTime = tagsToDates
                .mapValues(date -> date.answers.stream()
                        // find first answer for a question
                        .map(dateAnswer -> Duration.between(date.question, dateAnswer).abs()).sorted().findFirst())
                .filter(duration -> duration._2().isPresent())
                .mapValues(Optional::get)
                .mapValues(Duration::toMinutes);

        JavaPairRDD<Tag, BigInteger> totalAnswersByTag = tagToAnswerTime
                .mapValues(minutes -> BigInteger.ONE)
                .reduceByKey((lhs, rhs) -> lhs.add(rhs));

        JavaPairRDD<Tag, Long> dayTagAndTime = tagToAnswerTime
                .filter(tagAndTime -> tagAndTime._2().compareTo(DAY.toMinutes()) < 0);

        JavaPairRDD<Tag, BigInteger> dayTimeByTag = dayTagAndTime
                .mapValues(time -> BigInteger.valueOf(time))
                .reduceByKey((lhs, rhs) -> lhs.add(rhs));
        JavaPairRDD<Tag, BigInteger> dayAnswersByTag = dayTagAndTime
                .mapValues(minutes -> BigInteger.ONE)
                .reduceByKey((lhs, rhs) -> lhs.add(rhs));

        JavaPairRDD<Tag, Long> dayAnswer = dayTimeByTag.join(dayAnswersByTag)
                .mapValues(totalWithCount -> totalWithCount._1().divide(totalWithCount._2()).longValue());

        JavaPairRDD<Tag, Double> probability = dayAnswersByTag
                .join(totalAnswersByTag)
                .mapValues(counts -> counts._1().doubleValue() / counts._2().doubleValue());

        return dayAnswer.join(probability);
    }

    private static JavaPairRDD<Tag, Long> calculateTopTags(JavaPairRDD<Integer, Question> questions) {
        JavaPairRDD<Tag, Long> tagsCount = questions
                .flatMapToPair(question -> question._2().tags.stream().map(tag -> new Tuple2<>(tag, (long) 1)).iterator())
                .reduceByKey((lhs, rhs) -> lhs + rhs);

        // Sorted by value
        return tagsCount.mapToPair(Tuple2::swap).cache().sortByKey(/* ascending = */ false).mapToPair(Tuple2::swap);
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: StackOverflow <output directory>");
            System.exit(1);
        }

        String outputPath = args[0];

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
                .reduceByKey(StackOverflow::reduceAnswers).cache();

        calculateMeanTimeForAnswer(fullQuestions).saveAsTextFile(outputPath + "/" + "mean_time_answer");
        calculateTopTags(fullQuestions).saveAsTextFile(outputPath + "/" + "top_tags");

        spark.stop();
    }
}
