// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.eval;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import org.galagosearch.core.eval.RetrievalEvaluator.Document;
import org.galagosearch.core.eval.RetrievalEvaluator.Judgment;

/**
 *
 * @author trevor
 */
public class Main {
    /**
     * Loads a TREC judgments file.
     *
     * @param filename The filename of the judgments file to load.
     * @return Maps from query numbers to lists of judgments for each query.
     */
    public static TreeMap<String, ArrayList<Judgment>> loadJudgments(String filename) throws IOException, FileNotFoundException {
        // open file
        BufferedReader in = new BufferedReader(new FileReader(filename));
        String line = null;
        TreeMap<String, ArrayList<Judgment>> judgments = new TreeMap<String, ArrayList<Judgment>>();
        String recentQuery = null;
        ArrayList<Judgment> recentJudgments = null;

        while ((line = in.readLine()) != null) {
            int[] columns = splits(line, 4);

            String number = line.substring(columns[0], columns[1]);
            String unused = line.substring(columns[2], columns[3]);
            String docno = line.substring(columns[4], columns[5]);
            String judgment = line.substring(columns[6]);

            Judgment j = new Judgment(docno, Integer.valueOf(judgment));

            if (recentQuery == null || !recentQuery.equals(number)) {
                if (!judgments.containsKey(number)) {
                    judgments.put(number, new ArrayList<Judgment>());
                }

                recentJudgments = judgments.get(number);
                recentQuery = number;
            }

            recentJudgments.add(j);
        }

        in.close();
        return judgments;
    }

    private static int[] splits(String s, int columns) {
        int[] result = new int[2 * columns];
        boolean lastWs = true;
        int column = 0;
        result[0] = 0;

        for (int i = 0; i < s.length() && column < columns; i++) {
            char c = s.charAt(i);
            boolean isWs = (c == ' ') || (c == '\t');

            if (!isWs && lastWs) {
                result[2 * column] = i;
            } else if (isWs && !lastWs) {
                result[2 * column + 1] = i;
                column++;
            }

            lastWs = isWs;
        }

        return result;
    }

    /**
     * Reads in a TREC ranking file.
     *
     * @param filename The filename of the ranking file.
     * @return A map from query numbers to document ranking lists.
     */
    public static TreeMap<String, ArrayList<Document>> loadRanking(String filename) throws IOException, FileNotFoundException {
        // open file
        BufferedReader in = new BufferedReader(new FileReader(filename), 256 * 1024);
        String line = null;
        TreeMap<String, ArrayList<Document>> ranking = new TreeMap<String, ArrayList<Document>>();
        ArrayList<Document> recentRanking = null;
        String recentQuery = null;

        while ((line = in.readLine()) != null) {
            int[] splits = splits(line, 6);

            // 1 Q0 WSJ880711-0086 39 -3.05948 Exp

            String number = line.substring(splits[0], splits[1]);
            String unused = line.substring(splits[2], splits[3]);
            String docno = line.substring(splits[4], splits[5]);
            String rank = line.substring(splits[6], splits[7]);
            String score = line.substring(splits[8], splits[9]);
            String runtag = line.substring(splits[10]);

            Document document = new Document(docno, Integer.valueOf(rank), Double.valueOf(score));

            if (recentQuery == null || !recentQuery.equals(number)) {
                if (!ranking.containsKey(number)) {
                    ranking.put(number, new ArrayList<Document>());
                }

                recentQuery = number;
                recentRanking = ranking.get(number);
            }

            recentRanking.add(document);
        }

        in.close();
        return ranking;
    }

    /**
     * Creates a SetRetrievalEvaluator from data from loadRanking and loadJudgments.
     */
    public static SetRetrievalEvaluator create(TreeMap<String, ArrayList<Document>> allRankings, TreeMap<String, ArrayList<Judgment>> allJudgments) {
        TreeMap<String, RetrievalEvaluator> evaluators = new TreeMap<String, RetrievalEvaluator>();

        for (String query : allRankings.keySet()) {
            ArrayList<Judgment> judgments = allJudgments.get(query);
            ArrayList<Document> ranking = allRankings.get(query);

            if (judgments == null || ranking == null) {
                continue;
            }

            RetrievalEvaluator evaluator = new RetrievalEvaluator(query, ranking, judgments);
            evaluators.put(query, evaluator);
        }

        return new SetRetrievalEvaluator(evaluators.values());
    }

    /**
     * When run as a standalone application, this returns output 
     * very similar to that of trec_eval.  The first argument is 
     * the ranking file, and the second argument is the judgments
     * file, both in standard TREC format.
     */
    public static void singleEvaluation(SetRetrievalEvaluator setEvaluator) {
        String formatString = "%2$-16s%1$3s ";

        // print trec_eval relational-style output
        for (RetrievalEvaluator evaluator : setEvaluator.getEvaluators()) {
            String query = evaluator.queryName();

            // counts
            System.out.format(formatString + "%3$d\n", query, "num_ret", evaluator.
                              retrievedDocuments().size());
            System.out.format(formatString + "%3$d\n", query, "num_rel", evaluator.relevantDocuments().
                              size());
            System.out.format(formatString + "%3$d\n", query, "num_rel_ret", evaluator.
                              relevantRetrievedDocuments().size());

            // aggregate measures
            System.out.format(formatString + "%3$6.4f\n", query, "map", evaluator.averagePrecision());
            System.out.format(formatString + "%3$6.4f\n", query, "ndcg", evaluator.
                              normalizedDiscountedCumulativeGain());
            System.out.format(formatString + "%3$6.4f\n", query, "ndcg15", evaluator.
                              normalizedDiscountedCumulativeGain(15));
            System.out.format(formatString + "%3$6.4f\n", query, "R-prec", evaluator.rPrecision());
            System.out.format(formatString + "%3$6.4f\n", query, "bpref",
                              evaluator.binaryPreference());
            System.out.format(formatString + "%3$6.4f\n", query, "recip_rank", evaluator.
                              reciprocalRank());

            // precision at fixed points
            int[] fixedPoints = {5, 10, 15, 20, 30, 100, 200, 500, 1000};

            for (int i = 0; i < fixedPoints.length; i++) {
                int point = fixedPoints[i];
                System.out.format(formatString + "%3$6.4f\n", query, "P" + point, evaluator.
                                  precision(fixedPoints[i]));
            }
        }

        // print summary data
        System.out.format(formatString + "%3$d\n", "all", "num_ret", setEvaluator.numberRetrieved());
        System.out.format(formatString + "%3$d\n", "all", "num_rel", setEvaluator.numberRelevant());
        System.out.format(formatString + "%3$d\n", "all", "num_rel_ret", setEvaluator.
                          numberRelevantRetrieved());

        System.out.format(formatString + "%3$6.4f\n", "all", "map", setEvaluator.
                          meanAveragePrecision());
        System.out.format(formatString + "%3$6.4f\n", "all", "ndcg", setEvaluator.
                          meanNormalizedDiscountedCumulativeGain());
        System.out.format(formatString + "%3$6.4f\n", "all", "ndcg15", setEvaluator.
                          meanNormalizedDiscountedCumulativeGain(15));
        System.out.format(formatString + "%3$6.4f\n", "all", "R-prec", setEvaluator.meanRPrecision());
        System.out.format(formatString + "%3$6.4f\n", "all", "bpref", setEvaluator.
                          meanBinaryPreference());
        System.out.format(formatString + "%3$6.4f\n", "all", "recip_rank", setEvaluator.
                          meanReciprocalRank());

        // precision at fixed points
        int[] fixedPoints = {5, 10, 15, 20, 30, 100, 200, 500, 1000};

        for (int i = 0; i < fixedPoints.length; i++) {
            int point = fixedPoints[i];
            System.out.format(formatString + "%3$6.4f\n", "all", "P" + point, setEvaluator.
                              meanPrecision(fixedPoints[i]));
        }
    }

    /**
     * Compares two ranked lists with statistical tests on most major metrics.
     */
    public static void comparisonEvaluation(SetRetrievalEvaluator baseline, SetRetrievalEvaluator treatment, boolean useRandomized) {
        String[] metrics = {"averagePrecision", "ndcg", "ndcg15", "bpref", "P10", "P20"};
        String formatString = "%1$-20s%2$-20s%3$6.4f\n";
        String integerFormatString = "%1$-20s%2$-20s%3$d\n";

        for (String metric : metrics) {
            Map<String, Double> baselineMetric = baseline.evaluateAll(metric);
            Map<String, Double> treatmentMetric = treatment.evaluateAll(metric);

            SetRetrievalComparator comparator = new SetRetrievalComparator(baselineMetric,
                                                                           treatmentMetric);

            System.out.format(formatString, metric, "baseline", comparator.meanBaselineMetric());
            System.out.format(formatString, metric, "treatment", comparator.meanTreatmentMetric());

            System.out.format(integerFormatString, metric, "basebetter", comparator.
                              countBaselineBetter());
            System.out.format(integerFormatString, metric, "treatbetter", comparator.
                              countTreatmentBetter());
            System.out.format(integerFormatString, metric, "equal", comparator.countEqual());

            System.out.format(formatString, metric, "ttest", comparator.pairedTTest());
            System.out.format(formatString, metric, "signtest", comparator.signTest());
            if (useRandomized) {
                System.out.format(formatString, metric, "randomized", comparator.
                                                 randomizedTest());
            }
            System.out.format(formatString, metric, "h-ttest-0.05", comparator.supportedHypothesis(
                              "ttest", 0.05));
            System.out.format(formatString, metric, "h-signtest-0.05", comparator.
                              supportedHypothesis("sign", 0.05));
            if (useRandomized) {
                System.out.format(formatString, metric, "h-randomized-0.05",
                                                 comparator.supportedHypothesis("randomized", 0.05));
            }
            System.out.format(formatString, metric, "h-ttest-0.01", comparator.supportedHypothesis(
                              "ttest", 0.01));
            System.out.format(formatString, metric, "h-signtest-0.01", comparator.
                              supportedHypothesis("sign", 0.01));
            if (useRandomized) {
                System.out.format(formatString, metric, "h-randomized-0.01",
                                                 comparator.supportedHypothesis("randomized", 0.01));
            }
        }
    }

    public static void usage() {
        System.err.println("ireval: ");
        System.err.println(
                "   There are two ways to use this program.  First, you can evaluate a single ranking: ");
        System.err.println("      java -jar ireval.jar TREC-Ranking-File TREC-Judgments-File");
        System.err.println("   or, you can use it to compare two rankings with statistical tests: ");
        System.err.println(
                "      java -jar ireval.jar TREC-Baseline-Ranking-File TREC-Improved-Ranking-File TREC-Judgments-File");
        System.err.println("   you can also include randomized tests (these take a bit longer): ");
        System.err.println(
                "      java -jar ireval.jar TREC-Baseline-Ranking-File TREC-Treatment-Ranking-File TREC-Judgments-File randomized");
        System.err.println();
        System.err.println("Single evaluation:");
        System.err.println(
                "   The first column is the query number, or 'all' for a mean of the metric over all queries.");
        System.err.println(
                "   The second column is the metric, which is one of:                                        ");
        System.err.println(
                "       num_ret        Number of retrieved documents                                         ");
        System.err.println(
                "       num_rel        Number of relevant documents listed in the judgments file             ");
        System.err.println(
                "       num_rel_ret    Number of relevant retrieved documents                                ");
        System.err.println(
                "       map            Mean average precision                                                ");
        System.err.println(
                "       bpref          Bpref (binary preference)                                             ");
        System.err.println(
                "       ndcg           Normalized Discounted Cumulative Gain, computed over all documents    ");
        System.err.println(
                "       ndcg15         Normalized Discounted Cumulative Gain, 15 document cutoff             ");
        System.err.println(
                "       Pn             Precision, n document cutoff                                          ");
        System.err.println(
                "       R-prec         R-Precision                                                           ");
        System.err.println(
                "       recip_rank     Reciprocal Rank (precision at first relevant document)                ");
        System.err.println(
                "   The third column is the metric value.                                                    ");
        System.err.println();
        System.err.println("Compared evaluation: ");
        System.err.println("   The first column is the metric (e.g. averagePrecision, ndcg, etc.)");
        System.err.println(
                "   The second column is the test/formula used:                                               ");
        System.err.println(
                "       baseline       The baseline mean (mean of the metric over all baseline queries)       ");
        System.err.println(
                "       treatment      The \'improved\' mean (mean of the metric over all treatment queries)  ");
        System.err.println(
                "       basebetter     Number of queries where the baseline outperforms the treatment.        ");
        System.err.println(
                "       treatbetter    Number of queries where the treatment outperforms the baseline.        ");
        System.err.println(
                "       equal          Number of queries where the treatment and baseline perform identically.");
        System.err.println("       ttest          P-value of a paired t-test.");
        System.err.println(
                "       signtest       P-value of the Fisher sign test.                                       ");
        System.err.println(
                "       randomized      P-value of a randomized test.                                          ");

        System.err.println(
                "   The second column also includes difference tests.  In these tests, the null hypothesis is ");
        System.err.println(
                "     that the mean of the treatment is at least k times the mean of the baseline.  We run the");
        System.err.println(
                "     same tests as before, but we artificially improve the baseline values by a factor of k. ");

        System.err.println(
                "       h-ttest-0.5    Largest value of k such that the ttest has a p-value of less than 0.5. ");
        System.err.println(
                "       h-signtest-0.5 Largest value of k such that the sign test has a p-value of less than 0.5. ");
        System.err.println(
                "       h-randomized-0.5 Largest value of k such that the randomized test has a p-value of less than 0.5. ");

        System.err.println(
                "       h-ttest-0.1    Largest value of k such that the ttest has a p-value of less than 0.1. ");
        System.err.println(
                "       h-signtest-0.1 Largest value of k such that the sign test has a p-value of less than 0.1. ");
        System.err.println(
                "       h-randomized-0.1 Largest value of k such that the randomized test has a p-value of less than 0.1. ");
        System.err.println();

        System.err.println("  The third column is the value of the test.");

        System.exit(-1);
    }

    public static void main(String[] args) throws IOException {
        try {
            if (args.length >= 3) {
                TreeMap<String, ArrayList<Document>> baselineRanking = loadRanking(args[0]);
                TreeMap<String, ArrayList<Document>> treatmentRanking = loadRanking(args[1]);
                TreeMap<String, ArrayList<Judgment>> judgments = loadJudgments(args[2]);

                SetRetrievalEvaluator baseline = create(baselineRanking, judgments);
                SetRetrievalEvaluator treatment = create(treatmentRanking, judgments);

                comparisonEvaluation(baseline, treatment, args.length >= 4);
            } else if (args.length == 2) {
                TreeMap<String, ArrayList<Document>> ranking = loadRanking(args[0]);
                TreeMap<String, ArrayList<Judgment>> judgments = loadJudgments(args[1]);

                SetRetrievalEvaluator setEvaluator = create(ranking, judgments);
                singleEvaluation(setEvaluator);
            } else {
                usage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            usage();
        }
    }
}
