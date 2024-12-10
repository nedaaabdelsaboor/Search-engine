import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class Main {

    public static TreeMap<String, HashMap<Integer, HashSet<Integer>>> PositionalIndex = new TreeMap<>();
    public static TreeMap<String, HashMap<Integer, HashSet<Integer>>> QueryPositionalIndex = new TreeMap<>();
    public static TreeMap<String, Integer> DF = new TreeMap<>();
    public static TreeMap<String, HashMap<Integer, Integer>> TF = new TreeMap<>();
    public static TreeMap<String, HashMap<Integer, Double>> WTF = new TreeMap<>();
    public static TreeMap<String, Double> IDF = new TreeMap<>();
    public static TreeMap<String, HashMap<Integer, Double>> TF_IDF = new TreeMap<>();
    public static TreeMap<Integer, Double> Length = new TreeMap<>();
    public static TreeMap<String, HashMap<Integer, Double>> DocNormalize = new TreeMap<>();
    public static TreeMap<String, Integer> QTF = new TreeMap<>();
    public static TreeMap<String, Double> QWTF = new TreeMap<>();
    public static TreeMap<String, Double> QTF_IDF = new TreeMap<>();
    public static Double QLength;
    public static TreeMap<String, Double> QTermsNormalize = new TreeMap<>();
    public static ArrayList<Integer> matchedDocs = new ArrayList<>();
    public static TreeMap<Integer, Double> Similarity = new TreeMap<>();


    public static void main(String[] args) throws IOException {
        String filePath = "D:/Nedaa/FCAI/IR/Project/IR_Project/mapreduce_result.txt";

        readFileAndBuildPositionalIndex(filePath);
        printPositionalIndex();
        calculateDF();
        printDF();
        calculateTF();
        printTF();
        calculateWTF();
        printWTF();
        calculateIDF();
        printIDF();
        calculateTF_IDF();
        printTF_IDF();
        calculateDocLength();
        printLength();
        calculateNormalization();
        printDocNormalization();

        Scanner in = new Scanner(System.in);
        String queryString;
        do {
            System.out.println("Enter Query:(home AND/AND NOT increase)");
            queryString = in.nextLine();
            query(queryString);
            System.out.println();
            System.out.println();
            calculateQueryTF(queryString);
            calculateQueryWTF();
            calculateQueryTF_IDF();
            calculateQueryLength(queryString);
            calculateQueryNormalization();
            printAllQueryInfo(queryString);

            calculateAndPrintProduct();
            printSimilarity();
            System.out.println("to end search: stop");
        } while (!queryString.equals("stop"));


    }

    //    Handel file input and positional index
    public static void readFileAndBuildPositionalIndex(String filePath) throws IOException {
        System.out.println(filePath);
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int lineNumber = 1;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String term = parts[0];
                    String docInfo = parts[1];

                    String[] docEntries = docInfo.split(";");
                    for (String docEntry : docEntries) {
                        String[] idAndPosition = docEntry.split(":");
                        int docIDs = Integer.parseInt(idAndPosition[0]);
                        String[] positions = idAndPosition[1].replace("[", "").replace("]", "").split(",");

                        PositionalIndex.putIfAbsent(term, new HashMap<>());
                        HashMap<Integer, HashSet<Integer>> docMap = PositionalIndex.get(term);
                        docMap.putIfAbsent(docIDs, new HashSet<>());

                        for (String position : positions) {
                            docMap.get(docIDs).add(Integer.parseInt(position));
                        }
                    }
                }
                lineNumber++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void printPositionalIndex() {
        System.out.println("\n----------Positional Index--------------");
        for (var entry : PositionalIndex.entrySet()) {
            System.out.print(entry.getKey() + "\t");
            for (var docEntry : entry.getValue().entrySet()) {
                System.out.print(docEntry.getKey() + ":" + docEntry.getValue() + "; ");
            }
            System.out.println();
        }
    }

    //    Handel Documents Calculation
    private static void calculateDF() {
        for (var entry : PositionalIndex.entrySet()) {
            String term = entry.getKey();
            HashMap<Integer, HashSet<Integer>> docMap = PositionalIndex.get(term);
            int docFreq = docMap.size();
            DF.put(term, docFreq);
        }
    }

    private static void printDF() {
        System.out.println("\n------------------DF-------------");
        System.out.printf("%-12s", "Term");
        System.out.printf("%-5s", "DF");
        System.out.println();
        for (var DFentry : DF.entrySet()) {
            System.out.printf("%-12s", DFentry.getKey());
            System.out.printf("%-5s", DFentry.getValue());
            System.out.println();
        }

    }

    private static void calculateTF() {
        for (var entry : PositionalIndex.entrySet()) {
            String term = entry.getKey();
            HashMap<Integer, HashSet<Integer>> docMap = PositionalIndex.get(term);
            HashMap<Integer, Integer> docFrequency = new HashMap<>();
            for (var docEntry : docMap.entrySet()) {
                int docID = docEntry.getKey();
                int frequency = docEntry.getValue().size();
                docFrequency.put(docID, frequency);
            }
            TF.put(term, docFrequency);
        }
    }

    private static void printTF() {
        System.out.println("\n------------------TF-------------");
        System.out.printf("%-12s", "Term");
        for (int doc = 1; doc <= 10; doc++) {
            System.out.printf("%-5s", "d" + doc);
        }
        System.out.println();
        for (String term : TF.keySet()) {
            System.out.printf("%-12s", term);
            for (int doc = 1; doc <= 10; doc++) {
                int frequency = TF.get(term).getOrDefault(doc, 0);
                System.out.printf("%-5s", frequency);
            }
            System.out.println();
        }
    }

    private static void calculateWTF() {
        for (var entry : TF.entrySet()) {
            String term = entry.getKey();
            HashMap<Integer, Double> WeightTermFrequency = new HashMap<>();
            for (var docEntry : entry.getValue().entrySet()) {
                int docID = docEntry.getKey();
                double frequency = docEntry.getValue();

                double wtf = 1 + Math.log10(frequency);
                WeightTermFrequency.put(docID, wtf);
            }
            WTF.put(term, WeightTermFrequency);
        }
    }

    private static void printWTF() {
        System.out.println("\n------------------WTF-------------");
        System.out.printf("%-12s", "Term");
        for (int doc = 1; doc <= 10; doc++) {
            System.out.printf("%-5s", "d" + doc);
        }
        System.out.println();
        for (String term : WTF.keySet()) {
            System.out.printf("%-12s", term);
            for (int doc = 1; doc <= 10; doc++) {
                Double frequency = WTF.get(term).getOrDefault(doc, 0.0);
                System.out.printf("%-5s", frequency);
            }
            System.out.println();
        }
    }

    private static void calculateIDF() {
        double idf = 0.0;
        for (var entry : DF.entrySet()) {
            String term = entry.getKey();
            for (int docId = 1; docId <= 10; docId++) {
                double frequency = entry.getValue();
                idf = +Math.log10(10 / frequency);
            }
            IDF.put(term, idf);
        }
    }

    private static void printIDF() {
        System.out.println("\n------------------IDF-------------");
        System.out.printf("%-12s", "Term");
        System.out.printf("%-5s", "DF");
        System.out.printf("%-5s", "IDF");
        System.out.println();
        for (var DFentry : DF.entrySet()) {
            System.out.printf("%-12s", DFentry.getKey());
            System.out.printf("%-5s", DFentry.getValue());
            System.out.printf("%-5s", IDF.get(DFentry.getKey()));
            System.out.println();
        }
    }

    private static void calculateTF_IDF() {
        double tf_idf = 0.0;
        for (var entry : TF.entrySet()) {
            String term = entry.getKey();
            HashMap<Integer, Integer> termFrequency = TF.get(term);
            HashMap<Integer, Double> tfidf = new HashMap<>();
            for (var docEntry : termFrequency.entrySet()) {
                int docID = docEntry.getKey();
                int frequency = docEntry.getValue();
                tf_idf = frequency * IDF.get(term);
                tfidf.put(docID, tf_idf);
            }
            TF_IDF.put(term, tfidf);
        }
    }

    private static void printTF_IDF() {
        System.out.println("\n------------------TF*IDF-------------");
        System.out.printf("%-12s", "Term");
        for (int doc = 1; doc <= 10; doc++) {
            System.out.printf("%-12s", "d" + doc);
        }
        System.out.println();
        for (String term : TF_IDF.keySet()) {
            System.out.printf("%-12s", term);
            for (int doc = 1; doc <= 10; doc++) {
                Double frequency = TF_IDF.get(term).getOrDefault(doc, 0.0);
                System.out.printf("%.5f     ", frequency);
            }
            System.out.println();
        }
    }

    private static void calculateDocLength() {
        double length = 0.0;
        for (int docId = 1; docId <= 10; docId++) {
            for (String term : TF_IDF.keySet()) {
                double frequency = TF_IDF.get(term).getOrDefault(docId, 0.0);
                length += Math.pow(frequency, 2);
            }
            double sqr = Math.sqrt(length);
            Length.put(docId, sqr);
            length = 0;
        }
    }

    private static void printLength() {
        System.out.println("\n------------------Document Length-------------");
        System.out.printf("%-12s", "Document");
        System.out.printf("%-5s", "Length");
        System.out.println();
        for (var DFentry : Length.entrySet()) {
            System.out.printf("%-12s", "Doc " + DFentry.getKey());
            System.out.printf("%.6f     ", DFentry.getValue());
            System.out.println();
        }

    }

    private static void calculateNormalization() {
        double normalize = 0.0;
        for (var entry : TF_IDF.entrySet()) {
            String term = entry.getKey();
            HashMap<Integer, Double> Weight = TF_IDF.get(term);
            HashMap<Integer, Double> normalizationMap = new HashMap<>();
            for (var docEntry : Weight.entrySet()) {
                int docID = docEntry.getKey();
                double tfidf = docEntry.getValue();
                normalize = tfidf / Length.get(docID);
                normalizationMap.put(docID, normalize);
            }
            DocNormalize.put(term, normalizationMap);
        }
    }

    private static void printDocNormalization() {
        System.out.println("\n------------------Normalization-------------");
        System.out.printf("%-12s", "Term");
        for (int doc = 1; doc <= 10; doc++) {
            System.out.printf("%-12s", "d" + doc);
        }
        System.out.println();
        for (String term : DocNormalize.keySet()) {
            System.out.printf("%-12s", term);
            for (int doc = 1; doc <= 10; doc++) {
                Double frequency = DocNormalize.get(term).getOrDefault(doc, 0.0);
                System.out.printf("%.5f     ", frequency);
            }
            System.out.println();
        }
        System.out.println();
    }

    //    Handel Query entered
    private static void query(String QueryString) {
        if (QueryString.contains("AND NOT")) {
            String[] splitQuery = QueryString.split("AND NOT");
            if (splitQuery.length != 2) {
                System.out.println("Invalid QueryString");
            }
            String firstPart = splitQuery[0].trim();
            String secondPart = splitQuery[1].trim();

            ArrayList<String> firstQueryTerms = new ArrayList<>(Arrays.asList(firstPart.split(" ")));
            TreeMap<String, HashMap<Integer, HashSet<Integer>>> firstQueryIndex = getQueryPositionalIndex(firstQueryTerms);
            HashMap<Integer, HashSet<Integer>> firstResult = intersectDoc(firstQueryTerms, firstQueryIndex);

            System.out.println("intersect first query terms : " + firstResult);

            ArrayList<String> secondQueryTerms = new ArrayList<>(Arrays.asList(secondPart.split(" ")));
            TreeMap<String, HashMap<Integer, HashSet<Integer>>> secondQueryIndex = getQueryPositionalIndex(secondQueryTerms);
            HashMap<Integer, HashSet<Integer>> secondResult = intersectDoc(secondQueryTerms, secondQueryIndex);

            System.out.println("intersect second query terms : " + secondResult);
            if (firstResult == null || firstResult.isEmpty() || secondResult == null || secondResult.isEmpty()) {
                System.out.println("No Matching Documents found: ");
            }
            ArrayList<Integer> finalMatchedDocs = new ArrayList<>(firstResult.keySet());
            finalMatchedDocs.removeAll(secondResult.keySet());
            System.out.println("Matched AND NOT Query: " + finalMatchedDocs);
            matchedDocs = finalMatchedDocs;
        } else if (QueryString.contains("AND")) {
            String[] splitQuery = QueryString.split("AND");
            if (splitQuery.length != 2) {
                System.out.println("Invalid QueryString");
            }
            String firstPart = splitQuery[0].trim();
            String secondPart = splitQuery[1].trim();

            ArrayList<String> firstQueryTerms = new ArrayList<>(Arrays.asList(firstPart.split(" ")));
            System.out.println("first query terms : " + firstQueryTerms);
            TreeMap<String, HashMap<Integer, HashSet<Integer>>> firstQueryIndex = getQueryPositionalIndex(firstQueryTerms);
            HashMap<Integer, HashSet<Integer>> firstResult = intersectDoc(firstQueryTerms, firstQueryIndex);

            System.out.println("intersect first query terms : " + firstResult);

            ArrayList<String> secondQueryTerms = new ArrayList<>(Arrays.asList(secondPart.split(" ")));
            TreeMap<String, HashMap<Integer, HashSet<Integer>>> secondQueryIndex = getQueryPositionalIndex(secondQueryTerms);
            HashMap<Integer, HashSet<Integer>> secondResult = intersectDoc(secondQueryTerms, secondQueryIndex);

            System.out.println("intersect second query terms : " + secondResult);

            if (firstResult == null || firstResult.isEmpty() || secondResult == null || secondResult.isEmpty()) {
                System.out.println("No Matching Documents found: ");
            }
            ArrayList<Integer> finalMatchedDocs = new ArrayList<>(firstResult.keySet());
            finalMatchedDocs.retainAll(secondResult.keySet());
            System.out.println("Matched AND Query: " + finalMatchedDocs);
            matchedDocs = finalMatchedDocs;

        } else {
            ArrayList<String> termList = new ArrayList<>(Arrays.asList(QueryString.split(" ")));
            TreeMap<String, HashMap<Integer, HashSet<Integer>>> queryPositionalIndex = getQueryPositionalIndex(termList);
            HashMap<Integer, HashSet<Integer>> result = intersectDoc(termList, queryPositionalIndex);
            if (result != null && !result.isEmpty()) {
                ArrayList<Integer> matched = new ArrayList<>(result.keySet());
                matchedDocs = matched;

            }
        }
    }

    private static TreeMap<String, HashMap<Integer, HashSet<Integer>>> getQueryPositionalIndex(ArrayList<String> Terms) {
        for (String term : Terms) {
            HashMap<Integer, HashSet<Integer>> TermTemp = PositionalIndex.get(term);
            if (TermTemp != null)
                QueryPositionalIndex.put(term, TermTemp);
            else
                QueryPositionalIndex.put(term, new HashMap<>());

        }
        System.out.println("Query Positional Index: " + QueryPositionalIndex);
        return QueryPositionalIndex;

    }

    private static HashMap<Integer, HashSet<Integer>> intersectDoc(ArrayList<String> terms, TreeMap<String, HashMap<Integer, HashSet<Integer>>> positionalIndex) {
        HashMap<Integer, HashSet<Integer>> resultMap = new HashMap<>();
        for (var term : terms) {
            HashMap<Integer, HashSet<Integer>> currentTermMap = positionalIndex.get(term);
            if (currentTermMap == null || currentTermMap.isEmpty()) {
                return new HashMap<>();
            }
            if (resultMap.isEmpty()) {
                resultMap.putAll(currentTermMap);
            } else {
                resultMap = intersectPositional(resultMap, currentTermMap);
            }
            if (resultMap.isEmpty()) {
                break;
            }
        }
        return resultMap;
    }

    private static HashMap<Integer, HashSet<Integer>> intersectPositional(HashMap<Integer, HashSet<Integer>> firstMap, HashMap<Integer, HashSet<Integer>> secondMap) {

        HashMap<Integer, HashSet<Integer>> resultMap = new HashMap<>();

        for (var entry : firstMap.entrySet()) {
            int docID = entry.getKey();
            HashSet<Integer> firstPositions = entry.getValue();

            if (secondMap.containsKey(docID)) {
                HashSet<Integer> secondPositions = secondMap.get(docID);
                HashSet<Integer> matchedPositions = new HashSet<>();

                for (int pos : firstPositions) {
                    if (secondPositions.contains(pos + 1)) {
                        matchedPositions.add(pos + 1);
                    }
                }

                if (!matchedPositions.isEmpty()) {
                    resultMap.put(docID, matchedPositions);
                }
            }
        }

        return resultMap;
    }

    //    Handel Query Calculation
    private static void calculateQueryTF(String queryString) {
        String[] terms = queryString.split(" ");
        for (var entry : QueryPositionalIndex.entrySet()) {
            String term = entry.getKey();
            QTF.put(term, QTF.getOrDefault(term, 0) + 1);
        }
    }

    private static void calculateQueryWTF() {
        for (var entry : QTF.entrySet()) {
            String term = entry.getKey();
            int frequency = entry.getValue();
            double weight = 1 + Math.log(frequency);
            QWTF.put(term, weight);
        }
    }

    private static void calculateQueryTF_IDF() {
        double tf_idf = 0.0;
        for (var entry : QTF.entrySet()) {
            String term = entry.getKey();
            Integer termFrequency = QTF.get(term);
            tf_idf = termFrequency * IDF.get(term);
            QTF_IDF.put(term, tf_idf);
        }
    }

    private static void calculateQueryLength(String queryString) {
        double length = 0.0;
        for (String term : QueryPositionalIndex.keySet()) {
            if (IDF.containsKey(term) && QTF.containsKey(term)) {
                double TF_IDF = IDF.get(term) * QTF.get(term);
                length += Math.pow(TF_IDF, 2);
            } else {
                System.out.println("Warning: Term \"" + term + "\" not found in IDF or QTF.");
            }
        }
        QLength = Math.sqrt(length);
    }

    private static void calculateQueryNormalization() {
        double normalize = 0.0;
        for (var entry : QTF_IDF.entrySet()) {
            String term = entry.getKey();
            Double weight = QTF_IDF.get(term);
            normalize = weight / QLength;
            QTermsNormalize.put(term, normalize);
        }
    }

    private static void printAllQueryInfo(String queryString) {
        System.out.println("\n------------------Query information-------------");
        System.out.printf("%-12s", "Term");
        System.out.printf("%-5s", "TF");
        System.out.printf("%-5s ", "QWTF");
        System.out.printf("%-5s     ", "QIDF");
        System.out.printf("%-5s     ", "QTF*IDF");
        System.out.printf("%-5s", "Normalized");
        System.out.println();
        for (var term : QTF.entrySet()) {
            System.out.printf("%-12s", term.getKey());
            var frequency = term.getValue();
            System.out.printf("%-5s", frequency);
            System.out.printf("%-5s ", QWTF.get(term.getKey()));
            System.out.printf("%.4f     ", IDF.get(term.getKey()));
            System.out.printf("%.4f     ", QTF_IDF.get(term.getKey()));
            System.out.printf("%.4f     ", QTermsNormalize.get(term.getKey()));
            System.out.println();
        }
        System.out.println("Query Length: " + QLength);
    }

    //    Handel Similarity
    private static void calculateAndPrintProduct() {
        Similarity = new TreeMap<>();
        System.out.println("\n------------------Product (query * matched docs)-------------");
        System.out.printf("%-12s", "Term");
        for (int docid : matchedDocs) {
            System.out.printf("%-12s", "d" + docid);
            Similarity.put(docid, 0.0);
        }
        System.out.println();
        double product = 0.0;
        HashMap<Integer, Double> docMap = new HashMap<>();
        for (var entry : QTermsNormalize.entrySet()) {
            String term = entry.getKey();
            System.out.printf("%-12s", term);
            docMap = DocNormalize.get(term);
            for (int docId : matchedDocs) {
                double queryTermNormalize = QTermsNormalize.get(term);
                double docNormalize = docMap != null && docMap.containsKey(docId) ? docMap.get(docId) : 0.0;
                product = docNormalize * queryTermNormalize;
                System.out.printf("%.4f     ", product);
                Similarity.put(docId, Similarity.get(docId) + product);
            }
            System.out.println();
        }
        System.out.printf("%-12s", "Total");
        for (int docid : matchedDocs) {
            double similarityValue = Similarity.getOrDefault(docid, 0.0);
            System.out.printf("%.4f     ", Similarity.get(docid));
        }
        System.out.println();
    }

    private static void printSimilarity() {
        System.out.println();
        for (int docid : matchedDocs) {
            System.out.printf("Similarity( q , Doc " + docid + " ) : ");
            System.out.printf("%-4f", Similarity.get(docid));
            System.out.println();
        }
        List<Map.Entry<Integer, Double>> sortedSimilarity = new ArrayList<>(Similarity.entrySet());
        sortedSimilarity.sort((entry1, entry2) -> Double.compare(entry2.getValue(), entry1.getValue()));
        System.out.print("Returned Docs : ");
        for (var entry : sortedSimilarity) {
            System.out.print("Doc " + entry.getKey() + "\t");
        }
        System.out.println();
    }
}





















