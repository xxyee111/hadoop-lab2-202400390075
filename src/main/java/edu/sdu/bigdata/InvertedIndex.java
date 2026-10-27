package edu.sdu.bigdata;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/** One posting per word/document, followed by the corpus frequency. */
public final class InvertedIndex extends Configured implements Tool {
    public enum Stats { TOKENS_SEEN, STOPWORDS_REMOVED, TOKENS_INDEXED }
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Pattern WORD = Pattern.compile("[a-z]+");
        private final Set<String> stopwords = new HashSet<>();
        private String document;
        @Override protected void setup(Context ctx) throws IOException {
            document = ((FileSplit) ctx.getInputSplit()).getPath().getName();
            File local = new File("stopwords.txt");
            InputStream input;
            if (local.isFile()) input = new FileInputStream(local);
            else {
                Path path = new Path(ctx.getConfiguration().get("index.stopwords"));
                input = path.getFileSystem(ctx.getConfiguration()).open(path);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String word = line.trim().toLowerCase(Locale.ROOT);
                    if (!word.isEmpty()) stopwords.add(word);
                }
            }
            if (stopwords.isEmpty()) throw new IOException("Stopword file is empty");
        }
        @Override protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            Matcher matcher = WORD.matcher(line.toString().toLowerCase(Locale.ROOT));
            Map<String, Long> counts = new HashMap<>();
            while (matcher.find()) {
                String word = matcher.group();
                ctx.getCounter(Stats.TOKENS_SEEN).increment(1);
                if (stopwords.contains(word)) ctx.getCounter(Stats.STOPWORDS_REMOVED).increment(1);
                else {
                    ctx.getCounter(Stats.TOKENS_INDEXED).increment(1);
                    counts.put(word, counts.getOrDefault(word, 0L) + 1);
                }
            }
            for (Map.Entry<String, Long> entry : counts.entrySet())
                ctx.write(new Text(entry.getKey()), new Text(document + "\t" + entry.getValue()));
        }
    }
    private static SortedMap<String, Long> aggregate(Iterable<Text> values) throws IOException {
        SortedMap<String, Long> postings = new TreeMap<>();
        for (Text value : values) {
            String item = value.toString();
            int split = item.lastIndexOf('\t');
            if (split <= 0) throw new IOException("Invalid posting: " + item);
            String document = item.substring(0, split);
            long count = Long.parseLong(item.substring(split + 1));
            postings.put(document, Math.addExact(postings.getOrDefault(document, 0L), count));
        }
        return postings;
    }
    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {
        @Override protected void reduce(Text word, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            for (Map.Entry<String, Long> entry : aggregate(values).entrySet())
                ctx.write(word, new Text(entry.getKey() + "\t" + entry.getValue()));
        }
    }
    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override protected void reduce(Text word, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            long total = 0;
            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, Long> entry : aggregate(values).entrySet()) {
                total = Math.addExact(total, entry.getValue());
                result.append('<').append(entry.getKey()).append(',').append(entry.getValue()).append(">;");
            }
            result.append("<total,").append(total).append(">.");
            ctx.write(word, new Text(result.toString()));
        }
    }
    @Override public int run(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage: InvertedIndex <input> <output> <stopwords> [reducers]");
            return 2;
        }
        Path input = new Path(args[0]), output = new Path(args[1]), stop = new Path(args[2]);
        FileSystem fs = input.getFileSystem(getConf());
        Set<String> names = new HashSet<>();
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(input, true);
        while (files.hasNext()) {
            String name = files.next().getPath().getName();
            if (!name.startsWith("_") && !name.startsWith(".")) {
                if (name.equals("total") || name.matches(".*[<>,;\\t\\r\\n].*"))
                    throw new IOException("Unsupported document name: " + name);
                if (!names.add(name)) throw new IOException("Duplicate document basename: " + name);
            }
        }
        if (names.isEmpty()) throw new IOException("No input documents");
        int reducers = args.length == 4 ? Integer.parseInt(args[3]) : 1;
        if (reducers < 1) throw new IllegalArgumentException("At least one reducer is required");
        getConf().set("index.stopwords", stop.toString());
        Job job = Job.getInstance(getConf(), "lab2-document-inverted-index");
        job.setJarByClass(InvertedIndex.class);
        job.addCacheFile(new URI(stop.getFileSystem(getConf()).makeQualified(stop).toString() + "#stopwords.txt"));
        job.setMapperClass(IndexMapper.class);
        if (!getConf().getBoolean("index.disable.combiner", false)) job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean ok = job.waitForCompletion(true);
        if (ok) {
            System.out.println("DOCUMENTS=" + names.size());
            System.out.println("JOB_ID=" + job.getJobID());
            for (Stats counter : Stats.values())
                System.out.println(counter + "=" + job.getCounters().findCounter(counter).getValue());
            System.out.println("INVERTED_INDEX_JOB_SUCCEEDED");
        }
        return ok ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InvertedIndex(), args));
    }
}
