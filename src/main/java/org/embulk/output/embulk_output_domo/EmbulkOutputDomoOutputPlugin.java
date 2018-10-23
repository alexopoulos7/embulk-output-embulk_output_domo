/**
 * Embulk output plugin that can move really big input to domo stream
 * We are going to use algorithm for uploading in parallel found in https://developer.domo.com/docs/stream/upload-in-parallel
 */
package org.embulk.output.embulk_output_domo;

import java.lang.StringBuilder;
import com.google.common.base.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.spi.Schema;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.PageReader;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Timestamps;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Newline;
import org.slf4j.Logger;

import com.domo.sdk.datasets.model.ColumnType;
import com.domo.sdk.datasets.model.CreateDataSetRequest;
import com.domo.sdk.streams.model.Execution;
import com.domo.sdk.streams.model.Stream;
import com.domo.sdk.streams.model.StreamRequest;
import com.domo.sdk.streams.model.UpdateMethod;
import com.domo.sdk.datasets.model.DataSet;
import com.domo.sdk.datasets.DataSetClient;
import com.domo.sdk.DomoClient;
import com.domo.sdk.streams.StreamClient;

import okhttp3.logging.HttpLoggingInterceptor;

import static com.domo.sdk.request.Scope.DATA;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileOutputStream;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPOutputStream;
import java.util.Collections;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.io.FileUtils;


public class EmbulkOutputDomoOutputPlugin
        implements OutputPlugin
{
    private static DomoClient client = null;
    private static Execution execution = null;
    private static StreamClient streamClient = null;
    private static Stream sds = null;
    private static TimestampFormatter[] timestampFormatters = null;
    private int partNum = 1;
    private int totalRecords = 1;
    protected static Logger logger;
    private static ArrayList<StringBuilder> allRecords = new ArrayList<StringBuilder>();
    private static ArrayList<String> recordsParts = new ArrayList<String>();
    private int currentPartCounter = 1;
    public static int totalBatches = 1;
    private static int pageReaderCount = 0;
    private static String TEMP_DIR = "/tmp/csv/" +RandomStringUtils.randomAlphabetic(10)+"/";
    public static int totalRecordsCounter = 0;

    public enum QuotePolicy
    {
        ALL("ALL"),
        MINIMAL("MINIMAL"),
        NONE("NONE");

        private final String string;

        QuotePolicy(String string)
        {
            this.string = string;
        }

        public String getString()
        {
            return string;
        }
    }
    public interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption
    {
    }
    public interface PluginTask
            extends Task, TimestampFormatter.Task
    {
        @Config("clientId")
        public String getClientId();

        @Config("clientSecret")
        public String getClientSecret();

        @Config("apiHost")
        @ConfigDefault("api.domo.com")
        public String getApiHost();

        @Config("useHttps")
        @ConfigDefault("true")
        public boolean getUseHttps();

        @Config("streamName")
        public String getStreamName();

        @Config("column_options")
        @ConfigDefault("{}")
        Map<String, TimestampColumnOption> getColumnOptions();

        // Not used we get data in batches from input plugin, so we can configure there
        @Config("batchSize")
        @ConfigDefault("1000")
        public int getBatchSize();

        @Config("quote")
        @ConfigDefault("\"\\\"\"")
        char getQuoteChar();

        @Config("quote_policy")
        @ConfigDefault("\"MINIMAL\"")
        QuotePolicy getQuotePolicy();

        @Config("escape")
        @ConfigDefault("null")
        Optional<Character> getEscapeChar();

        @Config("newline_in_field")
        @ConfigDefault("\"LF\"")
        Newline getNewlineInField();
    }


    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        logger = Exec.getLogger(getClass());

        final String clientId = task.getClientId();
        final String clientSecret = task.getClientSecret();
        final String apiHost = task.getApiHost();
        final boolean useHttps = task.getUseHttps();

        try {
            if (client == null) {
                //getDomoSchema(schema);
                com.domo.sdk.request.Config domoConfig = com.domo.sdk.request.Config.with()
                        .clientId(clientId)
                        .clientSecret(clientSecret)
                        .apiHost(apiHost)
                        .useHttps(useHttps)
                        .scope(DATA)
                        .httpLoggingLevel(HttpLoggingInterceptor.Level.BODY)
                        .build();

                client = DomoClient.create(domoConfig);
                streamClient = client.streamClient();

                List<Stream> searchedSds = streamClient.search("dataSource.name:" + task.getStreamName());
                sds = searchedSds.get(0);
                logger.info("Stream "+ sds);
                execution = streamClient.createExecution(sds.getId());
                logger.info("Created Execution: " + execution);
                timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions());
                totalBatches = task.getBatchSize();
                File directory = new File(TEMP_DIR);
                if(!directory.exists()) {
                    directory.mkdirs();
                }
            }
        }
        catch(Exception ex){
            logger.error("Exception on Running Output plugin.");
            throw new RuntimeException(ex);
        }

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }
    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("embulk_output_domo output plugin does not support resuming");
    }
    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports)
    {
        try{
            ArrayList<File> csvFiles = loadCSVFiles(TEMP_DIR);
            File tempFolder = new File(TEMP_DIR);
            List<File> compressedCsvFiles = toGzipFilesUTF8(csvFiles, tempFolder.getPath() + "/");
            ExecutorService executorService = Executors.newCachedThreadPool();
            List<Callable<Object>> uploadTasks = Collections.synchronizedList(new ArrayList<>());

            // For each data part (csv gzip file), create a runnable upload task
            long partNum = 1;
            for (File compressedCsvFile : compressedCsvFiles){
                long myPartNum = partNum;
                // "uploadDataPart()" accepts csv strings, csv files, and compressed csv files
                Runnable partUpload = () -> streamClient.uploadDataPart(sds.getId(), execution.getId(), myPartNum, compressedCsvFile);
                uploadTasks.add(Executors.callable(partUpload));
                partNum++;
             }
            // Asynchronously execute all uploading tasks
            try {
                executorService.invokeAll(uploadTasks);
            }
            catch (Exception e){
                logger.error("Error uploading all data parts", e);
            }

        }catch(Exception e) {
            logger.error("Exception on uploading!! "+e);
            System.out.println(e.getMessage());
            return;
        }
        //Commit Execution
        Execution committedExecution = streamClient.commitExecution(sds.getId(), execution.getId());
        logger.info("Committed Execution: " + committedExecution);
        try {
            FileUtils.deleteDirectory(new File(TEMP_DIR));
            logger.info("Delete temp directory");
        }
        catch (IOException ex){
            logger.error("Delete temp directory Failed "+ ex);
        }

    }
    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final PageReader reader = new PageReader(schema);
        return new DomoPageOutput(reader, client, task, schema);
    }
    public class DomoPageOutput
            implements TransactionalPageOutput
    {
        private final String dateSuffix = new SimpleDateFormat("yyyyMMddhhmmssSSS").format(new Date());

        private final PageReader pageReader;
        private DomoClient client;
        private PluginTask task;
        private int partPageNum;

        private Schema schema;
        ArrayList<StringBuilder> recordsPage = null;
        private char delimiter = ',';
        private String delimiterString = ",";
        private String nullString = "";
        private QuotePolicy quotePolicy = null;
        private char quote = '"';
        private char escape =  quote;
        private String newlineInField;

        public DomoPageOutput(final PageReader pageReader,
                              DomoClient client, PluginTask task, Schema schema)
        {
            logger.info("NEW PAGE CONSTRUCTOR!!");
            this.pageReader = pageReader;
            this.client = client;
            this.task = task;
            this.schema = schema;

            this.partPageNum = partNum++;
            this.quotePolicy = this.task.getQuotePolicy();
            this.quote = this.task.getQuoteChar() != '\0' ? this.task.getQuoteChar() : '"';
            this.escape = this.task.getEscapeChar().or(this.quotePolicy == QuotePolicy.NONE ? '\\' : this.quote);
            this.newlineInField = this.task.getNewlineInField().getString();
            this.delimiter = ',';
            this.delimiterString = ",";
            this.nullString = "";
        }

        /**
         * Main Transactional Page that loops
         * @param page
         */
        @Override
        public void add(Page page)
        {
            final StringBuilder pageBuilder = new StringBuilder();
            //logger.info("New page");
            this.recordsPage = new ArrayList<StringBuilder>();
            try {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    StringBuilder lineBuilder = new StringBuilder();
                    pageReader.getSchema().visitColumns(new ColumnVisitor() {
                        private void addValue(String v)
                        {
                            lineBuilder.append(setEscapeAndQuoteValue(v, delimiter, quotePolicy, quote, escape, newlineInField, nullString));
                        }

                        private void addNullString()
                        {
                            lineBuilder.append(nullString);
                        }

                        private void addDelimiter(Column column)
                        {
                            if (column.getIndex() != 0) {
                                lineBuilder.append(delimiterString);
                            }
                        }
                        @Override
                        public void doubleColumn(Column column) {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(Long.toString(pageReader.getLong(column)));
                            } else {
                                addNullString();
                            }
                        }
                        @Override
                        public void timestampColumn(Column column) {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                Timestamp value = pageReader.getTimestamp(column);
                                addValue(timestampFormatters[column.getIndex()].format(value));
                            } else {
                                addNullString();
                            }
                        }
                        @Override
                        public void stringColumn(Column column) {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(pageReader.getString(column));
                            } else {
                                addNullString();
                            }
                        }
                        @Override
                        public void longColumn(Column column) {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(Long.toString(pageReader.getLong(column)));
                            } else {
                                addNullString();
                            }
                        }
                        @Override
                        public void booleanColumn(Column column) {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(Boolean.toString(pageReader.getBoolean(column)));
                            } else {
                                addNullString();
                            }
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(pageReader.getJson(column).toJson());
                            } else {
                                addNullString();
                            }
                        }
                    });
                    recordsPage.add(lineBuilder);
                }
                try {
                    //save as csv
                    WriteToFile(stringify(recordsPage), RandomStringUtils.randomNumeric(10)+RandomStringUtils.randomAlphabetic(20).toString()+".csv");
                }
                catch (IOException e){
                    logger.error("Exception on closing page!");
                    logger.error(e.getMessage());
                }
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void finish()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public void abort()
        {
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }
    }
    /************************ H E L P E R   M E T H O D S *****************************/

    /**
     *
     * @param v String value
     * @param delimiter csv delimeter
     * @param policy enum QuotePolicy
     * @param quote Quote Character
     * @param escape Escape Character
     * @param newline NewLine Character
     * @param nullString Null string
     * @return String
     */
    private String setEscapeAndQuoteValue(String v, char delimiter, QuotePolicy policy, char quote, char escape, String newline, String nullString)
    {
        StringBuilder escapedValue = new StringBuilder();
        char previousChar = ' ';

        boolean isRequireQuote = (policy == QuotePolicy.ALL || policy == QuotePolicy.MINIMAL && v.equals(nullString));

        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);

            if (policy != QuotePolicy.NONE && c == quote) {
                escapedValue.append(escape);
                escapedValue.append(c);
                isRequireQuote = true;
            } else if (c == '\r') {
                if (policy == QuotePolicy.NONE) {
                    escapedValue.append(escape);
                }
                escapedValue.append(newline);
                isRequireQuote = true;
            } else if (c == '\n') {
                if (previousChar != '\r') {
                    if (policy == QuotePolicy.NONE) {
                        escapedValue.append(escape);
                    }
                    escapedValue.append(newline);
                    isRequireQuote = true;
                }
            } else if (c == delimiter) {
                if (policy == QuotePolicy.NONE) {
                    escapedValue.append(escape);
                }
                escapedValue.append(c);
                isRequireQuote = true;
            } else {
                escapedValue.append(c);
            }
            previousChar = c;
        }

        if (policy != QuotePolicy.NONE && isRequireQuote) {
            return setQuoteValue(escapedValue.toString(), quote);
        } else {
            return escapedValue.toString();
        }
    }

    /**
     * Quote a string
     * @param v
     * @param quote
     * @return String
     */
    private String setQuoteValue(String v, char quote)
    {
        return String.valueOf(quote) + v + quote;
    }

    /**
     * Return a list of all CSV files inside a folder
     * @param searchFolder String
     * @return an Arraylist of File Objects
     */
    public static ArrayList<File> loadCSVFiles (String searchFolder) {
        File folder = new File(searchFolder);
        File[] listOfFiles = folder.listFiles();
        ArrayList<File> csvFiles = new ArrayList<File>();

        for (File file : listOfFiles) {
            if (file.isFile() && file.getName().indexOf(".csv")>0) {
                csvFiles.add(file);
            }
        }
        return csvFiles;
    }

    /**
     * Stringify an ArrayList of StringBuilder to a  String
     * @param records ArrayList of <StringBuilder>
     * @return String
     */
    private String stringify(ArrayList<StringBuilder> records) {
        StringBuilder sb = new StringBuilder();
        for (StringBuilder s : records)
        {
            sb.append(s);
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Not used currently. It slices a List of a Templated input to chunkSize
     * @param input List<T>
     * @param chunkSize Int
     * @param <T>
     * @return
     */
    public static <T> List<List<T>> batches(List<T> input, int chunkSize) {

        int inputSize = input.size();
        int chunkCount = (int) Math.ceil(inputSize / (double) chunkSize);

        Map<Integer, List<T>> map = new HashMap<>(chunkCount);
        List<List<T>> chunks = new ArrayList<>(chunkCount);

        for (int i = 0; i < inputSize; i++) {

            map.computeIfAbsent(i / chunkSize, (ignore) -> {

                List<T> chunk = new ArrayList<>();
                chunks.add(chunk);
                return chunk;

            }).add(input.get(i));
        }

        return chunks;
    }

    /**
     * Create a List of Zip files. Each zip file will contain a batch of csv files
     * @param sourceFiles A List of Source <Files>
     * @param path string
     * @return List<File>
     */
    public static List<File> toGzipFilesUTF8( List<File> sourceFiles, String path){
        List<File> files = new ArrayList<>();
        int currentCount = 0;
        int remaining = sourceFiles.size();
        ArrayList<File> batchFiles = new ArrayList<File>();
        //System.out.println("All source files are "+remaining);
        for (File sourceFile : sourceFiles) {
            currentCount++;
            batchFiles.add(sourceFile);
            if(currentCount>=totalBatches || currentCount>=remaining){
                remaining=remaining-totalBatches;
                String zipFileName = sourceFile.getName().replace(".csv", ".zip");
                files.add(toGzipFileUTF8(batchFiles, path + zipFileName));
                // System.out.println("Add file "+sourceFile.getName()+"to zip file name = "+zipFileName+". Current count = "+currentCount +" Total records counter = "+totalRecordsCounter);
                batchFiles.clear();
                currentCount = 0;
            }
            //System.out.println("Add file "+sourceFile.getName()+ ". Current count = "+currentCount);

        }
        return files;
    }

    /**
     * Read csv Files as UTF-8, convert to String
     * @param csvFiles
     * @param zipFilePath
     * @return a Zip File
     */
    public static File toGzipFileUTF8(ArrayList<File> csvFiles, String zipFilePath){
        File outputFile = new File(zipFilePath);
        try {
            GZIPOutputStream gzos = new GZIPOutputStream(new FileOutputStream(outputFile));
            for (File csvFile : csvFiles){
                BufferedReader reader = new BufferedReader(new FileReader(csvFile));

                String currentLine;
                while ((currentLine = reader.readLine()) != null){
                    currentLine += System.lineSeparator();
                    totalRecordsCounter++;
                    // Specifying UTF-8 encoding is critical; getBytes() uses ISO-8859-1 by default
                    gzos.write(currentLine.getBytes("UTF-8"));
                }
            }
            gzos.flush();
            gzos.finish();
            gzos.close();

        }
        catch(IOException e) {
            logger.error("Error compressing a string to gzip", e);
        }

        return outputFile;
    }

    /**
     * Writes a CSV File
     * @param fileContent
     * @param fileName
     * @throws IOException
     */
    public static void WriteToFile(String fileContent, String fileName) throws IOException {
        //logger.info("writing csv file to "+fileName);

        String tempFile = TEMP_DIR + fileName;
        File file = new File(tempFile);
        // if file does exists, then delete and create a new file
        if (file.exists()) {
            try {
                File newFileName = new File(TEMP_DIR + "backup_" + fileName);
                file.renameTo(newFileName);
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(fileContent);
        bw.close();
    }

    /**
     * Get Domo Schema
     * @param schema
     * @return
     */
    public com.domo.sdk.datasets.model.Schema getDomoSchema(Schema schema){
        /**
         * We need to return domo Schema
         * e.g. new com.domo.sdk.datasets.model.Schema(Lists.newArrayList(new Column(STRING, "Friend"), new Column(STRING, "Attending")))
         */
        ArrayList<com.domo.sdk.datasets.model.Column> domoSchema =  new ArrayList<com.domo.sdk.datasets.model.Column>();
        for (int i = 0; i < schema.size(); i++) {
            Column column = schema.getColumn(i);
            Type type = column.getType();
            System.out.println("{\n" +
                    "      \"type\" : \""+type.getName().toUpperCase()+"\",\n" +
                    "      \"name\" : \""+column.getName() +"\"\n" +
                    "    },");
            switch (type.getName()) {
                case "long":
                    domoSchema.add(new com.domo.sdk.datasets.model.Column(ColumnType.LONG, column.getName()));
                    break;
                case "double":
                    domoSchema.add(new com.domo.sdk.datasets.model.Column(ColumnType.DOUBLE, column.getName()));
                    break;
                case "boolean":
                    domoSchema.add(new com.domo.sdk.datasets.model.Column( ColumnType.LONG, column.getName()));
                    break;
                case "string":
                    domoSchema.add(new com.domo.sdk.datasets.model.Column(ColumnType.STRING, column.getName()));
                    break;
                case "timestamp":
                    domoSchema.add(new com.domo.sdk.datasets.model.Column( ColumnType.DATETIME, column.getName()));
                    break;
                default:
                    logger.info("Unsupported type " + type.getName());
                    break;
            }
        }
        if(domoSchema != null && domoSchema.size()>0){
            return new com.domo.sdk.datasets.model.Schema(domoSchema);
        }
        else{
            logger.error("Cannot create domo schema");
            throw new RuntimeException("Cannot create domo Schema");
        }
    }
}