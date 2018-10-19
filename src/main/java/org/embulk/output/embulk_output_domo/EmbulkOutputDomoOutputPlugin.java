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


public class EmbulkOutputDomoOutputPlugin
        implements OutputPlugin
{
    private static DomoClient client = null;
    private static Execution execution = null;
    private static StreamClient sdsClient = null;
    private static Stream sds = null;
    private static TimestampFormatter[] timestampFormatters = null;
    private int partNum = 1;
    private int totalRecords = 1;
    protected static Logger logger;
    private static ArrayList<StringBuilder> allRecords = new ArrayList<StringBuilder>();
    private static ArrayList<String> recordsParts = new ArrayList<String>();
    private int currentPartCounter = 1;
    private static int totalBatches = 1;
    private static int pageReaderCount = 0;

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

        @Config("updateMethod")
        @ConfigDefault("REPLACE")
        public String getUpdateMethod();

        @Config("streamName")
        public String getStreamName();

        @Config("column_options")
        @ConfigDefault("{}")
        Map<String, TimestampColumnOption> getColumnOptions();

        @Config("batchSize")
        @ConfigDefault("1000000")
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
        String updateMethod = task.getUpdateMethod();

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
                sdsClient = client.streamClient();

                List<Stream> searchedSds = sdsClient.search("dataSource.name:" + task.getStreamName());
                sds = searchedSds.get(0);
                logger.info("Stream "+ sds);
                execution = sdsClient.createExecution(sds.getId());
                logger.info("Created Execution: " + execution);
                timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions());
                totalBatches = task.getBatchSize();

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
        List<List<StringBuilder>> batchLists = batches(allRecords, totalBatches);
        int i=1;
        for(List<StringBuilder> l : batchLists){
            sdsClient.uploadDataPart(sds.getId(), execution.getId(), i, stringifyList(l));
            i++;
        }
        logger.info("Finished Uploading");
        //Commit Execution
        Execution committedExecution = sdsClient.commitExecution(sds.getId(), execution.getId());
        logger.info("Committed Execution: " + committedExecution);
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

        private Schema schema;
        ArrayList<StringBuilder> recordsPage = new ArrayList<StringBuilder>();

        public DomoPageOutput(final PageReader pageReader,
                                    DomoClient client, PluginTask task, Schema schema)
        {
            //logger.info("NEW PAGE CONSTRUCTOR!!");
            this.pageReader = pageReader;
            this.client = client;
            this.task = task;
            this.schema = schema;
        }

        @Override
        public void add(Page page)
        {
            try {
                pageReader.setPage(page);

                final char delimiter = ',';
                final String delimiterString = ",";
                final String nullString = "";
                final QuotePolicy quotePolicy = this.task.getQuotePolicy();
                final char quote = this.task.getQuoteChar() != '\0' ? this.task.getQuoteChar() : '"';
                final char escape = this.task.getEscapeChar().or(quotePolicy == QuotePolicy.NONE ? '\\' : quote);
                final String newlineInField = this.task.getNewlineInField().getString();

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
                    totalRecords++;
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
            allRecords.addAll(recordsPage);
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

    private String setQuoteValue(String v, char quote)
    {

        return String.valueOf(quote) + v + quote;
    }

    private String stringifyList(List<StringBuilder> records){
        StringBuilder sb = new StringBuilder();
        for (StringBuilder s : records)
        {
            if(s!=null) {
                sb.append(s);
                sb.append("\n");
            }
            else{
                logger.info("NULL Found!");
            }
        }
        return sb.toString();
    }

    private String stringify(ArrayList<StringBuilder> records) {
        StringBuilder sb = new StringBuilder();
        for (StringBuilder s : records)
        {
            sb.append(s);
            sb.append("\n");
        }
        return sb.toString();
    }
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
}
