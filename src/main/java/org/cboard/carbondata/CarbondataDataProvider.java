package org.cboard.carbondata;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.cboard.cache.CacheManager;
import org.cboard.cache.HeapCacheManager;
import org.cboard.dataprovider.DataProvider;
import org.cboard.dataprovider.Initializing;
import org.cboard.dataprovider.aggregator.Aggregatable;
import org.cboard.dataprovider.annotation.ProviderName;
import org.cboard.dataprovider.annotation.QueryParameter;
import org.cboard.dataprovider.config.*;
import org.cboard.dataprovider.result.AggregateResult;
import org.cboard.dataprovider.result.ColumnIndex;
import org.cboard.dto.Carbondata;
import org.cboard.jdbc.JdbcDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by frank on 17-8-29.
 */
@ProviderName(name="carbondata")
public class CarbondataDataProvider extends DataProvider implements Aggregatable,Initializing{



    private static final Logger LOG = LoggerFactory.getLogger(CarbondataDataProvider.class);

    @QueryParameter(label = "{{'DATAPROVIDER.JDBC.SQLTEXT'|translate}}",
            type = QueryParameter.Type.TextArea,
            required = true,
            order = 1)
    private String SQL = "sql";

    private static final CacheManager<Map<String, Integer>> typeCahce = new HeapCacheManager<>();

    private static final ConcurrentMap<String, DataSource> datasourceMap = new ConcurrentHashMap<>();

    private CarbondataDataProvider.DimensionConfigHelper dimensionConfigHelper;
    private String getKey() {
        return Hashing.md5().newHasher().putString(JSONObject.toJSON(dataSource).toString() + JSONObject.toJSON(query).toString(), Charsets.UTF_8).hash().toString();
    }

    @Override
    public String[] queryDimVals(String columnName, AggConfig config) throws Exception {
        return new String[0];
    }


    @Override
    public AggregateResult queryAggData(AggConfig config) throws Exception {
        String exec=getQueryAggDataSql(config);
        List<String[]> list=new LinkedList<>();
        LOG.info(exec);
        try{
            ReportServer_Interface.Client connection = getConnection();
            String json=connection.cboardsql(exec);
            Carbondata data=JSONObject.parseObject(json,Carbondata.class);
            for(String[] row:data.getData()){
                list.add(row);
            }
        }catch (Exception e ){
            LOG.error("ERROR:" + e.getMessage());
            throw new Exception("ERROR:" + e.getMessage(), e);
        }

        // recreate a dimension stream
        Stream<DimensionConfig> dimStream = Stream.concat(config.getColumns().stream(), config.getRows().stream());
        List<ColumnIndex> dimensionList = dimStream.map(ColumnIndex::fromDimensionConfig).collect(Collectors.toList());
        int dimSize = dimensionList.size();
        dimensionList.addAll(config.getValues().stream().map(ColumnIndex::fromValueConfig).collect(Collectors.toList()));
        IntStream.range(0, dimensionList.size()).forEach(j -> dimensionList.get(j).setIndex(j));
        list.forEach(row -> {
            IntStream.range(0, dimSize).forEach(i -> {
                if (row[i] == null) row[i] = NULL_STRING;
            });
        });
        String[][] result = list.toArray(new String[][]{});
        return new AggregateResult(dimensionList, result);
    }



    @Override
    public boolean doAggregationInDataSource() {
        return true;
    }

    @Override
    public String[][] getData() throws Exception {
        return new String[0][];
    }

    /**
     * Convert the sql text to subquery string:
     * remove blank line
     * remove end semicolon ;
     *
     * @param rawQueryText
     * @return
     */
    private String getAsSubQuery(String rawQueryText) {
        String deletedBlankLine = rawQueryText.replaceAll("(?m)^[\\s\t]*\r?\n", "").trim();
        return deletedBlankLine.endsWith(";") ? deletedBlankLine.substring(0, deletedBlankLine.length() - 1) : deletedBlankLine;
    }

    private String getQueryAggDataSql(AggConfig config) throws Exception {
        Stream<DimensionConfig> c = config.getColumns().stream();
        Stream<DimensionConfig> r = config.getRows().stream();
        Stream<ConfigComponent> f = config.getFilters().stream();
        Stream<ConfigComponent> filters = Stream.concat(Stream.concat(c, r), f);
        Map<String, Integer> types = getColumnType();
        Stream<DimensionConfig> dimStream = Stream.concat(config.getColumns().stream(), config.getRows().stream());

        String dimColsStr = assembleDimColumns(dimStream);
        String aggColsStr = assembleAggValColumns(config.getValues().stream(), types);
        String whereStr = assembleSqlFilter(filters, "WHERE");
        String groupByStr = StringUtils.isBlank(dimColsStr) ? "" : "GROUP BY " + dimColsStr;

        StringJoiner selectColsStr = new StringJoiner(",");
        if (!StringUtils.isBlank(dimColsStr)) {
            selectColsStr.add(dimColsStr);
        }
        if (!StringUtils.isBlank(aggColsStr)) {
            selectColsStr.add(aggColsStr);
        }

        String subQuerySql = getAsSubQuery(query.get(SQL));
        String fsql = "\nSELECT %s \n FROM (\n%s\n) cb_view \n %s \n %s";
        String exec = String.format(fsql, selectColsStr, subQuerySql, whereStr, groupByStr);
        return exec;
    }

    /**
     * Assemble all the filter to a legal sal where script
     *
     * @param filterStream
     * @param prefix       HAVING or WHERE
     * @return
     */
    private String assembleSqlFilter(Stream<ConfigComponent> filterStream, String prefix) {
        StringJoiner where = new StringJoiner("\nAND ", prefix + " ", "");
        where.setEmptyValue("");
        filterStream.map(e -> separateNull(e)).map(e -> configComponentToSql(e)).filter(e -> e != null).forEach(where::add);
        return where.toString();
    }

    private String assembleAggValColumns(Stream<ValueConfig> selectStream, Map<String, Integer> types) {
        StringJoiner columns = new StringJoiner(", ", "", " ");
        columns.setEmptyValue("");
        selectStream.map(m -> toSelect.apply(m, types)).filter(e -> e != null).forEach(columns::add);
        return columns.toString();
    }

    private String assembleDimColumns(Stream<DimensionConfig> columnsStream) {
        StringJoiner columns = new StringJoiner(", ", "", " ");
        columns.setEmptyValue("");
        columnsStream.map(g -> g.getColumnName()).distinct().filter(e -> e != null).forEach(columns::add);
        return columns.toString();
    }

    private ResultSetMetaData getMetaData(String subQuerySql, Statement stat) throws Exception {
        ResultSetMetaData metaData;
        try {
            stat.setMaxRows(100);
            String fsql = "\nSELECT * FROM (\n%s\n) cb_view WHERE 1=0";
            String sql = String.format(fsql, subQuerySql);
            LOG.info(sql);
            ResultSet rs = stat.executeQuery(sql);
            metaData = rs.getMetaData();
        } catch (Exception e) {
            LOG.error("ERROR:" + e.getMessage());
            throw new Exception("ERROR:" + e.getMessage(), e);
        }
        return metaData;
    }

    private Map<String, Integer> getColumnType() throws Exception {
        Map<String, Integer> result = null;
        String key = getKey();
        result = typeCahce.get(key);
        if (result != null) {
            return result;
        } else {
            try {

                String subQuerySql = getAsSubQuery(query.get(SQL));
                ReportServer_Interface.Client connection = getConnection();
                String json=connection.cboardsql(subQuerySql);
                Carbondata data=JSONObject.parseObject(json,Carbondata.class);
                int columnCount = data.getColumns().size();
                result = new HashedMap();
                for (int i = 0; i < columnCount; i++) {
                    result.put(data.getColumns().get(i), 1);
                }
                typeCahce.put(key, result, 12 * 60 * 60 * 1000);

            }catch (Exception e ){
                e.printStackTrace();
            }
            return result;
        }
    }

    @Override
    public String[] getColumn() throws Exception {
        String subQuerySql = getAsSubQuery(query.get(SQL));
        String[] row=null;
        try{
            ReportServer_Interface.Client connection = getConnection();
            String json=connection.cboardsql(subQuerySql);
            Carbondata data=JSONObject.parseObject(json,Carbondata.class);
            int columnCount = data.getColumns().size();
            row = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = data.getColumns().get(i);
            }

        }catch (Exception e ){
            e.printStackTrace();
        }
        return row;
    }

    @Override
    public String viewAggDataQuery(AggConfig config) throws Exception {
        return getQueryAggDataSql(config);
    }

    private BiFunction<ValueConfig, Map<String, Integer>, String> toSelect = (config, types) -> {
        String aggExp;
        if (config.getColumn().contains(" ")) {
            aggExp = config.getColumn();
            for (String column : types.keySet()) {
                aggExp = aggExp.replaceAll(" " + column + " ", " cb_view." + column + " ");
            }
        } else {
            aggExp = "cb_view." + config.getColumn();
        }
        switch (config.getAggType()) {
            case "sum":
                return "SUM(" + aggExp + ")";
            case "avg":
                return "AVG(" + aggExp + ")";
            case "max":
                return "MAX(" + aggExp + ")";
            case "min":
                return "MIN(" + aggExp + ")";
            case "distinct":
                return "COUNT(DISTINCT " + aggExp + ")";
            default:
                return "COUNT(" + aggExp + ")";
        }
    };

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            dimensionConfigHelper = new CarbondataDataProvider.DimensionConfigHelper();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class DimensionConfigHelper {
        private Map<String, Integer> types = getColumnType();

        private DimensionConfigHelper() throws Exception {
        }

        public String getValueStr(DimensionConfig dc, int index) {
            switch (types.get(dc.getColumnName())) {
                case Types.VARCHAR:
                case Types.CHAR:
                case Types.NVARCHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.DATE:
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return "'" + dc.getValues().get(index) + "'";
                default:
                    return dc.getValues().get(index);
            }
        }

    }

    private String configComponentToSql(ConfigComponent cc) {
        if (cc instanceof DimensionConfig) {
            return filter2SqlCondtion.apply((DimensionConfig) cc);
        } else if (cc instanceof CompositeConfig) {
            CompositeConfig compositeConfig = (CompositeConfig) cc;
            String sql = compositeConfig.getConfigComponents().stream().map(e -> separateNull(e)).map(e -> configComponentToSql(e)).collect(Collectors.joining(" " + compositeConfig.getType() + " "));
            return "(" + sql + ")";
        }
        return null;
    }


    /**
     * Parser a single filter configuration to sql syntax
     */
    private Function<DimensionConfig, String> filter2SqlCondtion = (config) -> {
        if (config.getValues().size() == 0) {
            return null;
        }
        if (NULL_STRING.equals(config.getValues().get(0))) {
            switch (config.getFilterType()) {
                case "=":
                case "≠":
                    return config.getColumnName() + ("=".equals(config.getFilterType()) ? " IS NULL" : " IS NOT NULL");
            }
        }

        switch (config.getFilterType()) {
            case "=":
            case "eq":
                return config.getColumnName() + " IN (" + IntStream.range(0, config.getValues().size()).boxed().map(i -> dimensionConfigHelper.getValueStr(config, i)).collect(Collectors.joining(",")) + ")";
            case "≠":
            case "ne":
                return config.getColumnName() + " NOT IN (" + IntStream.range(0, config.getValues().size()).boxed().map(i -> dimensionConfigHelper.getValueStr(config, i)).collect(Collectors.joining(",")) + ")";
            case ">":
                return config.getColumnName() + " > " + dimensionConfigHelper.getValueStr(config, 0);
            case "<":
                return config.getColumnName() + " < " + dimensionConfigHelper.getValueStr(config, 0);
            case "≥":
                return config.getColumnName() + " >= " + dimensionConfigHelper.getValueStr(config, 0);
            case "≤":
                return config.getColumnName() + " <= " + dimensionConfigHelper.getValueStr(config, 0);
            case "(a,b]":
                if (config.getValues().size() >= 2) {
                    return "(" + config.getColumnName() + " > '" + dimensionConfigHelper.getValueStr(config, 0) + "' AND " + config.getColumnName() + " <= " + dimensionConfigHelper.getValueStr(config, 1) + ")";
                } else {
                    return null;
                }
            case "[a,b)":
                if (config.getValues().size() >= 2) {
                    return "(" + config.getColumnName() + " >= " + dimensionConfigHelper.getValueStr(config, 0) + " AND " + config.getColumnName() + " < " + dimensionConfigHelper.getValueStr(config, 1) + ")";
                } else {
                    return null;
                }
            case "(a,b)":
                if (config.getValues().size() >= 2) {
                    return "(" + config.getColumnName() + " > " + dimensionConfigHelper.getValueStr(config, 0) + " AND " + config.getColumnName() + " < " + dimensionConfigHelper.getValueStr(config, 1) + ")";
                } else {
                    return null;
                }
            case "[a,b]":
                if (config.getValues().size() >= 2) {
                    return "(" + config.getColumnName() + " >= " + dimensionConfigHelper.getValueStr(config, 0) + " AND " + config.getColumnName() + " <= " + dimensionConfigHelper.getValueStr(config, 1) + ")";
                } else {
                    return null;
                }
        }
        return null;
    };

    private ReportServer_Interface.Client getConnection(){
        ReportServer_Interface.Client client=null;
        TTransport transport=null;
        Integer sendStatus =null;

        try{
            TSocket ts=  new TSocket("114.115.213.107", 9333);
            ts.setTimeout(180000);
            transport=ts;
            transport.open();
            TBinaryProtocol protocol = new  TBinaryProtocol(transport);
            client  = new ReportServer_Interface.Client(protocol);
            client.ping();
        }catch (Exception e){
            e.printStackTrace();

        }
        return  client;
    }

}
